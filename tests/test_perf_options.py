"""Tests for the performance options (Settings.log_snippets, monitor_repls, direct_exec,
header_timeout, prewarm), the event-driven disconnect wait and GET /stats."""

import asyncio
import os
import socket
import subprocess
import sys
import threading
import time
from uuid import uuid4

import httpx
import psutil
import pytest
import uvicorn
from fastapi.testclient import TestClient
from kimina_client import CheckRequest, Snippet
from starlette import status

from server.main import create_app
from server.manager import Manager
from server.settings import Environment, Settings


def _settings(**overrides: object) -> Settings:
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    defaults = {
        "max_repls": 2,
        "max_repl_uses": -1,
        "init_repls": {},
        "database_url": None,
        "environment": Environment.prod,
    }
    for k, v in {**defaults, **overrides}.items():
        setattr(s, k, v)
    return s


def _check(client: TestClient, code: str, timeout: int = 60) -> dict:  # type: ignore[type-arg]
    payload = CheckRequest(
        snippets=[Snippet(id=str(uuid4()), code=code)], timeout=timeout
    ).model_dump()
    resp = client.post("check", json=payload)
    assert resp.status_code == status.HTTP_200_OK
    return resp.json()["results"][0]  # type: ignore[no-any-return]


@pytest.mark.parametrize("log_snippets", [True, False])
def test_warm_responses_have_no_polling_floor(log_snippets: bool) -> None:
    """Upstream checked for a disconnect every 0.1 s before returning, so every response
    took at least 100 ms. A warm `#check Nat` takes a few milliseconds."""
    app = create_app(_settings(log_snippets=log_snippets, monitor_repls=False))
    with TestClient(app, base_url="http://testserver/api") as client:
        _check(client, "#check Nat")  # starts the REPL
        walls = []
        for _ in range(5):
            start = time.perf_counter()
            result = _check(client, "#check Nat")
            walls.append(time.perf_counter() - start)
            assert result["response"]["messages"][0]["data"] == "Nat : Type"
    assert min(walls) < 0.09, walls


def test_direct_exec_runs_the_repl_without_lake() -> None:
    app = create_app(_settings(direct_exec=True, monitor_repls=False))
    with TestClient(app, base_url="http://testserver/api") as client:
        result = _check(client, "#check Nat")
        assert result["response"]["messages"][0]["data"] == "Nat : Type"
        manager = app.state.manager
        (repl,) = list(manager._free)
        assert repl.proc is not None
        names = psutil.Process(repl.proc.pid).cmdline()
        # Upstream's process is `lake env <repl>`; here it is the REPL binary itself.
        assert os.path.basename(names[0]) == "repl", names


async def test_reserved_slots_never_exceed_max_repls() -> None:
    """With direct_exec the first start awaits `lake env`. Without counting the slots
    being started, every concurrent caller saw a free slot during that await."""
    manager = Manager(max_repls=2, max_repl_uses=-1, init_repls={}, direct_exec=True)
    results = await asyncio.gather(
        *(manager.get_repl(timeout=1) for _ in range(5)), return_exceptions=True
    )
    got = [r for r in results if not isinstance(r, BaseException)]
    assert len(got) == 2
    assert len(manager._busy) == 2
    await manager.cleanup()


async def test_prewarm_replaces_a_destroyed_repl() -> None:
    header = "import Init"
    manager = Manager(
        max_repls=1,
        max_repl_uses=-1,
        init_repls={header: 1},
        prewarm=True,
        header_timeout=120,
    )
    await manager.initialize_repls()
    repl = await manager.get_repl(header=header)
    await manager.destroy_repl(repl)  # as after a timeout
    assert manager._starting == 1  # the slot is kept for the replacement

    deadline = time.monotonic() + 60
    while manager.stats["prewarm_ready"] < 1 and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert manager.stats["prewarm_ready"] == 1
    assert manager._starting == 0
    (warm,) = manager._free
    assert warm.is_running and warm.header == header

    again = await manager.get_repl(header=header)
    assert again is warm  # reused, no import on the request's path
    await manager.release_repl(again)
    await manager.cleanup()


async def test_no_prewarm_for_headers_outside_init_repls() -> None:
    manager = Manager(max_repls=1, max_repl_uses=-1, init_repls={}, prewarm=True)
    repl = await manager.get_repl(header="import Init")
    await manager.destroy_repl(repl)
    assert manager._starting == 0
    assert manager.stats["prewarm_started"] == 0
    await manager.cleanup()


def test_stats_endpoint() -> None:
    app = create_app(_settings(monitor_repls=False))
    with TestClient(app, base_url="http://testserver") as client:
        payload = CheckRequest(snippets=[Snippet(id="a", code="#check Nat")]).model_dump()
        assert client.post("/api/check", json=payload).status_code == 200
        stats = client.get("/stats").json()
    assert stats["repls_started"] == 1
    assert stats["free"] == 1 and stats["busy"] == 0


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_client_disconnect_still_cancels_the_check() -> None:
    """The REPL running a request whose client went away is killed, as upstream."""
    app = create_app(_settings(monitor_repls=False, log_snippets=False))
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_config=None))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 30
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        payload = CheckRequest(
            snippets=[Snippet(id="slow", code="#eval IO.sleep 8000")], timeout=60
        ).model_dump()
        with pytest.raises(httpx.ReadTimeout):
            httpx.post(f"{base}/api/check", json=payload, timeout=3)
        deadline = time.monotonic() + 10
        stats: dict = {}  # type: ignore[type-arg]
        while time.monotonic() < deadline:
            stats = httpx.get(f"{base}/stats").json()
            if stats["repls_destroyed"] >= 1:
                break
            time.sleep(0.1)
        assert stats["repls_destroyed"] == 1, stats
        assert stats["busy"] == 0, stats
    finally:
        server.should_exit = True
        thread.join(timeout=10)


async def test_direct_exec_with_an_explicit_lean_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """lean_path/lean_sysroot replace `lake env`, e.g. for a copy of Lean in /dev/shm."""
    from server import manager as manager_module
    from server.lean_env import resolve_lake_env

    env = await resolve_lake_env(manager_module.settings.project_dir)
    monkeypatch.setattr(manager_module.settings, "lean_path", env["LEAN_PATH"])
    monkeypatch.setattr(manager_module.settings, "lean_sysroot", env["LEAN_SYSROOT"])

    async def no_lake(*_: object, **__: object) -> dict[str, str]:
        raise AssertionError("lake must not be asked")

    monkeypatch.setattr(manager_module, "resolve_lake_env", no_lake)
    manager = Manager(max_repls=1, max_repl_uses=-1, init_repls={}, direct_exec=True)
    repl = await manager.get_repl()
    await manager.prep(repl, snippet_id="t", timeout=60, debug=False)
    resp = await repl.send_timeout(Snippet(id="t", code="#check Nat"), timeout=60)
    assert resp.response is not None
    assert resp.response["messages"][0]["data"] == "Nat : Type"  # type: ignore[index]
    await manager.cleanup()


@pytest.mark.parametrize("check_header", [False, True])
def test_a_header_that_does_not_import(check_header: bool) -> None:
    """The REPL answers a failed import with `{"env": 0}` and no message. Upstream then
    checks the proof in an empty environment (a Lean error, as if the proof were wrong);
    with check_header the request gets an error under its own ID instead."""
    app = create_app(_settings(check_header=check_header, monitor_repls=False))
    with TestClient(app, base_url="http://testserver/api") as client:
        payload = CheckRequest(
            snippets=[Snippet(id="p", code="import NonExistentModuleXyz\n#check Nat")], timeout=60
        ).model_dump()
        result = client.post("check", json=payload).json()["results"][0]
        stats = app.state.manager.stats
    assert result["id"] == "p"
    if check_header:
        assert "did not import" in result["error"]
        assert stats["header_imports_failed"] == 1 and stats["repls_destroyed"] == 1
    else:
        assert result["response"]["messages"][0]["data"] == "Unknown identifier `Nat`"


def test_check_header_keeps_a_good_header() -> None:
    app = create_app(_settings(check_header=True, monitor_repls=False))
    with TestClient(app, base_url="http://testserver/api") as client:
        result = _check(client, "import Init\n#check Nat")
        again = _check(client, "import Init\n#check Nat")
        manager = app.state.manager
    assert result["response"]["messages"][0]["data"] == "Nat : Type"
    assert again["response"]["messages"][0]["data"] == "Nat : Type"
    assert manager.stats["repls_started"] == 1 and manager.stats["header_imports_failed"] == 0


async def test_a_dead_repl_is_reported_with_its_exit_code() -> None:
    from server.errors import ReplError

    # direct_exec: the process is the REPL itself (with `lake env`, killing lake leaves the
    # REPL running as its child, still answering).
    manager = Manager(max_repls=1, max_repl_uses=-1, init_repls={}, direct_exec=True)
    repl = await manager.get_repl()
    await manager.prep(repl, snippet_id="t", timeout=60, debug=False)
    await repl.send_timeout(Snippet(id="warm", code="#check Nat"), timeout=60)
    assert repl.proc is not None
    repl.proc.kill()  # as the kernel's OOM killer would
    with pytest.raises(ReplError, match="exited with code -9"):
        await repl.send_timeout(Snippet(id="t", code="#check Nat"), timeout=60)
    await manager.cleanup()


async def test_stderr_output_does_not_fail_a_command() -> None:
    manager = Manager(max_repls=1, max_repl_uses=-1, init_repls={})
    repl = await manager.get_repl()
    await manager.prep(repl, snippet_id="t", timeout=60, debug=False)
    resp = await repl.send_timeout(Snippet(id="t", code='#eval IO.eprintln "to-stderr"'), timeout=60)
    assert resp.error is None
    again = await repl.send_timeout(Snippet(id="u", code="#check Nat"), timeout=60)
    assert again.response is not None
    await manager.cleanup()


async def test_initial_repls_that_fail_do_not_stop_the_server() -> None:
    manager = Manager(
        max_repls=2,
        max_repl_uses=-1,
        init_repls={"import Init": 1, "import NonExistentModuleXyz": 1},
        check_header=True,
    )
    await manager.initialize_repls()
    assert manager.stats["init_failed"] == 1
    assert [r.header for r in manager._free] == ["import Init"]
    await manager.cleanup()


async def test_server_refuses_to_start_when_no_initial_repl_imports() -> None:
    manager = Manager(
        max_repls=2, max_repl_uses=-1, init_repls={"import NonExistentModuleXyz": 2}, check_header=True
    )
    with pytest.raises(RuntimeError, match="No initial REPL"):
        await manager.initialize_repls()
    await manager.cleanup()


async def test_no_replacement_starts_when_every_initial_repl_fails() -> None:
    manager = Manager(
        max_repls=2,
        max_repl_uses=-1,
        init_repls={"import NonExistentModuleXyz": 2},
        check_header=True,
        prewarm=True,
    )
    with pytest.raises(RuntimeError, match="No initial REPL"):
        await manager.initialize_repls()
    started = manager.stats["prewarm_started"]
    # Let any replacement finish before asserting: one still being spawned when the test's
    # event loop closes would hang the test run (the bug test_server_exits_... is about).
    deadline = time.monotonic() + 60
    while (manager._background or manager._starting) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert started == 0
    await manager.cleanup()
    assert not manager._closers


async def test_prewarm_failures_are_counted_before_the_next_replacement() -> None:
    """A replacement whose import fails used to start the next one before its failure was
    counted, so MAX_PREWARM_FAILURES was always exceeded."""
    from server.manager import MAX_PREWARM_FAILURES

    bad = "import NonExistentModuleXyz"
    manager = Manager(
        max_repls=2,
        max_repl_uses=-1,
        init_repls={"import Init": 1, bad: 1},
        check_header=True,
        prewarm=True,
    )
    await manager.initialize_repls()  # the bad header's REPL is replaced, as before
    deadline = time.monotonic() + 60
    while (manager._background or manager._starting) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert manager.stats["prewarm_started"] == MAX_PREWARM_FAILURES
    assert manager.stats["prewarm_failed"] == MAX_PREWARM_FAILURES
    assert [r.header for r in manager._free] == ["import Init"]
    await manager.cleanup()


def test_server_exits_when_its_start_up_fails() -> None:
    """With prewarm, the failed initial REPLs' replacements were still being spawned when
    uvicorn gave up. asyncio.run() then waited forever for them at exit: the server logged
    "Application startup failed" and kept running (and its REPLs with it)."""
    marker = uuid4().hex
    script = (
        "import uvicorn\n"
        "from server.main import create_app\n"
        "from server.settings import Environment, Settings\n"
        "s = Settings(_env_file=None)\n"
        "for k, v in dict(max_repls=8, max_repl_uses=-1, init_repls={'import NonExistentModuleXyz': 8},\n"
        "        prewarm=True, check_header=True, monitor_repls=False, log_snippets=False,\n"
        "        database_url=None, environment=Environment.prod).items():\n"
        "    setattr(s, k, v)\n"
        f"uvicorn.run(create_app(s), host='127.0.0.1', port={_free_port()}, log_config=None)\n"
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
        "KIMINA_TEST_MARKER": marker,  # inherited by the REPLs
    }
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        out, _ = proc.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
        pytest.fail("the server was still running 60 s after its start-up failed:\n" + out[-3000:])
    assert "Application startup failed" in out, out[-3000:]
    assert proc.returncode != 0

    left = []
    for p in psutil.process_iter():
        try:
            if p.environ().get("KIMINA_TEST_MARKER") == marker:
                left.append(p.cmdline())
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess, OSError):
            pass
    assert not left, left
