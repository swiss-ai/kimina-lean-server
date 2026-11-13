"""StreamingChecker against an in-process fake server (no Lean needed)."""

import asyncio
import json

import httpx
import pytest
from kimina_client import (
    UNCHECKED_PREFIX,
    SnippetStatus,
    StreamingChecker,
    check_streaming,
)
from kimina_client.models import Snippet


class FakeServer:
    """Answers /api/check after a per-snippet delay; records concurrency and requests."""

    def __init__(self, delays: dict[str, float], fail_first: dict[str, int] | None = None):
        self.delays = delays
        self.fail_first = dict(fail_first or {})
        self.inflight = 0
        self.max_inflight = 0
        self.requests: list[str] = []

    async def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        (snippet,) = body["snippets"]
        sid = snippet["id"]
        self.requests.append(sid)
        if self.fail_first.get(sid, 0) > 0:
            self.fail_first[sid] -= 1
            return httpx.Response(429, json={"detail": "No available REPLs"})
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            await asyncio.sleep(self.delays.get(sid, 0.0))
        finally:
            self.inflight -= 1
        return httpx.Response(
            200, json={"results": [{"id": sid, "time": 0.01, "response": {"env": 0}}]}
        )


def _checker(server: FakeServer, **kw: object) -> StreamingChecker:
    return StreamingChecker(
        api_url="http://fake",
        transport=httpx.MockTransport(server.handler),
        **kw,  # type: ignore[arg-type]
    )


async def test_results_keyed_by_id_and_in_input_order() -> None:
    delays = {"slow": 0.3, "a": 0.0, "b": 0.05}
    server = FakeServer(delays)
    async with _checker(server, max_inflight=8) as c:
        out = await c.check([Snippet(id=i, code="x") for i in ["slow", "a", "b"]])
    assert [r.id for r in out.results] == ["slow", "a", "b"]
    assert all(r.analyze().status == SnippetStatus.valid for r in out.results)


async def test_a_slow_proof_does_not_delay_the_others() -> None:
    server = FakeServer({"slow": 0.5})
    async with _checker(server, max_inflight=8) as c:
        slow = c.submit(Snippet(id="slow", code="x"))
        fast = c.submit(Snippet(id="fast", code="x"))
        done, _ = await asyncio.wait({slow, fast}, return_when=asyncio.FIRST_COMPLETED)
        assert done == {fast}
        await slow


async def test_max_inflight_is_respected() -> None:
    server = FakeServer({f"s{i}": 0.02 for i in range(40)})
    async with _checker(server, max_inflight=5) as c:
        await c.check([Snippet(id=f"s{i}", code="x") for i in range(40)])
    assert server.max_inflight == 5


async def test_retry_only_the_refused_snippet(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_sleep(_: float) -> None:
        return None

    server = FakeServer({}, fail_first={"b": 1})
    async with _checker(server, max_inflight=4, n_retries=3) as c:
        monkeypatch.setattr("kimina_client.stream.asyncio.sleep", no_sleep)
        out = await c.check([Snippet(id=i, code="x") for i in "abc"])
    assert sorted(server.requests) == ["a", "b", "b", "c"]
    assert all(r.error is None for r in out.results)
    assert c.stats["retries"] == 1 and c.stats["unchecked"] == 0


async def test_unchecked_after_last_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_sleep(_: float) -> None:
        return None

    server = FakeServer({}, fail_first={"b": 99})
    async with _checker(server, n_retries=2) as c:
        monkeypatch.setattr("kimina_client.stream.asyncio.sleep", no_sleep)
        out = await c.check([Snippet(id="b", code="x")])
    (r,) = out.results
    assert r.error is not None and r.error.startswith(UNCHECKED_PREFIX)
    assert r.analyze().status == SnippetStatus.server_error
    assert c.stats["unchecked"] == 1


def test_sync_wrapper() -> None:
    server = FakeServer({})
    out = check_streaming(
        ["#check Nat", Snippet(id="x", code="y")],
        api_url="http://fake",
        transport=httpx.MockTransport(server.handler),
    )
    assert len(out.results) == 2 and out.results[1].id == "x"
