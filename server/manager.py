from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from time import monotonic, time
from typing import Any

from kimina_client import ReplResponse, Snippet
from loguru import logger

from .errors import NoAvailableReplError, ReplError
from .lean_env import resolve_lake_env
from .repl import Repl, close_verbose
from .settings import settings
from .utils import is_blank

# A header whose warm replacement fails this many times in a row is no longer replaced,
# so a broken header (or a broken toolchain) cannot keep the server importing forever.
MAX_PREWARM_FAILURES = 3


class Manager:
    def __init__(
        self,
        *,
        max_repls: int = settings.max_repls,
        max_repl_uses: int = settings.max_repl_uses,
        max_repl_mem: int = settings.max_repl_mem,
        init_repls: dict[str, int] = settings.init_repls,
        monitor_repls: bool = settings.monitor_repls,
        log_snippets: bool = settings.log_snippets,
        direct_exec: bool = settings.direct_exec,
        header_timeout: float | None = settings.header_timeout,
        prewarm: bool = settings.prewarm,
        check_header: bool = settings.check_header,
    ) -> None:
        self.max_repls = max_repls
        self.max_repl_uses = max_repl_uses
        self.max_repl_mem = max_repl_mem
        self.init_repls = init_repls
        self.monitor_repls = monitor_repls
        self.log_snippets = log_snippets
        self.direct_exec = direct_exec
        self.header_timeout = header_timeout
        self.prewarm = prewarm
        self.check_header = check_header

        self._lock: asyncio.Lock | None = None
        self._cond: asyncio.Condition | None = None
        self._free: list[Repl] = []
        self._busy: set[Repl] = set()
        # Slots handed out but whose REPL is not created yet. Counted with free and busy,
        # so no await between the decision to start a REPL and its creation can let more
        # than max_repls REPLs exist.
        self._starting = 0
        self._exec_env: dict[str, str] | None = None
        self._exec_env_lock: asyncio.Lock | None = None
        self._background: set[asyncio.Task[None]] = set()
        self._prewarm_failures: dict[str, int] = {}

        self.stats: dict[str, float] = {
            "repls_started": 0,
            "repls_reused": 0,
            "repls_destroyed": 0,
            "repls_evicted": 0,
            "header_imports": 0,
            "header_imports_failed": 0,
            "header_import_seconds": 0.0,
            "waits": 0,
            "wait_seconds": 0.0,
            "prewarm_started": 0,
            "prewarm_ready": 0,
            "prewarm_failed": 0,
        }

        logger.info(
            "REPL manager initialized with: MAX_REPLS={}, MAX_REPL_USES={}, MAX_REPL_MEM={} MB, "
            "MONITOR={}, LOG_SNIPPETS={}, DIRECT_EXEC={}, HEADER_TIMEOUT={}, PREWARM={}",
            max_repls,
            max_repl_uses,
            max_repl_mem,
            monitor_repls,
            log_snippets,
            direct_exec,
            header_timeout,
            prewarm,
        )

    def _ensure_lock(self) -> None:
        """Ensure the lock and condition are initialized in an async context."""
        if self._lock is None:
            self._lock = asyncio.Lock()
            self._cond = asyncio.Condition(self._lock)
            self._exec_env_lock = asyncio.Lock()

    async def _get_exec_env(self) -> dict[str, str] | None:
        if not self.direct_exec:
            return None
        self._ensure_lock()
        assert self._exec_env_lock is not None
        async with self._exec_env_lock:
            if self._exec_env is None and settings.lean_path:
                env = dict(os.environ)
                env["LEAN_PATH"] = settings.lean_path
                if settings.lean_sysroot:
                    env["LEAN_SYSROOT"] = settings.lean_sysroot
                self._exec_env = env
                logger.info("REPLs start without lake, with the configured LEAN_PATH")
            elif self._exec_env is None:
                start = monotonic()
                self._exec_env = await resolve_lake_env(settings.project_dir)
                logger.info(
                    "Resolved `lake env` once in {:.2f}s; REPLs start without lake",
                    monotonic() - start,
                )
            return self._exec_env

    def snapshot(self) -> dict[str, Any]:
        """Pool state and counters, for GET /stats."""
        by_header: dict[str, int] = {}
        for r in self._free:
            key = r.header.splitlines()[0] if r.header else ""
            by_header[key] = by_header.get(key, 0) + 1
        return {
            "max_repls": self.max_repls,
            "free": len(self._free),
            "busy": len(self._busy),
            "starting": self._starting,
            "free_by_header_first_line": by_header,
            **self.stats,
        }

    async def initialize_repls(self) -> None:
        if self.direct_exec:
            await self._get_exec_env()
        if len(self.init_repls) == 0:
            return
        if self.max_repls < sum(self.init_repls.values()):
            raise ValueError(
                f"Cannot initialize REPLs: Σ (INIT_REPLS values) = {sum(self.init_repls.values())} > {self.max_repls} = MAX_REPLS"
            )
        initialized_repls: list[Repl] = []
        for header, count in self.init_repls.items():
            for _ in range(count):
                initialized_repls.append(await self.get_repl(header=header))

        async def _prep_and_release(repl: Repl) -> None:
            # All initialized imports should finish in 60 seconds (header_timeout if set).
            resp = await self.prep(repl, snippet_id="init", timeout=60, debug=False)
            if resp is not None and resp.error:
                raise ReplError(resp.error)  # prep() has destroyed it already
            await self.release_repl(repl)

        # One REPL that fails to import (killed for memory, a crash) no longer stops the
        # server, as it did upstream; it is discarded (and replaced when prewarm is on).
        # The server still refuses to start if no REPL at all could import its header.
        results = await asyncio.gather(
            *(_prep_and_release(r) for r in initialized_repls), return_exceptions=True
        )
        failed = [
            (r, e) for r, e in zip(initialized_repls, results) if isinstance(e, BaseException)
        ]
        for repl, error in failed:
            logger.error("Initial REPL {} failed: {}", repl.uuid.hex[:8], error)
            if repl in self._busy or repl in self._free:
                await self.destroy_repl(repl)
        if failed and len(failed) == len(initialized_repls):
            raise RuntimeError(f"No initial REPL could import its header: {failed[0][1]}")
        self.stats["init_failed"] = len(failed)

        logger.info(f"Initialized REPLs with: {json.dumps(self.init_repls, indent=2)}")

    async def get_repl(
        self,
        header: str = "",
        snippet_id: str = "",
        timeout: float = settings.max_wait,
        reuse: bool = True,
    ) -> Repl:
        """
        Async-safe way to get a `Repl` instance for a given header.
        Waits up to `timeout` seconds for a free slot, then raises NoAvailableReplError.
        """
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        deadline = time() + timeout
        wait_started: float | None = None
        repl_to_destroy: Repl | None = None
        while True:
            async with self._cond:
                if self.log_snippets:
                    logger.info(
                        f"# Free = {len(self._free)} | # Busy = {len(self._busy)} | # Max = {self.max_repls}"
                    )
                if reuse:
                    for i, r in enumerate(self._free):
                        if (
                            r.header == header
                        ):  # repl shouldn't be exhausted (max uses to check)
                            repl = self._free.pop(i)
                            self._busy.add(repl)
                            self.stats["repls_reused"] += 1
                            self._count_wait(wait_started)
                            if self.log_snippets:
                                logger.info(
                                    f"\\[{repl.uuid.hex[:8]}] Reusing ({'started' if repl.is_running else 'non-started'}) REPL for {snippet_id}"
                                )
                            return repl
                total = len(self._free) + len(self._busy) + self._starting
                if total < self.max_repls:
                    self._starting += 1
                    break

                if self._free:
                    oldest = min(
                        self._free, key=lambda r: r.last_check_at
                    )  # Use the one that's been around the longest
                    self._free.remove(oldest)
                    repl_to_destroy = oldest
                    self.stats["repls_evicted"] += 1
                    self._starting += 1
                    break

                remaining = deadline - time()
                if remaining <= 0:
                    self._count_wait(wait_started)
                    raise NoAvailableReplError(f"Timed out after {timeout}s")

                if wait_started is None:
                    wait_started = monotonic()
                try:
                    logger.info(
                        f"Waiting for a REPL to become available (timeout in {remaining:.2f}s)"
                    )
                    # Wait for a REPL to be released
                    await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    self._count_wait(wait_started)
                    raise NoAvailableReplError(
                        f"Timed out after {timeout}s while waiting for a REPL"
                    ) from None

        self._count_wait(wait_started)
        if repl_to_destroy is not None:
            asyncio.create_task(close_verbose(repl_to_destroy))

        return await self._start_reserved(header)

    def _count_wait(self, wait_started: float | None) -> None:
        if wait_started is not None:
            self.stats["waits"] += 1
            self.stats["wait_seconds"] += monotonic() - wait_started

    async def _start_reserved(self, header: str) -> Repl:
        """Create the REPL for a slot reserved with `_starting += 1` and mark it busy."""
        try:
            exec_env = await self._get_exec_env()
            repl = await Repl.create(
                header,
                max_repl_uses=self.max_repl_uses,
                max_repl_mem=self.max_repl_mem,
                monitor=self.monitor_repls,
                log_snippets=self.log_snippets,
                exec_env=exec_env,
            )
        except BaseException:
            assert self._cond is not None
            async with self._cond:
                self._starting -= 1
                self._cond.notify(1)
            raise
        # No await between these lines: the slot moves from starting to busy at once.
        self._starting -= 1
        self._busy.add(repl)
        self.stats["repls_started"] += 1
        return repl

    def _reserve_replacement(self, header: str) -> bool:
        """Under the lock: keep the slot of a removed REPL of a pre-warmed header for a
        warm replacement. Returns False (the caller then frees the slot) otherwise."""
        if not self.prewarm or self.init_repls.get(header, 0) <= 0:
            return False
        if self._prewarm_failures.get(header, 0) >= MAX_PREWARM_FAILURES:
            return False
        self._starting += 1
        self.stats["prewarm_started"] += 1
        task = asyncio.create_task(self._warm_replacement(header))
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return True

    async def _warm_replacement(self, header: str) -> None:
        try:
            repl = await self._start_reserved(header)
        except Exception:
            logger.exception("Could not create a replacement REPL")
            self._prewarm_failed(header)
            return
        try:
            resp = await self.prep(repl, snippet_id="prewarm", timeout=600, debug=False)
        except Exception:
            logger.exception("Warming a replacement REPL failed")
            self._prewarm_failed(header)
            await self.destroy_repl(repl)
            return
        if resp is not None and resp.error:
            # prep() already destroyed it.
            self._prewarm_failed(header)
            return
        self._prewarm_failures[header] = 0
        self.stats["prewarm_ready"] += 1
        await self.release_repl(repl)

    def _prewarm_failed(self, header: str) -> None:
        self.stats["prewarm_failed"] += 1
        self._prewarm_failures[header] = self._prewarm_failures.get(header, 0) + 1

    async def destroy_repl(self, repl: Repl) -> None:
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        async with self._cond:
            was_held = repl in self._busy or repl in self._free
            self._busy.discard(repl)
            if repl in self._free:
                self._free.remove(repl)
            asyncio.create_task(close_verbose(repl))
            self.stats["repls_destroyed"] += 1
            if not (was_held and self._reserve_replacement(repl.header)):
                self._cond.notify(1)

    async def release_repl(self, repl: Repl) -> None:
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        async with self._cond:
            if repl not in self._busy:
                logger.error(
                    f"Attempted to release a REPL that is not busy: {repl.uuid.hex[:8]}"
                )
                return

            if repl.exhausted:
                uuid = repl.uuid
                logger.info(f"REPL {uuid.hex[:8]} is exhausted, closing it")
                self._busy.discard(repl)

                asyncio.create_task(close_verbose(repl))
                if not self._reserve_replacement(repl.header):
                    self._cond.notify(1)
                return
            self._busy.remove(repl)
            self._free.append(repl)
            repl.last_check_at = datetime.now()
            if self.log_snippets:
                logger.info(f"\\[{repl.uuid.hex[:8]}] Released!")
            self._cond.notify(1)

    async def start_new(self, header: str) -> Repl:
        """Start a REPL outside get_repl's accounting (kept for API compatibility)."""
        self._ensure_lock()
        self._starting += 1
        return await self._start_reserved(header)

    async def cleanup(self) -> None:
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        for task in list(self._background):
            task.cancel()
        async with self._cond:
            logger.info("Cleaning up REPL manager...")
            for repl in self._free:
                asyncio.create_task(close_verbose(repl))
            self._free.clear()

            for repl in self._busy:
                asyncio.create_task(close_verbose(repl))
            self._busy.clear()

            logger.info("REPL manager cleaned up!")
        pass

    async def _header_failure(self, repl: Repl, snippet_id: str) -> str | None:
        """Elaborate `#check Nat` in the header's environment: it fails only if the import
        left the environment empty. Returns the error to report, or None. The probe does not
        count as a use of the REPL."""
        try:
            probe = await repl.send_timeout(
                Snippet(id=f"{snippet_id}-header-check", code="#check Nat"), timeout=60
            )
        except Exception as e:
            return f"Header check failed: {e}"
        finally:
            repl.use_count -= 1
        response = probe.response or {}
        if "message" in response or any(
            m.get("severity") == "error" for m in response.get("messages", [])  # type: ignore[union-attr]
        ):
            return (
                "Lean REPL header did not import: its environment is empty "
                "(missing or unreadable .olean/.ir files, or an unknown module)"
            )
        return None

    async def prep(
        self, repl: Repl, snippet_id: str, timeout: float, debug: bool
    ) -> ReplResponse | None:
        if repl.is_running:
            return None

        try:
            await repl.start()
        except Exception as e:
            logger.exception("Failed to start REPL: %s", e)
            raise ReplError("Failed to start REPL") from e

        if not is_blank(repl.header):
            if self.header_timeout is not None:
                timeout = self.header_timeout
            start = monotonic()
            try:
                cmd_response = await repl.send_timeout(
                    Snippet(id=f"{snippet_id}-header", code=repl.header),
                    timeout=timeout,
                    is_header=True,
                )
            except TimeoutError as e:
                logger.error("Header command timed out")
                raise e
            except Exception as e:
                logger.error("Failed to run header on REPL")
                raise ReplError("Failed to run header on REPL") from e
            finally:
                self.stats["header_imports"] += 1
                self.stats["header_import_seconds"] += monotonic() - start

            if self.check_header and not cmd_response.error:
                failure = await self._header_failure(repl, snippet_id)
                if failure:
                    cmd_response = ReplResponse(id=cmd_response.id, error=failure)

            if not debug:
                cmd_response.diagnostics = None

            if cmd_response.error:
                self.stats["header_imports_failed"] += 1
                logger.error(f"Header command failed: {cmd_response.error}")
                await self.destroy_repl(repl)

            repl.header_cmd_response = cmd_response

            return cmd_response
        return repl.header_cmd_response
