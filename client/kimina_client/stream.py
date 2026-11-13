"""Check each snippet in its own request, as soon as it is available.

`KiminaClient.check` cuts a list into batches of `batch_size` (8) and keeps `max_workers`
batches in flight. A batch's response waits for its slowest snippet, and its worker sends
no new batch until then: behind one slow proof, the batch's seven other slots stay empty.
So with 40 workers x 8 the server can have far fewer proofs in flight than REPLs, and none
before the whole list exists.

`StreamingChecker` sends one snippet per request, keeps at most `max_inflight` requests
open (set it to the server's MAX_REPLS) over one pooled set of connections, and resolves
each snippet's future when its own check finishes. Results are keyed by snippet ID, so
out-of-order completion is fine. A request the server refuses with 429/5xx, or that fails
on the network, is retried for that snippet only; after the last retry the snippet gets
an error starting with `UNCHECKED_PREFIX`, which `ReplResponse.analyze()` reports as
`server_error`, so callers can tell "never checked" from "Lean rejected it".

    async with StreamingChecker(max_inflight=128, timeout=60) as checker:
        futures = [checker.submit(s) for s in snippets_as_they_finish()]
        results = await asyncio.gather(*futures)

    results = check_streaming(snippets, max_inflight=128)  # synchronous wrapper
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Iterable
from typing import Any

import httpx

from .base import BaseKimina
from .models import CheckRequest, CheckResponse, Infotree, ReplResponse, Snippet

logger = logging.getLogger("kimina-client")

UNCHECKED_PREFIX = "unchecked:"
RETRY_STATUSES = frozenset({408, 429, 499, 500, 502, 503, 504})


class StreamingChecker(BaseKimina):
    def __init__(
        self,
        api_url: str | None = None,
        api_key: str | None = None,
        *,
        max_inflight: int = 64,
        timeout: int = 60,
        debug: bool = False,
        reuse: bool = True,
        infotree: Infotree | None = None,
        http_timeout: float | None = None,
        n_retries: int = 3,
        headers: dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if max_inflight < 1:
            raise ValueError("max_inflight must be at least 1")
        # The server may queue a request for a free REPL (LEAN_SERVER_MAX_WAIT, 60 s by
        # default) and import its header before the check's own timeout starts.
        if http_timeout is None:
            http_timeout = 2 * timeout + 180
        super().__init__(
            api_url=api_url,
            api_key=api_key,
            headers=headers,
            http_timeout=int(http_timeout),
            n_retries=n_retries,
        )
        self.max_inflight = max_inflight
        self.timeout = timeout
        self.debug = debug
        self.reuse = reuse
        self.infotree = infotree
        self._transport = transport
        self._sem: asyncio.Semaphore | None = None
        self._session: httpx.AsyncClient | None = None
        self.stats = {"requests": 0, "retries": 0, "unchecked": 0}

    async def __aenter__(self) -> "StreamingChecker":
        self._sem = asyncio.Semaphore(self.max_inflight)
        self._session = httpx.AsyncClient(
            headers=self.headers,
            timeout=httpx.Timeout(self.http_timeout, connect=30.0),
            limits=httpx.Limits(
                max_connections=self.max_inflight,
                max_keepalive_connections=self.max_inflight,
            ),
            transport=self._transport,
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session is not None:
            await self._session.aclose()
            self._session = None

    def submit(self, snippet: str | Snippet) -> "asyncio.Task[ReplResponse]":
        """Start checking one snippet now; the task resolves with its own result."""
        return asyncio.ensure_future(self.check_one(Snippet.from_snip(snippet)))

    async def check(self, snippets: Iterable[str | Snippet]) -> CheckResponse:
        """Check all snippets, each as its own request; results in input order."""
        tasks = [self.submit(s) for s in snippets]
        return CheckResponse(results=list(await asyncio.gather(*tasks)))

    async def check_one(self, snippet: Snippet) -> ReplResponse:
        if self._sem is None or self._session is None:
            raise RuntimeError("use `async with StreamingChecker(...) as checker:`")
        payload = CheckRequest(
            snippets=[snippet],
            timeout=self.timeout,
            debug=self.debug,
            reuse=self.reuse,
            infotree=self.infotree,
        ).model_dump()
        url = self.build_url("/api/check")
        last_error = ""
        async with self._sem:
            for attempt in range(self.n_retries):
                if attempt:
                    self.stats["retries"] += 1
                    await asyncio.sleep(min(10.0, 2.0**attempt) * (0.5 + random.random()))
                self.stats["requests"] += 1
                try:
                    resp = await self._session.post(url, json=payload)
                except httpx.HTTPError as e:
                    last_error = f"{type(e).__name__}: {e}"
                    continue
                if resp.status_code in RETRY_STATUSES:
                    last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    continue
                if resp.status_code != 200:
                    # A request the server rejects for what it is (401, 422) fails the
                    # same way when repeated.
                    last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    break
                results = CheckResponse.model_validate(resp.json()).results
                if len(results) != 1 or results[0].id != snippet.id:
                    last_error = "response does not match the request's snippet ID"
                    break
                return results[0]
        self.stats["unchecked"] += 1
        logger.error("Snippet %s was not checked: %s", snippet.id, last_error)
        return ReplResponse(id=snippet.id, error=f"{UNCHECKED_PREFIX} {last_error}")


def check_streaming(
    snippets: Iterable[str | Snippet],
    api_url: str | None = None,
    api_key: str | None = None,
    **kwargs: Any,
) -> CheckResponse:
    """Synchronous `StreamingChecker.check`, for callers without an event loop (for
    example a verl reward function)."""

    async def run() -> CheckResponse:
        async with StreamingChecker(api_url=api_url, api_key=api_key, **kwargs) as c:
            return await c.check(snippets)

    return asyncio.run(run())
