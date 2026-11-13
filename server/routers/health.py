from typing import Any

from fastapi import APIRouter, Request

router = APIRouter()


# TODO: add stats endpoint in webapp typescript


@router.get("/health")
@router.get("/health/", include_in_schema=False)
@router.get("/", include_in_schema=False)
async def get_health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/stats")
async def get_stats(request: Request) -> dict[str, Any]:
    """REPL pool state and counters: REPLs started, reused, destroyed, header imports and
    their total seconds, waits for a free REPL and their total seconds, pre-warming."""
    return request.app.state.manager.snapshot()  # type: ignore[no-any-return]
