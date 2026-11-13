from typing import Any

try:
    from prisma import Prisma  # type: ignore

    prisma: Any = Prisma()
except Exception:  # not installed, or `prisma generate` never ran

    class _NoPrisma:
        """Stands in when the Prisma client is unavailable. The server only uses the
        database when LEAN_SERVER_DATABASE_URL is set; then connect() fails loudly and
        the server runs without it, as it does when the database is unreachable."""

        async def connect(self) -> None:
            raise RuntimeError(
                "Prisma client unavailable: install `prisma` and run `prisma generate`"
            )

        async def disconnect(self) -> None:
            return None

    prisma = _NoPrisma()
