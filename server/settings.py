import os
import re
from enum import Enum
from pathlib import Path
from typing import cast

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    dev = "dev"
    prod = "prod"


BASE_DIR = Path(__file__).resolve().parent.parent  # Repository root directory


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    api_key: str | None = None

    environment: Environment = Environment.dev

    lean_version: str = "v4.26.0"
    repl_path: Path = BASE_DIR / "repl/.lake/build/bin/repl"
    project_dir: Path = BASE_DIR / "mathlib4"

    max_repls: int = max((os.cpu_count() or 1) - 1, 1)
    max_repl_uses: int = -1
    max_repl_mem: int = 12
    max_wait: int = 60

    init_repls: dict[str, int] = {}

    database_url: str | None = None

    # Performance options. The defaults keep the upstream behaviour.
    # Log every snippet's code and every response at INFO. Off skips the formatting
    # (and, in dev mode, the syntax highlighting) on the event loop.
    log_snippets: bool = True
    # Sample each REPL's CPU and memory with psutil once per second. Each sample walks
    # the process table, so many REPLs keep the single event-loop thread busy.
    monitor_repls: bool = True
    # Start the REPL binary directly with the environment `lake env` would set, computed
    # once at start-up, instead of one `lake env` wrapper process per REPL.
    direct_exec: bool = False
    # With direct_exec: use these instead of asking `lake env`, for example for a copy of
    # the toolchain and the compiled packages in node memory (/dev/shm). LEAN_PATH lists
    # the packages' `lib/lean` directories; LEAN_SYSROOT is the toolchain directory.
    lean_path: str | None = None
    lean_sysroot: str | None = None
    # Timeout for importing a REPL's header. Unset: the snippet's own timeout, as upstream,
    # so a slow cold import (Mathlib from a shared file system) fails the proof with it.
    header_timeout: float | None = None
    # Keep the headers of `init_repls` warm: a REPL of such a header that is destroyed
    # (timeout, error) or exhausted is replaced in the background, and requests wait for
    # the warm replacement instead of importing the header themselves.
    prewarm: bool = False
    # After importing a header, check that the import worked. A REPL whose import fails
    # (missing or broken .olean/.ir files, unknown module) answers `{"env": 0}` with no
    # message at all, and then checks every proof in an empty environment, where it fails
    # as if the proof were wrong. With this on, such a REPL is discarded and the request
    # gets an error saying the header did not import.
    check_header: bool = False

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="LEAN_SERVER_"
    )

    @field_validator("max_repl_mem", mode="before")
    def _parse_max_mem(cls, v: str) -> int:
        if isinstance(v, int):
            return cast(int, v * 1024)
        m = re.fullmatch(r"(\d+)([MmGg])", v)
        if m:
            n, unit = m.groups()
            n = int(n)
            return n if unit.lower() == "m" else n * 1024
        raise ValueError("max_repl_mem must be an int or '<number>[M|G]'")

    @field_validator("max_repls", mode="before")
    @classmethod
    def _parse_max_repls(cls, v: int | str) -> int:
        if isinstance(v, str) and v.strip() == "":
            return os.cpu_count() or 1
        return cast(int, v)


settings = Settings()
