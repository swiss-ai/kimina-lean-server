"""The environment `lake env` gives a process, resolved once for Settings.direct_exec."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

_DUMP_ENV = "import json, os, sys; sys.stdout.write(json.dumps(dict(os.environ)))"


async def resolve_lake_env(project_dir: Path, timeout: float = 300.0) -> dict[str, str]:
    """Run `lake env <python> -c <dump>` in project_dir and return that environment.

    Lake adds LEAN_PATH (the project's and its packages' build directories), LEAN_SYSROOT,
    the toolchain's PATH and library path, and its own variables. Starting the REPL with
    exactly this environment is what `lake env repl` does, without one lake process per REPL.
    """
    proc = await asyncio.create_subprocess_exec(
        "lake",
        "env",
        sys.executable,
        "-c",
        _DUMP_ENV,
        cwd=project_dir,
        env=dict(os.environ),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"`lake env` in {project_dir} timed out after {timeout}s")
    if proc.returncode != 0:
        raise RuntimeError(
            f"`lake env` in {project_dir} failed ({proc.returncode}): "
            f"{err.decode(errors='replace').strip()}"
        )
    env = json.loads(out)
    if "LEAN_PATH" not in env:
        raise RuntimeError(f"`lake env` in {project_dir} set no LEAN_PATH")
    return {str(k): str(v) for k, v in env.items()}
