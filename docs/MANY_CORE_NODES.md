# Running the server on many-core nodes (swiss-ai fork)

This fork adds settings for running the server with tens to hundreds of REPLs on one machine, as on the
GH200 nodes of CSCS Alps (Clariden: 288 Grace cores in 4 sockets, 4 GPUs, Slurm gives a job 450 GB of host
memory). Every setting defaults to upstream behaviour. The numbers below were measured on such nodes on
2026-11-12: Slurm debug jobs 3923541 and 3923726, one node each, CPUs only, Lean v4.34.0 with Mathlib.

## Recommended settings

```bash
LEAN_SERVER_ENVIRONMENT=prod
LEAN_SERVER_MONITOR_REPLS=false        # the psutil samplers saturate the event loop (below)
LEAN_SERVER_LOG_SNIPPETS=false         # no per-proof logging on the event loop
LEAN_SERVER_MAX_REPLS=128              # ~2 GB of memory per warm REPL; 64 already gave full speed
LEAN_SERVER_INIT_REPLS='{"import Mathlib\nimport Aesop": 128}'   # the header your proofs use, exactly
LEAN_SERVER_HEADER_TIMEOUT=900         # importing a header does not count against a proof's timeout
LEAN_SERVER_PREWARM=true               # replace a REPL killed by a timeout in the background
LEAN_SERVER_CHECK_HEADER=true          # refuse a REPL whose import silently failed
LEAN_SERVER_MAX_WAIT=3600              # queue rather than answer 429 when all REPLs are busy
LEAN_SERVER_MAX_REPL_MEM=32G           # address-space limit per REPL (upstream: 12G)
LEAN_SERVER_DIRECT_EXEC=true           # start the REPL binary itself, not `lake env repl`
LEAN_SERVER_LEAN_PATH=...              # optional: a copy of the packages' build/lib/lean dirs in /dev/shm
LEAN_SERVER_LEAN_SYSROOT=...           # optional: a copy of the toolchain in /dev/shm
```

The import header in `INIT_REPLS` must be the one the server sees: the leading `import` lines of a proof, with
all `import Mathlib...` lines merged into one `import Mathlib` (`server/split.py`). A proof with another header
evicts a warm REPL and imports again.

## What was measured

Workload: 2,048 proofs, one GRPO step of 128 prompts × 16 samples.
- Three quarters were a 108B model's own failed attempts at ProofNet theorems, mostly fast parse and elaboration errors with a few slow ones.
- One quarter were the Goedel-Prover lean_workbook proofs from `tests/match` (nlinarith-heavy).
- Each proof had 60 s.

| Server | REPLs | Proofs/s | Notes |
|---|---|---|---|
| This fork, recommended settings | 64 | 28.2 | median check 0.04 s, server CPU 11% |
| This fork, recommended settings | 128 | 30.5 | |
| This fork, recommended settings | 192 | 26.6 | the few slowest proofs set the time from 64 REPLs on |
| This fork, one server per CPU socket (4 × 48) | 192 | 31.3 | small gain, within noise |
| Upstream code | 64 | ≈ 0.98 | did not finish in 15 min; server process at 98.7% CPU |
| Upstream code | 192 | ≈ 0.55 | did not finish in 7 min; 97.3% CPU |
| This fork, but samplers and per-proof logging on | 192 | ≈ 0.74 | did not finish in 7 min; 97.4% CPU |
| One cold `lean` per proof (no server) | 128 at once | 3.2 | 26 s per proof (each re-imports Mathlib) |

All finished runs gave the same verdicts.

**Why upstream stalls.** The server starts two psutil sampling tasks per REPL. Each wakes every second and walks
the whole process table, all on the server's single asyncio thread. On a 288-core node with 64 or more REPLs,
that thread was busy about 98% of the time. It stayed at 99% even when no proof was in flight (256 idle REPLs),
which points to the samplers rather than the per-proof logging (the two were not switched separately).
Responses then wait behind the samplers.

**Other measurements:**
- The upstream `/api/check` answered no sooner than 100 ms after the check (a 0.1 s disconnect poll); a warm check takes 5–50 ms. Fixed for all settings.
- Memory: a warm REPL holds about 2.0 GB of its own (2.2 GB at peak; 1.5 GB right after `import Mathlib`). 256 REPLs went over a 450 GB job limit and were OOM-killed.
- Importing Mathlib in 32 REPLs at once took 13 s with Lean in `/dev/shm`, against 149 s from a Lustre file system. A parallel copy of the toolchain and the compiled packages (8.6 GiB) took 32 s.
- With all proofs available at once, batching (`KiminaClient.check`, 8 per request) and one request per proof (`StreamingChecker`) took the same time (75 vs 77 s).
- Sending proofs as a generation finishes them, instead of after it, left 34 s of checking after the generation's end instead of 72 s.

## Copying Lean into node memory

Lean opens about 34,000 files to `import Mathlib`. Copy the toolchain and each package's `.lake/build/lib/lean`
into `/dev/shm`, then point `LEAN_SERVER_LEAN_PATH` (the packages' copies, `:`-separated) and
`LEAN_SERVER_LEAN_SYSROOT` (the toolchain's copy) at them, with `LEAN_SERVER_DIRECT_EXEC=true`.

You may skip `.ilean`, `.trace` and `*.hash` files. **Keep `.ir.sig`.** Without those files the import fails, and
the REPL reports nothing: it answers `{"env": 0}` and then checks every proof in an empty environment, where
every proof fails as if it were wrong. `LEAN_SERVER_CHECK_HEADER=true` catches this. It refuses such a REPL, and
the server refuses to start if no initial REPL imports.

## Confining REPLs

Lean runs code while it elaborates (`#eval`, `IO`). To keep model-written proofs away from the file system and
the network, start each REPL inside bubblewrap. Point `LEAN_SERVER_REPL_PATH` at a wrapper that `exec`s
`bwrap ... <repl>`; `DIRECT_EXEC` passes `LEAN_PATH`/`LEAN_SYSROOT` through.

Mount `/proc`, `/dev` and `/tmp` *before* binding a Lean copy under `/dev/shm`. A later `--dev /dev` mounts a
fresh `/dev` over the copy, and the import silently fails. This happened in a test; `CHECK_HEADER` refused every
REPL. A wrapper is in swiss-ai/rl-infra, `backends/nemo-rl/lean-kimina/repl-bwrap.sh`.

## Streaming client

`kimina_client.StreamingChecker` sends one request per proof, at most `max_inflight` at once (set it to
`MAX_REPLS`), and resolves each proof's result as soon as its own check ends. Proofs it could not get checked
(429, 5xx or network failures after its retries) come back with an error starting with `unchecked:`, so they
can be told apart from proofs Lean rejected. `check_streaming(...)` is the synchronous form.
