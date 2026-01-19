# Write-Path Unification (Cached+ValueLog as the Only Write Path)

This file is an execution spec + runbook for eliminating “equivalent but slower” TreeDB write paths.

**Motivation**

- We observed a large real-world bench difference by switching `iavl-bench/2_run_fast.sh` from backend-only mode to cached mode.
- Having two “valid” write paths (backend/slab-direct vs cached/value-log–centric) is a persistent foot-gun: future users (and future us) will accidentally benchmark or deploy the slow path.
- Goal: make the **cached + value-log–centric** path the default and eventually the only supported write path.

Non-goal: “fail fast” enforcement. We prefer visibility + defaults over rejection (until deletion).

---

## Definitions (precise; avoid ambiguous “WAL”)

- **Cached mode**: `treedb.ModeCached` (memtables + background flush/checkpoint orchestration).
- **Backend-only mode**: `treedb.ModeBackend` (direct backend engine; no caching wrapper).
- **Value log**: append-only value store used for out-of-line values referenced by `ValuePtr`.
- **Slab**: on-disk value file format (may become an internal representation of frozen value-log segments).
- **Redo log / journal**: commit-intent / ordering records used for replay/recovery. (Historically called “WAL”; we avoid that term when it conflates with “value store”.)

Key intent:

- “Fast ingest” should mean **cached mode** and values landing in the **value log** (not slab-direct append from the backend writer).
- Disabling redo/journal should not implicitly force the system onto a different value storage path.

---

## Stage 1 (guardrails + defaults): Make the slow path hard to pick accidentally

### A) Visibility (stderr) – no fail fast

Add a single “write path summary” line, written to **stderr**, and a structured equivalent via `Stats()`.

Requirements:

- Print is **informational** (or warning when slow path selected). Must not `panic`, return error, or change behavior.
- Must not write to stdout (bench output stability).
- Must be stable and grep-friendly:
  - `treedb write_path mode=cached value_store=value_log redo_log=off`
  - `treedb write_path mode=backend value_store=slab_direct redo_log=n/a (WARNING: legacy slow path)`

Implementation guidance:

- Prefer `log.Printf(...)` (stderr by default) or a dedicated `fmt.Fprintf(os.Stderr, ...)`.
- Always expose the same values in `Stats()`:
  - `treedb.write_path.mode`
  - `treedb.write_path.value_store`
  - `treedb.write_path.redo_log`

Scope constraint:

- Do not add “assertions” in benches. Do not introduce new “fail closed” behavior in Stage 1.

### B) Encapsulate “fast ingest” as the default everywhere we control

Objective: if someone runs “fast” or “default” scripts in this repo, they should get cached+value-log–centric behavior.

Checklist:

- `iavl-bench`: already updated `2_run_fast.sh` default `TREEDB_BENCH_MODE=cached`.
- `gomap` benches:
  - Ensure `unified_bench` profiles / examples default to cached mode for TreeDB.
  - Prefer **one knob** for “fast ingest” rather than many interacting flags.
  - Where multiple flags are unavoidable, document the single recommended “fast ingest” bundle in one place and keep other scripts sourcing it.

Principle:

- Avoid exposing multiple “equivalent” ways to request performance. Pick one canonical configuration and make it the default.

### C) Temporary safety rail: warn loudly on backend+slab-direct writes (do not reject)

Objective: keep backend-only mode available, but make it hard to accidentally benchmark/deploy as the “fast path”.

Behavior:

- If `ModeBackend` is opened and writes occur via slab-direct append, emit a stderr warning at open (and/or first write):
  - `WARNING: backend-only write path uses slab_direct writer; cached+value_log is the intended fast ingest path`
- Do not reject writes. Do not add feature-gating booleans that could brick consumers.

Exit criteria for Stage 1:

- The default “fast” bench harnesses in this repo use cached mode and the value-log–centric path.
- Anyone using backend-only write path sees a clear warning on stderr and can observe the mode via `Stats()`.

---

## Stage 2 (deletion): Remove slab-direct writer; unify on value log

We treat TreeDB as pre-alpha: **no backward compatibility guarantees**. Use that freedom to simplify.

### D) Unify the value store implementation

Goal: there is exactly one way values are durably written to disk for the out-of-line path.

High-level outcomes:

- Remove backend-only slab-direct append as a write mechanism.
- Values are appended through the value-log writer; “slabs” become:
  - a representation of value-log segments, or
  - an internal implementation detail rather than a distinct write path.

Notes:

- If dropping on-disk backward compatibility reduces complexity (readers, migration, legacy replay), do so.
- Prefer simplifying invariants and state machines over keeping old formats.

### E) Delete the legacy path

- Remove the slab-direct writer code path entirely.
- Remove (or repurpose) configuration that selects the slab-direct writer.
- Update docs/tests to match: “value log is the primary append-only value store”.

Exit criteria for Stage 2:

- There is no configuration that produces slab-direct writes.
- The stderr “slow path” warning becomes dead code and is removed.

---

## Runbook: How to execute safely (commit discipline + verification)

**Work style**

- Make a dedicated PR for Stage 1 and a dedicated PR for Stage 2.
- Do not mix stages in one branch: Stage 2 depends on Stage 1 landing first.

### Branch + PR naming (fixed)

- **PR11 / Stage 1 (guardrails + defaults)**
  - Branch: `sprint/slabopt-pr11-writepath-guardrails-defaults`
  - PR title: `PR11: write path guardrails + defaults (stderr reporting)`

- **PR12 / Stage 2 (deletion/unification)**
  - Branch: `sprint/slabopt-pr12-writepath-delete-slab-direct`
  - PR title: `PR12: delete slab-direct writer; unify on value-log (no legacy support)`
- Commit early and often; push early and often.
- Use `gh` CLI to open PRs and post bench results in PR comments.

**Verification gates (minimum)**

- `go test ./... -count=1`
- `go test ./... -race -count=1` (when feasible)
- `iavl-bench`:
  - `bash ./2_run_fast.sh && bash ./3_visualize.sh`
  - Record the reported wall time for TreeDB and memdb.

**Bench hygiene**

- Ensure stderr logging does not contaminate stdout outputs that parsers consume.
- Prefer stable scripts: `RUNS=5 KEEP=3 SLEEP_S=5` when comparing branches.

---

## Notes / Known risks (explicit)

- Stage 1 must avoid introducing “reject writes” paths: warning-only.
- Stage 2 can remove compatibility, but must keep the implementation conceptually simpler than today (no new multi-mode combinatorics).
