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

#### What “cached+value-log–centric” means (required invariants)

To avoid ambiguity, we treat this as a small set of invariants that must hold
for “fast ingest”:

- **Engine mode**: cached mode (`ModeCached`).
- **Value store**: out-of-line values are written to the **value log**, and the
  backend index stores `ValuePtr` references to those value-log records (not to
  backend slab-direct append results).
- **Redo/journal policy**: may be enabled or disabled, but must not silently
  force a different value store.

Important current-code note:

- Cached mode currently computes `disableValueLog := opts.DisableValueLog || opts.DisableWAL`.
  This means `DisableWAL=true` implicitly disables value-log pointers today.

So, “fast ingest” must be expressible as:

- “redo/journal off” **while** “value log on” (so we do not fall back to a
  different value storage path).

#### Canonical knob (reduce sources of error)

Implement one canonical way to request this intent, and ensure all first-party
harnesses use it by default.

Acceptable approaches for PR11 (pick one; standardize on it everywhere):

1) **New profile name** (preferred):
   - Add `ProfileFastIngest` (or similarly explicit) whose intent is:
     - cached mode + value log enabled + relaxed durability (journal policy may be “off”).

2) **New explicit option**:
   - Add `Options.DisableJournal` / `DisableRedoLog` that is independent of
     `DisableValueLog`.
   - Keep `DisableValueLog` as “force legacy/no pointers”.
   - Treat `DisableWAL` as legacy/compat and migrate code to the new names.

The core requirement is not naming: it is that **one knob** expresses “cached +
value log”, and that toggling “journal off” does not implicitly toggle “value
log off”.

#### Harness wiring (how we ensure behavior everywhere)

For each harness we control, do two things:

1) **Default to the canonical knob** (so “fast” means the same thing everywhere).
2) **Print the write-path summary to stderr** (Stage 1.A) so the effective path
   is always visible.

Concrete checklist:

- `iavl-bench`:
  - Keep `TREEDB_BENCH_MODE` defaulted to `cached` (already done).
  - Keep value log enabled by default (already done via `TREEDB_BENCH_DISABLE_VALUE_LOG=0`).
  - Ensure the TreeDB open path prints the write-path summary to stderr.

- `cmd/unified_bench`:
  - Add a profile (or a single documented flag bundle) whose default for TreeDB
    is cached+value-log–centric.
  - Ensure the existing `-profile fast` does not accidentally select a
    different value store for TreeDB due to a single flag (this is why “fast ingest”
    should have its own explicit knob).

Success criteria for Stage 1.B:

- Running “fast” benches in this repo yields stderr output indicating
  `mode=cached` and `value_store=value_log`.
- There is no “obvious” first-party “fast” harness invocation that ends up on
  backend slab-direct writes.

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

## Stage 2 (PR12): Enable the cached-mode 1/2/3 matrix (decouple journal vs value-log)

We treat TreeDB as pre-alpha: **no backward compatibility guarantees**. Use that freedom to simplify.

Goal: make the following cached-mode comparisons possible and unambiguous:

1) journal/WAL **off** + values go to backend flush (legacy slab-ish path)
2) journal/WAL **off** + values go to the **new value log** (pointer path)
3) journal/WAL **on** + values go to the **new value log** (pointer path)

Current constraint (must be fixed in PR12):

- `DisableWAL=true` currently implies value-log pointers are disabled in cached
  mode, so (2) is not expressible.

### D) Decouple redo/journal from value-log pointers

Implementation requirements:

- Add a new option (name TBD but recommended):
  - `Options.DisableJournal` (or `DisableRedoLog`)
  - Semantics: “do not write redo/journal records; crash loses writes since the
    last checkpoint” **without** disabling value-log pointers/value store.
- Keep `Options.DisableValueLog` meaning:
  - “force legacy WAL framing / no value-log pointers”
- Keep `Options.DisableWAL` as legacy “everything off” for now:
  - `DisableWAL=true` still implies both journal and value-log pointers off
    (case 1).

After PR12, the matrix should be achievable with stable flags:

- (1) `DisableWAL=true`
- (2) `DisableJournal=true`, `DisableValueLog=false`, `SplitValueLog=true`, `MemtableValueLogPointers=true`
- (3) `DisableJournal=false`, `DisableValueLog=false`, `SplitValueLog=true`, `MemtableValueLogPointers=true`

### Stage 2 bench recipe (unified_bench)

Use this when validating that the matrix is actually selectable (and that “value_store” matches intent).

- Build once: `go build -o /tmp/unified_bench ./cmd/unified_bench`
- Hygiene: `RUNS=5 KEEP=3 SLEEP_S=5` (sleep between runs; keep the middle 3)
- Workload: `-suite lanes_probe -dbs treedb -keys 1000000 -valsize 1024 -batchsize 1000`

Cases:

- (1) `DisableWAL=true` (legacy “everything off”; values not persisted via value log)
  - `/tmp/unified_bench ... -treedb-disable-wal -treedb-allow-unsafe`
  - Expected stderr: `mode=cached value_store=backend_flush redo_log=off`
- (2) `DisableJournal=true` but value log enabled
  - `/tmp/unified_bench ... -treedb-disable-journal -treedb-split-value-log -treedb-memtable-value-log-pointers -treedb-allow-unsafe`
  - Expected stderr: `mode=cached value_store=value_log redo_log=off`
- (3) Journal on + value log enabled
  - `/tmp/unified_bench ... -treedb-split-value-log -treedb-memtable-value-log-pointers -treedb-allow-unsafe`
  - Expected stderr: `mode=cached value_store=value_log redo_log=on`

Bench harness requirement:

- stderr write-path summary must reflect the new state:
  - `value_store=value_log redo_log=off` must be observable for case (2).

Exit criteria for PR12:

- unified_bench can run (1)/(2)/(3) with no ambiguity.
- stderr output clearly identifies which case ran.

---

## Stage 3 (PR13, gated): Remove slab-direct writer; unify on value log

PR13 is gated on (2) demonstrating the expected speedup over (1). If (2) is not
a win, do not delete the slab-direct path yet.

### E) Unify the value store implementation (deletion)

Goal: there is exactly one way values are durably written to disk for the out-of-line path.

High-level outcomes:

- Remove backend-only slab-direct append as a write mechanism.
- Values are appended through the value-log writer; “slabs” become:
  - a representation of value-log segments, or
  - an internal implementation detail rather than a distinct write path.

Notes:

- If dropping on-disk backward compatibility reduces complexity (readers, migration, legacy replay), do so.
- Prefer simplifying invariants and state machines over keeping old formats.

Exit criteria for PR13:

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

- **PR12 / Stage 2 (enable 1/2/3 matrix)**
  - Branch: `sprint/slabopt-pr12-disable-journal-keep-valuelog`
  - PR title: `PR12: decouple journal off from value-log (enable 1/2/3 bench matrix)`

- **PR13 / Stage 3 (gated deletion/unification)**
  - Branch: `sprint/slabopt-pr13-delete-slab-direct`
  - PR title: `PR13: delete slab-direct writer; unify on value-log (no legacy support)`
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
