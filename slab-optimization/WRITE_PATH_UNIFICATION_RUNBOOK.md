> **Legacy note:** This runbook predates the WAL on/off simplification and may reference removed options.
> Use the current merge-gate runbook + docs for up-to-date guidance.

# Write-Path Unification (Cached+ValueLog as the Only Write Path)

This file is an execution spec + runbook for eliminating “equivalent but slower” TreeDB write paths.

**Motivation**

- We observed large real-world bench differences when shifting from legacy backend-only paths to cached mode.
- Having multiple “valid” write paths was a persistent foot-gun; the goal is now the **cached + value-log–centric** path only.

Non-goal: “fail fast” enforcement. We prefer visibility + defaults over rejection (until deletion).

---

## Definitions (precise; avoid ambiguous “WAL”)

- **Cached mode**: TreeDB’s default engine (memtables + background flush/checkpoint orchestration).
- **Value log**: append-only value store used for out-of-line values referenced by `ValuePtr`.
- **Redo log / journal**: commit-intent / ordering records used for replay/recovery. (Historically called “WAL”; we avoid that term when it conflates with “value store”.)

Key intent:

- “Fast ingest” should mean **cached mode** and values landing in the **value log**.
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

Current code note:

- Cached mode always uses value-log pointers.
- `-treedb-disable-wal` disables only the journal/redo log (value log remains on).

So, “fast ingest” is now simply “journal off while value log stays on”, using
`-treedb-disable-wal` (with `-treedb-allow-unsafe`).

#### Canonical knob (reduce sources of error)

Implement one canonical way to request this intent, and ensure all first-party
harnesses use it by default.

Preferred approach:

1) **Explicit WAL toggle**:
   - Use `-treedb-disable-wal` (with `-treedb-allow-unsafe`) for WAL off.
   - Value log pointers remain enabled; there is no separate “value log off” knob.

Optional:

2) **Profile name**:
   - Add/keep a `ProfileFastIngest` that bundles `-treedb-disable-wal` plus any
     relaxed durability toggles needed for benchmarking.

The core requirement is that **one knob** expresses “cached + value log”, and
that toggling “journal off” does not change the value store.

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

Principle:

- Avoid exposing multiple “equivalent” ways to request performance. Pick one canonical configuration and make it the default.

---

## Stage 2 (complete): WAL on/off only

We treat TreeDB as pre-alpha: **no backward compatibility guarantees**. Use that freedom to simplify.

Current state:

- The value log is always enabled in cached mode.
- WAL on/off is now a single toggle:
  - WAL on (default): journal enabled.
  - WAL off: `-treedb-disable-wal` (requires `-treedb-allow-unsafe`).

Legacy value-log-off paths and split-value-log flags are removed.

---

## Stage 3 (complete): Remove slab-direct writer; unify on value log

### E) Unify the value store implementation (deletion)

Goal: there is exactly one way values are durably written to disk for the out-of-line path.

High-level outcomes:

- Remove backend-only slab-direct append as a write mechanism.
- Values are appended through the value-log writer; legacy “slab” terminology is historical only.

Notes:

- If dropping on-disk backward compatibility reduces complexity (readers, migration, legacy replay), do so.
- Prefer simplifying invariants and state machines over keeping old formats.

Exit criteria for PR13:

- There is no configuration that produces slab-direct writes.

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
