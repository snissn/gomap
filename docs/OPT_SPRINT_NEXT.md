# Optimization Sprint Next (2026-01): Compression + Storage Simplification

This document defines the next optimization sprint for TreeDB with a **mergeable, milestone-based plan**.

## Goals (What We Land)

1) **Adaptive compression viability detection**
   - Skip compression quickly when data is incompressible.
   - Keep dictionary training alive at low duty cycle (during pause).
   - “Probe” occasionally to re-enable compression when it becomes worthwhile.

2) **Append-only dictionary store** (`dict.db`)
   - Small, durable, append-only file storing dictionaries and profiles.
   - Readers can fetch dictionaries by ID reliably.
   - Designed so it can be reused by *both* value compression and future key/index compression.

3) **Simpler slab compression model with arbitrary value lengths**
   - No hard 2MB “zones”.
   - No value size caps like “record must fit under ZoneSize”.
   - Optional micro-batching (K) to approach streaming compression ratios while preserving bounded point reads.

4) **Clear durability semantics with minimal concurrency**
   - Preserve the “written vs durable” contract, but do not introduce async slab writer machinery in early milestones.

5) **B-tree/index optimization groundwork**
   - Keep incremental wins small and testable (separator shortening, depth regressions, locality).
   - Defer major index redesign (range partition + columnar leaf format) to later milestones with prototypes and acceptance criteria.

## Non-Goals (Explicit Excludes)

- No “Slab V2” 2MB zone format in this sprint.
- No `SlabWriter` goroutine flush loops / shutdown choreography in early milestones.
- No new mmap usage on mutable/truncating files.
- No `ValueIndex`/`ValueID` indirection reintroduction unless explicitly approved as a separate experiment.
- No “range-partitioned index files” shipped to production without a dedicated migration story and benchmark proof.

## Guiding Principles

- **Separate compression intelligence from storage layout.**
  - Dictionary selection and training should live in `internal/` and be reusable.
  - Storage layers should consume dictionary IDs and record headers, not embed training policy.

- **Prioritize safety and measurability.**
  - Every milestone must include tests and at least one benchmark or measurement hook.
  - Favor deterministic tests (hooks/latches) over sleeps.

- **Small PRs, fast feedback.**
  - Each milestone below must be independently mergeable to `main`.
  - If a milestone is too large, split it into two.

## Milestone Plan (Mergeable PRs)

### Milestone 0 — Baselines + Metrics Plumbing

**Scope**
- Add minimal counters/telemetry for compression work:
  - raw bytes seen
  - stored bytes written
  - compression attempts / skips
  - pause/probe state
- Add a small benchmark harness (or extend an existing one) to measure:
  - compressible workload throughput + ratio
  - incompressible workload throughput (must not regress)

**Acceptance**
- Tests: unit tests for counters/rolling window math.
- Bench: a recorded baseline table in PR description (local results OK).

---

### Milestone 1 — Adaptive Compression “Pause + Probe” (No Format Change)

This is the “compression not viable, stop trying” optimization.

**Design**
- Maintain a rolling ratio window using bytes:
  - `windowRawBytes`, `windowStoredBytes`
  - observed ratio = `windowStoredBytes / windowRawBytes`
- If ratio stays above `AdaptiveBadRatio` for at least `AdaptiveWindowBytes` and `AdaptiveMinRecords`:
  - set `pauseRemainingBytes = AdaptivePauseBytes`
- While paused:
  - skip compression at near-zero overhead (atomic decrement by raw bytes).
  - still sample training data at low duty cycle (every Nth record).
  - every `AdaptiveProbeBytes`, do 1 probe compression; if it wins, clear pause.

**Config knobs (keep minimal)**
- `AdaptiveBadRatio` (float; default ~0.99)
- `AdaptiveWindowBytes` (int)
- `AdaptivePauseBytes` (int)
- `AdaptiveProbeBytes` (int)
- `AdaptiveMinRecords` (int)
- `AdaptivePauseSampleStride` (int)

**Tests**
- Deterministic regression:
  - incompressible writes trigger pause quickly
  - compressible writes during pause eventually resume compression after probe

**Acceptance**
- Incompressible micro-benchmark must show lower CPU vs “always attempt compression”.
- No correctness changes to record format.

---

### Milestone 2 — `dict.db`: Append-only Dictionary Store (Library + Format)

**Scope**
- New file: `dict.db` under the DB directory.
- A minimal append-only API:
  - `Append(dictBytes, meta) -> dictID`
  - `Get(dictID) -> (dictBytes, meta)`
  - `FindByHash(hash) -> (dictID, ok)` (optional)

**File format (v1)**
- Header:
  - magic `"TRDBDICT"`
  - version `uint32`
  - reserved
- Records (append-only):
  - `uint32 recordLen` (cap before allocation)
  - `uint32 headerCrc32c` (over record header fields)
  - `uint64 dictID`
  - `uint64 dictHash` (xxhash64 of dict bytes)
  - `uint32 dictLen` (cap)
  - `uint32 k` (0 if not set)
  - `uint32 level` (zstd level or 0)
  - `dictBytes[dictLen]`
  - `uint32 dictCrc32c` (CRC over dict bytes)

**Read safety**
- Hard caps before allocating (`recordLen`, `dictLen`).
- CRC validation returns clean `ErrCorrupt` (no panic/OOM).

**Durability**
- For now: append + flush at close / periodic sync.
- If used for sync writes later, `dict.db` must participate in the *Sync boundary* (separate milestone).

**Tests**
- Append/Get roundtrip.
- Corruption: wrong CRC, truncated record, oversized length → clean error.

**Acceptance**
- `dict.db` is readable with `pread` (no mmap requirement).
- On-disk format documented in this file.

---

### Milestone 3 — Dict Training + K Selection (Reusable `internal/compression`)

**Scope**
- Introduce `TreeDB/internal/compression/` as a reusable package:
  - `Trainer` (sample collection + dict build)
  - `ChooseKForDict` (K selection, default candidate set 1..8)
  - `ActiveProfile` (dict hash/id, K, level, ratio estimates)
- Explicitly decouple from:
  - slab zone boundaries
  - async slab writer machinery

**Policy (anti-thrash)**
- Update dict/profile only if all of:
  - bytes >= `MinProfileBytes`
  - records >= `MinProfileRecords`
  - time since last accept >= `MinProfileInterval`
  - observed drift >= `ProfileDriftThreshold`
  - new profile improves ratio >= `ProfileImproveThresh`

**Output**
- When the trainer accepts a new dict:
  - persist it to `dict.db` → returns `dictID`
  - publish `ActiveProfile{DictID, DictHash, K, Level, ...}` to readers/writers.

**Tests**
- Trainer never panics on bad samples; returns nil profile safely.
- Dict validation and retry smaller dict behavior is deterministic.
- K selection chooses within [1..8] and is stable under fixed input.

**Acceptance**
- Package usable by slab *and* future index/key compression code.

---

### Milestone 4 — Slab “Dictionary Epochs” (No Hard Zones, No Size Caps)

**Goal**
- Use `dictID` + optional `K` per record (or per group) without imposing physical boundaries.

**Design**
- Extend slab record headers to optionally include:
  - `dictID` (varint or fixed uint64)
  - `k` (small uint16)
  - `flags` (compressed, grouped, etc.)
- Rules:
  - **Arbitrary value lengths** must be supported.
  - If a value is extremely large, it must still be storable (either as a single frame or chunked records).

**Micro-batching (optional)**
- Group K records into one zstd frame for near-streaming ratio.
- Keep K small (default target K≈3–8) to keep point reads bounded.

**Tests**
- Roundtrip:
  - no dict / dict present
  - grouped / ungrouped
  - large values (including >2MB) must succeed
- Corruption:
  - missing dictID → clean error
  - wrong dict CRC → clean error

**Acceptance**
- No “ErrRecordTooLarge due to boundary math” class of failures.

---

### Milestone 5 — B-tree Near-term Wins (Small, Measured)

**Scope**
- Keep “internal separator shortening” and depth-limit regressions covered.
- Add targeted benchmarks:
  - long-key fanout and depth resilience
  - scan locality regressions

**Acceptance**
- Long-key workloads do not explode depth under append-heavy patterns.
- Any increase in page size or leaf format is deferred behind a separate proposal.

---

### Milestone 6 — Range Partition + Columnar Leaves (Design + Prototype Only)

This is explicitly deferred as an architecture effort. The deliverable is **prototype + benchmarks + migration sketch**, not a production merge.

**Topics**
- Range-partitioned keyspace (manifest + multiple index files)
- Columnar leaf layout (keys separate from value pointers)
- Larger “logical pages” / clusters (if justified by benchmarks)

**Acceptance**
- Prototype demonstrates a clear win on Celestia-like traces without introducing crash classes.

---

### Milestone 7 — WAL + Combined Slab Pipeline (Design + Minimal Implementation)

**Goal**
- A clean “double/triple buffering” design that minimizes lock contention and complexity.

**Constraints**
- Must not reintroduce brittle async shutdown choreography.
- Must preserve durability semantics:
  - payload durable happens-before WAL/index durable for `*Sync`.

**Proposed approach (sketch)**
- Define explicit buffers:
  - in-memory staging buffer(s)
  - append-only payload file(s)
  - WAL segments referencing only durable-or-protected payload ranges
- Consider “written” vs “durable” watermarks and segment deletion criteria.

**Acceptance**
- Clear invariants documented + regression tests for ordering.

## PR Hygiene: How We Merge This Sprint

- One PR per milestone.
- Each PR must include:
  - a strict include/exclude list (what files/symbols are touched)
  - tests added/updated
  - benchmark evidence (if applicable)
- If a milestone requires a format change:
  - call it out in the PR title/body
  - provide a minimal “new DB required” statement (pre-alpha OK)

