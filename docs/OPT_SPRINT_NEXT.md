# Optimization Sprint Next (2026-01): Compression + Storage Simplification

This is the **full sprint execution spec** for the next TreeDB optimization sprint.
It is written to be *actionable* and *mergeable*: every milestone below is a PR-sized unit with explicit deliverables, acceptance criteria, and test/bench requirements.

Backwards compatibility is **not required** (pre-alpha), but **silent corruption is not acceptable**.

---

## 0) Sprint Outcome (End State)

At the end of this sprint, `main` contains:

1) **Adaptive compression viability detection (production-safe)**
   - TreeDB quickly stops attempting compression when it is not paying off.
   - TreeDB resumes compression automatically when it becomes worthwhile again.

2) **Append-only dictionary store (`dict.db`)**
   - Dictionaries are persisted in a small append-only file and referenced by stable IDs (`DictID`).
   - The read path is hardened (caps before allocation, CRC validation, clean errors).
   - The store is reusable by **values now** and **keys/index later**.

3) **Dictionary epochs for values (no physical zones)**
   - Values can reference a `DictID` without introducing hard “2MB zones”.
   - Values support **arbitrary lengths** (including multi-megabyte values).

4) **Micro-batched value compression (bounded point reads)**
   - Optional micro-batching (`K`) for near-streaming ratios, while bounding point-read decode cost.

5) **Combined WAL + slab write protocol (synchronous first cut)**
   - A clean write/durability ordering is enforced without async slab writer goroutines.
   - Clear “written vs durable” semantics with explicit watermarks.
   - A clean path to future double/triple buffering is defined.

6) **Index optimization work that is real, test-backed, and benchmarked**
   - Long-key depth/fanout regressions are locked by tests.
   - A “columnar leaf layout” prototype exists behind an explicit experimental option, with benches and correctness tests.
   - Range-partition maintenance lands as **range-targeted vacuum**.

---

## 1) Hard Constraints (Non-Negotiable)

### C1 — No async slab writer machinery merged in this sprint

This sprint must not merge any “async writer” or “zone boundary choreography” machinery.
In particular, do **not** import anything like:

- `TreeDB/slab/writer.go`
- `activeSlabWriter` / flush goroutines / shutdown choreography
- hard zone boundaries (“2MB zones”) and associated packing rules

### C2 — Values must support arbitrary lengths

No record format in this sprint may impose a value size cap (e.g. “record must fit under ZoneSize”).
If the compression format requires it, implement **chunking** (multi-record values).

### C3 — No new mmap usage on mutable/truncating files

Do not introduce new mmap behavior on:
- `data-*.slab` (mutable)
- `index.db` (mutable)

Append-only immutable files (like `dict.db`) may use mmap in a later milestone, but **not required** for this sprint.

### C4 — Do not reintroduce ValueIndex/ValueID indirection

This sprint does not add:
- `FlagValueID` leaf entries
- ValueID → ptr mapping tables
- GC that depends on such indirection

If any ValueID remnants exist, they remain disabled and unsupported unless a dedicated follow-on PR explicitly reintroduces them with evidence.

---

## 2) Current Baseline (Reality Check)

`main` already includes:
- per-value slab compression (zstd) in `TreeDB/slab/compression.go`
- long-key separator improvements documented in `docs/BTREE_KEY_SIZE_ISSUE.md`
- copy-on-flush safety for cached mode (vlog pointers are resolved before backend persistence)

This sprint builds on that baseline by adding:
- adaptive “don’t waste CPU” gating
- dictionary storage + training + dictID plumbing
- optional micro-batching (K)
- range-targeted maintenance tooling
- an experimental columnar leaf prototype

---

## 3) Architecture We Are Building (Locked Design)

### A1 — `dict.db`: Append-only dictionary registry

Purpose:
- Persist dictionaries (and small profile metadata) and reference them by stable IDs from values (and later keys).

Properties:
- append-only, never in-place rewritten
- safe read path: caps before alloc, CRCs, no panics
- small relative to state (32KB dicts are typical)

Identifiers:
- `DictID uint64`: monotonic ID
- `DictHash uint64`: xxhash64(dict bytes) for dedup/lookup

### A2 — Value “dictionary epochs” without physical zones

Dictionary selection is a **logical epoch**, not a file layout constraint.
Each value record may optionally reference:
- `DictID`
- `K` (micro-batching size) if stored in grouped frames

### A3 — Micro-batching (K) with bounded point reads

We allow `K` only in a small, safe range:
- Candidate set: `K ∈ {1..8}`.
- Default behavior: `K=1` unless the profile selector proves wins.

Point read bound:
- Worst-case decode work is **one frame** (not megabytes of unrelated data).

### A4 — Adaptive compression viability detection (“pause + probe”)

Mechanism:
- Rolling window on raw vs stored bytes + record count.
- If ratio stays “bad” long enough, pause compression for `pauseBytes`.
- While paused:
  - skip compression at near-zero overhead
  - still sample training data at low duty cycle
  - periodically probe to resume

### A5 — Synchronous combined WAL + slab protocol (first cut)

Definitions (normative):
- written: bytes appended to slab file (page cache)
- durable: slab file crossed fsync boundary

Sync ordering (normative):
1. slab payload durable (fsync)
2. WAL durable (fsync)
3. index/meta durable
4. ack

This sprint ships a synchronous implementation with tests. Future buffering is built on top of the same ordering.

### A6 — Index work shipped this sprint

We ship:
1) **range-targeted vacuum** (maintenance partitioning)
2) **columnar leaf prototype** behind an explicit experimental flag

---

## 4) Sprint Execution Plan (PRs)

The sprint is executed as 10 PRs. Each PR is independently mergeable.

### PR 0 — Measurement + Baselines (must land first)

**Goal**
- Make performance measurable and regressions obvious.

**Deliverables**
- Benchmarks covering:
  - compressible values
  - incompressible values
  - mixed workloads
- Metrics counters for:
  - compression attempts / skips
  - rolling ratio window stats
  - pause/probe state transitions

**Acceptance**
- PR description includes baseline numbers (command + machine).

---

### PR 1 — Adaptive “Pause + Probe” on current slab compression (no format changes)

**Goal**
- When compression isn’t paying off, stop doing it quickly and cheaply.

**Deliverables**
- Rolling ratio window
- Pause budget + probe-based resume
- Minimal knobs with safe defaults

**Tests**
- Deterministic regression:
  - random bytes trigger pause quickly
  - compressible payload resumes compression after probe

**Acceptance**
- In incompressible bench: CPU decreases measurably vs baseline.
- In compressible bench: no meaningful throughput regression.

---

### PR 2 — `dict.db`: append-only dictionary store (new internal package)

**Goal**
- A hardened dictionary registry that can be used by values now and keys later.

**Deliverables**
- `TreeDB/internal/dictstore/` package:
  - `Append(dictBytes, meta) -> DictID`
  - `Get(DictID) -> dictBytes, meta`
  - `FindByHash(hash) -> DictID` (optional)
- File format v1 (documented in-code and in docs):
  - header magic + version
  - recordLen caps
  - CRCs (header + dict bytes)

**Tests**
- roundtrip append/get
- corruption: bad CRC, truncation, oversized lengths → clean errors

**Acceptance**
- OOM-safe by construction (caps tested).

---

### PR 3 — Training + profile selection library (reusable `internal/compression`)

**Goal**
- Land “compression intelligence” as a reusable library (no slab layout coupling).

**Deliverables**
- `TreeDB/internal/compression/`:
  - trainer sample collection
  - dict build + validation + safe retry
  - K selection for bounded point reads (default candidates 1..8)
  - anti-thrash gating (bytes/records/time/drift thresholds)
- Integration with `dictstore`:
  - accepted dicts are persisted to `dict.db` and published as `ActiveProfile`.

**Tests**
- trainer never panics (recover guard)
- deterministic K choice on fixed input
- gating prevents thrash

**Acceptance**
- A micro-benchmark exists that shows K selection and dict acceptance behavior.

---

### PR 4 — Slab record header vNext: optional `DictID` (no physical zones)

**Goal**
- Enable values to reference a `DictID` directly from the slab record format.

**Deliverables**
- New slab record header extension:
  - flags: compressed, hasDictID, grouped, chunked
  - optional `DictID`
  - optional group metadata
- Hardened read path:
  - validate lengths before allocation
  - clean errors for invalid headers/dict IDs

**Tests**
- roundtrip for:
  - raw
  - compressed without dict
  - compressed with dictID
- corruption tests for bad headers and missing dict

**Acceptance**
- No async writer introduced.

---

### PR 5 — Arbitrary large values: chunked slab values (must land)

**Goal**
- Remove any possibility of value-size caps by supporting chunked values.

**Deliverables**
- Encode a large value as multiple slab records:
  - start record includes total length and chunk count (or continuation pointer)
  - continuation records include chunk index
- Reader reassembles into the requested byte slice
- Hard caps on total length to avoid OOM on corrupt metadata

**Tests**
- roundtrip for 4MB+ values
- corruption tests: missing continuation, wrong indices, truncation

**Acceptance**
- No “ErrRecordTooLarge” exists as a normal operational outcome.

---

### PR 6 — Micro-batched compression (K) for slab values (bounded)

**Goal**
- Achieve near-streaming compression ratios for values while bounding point-read decode cost.

**Deliverables**
- A “grouped frame” encoding:
  - K values compressed into one zstd frame
  - small offset table for extracting a single value without scanning everything
- Integrate K selection from `internal/compression`

**Tests**
- point-read extracts one entry from a grouped frame
- corruption tests: invalid offset table, invalid K

**Bench**
- per-value vs K=3..8 on:
  - compressible
  - mixed
  - incompressible

**Acceptance**
- Total bytes decreases on compressible workloads.
- Point-read decode work remains bounded (one frame).

---

### PR 7 — Combined WAL+slab protocol (synchronous first cut)

**Goal**
- Make the durability ordering explicit and enforced with tests.

**Deliverables**
- Explicit written/durable watermarks for slab payloads
- `*Sync` ordering enforcement:
  - slab durable happens-before WAL durable happens-before index durable
- Harden WAL parsing (length caps before allocation)

**Tests**
- ordering regressions (hooks/latches)
- WAL corruption hardening regressions (no OOM/panic)

**Acceptance**
- Clear, test-backed contract that a future buffering refactor can build on.

---

### PR 8 — Range partition for maintenance: range-targeted vacuum (must land)

**Goal**
- Make vacuuming cheaper and more controllable by targeting subsets of keyspace.

**Deliverables**
- Offline vacuum option that rebuilds only `[start, end)` from a snapshot iterator:
  - preserves keys outside range
  - compacts keys inside range
- First cut is permitted to rewrite the full file if needed, but must iterate ranges and prove correctness.

**Tests**
- correctness for:
  - keys inside range preserved
  - keys outside range unchanged

**Bench**
- vacuum of a subset is measurably faster than full vacuum on synthetic workloads.

---

### PR 9 — Columnar leaf prototype (experimental but mergeable)

**Goal**
- Implement a real “keys-first, values-second” leaf layout prototype.

**Deliverables**
- Behind an explicit experimental flag (new DBs only):
  - `Options.ExperimentalColumnarLeaf = true`
- Encoding concept:
  - key block (front-coded or prefix-compressed)
  - value pointer/value area separate

**Tests**
- get/put correctness
- iterator correctness

**Bench**
- point lookup
- range scans

**Acceptance**
- Shows measurable wins in benchmarks; remains opt-in and off by default.

---

### PR 10 — Keyspace partition manifest (design + prototype, mergeable)

**Goal**
- Deliver the architecture for “range partitioned index files” as a real prototype, not a vague future plan.

**Deliverables**
- `docs/INDEX_PARTITIONING.md` (new doc) describing:
  - manifest format
  - routing (key range → file)
  - maintenance (vacuum per range)
  - migration strategy (pre-alpha allowed to be “new DB”)
- Prototype code (not enabled by default) that can:
  - create N partitions for a fresh DB
  - route writes by key prefix/range to a partition
  - run vacuum on one partition independently

**Acceptance**
- Prototype is runnable via a `cmd/` tool and has a benchmark harness.

---

## 5) Sprint-Level Definition of Done

- All PRs above are merged (or rejected with revert commits if a prototype fails acceptance).
- `go test ./... -count=1` passes.
- We have benchmark evidence recorded in PRs for:
  - compressible
  - incompressible
  - mixed
- There is a single “fastest configuration” runbook:
  - fastest local benchmark config
  - fastest safe Celestia config

