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
   - **Large-value handling is intentionally deferred**: new dict/K encodings must fall back to the existing K=1 record encoding when a record would exceed `slab.MaxRecordSize` (or any configured cap).

4) **Micro-batched value compression (bounded point reads)**
   - Optional micro-batching (`K`) for near-streaming ratios, while bounding point-read decode cost.

5) **Combined WAL + slab write protocol (synchronous first cut)**
   - A clean write/durability ordering is enforced without async slab writer goroutines.
   - Clear “written vs durable” semantics with explicit watermarks.
   - A clean path to future double/triple buffering is defined.

6) **Index optimization work that is real, test-backed, and benchmarked**
   - Long-key depth/fanout regressions are locked by tests.
   - A “columnar leaf layout” prototype exists behind an explicit experimental option, with benches and correctness tests.
   - A partitioned-index plan is executed (manifest + routing + per-partition maintenance), based on the deeper design in `TreeDB/btree_optimization.md`.

---

## 1) Hard Constraints (Non-Negotiable)

### C1 — No async slab writer machinery merged in this sprint

This sprint must not merge any “async writer” or “zone boundary choreography” machinery.
In particular, do **not** import anything like:

- `TreeDB/slab/writer.go`
- `activeSlabWriter` / flush goroutines / shutdown choreography
- hard zone boundaries (“2MB zones”) and associated packing rules

### C2 — Defer large-value format changes

This sprint does **not** attempt to redesign “very large value” storage (chunking / multi-record reassembly).

Constraints:
- Keep the existing `slab.MaxRecordSize`/`slab.ErrRecordTooLarge` behavior.
- Any new dict/K encoding must have a deterministic fallback to the existing K=1 record encoding when it would exceed the cap.
- Do not introduce new “boundary math” failure modes (no zones).

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
- deeper index.db work (columnar leaf + partitioned index plan)

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

#### A3.1 — Use the proven “grouped frame” model (from `feature/slab-optimizations`), but complete it

The previous implementation in `feature/slab-optimizations` established a good foundation:
- a grouped frame record type (one slab record holds K logical values)
- `ValuePtr` carries a `subIndex` (0..7) so each key can point into the grouped frame
- decode path extracts a single value from the decompressed frame

This sprint keeps the same conceptual model but makes it *complete and safe* for production:
- grouped records must remain compatible with slab compaction (which scans slab records)
- grouped records must have deterministic fallback when they exceed caps
- grouped records must be self-describing and corruption-hardened (caps before alloc)

#### A3.2 — Pointer encoding (minimal, preserves `slab.MaxRecordSize`)

We will extend `TreeDB/page/value_ptr_flags.go` with exactly what grouped frames need:
- `compressed` bit (already exists)
- `grouped` bit
- `subIndex` (3 bits; 0..7)

Important: we explicitly avoid consuming so many length bits that we can no longer represent ~64MiB records.
The target is to preserve enough length bits for the existing `slab.MaxRecordSize` default.

#### A3.3 — Grouped slab record format (K>1)

Grouped frames are represented as a normal slab record with `KeyLen=0` and a “group body” in the value bytes.

**Slab outer header (existing):**
- CRC32C (4)
- KeyLen (2) = 0
- ValueLen (4) = body length in bytes
- ValueBytes (body)

**Group body v0 (new, fixed header then tables):**
- `version` (u8) = 0
- `k` (u8) = number of logical rows (2..8)
- `offsetCount` (u16 LE) = k+1
- `dictID` (u64 LE) = dictionary ID used for this frame (0 means “no dict”)
- `offsets` (u32 LE)[k+1] = prefix sums into the decompressed payload (record boundaries)
- `compressed` (bytes) = zstd frame (optionally with dict) of the decompressed payload

`dictID` references `dict.db`. Readers resolve `dictID -> dict bytes` via a cached lookup and use a decoder pool keyed by `dictID` (avoid per-read decoder construction).

**Decompressed payload (bytes):**
Concatenation of K logical entries, each encoded as:
- `keyLen` (u16 LE)
- `valLen` (u32 LE)
- `keyBytes` (keyLen)
- `valBytes` (valLen)

This keeps grouped records compatible with compaction:
- the compactor can extract `(key,value)` per subIndex
- liveness checks remain key-based
- pointer identity includes `subIndex` so the correct live mapping is preserved

#### A3.4 — Grouping policy (when we do K>1)

Grouped frames are written only via `AppendMany`/flush paths (where we already have batches of keys/values).

Policy rules:
- `K` is chosen from the active profile (default K=1; candidate 2..8).
- Grouping is applied only when:
  - the batch has enough entries
  - the computed grouped record would not exceed `slab.MaxRecordSize`
  - the “pause/probe” gate allows compression attempts
- Otherwise fall back deterministically:
  - use smaller K (down to 2), or
  - K=1 legacy encoding (existing record format)

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
1) **columnar leaf prototype** behind an explicit experimental flag
2) **partitioned-index execution plan** (manifest + routing + per-partition maintenance), based on `TreeDB/btree_optimization.md`

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
- Introduce the grouped-frame encoding spec (K>1) and pointer flags:
  - `page.ValuePtrIsGrouped`, `page.ValuePtrSubIndex`, `page.ValuePtrMarkGrouped`
  - grouped frame body v0 (version/k/offsets/dictID + zstd frame)
- Decoder caching for grouped frames:
  - cache decoders by `dictID` (and optionally by `slab fileID`) in a bounded LRU
  - grouped reads must not create a new zstd decoder per call
- Hardened parsing:
  - cap `ValueLen` before allocation
  - cap `offsetCount` (must be k+1, k<=8)
  - cap decompressed payload length using `offsets[k]`
  - clean corruption errors (no panic/OOM)

**Tests**
- grouped frame roundtrip:
  - build grouped record (keys+values), write, read back each subIndex via pointer flags
- corruption tests:
  - invalid k/offsetCount
  - invalid offsets (non-monotonic / out of bounds)
  - truncated body / truncated zstd frame

**Acceptance**
- Group decode is bounded and safe (caps before alloc).
- No async writer introduced.

---

### PR 5 — Micro-batched compression (K) for slab values (bounded, production-complete)

**Goal**
- Achieve near-streaming compression ratios while bounding point-read decode cost, using the grouped-frame approach above.

**Deliverables**
- Write path integration (flush / `AppendMany`):
  - implement `AppendManyGrouped(keys, values, K)` (or equivalent)
  - deterministic fallback when grouped record would exceed `slab.MaxRecordSize`:
    - reduce K, then fall back to K=1
- Reader path integration:
  - `SlabManager.Read` and `ReadUnsafe` decode grouped pointers via `subIndex`
- Compaction correctness:
  - slab compactor must detect grouped records (`KeyLen=0` + group-body header)
  - for each grouped record:
    - decompress once
    - iterate subIndex 0..k-1
    - extract `(key,value)`
    - construct the exact old pointer (includes grouped bits + subIndex)
    - liveness check by key + pointer match
- Integrate K selection from `internal/compression`:
  - default candidates {1..8}
  - K is capped to keep point reads bounded
  - use score-based selection (ratio win vs estimated decode cost), like the tradeoff notes in `TreeDB/notes/slab_dict_k_tradeoff_2026-01-11.md`

**Tests**
- point-read extracts one value from a grouped frame (K>1)
- compaction preserves grouped values:
  - write grouped frames, run compaction, ensure values remain readable
- corruption tests: invalid K/offsets, truncated frame, invalid dictID (if used)

**Bench**
- per-value vs K=3..8 on:
  - compressible
  - mixed
  - incompressible

**Acceptance**
- Total bytes decreases on compressible workloads.
- Point-read decode work remains bounded (one frame).

---

### PR 6 — Combined WAL+slab protocol (synchronous first cut)

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

### PR 7 — Index.db deep work (v1): Columnar leaf + search accelerators (experimental)

**Goal**
- Implement deeper `index.db` improvements based on `TreeDB/btree_optimization.md`, starting with leaf layout and search accelerators.

**Deliverables**
- Experimental columnar leaf layout (new DBs only):
  - keys stored in a contiguous block (front-coded with restart points)
  - values/pointers stored in a contiguous block separate from keys
- Explicit leaf page format versioning:
  - a leaf body version byte (or equivalent) selects “legacy” vs “columnar” decode
  - avoids ambiguous mixed layouts in a single DB
- Optional “fingerprint strip” (1 byte per key) to accelerate comparisons:
  - used to filter candidate keys before full compare
- Optional pager-side negative-lookup accelerator (from `TreeDB/btree_optimization.md`):
  - per-page (or per-leaf) Bloom filter metadata to skip negative lookups before full decode
- Bench harness:
  - point reads
  - range scans
  - long-key workloads (iavl-bench style)

**Tests**
- correctness (get/put/iterators)
- long-key regression coverage (depth stays bounded; separators remain short)

**Bench**
- measurable speedup in key search and/or scans on synthetic and iavl-bench-like key distributions.

---

### PR 8 — Index.db deep work (v2): Partitioned pager + manifest + per-partition maintenance

**Goal**
- Replace the confusing “range vacuum” concept with a coherent partitioned-index design:
  - range partitioning is implemented as **multiple index files** + a **manifest**
  - maintenance (vacuum/compaction) is performed per-partition

**Deliverables**
- `docs/INDEX_PARTITIONING.md` (new doc) with a concrete design:
  - manifest format
  - routing rule (key range → partition)
  - partition ID encoding (e.g. upper bits of PageID route to the partition file)
  - pageID routing / file selection
  - atomic manifest swap protocol
- Prototype implementation (new DBs only):
  - create N partitions + manifest
  - route reads/writes to the correct partition
  - run vacuum/compaction on one partition without touching others

**Tests**
- correctness for routing (keys end up in correct partitions)
- correctness for per-partition vacuum (data preserved, size shrinks)

**Bench**
- partition-local vacuum time vs monolithic vacuum on synthetic workloads

**Acceptance**
- Prototype is runnable via a `cmd/` tool and has a benchmark harness.

---

### PR 9 — Index.db deep work (v3): Internal-node fanout + locality (separator/LCP/relative IDs)

**Goal**
- Implement the internal-node improvements from `TreeDB/btree_optimization.md` that directly increase fanout and reduce depth.

**Deliverables**
- Shortest-separator correctness + explicit bounds (already partially done; lock it in)
- Internal global LCP (strip shared prefix once per internal node)
- Internal layout improvements (pointer-first, compact separators)
- Relative page IDs within a partition (where feasible) to increase fanout

**Tests**
- long-key depth regression tests
- randomized insert/delete stability with the new internal encoding

**Acceptance**
- Demonstrably improves internal fanout and reduces depth on long-key workloads.

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
