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

TreeDB already uses mmap for:
- `index.db` via `TreeDB/pager` (chunked mmap)
- `data-*.slab` read-mostly views (existing behavior)

This sprint must not introduce **additional** mmap usage beyond that baseline:
- no mmap on WAL/vlog
- no mmap on `dict.db` (use `pread` in this sprint)
- no new mmap on any file that is truncated/rotated at runtime

### C4 — Do not reintroduce ValueIndex/ValueID indirection

This sprint does not add:
- `FlagValueID` leaf entries
- ValueID → ptr mapping tables
- GC that depends on such indirection

If any ValueID remnants exist, they remain disabled and unsupported unless a dedicated follow-on PR explicitly reintroduces them with evidence.

---

## 2) Current Baseline (Reality Check)

`main` already includes:
- per-value slab compression (zstd) in `TreeDB/slab/compression.go` (legacy “envelope v0”)
- long-key separator improvements documented in `docs/BTREE_KEY_SIZE_ISSUE.md`
- copy-on-flush safety for cached mode (vlog pointers are resolved before backend persistence)

This sprint builds on that baseline by adding:
- adaptive “don’t waste CPU” gating
- dictionary storage + training + dictID plumbing
- optional micro-batching (K)
- deeper index.db work (columnar leaf + partitioned index plan)

**Note on “zlib” vs “zstd”**
- TreeDB does not currently use `compress/zlib`. The existing per-value compressor is zstd (SpeedFastest) and is treated as the legacy baseline (“envelope v0”).
- The dict+K work in this sprint is not a parallel compression system: it extends the same envelope with a `DictID` (“envelope v1”) and adds grouped frames (K>1).
- PR1’s pause/probe gate is explicitly the mechanism that makes “envelope v0” effectively disappear on real workloads where it does not pay off.

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

#### A1.1 — `dict.db` on-disk format v1 (normative)

`dict.db` is append-only and lives alongside `index.db` and `data-*.slab` in the TreeDB directory.

**File header (offset 0)**
- `magic[8] = "TRDBDICT"`
- `version u32 = 1`
- `reserved u32 = 0`

**Record format (repeated until EOF)**
- `crc32c u32` (Castagnoli; covers bytes `[4 : 4+4+recordLen]`)
- `recordLen u32` (bytes after this field; MUST equal `24 + dictLen + metaLen`)
- `dictID u64` (monotonic starting at 1)
- `dictHash u64` (xxhash64(dictBytes))
- `dictLen u32`
- `metaLen u32`
- `dictBytes[dictLen]`
- `metaBytes[metaLen]`

**Read behavior (normative)**
- On open, sequentially scan and build an in-memory index `DictID -> fileOffset`.
- The final record may be partially written; treat a **truncated tail record** as EOF and ignore it.
- Any CRC failure or malformed record **before** the tail MUST fail-closed (return an error).

**Caps (normative)**
- Reject `dictLen == 0` or `dictLen > 1<<20`.
- Reject `metaLen > 1<<20`.
- Reject `recordLen > 2<<20`.

### A2 — Value “dictionary epochs” without physical zones

Dictionary selection is a **logical epoch**, not a file layout constraint.
Each value record may optionally reference:
- `DictID`
- `K` (micro-batching size) if stored in grouped frames

#### A2.1 — K=1 compressed value envelope (DictID optional; no slab header change)

We keep the slab outer record format **unchanged** (`CRC32C | KeyLen | ValueLen | KeyBytes | ValueBytes`).

For K=1 values, dictionary selection is represented inside the **compressed value bytes** only.
The pointer’s `compressed` bit continues to indicate “value bytes are encoded”.

**Encoding (little-endian):**
- `rawLenOrFlag` (u32)
  - If MSB=0: legacy envelope v0 (no dict), `rawLen = rawLenOrFlag`, payload is `zstdFrame`.
  - If MSB=1: envelope v1 (dict), `rawLen = rawLenOrFlag & 0x7fffffff`, then:
    - `dictID` (u64) (must be >0)
    - payload is `zstdFrame` compressed **with the dictionary referenced by `dictID`**.

**Safety caps (normative):**
- Reject if `rawLen == 0` or `rawLen > MaxRecordSize` (or configured cap).
- Reject if v1 and `dictID == 0`.
- Reject if the encoded bytes are too short to contain required fields.

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

We extend `TreeDB/page/value_ptr_flags.go` with exactly what grouped frames need, while preserving enough length bits to represent the default `slab.MaxRecordSize` (64MiB).

**Normative bit layout in `ValuePtr.Length` (u32):**
- bit 31: `compressed` (existing)
- bit 30: `grouped` (new)
- bits 27..29: `subIndex` (new; only meaningful when grouped; 0..7)
- bits 0..26: `recordLen` (length excluding CRC; same as today)

**Required constants (must match these values):**
- `valuePtrCompressedMask = 0x80000000`
- `valuePtrGroupedMask    = 0x40000000`
- `valuePtrSubIndexMask   = 0x38000000`
- `valuePtrSubIndexShift  = 27`
- `valuePtrFlagsMask      = valuePtrCompressedMask | valuePtrGroupedMask | valuePtrSubIndexMask`

**Required semantics (normative):**
- `ValuePtrRecordLength(ptr)` MUST clear all pointer flags: `ptr.Length &^ valuePtrFlagsMask`.
- Robust pointer identity used by maintenance MUST ignore the `compressed` bit, but MUST treat `grouped` + `subIndex` as part of identity:
  - match on `(FileID, Offset, ValuePtrRecordLength, IsGrouped, SubIndex)`
  - do NOT use raw struct equality in compaction/vacuum matching.

#### A3.3 — Grouped slab record format (K>1)

Grouped frames are represented as a normal slab record with `KeyLen=0` and a “group body” in the value bytes.

**Slab outer header (existing):**
- CRC32C (4)
- KeyLen (2) = 0
- ValueLen (4) = body length in bytes
- ValueBytes (body)

**Group body v0 (new, fixed header then tables):**
- `version` (u8) = 0
- `k` (u8) = number of logical rows (2..8) (k=1 MUST NOT use this format)
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

**Safety caps (normative):**
- `k` MUST be in `[2,8]`.
- `offsetCount` MUST equal `k+1`.
- `offsets[0]` MUST equal 0.
- `offsets` MUST be monotonic non-decreasing and `offsets[k]` MUST be the decompressed payload length.
- Decompressed payload length MUST be capped before allocation:
  - `payloadLen = offsets[k]`
  - Reject if `payloadLen == 0` or `payloadLen > CompressionGroupedMaxPayloadBytes` (new option; default 64KiB).
- Per-row bounds:
  - For each row, reject if it cannot parse `keyLen`/`valLen` fully within the row boundary.
  - Reject if `keyLen > CompressionGroupedMaxKeyBytes` (default 1KiB).
  - Reject if `valLen > CompressionGroupedMaxValueBytes` (default 4KiB).
  - Reject if `2+4+keyLen+valLen != rowLen` (exact match required).

**Write-time grouping constraints (normative):**
- Grouping MUST only be attempted when every candidate row fits:
  - `len(key) <= CompressionGroupedMaxKeyBytes`
  - `len(value) <= CompressionGroupedMaxValueBytes`
- Otherwise, fall back to K=1 encoding for that row.

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

### A7 — Index Optimization Strategy (Locked Decisions)

This section resolves the “advanced index ideas” uncertainty by explicitly choosing what we will and will not do in this sprint, given TreeDB’s current implementation:
- fixed `page.PageSize = 4096`
- chunked-mmap pager (`TreeDB/pager`)
- copy-on-write B+Tree pages rebuilt on writes

#### A7.1 — Performance Model (Hot vs Cold)

TreeDB index performance is bimodal:
- **Cold pages (I/O-bound):** reduce tree height by increasing fanout (more keys/page).
- **Hot pages (CPU/cache-bound):** reduce cache misses and wasted compares inside each page.

This sprint targets both, but only with changes that preserve:
- fixed-size 4KB page addressing (mmap friendly)
- order-preserving key encoding (binary search must still work)
- fail-closed parsing (no panics/OOM; caps before alloc)

#### A7.2 — Techniques We Adopt (this sprint)

These map directly to the external agent’s suggestions, but are compatible with TreeDB’s constraints:

**Fanout / cold-page wins**
- **Suffix truncation / shortest separators** (internal nodes): already in-tree; PR9 locks correctness + makes it denser.
- **Internal global LCP** (internal nodes): PR9 (strip common prefix once per node).
- **Pointer-first + compact separator layout** (internal nodes): PR9.
- **CSB+-style pointer compression (safe subset):**
  - PR9 stores child pointers as `base + u32 delta` *when possible* (within a partition), otherwise falls back to `u64`.

**CPU / hot-page wins**
- **Columnar leaf layout** (separate key bytes from values/pointers): PR7.
- **Fence keys via restart keys**:
  - PR7 uses restart keys as “fence keys” to narrow searches before reconstructing in-block keys.
- **Fingerprint strip** (1 byte per key):
  - PR7 uses a fingerprint strip to avoid reconstructing/comparing most candidate keys.

#### A7.3 — Techniques We Explicitly Do NOT Adopt (this sprint)

These are either incompatible with the mmap+fixed-page model, or too risky without a larger format redesign:

- **Zstd-compressed index keys:** rejected for this sprint.
  - zstd is not order-preserving; searching would require decompressing entire pages/blocks.
  - variable compressed sizes would break fixed `pageID * PageSize` addressing unless we add a page table.
  - Instead, we use order-preserving front-coding + restart points + fingerprints.
- **Large “virtual pages” (16–64KB nodes):** deferred.
  - would require multi-page node format and major pager/tree changes.
- **CSB+ “single pointer per node” layout:** deferred.
  - requires allocating children contiguously as an invariant across mutations; too invasive for now.
- **Pointer swizzling (rewrite mmapped offsets into pointers):** deferred.
  - mutates mmapped pages and complicates durability/dirty tracking.
- **SIMD/AVX search and assembly-heavy intrinsics:** deferred (Go portability + complexity).
- **Eytzinger layout inside nodes:** deferred.
  - plausible for immutable arrays, but TreeDB keys are variable-length and compare cost dominates; we take the simpler wins first.

#### A7.4 — Decision Matrix (External Suggestions → Sprint Plan)

This is the explicit mapping from the external agent’s proposals to TreeDB’s constraints and to concrete PRs in this sprint.

| External suggestion | Fit with TreeDB (today) | Sprint decision | Where it lands |
| --- | --- | --- | --- |
| **Suffix truncation** (“shortest separators”) | ✅ Already used by `zipper.shortestSeparator` and matches TreeDB’s “child start key” internal-node semantics | **Adopt + lock by tests** | PR9 (correctness invariants + density tests) |
| **CSB+ Trees** (contiguous children, single base ptr) | ❌ Requires contiguous child allocation invariant across COW mutations | **Defer** | Explicit non-goal (A7.3) |
| **CSB+-style pointer compaction** (fewer bytes for child IDs) | ✅ Feasible as an encoding *subset* with `base + u32 delta` when safe | **Adopt safe subset** | PR9 (Internal v1 `childEncoding=base+u32`) |
| **Eytzinger layout** (BFS order) | ⚠️ Possible but high risk/complexity with variable-length keys; compare cost dominates | **Defer** | Explicit non-goal (A7.3) |
| **SIMD search / AVX / asm** | ❌ Go portability + complexity; requires assembly/CGO for real wins | **Defer** | Explicit non-goal (A7.3) |
| **Branchless binary search** | ⚠️ Some wins possible, but key-compare is still branchy; do later after format wins | **Defer** | Explicit non-goal (A7.3) |
| **Pointer swizzling** (rewrite mmap offsets to pointers) | ❌ Mutates mmapped pages; complicates durability and dirty tracking | **Defer** | Explicit non-goal (A7.3) |
| **Zstd-compressed index keys** | ❌ Not order-preserving; breaks binary-search semantics and fixed 4KB page math | **Reject** | Explicit non-goal (A7.3) |
| **Fence keys** (narrow decompression) | ✅ We can use restart keys + fingerprints to reduce work without block decompression | **Adopt analogue** | PR7 (restart search + fingerprint strip; Appendix D) |

---

## 4) Sprint Execution Plan (PRs)

The sprint is executed as 10 PRs. Each PR is independently mergeable.

### Global Gates & PR Exposition (Mandatory)

Every PR in this sprint MUST satisfy the following process requirements.

#### G0 — Required PR writeup (no “drive-by” PRs)

Every PR description MUST include these sections (in this order):
1) **Objective**: 1–3 sentences.
2) **Context**: why this change is needed now.
3) **Non-goals**: explicitly list what is *not* being done.
4) **Design**: formats/flags/APIs and the invariants they must preserve.
5) **Scope**:
   - **Includes**: explicit list of touched files/symbols.
   - **Excludes**: explicit list of forbidden files/symbols (e.g. async slab writer, zones, ValueIndex).
6) **Correctness**:
   - invariants (bullet list)
   - tests run (exact commands)
   - failure modes + how they are fail-closed (caps before alloc, CRCs, truncation behavior)
7) **Performance**:
   - benchmarks run (exact commands)
   - results table (before/after) using the gate protocol below
   - any observed regressions and why they are acceptable (must be rare)
8) **Rollout / Toggle Plan**:
   - default setting
   - how to disable quickly
9) **Follow-ups**: list any deferred work with issue links.

PRs MUST use `.github/PULL_REQUEST_TEMPLATE/opt_sprint.md` and fill the narrative sections above (not just the checklist).

#### G1 — Benchmark gate protocol (explicit)

When a PR has a “Performance Gates” section below, it MUST follow this protocol:
- Run each benchmark **5 times**: `-count=5`.
- Use the **median** as the reported value.
- Report the machine details in the PR description:
  - `go env GOMAXPROCS`, `go env GOOS GOARCH`, CPU model, RAM, and storage type.
- If results are noisy, increase to `-count=10` and report median-of-10.

**Pass/fail thresholds (default)**
- A regression is defined as:
  - throughput decrease > **5%**, or
  - latency increase > **5%**, or
  - `allocs/op` increase > **10%**
  compared to the baseline specified in the PR’s gates.
- If a PR must accept a regression (rare), it MUST:
  - justify it explicitly (correctness win), and
  - open a follow-up issue with an owner + plan to recover performance.

#### G2 — Correctness gate protocol (explicit)

When a PR has a “Correctness Gates” section below, it MUST:
- add deterministic tests for the failure mode(s)
- include corruption tests (bad headers, truncation, invalid lengths) for any new parser/format
- be fail-closed (clean errors; no panics; caps before allocation)
- run `go test ./... -count=1` locally before merge

If a PR changes concurrency behavior or introduces new goroutines/locks:
- run `go test -race` for the affected packages and include the commands in the PR description.

### PR 0 — Measurement + Baselines (must land first)

**Goal**
- Make performance measurable and regressions obvious.

**Files (explicit)**
- Add: `TreeDB/slab/bench_gate_test.go`
- Modify (if needed): `TreeDB/bench_test.go` (only to add a `-short` fast path; must not change default behavior)
- Modify (if needed): `docs/BENCHMARK_SPEC.md` (document the gate protocol; no placeholder text)

**Deliverables (explicit)**
1) A standard benchmark suite used as the sprint-wide perf baseline:
   - `BenchmarkDB_Batch_100B` (wraps/aliases `TreeDB/db:BenchmarkBatch` with fixed params)
   - `BenchmarkDB_Get_100B` (wraps/aliases `TreeDB/db:BenchmarkGet` with fixed params)
   - `BenchmarkSlab_ValueCompress_Compressible`
   - `BenchmarkSlab_ValueCompress_Incompressible`
   - `BenchmarkSlab_ValueCompress_Mixed`
2) Each new slab benchmark MUST report:
   - throughput (`ns/op`)
   - `allocs/op`
   - stored bytes vs raw bytes (log line via `b.Logf`)
3) All metrics/logging added in this PR MUST be:
   - off by default
   - enabled only in tests/benchmarks (no production overhead)

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/db -run '^$' -bench 'BenchmarkBatch|BenchmarkGet' -count=1` (ensures benches build)

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- On the PR0 diff itself, performance MUST NOT regress (this PR is measurement-only):
  - `go test ./TreeDB/db -run '^$' -bench 'BenchmarkBatch|BenchmarkGet' -count=5`
  - median `ns/op` must not regress by >2% vs the base commit on the same machine.

**PR Writeup Notes**
- Include the baseline numbers table that later PRs will compare against.

---

### PR 1 — Adaptive “Pause + Probe” on current slab compression (no format changes)

**Goal**
- When compression isn’t paying off, stop doing it quickly and cheaply.

**Files (explicit)**
- Modify: `TreeDB/slab/compression.go`
- Modify: `TreeDB/slab/manager.go`
- Add: `TreeDB/slab/compression_adaptive_test.go`

**New knobs (explicit, with defaults)**
Add these to `slab.Options` in `TreeDB/slab/compression.go`:
- `CompressionMetricsWindowBytes int` (default `4 << 20`)
- `CompressionAdaptiveRatio float64` (default `0.0` = disabled; recommended “fast safe” default for prod configs: `0.98`)
- `CompressionAdaptiveMinRecords int` (default `1024`)
- `CompressionAdaptivePauseBytes int` (default `64 << 20`)
- `CompressionAdaptiveProbeBytes int` (default `4 << 20`)
- `CompressionAdaptivePauseSampleStride int` (default `64`) (used later by training; in PR1 this only exists for deterministic behavior + stats)

**Implementation (normative, bot-executable)**
1. Add the following fields to `slab.SlabManager`:
   - `compressionPauseRemaining atomic.Uint64`
   - `compressionProbeRemaining atomic.Uint64`
   - `compressionProbeBytes uint64`
   - `pausedSampleStride uint64`
   - `pausedSampleCounter atomic.Uint64`
   - pause/probe visibility counters (all monotonic; safe for stats only):
     - `compressionPauseCount atomic.Uint64`
     - `compressionProbeCount atomic.Uint64`
     - `compressionProbeResumeCount atomic.Uint64`
     - `compressionSkippedRawBytes atomic.Uint64` (raw bytes written while paused, with compression skipped)
   - rolling window counters (raw/stored/records) stored under `sm.mu`:
     - `windowRaw uint64`, `windowStored uint64`, `windowRecords uint64`
2. Implement `(*SlabManager).shouldAttemptCompression(rawLen int) (attempt bool, probe bool, paused bool)` with the same semantics as the proven version in `feature/slab-optimizations`:
   - If `compressionPauseRemaining > 0`:
     - decrement it by `rawLen` using CAS loop
     - increment `compressionSkippedRawBytes` by `rawLen`
     - if `CompressionAdaptiveProbeBytes == 0`: return `(false,false,true)`
     - otherwise decrement `compressionProbeRemaining` and return `(true,true,true)` exactly when it crosses zero (probe moment)
   - If not paused: return `(true,false,false)`
3. Wire `shouldAttemptCompression` into both write paths:
   - `SlabManager.Append`
   - `SlabManager.AppendMany`
   Behavior:
   - If `attempt==false`: write raw bytes (no compression) and do not set `ValuePtr` compressed flag.
   - If `attempt==true`: run the existing compression attempt (per-value zstd), and set compressed flag only when we actually store an encoded value.
4. Update the rolling window counters on every write (raw/stored/records), then:
   - if `windowRaw >= CompressionMetricsWindowBytes` AND `windowRecords >= CompressionAdaptiveMinRecords`:
     - compute `ratio = float64(windowStored)/float64(windowRaw)`
     - if `CompressionAdaptiveRatio > 0` AND `ratio >= CompressionAdaptiveRatio`:
       - `compressionPauseRemaining.Store(uint64(CompressionAdaptivePauseBytes))`
       - `compressionProbeBytes = uint64(CompressionAdaptiveProbeBytes)` (cached)
       - `compressionProbeRemaining.Store(compressionProbeBytes)`
       - increment `compressionPauseCount`
     - reset the window counters to 0
5. Probe resume rule (explicit):
   - If we are paused and `probe==true` and compression succeeds for the probed record (`compressed==true`), immediately clear the pause:
     - `compressionPauseRemaining.Store(0)`
     - increment `compressionProbeResumeCount`

**Tests (explicit)**
- Add `TestCompressionPauseAndProbeResume` to `TreeDB/slab/compression_adaptive_test.go`, based on the proven logic in `feature/slab-optimizations`:
  1) Configure tiny window/pause/probe values so the test runs quickly.
  2) Append random noise until `compressionPauseRemaining > 0`.
  3) Append highly-compressible payload until a compressed pointer is produced AND `compressionPauseRemaining == 0`.

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/slab -run TestCompressionPauseAndProbeResume -count=1`
- `go test -race ./TreeDB/slab -run TestCompressionPauseAndProbeResume -count=1`

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/slab -run '^$' -bench 'BenchmarkSlab_ValueCompress_(Compressible|Incompressible|Mixed)' -count=5`
- The slab benchmarks MUST include sub-benchmarks `adaptive_off` and `adaptive_on` (same dataset, same payload sizes).
- Gates:
  - Incompressible: median `ns/op` for `adaptive_on` MUST improve by ≥10% vs `adaptive_off`.
  - Compressible: median `ns/op` for `adaptive_on` MUST NOT regress by >5% vs `adaptive_off`.
  - Mixed: median `ns/op` for `adaptive_on` MUST NOT regress by >5% vs `adaptive_off`.

**PR Writeup Notes**
- Include a before/after table for the three slab benchmarks (adaptive_off vs adaptive_on).
- Include the pause/probe visibility counters from this PR (pause count, probe count, probe resumes, skipped raw bytes) and compute a “paused ratio”:
  - `paused_ratio = skipped_raw_bytes / total_raw_bytes` for the benchmark run (report as a single line).

---

### PR 2 — `dict.db`: append-only dictionary store (new internal package)

**Goal**
- A hardened dictionary registry that can be used by values now and keys later.

**Files (explicit)**
- Add: `TreeDB/internal/dictstore/store.go`
- Add: `TreeDB/internal/dictstore/store_test.go`

**API (explicit)**
Implement `TreeDB/internal/dictstore` with:
- `type DictID uint64`
- `type DictHash uint64`
- `var ErrNotFound = errors.New("dictstore: dict id not found")`
- `type Store struct { ... }` (thread-safe for concurrent `Get`, single-writer for `Append`)
- `func Open(path string) (*Store, error)`
- `func OpenReadOnly(path string) (*Store, error)`
- `func (s *Store) Close() error`
- `func (s *Store) Append(dictBytes []byte, meta []byte) (id DictID, hash DictHash, err error)`
- `func (s *Store) Get(id DictID) (dictBytes []byte, meta []byte, err error)`
- `func (s *Store) FindByHash(hash DictHash) (id DictID, ok bool)`

**Concurrency model (explicit)**
- `Store.Append` MUST be protected by a mutex (single-writer enforced in-process).
- `Store.Get` and `FindByHash` MUST be safe for concurrent callers and MUST NOT take the append mutex (RW lock or separate locks are acceptable).
- If `Store` is opened read-only, `Append` MUST return an error.

**On-disk format (normative)**
- Path: `dict.db` lives alongside `index.db` and `data-*.slab` in the TreeDB directory.
- File header (offset 0):
  - `magic[8] = "TRDBDICT"`
  - `version u32 = 1`
  - `reserved u32 = 0`
- Then repeated records:
  - `crc32c u32` (Castagnoli; covers bytes `[4 : 4+4+recordLen]`)
  - `recordLen u32` (bytes after this field; MUST equal `24 + dictLen + metaLen`)
  - `dictID u64` (monotonic starting at 1)
  - `dictHash u64` (xxhash64(dictBytes))
  - `dictLen u32`
  - `metaLen u32`
  - `dictBytes[dictLen]`
  - `metaBytes[metaLen]`
- Read behavior on open (explicit):
  - sequentially scan; for the final record, allow truncation (partial tail) and stop
  - if a non-tail record fails CRC or is malformed, return an error (fail-closed)

**Caps (normative)**
- Reject `dictLen == 0` or `dictLen > 1<<20`.
- Reject `metaLen > 1<<20`.
- Reject `recordLen > 2<<20`.

**Tests (explicit)**
- `TestDictStore_RoundTrip` (append 2 dicts; get by id; find by hash).
- `TestDictStore_TruncatedTailIsIgnored` (truncate mid-record; open succeeds; earlier dicts readable).
- `TestDictStore_CorruptCRC_FailsClosed` (flip one byte in non-tail record; open fails).

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/internal/dictstore -count=1`
- `go test -race ./TreeDB/internal/dictstore -count=1`

**Performance Gates (must pass)**
- Add to `TreeDB/internal/dictstore/store_test.go`:
  - `BenchmarkDictStore_Get_32KB`
  - `BenchmarkDictStore_Append_32KB`
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/internal/dictstore -run '^$' -bench 'BenchmarkDictStore_(Get|Append)_32KB' -count=5`
- Gates (hard numbers; fail if exceeded):
  - `BenchmarkDictStore_Get_32KB`: `allocs/op <= 3` and `B/op <= 65536`
  - `BenchmarkDictStore_Append_32KB`: `allocs/op <= 8` and `B/op <= 131072`

**PR Writeup Notes**
- Include the benchmark table and confirm the caps/fail-closed behavior by referencing the corruption tests.

---

### PR 3 — Training + profile selection library (reusable `internal/compression`)

**Goal**
- Land “compression intelligence” as a reusable library (no slab layout coupling).

**Files (explicit)**
- Add: `TreeDB/internal/compression/types.go`
- Add: `TreeDB/internal/compression/config.go`
- Add: `TreeDB/internal/compression/metrics.go`
- Add: `TreeDB/internal/compression/profile.go`
- Add: `TreeDB/internal/compression/trainer.go`
- Add: `TreeDB/internal/compression/trainer_test.go`

**Locked scope (explicit)**
- This PR is **pure library + unit tests**. It does NOT modify slabs, the B-tree, WAL, or compaction.

**API (explicit)**
Implement the following (port/adapt from `feature/slab-optimizations:TreeDB/internal/compression/*`):
- `types.go`:
  - `type Kind uint8` with `KindNone`, `KindZSTD`
  - `type Options struct { Kind Kind; MinBytes int; MinSavingsBytes int; Level int }`
  - `type TrainConfig struct { TrainBytes, DictBytes, MinRecords, MaxRecordBytes, SampleStride, DedupWindow, Level int }`
- `config.go`:
  - `type Config struct { Kind Kind; MinBytes int; MinSavings int; Level zstd.EncoderLevel; ZstdEncs, ZstdDecs, BufferPool *sync.Pool }`
  - `func NormalizeOptions(opts Options) (Config, error)`
  - `func (c *Config) CompressValue(...)` and `DecompressValue(...)` (plus a dict-aware variant added in PR4)
- `metrics.go`:
  - `type MetricsOptions struct { MetricsEnabled bool; AdaptiveRatio float64; WindowBytes int; MinRecords int; PauseBytes int }`
  - `type Metrics struct { ... }` with `Add(...) (pauseBytes uint64)` and `LogWindow()` as in feature branch
- `profile.go`:
  - `type ActiveProfile struct { DictID uint64; DictHash uint64; DictBytes int; Dict []byte; K int; PayloadRatio float64; TotalRatio float64; DecodeNsEstimate int64; Samples int; Timestamp time.Time }`
  - `func ChooseKForDict(dict []byte, samples [][]byte) *ActiveProfile` using the K scoring model with candidates `{1..8}`
- `trainer.go`:
  - `type Trainer struct { ... }`
  - `func NewTrainer(opts TrainConfig, cfg Config, readOnly bool, metricsEnabled bool) *Trainer`
  - `func (t *Trainer) Collect(value []byte)`
  - `func (t *Trainer) SignalDegraded(slabID uint32)`
  - `func (t *Trainer) ActiveProfile() (*ActiveProfile, bool)`
  - `func (t *Trainer) AcceptProfile(p *ActiveProfile)` (used by benches/tests)
  - Anti-thrash gating MUST use these constants (from feature branch):
    - `MinProfileBytes = 64<<20`
    - `MinProfileRecords = 250_000`
    - `MinProfileInterval = 10 * time.Minute`
    - `ProfileDriftThreshold = 0.07`
    - `ProfileImproveThresh = 0.02`

**Trainer behavior (normative)**
- Sample collection:
  - `Collect(value)` MUST copy at most `MaxRecordBytes` per sample (truncate).
  - `SampleStride` MUST be applied deterministically (collect every Nth record when `>1`).
  - Collection MUST be bounded by an internal queue; when full, samples are dropped (counters incremented).
- Training trigger:
  - When collected bytes ≥ `TrainBytes` AND collected records ≥ `MinRecords`, the trainer MUST:
    1) stop collecting
    2) build a dict and choose K (below)
    3) publish (or reject) a profile
    4) resume collecting only when explicitly re-armed (e.g., via `SignalDegraded`)
- Dict build:
  - Build the dict via `zstd.BuildDict(dictBytes, samples)` where `dictBytes == DictBytes`.
  - The trainer MUST recover from panics (return “no new profile” rather than crashing the process).
- K selection:
  - Run `ChooseKForDict(dict, samples)` and clamp to K ∈ {1..8}.
- Anti-thrash gating + acceptance:
  - Baseline = last accepted profile’s `TotalRatio` (if any).
  - The trainer MUST NOT publish a new profile unless:
    - time since last accept ≥ `MinProfileInterval`, AND
    - bytes since last accept ≥ `MinProfileBytes` OR records since last accept ≥ `MinProfileRecords`, AND
    - observed ratio drift ≥ `ProfileDriftThreshold` (drift signal is supplied via `SignalDegraded`)
  - Accept a new profile only if `new.TotalRatio <= baseline*(1-ProfileImproveThresh)`.
- Publication:
  - `ActiveProfile()` MUST return the latest accepted profile via `atomic.Value`.

**Tests (explicit)**
- `TestTrainer_NoPanic` (fuzz-like inputs; trainer recovers from panics).
- `TestChooseKForDict_SelectsBoundedK` (fixed samples; K in 1..8; deterministic).
- `TestTrainer_AntiThrashGate` (simulate drift; ensure accepts/rejects counters behave deterministically).

**Bench (explicit)**
- Add `BenchmarkChooseKForDict` to `TreeDB/internal/compression/trainer_test.go` that runs `ChooseKForDict` on fixed samples and reports allocations.

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/internal/compression -count=1`
- Add and run fuzz for safety:
  - Add `FuzzChooseKForDict` in `TreeDB/internal/compression/trainer_test.go`
  - Run: `go test ./TreeDB/internal/compression -run '^$' -fuzz FuzzChooseKForDict -fuzztime=10s -count=1`

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/internal/compression -run '^$' -bench BenchmarkChooseKForDict -count=5`
- Gates (hard numbers; fail if exceeded):
  - median `B/op <= 268435456` (256MiB)
  - median `allocs/op <= 20000`

**PR Writeup Notes**
- Include benchmark numbers and explain the fixed caps (`eval<=10000`, `decodeCostEstimate<=500`) that bound runtime.

---

### PR 4 — DictID plumbing (K=1) + grouped read foundation (no slab header change)

**Goal**
- Ship end-to-end DictID plumbing for **K=1 values** (A2.1) and lay the read-path foundation for grouped frames (writer comes in PR5).

**Locked scope (explicit)**
- No slab outer-header change.
- No async slab writer.
- No slab “zones” or boundary packing.

**Files (explicit)**
- Modify: `TreeDB/page/value_ptr_flags.go` (apply the bit layout in A3.2)
- Modify: `TreeDB/slab/compression.go` (add dict-aware encode/decode per A2.1)
- Modify: `TreeDB/slab/manager.go` (open `dict.db`, cache dict codecs, wire trainer/profile into write+read)
- Add: `TreeDB/slab/frame_group_v0.go` (encode/decode group body v0; read-only support only in this PR)
- Add: `TreeDB/slab/frame_group_v0_test.go`
- Add: `TreeDB/slab/dict_envelope_test.go`

**Implementation (normative)**
1) **Dict store wiring**
- `SlabManager` MUST open `dict.db` at `filepath.Join(dir, "dict.db")` using `internal/dictstore` (PR2).
- Add an in-memory cache:
  - dict bytes: LRU keyed by `dictID` (capacity 64)
  - decoder pools: LRU keyed by `dictID` (capacity 64; each entry is `*sync.Pool` of `*zstd.Decoder`)
  - encoder pools: LRU keyed by `dictID` (capacity 64; each entry is `*sync.Pool` of `*zstd.Encoder`)
  - **Pinned active dict (normative):** the currently active `profile.DictID` MUST be pinned and MUST NOT be evicted from any of the codec caches (dict bytes, decoder pools, encoder pools), even when LRU capacity is exceeded. If capacity pressure exists, evict non-active dicts first.
2) **Trainer/profile wiring**
- `SlabManager` MUST optionally create `internal/compression.Trainer` (PR3) when not read-only.
- On every `Append`/`AppendMany`, the raw (uncompressed) values MUST be offered to the trainer:
  - if not paused: always `Trainer.Collect(value)`
  - if paused: only collect when `pausedSampleStride` admits (PR1 knob)
- When PR1 triggers a pause due to degraded ratios, it MUST also call `Trainer.SignalDegraded(activeSlabID)`.
- `SlabManager` MUST periodically read `Trainer.ActiveProfile()` and, when a new profile is published:
  - persist the dict bytes to `dict.db` (dedup by `DictHash`) and obtain a stable `DictID`
  - publish the active profile to writers with `profile.DictID` set
3) **K=1 encode/decode with dict**
- Update the compression encode path so K=1 values can be compressed with a dict:
  - if active profile `DictID != 0`, encode using envelope v1 (A2.1) and zstd with that dict
  - otherwise fall back to legacy per-value compression
- Update decode path to support envelope v0 and v1:
  - v1 MUST look up `dictID` from `dict.db` (cached) and decode with the matching decoder pool
4) **Grouped read foundation**
- Add `frame_group_v0.go` parsing/decoding helpers (A3.3 caps), and wire reads so:
  - if `ValuePtrIsGrouped(ptr)`, `SlabManager.Read/ReadUnsafe` decode using `frame_group_v0` (writer in PR5)

**Tests (explicit)**
- `TreeDB/slab/dict_envelope_test.go`:
  - `TestCompressionEnvelopeV1_DictRoundTrip`:
    1) Build a small zstd dictionary via `zstd.BuildDict(...)` from synthetic samples.
    2) Append it to `dict.db`, obtain `dictID`.
    3) Encode a value with envelope v1 + dictID, then decode, and assert bytes match.
- `TreeDB/slab/frame_group_v0_test.go`:
  - `TestFrameGroupV0_RoundTrip` (manual grouped record body; read via grouped pointers)
  - `TestFrameGroupV0_Corruption` (invalid headers/offsets/truncation fail-closed)

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/slab -count=1`
- `go test -race ./TreeDB/slab -count=1`
- Add and run fuzz (fail-closed, no panics):
  - Add `FuzzCompressionEnvelopeV1` in `TreeDB/slab/dict_envelope_test.go`
  - Add `FuzzFrameGroupV0_Decode` in `TreeDB/slab/frame_group_v0_test.go`
  - Run:
    - `go test ./TreeDB/slab -run '^$' -fuzz FuzzCompressionEnvelopeV1 -fuzztime=10s -count=1`
    - `go test ./TreeDB/slab -run '^$' -fuzz FuzzFrameGroupV0_Decode -fuzztime=10s -count=1`

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Slab write/read overhead gates:
  - Run: `go test ./TreeDB/slab -run '^$' -bench 'BenchmarkSlab_ValueCompress_(Compressible|Mixed)' -count=5`
  - The compressible benchmark MUST include sub-benchmarks `dict_off` and `dict_on`:
    - `dict_on` MUST force a fixed `DictID` (do not rely on trainer timing in a benchmark)
  - Gates:
    - `dict_on` median `ns/op` MUST NOT regress by >10% vs `dict_off`.
    - `dict_on` MUST improve stored-bytes-per-record by ≥5% vs `dict_off` on the compressible dataset (printed by the benchmark; include in PR table).
- DB-level regression gate (dict feature off by default):
  - Run: `go test ./TreeDB/db -run '^$' -bench BenchmarkDB_Batch_100B -count=5`
  - median `ns/op` MUST NOT regress by >5% vs PR0 baseline.

**PR Writeup Notes**
- Include explicit byte diagrams for:
  - K=1 envelope v0/v1 (A2.1)
  - `ValuePtr.Length` bit layout (A3.2)
- Include a “fail-closed table”: each corruption class → expected error path.

---

### PR 5 — Micro-batched compression (K) for slab values (bounded, production-complete)

**Goal**
- Achieve near-streaming compression ratios while bounding point-read decode cost, using the grouped-frame approach above.

**Files (explicit)**
- Modify: `TreeDB/slab/manager.go` (write-path grouping + read-path decode integration)
- Modify: `TreeDB/compaction/compactor.go` (grouped record scanning + robust ptr matching)
- Add: `TreeDB/slab/grouped_appendmany_test.go`
- Add: `TreeDB/compaction/compactor_grouped_test.go`

**New knobs (explicit, with defaults)**
Add these to `slab.Options` in `TreeDB/slab/compression.go`:
- `CompressionGroupedMaxPayloadBytes int` (default `64 << 10`)
- `CompressionGroupedMaxKeyBytes int` (default `1 << 10`)
- `CompressionGroupedMaxValueBytes int` (default `4 << 10`)

**Write path integration (normative)**
1. Add `(*SlabManager).AppendManyGrouped(keys, values [][]byte, k int, dictID uint64) ([]page.ValuePtr, error)`.
2. `AppendManyGrouped` MUST:
   - group input sequentially into batches of size `k`
   - for each group:
     - if any row violates grouping caps, do not group that row (emit K=1 pointer for it)
     - build group body v0 with dictID
     - if `HeaderSize + len(body) > slab.MaxRecordSize`, deterministically reduce k until it fits, else fall back to K=1
     - write one slab record (`KeyLen=0`, `Value=body`) via `WriteBatch`
     - return pointers for each row with:
       - `Offset = recordOffset + 4`
       - `Length = ValuePtrMarkGrouped(ValuePtrMarkCompressed(recordLen), subIndex)`
3. K selection MUST use `internal/compression.ActiveProfile.K` and MUST clamp to `[1,8]`.
4. Dict selection MUST use `internal/compression.ActiveProfile.DictID` (0 means “no dict”).

**Read path integration (normative)**
- `SlabManager.Read` and `ReadUnsafe` MUST decode grouped pointers using A3.3.
- Decoder pools MUST be cached by `dictID` with a bounded LRU (size knob, default 64 dicts).

**Compaction correctness (normative; fixes existing compressed-pointer hazard)**
Update `TreeDB/compaction/compactor.go` so it never relies on raw struct equality for pointers:
1. Introduce `ptrMatches(a, b page.ValuePtr) bool` that matches:
   - `FileID`, `Offset`, `ValuePtrRecordLength`, `ValuePtrIsGrouped`, and `ValuePtrSubIndex`
   - ignores only the `compressed` bit
2. When checking liveness from a snapshot entry:
   - if entry is a pointer, use `ptrMatches(entry.ValuePtr, oldPtr)`
3. When building `liveSet`:
   - store “robust pointers” with the compressed bit cleared:
     - `p := ptr; p.Length = page.ValuePtrRecordLength(ptr)` plus re-apply grouped/subIndex bits (if present)
   - lookups during scanning MUST apply the same normalization so membership checks work regardless of compression bit.
4. Grouped record scanning:
   - detect `KeyLen==0` and group body v0 header
   - decompress once per record
   - iterate rows 0..k-1, parse `keyBytes`/`valBytes`
   - construct `oldPtr` for that row (grouped+subIndex set; compressed bit cleared for robust checks)
   - liveness check by key + `ptrMatches`

**Tests (explicit)**
- `TreeDB/slab/grouped_appendmany_test.go`:
  - write N small key/value pairs with profile K=3 and verify:
    - some pointers are grouped
    - point reads return correct values
- `TreeDB/compaction/compactor_grouped_test.go`:
  - write grouped frames, run slab compaction, verify values still readable
  - include a case where values are compressed (ensure compactor does not drop live records due to compressed flag)
  - `TestCompactor_Grouped_PartialTombstone`:
    1) Write `[A,B,C]` in a grouped frame (K=3).
    2) Delete `B` (tombstone).
    3) Run compaction and verify:
       - `A` and `C` remain readable with correct values.
       - `B` is not present.
       - The output is well-formed (no panic; grouped decoding still works for any grouped pointers produced).

**Bench (explicit)**
- Add `BenchmarkSlabGrouped` to `TreeDB/slab/grouped_bench_test.go` (port/adapt from `feature/slab-optimizations:TreeDB/slab/manager_grouped_bench_test.go`):
  - run in three modes: structured, mixed, random
  - report: bytes/record, grouped vs legacy pointer counts, ns/op for writes and point reads

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/slab -count=1`
- `go test ./TreeDB/compaction -count=1`
- `go test -race ./TreeDB/slab -count=1`
- `go test -race ./TreeDB/compaction -count=1`
- Add and run fuzz for grouped parsing/ptr flags:
  - Add `FuzzFrameGroupV0_Decode` (if not already added in PR4) and extend it to exercise all `subIndex` values.
  - Run: `go test ./TreeDB/slab -run '^$' -fuzz FuzzFrameGroupV0_Decode -fuzztime=10s -count=1`

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/slab -run '^$' -bench BenchmarkSlabGrouped -count=5`
  - `go test ./TreeDB/db -run '^$' -bench BenchmarkDB_Batch_100B -count=5`
- `BenchmarkSlabGrouped` MUST include:
  - `structured/K1` and `structured/K3` sub-benchmarks (or `K=1` vs `K=ActiveProfile.K`, but include `K=1` baseline).
  - `random/K1` and `random/K3` sub-benchmarks.
- Gates:
  - Structured:
    - `bytes/rec` for `K3` MUST improve by ≥5% vs `K1`.
    - point-read median `ns/op` MUST NOT regress by >15% vs `K1`.
  - Random (incompressible):
    - `bytes/rec` MUST NOT get worse by >2% for `K3` vs `K1`.
    - median write `ns/op` MUST NOT regress by >5% for `K3` vs `K1`.
  - DB-level:
    - `BenchmarkDB_Batch_100B` median `ns/op` MUST NOT regress by >5% vs PR0 baseline.

**PR Writeup Notes**
- Include an explicit “ptr identity” explanation:
  - why compressed-bit must be ignored for matching
  - why grouped+subIndex must be included
- Include the benchmark table (K=1 vs K=3) and the grouped-vs-legacy pointer counts.

---

### PR 6 — Combined WAL+slab protocol (synchronous first cut)

**Goal**
- Make the durability ordering explicit and enforced with tests.

**Files (explicit)**
- Modify: `TreeDB/caching/db.go` (enforce ordering for cached mode)
- Modify: `TreeDB/internal/wal/wal.go` (length caps before allocation; fail-closed)
- Modify: `TreeDB/slab/manager.go` (expose “written vs durable” checkpoints where needed)
- Add: `TreeDB/caching/durability_order_test.go`
- Add: `TreeDB/internal/wal/wal_hardening_test.go`

**Deliverables (explicit)**
1) A **normative ordering contract** is enforced by code:
   - `slab data durable` happens-before `WAL durable` happens-before `index durable` happens-before `WriteSync ack`
2) Explicit “written vs durable” state exists in the write path:
   - written = appended to file (page cache)
   - durable = crossed `fsync`
3) WAL reader is OOM-safe:
   - length fields are validated and capped before any allocation
   - CRC/parse failures are clean errors (no panic)

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/internal/wal -count=1`
- `go test ./TreeDB/caching -count=1`
- `go test -race ./TreeDB/caching -count=1`
- Fuzz WAL reader hardening:
  - `go test ./TreeDB/internal/wal -run '^$' -fuzz FuzzWALReader -fuzztime=10s -count=1`
- Ordering test requirements:
  - `TreeDB/caching/durability_order_test.go` MUST use latches/hooks (no sleeps)
  - test MUST assert the happens-before chain by observing recorded events in order

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/db -run '^$' -bench BenchmarkDB_Batch_100B -count=5`
  - `go test ./TreeDB/db -run '^$' -bench BenchmarkWriteParallel -count=5`
- Gates:
  - Both benchmarks MUST NOT regress by >5% vs PR0 baseline.

**PR Writeup Notes**
- Include a 4-step “ordering timeline” diagram and show exactly where the code enforces each edge.

---

### PR 7 — Index.db deep work (v1): Columnar leaf + search accelerators (experimental)

**Goal**
- Implement deeper `index.db` improvements based on `TreeDB/btree_optimization.md`, starting with leaf layout and search accelerators.

**Files (explicit)**
- Modify: `TreeDB/db/db.go` (add option `ExperimentalColumnarLeaves bool`, default false)
- Modify: `TreeDB/node/node.go`:
  - add `leafColumnarFlag uint16 = 0x4000`
  - update `pageTypeMask` to exclude `leafColumnarFlag`
  - add `func (n *Node) leafColumnar() bool`
  - update `SetType` to preserve all layout flags (`leafPrefixCompressedFlag`, `leafColumnarFlag`, and any future node-layout flags)
- Add: `TreeDB/node/leaf_columnar.go` (encode/decode/search for leaf columnar v1; see Appendix D)
- Add: `TreeDB/node/leaf_columnar_test.go`
- Add: `TreeDB/node/leaf_columnar_bench_test.go`
- Modify: `TreeDB/node/builder.go` (emit columnar leaves when `ExperimentalColumnarLeaves==true`)

**Format (normative)**
- Columnar leaf layout is **new DBs only** and MUST match “Appendix D — Columnar Leaf v1”.
- Legacy leaf pages MUST remain decodable unchanged when the option is off.

**Search accelerator (explicit)**
- Implement a “restart binary search + block scan” algorithm for columnar leaves:
  1) Binary search only the restart keys (`i % restartInterval == 0`).
  2) Scan forward within the chosen block (max `restartInterval` entries).
  3) Use `fingerprints[i]` to skip full key reconstruction unless fingerprint matches.
  - The block scan MUST be a simple linear scan (max `restartInterval` = 16 keys); do NOT binary search inside the block.
  - During the scan, check `fingerprints[i]` first and only reconstruct/compare the full key on fingerprint match (tight loop; no heap allocs).

**Bench harness (explicit)**
- Add `BenchmarkLeafColumnar_Find` in `TreeDB/node/leaf_columnar_bench_test.go`:
  - compares legacy vs columnar leaf search on:
    - long keys (iavl-like)
    - short keys
  - reports `ns/op` and `allocs/op`.

**Tests (explicit)**
- Add to `TreeDB/node/leaf_columnar_test.go`:
  - `TestLeafColumnar_RoundTrip` (encode→decode→lookup; values match).
  - `TestLeafColumnar_IteratorOrder` (iterator yields sorted keys; matches legacy).
  - `TestLeafColumnar_Corruption` (invalid offsets/lengths fail-closed with `ErrCorruptedNode`).

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/node -count=1`
- Add and run fuzz for the new decoder:
  - Add `FuzzLeafColumnar_Decode` in `TreeDB/node/leaf_columnar_test.go`
  - Run: `go test ./TreeDB/node -run '^$' -fuzz FuzzLeafColumnar_Decode -fuzztime=10s -count=1`

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/node -run '^$' -bench BenchmarkLeafColumnar_Find -count=5`
  - `go test ./TreeDB/db -run '^$' -bench BenchmarkDB_Get_100B -count=5`
- `BenchmarkLeafColumnar_Find` MUST include paired sub-benchmarks `legacy` and `columnar` for both datasets:
  - `long_keys/legacy`, `long_keys/columnar`
  - `short_keys/legacy`, `short_keys/columnar`
- Gates:
  - Long keys: `columnar` median `ns/op` MUST improve by ≥20% vs `legacy`.
  - Short keys: `columnar` median `ns/op` MUST NOT regress by >5% vs `legacy`.
  - `allocs/op` MUST NOT increase by >10% for either dataset.
  - DB-level (flag off by default): `BenchmarkDB_Get_100B` MUST NOT regress by >5% vs PR0 baseline.

**PR Writeup Notes**
- Include the Appendix D layout diagram excerpt and a short explanation of the restart-search algorithm + fingerprint filter.

---

### PR 8 — Index.db deep work (v2): Partitioned pager + manifest + per-partition maintenance

**Goal**
- Replace the confusing “range vacuum” concept with a coherent partitioned-index design:
  - range partitioning is implemented as **multiple index files** + a **manifest**
  - maintenance (vacuum/compaction) is performed per-partition

**Files (explicit)**
- Add: `docs/INDEX_PARTITIONING.md` (this PR writes the canonical format + routing spec; no TODO placeholders)
- Add: `TreeDB/cmd/index_partition/main.go` (offline tool)
- Add: `TreeDB/pager/partitioned.go` (read-only partitioned pager; used by the tool and tests)
- Add: `TreeDB/pager/partitioned_test.go`

**Manifest schema (normative)**
- The manifest format MUST be specified in full in `docs/INDEX_PARTITIONING.md`.
- The manifest MUST define:
  - `version = 1`
  - `partition_count` (fixed at creation)
  - `routing = "nibble"` (partition = `key[0] >> 4`, empty key → 0)
  - `part[i].file = "index.p%02x.db"` (hex 00..0f)
  - `part[i].root_page_id u64` (read-only prototype stores this; full integration is PR9)
  - `encoding = "json"` (UTF-8 JSON; fail-closed on parse errors)
  - PageID encoding within partitioned index files:
    - `partition = pageID >> 48` (0..partition_count-1)
    - `localPageID = pageID & ((1<<48)-1)`
    - `PartitionedPager` routes reads via `(partition, localPageID)`
  - an atomic swap protocol: write `index.manifest.new`, `fsync`, then rename over `index.manifest`, `fsync dir`

**Offline prototype (explicit)**
`TreeDB/cmd/index_partition` MUST:
1) Open an existing TreeDB directory in read-only mode.
2) Iterate all keys in sorted order using snapshots.
3) Route each key to a partition via the nibble rule.
4) Build one index file per partition using the existing `bulk.Builder` (no slab/WAL changes).
5) Emit `index.manifest` referencing the new partition index files.

**Important scope limit (explicit)**
- This PR does NOT make TreeDB *use* the partitioned index for live reads/writes yet.
- It produces a validated artifact + toolchain so PR9 can flip the runtime switch safely.

**Tests (explicit)**
- `TreeDB/pager/partitioned_test.go`:
  - `TestPartitionedPager_RoutesByPageID` (create 2 pager files + manifest; `Get` hits correct file)
  - `TestPartitionedPager_BadManifestFailsClosed` (bad JSON/unknown version → error)
- `TreeDB/cmd/index_partition` is validated by an integration test in `TreeDB/pager/partitioned_test.go`:
  - create a tiny DB with known keys
  - run the tool (as a `go run` subprocess)
  - open the output partition files with the partitioned pager and verify all keys are found

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/pager -count=1`
- `go test ./TreeDB/pager -run TestPartitionedPager_ -count=1`
- Integration test MUST run the tool and validate output partitions:
  - `go test ./TreeDB/pager -run TestPartitionedPager_ToolIntegration -count=1`

**Performance Gates (must pass)**
- Add to `TreeDB/pager/partitioned_test.go`:
  - `BenchmarkPager_Get`
  - `BenchmarkPartitionedPager_Get`
- Benchmark protocol: follow G1.
- Run:
  - `go test ./TreeDB/pager -run '^$' -bench 'Benchmark(Pager|PartitionedPager)_Get' -count=5`
- Gates:
  - `BenchmarkPartitionedPager_Get` median `ns/op` MUST NOT regress by >10% vs `BenchmarkPager_Get`.
  - Both benchmarks MUST report `allocs/op == 0`.

**PR Writeup Notes**
- Include Appendix E excerpt (PageID encoding + manifest schema) and the atomic swap procedure.

---

### PR 9 — Index.db deep work (v3): Internal-node fanout + locality (separator/LCP/relative IDs)

**Goal**
- Implement the internal-node improvements from `TreeDB/btree_optimization.md` that directly increase fanout and reduce depth.

**Files (explicit)**
- Modify: `TreeDB/node/node.go`:
  - add `internalV1Flag uint16 = 0x2000`
  - update `pageTypeMask` to exclude `internalV1Flag`
  - add `func (n *Node) internalV1() bool`
- Add: `TreeDB/node/internal_v1.go` (encode/decode/search for internal v1; see Appendix F)
- Modify: `TreeDB/node/internal.go` (route `SearchInternal`/`GetInternalChildID` to v1 when `internalV1Flag` is set; keep legacy decoder for existing DBs)
- Modify: `TreeDB/node/split.go` and `TreeDB/zipper/zipper.go` to:
  - compute shortest separators
  - compute internal-node global LCP
- Add: `TreeDB/node/internal_lcp_test.go`

**Internal-node encoding changes (normative)**
Internal-node v1 MUST match “Appendix F — Internal Node v1 (normative)”.

Implement internal-node v1 so that:
1) One `globalPrefix` is stored once per node (LCP of all entry keys).
2) Entry keys are stored as suffixes (without `globalPrefix`).
3) Child pointers are stored in a pointer-first layout:
   - `[childPointers][keyOffsets][keySuffixBlob]`
4) Search compares keys without allocating and preserves legacy ordering semantics.

**Relative page IDs (explicit; only when partitioned index is enabled later)**
- When all child PageIDs share a partition (see Appendix E PageID encoding), child page IDs MAY be stored as `u32` deltas from `basePageID = min(childPageIDs)` stored once per node.
- When partitioning is not enabled, store full `u64` page IDs.

**Tests (explicit)**
- `TreeDB/node/internal_lcp_test.go`:
  - `TestInternalNodeV1_GlobalLCP_RoundTrip` (encode→decode; separators compare equal)
  - `TestInternalNodeV1_ShortestSeparator` (separator correctness invariants)
  - `TestInternalNodeV1_SavesSpaceOnLongSeparators`:
    - build a synthetic internal node with separators sharing a long common prefix
    - encode using legacy layout and v1 layout
    - assert `len(v1Encoded) <= 0.85 * len(legacyEncoded)` (≥15% shrink)
  - `FuzzInternalNodeV1_Decode` (fail-closed; no panics; bounds checks)
- Add a DB-level depth regression test:
  - `TreeDB/db/long_key_depth_test.go:TestLongKeyDepthBounded`:
    - insert N keys with long shared prefixes
    - assert max depth ≤ current configured limit

**Correctness Gates (must pass)**
- `go test ./... -count=1`
- `go test ./TreeDB/node -count=1`
- `go test ./TreeDB/db -run TestLongKeyDepthBounded -count=1`
- Run fuzz:
  - `go test ./TreeDB/node -run '^$' -fuzz FuzzInternalNodeV1_Decode -fuzztime=10s -count=1`

**Performance Gates (must pass)**
- Benchmark protocol: follow G1.
- Structural fanout gate (test-enforced):
  - `TestInternalNodeV1_SavesSpaceOnLongSeparators` MUST pass (≥15% shrink vs legacy on the synthetic dataset).
- DB-level regression gate:
  - `go test ./TreeDB/db -run '^$' -bench 'BenchmarkDB_Get_100B|BenchmarkScan' -count=5`
  - median `ns/op` MUST NOT regress by >5% vs PR0 baseline.

**PR Writeup Notes**
- Include:
  - a short “why depth decreases” explanation (fanout math driven by byte savings)
  - depth measurement on the long-key workload (before/after) using the exact same command and dataset

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

---

## Appendix D — Columnar Leaf v1 (normative)

This format applies to **leaf pages only** when `leafColumnarFlag` is set in `PageHeader.Flags`.

### D1) Leaf Body Layout

All integers are little-endian.

Let `count = PageHeader.Count` and `hdr = NodeHeaderSize` (16 bytes).

At offset `hdr`:
- `version u8 = 1`
- `restartInterval u8 = 16`
- `reserved u16 = 0`

Then, in order:
- `fingerprints[count] u8`
- `keyOffsets[count+1] u16` (offsets into `keySuffixBlob`, relative to the start of that blob)
- `prefixLens[count] u16`
- `valueOffsets[count+1] u16` (offsets into `valueBlob`, relative to the start of that blob)
- `entryFlags[count] u8` (same semantics as legacy leaf flags: `FlagPointer`, `FlagTombstone`, inline otherwise)
- `pad u8` (present only if needed so the next blob starts at an even offset; pad byte MUST be 0)
- `keySuffixBlob[keyOffsets[count]] bytes`
- `valueBlob[valueOffsets[count]] bytes`

### D2) Key Reconstruction Rules

- `keyOffsets[0]` MUST equal 0.
- `keyOffsets` MUST be monotonic non-decreasing.
- Each entry’s suffix bytes are `suffix = keySuffixBlob[keyOffsets[i] : keyOffsets[i+1]]`.
- If `i % restartInterval == 0`, then `prefixLens[i]` MUST equal 0 and the full key is `suffix`.
- Otherwise:
  - `prefixLens[i]` MUST be `<= len(prevKey)` (where `prevKey` is the reconstructed key at `i-1` within the same restart block).
  - reconstructed key is `prevKey[:prefixLens[i]] + suffix`.

### D3) Fingerprint Rules

- `fingerprints[i]` MUST equal `uint8(xxhash64(key) >> 56)` where `key` is the reconstructed key at index `i`.
- Search MUST use fingerprints as an accelerator only; ordering comparisons MUST use full keys.

### D4) Value Rules

- `valueOffsets[0]` MUST equal 0.
- `valueOffsets` MUST be monotonic non-decreasing.
- For entry `i`, `valueBytes = valueBlob[valueOffsets[i] : valueOffsets[i+1]]`.
- Tombstones:
  - if `entryFlags[i] & FlagTombstone != 0`, then `len(valueBytes)` MUST be 0.
- Pointers:
  - if `entryFlags[i] & FlagPointer != 0`, then `len(valueBytes)` MUST equal `page.ValuePtrSize` and `page.DecodeValuePtr(valueBytes)` is the pointer.
- Inline:
  - otherwise `valueBytes` is the inline value.

### D5) Fail-Closed Corruption Handling

Any violation of the layout rules, bounds checks, or monotonic offset constraints MUST return `node.ErrCorruptedNode` (or a new dedicated error) and MUST NOT panic or allocate based on untrusted lengths.

---

## Appendix E — Partitioned Index Manifest v1 (normative)

This appendix defines the minimum required interoperability for PR8’s offline artifact.

### E1) PageID Encoding

Partitioned indexes use a globally-unique PageID encoding:
- `partition = pageID >> 48`
- `localPageID = pageID & ((1<<48)-1)`

`PartitionedPager` routes page reads/writes by selecting the underlying file for `partition`, and using `localPageID` as the page index inside that file.

### E2) Manifest Encoding

- Filename: `index.manifest` (UTF-8 JSON).
- Atomic swap: write `index.manifest.new`, `fsync`, rename over `index.manifest`, `fsync directory`.

### E3) Manifest Schema (JSON)

Required fields:
```json
{
  "version": 1,
  "partition_count": 16,
  "routing": "nibble",
  "page_id_partition_shift": 48,
  "partitions": [
    { "id": 0, "file": "index.p00.db", "root_page_id": 0 },
    { "id": 1, "file": "index.p01.db", "root_page_id": 0 }
  ]
}
```

Routing:
- `partition = key[0] >> 4` (empty key → 0)

Fail-closed requirements:
- Unknown `version` MUST error.
- Missing required fields MUST error.
- `partition_count` MUST equal `len(partitions)` and be in `[1,256]`.
- `id` MUST be unique and in `[0, partition_count)`.

---

## Appendix F — Internal Node v1 (normative)

This format applies to **internal pages only** when `internalV1Flag` is set in `PageHeader.Flags`.

### F1) Semantics (what the keys mean)

Internal nodes store `count = PageHeader.Count` entries, each representing:
- `startKey[i]` = the minimum key routed to `childPageID[i]` (a “child start key” / fence key)
- `childPageID[i]` = the child page to descend into

Keys are in strictly increasing order by `startKey`.
The first entry for a root-level internal node is typically:
- `startKey[0] = []byte{}` (empty key = “global minimum”)

Search semantics MUST match legacy `SearchInternal`:
- Find the largest `i` such that `startKey[i] <= targetKey`.
- If `targetKey < startKey[0]`, return `i=0`.
- Descend to `childPageID[i]`.

### F2) Body Layout

All integers are little-endian.

Let `count = PageHeader.Count` and `hdr = NodeHeaderSize` (16 bytes).

At offset `hdr`:
- `version u8 = 1`
- `childEncoding u8`:
  - `0` = `u64` child pointers
  - `1` = `basePageID + u32 delta` child pointers (see F4)
- `globalPrefixLen u16`
- `reserved u32 = 0`
- `basePageID u64` (meaningful only when `childEncoding==1`; otherwise MUST be 0)

Then, in order:
- `globalPrefixBytes[globalPrefixLen] bytes`
- `childPointers[count]`:
  - if `childEncoding==0`: `childPageIDs[count] u64`
  - if `childEncoding==1`: `childDeltas[count] u32`
- `keyOffsets[count+1] u16` (offsets into `keySuffixBlob`, relative to the start of that blob)
- `keySuffixBlob[keyOffsets[count]] bytes`

### F3) Key Reconstruction Rules

- `keyOffsets[0]` MUST equal 0.
- `keyOffsets` MUST be monotonic non-decreasing.
- For each entry `i`:
  - `suffix = keySuffixBlob[keyOffsets[i] : keyOffsets[i+1]]`
  - reconstructed `startKey[i] = globalPrefixBytes + suffix`

### F4) Child Pointer Rules (base+delta encoding)

When `childEncoding==1`, the node stores `childPageID[i]` as:
- `childPageID[i] = basePageID + uint64(childDeltas[i])`

Write-time requirements (fail closed if violated):
- All `childPageID[i]` MUST share the same partition:
  - `(childPageID[i] >> 48) == (childPageID[0] >> 48)` for all i.
- `basePageID` MUST equal `min(childPageID[i])`.
- For all i: `childPageID[i] >= basePageID`.
- For all i: `childPageID[i]-basePageID <= math.MaxUint32`.

If any requirement fails, the writer MUST use `childEncoding==0` instead.
This includes the `math.MaxUint32` delta overflow case even when all partition IDs match.

### F5) Fail-Closed Corruption Handling

Any violation of layout rules, bounds checks, `childEncoding` values, or monotonic offset constraints MUST return `node.ErrCorruptedNode` (or a new dedicated error) and MUST NOT panic or allocate based on untrusted lengths.
**Implementation note (explicit)**
- When making slab data durable, prefer a “data-only” sync when supported (e.g., `fdatasync` on Unix) to avoid unnecessary metadata work; fall back to `file.Sync()` where needed. The happens-before contract above must remain true regardless of the syscall used.
