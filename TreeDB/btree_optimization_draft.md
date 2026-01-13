# TreeDB: B-Tree / Index Optimization Draft (Best-of-Both)

This draft reconciles and supersedes two drifting plans:

- `TreeDB/btree_optimization.md` (index-only)
- `TreeDB/optimization_plan.md` Part I (B-Tree only; ignore Part II slab work)

It keeps the “north-star” ideas from both, but adds missing correctness rules and
reframes the roadmap around TreeDB’s current architecture (COW + MVCC snapshots).

Status: draft / planning doc (not a committed format contract).

---

## 0. Scope, Assumptions, Non‑Goals

**In scope**
- Optimizations to the on-disk / in-memory representation of the **index** (`index.db`), i.e. page layouts and search accelerators.
- Changes that preserve TreeDB’s current durability and snapshot semantics by default.

**Assumptions about current TreeDB**
- TreeDB is a **copy-on-write** B+Tree: writers build new pages; readers see a stable snapshot.
- Pages are currently small (today: 4KB pages in `index.db`), and there is existing MVCC/lifecycle machinery for safe reclamation.

**Non-goals (for near-term “optimization” work)**
- In-place B-tree updates with lock-coupling. This is a different engine design and must be treated as an architectural fork.
- Any slab/value-log changes (explicitly out of scope for this draft).

---

## 1. Compare / Contrast (What Drifted)

### What `btree_optimization.md` has that `optimization_plan.md` Part I lacks
- A slightly more “productized” narrative around *why* the leaf clustering + partitioning proposal matters (statesync, parallel maintenance).

### What `optimization_plan.md` Part I adds (and we should keep)
- **Explicit correctness rules**:
  - Bloom filters are **hints only**; they must never introduce false negatives.
  - Filter metadata must be updated atomically with page images (or be rebuildable and validated by existing CRC/integrity checks).
  - Relative page IDs require a stable remapping strategy during partition compaction/splits; pointers must not break across rewrites.
- More explicit acknowledgement that **page size is a format decision**, and some math assumes 16KB (while TreeDB currently uses 4KB).

### “Concept drift” worth correcting
- **Optimistic lock coupling (OLFIT)** is mostly irrelevant to TreeDB’s current COW design (readers already avoid concurrent in-place mutations).
  - Keep it as “future exploration” only if we intentionally pursue an in-place index variant.
- **Snapshot-pinned epoch reclamation** is already close to TreeDB’s lifecycle model; the plan should align to existing mechanisms rather than propose a second one.

---

## 2. Current State Analysis (Shared)

TreeDB’s current B-Tree key encoding uses **incremental front-coding** with restart points.

### Limitations (shared)
- **Serial reconstruction**: decoding keys inside a page is inherently sequential.
- **Binary search bottleneck**: reconstruction work is repeated across comparisons.
- **Interleaved layout**: mixing key bytes and value/pointer payloads hurts cache locality during search.
- **Fixed per-entry overhead**: a fixed header per entry limits density.

---

## 3. Design Goals (Shared, With Measurements)

1. **Parallelizable / order-independent lookup within a page**
   - Reduce “decode work per comparison” in point reads.
2. **Columnar locality**
   - Search should touch primarily a contiguous key structure (not values/pointers).
3. **Higher density**
   - More keys per page → lower tree height → fewer page touches per read.
4. **SIMD-friendly structure**
   - Make it easy to reject non-matching candidates quickly (fingerprints) before full key compare.

**Primary metrics**
- Point lookup: `ns/op`, `allocs/op`, page touches (leaf + internal), bytes read.
- Range scan: throughput + CPU per returned KV.
- Index size: bytes per entry (and growth under churn).

---

## 4. Design Rules (Correctness / Recovery)

These are mandatory constraints for any proposed optimization.

### 4.1 Bloom filters and fingerprints are hints only
- They must never cause TreeDB to return “not found” for an existing key.
- On mismatch / missing / corrupt metadata, we must fall back to full page search.

### 4.2 Atomicity & validation
- Any per-page metadata stored outside the canonical page image must be:
  - updated atomically with page writes, or
  - rebuildable from the page image during recovery/vacuum, and
  - validated by existing integrity mechanisms (page CRC, format versioning).

### 4.3 Stable pointer semantics
- Any scheme using relative IDs, partitioned files, or remapping must guarantee:
  - stable resolution for all pointers reachable from any live root, and
  - crash-safe recovery (no “half-swapped” manifests that orphan pages).

### 4.4 Compatibility strategy
- Prefer “new page format versions” + vacuum rewrite as the migration path.
- A “read old, write new” strategy should be explicit if adopted.

---

## 5. Proposed Roadmap (Best-of-Both, Reframed)

This is structured as incremental steps that can land independently.

### Phase 0: Instrumentation / Baselines (non-breaking)
- Add microbenchmarks for:
  - leaf search (key compare count, decode bytes touched),
  - internal node search,
  - scan iteration.
- Add pprof capture helpers for those benches.
- Define a repeatable dataset representative of Cosmos workloads (key shapes, prefix patterns, value sizes).

### Phase 1: Columnar leaf layout (within current page size)
**Goal**: Improve cache locality and reduce reconstruction overhead without changing higher-level tree logic.

Proposed leaf page structure (conceptual):
- Header (format version, counts, offsets)
- Key metadata column:
  - restart array or prefix-offset table
  - per-key offsets into key suffix blob
  - optional 1-byte fingerprint strip (see Phase 2)
- Key suffix blob (packed bytes)
- Value/pointer column (packed fixed-size structs)

Notes:
- This keeps the “columnar keys then values” idea from both plans, but does not assume 16KB pages.
- This is compatible with TreeDB’s COW zipper: page rewrite just emits the new format.

### Phase 2: SIMD-friendly fingerprints (leaf-local)
**Goal**: quickly reject most candidates during binary search or scan.

- Add a contiguous “fingerprint strip” (1 byte per key).
- Fingerprint must be a hint (no false negatives). If fingerprint matches, still do full compare.
- Use architecture-appropriate SIMD where available, but always provide scalar fallback.

### Phase 3: Internal node fan-out optimization (density)
**Goal**: reduce internal height and page touches.

- Ensure “shortest separator” splits are consistently used and documented as a *separator*, not necessarily an existing key.
- Add internal-node global LCP (store prefix once per node).
- Switch internal nodes to a pointer-first columnar layout:
  - `[child pointers][key offsets][key suffixes]`

### Phase 4: Pager-side hints (Bloom filters)
**Goal**: reduce CPU and/or page touches for negative lookups (missing keys).

Two viable storage strategies:
1. **In-page bloom** (part of the leaf image):
   - simplest correctness story; automatically COW’d and CRC’d.
2. **Pager metadata bloom** (sidecar):
   - requires explicit atomicity + rebuild rules (see §4.2).

Bloom filters should be leaf-only. Internal node lookups are already bounded and small.

### Phase 5 (Large / format-shift): Clustered leaves + partitioned index files
This is the “north-star” proposal from both docs, but it’s a major format change.

**Zonal leaf clusters (“mega pages”)**
- Treat clusters as a *physical container* for many logical leaf pages, primarily to improve locality and enable shared compression context.
- Prefer a **cold-tier format** produced by vacuum/compaction (not the hot write path) to avoid rewrite amplification under churn.
- Use a shared **cluster prefix** (global LCP) and (optionally) a shared **cluster dictionary** for compressing leaf key material.
- Compress **keys only** (key column / suffix blob). Keep the value/pointer column uncompressed (or lightly encoded) so reads can jump directly to pointers after key search.
- Cluster layout should explicitly include:
  - cluster header + version + CRC
  - optional cluster prefix bytes
  - optional fixed-size dict bytes + dict CRC
  - per-leaf offset table: `leafID -> (keyBlockOffset, keyBlockLen, ptrColOffset, ptrColLen)`
  - compressed key blocks per leaf + uncompressed pointer columns per leaf
- If we ever change logical page size (e.g., 4KB → 16KB), update the math; do not bake specific page sizes into the design.

**Index vs slab compression**
- Do **not** attempt to share the same dictionary bytes between index keys and slab values (different distributions; coupling increases complexity).
- It *is* desirable to share infrastructure: sampling, gating/anti-thrash policy, stats/telemetry, cache plumbing, and “profile selection” patterns, while keeping dict training domains independent.

**Range-partitioned `index.db`**
- Replace a single monolith with partitions, tracked by a manifest: `KeyRange -> FilePath`.
- Enables parallel maintenance and “attach ranges” for state sync.

**Partitioned pager**
- Page IDs encode partition identity (e.g., high bits route to a file).
- Requires a strict remap/relocation protocol during partition rebuilds.

### Phase X (Architectural fork): Optimistic lock coupling (OLFIT)
Only consider if we pursue an in-place index variant.
It is not a near-term optimization for the current COW tree.

---

## 6. Implementation Plan (Codex-ready)

This section turns the roadmap into an implementable sequence. It is written to
be actionable for an implementation agent: what to change, where, and how to
validate it without getting lost in the speculative “north-star” items.

### 6.1 Architecture Map (Where Changes Land)

Key packages involved in index/page layout work:

- `TreeDB/page`: page header, CRC rules, page size constants.
- `TreeDB/node`: leaf/internal encoding/decoding, search routines, builders.
- `TreeDB/tree`: search + COW rewrite orchestration (zipper merge uses `node`).
- `TreeDB/pager`: read/write pages to `index.db` (mmap + chunk growth).
- `TreeDB/db`: vacuum/rebuild, stats, integration surfaces.

For Phase 1–4, the work is mostly confined to `TreeDB/node` + a small amount of
flag/version plumbing in `TreeDB/page` and any call sites that interpret node
flags.

### 6.2 Flowcharts (End-to-End)

#### 6.2.1 Point read (`DB.Get`) flow (today; remains true after Phase 1–4)

```
DB.Get(key)
  -> AcquireSnapshot (pins root + slabs + vlogs; reader is lock-free)
  -> tree.Search(root, key)
       -> pager.ReadPage(pageID)          (mmap / pread)
       -> node.NewNodeView(pageBytes)
       -> node.SearchInternal / SearchLeaf
            -> leaf decode/compare path (this is where Phase 1–2 land)
  -> if leaf entry is pointer:
       -> resolve ValuePtr (slab/vlog)
  -> return value (copy)
```

Phase impact:
- Phase 1 changes leaf decode/search hot loops (less interleaving, less decode work).
- Phase 2 adds a fingerprint hint to cut full compares.
- Phase 3 changes internal-node decode/search.
- Phase 4 adds optional bloom “skip hint” before leaf decode.

#### 6.2.2 Write (`DB.Set`/batch commit) flow (where new formats get emitted)

```
DB.Set / Batch.Write
  -> writer lock
  -> zipper merge builds new path (COW)
       -> node.Builder emits new pages (leaf/internal encoding lives here)
       -> pager.Alloc + pager.WritePage for newly created pages
  -> update meta pages (new root + commit seq)
  -> release writer lock
```

Key consequence for compatibility:
- Once we teach readers to understand multiple leaf formats, writers can begin
  emitting the new format via COW without rewriting the entire DB in one shot.

### 6.3 Compatibility & Migration Strategy (Recommended)

We want “read old pages; write new pages” with a clean upgrade path.

**Mechanism**
- Introduce a **leaf format flag/version** that is:
  - readable from the canonical page image (so CRC already covers it),
  - cheap to branch on in hot paths.
- Readers decode based on `(PageType == Leaf) + (format flag/version bits)`.
- Writers emit the new format for newly-created leaf pages (COW rewrite).

**Upgrade story**
- Existing DB: mixed pages will exist during normal churn; the tree remains valid.
- Full conversion: run an index vacuum/rebuild to rewrite all reachable pages into the newest format.

**Hard rule**
- New per-page hints (fingerprints/bloom) must never introduce false negatives.
- Unknown/invalid flags must cause a safe fallback (decode as legacy or fail fast with checksum/format error, depending on strictness).

### 6.4 Phase 0: Instrumentation / Baselines (Implementation Checklist)

**Goal**: lock down measurements so we can prove each phase helps.

**Add**
- Microbenchmarks focused on node search/compare cost, not end-to-end DB noise:
  - `BenchmarkLeafSearch_*` (existing leaf format vs new leaf format)
  - `BenchmarkInternalSearch_*`
  - `BenchmarkRangeScan_*` (iterator stepping through leaf keys)
- Metrics to report:
  - comparisons per lookup,
  - bytes touched for key material,
  - allocations per op.

**Where**
- Prefer `TreeDB/node/*_bench_test.go` (bench node logic directly).
- Optionally add one end-to-end gate bench in `TreeDB/bench_*` that opens a DB
  and does many Gets.

**Acceptance**
- A committed baseline (in comments or benchmark logs) so regressions are detectable.

### 6.5 Phase 1: Columnar Leaf Layout (Detailed Plan)

**Goal**: keep search touching “keys first”, and avoid interleaving pointer/value bytes in the cacheline path.

#### 6.5.1 Decide the format indicator

Recommended: add a **new flag bit** in the existing 16-bit page header `Flags`
that is meaningful only for `PageTypeLeaf`.

Example (exact bits are an implementation detail):
- `leafPrefixCompressedFlag` already exists in `TreeDB/node/node.go` (0x8000).
- Add `leafColumnarFlag` (e.g., 0x4000).
- Update `pageTypeMask` to exclude both flags when extracting `PageType`.

This keeps the format indicator inside the canonical page image and therefore
inside the existing CRC coverage.

#### 6.5.2 Define a concrete on-page layout (v1)

Within the 4096-byte page body:

```
| PageHeader (16B) |
| LeafHeaderV1     |  (counts, offsets, feature bits)
| Fingerprints?    |  (optional, Phase 2)
| KeyOffsets[]     |  (uint16 offsets into KeyBlob; count entries)
| KeyBlob          |  (packed key suffix bytes; may include restart/LCP metadata)
| PtrOrInlineCol[] |  (fixed-size struct per entry: flags + (ValuePtr or inline offset/len))
```

Notes:
- Phase 1 can still use prefix compression semantics, but the *storage* should
  be “keys are contiguous” even if they are prefix-encoded.
- Keep pointer/value column fixed-size if possible. If inline values are
  variable-length, store them in a tail blob with offsets, and keep the main
  “pointer column” fixed.

#### 6.5.3 Implement encode/decode/search incrementally

**Implementation order**
1. Add format flag + routing:
   - Update `node.NewNodeView` / `node.NewNode` to parse flags correctly.
2. Add decode helpers for the new leaf header and columns.
3. Add a new `SearchLeafColumnar` implementation (or branch inside `SearchLeaf`).
4. Update the leaf builder (`TreeDB/node/builder.go`) to emit the new format on COW writes.
5. Keep legacy decode/search code path intact for old pages.

**Files likely touched**
- `TreeDB/node/node.go`: flags/masks; helper predicates (isColumnarLeaf).
- `TreeDB/node/leaf.go`: new decode/search path.
- `TreeDB/node/builder.go`: new encoder for columnar leaves.
- `TreeDB/node/leaf_entry_integrity_test.go` (or new tests): roundtrip validation.

#### 6.5.4 Tests

Add tests that are format-specific and fast:
- Roundtrip encode/decode for the new leaf format (keys, tombstones, inline, pointers).
- Mixed-format DB: create legacy pages, then force COW rewrite to emit new pages and verify reads still succeed.
- Corruption tests: invalid offsets / truncated blobs should return `ErrCorruptedNode`.

Acceptance:
- All existing node/unit tests pass.
- New tests prove mixed-format correctness.

### 6.6 Phase 2: Fingerprints (Detailed Plan)

**Goal**: reduce full key comparisons.

**Rules**
- Fingerprints are hints only; they must not cause false negatives.

**Implementation sketch**
- Add `fingerprint[i] = hash8(key)` stored contiguously.
- During binary search or scan:
  - compare fingerprint first,
  - only if it matches do a full key compare.

**Where**
- Add to new columnar leaf header layout (or as an optional section with a feature bit).
- Prefer implementing scalar first; optionally add SIMD later behind `//go:build` tags.

**Tests**
- Ensure keys with same fingerprint still resolve correctly (full compare decides).
- Fallback when fingerprints missing/disabled.

### 6.7 Phase 3: Internal Node Fan-out (Detailed Plan)

**Goal**: reduce internal node size and increase fanout (fewer levels).

Implementation steps:
- Ensure shortest-separator logic is correct and consistently used during splits.
- Introduce internal-node global LCP (store prefix once).
- Convert internal entries to pointer-first columnar layout (child pointers contiguous).

Tests:
- Search correctness across splits.
- Randomized insert/delete with verification against a reference map (reuse existing fuzz scaffolding).

### 6.8 Phase 4: Bloom Filters (Detailed Plan)

**Goal**: speed up negative lookups.

Recommended approach first: **in-page bloom** for leaves.
- Stored inside leaf page image → CRC already covers it → simplest recovery story.
- Recomputed during leaf page build (COW writes).

Only consider pager sidecar bloom after in-page is proven valuable.

Tests:
- Bloom never produces false negatives: add randomized test that verifies that any present key always passes bloom.
- Missing/disabled bloom falls back to full search.

### 6.9 Phase 5: Clustered Leaves + Partitioned Index (Implementation Notes)

This is intentionally *not* Phase 1–4 work. If we start it, treat it as a new
format project with its own design doc and rollout plan.

Concrete “medium bot” tasks that are still feasible:
- Prototype a **cluster container reader** for a read-only “cold index” file produced by vacuum.
- Add a minimal manifest format and a recovery-safe swap protocol (similar in spirit to existing index-swap).

### 6.10 Phase Ordering (Recommended)

Start with: Phase 0 → Phase 1 → Phase 2. Do not begin Phase 5 until Phase 1–2
prove that “keys-first” layouts materially improve the point-read hot path.

---

## 7. Validation Plan

### 7.1 Correctness
- Existing TreeDB fuzz/model tests must pass.
- Add targeted page-format roundtrip tests:
  - encode/decode leaf and internal nodes (including prefixes, fingerprints, bloom presence/absence).
- Crash recovery tests must ensure new metadata does not create unrecoverable states.

### 7.2 Performance
- Gate changes with:
  - point lookup microbench,
  - scan microbench,
  - at least one “churn + vacuum” workload to catch index growth regressions.

---

## 8. Open Questions / Decisions

- Should “Phase 1 leaf layout” be introduced as a new leaf format version while keeping internal nodes unchanged?
- Do we want in-page bloom (simpler) or pager-metadata bloom (potentially smaller pages but higher complexity)?
- Is partitioning a near-term objective (for statesync), or should we first maximize single-file performance and keep partitioning as a separate track?

---

## Appendix: Source Notes

- `TreeDB/btree_optimization.md` is the original index-focused brainstorm.
- `TreeDB/optimization_plan.md` Part I adds important correctness constraints.
- This draft intentionally ignores slab sections in `TreeDB/optimization_plan.md`.
