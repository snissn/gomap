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
- Group pages into ~2MB clusters for better compression and shared prefixes.
- If we ever move to larger pages (e.g., 16KB), update the math; do not bake 16KB assumptions into logic.

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

## 6. Validation Plan

### Correctness
- Existing TreeDB fuzz/model tests must pass.
- Add targeted page-format roundtrip tests:
  - encode/decode leaf and internal nodes (including prefixes, fingerprints, bloom presence/absence).
- Crash recovery tests must ensure new metadata does not create unrecoverable states.

### Performance
- Gate changes with:
  - point lookup microbench,
  - scan microbench,
  - at least one “churn + vacuum” workload to catch index growth regressions.

---

## 7. Open Questions / Decisions

- Should “Phase 1 leaf layout” be introduced as a new leaf format version while keeping internal nodes unchanged?
- Do we want in-page bloom (simpler) or pager-metadata bloom (potentially smaller pages but higher complexity)?
- Is partitioning a near-term objective (for statesync), or should we first maximize single-file performance and keep partitioning as a separate track?

---

## Appendix: Source Notes

- `TreeDB/btree_optimization.md` is the original index-focused brainstorm.
- `TreeDB/optimization_plan.md` Part I adds important correctness constraints.
- This draft intentionally ignores slab sections in `TreeDB/optimization_plan.md`.

