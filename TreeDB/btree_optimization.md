# TreeDB: B-Tree Index Optimization Plan

## 1. Current State Analysis
TreeDB's current B-Tree implementation uses standard **Incremental Front-Coding** with periodic restart points.

### Limitations:
- **Serial Reconstruction**: Sequential decoding of keys within a block ($O(N)$ local lookup).
- **Binary Search Bottleneck**: Redundant reconstruction during search steps.
- **Interleaved Layout**: Poor cache locality due to mixed keys/values.
- **Fixed Overhead**: 4-byte header per key entry.

---

## 2. Design Goals
1. **Parallelizable Access**: Decouple key reconstruction from search order.
2. **Columnar Locality**: Separate keys from values to optimize for search performance.
3. **High Density**: Increase keys-per-page to reduce tree depth.
4. **SIMD-Friendly**: Structure data for vectorized comparisons.

---

## 3. Optimization Concepts

### 3.1 Columnar B-Tree Layout
- **Concept**: Transform the leaf structure from interleaved `{[K,V], [K,V]}` to columnar `{[Keys...], [Values/Pointers...]}`.
- **Why**: CPU cache only pulls in the **contiguous Key Block** during search, avoiding cache pollution from values. This layout is a prerequisite for SIMD optimizations.
- **Benefit**: 2x-5x faster key-scans and binary searches.

### 3.2 Bloom Filters in the Pager
- **Concept**: Maintain a tiny bitset (Bloom Filter) for every 16KB page, stored in the pager metadata.
- **Why**: Check the filter before performing I/O or `mmap` to search a page.
- **Benefit**: Skip negative lookups (missing keys) with 0 I/O.

### 3.3 SIMD Fingerprinting (Vectorized Search)
- **Concept**: Within the Columnar Key Block, store a 1-byte suffix hash for every key in a contiguous "Fingerprint Strip."
- **Benefit**: Use AVX2/NEON to compare 32 keys at once. If the hash doesn't match, skip the key without decompressing or comparing its full bytes.

### 3.5 Internal Node Fan-out Optimization
- **Shortest Separator Split**: When splitting pages, the `Zipper` identifies the shortest possible string that acts as a valid separator between children (e.g., separating `...apple` and `...banana` with `b`) rather than using the full key.
- **Internal Global LCP**: Stores a Longest Common Prefix for the entire internal node, stripping it from all separator keys to maximize space for child pointers.
- **Pointer-First Columnar Layout**: Organizes internal nodes into contiguous arrays: `[Child Pointers][Key Offsets][Key Suffixes]`. This ensures the CPU can binary-search keys and then O(1) jump to the corresponding Page ID with perfect cache alignment.
- **Relative Page IDs**: Uses 4-byte relative offsets for child pointers within the same range partition, effectively doubling fan-out compared to 8-byte global IDs.

### 3.6 Optimistic Lock Coupling
- **Concept**: Allow readers to traverse the tree without acquiring `RLock`. Readers use version-checks on nodes to detect concurrent writes.
- **Benefit**: Read throughput scales linearly with CPU cores, eliminating mutex bottleneck for concurrent lookups.

### 3.7 Predictive I/O & Adaptive Read-Ahead
- **Concept**: The `DBIterator` proactively pre-loads upcoming 2MB clusters into the buffer pool.
- **Adaptive Dampening**: If a pre-fetched cluster is not accessed within a specific time window, the pre-fetcher automatically throttles back to prevent I/O waste and cache pollution.

### 3.8 Epoch-Based Reclamation (Concurrency Safety)
- **Concept**: To support Optimistic Lock Coupling, retired pages (from splits/merges) are placed in an Epoch Queue.
- **Benefit**: Ensures that memory is only reused once all concurrent readers who may have seen the old page have safely moved on, preventing use-after-free crashes in a lock-free environment.

---

## 4. Specific Proposal: The "Clustered Columnar Leaf"

We propose a breaking change to the Leaf Page layout and Pager architecture to implement **Zonal Leaf Clusters** and **Range Partitioning**.

### 4.1 Zonal Leaf Clusters ("Mega Pages")
Instead of standalone 16KB pages, leaf pages are organized into **2MB Clusters** (128 pages).
- **Cluster Dictionary**: The first 32KB of a 2MB cluster is a shared Zstd dictionary.
- **Global LCP**: The cluster also defines a "Cluster Prefix" that all 128 pages share.
- **Benefit**: High compression ratios similar to Slab Zonal design while maintaining 16KB random-access granularity.

### 4.2 Range-Partitioned Index Files
The single `index.db` is replaced by a set of partitioned files:
- **Manifest**: Tracks `KeyRange -> FilePath`.
- **Parallel Maintenance**: Vacuuming or Compaction can operate on a single range file without locking the whole index.
- **Statesync Optimization**: Download specific range files and attach them to the live DB instantly.

### 4.3 Pager Evolution: The "Partitioned Pager"
- **Page ID Mapping**: Bits 48-63 of Page ID used for $O(1)$ routing to the correct partition file.
- **Zonal Alignment**: Ensures clustered pages are written at 2MB boundaries for dictionary lookup math.

---

## 5. Maintenance: Partitioned Vacuuming & Coalescing
With range partitioning, vacuuming becomes a background maintenance task:
1. **Shadow Partitioning**: Create a compacted shadow of a fragmented range file.
2. **Background Coalescing**: Proactively merge adjacent "underfull" pages (created by high compression density) to shrink total page count and improve read-ahead.
3. **Two-Pass Clustered Compaction**: Perform perfect-path dictionary training during the swap.
4. **Atomic Manifest swap**.

---
**Status**: Alpha / Architectural Shift.
**Next Steps**: Prototype Columnar Layout in `TreeDB/node`.
