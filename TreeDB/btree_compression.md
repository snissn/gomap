# TreeDB: B-Tree Index Compression Plan

## 1. Current State Analysis
TreeDB's current B-Tree implementation uses standard **Incremental Front-Coding** with periodic restart points (Front-Coding).

### Limitations:
- **Serial Reconstruction**: To access Key #15 in a block of 16, you must sequentially decode Keys #0 through #14. This is $O(N)$ for local lookups.
- **Binary Search Bottleneck**: Every step in a binary search triggers a reconstruction, causing redundant CPU cycles and cache misses.
- **Interleaved Layout**: Keys and Values are interleaved in the heap, which is poor for CPU cache locality during search-heavy workloads.
- **Fixed Overhead**: `PrefixLen` and `SuffixLen` take 4 bytes per entry regardless of key size.

---

## 2. Design Goals
1. **Parallelizable Access**: Move away from serial reconstruction where possible.
2. **Columnar Locality**: Separate keys from values to optimize for search performance.
3. **High Density**: Increase the number of keys per 16KB page to reduce the total B-Tree depth and file footprint.
4. **SIMD-Friendly**: Structure data to allow vectorized comparisons.

---

## 4. Specific Proposal: The "Clustered Columnar Leaf"

We propose a breaking change to the Leaf Page layout and Pager architecture to implement **Zonal Leaf Clusters** and **Range Partitioning**.

### 4.1 Zonal Leaf Clusters ("Mega Pages")
Instead of standalone 16KB pages, leaf pages are organized into **2MB Clusters** (128 pages).
- **Cluster Dictionary**: The first 32KB of a 2MB cluster is a shared Zstd dictionary.
- **Global LCP**: The cluster also defines a "Cluster Prefix" that all 128 pages share.
- **Benefit**: This captures the high compression ratios of the Slab "Zonal" design while allowing the Pager to still serve 16KB requests.

### 4.2 Range-Partitioned Index Files
The single `index.db` is replaced by a set of partitioned files:
- **Manifest**: A master metadata file tracks `KeyRange -> FilePath`.
- **Parallel Maintenance**: Maintenance tasks like **Vacuuming** and **Compaction** can now operate on a single range file (e.g., `index_A.db`) without locking the rest of the index.
- **Dynamic Resizing**: Range files can be split or merged based on size, preventing any single file from becoming a bottleneck.

### 4.3 Pager Evolution: The "Partitioned Pager"
The Pager will be refactored to support multiple underlying files:
- **Page ID Mapping**: Page IDs will include a "File Index" prefix (e.g., bits 48-63) to allow $O(1)$ routing to the correct partitioned file.
- **Zonal Alignment**: The Pager will ensure that clustered leaf pages are always written at 2MB boundaries within their respective partition files to preserve dictionary lookup math.

---

## 5. Maintenance: Partitioned Vacuuming
With range partitioning, vacuuming becomes a "background maintenance" task rather than an "offline" necessity:
1. Identify a range file with high fragmentation.
2. Create a new "Shadow Partition" for that range.
3. Perform a **Two-Pass Clustered Compaction** into the shadow file.
4. Atomically swap the partition in the Manifest.

---
**Status**: Alpha / Architectural Shift.
**Next Steps**: Prototype the "Partitioned Pager" to handle multiple files.

## 4. Implementation Path

### Phase 1: Columnar Layout (Internal Refactor)
- Modify `node.Builder` and `node.Node` to separate Keys and Values into two heaps.
- This provides an immediate CPU win for searches without changing compression logic.

### Phase 2: Page-Level Block Compression
- Implement `PageTypeCompressedLeaf`.
- Compress the "Key Heap" as a single block using Zstd (no dictionary).
- Evaluate the "Decompress-Once-per-Search" tax vs. the density gain.

### Phase 3: Global Dictionary Registry
- Implement the Dictionary Registry in the System Tree.
- Add `DictID` to the page header and implement dictionary-aware decompression.

---

## 5. Memory & I/O Impact
- **I/O**: No change to 16KB offsets. Total file size shrinks because the B-Tree total page count decreases (increased keys-per-page).
- **Memory**: Global dictionaries are shared. A 64KB dictionary shared across a 1GB index adds only 0.006% memory overhead.
- **CPU**: Search latency may increase slightly ($O(1)$ decompression of a 16KB block), but this is mitigated by the fact that the B-Tree is now shallower (fewer levels to traverse).

---
**Status**: Proposal. 
**Next Steps**: Prototype Columnar Layout in `TreeDB/node`.
