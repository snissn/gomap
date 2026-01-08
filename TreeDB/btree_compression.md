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

## 3. Specific Proposal: The "Clustered Columnar Leaf"

We propose a breaking change to the Leaf Page layout to implement **Columnar Block Compression**.

### 3.1 Page Layout (V2 Leaf)
The 16KB page will be divided into three distinct segments:
1. **Page Header (Fixed Size)**:
   - `LCP_Len (uint16)`: Length of the Longest Common Prefix shared by *all* keys in this page.
   - `LCP_Bytes`: The actual shared prefix bytes (stored once).
   - `DictID (uint8)`: ID of the Global B-Tree Dictionary used for this page.
   - `KeyBlock_Len (uint16)`: Size of the compressed key buffer.
2. **Compressed Key Buffer**:
   - A single Zstd-compressed blob containing all key suffixes (Key minus LCP).
3. **Value Heap**:
   - Uncompressed values and pointers (enables $O(1)$ value retrieval once the key index is found).

### 3.2 Global Dictionary Selection
Instead of training a dictionary per page (too much overhead), we use **Typed Global Dictionaries**:
- The Pager/System Tree maintains a registry of dictionaries (e.g., `DICT_JSON`, `DICT_HEX`, `DICT_ADDRESSES`).
- When a page is flushed, the `Builder` tests it against the registry and selects the `DictID` with the best ratio.
- The dictionary is loaded into memory once and shared across thousands of pages.

### 3.3 Search Optimization: LCP Skipping
By storing the **Longest Common Prefix (LCP)** in the header:
- `SearchLeaf` can immediately skip the first `LCP_Len` bytes of the search key.
- If the search key is shorter than `LCP_Len` or differs within the LCP, we know the key is not in this page without decompressing a single byte.

---

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
