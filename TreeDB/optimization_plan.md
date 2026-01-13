# TreeDB: Unified Optimization Plan (B-Tree + Slab)

Note: TreeDB is in alpha/early development. All formats and layouts described here are provisional and may change without backward compatibility.

This document consolidates the B-Tree index optimization plan and the slab zonal dictionary compression plan into a single source of truth.

---

## Part I: B-Tree Index Optimization Plan

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

### 3.1 Design Rules (Correctness)
- Bloom filters are leaf-page hints only and must never introduce false negatives; missing/corrupt filters fall back to full page search.
- Page-level bloom filters are rebuilt from the page image on every COW write and validated via the page CRC.
- Relative page IDs require a stable remap strategy during partition compaction/splits; pointers must not break across range rewrites.

### 3.2 Columnar B-Tree Layout
- **Concept**: Transform the leaf structure from interleaved `{[K,V], [K,V]}` to columnar `{[Keys...], [Values/Pointers...]}`.
- **Why**: CPU cache only pulls in the **contiguous Key Block** during search, avoiding cache pollution from values. This layout is a prerequisite for SIMD optimizations.
- **Benefit**: 2x-5x faster key-scans and binary searches.

### 3.3 Bloom Filters in the Pager
- **Concept**: Maintain a tiny bitset (Bloom Filter) for every **B-Tree leaf page**, stored in the pager metadata. (Page size is a future-format decision; examples assume 16KB.)
- **Why**: Check the filter before performing I/O or `mmap` to search a page.
- **Correctness constraint**: The filter must never produce false negatives. Updates must be atomic with page writes, and rebuild/validation rules should exist for recovery or compaction.
- **Benefit**: Skip negative lookups (missing keys) with 0 I/O.

### 3.4 SIMD Fingerprinting (Vectorized Search)
- **Concept**: Within the Columnar Key Block, store a 1-byte suffix hash for every key in a contiguous "Fingerprint Strip."
- **Benefit**: Use AVX2/NEON to compare 32 keys at once. If the hash doesn't match, skip the key without decompressing or comparing its full bytes.

### 3.5 Internal Node Fan-out Optimization
- **Shortest Separator Split**: When splitting pages, the `Zipper` identifies the shortest possible string that acts as a valid separator between children (e.g., separating `...apple` and `...banana` with `b`) rather than using the full key.
- **Internal Global LCP**: Stores a Longest Common Prefix for the entire internal node, stripping it from all separator keys to maximize space for child pointers.
- **Pointer-First Columnar Layout**: Organizes internal nodes into contiguous arrays: `[Child Pointers][Key Offsets][Key Suffixes]`. This ensures the CPU can binary-search keys and then O(1) jump to the corresponding Page ID with perfect cache alignment.
- **Relative Page IDs**: Uses 4-byte relative offsets for child pointers within the same range partition, effectively doubling fan-out compared to 8-byte global IDs. This requires a stable remapping scheme during range splits/merges/compaction to avoid pointer breakage.

### 3.6 Optimistic Lock Coupling (OLFIT)
- **Versioned Node Protocol**: Every 16KB page header includes a `uint64 Version`. Bit 0 is the "Write Lock"; bits 1-63 are a monotonic counter. (Page size is a future-format decision; this example assumes 16KB.)
- **Reader Workflow**: 
  1. Load `v1`. If locked, spin. 
  2. Search node, extract pointer. 
  3. Load `v2`. If `v1 != v2`, retry.
- **Benefit**: Read throughput scales linearly with CPU cores, eliminating mutex contention for concurrent lookups.

### 3.7 Snapshot-Pinned Epoch Reclamation
- **Concept**: Retired pages (from splits/merges) are added to a `PendingFree` list with the current `CommitSeq`.
- **Logic**: Pages are only moved to the active `FreeList` once `Page.RetiredSeq < MinActiveSnapshotSeq`.
- **Benefit**: Leverages existing Snapshot infrastructure to ensure thread-safe memory reclamation in a lock-free environment without needing complex hazard pointers.

---

## 4. Specific Proposal: The "Clustered Columnar Leaf"

We propose a breaking change to the Leaf Page layout and Pager architecture to implement **Zonal Leaf Clusters** and **Range Partitioning**.

### 4.1 Zonal Leaf Clusters ("Mega Pages")
Instead of standalone 16KB pages, leaf pages are organized into **2MB Clusters** (128 pages). If TreeDB remains 4KB pages, the math changes; treat this as a forward-looking format change.
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

---

## Part II: Local Dictionary Compression (Zonal Dictionaries) - V2 (Optimized)

## 1. Overview
TreeDB Version 2 implements **Zonal Dictionary Compression** with **Global-Local Tiering**. This design achieves maximum compression ratios with near-zero I/O overhead for dictionary management by leveraging data homogeneity within a single Slab.

---

## 2. Technical Specification (Slab V2)

### 2.1 File Layout (The "Sticky" Design)
The file layout below is illustrative; the key rule is that each 2MB zone begins with a fixed header, optionally followed by a local dictionary, then the zone's records.

| Offset | Size | Description |
| :--- | :--- | :--- |
| 0 | 32KB | **File Header**: Magic (`TRDB-SLB`), Version (`0x02`), and Metadata. |
| 32KB | 32KB | **Global Dictionary**: Trained on the start of the slab. Used by all zones by default. |
| 64KB | 2MB - 64KB | **Zone 0 Data**: Records compressed against Global Dict. (Zone 0 has no extra header beyond the file header.) |
| 2MB * N | 64B | **Zone N Header**: Flags and dictionary selection metadata. |
| 2MB * N + 64B | 0 or 32KB | **Zone N Dictionary** (optional): present only when `USE_LOCAL` is set. |
| 2MB * N + 64B + dict | ... | **Zone N Data**: Records. |

### 2.2 Dictionary Selection Logic (The $O(1)$ Path)
To read a record at `ptr.Offset`:
1. `zoneID = offset / 2MB`.
2. If `zoneID == 0`, use the Global Dictionary (already in memory).
3. Otherwise, read the 64-byte **Zone Header** at `headerOffset = zoneID * 2MB`.
4. **Flags**:
   - `0x00 (USE_GLOBAL)`: Use the Global Dictionary. **[0 extra I/O]**
   - `0x01 (USE_LOCAL)`: Read the 32KB dictionary immediately following the header. **[1 extra I/O]**
   - `0x02 (USE_REF)`: Use the dictionary already loaded for Zone `N` (Index provided in header). **[0 extra I/O if cached]**

---

## 3. Optimizations for I/O and CPU

### 3.1 Design Rules (Correctness)
- Slab zone Bloom filters are offline rebuild/audit hints only; missing or invalid filters must never be used to skip zones during recovery or reads.
- Delta chains require base retention and snapshot/iterator safety; compaction must materialize deltas into new bases before reclaiming old bases.
- Multi-stream slab writers must reserve offsets via a single allocator (atomic tail or per-stream files) and support both batch reservations and concurrent goroutine writes.

### 3.2 Dictionary Deduplication
When the `SlabManager` trains a new dictionary for a zone:
1. It computes a **XXHash64** of the new 32KB dictionary.
2. It compares it against the Global Dictionary and the last 3 Local Dictionaries.
3. If a match is found (>95% similarity or exact hash), it writes a `USE_REF` or `USE_GLOBAL` flag instead of the full 32KB.
4. **Benefit**: Drastically reduces Slab size and write I/O for homogenous datasets.

### 3.3 Pre-emptive Training & Entropy Monitoring
- **Entropy Check**: The compressor tracks the "Compression Ratio" of the last 100 records. 
- If the ratio degrades by >20% compared to the start of the zone, it signals that the Global/Current dictionary is becoming "stale."
- It then triggers **Background Training** for a new Local Dictionary to be used in the *next* zone.

### 3.4 Two-Pass Compaction (The "Gold Standard")
During compaction, the `Compactor` analyzes the entire 4GB slab to be written:
1. It selects the "Most Representative" 32KB for the Global Dictionary.
2. It identifies "Shift Points" where data distribution changes and schedules Local Dictionary overrides for those specific zones.

### 3.5 Value Delta Encoding (Future Optimization)
- **Concept**: If a value is updated, instead of writing the whole new value to the Slab, we write a `Diff(Old, New)`.
- **Why**: In blockchain state, often only a balance or a nonce changes.
- **Correctness constraint**: Base values referenced by deltas must be retained (pinning or chain compaction) until all dependent deltas are compacted into a new base. GC/compaction must not discard a base needed to replay deltas, and must respect active snapshots/iterators before reclaiming any base or delta chain.
- **Benefit**: Massive I/O reduction for "Account Updates."

### 3.6 Multi-Level Slab Tiering
- **Concept**: Move "Zombie" (Compacted) Slabs to a compressed read-only file format, while keeping "Active" Slabs in a raw, fast-append format.
- **Why**: Cold data shouldn't occupy the same high-speed SSD space as the active write-head.

### 3.7 Multi-Stream Slabs (Parallel Writing)
- **Thread-Local Affinity**: Writers are assigned to one of $N$ streams based on their current `G` context (goroutine). This maximizes CPU cache hits and eliminates global mutex contention.
- **The Soft Barrier**: Dictionary rotation is handled via a "Soft Barrier." The training goroutine publishes the dictionary to a pending slot; the primary writer thread performs the atomic swap and alignment write at its next available opportunity, ensuring the file handle is never contended.
- **Correctness constraint**: Multi-stream writes still require a single allocation authority for offsets (e.g., atomic `fetch_add` on the file tail or per-stream files with a merge map) to prevent overlapping writes. The allocator must support large batch reservations (single goroutine) and independent concurrent goroutine writes.

### 3.8 Zonal Bloom Filters (Disaster Recovery & Audit)
- **Concept**: Store a 2KB Bloom Filter alongside each 2MB Zone Header, hashing **Keys Only**. (This implies a larger header region than the 64B zone header above; any on-disk layout should make this explicit.)
- **Correctness constraint**: Zone BFs are for offline rebuild/audit hints only; missing or invalid BFs must never be used to skip zones during recovery or reads.
- **Use Case**: 
  - **Disaster Recovery**: Accelerates rebuilding a lost `index.db` by allowing the scanner to skip irrelevant zones.
  - **Integrity Audits**: Verifies B-Tree correctness by matching index keys against the slab without full record decompression.
- **Note on OmitKeys**: In `OmitSlabKeys` mode, the Bloom Filter provides a niche "Identity Proof" that a known key *was* stored in a zone, even if the full key string is absent from the data record.

### 3.9 Huge-Page Awareness (OS Optimization)
- **Concept**: Align 2MB Zones to physical 2MB page boundaries and use `madvise(MADV_HUGEPAGE)`.
- **Benefit**: Reduces TLB misses for large-memory workloads, providing a 5-10% CPU win during random-access reads.

### 3.10 Dictionary Integrity (Safety)
- **Concept**: Store a dedicated CRC32C checksum for the 32KB Dictionary in the Zone Header.
- **Benefit**: Prevents "Cascading Corruption" where a single bit-flip in a dictionary renders 2MB of data unreadable.

---

## 4. Implementation Detail: Go Memory Management

To ensure high performance and low GC pressure, the Go implementation utilizes a two-tier management strategy for dictionaries and decoders.

### 4.1 Zero-Copy Dictionaries
- **Mmap Integration**: Dictionaries are never "copied" from disk to the Go heap. The `SlabManager` creates a sub-slice of the `SlabFile.mmapData` for the required 32KB dictionary.
- **Lifetime**: Since `SlabFile` already uses reference counting (`RefCount`) to keep mmap mappings alive, dictionaries remain valid as long as at least one Snapshot or the Manager is using the slab.

### 4.2 Two-Tier Decoder Lifecycle
- **Tier 1: Global Decoder Pool (Per-Slab)**
  - Each `SlabFile` struct gains a `globalDecPool *sync.Pool`.
  - This pool stores `*zstd.Decoder` instances pre-initialized with that slab's Global Dictionary.
  - **Read Path**: `Get` -> `slab.globalDecPool.Get()` -> `Decode` -> `Put()`.
- **Tier 2: Zonal LRU Cache (Global)**
  - A global `LRUCache[(SlabID, ZoneID)]*zstd.Decoder` manages "Local" and "Referenced" dictionaries.
  - **Eviction**: Uses a "Least Recently Used" policy to cap the total number of active local decoders across all open slabs.
  - **Optimization**: Uses `zstd.WithDecoderLowmem(true)` to minimize RAM footprint per cached decoder.

### 4.3 Training Memory & Concurrent Safety
- **Atomic Dictionary Swapping**: Uses `atomic.Pointer` to transition from Global to Local dictionaries.
- **Sample Pooling**: 128KB training samples are gathered into pooled buffers to avoid heap allocations during high-frequency writes.
- **Concurrent Safety**: Dictionary training happens in a background goroutine. The `SlabManager` uses an `atomic.Pointer` to swap the "Active Encoder" once training completes, ensuring zero-lock contention for the writer.

---

## 5. Verification Plan
- **Benchmark: "The Stable Schema"**: Verify that for a 4GB slab of similar JSON objects, only **one** dictionary (the Global one) is written, and point-read latency is identical to naive compression.
- **Benchmark: "The Drifting Dataset"**: Verify that when data changes from JSON to Protobuf mid-slab, a new Local Dictionary is written and the compression ratio recovers.

---
**Status**: Alpha / Breaking / Optimized.
