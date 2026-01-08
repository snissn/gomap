# TreeDB: Local Dictionary Compression (Zonal Dictionaries) - V2 (Optimized)

## 1. Overview
TreeDB Version 2 implements **Zonal Dictionary Compression** with **Global-Local Tiering**. This design achieves maximum compression ratios with near-zero I/O overhead for dictionary management by leveraging data homogeneity within a single Slab.

---

## 2. Technical Specification (Slab V2)

### 2.1 File Layout (The "Sticky" Design)
| Offset | Size | Description |
| :--- | :--- | :--- |
| 0 | 32KB | **File Header**: Magic (`TRDB-SLB`), Version (`0x02`), and Metadata. |
| 32KB | 32KB | **Global Dictionary**: Trained on the start of the slab. Used by all zones by default. |
| 64KB | 2MB - 64KB | **Zone 0 Data**: Records compressed against Global Dict. |
| 2,097,152 | 64B | **Zone 1 Header**: Flags indicating which dictionary to use. |
| 2,097,152 + 64B| ... | **Zone 1 Data**: Records. |

### 2.2 Dictionary Selection Logic (The $O(1)$ Path)
To read a record at `ptr.Offset`:
1. `zoneID = offset / 2MB`.
2. `headerOffset = zoneID * 2MB`. (If `zoneID == 0`, use Global).
3. Read 64-byte **Zone Header** at `headerOffset`.
4. **Flags**:
   - `0x00 (USE_GLOBAL)`: Use the Global Dictionary (already in memory). **[0 extra I/O]**
   - `0x01 (USE_LOCAL)`: Read the 32KB dictionary immediately following the header. **[1 extra I/O]**
   - `0x02 (USE_REF)`: Use the dictionary already loaded for Zone `N` (Index provided in header). **[0 extra I/O if cached]**

---

## 3. Optimizations for I/O and CPU

### 3.1 Dictionary Deduplication
When the `SlabManager` trains a new dictionary for a zone:
1. It computes a **XXHash64** of the new 32KB dictionary.
2. It compares it against the Global Dictionary and the last 3 Local Dictionaries.
3. If a match is found (>95% similarity or exact hash), it writes a `USE_REF` or `USE_GLOBAL` flag instead of the full 32KB.
4. **Benefit**: Drastically reduces Slab size and write I/O for homogenous datasets.

### 3.2 Pre-emptive Training & Entropy Monitoring
- **Entropy Check**: The compressor tracks the "Compression Ratio" of the last 100 records. 
- If the ratio degrades by >20% compared to the start of the zone, it signals that the Global/Current dictionary is becoming "stale."
- It then triggers **Background Training** for a new Local Dictionary to be used in the *next* zone.

### 3.3 Two-Pass Compaction (The "Gold Standard")
During compaction, the `Compactor` analyzes the entire 4GB slab to be written:
1. It selects the "Most Representative" 32KB for the Global Dictionary.
2. It identifies "Shift Points" where data distribution changes and schedules Local Dictionary overrides for those specific zones.

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

### 4.3 Training Memory
- **Sample Pooling**: 128KB training samples are gathered into pooled buffers to avoid heap allocations during high-frequency writes.
- **Concurrent Safety**: Dictionary training happens in a background goroutine. The `SlabManager` uses an `atomic.Pointer` to swap the "Active Encoder" once training completes, ensuring zero-lock contention for the writer.

---

---

## 5. Verification Plan
- **Benchmark: "The Stable Schema"**: Verify that for a 4GB slab of similar JSON objects, only **one** dictionary (the Global one) is written, and point-read latency is identical to naive compression.
- **Benchmark: "The Drifting Dataset"**: Verify that when data changes from JSON to Protobuf mid-slab, a new Local Dictionary is written and the compression ratio recovers.

---
**Status**: Alpha / Breaking / Optimized.