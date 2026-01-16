# Project Plan: TreeDB Unified Bench Enhancements & Matrix Runner

This plan outlines the evolution of the benchmark suite to support the new Slab V2 features and enable comprehensive optimization sweeps for TreeDB.

## Phase 1: Unified Bench Enhancements (Immediate)
**Goal**: Update the existing `unified_bench` tool to expose and measure the latest TreeDB optimizations.

### 1.1 Expose Slab V2 & Training Parameters
Update `cmd/unified_bench/adapter_treedb.go` to support:
- `--treedb-slab-version=1|2`: Force a specific slab format.
- `--treedb-train-bytes=N`: Set the background training sample target (Path B).
- `--treedb-train-dict-bytes=N`: Set the target dictionary size (default 32KB).
- `--treedb-train-min-records=N`: Minimum records before training triggers.

### 1.2 Support for Predefined Profiles
Add a high-level flag to quickly apply optimized configuration sets:
- `--treedb-profile=default|write-heavy|sync-heavy|compact`: Maps to internal `treedb.DefaultProfile()`, etc.

### 1.3 Disk Efficiency Metrics
The current tool focuses on TPS/latency. We need to add:
- **Disk Footprint**: Total size of `index.db` + `data-*.slab` files after each test.
- **Compression Ratio**: Real-time reporting of Slab V2 effectiveness in the benchmark table.
- **Write Amplification (Estimated)**: Bytes written to disk vs. raw value bytes.

---

## Phase 2: TreeDB Matrix Runner (Significant Change)
**Goal**: Enable "Parameter Sweeps" to compare different TreeDB settings side-by-side in a single run.

### 2.1 Matrix Definition CLI
Implement a syntax to define multiple TreeDB variations without running the whole suite multiple times.
- **Example**: `./unified_bench -dbs treedb -treedb-matrix="wal=true,false;compression=none,zstd"`
- This should generate 4 "Virtual DBs":
    1. `treedb-wal-none`
    2. `treedb-wal-zstd`
    3. `treedb-nowal-none`
    4. `treedb-nowal-zstd`

### 2.2 Variation Architecture
- Refactor `main.go` to support `DBVariation` objects.
- The `VariationRunner` will clone the base TreeDB adapter and apply specific flag overrides for each virtual DB.
- Results will be grouped by test and then by variation for easy comparison.

### 2.3 Automated Best-Setting Discovery
- Add a mode that identifies the "Winner" for a specific metric (e.g., "Best TPS" or "Smallest Disk Space") across all matrix variations.

---

## Evaluation of Current Framework
- **Strengths**: Solid cross-DB baseline, good profiling hooks, stable harness.
- **Weaknesses**: Static flag-to-option mapping is hard to maintain; no native support for multi-configuration runs of the same DB engine.

## Next Steps
1. **Halt for Review**: User to review this plan.
2. **Implementation**: Begin with Phase 1.1 (Slab V2 flags) once approved.