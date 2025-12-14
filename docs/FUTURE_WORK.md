# Future Work Plan

This document outlines the roadmap for making `gomap` a robust, production-ready key-value store.

## Phase 1: Core Operations & Lifecycle (Priority: High)
**Objective:** Complete the basic CRUD feature set and ensure stability.
*   **Deletes:** Implement logical deletion (tombstones).
    *   Add a `deleted` flag to the index or a specific tombstone value in the slab.
    *   Update `Get` to return `nil` if it encounters a tombstone.
*   **Updates:** Explicitly verify update behavior (currently implicit via `Add`).
    *   Ensure updates do not create duplicate index entries (already handled by `addKey`).
    *   Benchmark Update vs Insert performance (as seen in `BenchmarkAddMany`).

## Phase 2: Durability & Safety (Priority: Critical)
**Objective:** Prevent data corruption and ensure consistency after crashes.
*   **Write Ahead Log (WAL):**
    *   Record operations to a small append-only log before modifying the memory/index.
    *   On startup, replay the WAL to restore consistency if the shutdown was unclean.
*   **Crash Recovery / Index Rebuild:**
    *   Ability to rebuild the entire hash index (`hashkeys-N` and `metadata`) by scanning the `slab-real` file from start to finish. This makes the index ephemeral/reconstructible.

## Phase 3: Storage Engine Enhancements (Priority: Medium)
**Objective:** Manage disk space efficiency and performance scaling.
*   **Compaction & Garbage Collection:**
    *   **Architecture Change:** Move from a single `slab-real` file to multiple **Slab Segments** (e.g., `slab-001`, `slab-002`).
    *   **Rolling Segments:** Close the current slab segment when it reaches a size limit (e.g., 64MB) and open a new one.
    *   **Compaction:** Background process that reads old segments, filters out overwritten/deleted keys, writes valid keys to a new segment, and deletes the old files.
*   **Incremental Resizing:**
    *   Implement "consistent hashing" or "linear hashing" approach where we migrate keys bucket-by-bucket during `Get`/`Add` operations to avoid "stop-the-world" latency.

## Phase 4: Advanced Performance (Priority: Low)
**Objective:** Optimize specific workloads.
*   **Compression:**
    *   Add optional Snappy/Zstd compression for values in the slab.
    *   Add a header flag to indicate if a value is compressed.
*   **Zero-Copy Reads:**
    *   Option to return byte slices directly mapped to the mmap region (unsafe but fast) vs copying (safe).

## Phase 5: Ecosystem (Priority: Future)
*   **Full Redis Protocol:** Expand `redisserver` support.
*   **CLI Tool:** Admin tools for inspection and repair.
