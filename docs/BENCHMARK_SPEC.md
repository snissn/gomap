# Benchmark & Stress Test Specification

This document outlines the plan for advanced benchmarking and stress testing of `gomap` to validate its performance under realistic workloads and its durability under failure conditions.

## 1. Advanced Performance Benchmark ("Realistic Workload")
**Objective:** Measure throughput and latency under conditions that mimic production use cases, moving beyond simple burst SET/GET.

### Configurations
*   **Mixed Workload:** 80% GET, 20% SET.
*   **Value Sizes:** 
    *   Small: 32 bytes (Testing index/lock overhead).
    *   Medium: 1 KB (Testing memory copy/allocations).
    *   Large: 100 KB (Testing disk I/O bandwidth and compression).
*   **Key Distribution:**
    *   Uniform Random: Standard distribution.
    *   Zipfian/Pareto: 20% of keys accessed 80% of the time (Hot keys).
*   **Concurrency:** High concurrency (e.g., 200 clients) with Pipelining (16 commands/batch).

### Implementation Strategy
*   Extend `benchmark/config.go` to support defining "Scenarios".
*   Each Scenario defines: `Command` (e.g., `redis-benchmark`), `Args` (flags for size, randomness), and `Description`.
*   Run scenarios sequentially and report results.

## 2. Durability Stress Test ("Chaos Monkey")
**Objective:** Verify that `Recover()` correctly restores the database state after an unclean shutdown (`kill -9`), ensuring no data corruption and minimal data loss (ACKed writes must exist).

### Test Logic
1.  **Setup:** Start `gomap` server.
2.  **Load:** Spawn N workers writing unique keys (e.g., `key-0` to `key-1000000`) continuously.
3.  **Chaos:** After a random duration (e.g., 2-5 seconds), send `SIGKILL` to the server process.
4.  **Verification:**
    *   Restart server.
    *   The server should start successfully (auto-recovery).
    *   Read all keys that were *successfully* written (client received "OK").
    *   **Assertion:** 100% of ACKed keys must exist and have correct values.
5.  **Repeat:** Run this cycle 10 times.

### Implementation Strategy
*   New Go test file: `stress/chaos_test.go`.
*   Uses `exec.Command` to manage the server process lifecycle.
*   Uses a Go Redis client to push load and track ACKed writes.

## 3. Compaction Stress Test ("GC Lag")
**Objective:** Measure the impact of the Stop-the-World `Compact()` operation on read latency.

### Test Logic
1.  **Fill:** Write 1GB of data (updating the same 100k keys repeatedly to generate garbage).
2.  **Load:** Start a reader loop measuring latency of `GET` operations.
3.  **Trigger:** Send `BGREWRITEAOF` (triggers `Compact`).
4.  **Measure:** Record latency spike magnitude and duration during compaction.

### Implementation Strategy
*   Script/Test that outputs a histogram of latencies during the compaction window.
