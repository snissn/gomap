# Addendum A: Testing Specification v2.5 (Updated for Spec v2.5)

## 1. Unit Testing Strategies

Focus on isolating the complex low-level data structures before system integration.

### 1.1 The Pager (Chunked MMap)

* **Boundary Crossing:** Initialize with a small chunk size (e.g., 1KB) to force frequent chunk allocations. Verify reads spanning across chunk boundaries (`Offset` in Chunk A, `Length` extends into Chunk B).
* **Growth Safety (RFC-01):**
    * Simulate file growth (Truncate + Remap) while a "Reader" holds a pointer to the old memory region. Ensure no panic/segfault occurs.
    * **Negative Test:** Attempt to `Truncate` (shrink) the file while mapped. Verify the internal safety wrapper forbids this or that the test environment handles the panic gracefully without crashing the suite.
* **Ref-Counting:** Verify that `Unmap` is never called on active chunks.
* **Durability Plumbing (RFC-03):**
    * **Strict Ordering:** Verify that `WriteSync()` calls `fdatasync` on the active slab file **before** issuing any `msync`/`fsync` on the index file.
    * Verify that `Write()` does not necessarily force durability (allowed), but remains recoverable up to the last completed `WriteSync()`.

### 1.2 Slotted Pages & Cursor Stack (Updated)

* **Leaf Node Data Integrity:**
    * **The "Walk" Test:** Insert 10,000 keys causing multiple splits.
        * Traverse `Head -> Tail` via `Iterator` (Logic: Stack Drill-Down). Count items.
        * Traverse `Tail -> Head` via `ReverseIterator`. Count items.
        * **Assert:** Both counts must be identical.

* **Split Logic & Parent Wiring:**
    * Fill a page (`Page A`) to capacity and trigger a split (creating `Page B`).
    * **Assert:**
        * `Page A` and `Page B` are valid Leaf nodes.
        * The **Parent Node** directory now contains entries pointing to both `Page A` and `Page B`.
        * **Keys:** The first key of `Page B` is strictly greater than the last key of `Page A`.

* **Cursor Stack Mechanics:**
    * **Stack Depth:** Traverse a tree with height 3.
    * **Assert:** The internal `Cursor.Stack` slice has length 3 when at a leaf, and correctly pushes/pops during `Next()` calls that cross subtree boundaries.
    * **Drill Down:** Verify `Next()` correctly descends from a Branch node to the **left-most** leaf descendant.
    * **Reverse Drill Down:** Verify `Prev()` correctly descends from a Branch node to the **right-most** leaf descendant.

* **Defragmentation:** Delete every other key in a page. Verify that subsequent writes utilize the freed space (compaction within the page heap).

### 1.3 The Graveyard (Epochs) — Strict Snapshot Safety

* **Hold & Release:**
    1.  Create `Iterator A` (pins CommitSeq 10).
    2.  Perform `Write Batch` (generates "Old Pages").
    3.  Verify "Old Pages" are in Graveyard but **not** in On-Disk Freelist.
    4.  Close `Iterator A`.
    5.  Run `Prune()`. Verify pages move to On-Disk Freelist.

* **Reachability Barrier (MANDATORY):**
    * **Scenario:** Create `Iterator A` and force a workload that causes:
        * leaf splits and rewrites across multiple adjacent leaves,
        * internal node rewrites,
        * and at least one full commit producing a new root.
    * **Trigger:** While `Iterator A` is still open:
        * repeatedly call `Prune()` (or trigger automatic pruning),
        * apply additional commits to produce more "Old Pages",
        * and attempt page allocations that would prefer freelist reuse.
    * **Assert (Safety):**
        * No PageID that is reachable from `Iterator A`’s pinned root (via **Cursor Stack traversal**) is ever observed as reclaimed or reused.
        * Iteration completes without missing keys, loops, dead ends, or corruption.
        * If instrumentation is available: pages reachable from the pinned snapshot must not appear in the On-Disk Freelist until `Iterator A` is closed.

* **Reuse:** Verify that the Allocator actually pulls IDs from the Freelist before growing the file.

* **TTL is GC-Only:**
    * **Scenario:** Create a reader `Iterator A` and sleep exceeding the configured TTL (e.g., 10 minutes).
    * **Trigger:** Run `Prune()` repeatedly during the TTL exceedance (and/or continue committing new roots), then continue iterating.
    * **Assert:** The iterator remains operational and correct:
        * no `ErrSnapshotExpired` (or any TTL-derived failure) is surfaced as an API failure,
        * no missing keys,
        * no broken leaf traversal,
        * no corruption / CRC errors attributable to reclamation.

### 1.4 Slab Record Format (RFC-04)

* **Record Round-Trip:**
    * Write a large value record with key+value.
    * Read by `ValuePtr`, parse record, verify **CRC32C** and payload integrity.
* **Corrupt Record CRC:**
    * Flip one bit in `ValueBytes` or the CRC field.
    * **Assert:** `Get()` returns an error (or safe panic in lower-level tests), never silent corruption.
* **Record Enumeration:**
    * Append N records; iterate sequentially through slab parsing.
    * **Assert:** All records are discoverable and offsets advance correctly (no desync on variable lengths).

### 1.5 Superblocks (Redundant Meta)

* **Dual Meta Selection:**
    * Create a DB, commit multiple times.
    * Corrupt Meta Page A (CRC32C mismatch).
    * **Assert:** Open selects Meta Page B and loads correctly.
* **Monotonic CommitSeq:**
    * Ensure `CommitSeq` increments exactly once per successful commit and never decreases across restarts.

### 1.6 Adaptive Inline Threshold (Telemetry + Feedback)

These tests validate that the adaptive controller is (a) safe, (b) low-overhead, and (c) converges toward sane behavior.

* **Latch Semantics (MANDATORY):**
    * **Scenario:** Enable adaptive threshold updates with an aggressive evaluation interval (e.g., update every commit) and induce oscillating pressures.
    * **Trigger:** Create a single large `Batch` that takes non-trivial time to commit (e.g., many keys and values around the current threshold).
    * **Assert:**
        * The commit uses exactly one latched threshold value for all ops in that commit (instrumentation: record the threshold observed by the commit).
        * No operation within the same commit uses a different threshold, regardless of concurrent controller updates.

* **Controller Bounded Step (MANDATORY):**
    * **Scenario:** Configure `InlineHardMin`, `InlineHardMax`, and `step` (e.g., 64).
    * **Trigger:** Force sustained index pressure for several evaluation intervals, then sustained slab pressure.
    * **Assert:**
        * Threshold moves by at most `step` per evaluation.
        * Threshold never exceeds hard bounds.
        * Threshold changes direction when pressure flips.

* **Low Overhead / Evaluation Frequency:**
    * **Scenario:** Configure `K=100` (update every 100 commits).
    * **Trigger:** Run 10,000 small commits.
    * **Assert:** Controller evaluation occurs approximately `10000 / K` times (instrumentation counter), and does not allocate per-operation.

---

## 2. Integration & Functional Testing

Focus on the public `DB` interface and ACID properties.

### 2.1 Basic CRUD

* **CRUD Cycle:** `Put(K, V)` -> `Get(K)` -> `Delete(K)` -> `Get(K)` (expect NotFound).
* **Overwrite:** `Put(K, V1)` -> `Put(K, V2)`. Ensure `Get(K)` returns V2 and space for V1 is eventually reclaimed (dead bytes increase, later compaction reduces).
* **Persistence:** `Put(K, V)` -> `Close()` -> `Reopen()` -> `Get(K)`.

### 2.2 Iterators (Forward & Reverse)

* **Forward Scan:** Insert keys `A, C, E`. Scan `start=A, end=Z`. Expect `A, C, E`.
* **Reverse Scan (Cosmos Semantics):**
    * Insert keys `A, B, C, D, E`.
    * Call `ReverseIterator(start=B, end=D)`.
    * Advance by calling `Next()` repeatedly.
    * **Expect:** `C`, then `B`.
    * **Boundary Check:** Call `ReverseIterator(start=nil, end=nil)`. Expect `E, D, C, B, A`.
* **End-Exclusive Exact Hit:**
    * Insert `A,B,C`.
    * `ReverseIterator(start=nil, end=C)` should yield `B,A` (since end is exclusive).
    * `Iterator(start=B, end=C)` yields `B` only.
* **Concurrent Iteration (Snapshot Visibility):**
    * Start `ReverseIterator`.
    * Delete or overwrite half the keys in the range via a new commit.
    * Verify iterator continues to see a stable snapshot and successfully traverses the logical order even if pages were replaced in newer roots.
* **Concurrent Iteration Under Aggressive Pruning:**
    * Start `Iterator` and `ReverseIterator` over a wide range (or full domain).
    * In parallel:
        * run continuous commits that rewrite leaves and internal nodes,
        * invoke `Prune()` frequently.
    * **Assert:** Both iterators complete with a logically consistent snapshot:
        * no missing keys relative to the snapshot start,
        * no duplicates,
        * correct ordering,
        * no traversal failures (cursor stack correctly handles cached nodes),
        * no CRC/page-corruption errors caused by reclamation/reuse.

### 2.3 The "Zipper" Batch Merge

* **Atomicity:** Construct a Batch with 100 keys. Inject a panic/error at key 50 during commit. Reopen DB. Verify **0 keys** are present (no partial root commit).
* **Large Value Handling:** Batch insert 10 values of 10MB each. Verify `.slab` grows, but memory usage remains low.

### 2.4 NewBatchWithSize

* **Smoke:** Call `NewBatchWithSize(1<<20)`, write keys, ensure correctness matches `NewBatch()`.
* **Hint Utilization (Optional):** Track internal capacity growth (via instrumentation) to ensure the hint reduces reallocations (non-functional, best-effort).

### 2.5 Adaptive Inline Threshold Integration

* **Mixed Workload Convergence (Smoke):**
    * Run a workload with two phases:
        1. IAVL-like: many medium values (e.g., 200–800 bytes).
        2. Blob-like: periodic very large values (e.g., 1–10MB).
    * **Assert:** Threshold moves sensibly (does not peg min/max permanently) and system remains correct (all values readable, iterators correct, no corruption).
* **Hard Max Enforcement:**
    * Write values larger than `InlineHardMax` while the controller attempts to increase threshold.
    * **Assert:** those values always go out-of-line; reads succeed.

### 2.6 Manual Compaction (RFC-07)

* **Blocking Behavior:**
    * Fill DB with large values, then delete them (high dead bytes ratio).
    * Call `db.Compact()`.
    * **Assert:** The call blocks until compaction is complete.
    * **Assert:** Active slab size is reduced; dead bytes metric is near zero.

---

## 3. Advanced Stress Testing (Fuzzing)

Standard unit tests miss edge cases. We must use **Property-Based Testing** (e.g., `gopter` or Go 1.18+ Fuzzing).

### 3.1 Randomized "Model" Testing

* **The Oracle:** Maintain a simple Go `map[string][]byte` in memory.
* **The Subject:** The TreeDB instance.
* **The Loop:**
    1.  Generate random op (`Put`, `Delete`, `Get`, `Iterator`, `ReverseIterator`, `Batch`, `WriteSync`, `Prune`, `Compact`).
    2.  Apply to both Oracle and Subject.
    3.  Compare output.
    4.  **Invariant Check:** After every N ops, iterate the entire Subject and verify it matches the Oracle.
* **Snapshot-Reachability Invariant:**
    * Randomly keep some iterators open across multiple commits while invoking `Prune()`.
    * **Assert:** Open iterators never observe missing keys, duplicated keys, broken traversal, or reclaimed-page corruption.

### 3.2 Fuzzing Inputs

* **Key Fuzzing:** Feed arbitrary bytes (including nulls, max-byte `0xFF`, and massive keys) into `Put`.
* **Corrupt Files (RFC-04):**
    1.  Write a valid DB.
    2.  Flip 1 bit in `index.db` or `data.slab`.
    3.  Run `Open()` or `Get()`.
    4.  **Expectation:** Error or safe panic (due to CRC32C mismatch), **NEVER** silent corrupt data.
* **Corrupt Superblock Targeting:**
    * Flip bits in Meta Page 0 only; ensure Meta Page 1 recovery works.
    * Flip bits in both meta pages; ensure Open fails loudly.

---

## 4. Failure Injection (Crash Recovery)

Simulate power loss to ensure durability.

### 4.1 The "Kill" Test

* **Setup:** A separate process writes to the DB continuously.
* **Trigger:** `kill -9` the process randomly.
* **Recovery:**
    * Restart process.
    * Verify consistency: The DB should either be at State N or State N-1.
    * **Traversal Integrity:** Verify `Iterator` and `ReverseIterator` can traverse the entire key range without errors.

### 4.2 IO Failure Simulation

* **Disk Full:** Mock the filesystem to return `ENOSPC` during a slab write or index extension.
    * **Expectation:** Batch returns error. DB remains readable and consistent.
* **ReadOnly Mount:** Remount FS as Read-Only during operation.

### 4.3 Torn Write Recovery (RFC-03 Compliance)

* **Scenario A (Slab durable, meta not committed):**
    1.  Prepare a batch with large values.
    2.  Inject a panic **after** slab `fdatasync` completes but **before** the meta page update is persisted.
    * **Recovery:**
        1.  Restart the DB.
        2.  **Assert:** DB loads the previous superblock (highest valid `CommitSeq`).
        3.  **Assert:** The values from the "ghost" slab write are not accessible; the index points to the old version (if any).
* **Scenario B (Torn meta write):**
    1.  Crash during writing the inactive meta page (partial/torn).
    * **Recovery:**
        1.  Restart DB.
        2.  **Assert:** Open selects the other meta page (older but valid) and DB is consistent.

### 4.4 Torn Index Page Recovery (Detection)

* **Scenario:** Flip a bit in a non-meta B+Tree page used by the active root.
* **Expectation:** CRC32C detects corruption; Open or Get fails loudly.

---

## 5. Concurrency & Race Detection (Updated)

Go's `-race` detector is mandatory.

### 5.1 The Reader/Writer Duel

* **Routine 1 (Writer):** Continually updates `Key1` with incrementing integers.
* **Routine 2 (Reader):** Continually reads `Key1`.
* **Assert:** Reader sees monotonic non-decreasing values and never a torn read.

### 5.2 Zombie Life-Support (Reader vs. Deleter)

* **Scenario:**
    * Start a long-running **Reverse Iterator** on an "Old Slab".
    * Trigger compaction (marks old slab zombie and attempts deletion).
* **Assert (During Iteration):**
    * `Slab.IsZombie == true`
    * `Slab.RefCount > 0`
    * Underlying file exists on disk; iterator successfully reads values.
* **Assert (After Close):**
    * Close iterator.
    * Verify `Slab.RefCount` drops to 0.
    * Verify file is deleted from disk.

### 5.3 The "Resurrection" Write (CAS Verification)

* **Scenario:**
    1.  Compactor reads `Key A` (Val=1) from `OldSlab` and copies to `NewSlab`.
    2.  Pause compactor just before CAS.
    3.  User calls `Set(Key A, Val=2)` (points to write slab).
    4.  Resume compactor.
* **Assert:**
    * CAS fails (returns false).
    * `Get(Key A)` returns Val=2 (user wins).

### 5.4 Torn Compaction Recovery

* **Scenario:**
    1.  Start compaction on a slab with 1,000 keys.
    2.  Kill halfway through CAS loop (500 keys updated, 500 still old).
* **Recovery:**
    * Restart DB.
    * **Assert:** `Get()` works for all 1,000 keys.
    * **Assert:** `SlabManager` loads both slabs.
    * **Assert:** Old slab is not deleted while still referenced. (Verifies no premature cleanup occurred).

### 5.5 Compaction Serialization (RFC-02)

* **Scenario:**
    * Run continuous user batches.
    * Run compactor CAS commits in parallel.
* **Assert:**
    * All CAS commits occur under the **same single-writer lock** as user commits.
    * Instrumentation: Ensure no "Read-Modify-Write" gaps exist where a user write could slip in between a Compactor's pointer read and pointer CAS.

### 5.6 Compaction Throttling

* **Scenario:**
    * Populate DB to trigger a large compaction (e.g., 2GB).
    * Configure leaky-bucket rate limiter to low value.
* **Assert:**
    * Duration matches expected rate.
    * Concurrent writer latency does not spike.

### 5.7 Dead Hint Optimization Verification (RFC-05)

* **Scenario:**
    1.  Write 1,000 large keys to Slabs.
    2.  Delete all 1,000 keys (updates B+Tree, leaves Slab data).
    3.  Trigger Compaction.
* **Assert:**
    * Compactor uses hints to skip B+Tree lookups (validate via internal metrics: `tree_lookups_skipped > 0`).
    * Target slab is empty (or contains only live keys if mixed).

---

## 6. Cosmos-Specific Compliance

The database must pass the Cosmos SDK integration definition.

### 6.1 The Standard Suite

* **Requirement:** TreeDB must pass the generic `cosmos-db` backend test suite:
    * `github.com/cosmos/cosmos-db/db/db_test.go`
* **Key Tests:**
    * `TestDBIterator`: Validates forward iteration.
    * `TestDBReverseIterator`: Validates reverse iteration semantics (descending via `Next()`).
    * `TestDBBatch`: Validates batch correctness and sorting guarantees.

### 6.2 Pruning Validation

* **Scenario:**
    1.  Commit 10,000 blocks.
    2.  Set `Pruning = KeepRecent(100)`.
    3.  Run `Prune()`.
    4.  Verify disk usage drops (index pages reused; slab dead bytes reduced after compaction).
    5.  Verify querying Block 1 fails (correctly).
    6.  Verify querying Block 9,999 succeeds.

