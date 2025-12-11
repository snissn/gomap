# Addendum A: Testing Specification v2.3 (Updated for Redundant Superblocks, Slab Records, and Cosmos Iterator Semantics)

## 1. Unit Testing Strategies

Focus on isolating the complex low-level data structures before system integration.

### 1.1 The Pager (Chunked MMap)

* **Boundary Crossing:** Initialize with a small chunk size (e.g., 1KB) to force frequent chunk allocations. Verify reads spanning across chunk boundaries (`Offset` in Chunk A, `Length` extends into Chunk B).
* **Growth Safety:** Simulate file growth (Truncate + Remap) while a "Reader" holds a pointer to the old memory region. Ensure no panic/segfault occurs.
* **Ref-Counting:** Verify that `Unmap` is never called on active chunks.
* **Durability Plumbing (NEW):**

  * Verify that `WriteSync()` causes the required persistence boundary: dirty index pages are flushed (`msync` as needed) and `index.db` is `fdatasync`’d.
  * Verify that `Write()` does not necessarily force durability (allowed), but remains recoverable up to the last completed `WriteSync()`.

### 1.2 Slotted Pages (Leaf Node Linkage)

* **Doubly Linked List Integrity:**

  * **The "Walk" Test:** Insert 10,000 keys causing multiple splits.

    * Traverse `Head -> Tail` via `NextLeafID`. Count pages.
    * Traverse `Tail -> Head` via `PrevLeafID`. Count pages.
    * **Assert:** Both counts must be identical.

* **Split Logic & Pointer Wiring:**

  * Fill a page (`Page A`) to capacity and trigger a split (creating `Page B`).
  * **Assert:**

    * `Page A.Next == Page B`
    * `Page B.Prev == Page A`
    * If `Page C` existed to the right of A, `Page B.Next == Page C` and `Page C.Prev == Page B`.

* **Boundary Stitch Regression (NEW):**

  * Construct a batch that rewrites keys spanning multiple leaves, but where the left boundary leaf is *not* rewritten and only its right neighbor is, and similarly on the right boundary.
  * **Assert:** Leaf chain remains consistent and sorted; no missing/duplicated leaves.

* **Defragmentation:** Delete every other key in a page. Verify that subsequent writes utilize the freed space (compaction within the page heap).

### 1.3 The Graveyard (Epochs) — Strict Snapshot Safety

* **Hold & Release:**

  1. Create `Iterator A` (pins CommitSeq 10).
  2. Perform `Write Batch` (generates "Old Pages").
  3. Verify "Old Pages" are in Graveyard but **not** in On-Disk Freelist.
  4. Close `Iterator A`.
  5. Run `Prune()`. Verify pages move to On-Disk Freelist.

* **Reachability Barrier (NEW, MANDATORY):**

  * **Scenario:** Create `Iterator A` and force a workload that causes:

    * leaf splits and rewrites across multiple adjacent leaves,
    * internal node rewrites,
    * and at least one full commit producing a new root.
  * **Trigger:** While `Iterator A` is still open:

    * repeatedly call `Prune()` (or trigger automatic pruning),
    * apply additional commits to produce more "Old Pages",
    * and attempt page allocations that would prefer freelist reuse.
  * **Assert (Safety):**

    * No PageID that is reachable from `Iterator A`’s pinned root (including via `NextLeafID` / `PrevLeafID` traversal) is ever observed as reclaimed or reused.
    * Iteration completes without missing keys, loops, dead ends, or corruption.
    * If instrumentation is available: pages reachable from the pinned snapshot must not appear in the On-Disk Freelist until `Iterator A` is closed.

* **Reuse:** Verify that the Allocator actually pulls IDs from the Freelist before growing the file.

* **TTL is GC-Only (REVISED):**

  * **Scenario:** Create a reader `Iterator A` and sleep exceeding the configured TTL (e.g., 10 minutes).
  * **Trigger:** Run `Prune()` repeatedly during the TTL exceedance (and/or continue committing new roots), then continue iterating.
  * **Assert:** The iterator remains operational and correct:

    * no `ErrSnapshotExpired` (or any TTL-derived failure) is surfaced as an API failure,
    * no missing keys,
    * no duplicated keys,
    * no broken leaf traversal,
    * no corruption / CRC errors attributable to reclamation.
  * **Verify Cleanup Policy:** TTL may delay GC scheduling/metrics, but it must **not** allow reclamation of pages reachable from any pinned snapshot. (Pruner must remain blocked on reachability, not time.)

### 1.4 Slab Record Format (NEW)

* **Record Round-Trip:**

  * Write a large value record with key+value.
  * Read by `ValuePtr`, parse record, verify CRC32 and payload integrity.

* **Corrupt Record CRC:**

  * Flip one bit in `ValueBytes` or the CRC field.
  * **Assert:** `Get()` returns an error (or safe panic in lower-level tests), never silent corruption.

* **Record Enumeration:**

  * Append N records; iterate sequentially through slab parsing.
  * **Assert:** All records are discoverable and offsets advance correctly (no desync on variable lengths).

### 1.5 Superblocks (Redundant Meta) (NEW)

* **Dual Meta Selection:**

  * Create a DB, commit multiple times.
  * Corrupt Meta Page A (checksum mismatch).
  * **Assert:** Open selects Meta Page B and loads correctly.

* **Monotonic CommitSeq:**

  * Ensure `CommitSeq` increments exactly once per successful commit and never decreases across restarts.

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
  * Verify iterator continues to see a stable snapshot and successfully traverses `PrevLeafID` pointers even if pages were replaced in newer roots.

* **Concurrent Iteration Under Aggressive Pruning (NEW, MANDATORY):**

  * Start `Iterator` and `ReverseIterator` over a wide range (or full domain).
  * In parallel:

    * run continuous commits that rewrite leaves and internal nodes,
    * invoke `Prune()` frequently.
  * **Assert:** Both iterators complete with a logically consistent snapshot:

    * no missing keys relative to the snapshot start,
    * no duplicates,
    * correct ordering,
    * no leaf-link traversal failures,
    * no CRC/page-corruption errors caused by reclamation/reuse.

### 2.3 The "Zipper" Batch Merge

* **Atomicity:** Construct a Batch with 100 keys. Inject a panic/error at key 50 during commit. Reopen DB. Verify **0 keys** are present (no partial root commit).
* **Large Value Handling:** Batch insert 10 values of 10MB each. Verify `.slab` grows, but memory usage remains low.

### 2.4 NewBatchWithSize (NEW)

* **Smoke:** Call `NewBatchWithSize(1<<20)`, write keys, ensure correctness matches `NewBatch()`.
* **Hint Utilization (Optional):** Track internal capacity growth (via instrumentation) to ensure the hint reduces reallocations (non-functional, best-effort).

---

## 3. Advanced Stress Testing (Fuzzing)

Standard unit tests miss edge cases. We must use **Property-Based Testing** (e.g., `gopter` or Go 1.18+ Fuzzing).

### 3.1 Randomized "Model" Testing

* **The Oracle:** Maintain a simple Go `map[string][]byte` in memory (The Oracle).

* **The Subject:** The TreeDB instance.

* **The Loop:**

  1. Generate random op (`Put`, `Delete`, `Get`, `Iterator`, `ReverseIterator`, `Batch`, `WriteSync`, `Prune`).
  2. Apply to both Oracle and Subject (subject operations may be grouped into commits as required by the implementation).
  3. Compare output.
  4. **Invariant Check:** After every N ops, iterate the entire Subject (Forward AND Reverse) and verify it matches the Oracle 1:1 at the chosen snapshot boundary.
  5. **Restart:** Randomly close/reopen the DB to verify persistence.

* **Snapshot-Reachability Invariant (NEW):**

  * Randomly keep some iterators open across multiple commits while invoking `Prune()`.
  * **Assert:** Regardless of TTL and pruning activity, open iterators never observe missing keys, duplicated keys, broken traversal, or reclaimed-page corruption.

### 3.2 Fuzzing Inputs

* **Key Fuzzing:** Feed arbitrary bytes (including nulls, max-byte `0xFF`, and massive keys) into `Put`.

* **Corrupt Files:**

  1. Write a valid DB.
  2. Flip 1 bit in `index.db` or `data.slab`.
  3. Run `Open()` or `Get()`.
  4. **Expectation:** Error or safe panic, **NEVER** silent corrupt data.

* **Corrupt Superblock Targeting (NEW):**

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
  * Verify consistency: The DB should either be at State N or State N-1 (at worst last commit lost). It cannot be in a half-committed state.
  * **Leaf Link Integrity:** Verify `PrevLeafID` and `NextLeafID` chains are not broken (no loops or dead ends).

### 4.2 IO Failure Simulation

* **Disk Full:** Mock the filesystem to return `ENOSPC` during a slab write or index extension.

  * **Expectation:** Batch returns error. DB remains readable and consistent.

* **ReadOnly Mount:** Remount FS as Read-Only during operation.

### 4.3 Torn Write Recovery (UPDATED for Redundant Superblocks)

* **Scenario A (Slab durable, meta not committed):**

  1. Prepare a batch with large values.
  2. Inject a panic **after** slab `fsync` completes but **before** the meta page update is persisted.

* **Recovery:**

  1. Restart the DB.
  2. **Assert:** DB loads the previous superblock (highest valid `CommitSeq`).
  3. **Assert:** Orphaned slab records are ignored (unreferenced) and do not affect reads.

* **Scenario B (Torn meta write):**

  1. Crash during writing the inactive meta page (partial/torn).

* **Recovery:**

  1. Restart DB.
  2. **Assert:** Open selects the other meta page (older but valid) and DB is consistent.

### 4.4 Torn Index Page Recovery (Detection)

* **Scenario:**

  * Flip a bit in a non-meta B+Tree page used by the active root.

* **Expectation:**

  * CRC detects corruption; Open or Get fails loudly (no silent corruption).

---

## 5. Concurrency & Race Detection (Updated)

Go's `-race` detector is mandatory. We also use "Logical Race Tests" to verify the compaction protocol and ref-counted deletion.

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
  * underlying file exists on disk
  * iterator successfully reads values

* **Assert (After Close):**

  * Close iterator.
  * Verify `Slab.RefCount` drops to 0.
  * Verify file is deleted from disk.

### 5.3 The "Resurrection" Write (CAS Verification)

* **Scenario:**

  1. Compactor reads `Key A` (Val=1) from `OldSlab` and copies to `NewSlab`.
  2. Pause compactor just before CAS.
  3. User calls `Set(Key A, Val=2)` (points to write slab).
  4. Resume compactor.

* **Assert:**

  * CAS fails (returns false).
  * `Get(Key A)` returns Val=2 (user wins).
  * `NewSlab` contains garbage copy of Val=1, but index does not point to it.

### 5.4 Torn Compaction Recovery

* **Scenario:**

  1. Start compaction on a slab with 1,000 keys.
  2. Kill halfway through CAS loop (500 keys updated, 500 still old).

* **Recovery:**

  * Restart DB.
  * **Assert:** `Get()` works for all 1,000 keys.
  * **Assert:** `SlabManager` loads both slabs.
  * **Assert:** Old slab is not deleted while still referenced.

### 5.5 Compaction Serialization (NEW)

* **Scenario:**

  * Run continuous user batches.
  * Run compactor CAS commits in parallel.

* **Assert:**

  * All CAS commits occur under the same single-writer serialization as user commits (no interleaving that violates atomicity).
  * No lost updates: user write after a compaction copy must win if it changes the pointer.

### 5.6 Compaction Throttling

* **Scenario:**

  * Populate DB to trigger a large compaction (e.g., 2GB).
  * Configure leaky-bucket rate limiter to low value (e.g., 5MB/s).
  * Start compaction in goroutine.

* **Assert:**

  * Duration is at least `Size / Rate`.
  * Concurrent writer latency does not spike significantly (IO throttling effective).

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

  1. Commit 10,000 blocks.
  2. Set `Pruning = KeepRecent(100)`.
  3. Run `Prune()`.
  4. Verify disk usage drops (index pages reused; slab dead bytes reduced after compaction).
  5. Verify querying Block 1 fails (correctly).
  6. Verify querying Block 9,999 succeeds.


