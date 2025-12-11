This addendum outlines the rigorous testing protocols required to certify **GoSlabDB v2.1** for production use in the Cosmos ecosystem. Given the catastrophic nature of database corruption in blockchain applications, this suite prioritizes correctness, crash recovery, and concurrency safety above raw performance.

---

# Addendum A: Testing Specification

## 1. Unit Testing Strategies

Focus on isolating the complex low-level data structures before system integration.

### 1.1 The Pager (Chunked MMap)
* **Boundary Crossing:** Initialize with a small chunk size (e.g., 1KB) to force frequent chunk allocations. Verify reads spanning across chunk boundaries (`Offset` in Chunk A, `Length` extends into Chunk B).
* **Growth Safety:** Simulate file growth (Truncate + Remap) while a "Reader" holds a pointer to the old memory region. Ensure no panic/segfault occurs.
* **Ref-Counting:** Verify that `Unmap` is never called on active chunks.

### 1.2 Slotted Pages (Variable Keys)
* **Insertion Logic:**
    * Insert keys in random order; verify the `Directory` array remains sorted.
    * Insert keys of varying lengths (1 byte to 1KB).
* **Split Logic:**
    * Fill a page to exactly 4096 bytes. Verify the split occurs at the median key.
    * **Edge Case:** Insert a single key larger than `PageSize / 2`. (Should trigger an immediate split or dedicated overflow page logic, though spec currently relies on MaxKeySize constraints).
* **Defragmentation:** Delete every other key in a page. Verify that subsequent writes utilize the freed space (compaction within the page heap).

### 1.3 The Graveyard (Epochs)
* **Hold & Release:**
    1.  Create `Iterator A` (holds Seq 10).
    2.  Perform `Write Batch` (generates "Old Pages").
    3.  Verify "Old Pages" are in Graveyard but **not** in On-Disk Freelist.
    4.  Close `Iterator A`.
    5.  Run `Prune()`. Verify pages move to On-Disk Freelist.
* **Reuse:** Verify that the Allocator actually pulls IDs from the Freelist before growing the file.

---

## 2. Integration & Functional Testing

Focus on the public `DB` interface and ACID properties.

### 2.1 Basic CRUD
* **CRUD Cycle:** `Put(K, V)` -> `Get(K)` -> `Delete(K)` -> `Get(K)` (expect NotFound).
* **Overwrite:** `Put(K, V1)` -> `Put(K, V2)`. Ensure `Get(K)` returns V2 and the space for V1 is eventually reclaimed (if checking internals).
* **Persistence:** `Put(K, V)` -> `Close()` -> `Reopen()` -> `Get(K)`.

### 2.2 Range Scans (Iterators)
* **Forward Scan:** Insert keys `A, C, E`. Scan `A..Z`. Expect `A, C, E`.
* **Boundary Conditions:**
    * Scan `start=nil` (First key).
    * Scan `end=nil` (Last key).
    * Scan range with no matching keys.
* **Concurrent Iteration:**
    * Start Iterator.
    * Delete half the keys in the range.
    * Verify Iterator still sees the "Snapshot" state (old keys).

### 2.3 The "Zipper" Batch Merge
* **Atomicity:** Construct a Batch with 100 keys. Inject a panic/error at key 50 during commit. Reopen DB. Verify **0 keys** are present.
* **Large Value Handling:** Batch insert 10 values of 10MB each. Verify `.slab` file grows, but memory usage remains low.

---

## 3. Advanced Stress Testing (Fuzzing)

Standard unit tests miss edge cases. We must use **Property-Based Testing** (e.g., `gopter` or Go 1.18+ Fuzzing).

### 3.1 Randomized "Model" Testing
* **The Oracle:** Maintain a simple Go `map[string][]byte` in memory (The Oracle).
* **The Subject:** The GoSlabDB instance.
* **The Loop:**
    1.  Generate random op (`Put`, `Delete`, `Get`, `Batch`).
    2.  Apply to both Oracle and Subject.
    3.  Compare output.
    4.  **Invariant Check:** After every N ops, iterate the entire Subject and verify it matches the Oracle 1:1.
    5.  **Restart:** Randomly close/reopen the DB to verify persistence.

### 3.2 Fuzzing Inputs
* **Key Fuzzing:** Feed arbitrary bytes (including nulls, max-byte `0xFF`, and massive keys) into `Put`.
* **Corrupt Files:**
    1.  Write a valid DB.
    2.  Flip 1 bit in `index.db` or `data.slab`.
    3.  Run `Open()` or `Get()`.
    4.  **Expectation:** Error or Panic (Safe), **NEVER** return silent corrupt data.

---

## 4. Failure Injection (Crash Recovery)

Simulate power loss to ensure durability.

### 4.1 The "Kill" Test
* **Setup:** A separate process writes to the DB continuously.
* **Trigger:** `kill -9` the process randomly.
* **Recovery:**
    * Restart process.
    * Verify consistency: The DB should either be at State N or State N-1. It cannot be in a half-committed state (e.g., Key A exists but Key B from the same batch is missing).
* **Slab Integrity:** Verify the `.slab` file doesn't contain half-written values that cause the parser to hang.

### 4.2 IO Failure Simulation
* **Disk Full:** Mock the filesystem to return `ENOSPC` (No space left on device) during a `.slab` write or `.index` extension.
    * **Expectation:** The Batch must return an error. The DB must remain readable and consistent.
* **ReadOnly Mount:** Remount FS as Read-Only during operation.

---

## 5. Concurrency & Race Detection

Go's `-race` detector is mandatory, but we need logical race tests too.

### 5.1 The Reader/Writer Duel
* **Routine 1 (Writer):** Continually updates `Key1` with incrementing integers (1, 2, 3...).
* **Routine 2 (Reader):** Continually reads `Key1`.
* **Assert:** Reader always sees a monotonically increasing number. It never sees a "torn read" (e.g., a mix of bytes from Value 2 and Value 3).

### 5.2 Compaction Race
* **Scenario:**
    * Fill DB to trigger Slab Rotation.
    * Start a long-running Iterator on the "Old Slab".
    * Trigger Slab GC (which deletes Old Slabs).
    * **Assert:** The GC must **wait** or **skip** deletion because the Iterator holds a reference. The Iterator must successfully read the values.

---

## 6. Cosmos-Specific Compliance

The database must pass the Cosmos SDK integration definition.

### 6.1 IAVL Compatibility
* **Import/Export:** Load a standard IAVL snapshot (approx 10GB). Measure time and memory.
* **Proof Generation:** Ensure point lookups (needed for Merkle paths) are efficient (cached pages).

### 6.2 Pruning Validation
* **Scenario:**
    1.  Commit 10,000 blocks.
    2.  Set `Pruning = KeepRecent(100)`.
    3.  Run `Prune()`.
    4.  Verify disk usage drops (Index pages reused).
    5.  Verify querying Block 1 fails (correctly).
    6.  Verify querying Block 9,999 succeeds.
