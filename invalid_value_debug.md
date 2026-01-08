# Debugging Context: "invalid value id length in Get"

## 1. Issue Description
**Error:** `multistore restore: failed to load store: invalid value id length in Get: 0`
**Context:** This error occurred during a Celestia mainnet sync using `TreeDB` as the backend. The error originates from `TreeDB/db/db.go` (or `api.go`) when `Get()` encounters a leaf entry with the `FlagValueID` flag set, but the associated value payload is not exactly 8 bytes long (in this case, it was 0 bytes).

## 2. Configuration (from `celestia_testing_info.md`)
The environment where the error occurred used the following settings:
- `DB_BACKEND=treedb`
- `TREEDB_LEAF_PREFIX_COMPRESSION=1` (Enabled)
- `TREEDB_FORCE_VALUE_POINTERS=1` (Forces values to be pointer-backed, though behavior depends on inline threshold)
- `TREEDB_SLAB_COMPRESSION=zstd`

## 3. Root Cause Analysis
The error indicates database corruption. A `FlagValueID` entry serves as a pointer to the "Value Index" (System Tree), and it *must* contain exactly 8 bytes (the `ValueID` uint64).

The corruption likely occurred during the write path (`AddLeafEntry`). If an upstream component (e.g., `Batch` processing in `db/batch.go` or `Zipper` merge logic) incorrectly passed an empty slice for the value while setting `FlagValueID`, the node builder would previously write this invalid entry to disk without complaint. Upon subsequent read (`Get`), the validation check would fail.

**Suspect Areas:**
- `TreeDB/db/batch.go`: Logic that transforms large values into `ValueID`s (`write` / `writeSerialized`).
- `TreeDB/zipper/zipper.go`: Logic that merges batch entries into the tree.
- `TreeDB/node/builder.go` & `leaf.go`: The low-level routines writing the node format.

## 4. Fix Implementation
To prevent this corruption from being written to disk, strict validation was added to the node insertion paths.

**Files Modified:**
- `TreeDB/node/node.go`: Added `ErrInvalidValueIDLength` to common errors.
- `TreeDB/node/builder.go`: Updated `AddLeafEntry` to check:
  ```go
  if flags&FlagValueID != 0 && len(value) != 8 {
      return ErrInvalidValueIDLength
  }
  ```
- `TreeDB/node/leaf.go`: Updated `AddLeafEntry` (used for updates/inserts in existing nodes) with the same check.

Now, any attempt to write a malformed `ValueID` entry will return an error at write time, preventing corruption and likely identifying the buggy caller immediately.

## 5. Secondary Issue: `index.db` Ballooning
**Observation:** After fixing the crash, it was observed that `index.db` grew to 16GB for a workload with ~1.6GB of active data (~10x bloat).
**Root Cause:** The `ProfileFast` and `ProfileBench` high-level presets in `TreeDB/profiles.go` explicitly enabled `PreferAppendAlloc = true`. This flag tells the page allocator to ignore the freelist and always append new pages to the end of the file. This is intended for short, high-speed benchmarks but causes infinite file growth in long-running nodes (like Celestia sync) unless background vacuum is very aggressive.
**Fix:** Removed `PreferAppendAlloc = true` from the default profile definitions in `TreeDB/profiles.go`. This ensures that pages are reused from the freelist by default, maintaining a stable file size.

## 6. Tertiary Issue: Corrupted Slab Stats (26PB)
**Observation:** `treemap info` reported `treedb.slabs.total_bytes=26691663622943823` (approx 26 Petabytes) despite the slab file being only 4GB.
**Root Cause:** A data race was found in `TreeDB/db/batch.go`. The `writeOptimistic` and `writeSerialized` methods were taking a direct reference to the `SlabWriteBytesByFile` map from the batch. Since batches are pooled and reused, another goroutine could clear or modify this map while `finalizeCommit` (and `applySystemUpdates`) was still processing it, leading to corrupted summation.
**Fix:** Modified `TreeDB/db/batch.go` to always copy the map before passing it to `finalizeCommit`. Additionally, added safety caps in `applySystemUpdates` to ignore unreasonably large deltas (>100GB per commit).

## 7. Pruner Configurability
Added environment variable support for tuning background pruning without changing application code:
- `TREEDB_BACKGROUND_PRUNE_INTERVAL`: Duration (e.g. `100ms`) or milliseconds.
- `TREEDB_BACKGROUND_PRUNE_MAX_PAGES`: Integer (e.g. `8192`).

## 8. Regression Test
A new integrity test was added to verify robustness under the specific configuration used by Celestia.

**File:** `TreeDB/db/value_id_integrity_test.go`
**Test Name:** `TestValueID_Integrity`
**Details:**
- Configures DB with `EnableValueIndex`, `ForceValuePointers`, and `LeafPrefixCompression`.
- Performs 2000 inserts (mixed small and large values).
- Performs 200 updates.
- Reads all keys to verify integrity.
- Closes and reopens the DB to verify persistence.
