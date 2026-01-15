# Agent TODO: SlabWriter/WAL Durability & Concurrency Remediation (PR #50)

This file is the TODO checklist + work log for resolving deep-dive review concerns around:
- `SlabWriter` concurrency/shutdown correctness
- durability barriers between async slab payloads and WAL/index metadata
- V2 zone boundary robustness
- WAL segment compression/read-safety hardening

Goal: land a **durability-safe** and **panic-free** implementation with deterministic regression tests that run under tight timeouts in CI (`go test -p 1 ./...`).

---

## Non‑Negotiable Contracts (must hold)

- **No panics** under concurrent `Write/Flush/Sync/Close` interleavings.
- **Durability definitions (normative):**
  - **Payload written:** bytes have been appended to the slab file (write completed; visible in page cache).
  - **Payload durable:** slab file has crossed an fsync boundary (`SlabManager.Sync()` or equivalent platform durability boundary), not merely that `WaitForOffset` advanced.
- **`*Sync` durability:** slab payload durability boundary (fsync) happens‑before WAL/index/root commit durability boundary (and the caller ack).
- **WAL deletion safety:** no WAL segment containing the only durable record of a write is deleted before the corresponding payload is durable elsewhere.
- **V2 safety:** no async flush writes a buffer that straddles a `ZoneSize` boundary (unless explicitly in a safe “boundary/header” path).

---

## P0 (Blockers) — SlabWriter Concurrency & Shutdown

- [x] **Fix Data Corruption (Lock Gap):** rewrite `rotateBufferLocked` so `w.mu` is never released while `w.activeBuf` is in an invalid/transient state (e.g. `nil`).
  - [x] Preferred invariant: under `w.mu`, keep `activeBuf` always non‑nil; rotate by swapping `bufToFlush := activeBuf; activeBuf = nextBuf[:0]` *before* any unlock; only then enqueue `bufToFlush` (it may block, that’s fine).
  - [ ] If rotation must unblock writers while enqueue is blocked, implement a writer gate (`rotating`/`writing` + `sync.Cond`) so concurrent `Write()` calls wait until rotation completes.
  - [x] Add regression: `TestSlabWriter_ConcurrentWrites_Rotation_NoLoss` (forces many concurrent rotates and verifies no missing/overwritten bytes).

- [x] **Close/Write panic-proofing:** remove `close(pendingCh)` producer-side shutdown; use `stopCh`/state so writers never send on a closed channel.
  - [x] Add regression: `TestSlabWriter_CloseWhileWriting_NoPanic` (`TreeDB/slab/writer_concurrency_test.go`).
  - [x] Add regression: `TestSlabWriter_Close_UnblocksPendingEnqueue` (fill `pendingCh`, block flushLoop, `Close()` must not deadlock; writers return `ErrClosed`).

- [x] **Error propagation without deadlocks:** flush goroutine must not block on writer mutex to publish errors.
  - [x] Implement lock-free “first error wins” (e.g. `sync.Once` + `atomic.Value`).
  - [x] Sticky terminal error state: once set, all subsequent `Write/Flush/WaitForOffset/Sync` return immediately without blocking.
  - [x] Add regression: `TestSlabWriter_FlushLoopError_NoDeadlock` with a stub `WriteBatch` that returns error while producer is blocked.

- [x] **WaitForOffset/Close correctness:** eliminate missed-wakeup hangs.
  - [x] Ensure waiters are always woken when flushLoop exits (`defer signalDurable(); close(doneCh)` ordering).
  - [x] Ensure `WaitForOffset` returns `ErrClosed` if the writer closes before the target offset is reached.
  - [x] Add regression: `TestSlabWriter_WaitForOffset_Close_NoHang`.
  - [x] Add regression: `TestSlabWriter_Close_UnblocksWaiters` (covers `Sync()` and `WaitForOffset` concurrently).

- [ ] **Serialize ignore-boundary direct writes:** `WriteBatch(ignoreBoundary=true)` must not race with concurrent buffered writes.
  - [ ] Add `ioMu` (or equivalent) to serialize file I/O between flushLoop and direct writes.
  - [ ] Block concurrent `Write()` during ignore-boundary “flush + direct write + resync offset”.
  - [ ] Offset correctness invariant: offsets returned to callers are reserved under `w.mu` before I/O begins; avoid “snap to file size” corrections unless proven equivalent to the reserved range.
  - [ ] Add regression: `TestSlabWriter_IgnoreBoundary_ConcurrentWrites_NoCorruption`.
  - [ ] Add regression: `TestSlabWriter_RotateWhileFlushInFlight` (file switch / rotation during an in-flight flush).

- [ ] **Queue depth sanity:** consider raising `pendingCh` capacity (or moving to bounded queue) to avoid pathological backpressure stalls.

---

## P0 (Blockers) — Durability Barrier (payload vs WAL/index metadata)

- [ ] **Expose a batch-level slab durability barrier** to preserve group-commit performance (avoid per-record fsync).
  - [ ] Add `WaitForOffset(fileID, offset)` to the caching layer `BackendDB` interface.
  - [ ] Implement `WaitForOffset` in backends (including `TreeDB/tree/tree.go`) by delegating to `SlabManager.WaitForOffset`.
  - [ ] Sync ordering for `*Sync` paths:
    - [ ] `WaitForOffset` is a **written** barrier (write completion), not an fsync boundary.
    - [ ] `SlabManager.Sync()` is the **durability** barrier (fsync boundary).
    - [ ] `Batch.WriteSync` and cached `SetSync` must: `SlabManager.Flush()` (best-effort) → `WaitForOffset(maxEndByFile)` → `SlabManager.Sync()` → WAL/index/meta durability boundary.
    - [ ] Multi-file batches: gather `maxEndByFile` and wait deterministically (iterate slab fileIDs in sorted order).
  - [ ] Avoid “AppendSync per write”; use “append N records → wait once (max offsets) → commit”.

- [ ] **Enforce ordering in cached write path:**
  - [ ] If cached `SetSync` emits slab pointer via backend append, it must wait for slab durability barrier **before** WAL fsync/ack and before the pointer can be recovered from WAL.

- [ ] **Prevent premature WAL deletion:**
  - [ ] Ensure any path that deletes WAL segments (checkpoint/flush) establishes the slab durability barrier first; only then rotate/delete WAL segments.
  - [ ] Add explicit eligibility rule with watermarks:
    - [ ] Maintain per-slab durability watermark `slabDurableEnd[fileID]` updated only after `SlabManager.Sync()`.
    - [ ] Track per-WAL-segment `walMaxEndByFile[fileID]` for any slab offsets referenced by that segment.
    - [ ] Segment deletable iff for all referenced slab files: `slabDurableEnd[fileID] >= walMaxEndByFile[fileID]` **and** the corresponding checkpoint/root that makes those pointers reachable is durable.

**Required tests (tight timeouts):**
- [ ] `TestDurability_SetSync_PointerWrite_DurableBeforeAck` (pause slab flush loop; ensure `SetSync` blocks until unpaused).
- [ ] `TestWALRotation_Safety_PausedSlabWriter` (WAL segment must not be deleted while payload still only in RAM).
- [ ] `TestWALDeletion_DoesNotAdvancePastSlabDurability` (checkpoint/cleanup respects slab durability watermark).
- [ ] `TestDurability_WaitForOffsets_MultiSlab_Deterministic` (touch 2 slabs in one sync batch; pause one; ensure wait order is deterministic and completes when unpaused).

---

## P1 — V2 Boundary Robustness

- [ ] **Edge-case boundary test:** write record ending at `ZoneSize-1`, then a small record; must not fail with `ErrRecordTooLarge`.
  - [ ] Add regression: `TestSlabV2_Append_BoundaryEdge`.

- [ ] **Belt-and-suspenders check:** add writer-level preflight that refuses to buffer payload that would straddle a boundary (returns typed error so manager can rotate/insert header and retry).

---

## P1 — WAL Compression/Reader Hardening

- [ ] **Validate segment lengths before allocation (single rule):**
  - [ ] Compute `effectiveMax = min(configMax (if set), hardCap)` and enforce `length <= effectiveMax` before any allocation (prevents OOM from `FlagCompressed` MSB or corrupt headers), returning `ErrCorrupt`.
- [ ] Add regression: “compressed flag with huge length” yields clean error (no large alloc).

---

## P2 — Performance Follow-ups (only after correctness)

- [ ] Audit memtable flush paths for avoidable key/value copying; prefer view-based APIs (`SetView/DeleteView`) or pooled arenas where lifetime requires copying.
- [ ] Add/refresh benchmarks for flush path allocation counts and throughput.

---

## Platform Notes

- Windows CI lacks some online-vacuum/index-swap features; tests must `t.Skip` where unsupported.
- Keep regression tests deterministic: avoid long sleeps; prefer hooks/latches; set per-test timeouts.
- Durability tests should validate **ordering** via hooks/latches rather than attempting to prove physical persistence across OS/filesystems.

---

## Validation Commands (use short timeouts)

- `go test -p 1 ./TreeDB/slab -count=1 -timeout 60s`
- `GOMAXPROCS=2 go test ./TreeDB/caching -run TestConsistencyStress -count=1 -timeout 90s`
- `go test -p 1 ./... -count=1 -timeout 10m` (only once P0/P1 are green)
- `go test -race ./TreeDB/slab -run 'TestSlabWriter_.*' -count=1 -timeout 90s` (optional; keep small)

---

## Work Log

- 2026-01-15: Added regression for DB.Close nil slabManager race; fixed by snapshotting managers and returning `ErrClosed`.
- 2026-01-15: Fixed `SlabWriter.rotateBufferLocked` “lock gap” hang under concurrent rotate/flush (macOS CI timeout).
- 2026-01-15: Reworked `SlabWriter.rotateBufferLocked` to keep `activeBuf` non-nil during rotation; added `TestSlabWriter_ConcurrentWrites_Rotation_NoLoss` (go test -p 1 ./TreeDB/slab -run TestSlabWriter_ConcurrentWrites_Rotation_NoLoss -count=1 -timeout 60s).
- 2026-01-15: Replaced Close shutdown with stop/done channels; gated rotation to avoid send-on-closed and deadlock; added `TestSlabWriter_CloseWhileWriting_NoPanic` and `TestSlabWriter_Close_UnblocksPendingEnqueue` (go test -p 1 ./TreeDB/slab -run 'TestSlabWriter_CloseWhileWriting_NoPanic|TestSlabWriter_Close_UnblocksPendingEnqueue' -count=1 -timeout 60s).
- 2026-01-15: Added terminal error publication via atomic once; added `TestSlabWriter_FlushLoopError_NoDeadlock` (go test -p 1 ./TreeDB/slab -run TestSlabWriter_FlushLoopError_NoDeadlock -count=1 -timeout 60s).
- 2026-01-15: Switched WaitForOffset/Sync waiters to w.cond with flushLoop exit wakeups; added `TestSlabWriter_WaitForOffset_Close_NoHang` and `TestSlabWriter_Close_UnblocksWaiters` (go test -p 1 ./TreeDB/slab -run 'TestSlabWriter_WaitForOffset_Close_NoHang|TestSlabWriter_Close_UnblocksWaiters' -count=1 -timeout 60s).
