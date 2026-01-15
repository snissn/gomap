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
- **`*Sync` durability:** when `SetSync` / `WriteSync` returns success, referenced slab payload bytes are durable **before** metadata commit is durable.
- **WAL deletion safety:** no WAL segment containing the only durable record of a write is deleted before the corresponding payload is durable elsewhere.
- **V2 safety:** no async flush writes a buffer that straddles a `ZoneSize` boundary (unless explicitly in a safe “boundary/header” path).

---

## P0 (Blockers) — SlabWriter Concurrency & Shutdown

- [ ] **Close/Write panic-proofing:** remove `close(pendingCh)` producer-side shutdown; use `stopCh`/state so writers never send on a closed channel.
  - [ ] Add regression: `TestSlabWriter_CloseWhileWriting_NoPanic` (`TreeDB/slab/writer_concurrency_test.go`).

- [ ] **Error propagation without deadlocks:** flush goroutine must not block on writer mutex to publish errors.
  - [ ] Implement lock-free “first error wins” (e.g. `sync.Once` + `atomic.Value`).
  - [ ] Add regression: `TestSlabWriter_FlushLoopError_NoDeadlock` with a stub `WriteBatch` that returns error while producer is blocked.

- [ ] **WaitForOffset/Close correctness:** eliminate missed-wakeup hangs.
  - [ ] Ensure waiters are always woken when flushLoop exits (`defer signalDurable(); close(doneCh)` ordering).
  - [ ] Add regression: `TestSlabWriter_WaitForOffset_Close_NoHang`.

- [ ] **Serialize ignore-boundary direct writes:** `WriteBatch(ignoreBoundary=true)` must not race with concurrent buffered writes.
  - [ ] Add `ioMu` (or equivalent) to serialize file I/O between flushLoop and direct writes.
  - [ ] Block concurrent `Write()` during ignore-boundary “flush + direct write + resync offset”.
  - [ ] Add regression: `TestSlabWriter_IgnoreBoundary_ConcurrentWrites_NoCorruption`.

- [ ] **Queue depth sanity:** consider raising `pendingCh` capacity (or moving to bounded queue) to avoid pathological backpressure stalls.

---

## P0 (Blockers) — Durability Barrier (payload vs WAL/index metadata)

- [ ] **Add a backend “sync append” capability** so `SetSync` cannot acknowledge until slab payload is durable.
  - [ ] Option A (preferred): `AppendSync(key,value) (page.ValuePtr,error)` on backend.
  - [ ] Option B: `AppendWithOptions(..., {Sync:true})`.
  - [ ] Plumb through `TreeDB/caching.BackendDB` and implement in `TreeDB/db` via `SlabManager`/`SlabWriter`.

- [ ] **Enforce ordering in cached write path:**
  - [ ] If cached `SetSync` emits slab pointer via backend append, it must wait for slab durability **before** WAL fsync/ack.

- [ ] **Prevent premature WAL deletion:**
  - [ ] Ensure any path that deletes WAL segments (checkpoint/flush) has established an equivalent backend durability boundary covering those writes.

**Required tests (tight timeouts):**
- [ ] `TestDurability_SetSync_PointerWrite_DurableBeforeAck` (pause slab flush loop; ensure `SetSync` blocks until unpaused).
- [ ] `TestWALRotation_Safety_PausedSlabWriter` (WAL segment must not be deleted while payload still only in RAM).

---

## P1 — V2 Boundary Robustness

- [ ] **Edge-case boundary test:** write record ending at `ZoneSize-1`, then a small record; must not fail with `ErrRecordTooLarge`.
  - [ ] Add regression: `TestSlabV2_Append_BoundaryEdge`.

- [ ] **Belt-and-suspenders check:** add writer-level preflight that refuses to buffer payload that would straddle a boundary (returns typed error so manager can rotate/insert header and retry).

---

## P1 — WAL Compression/Reader Hardening

- [ ] **Reader hard cap even when MaxSegmentSize disabled:** prevent OOM on corrupt lengths; return `ErrCorrupt` before allocation.
- [ ] Add regression: “compressed flag with huge length” yields clean error (no large alloc).

---

## P2 — Performance Follow-ups (only after correctness)

- [ ] Audit memtable flush paths for avoidable key/value copying; prefer view-based APIs (`SetView/DeleteView`) or pooled arenas where lifetime requires copying.
- [ ] Add/refresh benchmarks for flush path allocation counts and throughput.

---

## Platform Notes

- Windows CI lacks some online-vacuum/index-swap features; tests must `t.Skip` where unsupported.
- Keep regression tests deterministic: avoid long sleeps; prefer hooks/latches; set per-test timeouts.

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
