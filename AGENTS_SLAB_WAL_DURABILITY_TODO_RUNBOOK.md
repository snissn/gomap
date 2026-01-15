# AGENTS: SlabWriter/WAL Durability & Concurrency Remediation (PR #50)

This document is a normative implementation runbook. The coding agent must follow it as a checklist.
Goal: land a durability-safe and panic-free implementation with deterministic regression tests under CI:
  go test -p 1 ./... -count=1 -timeout 10m

DO NOT do speculative redesigns. Make surgical edits only.

---

## 0) Normative Definitions / Contracts

### 0.1 Durability definitions (normative)
- Payload written:
  - slab file write completed (WriteBatch returned success), bytes visible in page cache.
- Payload durable:
  - slab file crossed an fsync boundary via SlabManager.Sync() (or equivalent).
- *Sync contract:
  - For SetSync / Batch.WriteSync: slab payload durable happens-before WAL/index/root commit durable happens-before caller ack.

### 0.2 WAL deletion safety (normative)
A WAL segment is deletable iff:
- For every slab file referenced by that segment:
  slabDurableEnd[fileID] >= walMaxEndByFile[fileID]
- AND the checkpoint/root that makes those pointers reachable is durable.

### 0.3 V2 Zone safety (normative)
No async flush may write a buffer that straddles ZoneSize unless explicitly in a safe boundary/header path.

### 0.4 Non-negotiable: no panics
No panics under concurrent Write/Flush/Sync/Close interleavings.

---

## 1) SlabWriter: Required State Machine (must implement exactly)

### 1.1 State variables
Under w.mu:
- activeBuf: []byte (MUST be non-nil at all times after NewSlabWriter returns)
- offset: uint64 (next logical write offset to assign)
- closed: bool
- err: error (terminal; once set, never cleared)
- rotating: bool (optional gate)
Additionally:
- stopCh: closed once to request flushLoop exit (DO NOT close pendingCh)
- doneCh: closed once flushLoop exits
- cond: *sync.Cond on w.mu (used for waiters + optional gating)
- ioMu: mutex to serialize all calls to slab file I/O (flushLoop and direct ignore-boundary writes)

### 1.2 Required invariants (must add comments and enforce)
INV-1: activeBuf is never nil while w.mu is unlocked.
INV-2: offset assignment is linearizable: every Write reserves its offset range under w.mu before any I/O.
INV-3: terminal error/closed causes Write/Flush/WaitForOffset/Sync to return immediately (no blocking).
INV-4: waiters are always woken on progress (durableSize update), Close, or terminal error.

### 1.3 Channel rule (critical)
- pendingCh MUST NOT be closed by producers.
- flushLoop exit is signaled via stopCh and doneCh only.

STOP CONDITION: if any implementation uses close(pendingCh) anywhere, reject and fix.

---

## 2) Execution Plan (must follow order; each step must end with tests)

### Step A (P0): Lock-gap corruption fix (rotateBufferLocked)
Goal: ensure INV-1 (activeBuf never nil under unlock) and no data loss under concurrent rotation.

Implementation recipe:
- Under w.mu:
  - if terminal (closed or err): return error immediately
  - rotate by:
      bufToFlush := activeBuf
      activeBuf = nextBuf[:0]  (ensure nextBuf non-nil)
    before any unlock or blocking
- After state is valid, enqueue bufToFlush (may block).
- If enqueue blocking must not stall other writers, add optional gate:
  - rotating=true while enqueueing; writers wait on cond until rotating=false
  - do NOT set activeBuf nil ever.

Add test:
- TestSlabWriter_ConcurrentWrites_Rotation_NoLoss

Run:
- go test -p 1 ./TreeDB/slab -run TestSlabWriter_ConcurrentWrites_Rotation_NoLoss -count=1 -timeout 60s

STOP CONDITION: any missing/overwritten bytes → fix before moving on.

---

### Step B (P0): Close/Write panic-proofing (no send on closed channel)
Goal: eliminate send-on-closed possibility.

Implementation recipe:
- Replace close(pendingCh) shutdown with:
  - Close(): under w.mu set closed=true; close(stopCh) via sync.Once; broadcast cond; unlock; wait for doneCh; return
  - flushLoop: select on pendingCh receive and stopCh; on stopCh drain behavior must be well-defined (either drain pending + flush or exit immediately; document choice).
- Writes must check terminal state under w.mu before enqueueing.
- If close occurs while enqueue blocked, writers must return ErrClosed (not deadlock).

Add tests:
- TestSlabWriter_CloseWhileWriting_NoPanic
- TestSlabWriter_Close_UnblocksPendingEnqueue

Run:
- go test -p 1 ./TreeDB/slab -run 'TestSlabWriter_CloseWhileWriting_NoPanic|TestSlabWriter_Close_UnblocksPendingEnqueue' -count=1 -timeout 60s

STOP CONDITION: any deadlock/hang → fix before moving on.

---

### Step C (P0): Error publication is lock-safe and wake-safe
Goal: flushLoop can publish error without deadlocking producers.

Implementation recipe:
- Introduce terminalErr setter:
  - firstErrOnce.Do(func(){ store err; close(stopCh); broadcast cond })
- Ensure flushLoop never needs to acquire w.mu while a producer might be blocked trying to enqueue.
- Any terminal err causes:
  - Write/Flush/WaitForOffset/Sync return immediately.

Add test:
- TestSlabWriter_FlushLoopError_NoDeadlock (stub slab file WriteBatch returns error while producer blocked)

Run:
- go test -p 1 ./TreeDB/slab -run TestSlabWriter_FlushLoopError_NoDeadlock -count=1 -timeout 60s

STOP CONDITION: deadlock or blocked goroutine → fix.

---

### Step D (P0): WaitForOffset / Sync / Close wakeups (no missed wakeups)
Goal: no missed-wakeup hangs; WaitForOffset returns ErrClosed if closed before target is reached.

Implementation recipe:
- WaitForOffset under w.mu:
  - loop while durableSize < target and not terminal:
      cond.Wait()
  - if err != nil return err
  - if closed && durableSize < target return ErrClosed
  - else return nil
- Ensure flushLoop updates durableSize then broadcasts.
- Ensure Close() broadcasts and closes doneCh after flushLoop exit.

Add tests:
- TestSlabWriter_WaitForOffset_Close_NoHang
- TestSlabWriter_Close_UnblocksWaiters

Run:
- go test -p 1 ./TreeDB/slab -run 'TestSlabWriter_WaitForOffset_Close_NoHang|TestSlabWriter_Close_UnblocksWaiters' -count=1 -timeout 60s

---

### Step E (P0): Serialize ignore-boundary direct writes + offset correctness
Goal: ignore-boundary direct writes cannot race buffered writes and cannot corrupt offsets.

Implementation recipe:
- Add ioMu; ALL slab file I/O must hold ioMu:
  - flushLoop around slab.WriteBatch
  - ignore-boundary path around Sync+WriteBatch
- For ignore-boundary:
  - Under w.mu: force flush/rotate, reserve offset range if needed, block writes via gate, release w.mu
  - Acquire ioMu, complete writes, release ioMu
  - Under w.mu: update state, ungate, broadcast cond
- Remove “snap to file size” unless proven identical to reserved range.

Add tests:
- TestSlabWriter_IgnoreBoundary_ConcurrentWrites_NoCorruption
- TestSlabWriter_RotateWhileFlushInFlight

Run:
- go test -p 1 ./TreeDB/slab -run 'TestSlabWriter_IgnoreBoundary_ConcurrentWrites_NoCorruption|TestSlabWriter_RotateWhileFlushInFlight' -count=1 -timeout 60s

STOP CONDITION: any corruption or nondeterminism → fix.

---

## 3) P0: Durability barrier wiring (payload vs WAL/index/root)

### 3.1 Required semantics (normative)
- WaitForOffset is a WRITTEN barrier only.
- SlabManager.Sync is the DURABILITY barrier (fsync).
- WriteSync / SetSync ordering:
    SlabManager.Flush() (best-effort) →
    WaitForOffset(maxEndByFile) (deterministic fileID order) →
    SlabManager.Sync() →
    WAL/index/root durable boundary →
    ack

### 3.2 Interface changes
- Add BackendDB.WaitForOffset(fileID, offset) or WaitForOffsets(map[fileID]offset).
- Multi-file waits MUST be deterministic:
  - sort fileIDs and wait in ascending order.

### 3.3 WAL deletion eligibility tracking
- Maintain slabDurableEnd[fileID] updated only after SlabManager.Sync returns success.
- Track walMaxEndByFile[fileID] per WAL segment (during write/segment finalize).
- Segment deletable iff:
  - for all fileID in segment map: slabDurableEnd[fileID] >= walMaxEndByFile[fileID]
  - AND checkpoint/root that makes pointers reachable is durable.

Add tests:
- TestDurability_SetSync_PointerWrite_DurableBeforeAck
- TestWALRotation_Safety_PausedSlabWriter
- TestWALDeletion_DoesNotAdvancePastSlabDurability
- TestDurability_WaitForOffsets_MultiSlab_Deterministic

Run:
- go test -p 1 ./TreeDB/caching -run 'TestDurability_.*|TestWAL.*' -count=1 -timeout 90s
- go test -p 1 ./... -count=1 -timeout 10m

STOP CONDITION: any ordering violation or flaky test → fix.

---

## 4) P1: V2 Zone boundary robustness
- Add TestSlabV2_Append_BoundaryEdge
- Add writer-level preflight for straddle; return typed error; manager rotates/inserts header then retries (bounded retries).

Run:
- go test -p 1 ./TreeDB/slab -run TestSlabV2_Append_BoundaryEdge -count=1 -timeout 60s

---

## 5) P1: WAL compression reader hardening
- Enforce: effectiveMax = min(configMax if set, hardCap)
- Validate length <= effectiveMax before any allocation; ErrCorrupt on violation.
- Add regression: huge compressed length yields clean error (no large alloc).

Run:
- go test -p 1 ./TreeDB/internal/wal -run 'Test.*' -count=1 -timeout 60s

---

## 6) Optional: Race check
- go test -race ./TreeDB/slab -run 'TestSlabWriter_.*' -count=1 -timeout 90s

---

## Work Log (append-only)
- YYYY-MM-DD: <what changed, tests run, result>

