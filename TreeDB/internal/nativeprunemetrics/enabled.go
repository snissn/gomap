//go:build mvcc_native_prune

package nativeprunemetrics

import (
	"errors"
	"os"
	"sync/atomic"
	"time"
)

const Enabled = true

var active atomic.Bool
var syncs [syncKindCount]struct{ attempts, failures atomic.Uint64 }
var fences [fenceCount]struct{ foreground, waitMax, holds, holdMax, writers, writerWaitMax atomic.Uint64 }

// Begin is called only after setup is quiescent. One fresh-process case owns
// these scalars; concurrent/resetting collectors are deliberately unsupported.
func Begin() {
	active.Store(false)
	for i := range syncs {
		syncs[i].attempts.Store(0)
		syncs[i].failures.Store(0)
	}
	for i := range fences {
		fences[i].foreground.Store(0)
		fences[i].waitMax.Store(0)
		fences[i].holds.Store(0)
		fences[i].holdMax.Store(0)
		fences[i].writers.Store(0)
		fences[i].writerWaitMax.Store(0)
	}
	active.Store(true)
}
func Stop() { active.Store(false) }
func Start() Stamp {
	if active.Load() {
		return time.Now()
	}
	return Stamp{}
}
func max(dst *atomic.Uint64, v uint64) {
	for old := dst.Load(); v > old; old = dst.Load() {
		if dst.CompareAndSwap(old, v) {
			return
		}
	}
}
func elapsed(start Stamp) uint64 {
	if start.IsZero() {
		return 0
	}
	n := time.Since(start).Nanoseconds()
	if n < 0 {
		return 0
	}
	return uint64(n)
}
func Foreground(site Fence, start Stamp) {
	if start.IsZero() {
		return
	}
	fences[site].foreground.Add(1)
	max(&fences[site].waitMax, elapsed(start))
}
func Hold(site Fence, start Stamp) {
	if start.IsZero() {
		return
	}
	fences[site].holds.Add(1)
	max(&fences[site].holdMax, elapsed(start))
}

// Barrier runs only at an actual physical call, including a failed attempt.
// No certification, clone, logical SyncThrough, or observer invocation counts.
func Barrier(kind SyncKind, err error) {
	if !active.Load() {
		return
	}
	syncs[kind].attempts.Add(1)
	if err != nil {
		syncs[kind].failures.Add(1)
	}
}

// SyncFile counts entered file-barrier operations, not Go-internal EINTR retry
// syscalls. Closed/invalid-handle rejection executes no storage barrier.
func SyncFile(file *os.File, kind SyncKind) error {
	err := file.Sync()
	if !errors.Is(err, os.ErrClosed) && !errors.Is(err, os.ErrInvalid) {
		Barrier(kind, err)
	}
	return err
}
func Snapshot() Receipt {
	r := Receipt{Scope: "combined_process_events_since_begin_including_ack_reopen_cleanup_close"}
	r.FileAttempts, r.FileFailures = syncs[File].attempts.Load(), syncs[File].failures.Load()
	r.NamespaceAttempts, r.NamespaceFailures = syncs[Namespace].attempts.Load(), syncs[Namespace].failures.Load()
	r.MmapAttempts, r.MmapFailures = syncs[Mmap].attempts.Load(), syncs[Mmap].failures.Load()
	r.DurableWriteAttempts, r.DurableWriteFailures = syncs[DurableWrite].attempts.Load(), syncs[DurableWrite].failures.Load()
	r.StorageSyncCount = r.FileAttempts + r.NamespaceAttempts + r.MmapAttempts + r.DurableWriteAttempts
	for i := range fences {
		f := &fences[i]
		r.Fences[i] = FenceStats{fenceNames[i], f.foreground.Load(), f.waitMax.Load(), f.holds.Load(), f.holdMax.Load(), f.writers.Load(), f.writerWaitMax.Load()}
		r.FenceWaitMaxNS = maxValue(r.FenceWaitMaxNS, r.Fences[i].ForegroundWaitMaxNS)
		r.FenceHoldMaxNS = maxValue(r.FenceHoldMaxNS, r.Fences[i].MaintenanceHoldMaxNS)
	}
	return r
}
func maxValue(a, b uint64) uint64 {
	if b > a {
		return b
	}
	return a
}

// Backend batch/build admission is shared with cache flush/background writers.
// Its waits are truthful writer receipts, never attributed as foreground waits.
func Writer(site Fence, start Stamp) {
	if start.IsZero() {
		return
	}
	fences[site].writers.Add(1)
	max(&fences[site].writerWaitMax, elapsed(start))
}
