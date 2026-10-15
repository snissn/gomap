package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// AcquireNativeBuilderWithWorkV6 initializes the SAME builder types in the
// original admitted publication frame. It is nonblocking: preparing/closing
// refuses before an active-builder effect. No handle is exported or nested;
// the next handoff consumes it in this same nonyielding final invocation.
func (c *Coordinator) AcquireNativeBuilderWithWorkV6(r *NativePublicationV6, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || w == nil {
		return false, ErrBuilderLease
	}
	bytes := uint64(unsafe.Sizeof(*r)) + uint64(unsafe.Sizeof(builderLease{})) + uint64(unsafe.Sizeof(builderHandle{})) + uint64(unsafe.Sizeof(BuilderToken{})) + 32
	if !w.Reserve(2, bytes) {
		return false, nil
	}
	r.builderHandle.mu.Lock()
	defer r.builderHandle.mu.Unlock()
	r.builderLease.mu.Lock()
	defer r.builderLease.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound || r.complete || r.turn || r.handoffReady {
		return false, ErrBuilderLease
	}
	if r.builderReady {
		return !r.builderHandle.released && !r.builderLease.released, nil
	}
	if err := c.terminalErrorLocked(); err != nil {
		return false, err
	}
	if c.preparing || c.activeBuilders == ^uint64(0) {
		return false, nil
	}
	r.builderLease.coordinator, r.builderLease.refs = c, 1
	r.builderHandle.lease = &r.builderLease
	r.builderToken.handle = &r.builderHandle
	r.builderReady = true
	c.activeBuilders++
	return true, nil
}

// ReleaseNativeBuilderWithWorkV6 closes only the original unconsumed builder
// control. Its original notify/wake operations are admitted before mutation;
// all aliases remain in the SAME containing frame until terminal completion.
func (c *Coordinator) ReleaseNativeBuilderWithWorkV6(r *NativePublicationV6, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || w == nil {
		return false, ErrBuilderLease
	}
	if !w.Reserve(3, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(builderHandle{}))+uint64(unsafe.Sizeof(builderLease{}))+24) {
		return false, nil
	}
	notify, ready, err := admitNativeNotifyV6(w)
	if !ready || err != nil {
		return false, err
	}
	wake, ready, err := admitNativeWakeV6(w)
	if !ready || err != nil {
		return false, err
	}
	r.builderHandle.mu.Lock()
	defer r.builderHandle.mu.Unlock()
	r.builderLease.mu.Lock()
	defer r.builderLease.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound || r.complete {
		return false, ErrBuilderLease
	}
	if !r.builderReady || r.builderHandle.released {
		return true, nil
	}
	if r.builderHandle.lease != &r.builderLease || r.builderLease.coordinator != c || r.builderLease.released || r.builderLease.refs != 1 || c.activeBuilders == 0 {
		return false, ErrBuilderLease
	}
	r.builderHandle.released, r.builderHandle.lease = true, nil
	r.builderLease.refs, r.builderLease.released = 0, true
	c.activeBuilders--
	notify.commit(c)
	wake.commit(c)
	return true, nil
}

// EnqueueNativeBuiltWithWorkV6 uses the original atomic enqueue body and SAME
// builder lease. Success consumes those original controls and initializes the
// original handoff receipt in-place. Refusal retains builder/candidate custody;
// the caller uses ReleaseNativeBuilderWithWorkV6 before any frame refund.
func (c *Coordinator) EnqueueNativeBuiltWithWorkV6(r *NativePublicationV6, candidate *PreparedRootCandidate, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || candidate == nil || w == nil {
		return false, ErrBuilderLease
	}
	bytes := uint64(unsafe.Sizeof(*r)) + uint64(unsafe.Sizeof(builderHandle{})) + uint64(unsafe.Sizeof(builderLease{})) + uint64(unsafe.Sizeof(BuilderHandoffReceipt{})) + 24
	// Original handle/lease validation, final builder consumption, receipt
	// initialization. These are admitted before entering enqueue/activation.
	if !w.Reserve(3, bytes) {
		return false, nil
	}
	notify, ready, err := admitNativeNotifyV6(w)
	if !ready || err != nil {
		return false, err
	}
	wake, ready, err := admitNativeWakeV6(w)
	if !ready || err != nil {
		return false, err
	}
	r.builderHandle.mu.Lock()
	defer r.builderHandle.mu.Unlock()
	r.builderLease.mu.Lock()
	defer r.builderLease.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound || r.complete || !r.builderReady || r.handoffReady || r.builderToken.handle != &r.builderHandle || r.builderHandle.released || r.builderHandle.lease != &r.builderLease || r.builderLease.released || r.builderLease.coordinator != c || r.builderLease.refs != 1 || c.activeBuilders == 0 {
		return false, ErrBuilderLease
	}
	decision, err := c.enqueueLockedWithNativeReceiptV6(candidate, false, w, r)
	if err != nil {
		return false, err
	}
	// The original enqueue crossed its last fallible operation. All remaining
	// work, backing and notifications were admitted before that boundary.
	r.builderHandle.released, r.builderHandle.lease = true, nil
	r.builderLease.refs, r.builderLease.released = 0, true
	c.activeBuilders--
	r.handoff = BuilderHandoffReceipt{coordinator: c, sequence: decision.sequence, failureGeneration: decision.failureGeneration, pendingBytes: c.pendingBytes, pendingCommits: uint64(len(c.pending)), hard: decision.hard}
	r.handoffReady = true
	notify.commit(c)
	wake.commit(c)
	return true, nil
}
