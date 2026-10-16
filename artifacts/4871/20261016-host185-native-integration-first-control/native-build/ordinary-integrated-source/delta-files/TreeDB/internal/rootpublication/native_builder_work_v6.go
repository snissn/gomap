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
	p, ready, err := c.AdmitNativeBuilderReleaseV6(r, w)
	if !ready || err != nil {
		return false, err
	}
	return p.Release()
}

// NativeBuilderReleasePlanV6 covers the original refusal/unwind before builder
// acquisition. It is local to that nonyielding invocation and owns no edge.
type NativeBuilderReleasePlanV6 struct {
	coordinator *Coordinator
	receipt     *NativePublicationV6
	notify      nativeNotifyPermitV6
	wake        nativeWakePermitV6
	admitted    bool
}

func (c *Coordinator) AdmitNativeBuilderReleaseV6(r *NativePublicationV6, w *iterator.OrdinalScanWork) (NativeBuilderReleasePlanV6, bool, error) {
	var p NativeBuilderReleasePlanV6
	if c == nil || r == nil || w == nil {
		return p, false, ErrBuilderLease
	}
	if !w.Reserve(4, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(builderHandle{}))+uint64(unsafe.Sizeof(builderLease{}))+2*uint64(unsafe.Sizeof(p))+24) {
		return p, false, nil
	}
	notify, ready, err := admitNativeNotifyV6(w)
	if !ready || err != nil {
		return p, false, err
	}
	wake, ready, err := admitNativeWakeV6(w)
	if !ready || err != nil {
		return p, false, err
	}
	p.coordinator, p.receipt, p.notify, p.wake, p.admitted = c, r, notify, wake, true
	return p, true, nil
}

func (p *NativeBuilderReleasePlanV6) Release() (bool, error) {
	if p == nil || !p.admitted {
		return false, ErrBuilderLease
	}
	c, r := p.coordinator, p.receipt
	r.builderHandle.mu.Lock()
	defer r.builderHandle.mu.Unlock()
	r.builderLease.mu.Lock()
	defer r.builderLease.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound || r.complete || r.handoffReady {
		return false, ErrBuilderLease
	}
	if !r.builderReady || r.builderHandle.released {
		p.admitted = false
		return true, nil
	}
	if r.builderHandle.lease != &r.builderLease || r.builderLease.coordinator != c || r.builderLease.released || r.builderLease.refs != 1 || c.activeBuilders == 0 {
		return false, ErrBuilderLease
	}
	r.builderHandle.released, r.builderHandle.lease = true, nil
	r.builderLease.refs, r.builderLease.released = 0, true
	c.activeBuilders--
	// These native-only controls have no exported/copied handles. Reset scalar
	// operands under their SAME mutexes so a pre-effect work refusal can retry;
	// never copy or reset an initialized mutex, or a consumed handoff.
	r.builderToken.handle = nil
	r.builderLease.coordinator, r.builderLease.released = nil, false
	r.builderHandle.released = false
	r.builderReady = false
	p.admitted = false
	p.notify.commit(c)
	p.wake.commit(c)
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
	// Attempt start is preadmitted before enqueue can activate. Its notify is
	// distinct from the original enqueue notification; neither is elided.
	if !w.Reserve(1, uint64(unsafe.Sizeof(*r))+128) {
		return false, nil
	}
	startNotify, ready, err := admitNativeNotifyV6(w)
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
	startClock, ready, err := c.admitNativeClockLockedV6(w)
	if !ready || err != nil {
		return false, err
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
	c.startNativePublicationTurnLockedV6(r, candidate, &startNotify, &startClock)
	notify.commit(c)
	wake.commit(c)
	return true, nil
}
