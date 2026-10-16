package rootpublication

import (
	"runtime"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// These two sizes are diagnostic evidence from the pinned go1.26.3 linux/amd64
// ELF, not private runtime layout mirrors or a portable heap contract. See
// native-build/runtime-layout-evidence/{dwarf-types.json,
// real-clock-new-timer-evidence.json}. Global runtime scheduling/allocator state
// remains the original coordinator service domain. Channel/timer outputs stay
// GC-owned; no retained-owner refund is claimed when a channel is replaced.
const nativeDiagnosticHchanBytesV6 = uint64(112)
const nativeDiagnosticTimeTimerBytesV6 = uint64(112)

func nativeDiagnosticRuntimeSupportedV6() bool {
	return runtime.Version() == "go1.26.3" && runtime.GOOS == "linux" && runtime.GOARCH == "amd64"
}

// A permit is an invocation-local operation plan, never a callback, resource
// owner or stored credit. Each flag names the actual original operation it
// admits. It must be consumed in the same nonyielding coordinator invocation;
// it cannot be attached to a cursor or reused after a return. In particular it
// does not grant permission to execute an opaque Clock implementation.
type nativeNotifyPermitV6 struct{ admitted bool }

func admitNativeNotifyV6(w *iterator.OrdinalScanWork) (nativeNotifyPermitV6, bool, error) {
	if w == nil || !nativeDiagnosticRuntimeSupportedV6() {
		return nativeNotifyPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	// close old channel, allocate the new unbuffered channel, replace the
	// channel operand. There is no element backing for chan struct{}.
	if !w.Reserve(3, nativeDiagnosticHchanBytesV6+3*uint64(unsafe.Sizeof((chan struct{})(nil)))) {
		return nativeNotifyPermitV6{}, false, nil
	}
	return nativeNotifyPermitV6{admitted: true}, true, nil
}

func (permit *nativeNotifyPermitV6) commit(c *Coordinator) {
	if !permit.admitted {
		panic("rootpublication: notification without admitted original operation")
	}
	permit.admitted = false
	c.notifyLocked()
}

// nativeTimerInstallPermitV6 admits both branches before enqueue activation:
// the later clock may make the delay expire, so timer creation is an upper
// bound rather than a claim that a timer was created. The raw allocation
// traffic is hchan + one time.Time backing + runtime.timeTimer. The public
// time.Timer prefix and embedded runtime.timer are not counted a second time.
// The pinned ELF shows realTimer is returned as the original *time.Timer in
// interface data, with no separate wrapper allocation.
type nativeTimerInstallPermitV6 struct{ admitted bool }

func (c *Coordinator) admitNativeTimerInstallLockedV6(w *iterator.OrdinalScanWork) (nativeTimerInstallPermitV6, bool, error) {
	if w == nil || !nativeDiagnosticRuntimeSupportedV6() {
		return nativeTimerInstallPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	// Clock/timer fields are inspected only after this fixed header read.
	if !w.Reserve(1, uint64(unsafe.Sizeof(c.clock))+uint64(unsafe.Sizeof(c.timer))+uint64(unsafe.Sizeof(c.firstPendingAt))+3*uint64(unsafe.Sizeof(time.Duration(0)))) {
		return nativeTimerInstallPermitV6{}, false, nil
	}
	if _, real := c.clock.(realClock); !real || c.timer != nil {
		return nativeTimerInstallPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	// firstPendingAt clock, installTimer clock, delay/elapsed calculation,
	// request-or-generation mutation, NewTimer, timer interface install. The
	// actual no-timer branch is covered without calling a custom callback.
	bytes := 3*uint64(unsafe.Sizeof(time.Time{})) + 3*uint64(unsafe.Sizeof(time.Duration(0))) + uint64(unsafe.Sizeof(c.timer)) + nativeDiagnosticHchanBytesV6 + uint64(unsafe.Sizeof(time.Time{})) + nativeDiagnosticTimeTimerBytesV6
	if !w.Reserve(6, bytes) {
		return nativeTimerInstallPermitV6{}, false, nil
	}
	return nativeTimerInstallPermitV6{admitted: true}, true, nil
}

func (permit *nativeTimerInstallPermitV6) commit(c *Coordinator) {
	if !permit.admitted {
		panic("rootpublication: timer installation without admitted original operation")
	}
	permit.admitted = false
	c.firstPendingAt = c.clock.Now()
	c.installTimerLocked()
}

// Wake is the original nonblocking channel send/select. It introduces no
// native retained output or separately allocated payload.
type nativeWakePermitV6 struct{ admitted bool }

func admitNativeWakeV6(w *iterator.OrdinalScanWork) (nativeWakePermitV6, bool, error) {
	if w == nil {
		return nativeWakePermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof((chan struct{})(nil)))) {
		return nativeWakePermitV6{}, false, nil
	}
	return nativeWakePermitV6{admitted: true}, true, nil
}

func (permit *nativeWakePermitV6) commit(c *Coordinator) {
	if !permit.admitted {
		panic("rootpublication: wake without admitted original operation")
	}
	permit.admitted = false
	c.signal()
}

type nativeClockPermitV6 struct{ admitted bool }

func (c *Coordinator) admitNativeClockLockedV6(w *iterator.OrdinalScanWork) (nativeClockPermitV6, bool, error) {
	if w == nil {
		return nativeClockPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(c.clock))+uint64(unsafe.Sizeof(time.Time{}))) {
		return nativeClockPermitV6{}, false, nil
	}
	switch c.clock.(type) {
	case realClock:
		return nativeClockPermitV6{admitted: true}, true, nil
	case *FakeClock:
		// Existing deterministic coordinator controls retain their known Now
		// implementation (one mutex and by-value copy), never an opaque Clock.
		if !w.Reserve(1, uint64(unsafe.Sizeof(FakeClock{}))) {
			return nativeClockPermitV6{}, false, nil
		}
		return nativeClockPermitV6{admitted: true}, true, nil
	default:
		return nativeClockPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
}

func (permit *nativeClockPermitV6) now(c *Coordinator) time.Time {
	if !permit.admitted {
		panic("rootpublication: clock without admitted original operation")
	}
	permit.admitted = false
	return c.clock.Now()
}

// Stop and clear retain the original timer service lifetime. A stopped timer
// may remain reachable from the scheduler's local Timer interface; its heap is
// not refunded as a selected native allocation.
type nativeTimerStopPermitV6 struct{ admitted, present bool }

func (c *Coordinator) admitNativeTimerStopLockedV6(w *iterator.OrdinalScanWork) (nativeTimerStopPermitV6, bool, error) {
	if w == nil {
		return nativeTimerStopPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(c.timer))) {
		return nativeTimerStopPermitV6{}, false, nil
	}
	if c.timer == nil {
		return nativeTimerStopPermitV6{admitted: true}, true, nil
	}
	bytes := uint64(unsafe.Sizeof(c.timer))
	switch c.timer.(type) {
	case realTimer:
		bytes += uint64(unsafe.Sizeof(time.Timer{}))
	case *fakeTimer:
		bytes += uint64(unsafe.Sizeof(fakeTimer{})) + uint64(unsafe.Sizeof(FakeClock{}))
	default:
		return nativeTimerStopPermitV6{}, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(2, bytes) {
		return nativeTimerStopPermitV6{}, false, nil
	}
	return nativeTimerStopPermitV6{admitted: true, present: true}, true, nil
}

func (permit *nativeTimerStopPermitV6) commit(c *Coordinator) {
	if !permit.admitted {
		panic("rootpublication: timer stop without admitted original operation")
	}
	permit.admitted = false
	if permit.present {
		c.timer.Stop()
		c.timer = nil
	}
}
