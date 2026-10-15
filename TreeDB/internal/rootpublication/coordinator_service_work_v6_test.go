package rootpublication

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// Pricing/refusal only: no coordinator goroutine, timer/channel creation,
// activation, enqueue, publication, authority write or physical Finish runs.
func TestNativeFinitePublicationPricingRefusalV6(t *testing.T) {
	if !nativeDiagnosticRuntimeSupportedV6() {
		t.Skip("diagnostic representation is pinned to go1.26.3 linux/amd64")
	}
	t.Run("notification", func(t *testing.T) {
		bytes := nativeDiagnosticHchanBytesV6 + 3*uint64(unsafe.Sizeof((chan struct{})(nil)))
		for _, limit := range []iterator.OrdinalScanWork{
			{RecordLimit: 2, ByteLimit: bytes},
			{RecordLimit: 3, ByteLimit: bytes - 1},
		} {
			if p, ready, err := admitNativeNotifyV6(&limit); ready || err != nil || p.admitted || limit.Records != 0 || limit.Bytes != 0 {
				t.Fatal(p, ready, err, limit)
			}
		}
		w := iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: bytes}
		if p, ready, err := admitNativeNotifyV6(&w); !ready || err != nil || !p.admitted || w.Records != 3 || w.Bytes != bytes {
			t.Fatal(p, ready, err, w)
		}
		t.Logf("notification source=Coordinator.notifyLocked records=%d bytes=%d raw-allocation=%d", w.Records, w.Bytes, nativeDiagnosticHchanBytesV6)
	})
	t.Run("timer-plan-is-pre-effect", func(t *testing.T) {
		c := &Coordinator{clock: realClock{}}
		w := iterator.OrdinalScanWork{RecordLimit: 7, ByteLimit: 1024}
		p, ready, err := c.admitNativeTimerInstallLockedV6(&w)
		if !ready || err != nil || !p.admitted || w.Records != 7 || c.timer != nil || !c.firstPendingAt.IsZero() || c.timerGeneration != 0 || c.publishNow {
			t.Fatal(p, ready, err, w, c.timerGeneration)
		}
		t.Logf("timer upper-branch source=Coordinator.installTimerLocked records=%d bytes=%d raw-allocation=%d", w.Records, w.Bytes, nativeDiagnosticHchanBytesV6+24+nativeDiagnosticTimeTimerBytesV6)
		c.clock = NewFakeClock(c.firstPendingAt)
		w = iterator.OrdinalScanWork{RecordLimit: 7, ByteLimit: 1024}
		if p, ready, err := c.admitNativeTimerInstallLockedV6(&w); ready || !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) || p.admitted || c.timer != nil {
			t.Fatal(p, ready, err)
		}
	})
	t.Run("inline-pending-refuses-early-completion", func(t *testing.T) {
		c := &Coordinator{activeDispatch: 1, nativeDispatchV6: 1}
		r := &NativePublicationV6{coordinator: c, bound: true}
		candidate := &PreparedRootCandidate{}
		r.pending[0] = pendingEntry{candidate: candidate, bytes: 1}
		c.pending, c.nativePublicationsV6 = r.pending[:], r
		failure := errors.New("unfenced original physical failure")
		if err := c.CompleteNativePublicationV6(r, failure, true); !errors.Is(err, ErrPublisherProtocol) || !errors.Is(err, failure) {
			t.Fatal(err)
		}
		if r.complete || c.nativePublicationsV6 != r || c.activeDispatch != 1 || c.nativeDispatchV6 != 1 || r.pending[0].candidate != candidate || len(c.pending) != 1 {
			t.Fatal("completion discarded live inline custody")
		}
		t.Logf("containing source=NativePublicationV6 bytes=%d pending-backing=%d", unsafe.Sizeof(*r), unsafe.Sizeof(r.pending))
	})
}
