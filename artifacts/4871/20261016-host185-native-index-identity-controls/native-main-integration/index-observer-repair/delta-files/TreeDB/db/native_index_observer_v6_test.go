package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// These are actual original captures and their original cleanup. No seal,
// member, receipt, activation or publication is constructed for observation.
func TestNativeIndexObserverOriginalCapturesV6(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("original stable namespace capability unavailable")
	}
	for _, primary := range []bool{false, true} {
		name := "DATA"
		if primary {
			name = "PRIMARY"
		}
		t.Run(name, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()
			c := NativeMVCCCurrentDeleteV6{db: d}
			c.rootPreparation.expectedSequence = d.meta.CommitSeq
			closeCapture := func(s *nativeStablePagerCaptureV6) bool {
				t.Helper()
				for turn := 0; turn < 100; turn++ {
					w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					ready, err := s.closeStep(&w)
					if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
						t.Error("original cleanup", ready, err, w)
						return false
					}
					if ready {
						return true
					}
				}
				t.Error("original cleanup did not complete")
				return false
			}
			capture := func(s *nativeStablePagerCaptureV6, isPrimary bool) {
				t.Helper()
				for turn := 0; turn < 100; turn++ {
					w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					ready, err := s.step(&c, isPrimary, &w)
					if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
						t.Fatal("original capture", ready, err, w)
					}
					if ready {
						return
					}
				}
				t.Fatal("original capture did not complete")
			}
			// Establish both original persistent namespace proofs before scratch
			// baselines; the first capture also performs the supported lazy setup.
			for _, isPrimary := range []bool{false, true} {
				var warm nativeStablePagerCaptureV6
				capture(&warm, isPrimary)
				if !closeCapture(&warm) {
					t.FailNow()
				}
			}
			idx := d.idx.Load()
			metadata := idx.primary.MetadataOwner()
			before, counter, stable := metadata.Bytes(), d.durableCandidateIndexCaptures.Load(), d.stableIndexCaptures.Load()
			idx.wrapperMu.Lock()
			borrows := idx.wrapperBorrowCountV6()
			idx.wrapperMu.Unlock()
			idx.primaryOwner.mu.Lock()
			ownerRefs := idx.primaryOwner.refs
			idx.primaryOwner.mu.Unlock()
			var s nativeStablePagerCaptureV6
			defer closeCapture(&s)
			capture(&s, primary)
			ops := s.snapshot.stablePagerCompletion
			if s.phase != 8 || s.operationsPhase != 3 || s.token == nil || ops == nil || ops.originalCustody == nil {
				t.Fatal("original capture did not transfer")
			}
			observe := func(isPrimary bool, limit uint64) (NativeIndexGeometryForTestV6, bool) {
				t.Helper()
				w := iterator.OrdinalScanWork{RecordLimit: limit, ByteLimit: 1 << 20}
				g, ready := observeNativeIndexCaptureForTestV6(&s, s.token, idx, isPrimary, 0, 0, 0, 0, &w)
				if w.Records > limit || w.Bytes > 1<<20 {
					t.Fatal("observer exceeded original work", w)
				}
				return g, ready
			}
			charged, refs := metadata.Bytes(), ops.refs.Load()
			for _, limit := range []uint64{0, 1} {
				if _, ready := observe(primary, limit); ready || metadata.Bytes() != charged || ops.refs.Load() != refs || s.snapshot == nil || ops.originalCustody == nil {
					t.Fatal("short observation consumed original custody", limit)
				}
			}
			g, ready := observe(primary, 32)
			if !ready || !g.SameSnapshot || !g.SameNativeCallback || !g.SameSelectedPager || !g.SameTokenOperations || !g.SameRetainedCapture || !g.SameMetadata || !g.SameRegistry || !g.SamePhysicalOwner || !g.CustodyPresent || g.CustodyCharge == 0 || g.CustodyComplete || g.OperationRefs != 1 {
				t.Fatalf("original capture observation: ready=%v geometry=%+v", ready, g)
			}
			if bad, ok := observe(!primary, 32); !ok || bad.SameSelectedPager {
				t.Fatal("wrong selected pager accepted", ok, bad)
			}
			originalNative := ops.native
			ops.native = &nativeStablePagerCaptureV6{}
			bad, ok := observe(primary, 32)
			ops.native = originalNative
			if !ok || bad.SameNativeCallback {
				t.Fatal("wrong native frame accepted", ok, bad)
			}
			originalIdentity := s.identity
			s.identity.ObjectID[0] ^= 1
			bad, ok = observe(primary, 32)
			s.identity = originalIdentity
			if !ok || bad.SameRetainedCapture {
				t.Fatal("wrong captured physical object accepted", ok, bad)
			}
			identity := s.identity
			identity.Generation = idx.id + 1
			w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			sameOps, sameCapture, ok := s.token.MaintenanceNativeCaptureBindingForTestV6(ops, identity, metadata, s.registry, &w)
			if !ok || !sameOps || sameCapture {
				t.Fatal("wrong captured generation accepted")
			}
			if metadata.Bytes() != charged || ops.refs.Load() != refs || ops.originalCustody == nil {
				t.Fatal("observation changed original retained edges")
			}
			if !closeCapture(&s) {
				t.FailNow()
			}
			if !closeCapture(&s) {
				t.FailNow()
			} // terminal cleanup cannot replay
			idx.wrapperMu.Lock()
			afterBorrows := idx.wrapperBorrowCountV6()
			idx.wrapperMu.Unlock()
			idx.primaryOwner.mu.Lock()
			afterOwnerRefs := idx.primaryOwner.refs
			idx.primaryOwner.mu.Unlock()
			if metadata.Bytes() != before || d.durableCandidateIndexCaptures.Load() != counter || d.stableIndexCaptures.Load() != stable || afterBorrows != borrows || afterOwnerRefs != ownerRefs || s.snapshot != nil || s.token != nil || s.metadata != nil {
				t.Fatal("observation/cleanup leaked original custody", metadata.Bytes(), before)
			}
		})
	}
}
