package db

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Uses the original previsibility Snapshot/callback constructor and actual
// cancellation. No candidate, enqueue, native activation or fence is invoked.
func TestNativePagerOperationsPartialConstructionAndCancellationV6(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("original stable namespace capability unavailable")
	}
	for _, tc := range []struct {
		name                  string
		limit                 uint64
		stage                 uint8
		resume, ownerOverflow bool
	}{
		{"before-output", 3, 0, false, false}, {"output-only", 4, 1, false, false}, {"custody-before-owner", 7, 2, false, false}, {"complete", 8, 3, false, false}, {"resume-output", 4, 1, true, false}, {"resume-custody", 7, 2, true, false}, {"owner-overflow", 7, 2, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			// Use the same original no-WAL capture setup as the existing partial
			// token controls. Its persistent namespace proof belongs to the
			// original owner, so construct and actually close it before taking
			// callback/refund baselines for this scratch constructor.
			var warm nativeStablePagerCaptureV6
			closeCapture := func(s *nativeStablePagerCaptureV6) bool {
				t.Helper()
				for turn := 0; turn <= 100; turn++ {
					work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					ready, err := s.closeStep(&work)
					if err != nil || work.Records > 32 || work.Bytes > 1<<20 {
						t.Error("original capture cleanup", ready, err, work)
						return false
					}
					if ready {
						return true
					}
				}
				t.Error("original capture cleanup did not complete")
				return false
			}
			defer closeCapture(&warm)
			for turn := 0; ; turn++ {
				work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err := warm.step(&c, false, &work)
				if err != nil || work.Records > 32 || work.Bytes > 1<<20 || turn > 100 {
					t.Fatal("original capture warmup", ready, err, work)
				}
				if ready {
					break
				}
			}
			if !closeCapture(&warm) {
				t.FailNow()
			}
			idx := d.idx.Load()
			if idx == nil || idx.primary == nil || idx.primaryOwner == nil || idx != d.rootPublication.idx {
				t.Fatal("original no-WAL setup did not install the expected PRIMARY owner")
			}
			if !idx.pager.SupportsBoundedPhysicalClose() || !idx.primary.Pager().SupportsBoundedPhysicalClose() {
				t.Skip("native physical-close capability unsupported")
			}
			s := nativeStablePagerCaptureV6{}
			// Always join the original partial cleanup before deferred DB Close,
			// including an assertion failure at a construction boundary.
			defer func() {
				for turn := 0; turn <= 100; turn++ {
					work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					ready, err := s.closeStep(&work)
					if err != nil {
						t.Error("deferred original cleanup", err)
						return
					}
					if ready {
						return
					}
				}
				t.Error("deferred original cleanup did not complete")
			}()
			metadata := idx.primary.MetadataOwner()
			before, counter := metadata.Bytes(), d.durableCandidateIndexCaptures.Load()
			beforeStableCaptures := d.stableIndexCaptures.Load()
			idx.wrapperMu.Lock()
			beforeBorrows := idx.wrapperBorrowCountV6()
			idx.wrapperMu.Unlock()
			d.rootReuseMu.RLock()
			d.mu.RLock()
			setup := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			ready, err := s.stepLocked(&c, false, &setup)
			d.mu.RUnlock()
			d.rootReuseMu.RUnlock()
			if !ready || err != nil || s.snapshot == nil {
				t.Fatal("original Snapshot setup", ready, err)
			}
			if err := idx.pager.WithStableResourceFile(func(file *os.File) error { s.spec.File = file; return nil }); err != nil {
				t.Fatal(err)
			}
			idx.primaryOwner.mu.Lock()
			refs := idx.primaryOwner.refs
			idx.primaryOwner.mu.Unlock()
			w := iterator.OrdinalScanWork{RecordLimit: tc.limit, ByteLimit: 1 << 20}
			ready, err = s.constructOwnedOperationsV6(idx.pager, metadata, &w)
			if err != nil || ready != (tc.stage == 3) || s.operationsPhase != tc.stage || w.Records > tc.limit {
				t.Fatal("wrong original constructor boundary", ready, err, s.operationsPhase, w)
			}
			if tc.stage > 0 && (s.operations == nil || s.operations.native != &s || s.operations.originalCustody == nil != (tc.stage < 2) || s.operations.owner == nil != (tc.stage < 3)) {
				t.Fatal("accepted original edge not recorded")
			}
			idx.primaryOwner.mu.Lock()
			actualRefs := idx.primaryOwner.refs
			idx.primaryOwner.mu.Unlock()
			if actualRefs != refs+uint64(tc.stage/3) {
				t.Fatal("physical retain crossed boundary")
			}
			if tc.ownerOverflow {
				originalOps, originalCustody, charged := s.operations, s.operations.originalCustody, metadata.Bytes()
				idx.primaryOwner.mu.Lock()
				idx.primaryOwner.refs = ^uint64(0)
				idx.primaryOwner.mu.Unlock()
				work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err := s.constructOwnedOperationsV6(idx.pager, metadata, &work)
				idx.primaryOwner.mu.Lock()
				overflowRefs := idx.primaryOwner.refs
				idx.primaryOwner.refs = refs
				idx.primaryOwner.mu.Unlock()
				if ready || err == nil || s.operations != originalOps || s.operations.originalCustody != originalCustody || s.operations.owner != nil || metadata.Bytes() != charged || overflowRefs != ^uint64(0) {
					t.Fatal("retain error lost accepted original custody", ready, err)
				}
			}
			if tc.resume {
				originalOps := s.operations
				work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err := s.constructOwnedOperationsV6(idx.pager, metadata, &work)
				if !ready || err != nil || s.operations != originalOps || s.operationsPhase != 3 {
					t.Fatal("partial constructor did not resume same output", ready, err)
				}
				// Cancellation must consume this newly accepted owner edge once.
			}
			charged := metadata.Bytes()
			if ready, err := s.closeStep(&iterator.OrdinalScanWork{}); ready || err != nil || metadata.Bytes() != charged || s.snapshot == nil {
				t.Fatal("zero-work cancellation consumed custody", ready, err)
			}
			for turn := 0; ; turn++ {
				work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err := s.closeStep(&work)
				if err != nil || work.Records > 32 || work.Bytes > 1<<20 {
					t.Fatal("original partial cleanup", ready, err, work)
				}
				if ready {
					break
				}
				if turn > 100 {
					t.Fatal("partial cleanup did not resume")
				}
			}
			idx.primaryOwner.mu.Lock()
			actualRefs = idx.primaryOwner.refs
			idx.primaryOwner.mu.Unlock()
			idx.wrapperMu.Lock()
			afterBorrows := idx.wrapperBorrowCountV6()
			idx.wrapperMu.Unlock()
			if s.operations != nil || s.snapshot != nil || s.metadata != nil || metadata.Bytes() != before || d.durableCandidateIndexCaptures.Load() != counter || actualRefs != refs || afterBorrows != beforeBorrows || d.stableIndexCaptures.Load() != beforeStableCaptures {
				t.Fatal("partial cleanup leaked/replayed original edges", metadata.Bytes(), before)
			}
		})
	}
}
