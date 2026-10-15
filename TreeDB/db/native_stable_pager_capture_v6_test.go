package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativeStablePagerPartialTokenCancellationV6(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("original stable namespace capability unavailable")
	}
	for _, boundary := range []string{"before-token", "before-retain", "after-retain"} {
		t.Run(boundary, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			c := NativeMVCCCurrentDeleteV6{db: d}
			c.rootPreparation.expectedSequence = d.meta.CommitSeq
			closeCapture := func(s *nativeStablePagerCaptureV6) {
				t.Helper()
				for turn := 0; turn < 100; turn++ {
					w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					ready, err := s.closeStep(&w)
					if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
						t.Fatalf("original partial capture cleanup: ready=%v work=%+v err=%v", ready, w, err)
					}
					if ready {
						return
					}
				}
				t.Fatal("partial capture cleanup did not complete")
			}
			// Establish the SAME original owner's persistent namespace proof first;
			// its canonical output is intentionally retained outside this scratch.
			var warm nativeStablePagerCaptureV6
			for turn := 0; ; turn++ {
				w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err := warm.step(&c, false, &w)
				if err != nil || turn > 100 {
					t.Fatal("original capture warmup", err)
				}
				if ready {
					break
				}
			}
			closeCapture(&warm)
			idx := d.idx.Load()
			metadata := idx.primary.MetadataOwner()
			beforeBytes, beforeCaptures := metadata.Bytes(), d.stableIndexCaptures.Load()
			idx.wrapperMu.Lock()
			beforeBorrows := idx.wrapperBorrowCountV6()
			idx.wrapperMu.Unlock()
			var capture nativeStablePagerCaptureV6
			for capture.phase < 5 {
				w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				d.rootReuseMu.RLock()
				d.mu.RLock()
				_, err := capture.stepLocked(&c, false, &w)
				d.mu.RUnlock()
				d.rootReuseMu.RUnlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			// Four records admit the actual original policy/descriptor boundary.
			// Twelve then allocate the SAME partial token, before callback Retain.
			// One more takes the original callback edge, before string copies.
			limits := []uint64{4}
			if boundary != "before-token" {
				limits = append(limits, 12)
			}
			if boundary == "after-retain" {
				limits = append(limits, 1)
			}
			for _, limit := range limits {
				w := iterator.OrdinalScanWork{RecordLimit: limit, ByteLimit: 1 << 20}
				_, ready, err := capture.construction.Step(capture.spec, &w)
				if ready || err != nil || w.Records != limit {
					t.Fatalf("constructor boundary limit=%d ready=%v work=%+v err=%v", limit, ready, w, err)
				}
			}
			wantRefs := int64(1)
			if boundary == "after-retain" {
				wantRefs = 2
			}
			if got := capture.operations.refs.Load(); got != wantRefs {
				t.Fatalf("wrong original callback boundary: got=%d want=%d", got, wantRefs)
			}
			if ready, err := capture.closeStep(&iterator.OrdinalScanWork{}); ready || err != nil || capture.operations == nil {
				t.Fatal("zero work consumed partial custody", err)
			}
			closeCapture(&capture)
			idx.wrapperMu.Lock()
			afterBorrows := idx.wrapperBorrowCountV6()
			idx.wrapperMu.Unlock()
			if got := metadata.Bytes(); got != beforeBytes || d.stableIndexCaptures.Load() != beforeCaptures || afterBorrows != beforeBorrows {
				t.Fatalf("partial token leaked original custody: bytes=%d/%d captures=%d/%d borrows=%d/%d", got, beforeBytes, d.stableIndexCaptures.Load(), beforeCaptures, afterBorrows, beforeBorrows)
			}
		})
	}
}
