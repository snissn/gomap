package mvcc

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is a previsibility safety control, not the native success gate. The
// retained route must never ACK a selected history row at its missing seam.
func TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6(t *testing.T) {
	testNativeMVCCRealBackendRouteCustodyV6(t, "promoted")
}

func TestNativeMVCCCopiedParentAcrossBothDurableSlotsV6(t *testing.T) {
	for _, mode := range []string{"cancel-parent", "stale-parent"} {
		t.Run(mode, func(t *testing.T) { testNativeMVCCRealBackendRouteCustodyV6(t, mode) })
	}
}

func testNativeMVCCRealBackendRouteCustodyV6(t *testing.T, mode string) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 2048
	opts.BackgroundCheckpointInterval = -1
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	s := New(d)
	key := []byte("actual-selected-anchor")
	for ts := uint64(1); ts <= 33; ts++ {
		if err := s.CommitAt(ts, []Mutation{{Key: key, Value: []byte{byte(ts)}}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AdvanceDiscardFloor(32, CommitDurable); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s = New(d)
	oldKey, _ := mvcckey.Encode(key, 1)
	oldValue, err := d.Get(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	oldReader := d.AcquireSnapshot()
	if oldReader == nil {
		t.Fatal("actual old reader missing")
	}
	defer oldReader.Close()
	var c *PruneCursor
	defer func() {
		if c != nil {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	var lastPhase uint8 = 255
	for calls := 0; calls < 10000; calls++ {
		stats, err := s.PruneVersions(PruneOptions{Mode: CommitDurable, BatchSize: 1, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: c})
		c = stats.Cursor
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Pruned != 0 || stats.Complete {
			t.Fatalf("unqualified route acknowledged work: %+v err=%v", stats, err)
		}
		if c == nil {
			t.Fatalf("missing original cursor: %v", err)
		}
		if c.phase != lastPhase {
			t.Logf("call=%d phase=%d work=%d/%d selected=%d anchor=%d requestFloor=%d", calls, c.phase, stats.WorkRecords, stats.WorkBytes, len(c.batchEntries), c.anchorSize, c.request.Floor())
			lastPhase = c.phase
		}
		if mode != "promoted" && c.preparation != nil {
			sealStage, promoted := c.preparation.MaintenanceNativeSealScalarsForTest()
			if sealStage > 0 && sealStage < 9 && !promoted {
				t.Logf("copied parent retained before promotion: calls=%d stage=%d work=%d/%d", calls+1, sealStage, stats.WorkRecords, stats.WorkBytes)
				// Two actual durable writes move both physical slots. The raw
				// copied parent is evidence and has acquired no parent-root edge.
				for ts := uint64(34); ts <= 35; ts++ {
					if err := s.CommitAt(ts, []Mutation{{Key: []byte("newer-real-writer"), Value: []byte{byte(ts)}}}, CommitDurable); err != nil {
						t.Fatal("foreground publication while parent is private", ts, err)
					}
				}
				if mode == "stale-parent" {
					stale := false
					for turn := 0; turn < 100; turn++ {
						w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
						ready, e := c.preparation.StepNativeMVCCSealPreparationV6(&w)
						if w.Records > 32 || w.Bytes > 1<<20 || ready {
							t.Fatalf("stale copied parent acquired promotion: ready=%v err=%v work=%+v", ready, e, w)
						}
						if e != nil {
							t.Logf("stale copied parent refused before promotion: stage=%d records=%d bytes=%d err=%v", sealStage, w.Records, w.Bytes, e)
							stale = true
							break
						}
					}
					_, promoted = c.preparation.MaintenanceNativeSealScalarsForTest()
					if !stale || promoted {
						t.Fatal("copied parent did not refuse before acquiring old authority")
					}
				}
				if got, e := d.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
					t.Fatal("private parent preparation changed selected DATA", got, e)
				}
				if err := c.Close(); err != nil {
					t.Fatal("finite stale/private-parent cancellation", err)
				}
				c = nil
				newKey, _ := mvcckey.Encode([]byte("newer-real-writer"), 35)
				newValue, e := d.Get(newKey)
				if e != nil || len(newValue) == 0 {
					t.Fatal("cancel damaged newest DATA", e)
				}
				if e := oldReader.Close(); e != nil {
					t.Fatal(e)
				}
				if e := d.Checkpoint(); e != nil {
					t.Fatal(e)
				}
				if e := d.Close(); e != nil {
					t.Fatal(e)
				}
				d, e = treedb.Open(opts)
				if e != nil {
					t.Fatal(e)
				}
				defer d.Close()
				if got, e := d.Get(newKey); e != nil || !bytes.Equal(got, newValue) {
					t.Fatalf("reopen after both-slot advancement lost newest DATA: %q %v", got, e)
				}
				return
			}
		}
		if err == nil {
			continue
		}
		t.Logf("first actual route refusal: calls=%d phase=%d error=%v", calls+1, c.phase, err)
		if !errors.Is(err, rootpublication.ErrNativeMVCCRootWorkRequiredV6) {
			t.Fatal(err)
		}
		if c.phase != 26 || c.preparation == nil || len(c.batchEntries) != 1 || !c.haveAnchor || c.request.Floor() != 32 {
			t.Fatalf("refused before real selected+anchor producer: phase=%d", c.phase)
		}
		stage, bound, candidate := c.preparation.MaintenanceNativeCandidateScalarsForTest()
		if stage != 8 || !bound || !candidate {
			t.Fatalf("actual member/candidate not reached: stage=%d bound=%v candidate=%v", stage, bound, candidate)
		}
		sealStage, promoted := c.preparation.MaintenanceNativeSealScalarsForTest()
		if sealStage != 13 || !promoted {
			t.Fatalf("actual private promotion not reached: stage=%d promoted=%v", sealStage, promoted)
		}
		if count, same := c.preparation.MaintenanceNativeIndexCaptureScalarsForTest(); count != 2 || !same {
			t.Fatalf("actual two original index operands not retained: count=%d sameGeneration=%v", count, same)
		}

		if got, e := d.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("refused output became visible: %x %v", got, e)
		}
		if got, e := oldReader.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("old reader changed: %x %v", got, e)
		}
		if err := s.CommitAt(34, []Mutation{{Key: []byte("newer-real-writer"), Value: []byte("durable")}}, CommitDurable); err != nil {
			t.Fatalf("foreground writer after retained refusal: %v", err)
		}
		if ready, err := c.preparation.StepNativeMVCCAllocatorV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(err, freelist.ErrCOWPreparationStaleV6) {
			t.Fatalf("newer real writer did not revoke private source: ready=%v err=%v", ready, err)
		}
		newKey, _ := mvcckey.Encode([]byte("newer-real-writer"), 34)
		newValue, err := d.Get(newKey)
		if err != nil || len(newValue) == 0 {
			t.Fatal("missing actual newer DATA output", err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		c = nil
		if got, err := d.Get(newKey); err != nil || !bytes.Equal(got, newValue) {
			t.Fatalf("native stale cancellation damaged newer durable output: %q %v", got, err)
		}
		if err := oldReader.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if got, err := d.Get(newKey); err != nil || !bytes.Equal(got, newValue) {
			t.Fatalf("reopen lost newer durable output after stale cancellation: %q %v", got, err)
		}
		return
	}
	t.Fatalf("real route did not reach finite seam: phase=%d", c.phase)
}
