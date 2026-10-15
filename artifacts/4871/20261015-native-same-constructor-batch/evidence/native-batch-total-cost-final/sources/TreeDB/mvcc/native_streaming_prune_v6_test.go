//go:build treedb_test

package mvcc

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"runtime"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// These exercise continuation of the SAME retained capture and Request after
// checked physical Finish, rather than starting another namespace scan per ACK.
func TestNativePruneStreamingContinuation(t *testing.T) {
	for _, scenario := range []string{"value-anchors", "later-malformed", "tombstone-boundary", "tombstone-end", "batch-two", "batch-one", "batch-remainder", "batch-malformed", "batch-tombstone-boundary", "batch-tombstone-end", "batch-conflict", "batch-capacity"} {
		t.Run(scenario, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_btree", 4
			opts.DisableSideStores, opts.BackgroundCheckpointInterval = true, -1
			opts.ValueLog.ForcePointers, opts.ValueLog.PointerThreshold = true, 1
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := d.Close(); e != nil {
					t.Error("DB Close", e)
				}
			})
			s := New(d)
			physical := func(k string, ts uint64) []byte {
				p, e := mvcckey.Encode([]byte(k), ts)
				if e != nil {
					t.Fatal(e)
				}
				return p
			}
			for ts := uint64(1); ts <= 3; ts++ {
				if e := s.CommitAt(ts, []Mutation{{Key: []byte("a"), Value: []byte{byte('0' + ts)}}}, CommitDurable); e != nil {
					t.Fatal(e)
				}
			}
			if e := d.Checkpoint(); e != nil {
				t.Fatal(e)
			}
			tombstone := scenario == "tombstone-boundary" || scenario == "tombstone-end" || scenario == "batch-tombstone-boundary" || scenario == "batch-tombstone-end"
			if tombstone {
				if e := s.CommitAt(3, []Mutation{{Key: []byte("a"), Delete: true}}, CommitDurable); e != nil {
					t.Fatal(e)
				}
			}
			if scenario == "value-anchors" {
				for ts := uint64(1); ts <= 2; ts++ {
					if e := s.CommitAt(ts, []Mutation{{Key: []byte("b"), Value: []byte("b")}}, CommitDurable); e != nil {
						t.Fatal(e)
					}
				}
			}
			if scenario == "tombstone-boundary" || scenario == "batch-tombstone-boundary" {
				if e := s.CommitAt(1, []Mutation{{Key: []byte("b"), Value: []byte("boundary")}}, CommitDurable); e != nil {
					t.Fatal(e)
				}
			}
			malformed := physical("z", 1)
			if scenario == "later-malformed" || scenario == "batch-malformed" {
				if e := d.Set(malformed, []byte{0xff}); e != nil {
					t.Fatal(e)
				}
			}
			if e := s.AdvanceDiscardFloor(3, CommitDurable); e != nil {
				t.Fatal(e)
			}
			old, e := d.AcquireMVCCReadCut()
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				if e := old.Close(); e != nil {
					t.Error("old Close", e)
				}
			})
			oldSelected, e := old.Get(physical("a", 1))
			if e != nil {
				t.Fatal(e)
			}
			if scenario == "batch-capacity" {
				x, e := s.PruneVersionsUnboundedDiagnosticForTest(PruneOptions{BatchSize: int(^uint(0) >> 1), Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20})
				var unit *iterator.OrdinalUnitTooLarge
				if !errors.As(e, &unit) || x.Pruned != 0 || x.Complete || x.Cursor == nil || x.Cursor.captureBegun || len(x.Cursor.batchEntries) != 0 {
					t.Fatal("requested capacity must refuse before capture/allocation, not clamp", x, e)
				}
				if e := x.Cursor.Close(); e != nil {
					t.Fatal(e)
				}
				got, e := d.Get(physical("a", 1))
				if e != nil || !bytes.Equal(got, oldSelected) {
					t.Fatal("capacity refusal changed history", got, e)
				}
				t.Log("BATCH_CAPACITY original requested size refused before capture and allocation")
				return
			}

			batchSize := 1
			if scenario == "batch-two" || scenario == "batch-malformed" || scenario == "batch-tombstone-boundary" || scenario == "batch-tombstone-end" || scenario == "batch-conflict" {
				batchSize = 2
			}
			if scenario == "batch-remainder" {
				batchSize = 3
			}
			measured := scenario == "batch-one" || scenario == "batch-two"
			var elapsedNS, allocBytes, allocs, calls, publications uint64
			var before, after runtime.MemStats
			conflicted := false
			var cursor *PruneCursor
			t.Cleanup(func() {
				if cursor != nil {

					if e := cursor.Close(); e != nil {
						t.Error("cursor Close", e)
					}
				}
			})
			var request mvccadmission.Request
			var captured bool
			var ack, visited, writes uint64
			var complete bool
			var scanErr error
			expected := [][]byte{physical("a", 2), physical("a", 1)}
			if tombstone {
				expected = append(expected, physical("a", 3))
			} else if scenario == "value-anchors" {
				expected = append(expected, physical("b", 1))
			}
			for call := 0; call < 4096; call++ {
				if scenario == "batch-conflict" && cursor != nil && cursor.phase == 16 && !conflicted {
					// A legitimate competing prune consumes the FIRST buffered
					// row under the same floor. The last constructor-bound row
					// remains present, so validating only that row is unsafe.
					var other *PruneCursor
					t.Cleanup(func() {
						if other != nil {
							if e := other.Close(); e != nil {
								t.Error(e)
							}
						}
					})
					for i := 0; i < 4096; i++ {
						y, e := s.PruneVersionsUnboundedDiagnosticForTest(PruneOptions{BatchSize: 1, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: other})
						if y.Cursor != nil {
							other = y.Cursor
						}
						if e != nil {
							t.Fatal("competing prune", e)
						}
						if y.Pruned != 0 {
							if y.Pruned != 1 {
								t.Fatal(y)
							}
							conflicted = true
							break
						}
					}
					if !conflicted {
						t.Fatal("competing prune never published")
					}
					if e := other.Close(); e != nil {
						t.Fatal(e)
					}
				}
				if measured {
					runtime.ReadMemStats(&before)
				}
				start := time.Now()
				x, e := s.PruneVersionsUnboundedDiagnosticForTest(PruneOptions{BatchSize: batchSize, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
				duration := time.Since(start)
				if measured {
					runtime.ReadMemStats(&after)
					elapsedNS += uint64(duration)
					allocBytes += after.TotalAlloc - before.TotalAlloc
					allocs += after.Mallocs - before.Mallocs
					calls++
					publications += x.Batches
				}
				if x.Cursor != nil {
					cursor = x.Cursor
				}
				visited += x.Visited
				if cursor != nil && cursor.requestCaptured {
					if captured && request != cursor.request {
						t.Fatal("Request changed across selection")
					}
					request, captured = cursor.request, true
				}
				if x.Pruned != 0 {
					if scenario == "batch-two" && (x.Pruned != 2 || x.Batches != 1) {
						t.Fatalf("BatchSize=2 must use one actual publication for two verified deletions: pruned=%d batches=%d", x.Pruned, x.Batches)
					}
					if x.Pruned > uint64(batchSize) || ack+x.Pruned > uint64(len(expected)) {
						t.Fatal("unexpected ACK", ack, x)
					}
					if x.Complete || !cursor.integrityDone {
						t.Fatal("ACK incorrectly claims EOF or lacks selected integrity", x.Complete, cursor.integrityDone)
					}
					for i := uint64(0); i < x.Pruned; i++ {
						got, ge := d.Get(expected[ack])
						if ge != nil || got != nil {
							t.Fatal("ACK lacks actual physical deletion", ack, ge, got)
						}
						ack++
					}
					if tombstone && ack < 3 {
						got, ge := d.Get(physical("a", 3))
						if ge != nil || !bytes.Equal(got, []byte{recordTombstoneV1}) {
							t.Fatal("tombstone removed before older history", ack, ge, got)
						}
					}
				}
				if e != nil {
					scanErr = e
					break
				}
				if !x.Complete {
					if e := s.CommitAt(100, []Mutation{{Key: []byte("zz"), Value: []byte("successor")}}, CommitDurable); e != nil {
						t.Fatal("same-caller legal-yield writer", e)
					}
					writes++
				}
				if x.Complete {
					complete = true
					break
				}
			}
			if scenario == "batch-conflict" {
				if !conflicted || ack != 0 || !errors.Is(scanErr, rootpublication.ErrDurableRootOwnership) {
					t.Fatal("nonlast current conflict did not refuse whole original batch", conflicted, ack, scanErr)
				}
				got, e := d.Get(physical("a", 2))
				if e != nil || got != nil {
					t.Fatal("competing physical deletion lost", got, e)
				}
				got, e = d.Get(physical("a", 1))
				if e != nil || !bytes.Equal(got, oldSelected) {
					t.Fatal("batch prefix deleted before full validation", got, e)
				}
				if e := cursor.Close(); e != nil {
					t.Fatal(e)
				}
				if e := old.Close(); e != nil {
					t.Fatal(e)
				}
				if e := d.Close(); e != nil {
					t.Fatal(e)
				}
				reopened, e := treedb.Open(opts)
				if e != nil {
					t.Fatal(e)
				}
				got, e = reopened.Get(physical("a", 1))
				if e != nil || !bytes.Equal(got, oldSelected) {
					t.Fatal("refused batch changed reopened history", got, e)
				}
				if e := reopened.Close(); e != nil {
					t.Fatal(e)
				}
				t.Log("BATCH_CONFLICT nonlast winner refused before physical deletion; old and current custody closed")
				return
			}
			if ack != uint64(len(expected)) || writes == 0 {
				t.Fatal("continuation did not ACK exact history", ack, len(expected), writes, scanErr)
			}
			if scenario == "later-malformed" || scenario == "batch-malformed" {
				if !errors.Is(scanErr, ErrMalformedRecord) || complete {
					t.Fatal("later malformed must preserve earlier ACK and report error", scanErr, complete)
				}
				x, e := s.PruneVersionsUnboundedDiagnosticForTest(PruneOptions{BatchSize: batchSize, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
				if !errors.Is(e, ErrMalformedRecord) || x.Pruned != 0 || x.Complete {
					t.Fatal("malformed continuation replay", x, e)
				}
				got, e := d.Get(malformed)
				if e != nil || !bytes.Equal(got, []byte{0xff}) {
					t.Fatal("malformed payload suppressed", got, e)
				}
			} else if scanErr != nil || !complete {
				t.Fatal("scan did not complete honestly", complete, scanErr)
			}
			wantVisited := uint64(3)
			if scenario == "value-anchors" {
				wantVisited = 5
			}
			if scenario == "tombstone-boundary" || scenario == "batch-tombstone-boundary" {
				wantVisited = 4
			}
			if visited != wantVisited {
				t.Fatal("retained scan reread/lost records", visited, wantVisited)
			}
			if got, e := old.Get(physical("a", 1)); e != nil || !bytes.Equal(got, oldSelected) {
				t.Fatal("old cut changed", got, e)
			}
			if !tombstone {
				got, e := d.Get(physical("a", 3))
				if e != nil || !bytes.Equal(got, []byte{recordValueV1, '3'}) {
					t.Fatal("retained value anchor", got, e)
				}
			} else {
				r, e := s.GetAt([]byte("a"), 4)
				if e != nil || r.State != Absent {
					t.Fatal("tombstone history resurrected", r, e)
				}
			}
			if e := s.CommitAt(101, []Mutation{{Key: []byte("zz"), Value: []byte("after-scan")}}, CommitDurable); e != nil {
				t.Fatal(e)
			}
			if measured {
				t.Logf("BATCH_PUBLIC_COST batch=%d ACK=%d publications=%d calls=%d writes=%d ns=%d B=%d allocs=%d", batchSize, ack, publications, calls, writes, elapsedNS, allocBytes, allocs)
			}
			if e := cursor.Close(); e != nil {
				t.Fatal(e)
			}
			if e := old.Close(); e != nil {
				t.Fatal(e)
			}
			if e := d.Close(); e != nil {
				t.Fatal(e)
			}
			reopened, e := treedb.Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			for _, p := range expected {
				got, e := reopened.Get(p)
				if e != nil || got != nil {
					t.Fatal("reopen resurrection", p, got, e)
				}
			}
			got, e := reopened.Get(physical("zz", 101))
			if e != nil || !bytes.Equal(got, append([]byte{recordValueV1}, []byte("after-scan")...)) {
				t.Fatal("reopen successor", got, e)
			}
			if e := reopened.Close(); e != nil {
				t.Fatal(e)
			}
			t.Logf("STREAMING_CONTINUATION scenario=%s ACK=%d visited=%d writes=%d complete=%t later_error=%v", scenario, ack, visited, writes, complete, scanErr)
		})
	}
}
