//go:build treedb_test

package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"testing"
	"unsafe"
)

// This is an output/storage capability probe, NOT a prune ACK/publication test.
// The protected public positive tests remain separate and unchanged.
func TestNativeMVCCCapturedLeafOutputV6(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		t.Run(fmt.Sprintf("pointer%t", pointers), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 4
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.ValueLog.ForcePointers = pointers
			opts.ValueLog.PointerThreshold = 4096
			if pointers {
				opts.ValueLog.PointerThreshold = 1
			}
			d, e := treedb.Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			defer func() {
				if e := d.Close(); e != nil {
					t.Error("Close", e)
				}
			}()
			s := New(d)
			commit := func(ts uint64, key, value string) {
				t.Helper()
				if e := s.CommitAt(ts, []Mutation{{Key: []byte(key), Value: []byte(value)}}, CommitDurable); e != nil {
					t.Fatal(e)
				}
			}
			commit(1, "a", "disk-old")
			commit(1, "b", "disk-b")
			commit(1, "c", "disk-c")
			if e = d.Checkpoint(); e != nil {
				t.Fatal(e)
			}
			commit(1, "a", "overlay-replacement")
			commit(2, "a", "keeper-a")
			commit(2, "b", "keeper-b")
			commit(1, "c", "survivor-c")
			commit(100, "z", "captured-future")
			if e = s.AdvanceDiscardFloor(2, CommitDurable); e != nil {
				t.Fatal(e)
			}
			writes, calls, maxR, maxB := 0, 0, uint64(0), uint64(0)
			quantum := func() *iterator.OrdinalScanWork {
				return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			}
			yielded := func(w *iterator.OrdinalScanWork) {
				t.Helper()
				calls++
				maxR = max(maxR, w.Records)
				maxB = max(maxB, w.Bytes)
				if w.Records > 32 || w.Bytes > 1<<20 {
					t.Fatal("actual work", w)
				}
				commit(100, "z", "newer-future")
				writes++
			}
			type valueScratch struct {
				reader     valuelog.COWLeafContinuation
				input, raw [64 << 10]byte
			}
			var capture treedb.NativeMVCCCaptureV6
			var reader backenddb.NativeMVCCFrameReaderV6
			w := quantum()
			budget, ready, e := d.NativeMVCCBudgetV6(w)
			if !ready || e != nil {
				t.Fatal(ready, e)
			}
			containing, e := budget.AcquireExternal(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(capture)) + uint64(unsafe.Sizeof(reader))))
			if e != nil {
				t.Fatal(e)
			}
			valueLease, e := budget.AcquireExternal(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(valueScratch{}))))
			if e != nil {
				t.Fatal(e)
			}
			scratch := new(valueScratch)
			for i := 0; i < 512; i++ {
				w = quantum()
				snapshot, done, err := d.StepNativeMVCCOwnedCaptureV6(&capture, w)
				if err != nil {
					t.Fatal("capture", err)
				}
				yielded(w)
				if done {
					reader.Snapshot = snapshot
					break
				}
			}
			if reader.Snapshot == nil {
				t.Fatal("capture stalled")
			}
			projectionLease, e := budget.AcquireExternal(tree.ChargedProjectionRetainedCharge())
			if e != nil {
				t.Fatal(e)
			}
			cert, e := reader.Snapshot.PrimaryReadRootCertificateV6()
			if e != nil {
				t.Fatal(e)
			}
			var projection *tree.ChargedProjection
			for i := 0; i < 32; i++ {
				w = quantum()
				projection, e = reader.Snapshot.NewNativeMVCCProjectionV6(cert, nil, nil, &reader, w)
				if e != nil && !errors.Is(e, tree.ErrProjectionPending) {
					t.Fatal(e)
				}
				yielded(w)
				if projection != nil {
					break
				}
			}
			if projection == nil {
				t.Fatal("projection stalled")
			}
			var output *bulk.LeafOutput
			w = quantum()
			output, ready, e = reader.Snapshot.NewNativeMVCCLeafOutputV6(bulk.BuildOptions{LeafPrefixCompression: true, PackedValuePtr: true, EntryRevisions: true}, w)
			if !ready || e != nil {
				t.Fatal(ready, e)
			}
			yielded(w)
			keys := make([][]byte, 0, 4)
			for _, x := range []struct {
				k  string
				ts uint64
			}{{"a", 2}, {"b", 2}, {"c", 1}, {"z", 100}} {
				k, _ := mvcckey.Encode([]byte(x.k), x.ts)
				keys = append(keys, k)
			}
			omittedA, _ := mvcckey.Encode([]byte("a"), 1)
			omittedB, _ := mvcckey.Encode([]byte("b"), 1)
			var expected [4]node.LeafEntry
			seen, omitted, added := 0, 0, 0
			op := tree.ProjectionSeekGE
			for rows := 0; rows < 32; rows++ {
				for i := 0; i < 512; i++ {
					w = quantum()
					e = capture.BeginNativeProjectionV6(projection, op, nil, w)
					yielded(w)
					if e == nil {
						break
					}
					if !errors.Is(e, cowbtree.ErrWork) {
						t.Fatal(e)
					}
				}
				complete := false
				for i := 0; i < 512; i++ {
					w = quantum()
					done, err := capture.StepNativeProjectionV6(projection, w)
					if err != nil {
						t.Fatal("project", err)
					}
					yielded(w)
					if done {
						complete = true
						break
					}
				}
				if !complete {
					t.Fatal("projection work stalled")
				}
				w = quantum()
				r, valid, err := capture.NativeProjectionRecordV6(projection, w)
				if err != nil {
					t.Fatal(err)
				}
				yielded(w)
				if !valid {
					break
				}
				seen++
				if bytes.Equal(r.Key, omittedA) || bytes.Equal(r.Key, omittedB) {
					omitted++
					op = tree.ProjectionNext
					continue
				}
				index := -1
				for i, k := range keys {
					if bytes.Equal(k, r.Key) {
						index = i
						break
					}
				}
				if index >= 0 {
					// c@1 exists in the materialized backend and was overwritten
					// in the captured overlay. This literal is independent of
					// projection output and catches selecting the old disk row.
					if index == 2 {
						if (r.Flags&node.FlagPointer != 0) != pointers {
							t.Fatal("winner physical layout", r.Flags)
						}
						value := r.Value
						if pointers {
							begun := false
							for step := 0; step < 512; step++ {
								w = quantum()
								file, found, err := capture.NativePinnedValueLogFileV6(r.ValuePtr.FileID, w)
								if err != nil {
									t.Fatal("winner file", err)
								}
								if found {
									begun, err = scratch.reader.BeginValue(file, r.ValuePtr, valuelog.COWReadLimits{MaxRecordBytes: uint64(len(scratch.input)), MaxRawBytes: uint64(len(scratch.raw)), MaxValueBytes: uint64(len(scratch.raw))}, scratch.input[:], scratch.raw[:], true, w)
									if err != nil {
										t.Fatal("winner read begin", err)
									}
								}
								yielded(w)
								if begun {
									break
								}
							}
							if !begun {
								t.Fatal("winner read begin stalled")
							}
							decoded := false
							for step := 0; step < 512; step++ {
								w = quantum()
								value, decoded, err = scratch.reader.StepWithCodec(w)
								if err != nil {
									t.Fatal("winner pointer", err)
								}
								yielded(w)
								if decoded {
									break
								}
							}
							if !decoded {
								t.Fatal("winner read stalled")
							}
						}
						if !bytes.Equal(value, append([]byte{1}, []byte("survivor-c")...)) {
							t.Fatalf("captured duplicate winner: got %q", value)
						}
						value = nil
						for step := 0; step < 512; step++ {
							w = quantum()
							done, err := scratch.reader.CancelWithCodec(w)
							if err != nil {
								t.Fatal("winner decoder close", err)
							}
							yielded(w)
							if done {
								break
							}
							if step == 511 {
								t.Fatal("winner decoder close stalled")
							}
						}
						t.Logf("KNOWN_DUPLICATE_WINNER pointer=%t encodedKey=c@1 literal=survivor-c persistentPointerVerified=%t", pointers, pointers)
					}
					expected[index] = node.LeafEntry{Key: bytes.Clone(r.Key), Value: bytes.Clone(r.Value), ValuePtr: r.ValuePtr, Flags: r.Flags, Revision: r.Revision}
					if r.Revision == 0 {
						t.Fatal("lost original revision")
					}
					w = quantum()
					done, err := output.Add(r, w)
					if !done || err != nil {
						t.Fatal("encoder", done, err)
					}
					yielded(w)
					added++
				}
				op = tree.ProjectionNext
			}
			if added != 4 || omitted != 2 {
				t.Fatal("actual composite rows", seen, added, omitted)
			}
			// The original allocator is genuinely invoked. A capacity-continuation
			// obstruction refuses BEFORE mutation; no ordinary allocation fallback.
			stored := false
			var obstruction error
			var pageID uint64
			for i := 0; i < 512; i++ {
				w = quantum()
				id, done, err := reader.Snapshot.StoreNativeMVCCLeafOutputV6(output, w)
				yielded(w)
				if err != nil {
					obstruction = err
					break
				}
				if done {
					pageID = id
					stored = true
					break
				}
			}
			if !stored {
				if !errors.Is(obstruction, freelist.ErrAppendCapacityContinuation) && !errors.Is(obstruction, freelist.ErrAppendRangeContinuation) {
					t.Fatal("unexpected storage obstruction", obstruction)
				}
				w = quantum()
				done, err := output.Finish(0, w)
				if !done || err != nil {
					t.Fatal(done, err)
				}
				yielded(w)
			}
			t.Logf("OUTPUT_STAGE pointer=%t actualCompositeRows=%d survivors=%d omitted=%d writes=%d calls=%d maxRecords=%d maxBytes=%d originalAllocatorInvoked=true stored=%t pageID=%d storageObstruction=%v", pointers, seen, added, omitted, writes, calls, maxR, maxB, stored, pageID, obstruction)
			image := output.Image()
			n := node.NewNodeView(image)
			if !n.VerifyChecksum() || n.Count() != 4 {
				t.Fatal("immutable output")
			}
			for i := uint16(0); i < 4; i++ {
				got, err := n.GetLeafEntry(i)
				if err != nil {
					t.Fatal(err)
				}
				want := expected[i]
				if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.ValuePtr != want.ValuePtr || got.Flags != want.Flags || got.Revision != want.Revision {
					t.Fatal("output key/value/pointer/revision", i, got, want)
				}
			}
			image = nil
			n = node.Node{}
			w = quantum()
			owner, charge, done, err := output.Close(w)
			if stored {
				var unit *iterator.OrdinalUnitTooLarge
				if done || !errors.As(err, &unit) || unit.Records <= 32 || charge != 0 || owner != nil {
					t.Fatal("retire must retain actual custody", done, err)
				}
				t.Logf("ABORT_OBSTRUCTION pointer=%t recordsLowerBound=%d bytesLowerBound=%d outputCustodyRetained=true", pointers, unit.Records, unit.Bytes)
				// Diagnostic teardown only: ordinary unlimited driver of SAME stages.
				owner, charge, done, err = output.Close(nil)
			}
			if !done || err != nil {
				t.Fatal("output cleanup", done, err)
			}
			output = nil
			owner.RemovePending(charge)
			for i := 0; i < 512; i++ {
				w = quantum()
				done, err := projection.CloseStep(w)
				if err != nil {
					t.Fatal("projection close", err)
				}
				if done {
					projection = nil
					break
				}
			}
			if projection != nil {
				t.Fatal("projection close stalled")
			}
			projectionLease.Close()
			reader = backenddb.NativeMVCCFrameReaderV6{}
			for i := 0; i < 1024; i++ {
				w = quantum()
				done, err := capture.CloseNativeMVCCOwnedCaptureV6(w)
				if err != nil {
					t.Fatal("capture close", err)
				}
				if done {
					ready = true
					break
				}
				ready = false
			}
			if !ready {
				t.Fatal("capture close stalled")
			}
			scratch = nil
			valueLease.Close()
			containing.Close()
			if e = d.Close(); e != nil {
				t.Fatal("checked Close", e)
			}
			t.Logf("LEAF_OUTPUT_PROBE pointer=%t actualCompositeRows=%d survivors=%d omitted=%d writes=%d calls=%d maxRecords=%d maxBytes=%d originalAllocatorInvoked=true stored=%t pageID=%d storageObstruction=%v boundedPublication=false", pointers, seen, added, omitted, writes, calls, maxR, maxB, stored, pageID, obstruction)
		})
	}
}
