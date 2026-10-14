//go:build treedb_test

package mvcc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/m7routetrace"
	"os"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// This is an actual public request, not a manually constructed apply result.
// The same Store continues equal-timestamp future commits after each preparation
// return. The existing original request/cap fixtures remain byte-identical.
func TestPruneWholeCurrentPublicationSustainedPreflight(t *testing.T) {
	if os.Getenv("M7_CURRENT_SEGMENT_CONSTRUCTION") != "1" {
		t.Skip("explicit whole construction gate")
	}
	for _, rows := range []int{33, 80} {
		for _, pointer := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows%d/pointer%t", rows, pointer), func(t *testing.T) {
				dir := t.TempDir()
				opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
				opts.DisableSideStores = true
				opts.BackgroundCheckpointInterval = -1
				if pointer {
					opts.ValueLog.PointerThreshold = 1
				} else {
					opts.ValueLog.PointerThreshold = 4096
				}
				d, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				st := New(d)
				selected, future := []byte("k000"), []byte(fmt.Sprintf("k%03d", rows-2))
				before, after := []byte("before-pointer"), []byte("after!-pointer")
				for _, ts := range []uint64{1, 2} {
					if err = st.CommitAt(ts, []Mutation{{Key: selected, Value: before}}, CommitDurable); err != nil {
						t.Fatal(err)
					}
				}
				mutations := make([]Mutation, 0, rows-2)
				for i := 1; i < rows-1; i++ {
					mutations = append(mutations, Mutation{Key: []byte(fmt.Sprintf("k%03d", i)), Value: before})
				}
				if err = st.CommitAt(100, mutations, CommitDurable); err != nil {
					t.Fatal(err)
				}
				if err = st.AdvanceDiscardFloor(2, CommitDurable); err != nil {
					t.Fatal(err)
				}
				old := d.AcquireSnapshot()
				if old == nil {
					t.Fatal("missing real old reader")
				}
				defer old.Close()
				futureKey, err := mvcckey.Encode(future, 100)
				if err != nil {
					t.Fatal(err)
				}
				trace := d.BeginMaintenanceRouteTraceForTest(uintptr(unsafe.Pointer(st)))
				defer trace.End()
				var cursor *PruneCursor
				var originalRequestCaptured bool
				var originalRequest mvccadmission.Request
				// The helper's inferred type is the exact request value; no authority is fabricated.
				_ = originalRequest
				var ack, writes, nativeSelected, nativePrepared, nativeCut uint64
				var nativePhase uint8
				var firstErr error
				var last PruneStats
				for call := 0; call < 4096; call++ {
					x, e := st.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
					last = x
					m7routetrace.Observe(uintptr(unsafe.Pointer(st)), m7routetrace.Return, x.WorkRecords, x.WorkBytes, x.Pruned, 0)
					if x.WorkRecords > 32 || x.WorkBytes > 1<<20 {
						t.Fatal("public cap", x)
					}
					if x.Cursor != nil {
						cursor = x.Cursor
					}
					if cursor != nil && !originalRequestCaptured && cursor.requestCaptured {
						originalRequest = cursor.request
						originalRequestCaptured = true
					}
					if cursor != nil && originalRequestCaptured && cursor.request != originalRequest {
						t.Fatal("original request replaced")
					}
					if cursor != nil && cursor.preparation != nil {
						selected, prepared, cut, phase := cursor.preparation.MaintenanceCurrentDeleteScalarsForTest()
						if selected {
							nativeSelected++
						}
						if prepared {
							nativePrepared++
						}
						if cut {
							nativeCut++
						}
						nativePhase = phase
					}
					ack += x.Pruned
					if e != nil {
						firstErr = e
						break
					}
					if ack != 0 {
						break
					}
					if cursor != nil && !cursor.loadingFloor && cursor.integrityDone {
						m7routetrace.Observe(uintptr(unsafe.Pointer(st)), m7routetrace.PublicWriteBefore, writes, 0, 0, 0)
						if err = st.CommitAt(100, []Mutation{{Key: future, Value: after}}, CommitDurable); err != nil {
							t.Fatal(err)
						}
						writes++
						m7routetrace.Observe(uintptr(unsafe.Pointer(st)), m7routetrace.PublicWriteAfter, writes, 0, 0, 0)
					}
					if x.Complete {
						break
					}
				}
				if ack != 0 {
					selectedKey, e := mvcckey.Encode(selected, 1)
					if e != nil {
						t.Fatal(e)
					}
					got, e := d.Get(selectedKey)
					if e != nil || got != nil {
						t.Fatal("actual physical selected row survives ACK", e, got)
					}
					keeperKey, e := mvcckey.Encode(selected, 2)
					if e != nil {
						t.Fatal(e)
					}
					got, e = d.Get(keeperKey)
					if e != nil || !bytes.Equal(got, append([]byte{1}, before...)) {
						t.Fatal("keeper", e, got)
					}
				}
				beforeClose, _ := json.Marshal(trace.Snapshot())
				t.Logf("M7_ROUTE_BEFORE_CLOSE rows=%d pointer=%t %s", rows, pointer, beforeClose)
				if cursor != nil {
					if e := cursor.Close(); e != nil {
						t.Fatal("admitted cursor cleanup", e)
					}
				}
				afterClose, _ := json.Marshal(trace.Snapshot())
				t.Logf("M7_ROUTE_AFTER_CLOSE rows=%d pointer=%t %s", rows, pointer, afterClose)
				t.Logf("WHOLE_PUBLIC_CURRENT rows=%d pointer=%t writes=%d ACK=%d error=%v last=%+v requestCaptured=%t nativeSelected=%d nativePrepared=%d nativeCut=%d nativePhase=%d", rows, pointer, writes, ack, firstErr, last, originalRequestCaptured, nativeSelected, nativePrepared, nativeCut, nativePhase)
				want := before
				if writes != 0 {
					want = after
				}
				got, e := d.Get(futureKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, want...)) {
					t.Fatal("future bytes", e, got)
				}
				got, e = old.Get(futureKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, before...)) {
					t.Fatal("old reader", e, got)
				}
				if e = old.Close(); e != nil {
					t.Fatal(e)
				}
				if e = d.Close(); e != nil {
					t.Fatal(e)
				}
				reopened, e := treedb.Open(opts)
				if e != nil {
					t.Fatal(e)
				}
				got, e = reopened.Get(futureKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, want...)) {
					t.Fatal("reopen future", e, got)
				}
				if e = reopened.Close(); e != nil {
					t.Fatal(e)
				}

				if firstErr != nil || ack != 1 || writes == 0 {
					t.Fatalf("whole sustained gate: error=%v ACK=%d writes=%d", firstErr, ack, writes)
				}
			})
		}
	}
}
