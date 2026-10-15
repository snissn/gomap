//go:build treedb_test

package mvcc

import (
	"bytes"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Positive target-profile control: the same physical key exists in the backend
// and a newer overlay incarnation. Backend-only deletion would resurrect it.
func TestNativePruneCOWUnboundedDiagnosticPublication(t *testing.T) {
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
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := d.Close(); e != nil {
					t.Error("checked DB Close", e)
				}
			})
			s := New(d)
			selected := []byte("a")
			future := []byte("b")
			if err = s.CommitAt(1, []Mutation{{Key: selected, Value: []byte("disk-original")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// No checkpoint/drain follows: these are genuine C1 overlay publications.
			if err = s.CommitAt(1, []Mutation{{Key: selected, Value: []byte("overlay-replacement")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = s.CommitAt(2, []Mutation{{Key: selected, Value: []byte("keeper")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = s.CommitAt(100, []Mutation{{Key: future, Value: []byte("future-before")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = s.AdvanceDiscardFloor(2, CommitDurable); err != nil {
				t.Fatal(err)
			}
			stats := d.Stats()
			mutable, e := strconv.ParseUint(stats["treedb.cache.mutable_bytes"], 10, 64)
			if e != nil || mutable == 0 || stats["treedb.cache.memtable_mode"] != "cow_btree" {
				t.Fatal("missing actual nonempty COW overlay", stats["treedb.cache.mutable_bytes"], e)
			}
			old, err := d.AcquireMVCCReadCut()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := old.Close(); e != nil {
					t.Error("checked old cut Close", e)
				}
			})
			selectedKey, _ := mvcckey.Encode(selected, 1)
			keeperKey, _ := mvcckey.Encode(selected, 2)
			futureKey, _ := mvcckey.Encode(future, 100)
			oldBytes, e := old.Get(selectedKey)
			if e != nil || !bytes.Equal(oldBytes, append([]byte{1}, []byte("overlay-replacement")...)) {
				t.Fatal("overlay winner", e, oldBytes)
			}
			var cursor *PruneCursor
			var ack, writes, prepared, cut uint64
			var pruneErr error
			var elapsed time.Duration
			var allocBytes, allocCount uint64
			for call := 0; call < 4096; call++ {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				x, e := s.PruneVersionsUnboundedDiagnosticForTest(PruneOptions{BatchSize: 1, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
				elapsed += time.Since(started)
				runtime.ReadMemStats(&after)
				allocBytes += after.TotalAlloc - before.TotalAlloc
				allocCount += after.Mallocs - before.Mallocs
				if x.Cursor != nil {
					cursor = x.Cursor
				}
				ack += x.Pruned
				if cursor != nil && cursor.preparation != nil {
					_, p, c, _ := cursor.preparation.MaintenanceCurrentDeleteScalarsForTest()
					if p {
						prepared++
					}
					if c {
						cut++
					}
				}
				if e != nil {
					pruneErr = e
					break
				}
				// Every legal nonterminal return must let this same caller complete
				// an ordinary durable write, including an ACK-bearing return.
				if !x.Complete {
					if e = s.CommitAt(100, []Mutation{{Key: future, Value: []byte("future-after")}}, CommitDurable); e != nil {
						t.Fatal(e)
					}
					writes++
				}
				if ack != 0 {
					break
				}
				if x.Complete {
					break
				}
			}
			// Terminal ACK must release the original prepared/publication authority.
			// This same caller immediately publishes a later durable version.
			if ack == 1 {
				if e = s.CommitAt(101, []Mutation{{Key: future, Value: []byte("after-ACK")}}, CommitDurable); e != nil {
					t.Fatal("post-ACK durable writer", e)
				}
			}
			postACKKey, _ := mvcckey.Encode(future, 101)
			cleanupStarted := time.Now()
			if cursor != nil {
				if e = cursor.Close(); e != nil {
					t.Error("checked native cursor Close", e)
				}
			}
			t.Logf("UNBOUNDED_FULL_CURRENT_RECONSTRUCTION public_ns=%d alloc_bytes=%d alloc_count=%d cursor_cleanup_ns=%d work_counters_incomplete=true", elapsed.Nanoseconds(), allocBytes, allocCount, time.Since(cleanupStarted).Nanoseconds())
			t.Logf("COW_NATIVE_UNBOUNDED_DIAGNOSTIC pointers=%t overlayBytes=%d ACK=%d writes=%d nativePrepared=%d nativeCut=%d error=%v", pointers, mutable, ack, writes, prepared, cut, pruneErr)
			if ack == 1 {
				got, e := d.Get(selectedKey)
				if e != nil || got != nil {
					t.Fatal("selected survives actual ACK", e, got)
				}
				got, e = d.Get(keeperKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, []byte("keeper")...)) {
					t.Fatal("keeper", e, got)
				}
			}
			got, e := old.Get(selectedKey)
			if e != nil || !bytes.Equal(got, oldBytes) {
				t.Fatal("retained old cut", e, got)
			}
			wantFuture := []byte("future-before")
			if writes > 0 {
				wantFuture = []byte("future-after")
			}
			got, e = d.Get(futureKey)
			if e != nil || !bytes.Equal(got, append([]byte{1}, wantFuture...)) {
				t.Fatal("current future", e, got)
			}
			if ack == 1 {
				got, e = d.Get(postACKKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, []byte("after-ACK")...)) {
					t.Fatal("current post-ACK writer", e, got)
				}
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
			if ack == 1 {
				got, e = reopened.Get(selectedKey)
				if e != nil || got != nil {
					t.Fatal("selected resurrects after reopen", e, got)
				}
			}
			got, e = reopened.Get(futureKey)
			if e != nil || !bytes.Equal(got, append([]byte{1}, wantFuture...)) {
				t.Fatal("reopen future", e, got)
			}
			if ack == 1 {
				got, e = reopened.Get(postACKKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, []byte("after-ACK")...)) {
					t.Fatal("reopen post-ACK writer", e, got)
				}
			}
			if e = reopened.Close(); e != nil {
				t.Fatal(e)
			}
			if pruneErr != nil || ack != 1 || prepared == 0 || writes == 0 {
				t.Fatalf("positive composite publication: error=%v ACK=%d writes=%d prepared=%d cut=%d", pruneErr, ack, writes, prepared, cut)
			}
		})
	}
}
