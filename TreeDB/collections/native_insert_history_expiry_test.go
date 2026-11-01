package collections

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Expire the original opening using actual accepted effects, without injecting
// ErrTxnTooOld, altering finite limits, or changing ordinary batching defaults.
// BeforeCOWAdmission is outside the writer/append gates, so the advancing
// producer can finish while the target owns its original staged opening.
func TestNativeOrdinaryInsertHistoryExpiry(t *testing.T) {
	id := []byte("original-request")
	document, err := bson.Marshal(bson.D{{Key: "marker", Value: "exact acknowledged contents"}})
	if err != nil {
		t.Fatal(err)
	}
	testNativeOrdinaryInsertHistoryExpiry(t,
		&CollectionMeta{Name: "target", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}, id, document,
		func(target *Collection) ([][]byte, CollectionInsertStats, error) {
			ids, err := target.InsertBatchValidatedBSON([][]byte{id}, [][]byte{document})
			return ids, CollectionInsertStats{}, err
		}, nil)
}

// Keep genuine eviction, acceptance, held reads, checkpoint, checked original
// Close and fresh reopen identical for the BSON and trusted projection routes.
func testNativeOrdinaryInsertHistoryExpiry(t *testing.T, meta *CollectionMeta, id, document []byte,
	insert func(*Collection) ([][]byte, CollectionInsertStats, error), verifyTyped func(*Collection, bool),
) CollectionInsertStats {
	t.Helper()
	dir := t.TempDir()
	opts := nativeTypedGroupOptions(dir)
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ownerOpen := true
	t.Cleanup(func() {
		if ownerOpen {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	managerOpen := true
	t.Cleanup(func() {
		if managerOpen {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	for _, meta := range []*CollectionMeta{
		meta,
		{Name: "advance"},
	} {
		if _, err := manager.CreateCollection(meta); err != nil {
			t.Fatal(err)
		}
	}
	target, err := manager.OpenCollection("target")
	if err != nil {
		t.Fatal(err)
	}
	advance, err := manager.OpenCollection("advance")
	if err != nil {
		t.Fatal(err)
	}
	if !manager.UsesNativeCommandWALApply() || target.nativeJSON == nil {
		t.Fatal("fixture did not select native shared-COW command WAL")
	}
	held, err := target.nativeJSON.Capture("target")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	opening := held.capture.LogicalSequence()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var first atomic.Bool
	first.Store(true)
	var admissions atomic.Int32
	restoreHook := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeCOWAdmission {
			admissions.Add(1)
			if first.CompareAndSwap(true, false) {
				close(entered)
				<-release
			}
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreHook) }
	defer restore()
	type outcome struct {
		ids   [][]byte
		stats CollectionInsertStats
		err   error
	}
	done := make(chan outcome, 1)
	joined := false
	defer func() {
		unblock()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("original producer did not terminate after release")
			}
		}
	}()
	go func() {
		ids, stats, err := insert(target)
		done <- outcome{ids, stats, err}
	}()
	select {
	case <-entered:
	case result := <-done:
		joined = true
		t.Fatalf("target returned before the admission seam: %v", result.err)
	case <-time.After(10 * time.Second):
		t.Fatal("target did not reach pre-admission seam")
	}
	// A separate collection also works when the target holds exclusive vector
	// admission. Five small public batches exceed the unchanged 4096-slot ring.
	const effects = 4097
	advancingBatches := 0
	for start := 0; start < effects; start += 1024 {
		count := min(1024, effects-start)
		ids, docs := make([][]byte, count), make([][]byte, count)
		for i := range count {
			ids[i] = []byte(fmt.Sprintf("advance-%05d", start+i))
			docs[i] = []byte(`{"accepted":true}`)
		}
		if got, err := advance.InsertBatch(ids, docs); err != nil || len(got) != count {
			t.Fatalf("advancing public batch: ACKs=%d want=%d err=%v", len(got), count, err)
		}
		advancingBatches++
	}
	floor, err := strconv.ParseUint(owner.Stats()["treedb.cache.cow.txn.history_floor"], 10, 64)
	if err != nil || floor <= opening {
		t.Fatalf("history did not actually expire: floor=%d opening=%d err=%v", floor, opening, err)
	}
	next := backend.CommandWALNextLSN()
	unblock()
	var result outcome
	select {
	case result = <-done:
		joined = true
	case <-time.After(10 * time.Second):
		t.Fatal("target did not complete after genuine history expiry")
	}
	restore()
	if result.err != nil {
		t.Fatalf("ordinary insert failed after genuine history expiry: opening=%d floor=%d next_before=%d next_after=%d admissions=%d advancing_batches=%d err=%v", opening, floor, next, backend.CommandWALNextLSN(), admissions.Load(), advancingBatches, result.err)
	}
	if len(result.ids) != 1 || !bytes.Equal(result.ids[0], id) || backend.CommandWALNextLSN() != next+1 || admissions.Load() != int32(advancingBatches+2) {
		t.Fatalf("target was not accepted exactly once after one fresh attempt: ids=%q next=%d before=%d admissions=%d", result.ids, backend.CommandWALNextLSN(), next, admissions.Load())
	}
	if value, err := held.Get(id); err != tree.ErrKeyNotFound || value != nil {
		t.Fatalf("held opening observed later target: value=%x err=%v", value, err)
	}
	verify := func(col *Collection, physical bool) {
		t.Helper()
		if value, err := col.Get(id); err != nil || !bytes.Equal(value, document) {
			t.Fatalf("acknowledged contents differ: value=%x err=%v", value, err)
		}
		if verifyTyped != nil {
			verifyTyped(col, physical)
		}
	}
	verify(target, false)
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	verify(target, true)
	if value, err := held.Get(id); err != tree.ErrKeyNotFound || value != nil {
		t.Fatalf("held opening changed after checkpoint: value=%x err=%v", value, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	freshBackend, err := reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewCollectionManager(freshBackend)
	defer func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
	}()
	freshTarget, err := fresh.OpenCollection("target")
	if err != nil {
		t.Fatal(err)
	}
	verify(freshTarget, true)
	return result.stats
}
