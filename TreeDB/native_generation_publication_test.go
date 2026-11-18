package treedb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Reach the real generation watermark with acknowledged BSON commands, then
// hold the original physical DATA write. The logical suffix and a retained
// reader must both stay usable while this exact older publication is stopped.
func TestNativeBSONGenerationPressureForeground(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{})
}

func TestNativeBSONGenerationPressureForegroundWALSync(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{wal: true})
}

func TestNativeBSONGenerationPressureForegroundWALSyncFailure(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{
		wal: true, failure: errors.New("generation captured WAL sync failed"),
	})
}

func TestNativeBSONSystemPagerReplaysOriginalWAL(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{
		wal: true, failure: errors.New("generation captured WAL sync failed"), systemPager: true,
	})
}

func TestNativeBSONGenerationPressureNotifyCanClose(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{
		wal: true, failure: errors.New("generation captured WAL sync failed"), closeFromNotify: true,
	})
}

func TestNativeBSONGenerationPressureForegroundWALSyncRotation(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{wal: true, rotate: true})
}

func TestNativeBSONGenerationPressureSealsExactFrozenPrefix(t *testing.T) {
	testNativeBSONGenerationPressureForeground(t, nativeGenerationSyncTest{wal: true, exactFrozen: true})
}

type nativeGenerationSyncTest struct {
	closeFromNotify bool
	exactFrozen     bool
	systemPager     bool
	wal             bool
	rotate          bool
	failure         error
}

func testNativeBSONGenerationPressureForeground(t *testing.T, control nativeGenerationSyncTest) {
	t.Helper()
	holdWAL, syncFailure := control.wal, control.failure
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.FreshSystemPagerLeaves = control.systemPager
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	// Use the gateway's parent budgets so a slow cleanup scheduler cannot
	// select generic parent pressure before the targeted generation crosses.
	opts.COWMemtableLimits.MaxInFlightBytes = 512 << 20
	opts.COWMemtableLimits.MaxTotalBytes = 1 << 30
	opts.COWMemtableLimits.MaxRetiredBytes = 1 << 30
	opts.COWMemtableLimits.MaxGenerationBytes = 2 << 20
	if control.exactFrozen {
		opts.COWMemtableLimits.MaxGenerationBytes = 128 << 10
	}
	opts.MaxWALBytes = 2 << 30
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	backgroundFailure := make(chan error, 1)
	callbackClose := make(chan error, 1)
	var calledClose atomic.Bool
	var db *DB
	var err error
	var callbackBlocked bool
	if syncFailure != nil {
		// Reopen physically replays every unapplied update after injected failure.
		// Keep this fixture small while reaching the same real generation threshold.
		opts.COWMemtableLimits.MaxGenerationBytes = 128 << 10
		opts.NotifyError = func(err error) {
			if errors.Is(err, syncFailure) {
				select {
				case backgroundFailure <- err:
				default:
				}
				if control.closeFromNotify && calledClose.CompareAndSwap(false, true) {
					callbackClose <- db.Close()
				}
			}
		}
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil && !callbackBlocked {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	cache := db.cached
	backend, err := db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(db.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("row")
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held, err := reader.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	})
	write := func(n int64) error {
		typ, buf, err := bson.MarshalValue(n)
		if err != nil {
			return err
		}
		r, ok, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{DocumentID: id, Fields: []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: buf}}}}})
		if err != nil {
			return err
		}
		if !ok || len(r) != 1 || !r[0].Matched || !r[0].Modified {
			return fmt.Errorf("unacknowledged update %d: %+v %v", n, r, ok)
		}
		return nil
	}
	last := int64(0)
	for {
		last++
		if err := write(last); err != nil {
			t.Fatal(err)
		}
		o, err := backend.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		if o.UnderPressure {
			l := opts.COWMemtableLimits
			if !o.Semantic.GenerationUnderPressure || o.Semantic.ConstructionBytes != 0 || o.ReservedBytes >= l.MaxInFlightBytes-l.MaxInFlightBytes/4 || o.Sources >= l.MaxSources || o.Generations >= l.MaxGenerations {
				t.Fatalf("unexpected pressure %+v", o)
			}
			break
		}
		if last >= 12000 {
			t.Fatal("never reached actual generation pressure")
		}
	}
	old := prefix.LogicalFrontiers()
	reached, release := make(chan durabilitycut.Event, 1), make(chan struct{})
	type syncWitness struct {
		path string
		lsn  uint64
		info os.FileInfo
		err  error
	}
	completedSync := make(chan syncWitness, 1)
	var armed atomic.Bool
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	path := filepath.Join(opts.Dir, "maindb", "index.db")
	wal := filepath.Join(opts.Dir, "maindb", "wal")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		match := e.Point == durabilitycut.BeforeMetaWrite && e.Resource == durabilitycut.ResourceMeta && e.Path == path
		if holdWAL {
			match = e.Point == durabilitycut.BeforeDependencyFileSync && e.Resource == durabilitycut.ResourceCommandWAL && e.StablePath != nil && filepath.Dir(e.Path) == wal && strings.HasPrefix(filepath.Base(e.Path), "commit-l0-")
		}
		if control.rotate && e.Point == durabilitycut.AfterDependencyFileSync && e.Resource == durabilitycut.ResourceCommandWAL && e.LSN != 0 && e.StablePath != nil {
			info, err := e.StablePath.Stat()
			select {
			case completedSync <- syncWitness{path: e.Path, lsn: e.LSN, info: info, err: err}:
			default:
			}
		}
		if match && armed.CompareAndSwap(false, true) {
			reached <- e
			<-release
			return syncFailure
		}
		return nil
	})
	defer restore()
	defer resume()
	done := make(chan error, 1)
	go func() { done <- write(last + 1) }()
	var heldSync durabilitycut.Event
	select {
	case heldSync = <-reached:
	case <-time.After(10 * time.Second):
		resume()
		t.Fatalf("physical generation publication did not reach held sync (WAL=%v)", holdWAL)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		resume()
		err := <-done
		t.Fatalf("generation-triggering UPDATE waited for held physical publication (WAL=%v), eventual error %v", holdWAL, err)
	}
	var heldFile os.FileInfo
	if control.rotate {
		heldFile, err = heldSync.StablePath.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if db.backend.CommandWALActiveBytes() == 0 {
			t.Fatal("captured WAL segment has no accepted bytes before rotation")
		}
		go func() { done <- db.backend.RotateCommandWALActiveSegment(false) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			resume()
			err := <-done
			t.Fatalf("journal rotation waited for the captured WAL sync, eventual error %v", err)
		}
		if db.backend.CommandWALActiveBytes() != 0 {
			t.Fatal("journal rotation did not install an empty successor")
		}
	}
	// Start the successor only after the older physical operation is held.
	go func() { done <- write(last + 2) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		resume()
		err := <-done
		t.Fatalf("successor UPDATE waited for held physical publication (WAL=%v), eventual error %v", holdWAL, err)
	}
	f := prefix.LogicalFrontiers()
	if f.V != old.V+2 || f.C != old.C+2 || f.S != old.S || f.W != old.W {
		t.Fatalf("while physical publication held (WAL=%v): old=%+v now=%+v", holdWAL, old, f)
	}
	check := func(raw []byte, err error, n int64) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := bson.Raw(raw).Lookup("n").Int64OK(); !ok || got != n {
			t.Fatalf("BSON n=%d,%v want %d", got, ok, n)
		}
	}
	raw, err := held.Get(id)
	check(raw, err, 0)
	raw, err = reader.Get("rows", id)
	check(raw, err, last+2)
	last += 2
	if control.exactFrozen {
		// While the sole pressure worker still owns the older WAL sync,
		// fill and rotate a real second generation and accept a suffix.
		// Both the certification and the suffix predate the worker's next
		// pass, so its applied endpoint distinguishes an extra rollover.
		publish := func() {
			last++
			go func(n int64) { done <- write(n) }(last)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				resume()
				t.Fatalf("generation rollover or suffix waited for older WAL sync: %v", <-done)
			}
		}
		for attempts := 0; ; attempts++ {
			if attempts == 1024 {
				t.Fatal("second generation never crossed its original watermark")
			}
			o, err := backend.ObserveNativeProducer()
			if err != nil {
				t.Fatal(err)
			}
			if o.UnderPressure {
				if !o.Semantic.GenerationUnderPressure || o.Semantic.ConstructionBytes != 0 {
					t.Fatalf("expected only generation pressure: %+v", o)
				}
				break
			}
			publish()
		}
		frozen := prefix.LogicalFrontiers()
		publish()
		publish()
		resume()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		endpoint := rootpublication.SealedPrefix{LogicalSequence: frozen.V, CanonicalLSN: frozen.C}
		if err := prefix.WaitAccepted(ctx, endpoint); err != nil {
			t.Fatal(err)
		}
		f = prefix.LogicalFrontiers()
		if f.S != endpoint || f.V != frozen.V+2 || f.C != frozen.C+2 || f.W != old.W {
			t.Fatalf("generation worker applied past exact frozen endpoint %+v: %+v", endpoint, f)
		}
		raw, err = held.Get(id)
		check(raw, err, 0)
		raw, err = reader.Get("rows", id)
		check(raw, err, last)
	}
	resume()
	if syncFailure == nil {
		if control.rotate {
			select {
			case synced := <-completedSync:
				if synced.err != nil || synced.path != heldSync.Path || synced.lsn != heldSync.LSN || !os.SameFile(heldFile, synced.info) {
					t.Fatalf("captured WAL sync changed across rotation: before=%+v after=%+v", heldSync, synced)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("did not observe the physical sync of the retired captured WAL file")
			}
		}
		if holdWAL {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := prefix.WaitAccepted(ctx, rootpublication.SealedPrefix{LogicalSequence: old.V, CanonicalLSN: old.C}); err != nil {
				t.Fatal(err)
			}
			after := prefix.LogicalFrontiers()
			if after.S.CanonicalLSN < old.C || after.S.CanonicalLSN >= f.C || after.C != f.C || after.W != old.W {
				t.Fatalf("generation seal must exclude the newer accepted suffix: before=%+v now=%+v", f, after)
			}
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		f = prefix.LogicalFrontiers()
		if f.S.LogicalSequence != f.V || f.S.CanonicalLSN != f.C || prefix.AcceptedPendingCount() != 0 {
			t.Fatalf("incomplete original physical retirement %+v", f)
		}
	} else {
		select {
		case <-backgroundFailure:
		case <-time.After(10 * time.Second):
			t.Fatal("generation worker did not report its original WAL sync failure")
		}
		if control.closeFromNotify {
			select {
			case closeErr := <-callbackClose:
				if closeErr == nil || !strings.Contains(closeErr.Error(), "Close from active publication Finish callback") {
					t.Fatalf("callback Close did not refuse reentry: %v", closeErr)
				}
			case <-time.After(2 * time.Second):
				callbackBlocked = true // A second Close would join the same stuck worker.
				t.Fatal("generation NotifyError callback deadlocked calling Close")
			}
		}
		before := db.backend.CommandWALNextLSN()
		if err := write(last + 1); !errors.Is(err, backenddb.ErrRecoveryRequired) {
			t.Fatalf("post-failure UPDATE = %v, want recovery required", err)
		}
		if after := db.backend.CommandWALNextLSN(); after != before {
			t.Fatalf("post-failure UPDATE assigned a canonical LSN: %d -> %d", before, after)
		}
		if after := prefix.LogicalFrontiers(); after != f {
			t.Fatalf("failed WAL sync changed logical, durable or applied frontiers: %+v -> %+v", f, after)
		}
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	closeErr := db.Close()
	db = nil
	if !errors.Is(closeErr, syncFailure) {
		t.Fatalf("checked Close = %v, want %v", closeErr, syncFailure)
	}
	requirePublicCOWFinalZero(t, cache)
	opts.FreshSystemPagerLeaves = false
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	reader, err = collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = reader.Get("rows", id)
	check(raw, err, last)
	if control.systemPager {
		snapshot := backend.AcquireSnapshot()
		if snapshot == nil {
			t.Fatal("missing recovered physical snapshot")
		}
		if err := leafrefscan.Walk(context.Background(), snapshot.State().SystemRootPageID, snapshot.Pager().Get, nil, func(page.LeafLogPtr) error {
			return errors.New("WAL replay wrote a system leaf to the outer log")
		}); err != nil {
			t.Error(err)
		}
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
