package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type sealedWALForegroundFixture struct {
	db      *DB
	opts    Options
	rows    *collections.Collection
	prefix  *rootpublication.Coordinator
	oldPath string
	oldFile os.FileInfo
	value   int64
}

func newSealedWALForegroundFixture(t *testing.T) *sealedWALForegroundFixture {
	t.Helper()
	// The explicit force below drives the original automatic checkpoint worker.
	// An independent value-log generation kick would consume the same WAL debt.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.FreshRelaxedStandaloneWALStream = true
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = StandaloneCOWMemtableLimits()
	opts.CommandWALSegmentTargetBytes = 4 << 10
	opts.MaxWALBytes = 2 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	f := &sealedWALForegroundFixture{db: database, opts: opts}
	t.Cleanup(func() {
		if f.db != nil {
			if err := f.db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	f.prefix, _, err = backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	f.rows, err = manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	row, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.rows.InsertBatchValidatedBSON([][]byte{[]byte("row")}, [][]byte{row}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	names := publicCommandWALSegmentNames(t, filepath.Join(opts.Dir, "maindb"))
	if len(names) == 0 {
		t.Fatal("missing active WAL")
	}
	f.oldPath = filepath.Join(opts.Dir, "maindb", "wal", names[len(names)-1])
	f.oldFile, err = os.Stat(f.oldPath)
	if err != nil {
		t.Fatal(err)
	}
	for f.value = 1; f.value <= 512; f.value++ {
		if err := f.write(f.value); err != nil {
			t.Fatal(err)
		}
		names = publicCommandWALSegmentNames(t, filepath.Join(opts.Dir, "maindb"))
		if names[len(names)-1] != filepath.Base(f.oldPath) {
			return f
		}
	}
	t.Fatal("relaxed BSON UPDATEs did not rotate the original WAL")
	return nil
}

func (f *sealedWALForegroundFixture) write(value int64) error {
	typ, data, err := bson.MarshalValue(value)
	if err != nil {
		return err
	}
	result, accepted, err := f.rows.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{
		DocumentID: []byte("row"),
		Fields:     []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: data}}},
	}})
	if err != nil {
		return err
	}
	if !accepted || len(result) != 1 || !result[0].Matched || !result[0].Modified {
		return fmt.Errorf("BSON UPDATE %d returned %+v, accepted=%v", value, result, accepted)
	}
	return nil
}

func (f *sealedWALForegroundFixture) verifyOld(event durabilitycut.Event) error {
	if event.StablePath == nil {
		return errors.New("selected WAL operation has no retained descriptor")
	}
	info, err := event.StablePath.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, f.oldFile) || !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("selected WAL descriptor differs from sealed predecessor %s", f.oldPath)
	}
	return nil
}

func (f *sealedWALForegroundFixture) reopen(t *testing.T) {
	t.Helper()
	current := f.db
	f.db = nil
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := Open(f.opts)
	if err != nil {
		t.Fatal(err)
	}
	f.db = database
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := collections.NewCollectionManager(backend).OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := rows.Get([]byte("row"))
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bson.Raw(doc).Lookup("n").Int64OK(); !ok || n != f.value {
		t.Fatalf("reopened BSON n=%d ok=%v, want %d", n, ok, f.value)
	}
}

func TestAutomaticCheckpointSealedWALAdmitsRelaxedBSONThroughRotation(t *testing.T) {
	f := newSealedWALForegroundFixture(t)
	beforeCount := statMapUint64(t, f.db.cached.Stats(), "treedb.cache.auto_checkpoint.count")
	entered := make(chan error, 1)
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	var beforeSync, afterSync atomic.Int32
	var successor atomic.Pointer[string]
	var lastSuccessorLSN atomic.Uint64
	var held atomic.Bool
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if held.Load() {
			if e.Namespace == durabilitycut.NamespaceCreate && filepath.Dir(e.NewPath) == filepath.Dir(f.oldPath) && commitlog.IsCommandSegmentName(filepath.Base(e.NewPath)) {
				successor.CompareAndSwap(nil, &e.NewPath)
			}
			if path := successor.Load(); path != nil && e.Point == durabilitycut.AfterDependencyAppend && e.Path == *path {
				lastSuccessorLSN.Store(e.LSN)
			}
		}
		if e.Path != f.oldPath {
			return nil
		}
		switch e.Point {
		case durabilitycut.BeforeDependencyFileSync:
			if beforeSync.Add(1) == 1 {
				err := f.verifyOld(e)
				held.Store(true)
				entered <- err
				<-release
				held.Store(false)
				return err
			}
		case durabilitycut.AfterDependencyFileSync:
			afterSync.Add(1)
		}
		return nil
	}))
	defer restore()
	defer resume()
	f.db.triggerAutoCheckpointForTest()
	select {
	case err := <-entered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("automatic checkpoint did not visit the original sealed descriptor")
	}
	base := f.prefix.LogicalFrontiers()
	// Only the original checkpoint is running when the observer parks. Force a
	// second real size rollover, then require a fresh UPDATE on its successor.
	start := f.value
	type completion struct {
		value int64
		err   error
	}
	done := make(chan completion, 1)
	go func() {
		for i := start + 1; i <= start+512; i++ {
			alreadyRotated := successor.Load() != nil
			if err := f.write(i); err != nil {
				done <- completion{i, err}
				return
			}
			if alreadyRotated {
				done <- completion{i, nil}
				return
			}
		}
		done <- completion{err: errors.New("no real successor rotation while checkpoint held the predecessor")}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		f.value = r.value
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		resume()
		<-done
		t.Fatal("fresh relaxed BSON UPDATE could not finish during the old sealed-file sync")
	}
	next := f.prefix.LogicalFrontiers()
	delta := uint64(f.value - start)
	if delta < 2 || next.C != base.C+delta || next.V != base.V+delta || next.W != base.W || next.S != base.S || lastSuccessorLSN.Load() != next.C || afterSync.Load() != 0 {
		t.Fatalf("acknowledged %d updates at held predecessor: before=%+v after=%+v last successor LSN=%d physical successes=%d", delta, base, next, lastSuccessorLSN.Load(), afterSync.Load())
	}
	resume()
	if err := waitForPublicCommandWALAutoCheckpointCount(f.db, beforeCount+1); err != nil {
		t.Fatal(err)
	}
	restore()
	if beforeSync.Load() != 1 || afterSync.Load() != 1 {
		t.Fatalf("sealed file physical sync before=%d after=%d", beforeSync.Load(), afterSync.Load())
	}
	if got := f.prefix.LogicalFrontiers().W; got < next.C {
		t.Fatalf("completed checkpoint W=%d does not cover the acknowledged successor %d", got, next.C)
	}
	f.reopen(t)
}

func TestDurableMarkerRefusesFailedSealedWALDependency(t *testing.T) {
	f := newSealedWALForegroundFixture(t)
	nextLSN := f.db.backend.CommandWALNextLSN()
	base := f.prefix.LogicalFrontiers()
	fault := errors.New("sealed WAL pre-physical refusal")
	var refused, later atomic.Int32
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Path == f.oldPath && e.Point == durabilitycut.BeforeDependencyFileSync {
			if err := f.verifyOld(e); err != nil {
				return err
			}
			refused.Add(1)
			return fault
		}
		if e.Point == durabilitycut.AfterDependencyAppend || (refused.Load() != 0 &&
			(e.Point == durabilitycut.AfterDependencyFileSync || e.Point == durabilitycut.BeforeDependencyFileSync || e.Point == durabilitycut.BeforeNewFileDirectorySync)) {
			later.Add(1)
		}
		return nil
	}))
	defer restore()
	err := writeEmptyPublicCommandWALSync(f.db)
	restore()
	if !errors.Is(err, fault) || refused.Load() != 1 || later.Load() != 0 || f.db.backend.CommandWALNextLSN() != nextLSN || f.prefix.LogicalFrontiers() != base {
		t.Fatalf("refused marker err=%v old=%d subsequent operations=%d nextLSN=%d frontiers=%+v (before=%+v)", err, refused.Load(), later.Load(), f.db.backend.CommandWALNextLSN(), f.prefix.LogicalFrontiers(), base)
	}
	if err := writeEmptyPublicCommandWALSync(f.db); err != nil {
		t.Fatalf("retry after pre-physical refusal: %v", err)
	}
	if f.db.backend.CommandWALNextLSN() != nextLSN+1 || f.prefix.LogicalFrontiers().W < nextLSN {
		t.Fatalf("successful retry failed to cover marker %d", nextLSN)
	}
	f.value++
	if err := f.write(f.value); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
}

// A vacuum can own raw publication before it enters backend maintenance.
// Hold that exact lock through re-entry: the checkpoint must defer and
// release maintenance without certifying obsolete work.
func TestAutomaticCheckpointSealedWALDefersBusyReentry(t *testing.T) {
	f := newSealedWALForegroundFixture(t)
	backend, err := f.db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	held, resume := make(chan error, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	var oldSyncs atomic.Int32
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Path == f.oldPath && e.Point == durabilitycut.BeforeDependencyFileSync && oldSyncs.Add(1) == 1 {
			err := f.verifyOld(e)
			held <- err
			<-resume
			return err
		}
		return nil
	}))
	defer restore()
	defer unblock()
	type result struct {
		pending bool
		err     error
	}
	done := make(chan result, 1)
	go func() { p, err := backend.PrepareCommandWALCoveredPrefixCleanup(); done <- result{p, err} }()
	select {
	case err := <-held:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("no original sealed I/O")
	}
	before := f.prefix.LogicalFrontiers()
	// Use the actual raw-publish guard and wait for positive acquisition.
	acquired := make(chan error, 1)
	release := make(chan struct{})
	releaseGuard := sync.OnceFunc(func() { close(release) })
	defer releaseGuard()
	released := make(chan struct{})
	go func() {
		guard, err := backend.LockPrivateCOWStagingGuard(f.db.txnOwner)
		acquired <- err
		if err == nil {
			<-release
			guard.Release()
		}
		close(released)
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("raw guard stayed locked during exact old-file sync")
	}
	unblock()
	select {
	case r := <-done:
		if r.pending || r.err != nil || f.prefix.LogicalFrontiers() != before {
			t.Fatalf("busy re-entry returned %+v at frontiers %+v, want deferred at %+v", r, f.prefix.LogicalFrontiers(), before)
		}
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("re-entering checkpoint waited for raw publication while holding maintenance")
	}
	releaseGuard()
	<-released
	if _, err := backend.PrepareCommandWALCoveredPrefixCleanup(); err != nil {
		t.Fatal(err)
	}
	if oldSyncs.Load() != 2 || f.prefix.LogicalFrontiers().W < before.C {
		t.Fatalf("fresh checkpoint failed to repeat uncredited file or cover prefix: old syncs=%d, frontier=%+v", oldSyncs.Load(), f.prefix.LogicalFrontiers())
	}
	restore()
	f.reopen(t)
}
