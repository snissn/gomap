package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func newSealedWALForegroundFixture(t *testing.T, dedicated bool) *sealedWALForegroundFixture {
	t.Helper()
	// The explicit force below drives the original automatic checkpoint worker.
	// An independent value-log generation kick would consume the same WAL debt.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := t.TempDir()
	if dedicated {
		// The caller supplies one empty store with a real ext4 WAL submount.
		// A symlink cannot satisfy the WAL's stable namespace checks.
		dir = os.Getenv("TREEDB_TEST_DEDICATED_WAL_ROOT")
		if dir == "" {
			t.Skip("requires a private ext4 WAL submount")
		}
		main := filepath.Join(dir, "maindb")
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 1 || entries[0].Name() != "maindb" {
			t.Fatalf("expected an empty dedicated test store: %v, %v", entries, err)
		}
		entries, err = os.ReadDir(main)
		if err != nil || len(entries) != 1 || entries[0].Name() != "wal" {
			t.Fatalf("expected only the mounted WAL directory: %v, %v", entries, err)
		}
		wal, err := os.Open(filepath.Join(main, "wal"))
		if err != nil {
			t.Fatal(err)
		}
		names, readErr := wal.Readdirnames(-1)
		ext4 := commitlog.IsExt4WALFilesystem(wal)
		if err := errors.Join(readErr, wal.Close()); err != nil || len(names) != 0 || !ext4 {
			t.Fatalf("expected an empty ext4 WAL mount: %v, %v, ext4=%t", names, err, ext4)
		}
	}
	opts := OptionsFor(ProfileCommandWALRelaxed, dir)
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
	f.rotateRelaxedWAL(t)
	return f
}

func (f *sealedWALForegroundFixture) rotateRelaxedWAL(t *testing.T) {
	t.Helper()
	main := filepath.Join(f.opts.Dir, "maindb")
	names := publicCommandWALSegmentNames(t, main)
	if len(names) == 0 {
		t.Fatal("missing active WAL")
	}
	f.oldPath = filepath.Join(main, "wal", names[len(names)-1])
	oldFile, err := os.Stat(f.oldPath)
	if err != nil {
		t.Fatal(err)
	}
	f.oldFile = oldFile
	for range 512 {
		f.value++
		if err := f.write(f.value); err != nil {
			t.Fatal(err)
		}
		names = publicCommandWALSegmentNames(t, main)
		if names[len(names)-1] != filepath.Base(f.oldPath) {
			return
		}
	}
	t.Fatal("relaxed BSON UPDATEs did not rotate the original WAL")
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
	f := newSealedWALForegroundFixture(t, true)
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
	f := newSealedWALForegroundFixture(t, false)
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
	f := newSealedWALForegroundFixture(t, true)
	backend, err := f.db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	held, resume := make(chan error, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	var oldSyncs atomic.Int32
	var canonicalRetries, extraHandoffs int
	var releaseSecondGuard func()
	defer func() {
		if releaseSecondGuard != nil {
			releaseSecondGuard()
		}
	}()
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL || e.Path != f.oldPath || e.Point != durabilitycut.BeforeDependencyFileSync {
			return nil
		}
		if err := f.verifyOld(e); err != nil {
			return err
		}
		switch oldSyncs.Add(1) {
		case 1:
			held <- nil
			<-resume
		case 2, 3:
			// Apply contention to a handoff every time it yields. The canonical
			// producer holds raw already, so never acquire a second guard there.
			var stack [8192]byte
			frames := string(stack[:runtime.Stack(stack[:], false)])
			switch {
			case strings.Contains(frames, ".syncCheckpointRotationFile("):
				extraHandoffs++
				if extraHandoffs == 1 && canonicalRetries == 0 {
					guard, err := backend.LockPrivateCOWStagingGuard(f.db.txnOwner)
					if err != nil {
						return err
					}
					releaseSecondGuard = guard.Release
				}
			case strings.Contains(frames, ".closeCommandWALCheckpointPrefix("):
				canonicalRetries++
			default:
				return fmt.Errorf("unknown producer for exact sealed file: %s", frames)
			}
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
	// Even after a lost reentry, an initially busy automatic pass must return
	// without waiting, performing I/O or consuming the next admitted retry.
	go func() { p, err := backend.PrepareCommandWALCoveredPrefixCleanup(); done <- result{p, err} }()
	select {
	case r := <-done:
		if r.pending || r.err != nil || oldSyncs.Load() != 1 || f.prefix.LogicalFrontiers() != before {
			t.Fatalf("initially busy retry did work: %+v, old syncs=%d", r, oldSyncs.Load())
		}
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("initially busy retry blocked")
	}
	releaseGuard()
	<-released
	if _, err := backend.PrepareCommandWALCoveredPrefixCleanup(); err != nil {
		t.Fatal(err)
	}
	if oldSyncs.Load() != 2 || canonicalRetries != 1 || extraHandoffs != 0 || f.prefix.LogicalFrontiers().W < before.C {
		t.Fatalf("admitted retry failed to close prefix under raw publication: old syncs=%d canonical=%d handoffs=%d W=%d target=%d", oldSyncs.Load(), canonicalRetries, extraHandoffs, f.prefix.LogicalFrontiers().W, before.C)
	}
	// A later, independently registered sealed file may use the handoff again.
	f.rotateRelaxedWAL(t)
	next := f.prefix.LogicalFrontiers().C
	if _, err := backend.PrepareCommandWALCoveredPrefixCleanup(); err != nil {
		t.Fatal(err)
	}
	if oldSyncs.Load() != 3 || canonicalRetries != 1 || extraHandoffs != 1 || f.prefix.LogicalFrontiers().W < next {
		t.Fatalf("subsequent prefix failed to hand off: old syncs=%d canonical=%d handoffs=%d W=%d target=%d", oldSyncs.Load(), canonicalRetries, extraHandoffs, f.prefix.LogicalFrontiers().W, next)
	}
	restore()
	f.reopen(t)
}

// The same-device layout must take the existing in-lock route even when its
// WAL otherwise qualifies for the ext4 stream policy.
func TestAutomaticCheckpointSealedWALSharedVolumeKeepsRaw(t *testing.T) {
	f := newSealedWALForegroundFixture(t, false)
	syncs := 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL || e.Path != f.oldPath || e.Point != durabilitycut.BeforeDependencyFileSync {
			return nil
		}
		if err := f.verifyOld(e); err != nil {
			return err
		}
		var stack [8192]byte
		frames := string(stack[:runtime.Stack(stack[:], false)])
		if !strings.Contains(frames, ".closeCommandWALCheckpointPrefix(") ||
			strings.Contains(frames, ".syncCheckpointRotationFile(") {
			return fmt.Errorf("shared WAL bypassed canonical checkpoint closure: %s", frames)
		}
		syncs++
		return nil
	})
	_, err := f.db.backend.PrepareCommandWALCoveredPrefixCleanup()
	restore()
	if err != nil || syncs != 1 {
		t.Fatalf("checkpoint observed %d canonical predecessor syncs, err=%v", syncs, err)
	}
	f.value++
	if err := f.write(f.value); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
}
