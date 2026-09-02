package treedbadapter

import (
	"bytes"
	"errors"
	"math"
	"runtime"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

func TestAdapterGetAfterCloseDoesNotError(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	adapter := Wrap(db)
	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got, err := adapter.Get([]byte("missing")); err != nil {
		t.Fatalf("get after close err=%v got=%q", err, got)
	}
	if got, err := adapter.GetUnsafe([]byte("missing")); err != nil {
		t.Fatalf("get unsafe after close err=%v got=%q", err, got)
	}
	if got, err := adapter.GetAppend([]byte("missing"), []byte("x")); err != nil {
		t.Fatalf("get append after close err=%v got=%q", err, got)
	}
	if ok, err := adapter.Has([]byte("missing")); err != nil || ok {
		t.Fatalf("has after close err=%v ok=%v", err, ok)
	}
}

func TestAdapterWritesAfterCloseDoNotError(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                 dir,
		Durability:          treedb.DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	adapter := Wrap(db)

	batchWrite, err := adapter.NewBatch()
	if err != nil {
		t.Fatalf("new batch: %v", err)
	}
	if err := batchWrite.Set([]byte("batch-k"), []byte("batch-v")); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	batchSync, err := adapter.NewBatch()
	if err != nil {
		t.Fatalf("new sync batch: %v", err)
	}
	if err := batchSync.Set([]byte("batch-sync-k"), []byte("batch-sync-v")); err != nil {
		t.Fatalf("batch sync set: %v", err)
	}

	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := adapter.Set([]byte("late-set"), []byte("v")); err != nil {
		t.Fatalf("set after close: %v", err)
	}
	if err := adapter.SetSync([]byte("late-set-sync"), []byte("v")); err != nil {
		t.Fatalf("set sync after close: %v", err)
	}
	if err := adapter.Delete([]byte("late-delete")); err != nil {
		t.Fatalf("delete after close: %v", err)
	}
	if err := adapter.DeleteSync([]byte("late-delete-sync")); err != nil {
		t.Fatalf("delete sync after close: %v", err)
	}
	if err := batchWrite.Commit(); err != nil {
		t.Fatalf("batch commit after close: %v", err)
	}
	if err := batchSync.CommitSync(); err != nil {
		t.Fatalf("batch commit sync after close: %v", err)
	}
	_ = batchWrite.Close()
	_ = batchSync.Close()
}

func TestAdapterWriteErrorsPreserveNonClosedErrors(t *testing.T) {
	errBoom := errors.New("boom")
	if err := ignoreClosedWrite(errBoom); !errors.Is(err, errBoom) {
		t.Fatalf("ignoreClosedWrite(%v)=%v, want original error", errBoom, err)
	}
}

func TestAdapterReadBatch_IgnoresMissingAndDuplicates(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	adapter := wrapNamedWithReadWorkers(db, "TreeDB", 8)
	err = adapter.ReadBatch([][]byte{
		[]byte("k1"),
		[]byte("missing"),
		[]byte("k1"),
		[]byte("k2"),
		[]byte("missing2"),
	})
	if err != nil {
		t.Fatalf("readbatch: %v", err)
	}
}

func TestAdapterReadBatch_DuplicateHeavyInputDoesNotError(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("hot1"), []byte("v1")); err != nil {
		t.Fatalf("set hot1: %v", err)
	}
	if err := db.Set([]byte("hot2"), []byte("v2")); err != nil {
		t.Fatalf("set hot2: %v", err)
	}
	if err := db.Set([]byte("hot3"), []byte("v3")); err != nil {
		t.Fatalf("set hot3: %v", err)
	}

	keys := make([][]byte, 0, 96)
	for i := 0; i < 96; i++ {
		switch i % 6 {
		case 0, 1, 2:
			keys = append(keys, []byte("hot1"))
		case 3:
			keys = append(keys, []byte("hot2"))
		case 4:
			keys = append(keys, []byte("hot3"))
		default:
			keys = append(keys, []byte("missing"))
		}
	}

	adapter := wrapNamedWithReadWorkers(db, "TreeDB", 8)
	if err := adapter.ReadBatch(keys); err != nil {
		t.Fatalf("readbatch duplicate-heavy: %v", err)
	}
	if got, err := adapter.Get([]byte("hot1")); err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("get hot1 after readbatch err=%v got=%q", err, got)
	}
	if got, err := adapter.Get([]byte("hot2")); err != nil || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("get hot2 after readbatch err=%v got=%q", err, got)
	}
	if got, err := adapter.Get([]byte("hot3")); err != nil || !bytes.Equal(got, []byte("v3")) {
		t.Fatalf("get hot3 after readbatch err=%v got=%q", err, got)
	}
}

func TestAdapterReadBatch_DuplicateHeavy_UsesGetManyPath(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("hot1"), []byte("v1")); err != nil {
		t.Fatalf("set hot1: %v", err)
	}
	if err := db.Set([]byte("hot2"), []byte("v2")); err != nil {
		t.Fatalf("set hot2: %v", err)
	}
	if err := db.Set([]byte("hot3"), []byte("v3")); err != nil {
		t.Fatalf("set hot3: %v", err)
	}

	adapter := wrapNamedWithReadWorkers(db, "TreeDB", 8)
	origGetMany := adapter.readBatchGetMany
	t.Cleanup(func() { adapter.readBatchGetMany = origGetMany })

	calls := 0
	var captured [][]byte
	adapter.readBatchGetMany = func(innerDB *treedb.DB, keys [][]byte) ([][]byte, error) {
		calls++
		captured = make([][]byte, len(keys))
		for i := range keys {
			captured[i] = append([]byte(nil), keys[i]...)
		}
		return origGetMany(innerDB, keys)
	}

	keys := [][]byte{
		[]byte("hot1"),
		[]byte("hot1"),
		[]byte("hot2"),
		[]byte("hot1"),
		[]byte("missing"),
		[]byte("hot2"),
		[]byte("hot3"),
		[]byte("hot3"),
		[]byte("hot1"),
		[]byte("missing"),
		[]byte("hot2"),
		[]byte("hot3"),
	}
	if err := adapter.ReadBatch(keys); err != nil {
		t.Fatalf("readbatch duplicate-heavy: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected GetMany path call count=1, got=%d", calls)
	}

	wantCaptured := [][]byte{
		[]byte("hot1"),
		[]byte("hot2"),
		[]byte("missing"),
		[]byte("hot3"),
	}
	if len(captured) != len(wantCaptured) {
		t.Fatalf("captured key count=%d want=%d", len(captured), len(wantCaptured))
	}
	for i := range wantCaptured {
		if !bytes.Equal(captured[i], wantCaptured[i]) {
			t.Fatalf("captured[%d]=%q want=%q", i, captured[i], wantCaptured[i])
		}
	}
}

func TestAdapterReadBatch_ClampsWorkerCount(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set k: %v", err)
	}

	adapter := wrapNamedWithReadWorkers(db, "TreeDB", -1)
	// This intentionally inspects internal state because the clamped worker
	// value is adapter configuration and is not otherwise observable via API.
	if got := int(adapter.readWorkers.Load()); got != 1 {
		t.Fatalf("expected resolved readWorkers=%d got=%d", 1, got)
	}

	if err := adapter.ReadBatch([][]byte{[]byte("k"), []byte("missing")}); err != nil {
		t.Fatalf("readbatch: %v", err)
	}
}

func TestAdapterReadBatch_AfterCloseReturnsErrUnsupported(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	adapter := Wrap(db)
	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := adapter.ReadBatch([][]byte{[]byte("missing"), []byte("missing2")}); !errors.Is(err, kvstore.ErrUnsupported) {
		t.Fatalf("readbatch after close expected ErrUnsupported, got=%v", err)
	}
}

func TestAdapterReadBatch_AfterCloseGetManyEligibleReturnsErrUnsupported(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	adapter := wrapNamedWithReadWorkers(db, "TreeDB", 8)
	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	keys := [][]byte{
		[]byte("dup"),
		[]byte("dup"),
		[]byte("dup"),
		[]byte("other"),
		[]byte("dup"),
		[]byte("other"),
		[]byte("dup"),
		[]byte("other"),
	}
	if err := adapter.ReadBatch(keys); !errors.Is(err, kvstore.ErrUnsupported) {
		t.Fatalf("readbatch duplicate-heavy after close expected ErrUnsupported, got=%v", err)
	}
}

func TestAdapterReadBatch_GetManyPathRespectsReadWorkersBudget(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	prevGOMAXPROCS := runtime.GOMAXPROCS(16)
	defer runtime.GOMAXPROCS(prevGOMAXPROCS)

	keys := make([][]byte, 0, 256)
	for i := 0; i < 256; i++ {
		keys = append(keys, []byte("key-"+strconv.Itoa(i)))
	}

	limited := wrapNamedWithReadWorkers(db, "TreeDB", 2)
	limitedCalls := 0
	origLimited := limited.readBatchGetMany
	limited.readBatchGetMany = func(innerDB *treedb.DB, in [][]byte) ([][]byte, error) {
		limitedCalls++
		return origLimited(innerDB, in)
	}
	if err := limited.ReadBatch(keys); err != nil {
		t.Fatalf("limited readbatch: %v", err)
	}
	if limitedCalls != 0 {
		t.Fatalf("expected GetMany path skip for limited workers, calls=%d", limitedCalls)
	}

	wide := wrapNamedWithReadWorkers(db, "TreeDB", 16)
	wideCalls := 0
	origWide := wide.readBatchGetMany
	wide.readBatchGetMany = func(innerDB *treedb.DB, in [][]byte) ([][]byte, error) {
		wideCalls++
		return origWide(innerDB, in)
	}
	if err := wide.ReadBatch(keys); err != nil {
		t.Fatalf("wide readbatch: %v", err)
	}
	if wideCalls != 1 {
		t.Fatalf("expected GetMany path call count=1 for wide workers, got=%d", wideCalls)
	}
}

func TestAdapterSetReadWorkers_ClampsAndNormalizes(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	adapter := Wrap(db)
	adapter.SetReadWorkers(0)
	if got := adapter.readWorkers.Load(); got != 1 {
		t.Fatalf("expected normalized readWorkers=%d got=%d", 1, got)
	}

	if strconv.IntSize <= 32 {
		return
	}

	overMax := int64(math.MaxInt32) + 123
	adapter.SetReadWorkers(int(overMax))
	if got := adapter.readWorkers.Load(); got != math.MaxInt32 {
		t.Fatalf("expected clamped readWorkers=%d got=%d", math.MaxInt32, got)
	}
}
