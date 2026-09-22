package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
)

func openLeafLogLaneReadTestDB(t *testing.T, cacheEntries int) (*DB, LeafPageLogCloser, Options) {
	t.Helper()
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 * 1024,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPageReadCacheEntries:   cacheEntries,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionOff,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionOff,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	db.SetLeafPageLog(leafLog)
	return db, leafLog, opts
}

func leafLaneReadKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%06d", i))
}

func leafLaneReadValue(prefix string, i int) []byte {
	return []byte(fmt.Sprintf("%s-%06d", prefix, i))
}

func writeLeafLaneReadBatch(t *testing.T, db *DB, count int, prefix string) map[string][]byte {
	t.Helper()
	b := db.NewBatch()
	model := make(map[string][]byte, count)
	for i := 0; i < count; i++ {
		key := leafLaneReadKey(i)
		value := leafLaneReadValue(prefix, i)
		if err := b.Set(key, value); err != nil {
			_ = b.Close()
			t.Fatalf("Set(%d): %v", i, err)
		}
		model[string(key)] = value
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write %s batch: %v", prefix, err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close %s batch: %v", prefix, err)
	}
	return model
}

func requireLeafLaneReferencedFileIDs(t *testing.T, db *DB, min int) map[uint32]struct{} {
	t.Helper()
	state := db.State()
	if state == nil || state.RootPageID == 0 || state.ValueLogSet == nil {
		t.Fatalf("state missing root/value-log set: %+v", state)
	}
	refs := collectLeafRefIDsFromRoot(t, db, state.RootPageID)
	fileIDs := make(map[uint32]struct{}, len(refs))
	for ptr := range refs {
		fileID := ptr.ValuePtr().FileID
		fileIDs[fileID] = struct{}{}
		if _, ok := state.ValueLogSet.Files[fileID]; !ok {
			t.Fatalf("referenced leaf-log file %d missing from ValueLogSet", fileID)
		}
	}
	if len(fileIDs) < min {
		t.Fatalf("referenced leaf-log files=%d want >=%d (refs=%d)", len(fileIDs), min, len(refs))
	}
	return fileIDs
}

func requireLeafLaneReadSurfaces(t *testing.T, db *DB, model map[string][]byte, count int) {
	t.Helper()
	const minReadSurfaceKeyCount = 3080
	if count < minReadSurfaceKeyCount {
		t.Fatalf("read-surface fixture count=%d want >=%d", count, minReadSurfaceKeyCount)
	}
	sampleIdx := []int{0, 1, 97, 1029, 2053, 3079, count - 1}
	getManyKeys := make([][]byte, 0, len(sampleIdx)+1)
	for _, idx := range sampleIdx {
		key := leafLaneReadKey(idx)
		want := model[string(key)]
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q)=%q want %q", key, got, want)
		}
		unsafe, err := db.GetUnsafe(key)
		if err != nil {
			t.Fatalf("GetUnsafe(%q): %v", key, err)
		}
		if !bytes.Equal(unsafe, want) {
			t.Fatalf("GetUnsafe(%q)=%q want %q", key, unsafe, want)
		}
		buf := []byte("prefix:")
		appended, err := db.GetAppend(key, buf)
		if err != nil {
			t.Fatalf("GetAppend(%q): %v", key, err)
		}
		if !bytes.Equal(appended, append([]byte("prefix:"), want...)) {
			t.Fatalf("GetAppend(%q)=%q want prefix+%q", key, appended, want)
		}
		getManyKeys = append(getManyKeys, key)
	}
	missingKey := []byte("key-missing")
	getManyKeys = append(getManyKeys, missingKey)
	many, err := db.GetMany(getManyKeys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(many) != len(getManyKeys) {
		t.Fatalf("GetMany len=%d want %d", len(many), len(getManyKeys))
	}
	for i, key := range getManyKeys {
		want := model[string(key)]
		if want == nil {
			if many[i] != nil {
				t.Fatalf("GetMany(%q)=%q want nil", key, many[i])
			}
			continue
		}
		if !bytes.Equal(many[i], want) {
			t.Fatalf("GetMany(%q)=%q want %q", key, many[i], want)
		}
	}
	seen := make([]bool, len(getManyKeys))
	var seenMu sync.Mutex
	if err := db.GetManyView(getManyKeys, func(index int, key []byte, value []byte, found bool) error {
		if index < 0 || index >= len(getManyKeys) {
			return fmt.Errorf("callback index %d out of range", index)
		}
		seenMu.Lock()
		seen[index] = true
		seenMu.Unlock()
		want := model[string(key)]
		if want == nil {
			if found || value != nil {
				return fmt.Errorf("GetManyView(%q) found=%v value=%q want missing", key, found, value)
			}
			return nil
		}
		if !found || !bytes.Equal(value, want) {
			return fmt.Errorf("GetManyView(%q) found=%v value=%q want %q", key, found, value, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("GetManyView: %v", err)
	}
	seenMu.Lock()
	for i, ok := range seen {
		if !ok {
			seenMu.Unlock()
			t.Fatalf("GetManyView did not visit index %d", i)
		}
	}
	seenMu.Unlock()
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	seenCount := 0
	for ; it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		want := model[string(key)]
		if want == nil {
			t.Fatalf("Iterator unexpected key %q", key)
		}
		if got := it.Value(); !bytes.Equal(got, want) {
			t.Fatalf("Iterator value for %q=%q want %q", key, got, want)
		}
		seenCount++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}
	if seenCount != count {
		t.Fatalf("Iterator count=%d want %d", seenCount, count)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Iterator close: %v", err)
	}
	it = nil

	start, end := leafLaneReadKey(1000), leafLaneReadKey(1100)
	rangeIt, err := db.Iterator(start, end)
	if err != nil {
		t.Fatalf("range Iterator: %v", err)
	}
	rangeCount := 0
	for ; rangeIt.Valid(); rangeIt.Next() {
		key := append([]byte(nil), rangeIt.Key()...)
		if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
			t.Fatalf("range Iterator key %q outside [%q,%q)", key, start, end)
		}
		want := model[string(key)]
		if !bytes.Equal(rangeIt.Value(), want) {
			t.Fatalf("range Iterator value for %q=%q want %q", key, rangeIt.Value(), want)
		}
		rangeCount++
	}
	if err := rangeIt.Error(); err != nil {
		t.Fatalf("range Iterator error: %v", err)
	}
	if err := rangeIt.Close(); err != nil {
		t.Fatalf("range Iterator close: %v", err)
	}
	if rangeCount != 100 {
		t.Fatalf("range Iterator count=%d want 100", rangeCount)
	}
}

func TestLeafPageLogLanes_ReadSurfacesNoCacheAndReadOnlyOpen(t *testing.T) {
	const count = 4096
	for _, cacheEntries := range []int{-1, 128} {
		cacheEntries := cacheEntries
		name := "cache_enabled"
		if cacheEntries < 0 {
			name = "cache_disabled"
		}
		t.Run(name, func(t *testing.T) {
			db, leafLog, opts := openLeafLogLaneReadTestDB(t, cacheEntries)
			closed := false
			defer func() {
				if closed {
					return
				}
				if db != nil {
					_ = db.Close()
				}
				if leafLog != nil {
					_ = leafLog.Close()
				}
			}()

			writeLeafLaneReadBatch(t, db, count, "base")
			model := writeLeafLaneReadBatch(t, db, count, "final")
			if got := requireDBStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
				t.Fatalf("span-native used ops = 0, want lane-routed leaf output")
			}
			requireLeafLogCurrentSegments(t, db, 2)
			referencedBeforeClose := requireLeafLaneReferencedFileIDs(t, db, 2)
			requireLeafLaneReadSurfaces(t, db, model, count)
			if cacheEntries < 0 {
				if got := requireDBStatUint64(t, db, "treedb.process.read_path.outer_leaf.cache.capacity"); got != 0 {
					t.Fatalf("disabled leaf-page read cache capacity=%d want 0", got)
				}
			}

			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close DB: %v", err)
			}
			if err := leafLog.Close(); err != nil {
				t.Fatalf("Close leaf log: %v", err)
			}
			closed = true

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			requireReferencedFileIDsInState(t, reopened, referencedBeforeClose)
			requireLeafLaneReadSurfaces(t, reopened, model, count)
			if err := reopened.Close(); err != nil {
				t.Fatalf("Close reopened: %v", err)
			}

			readOnlyOpts := opts
			readOnlyOpts.ReadOnly = true
			readOnly, err := Open(readOnlyOpts)
			if err != nil {
				t.Fatalf("read-only Open: %v", err)
			}
			defer func() { _ = readOnly.Close() }()
			requireReferencedFileIDsInState(t, readOnly, referencedBeforeClose)
			requireLeafLaneReadSurfaces(t, readOnly, model, count)
		})
	}
}

func TestLeafPageLogLanes_IteratorSnapshotSurvivesMultiLaneGC(t *testing.T) {
	const count = 4096
	db, leafLog, _ := openLeafLogLaneReadTestDB(t, -1)
	defer func() {
		_ = db.Close()
		_ = leafLog.Close()
	}()

	writeLeafLaneReadBatch(t, db, count, "base")
	midModel := writeLeafLaneReadBatch(t, db, count, "mid")
	requireLeafLogCurrentSegments(t, db, 2)
	midState := db.State()
	if midState == nil || midState.ValueLogSet == nil {
		t.Fatalf("mid state missing value-log set: %+v", midState)
	}
	midRefs := collectLeafRefIDsFromRoot(t, db, midState.RootPageID)
	if len(midRefs) == 0 {
		t.Fatal("mid root has no leaf-log refs")
	}
	midPaths := make(map[string]struct{})
	for ptr := range midRefs {
		fileID := ptr.ValuePtr().FileID
		f := midState.ValueLogSet.Files[fileID]
		if f == nil || f.Path == "" {
			t.Fatalf("mid leaf ref file %d missing from ValueLogSet", fileID)
		}
		midPaths[f.Path] = struct{}{}
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	writeLeafLaneReadBatch(t, db, count, "final")
	// Settle the activated publication before destructive maintenance captures
	// its recoverable-root capability. The open iterator still pins the mid root
	// and is the retention boundary this test is intended to exercise.
	if err := db.Checkpoint(); err != nil {
		_ = it.Close()
		t.Fatalf("Checkpoint final batch with iterator open: %v", err)
	}
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		_ = it.Close()
		t.Fatalf("LeafGenerationGC with iterator open: %v", err)
	}
	for path := range midPaths {
		if _, err := os.Stat(path); err != nil {
			_ = it.Close()
			t.Fatalf("snapshot-referenced leaf-log path removed during GC: %s err=%v", path, err)
		}
	}
	seen := 0
	for ; it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		want := midModel[string(key)]
		if want == nil {
			_ = it.Close()
			t.Fatalf("snapshot iterator unexpected key %q", key)
		}
		if got := it.Value(); !bytes.Equal(got, want) {
			_ = it.Close()
			t.Fatalf("snapshot iterator value for %q=%q want %q", key, got, want)
		}
		seen++
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		t.Fatalf("snapshot iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("snapshot iterator close: %v", err)
	}
	if seen != count {
		t.Fatalf("snapshot iterator count=%d want %d", seen, count)
	}
}

func requireReferencedFileIDsInState(t *testing.T, db *DB, fileIDs map[uint32]struct{}) {
	t.Helper()
	state := db.State()
	if state == nil || state.ValueLogSet == nil {
		t.Fatalf("state missing value-log set: %+v", state)
	}
	for fileID := range fileIDs {
		if _, ok := state.ValueLogSet.Files[fileID]; !ok {
			t.Fatalf("state ValueLogSet missing referenced leaf-log file %d", fileID)
		}
	}
}
