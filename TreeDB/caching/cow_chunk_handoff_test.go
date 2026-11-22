package caching

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type cowChunkPauseBackend struct {
	*cowAcceptedReportBackend
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (b *cowChunkPauseBackend) NewPhysicalBatchWithSize(n int) batch.Interface {
	return &cowChunkPauseBatch{cowAcceptedReportBatch: &cowAcceptedReportBatch{Batch: b.DB.NewPhysicalBatchWithSize(n).(*backenddb.Batch), owner: b.cowAcceptedReportBackend}, owner: b}
}
func (b *cowChunkPauseBackend) NewPhysicalBatch() batch.Interface {
	return b.NewPhysicalBatchWithSize(0)
}

type cowChunkPauseBatch struct {
	*cowAcceptedReportBatch
	owner *cowChunkPauseBackend
}

func (b *cowChunkPauseBatch) wait(err error) error {
	if err == nil && b.group != nil && !b.final {
		b.owner.once.Do(func() { close(b.owner.paused); <-b.owner.resume })
	}
	return err
}
func (b *cowChunkPauseBatch) Write() error     { return b.wait(b.cowAcceptedReportBatch.Write()) }
func (b *cowChunkPauseBatch) WriteSync() error { return b.wait(b.cowAcceptedReportBatch.WriteSync()) }

func TestCOWStreamedPrivateChunksPreserveCapturedPrefixAndNewerSuffix(t *testing.T) {
	db := cowPointerFixture(t)
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Set([]byte(key), []byte("base")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	backend := &cowChunkPauseBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: db.backend.(*backenddb.DB)}, paused: make(chan struct{}), resume: make(chan struct{})}
	db.backend = backend
	db.flushBackendMaxEntries = 1
	if err := db.Set([]byte("a"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("a"), []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("d"), []byte("covered")); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	before := backend.State().CommitSeq
	done := make(chan error, 1)
	go func() { db.flushMu.Lock(); defer db.flushMu.Unlock(); done <- db.flushCOWFrozen(true, nil) }()
	resumed := false
	defer func() {
		if !resumed {
			close(backend.resume)
		}
	}()
	select {
	case <-backend.paused:
	case <-time.After(10 * time.Second):
		t.Fatal("streamed chunk did not pause")
	}
	if backend.State().CommitSeq != before || backend.publications.Load() != 0 {
		t.Fatal("private intermediate chunk published backend root")
	}
	if err = db.Set([]byte("a"), []byte("late-frozen")); err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("b"), []byte("late-frozen")); err != nil {
		t.Fatal(err)
	}
	if err = db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("a"), []byte("latest-mutable")); err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "second" {
		t.Fatalf("captured old=(%q,%v)", got, err)
	}
	if value, err := old.Get([]byte("b")); !errors.Is(err, tree.ErrKeyNotFound) || value != nil {
		t.Fatalf("covered tombstone=%v", err)
	}
	close(backend.resume)
	resumed = true
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("chunk handoff did not finish")
	}
	if backend.publications.Load() != 1 || backend.State().CommitSeq != before+1 {
		t.Fatal("captured chunks did not publish exactly once")
	}
	if len(db.cow.cut.frozen) == 0 {
		t.Fatal("new frozen suffix was removed with covered prefix")
	}
	for key, want := range map[string]string{"a": "latest-mutable", "b": "late-frozen", "c": "base", "d": "covered"} {
		if got, err := db.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("handoff Get(%s)=(%q,%v)", key, got, err)
		}
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"a": "latest-mutable", "b": "late-frozen", "c": "base", "d": "covered"} {
		if got, err := backend.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("final backend Get(%s)=(%q,%v)", key, got, err)
		}
	}
}

type cowFrozenRawOp struct {
	key, value string
	ptr        page.ValuePtr
	revision   page.EntryRevision
	deleted    bool
}

type cowFrozenRawChunk struct {
	ops   []cowFrozenRawOp
	final bool
}

type cowFrozenRawBackend struct {
	*cowAcceptedReportBackend
	chunks []cowFrozenRawChunk
}

func (b *cowFrozenRawBackend) NewPhysicalBatchWithSize(n int) batch.Interface {
	return &cowFrozenRawBatch{cowAcceptedReportBatch: b.cowAcceptedReportBackend.NewPhysicalBatchWithSize(n).(*cowAcceptedReportBatch), owner: b}
}

type cowFrozenRawBatch struct {
	*cowAcceptedReportBatch
	owner *cowFrozenRawBackend
	ops   []cowFrozenRawOp
}

func (b *cowFrozenRawBatch) SetViewWithRevision(k, v []byte, rev page.EntryRevision) error {
	b.ops = append(b.ops, cowFrozenRawOp{key: string(k), value: string(v), revision: rev})
	return b.Batch.SetViewWithRevision(k, v, rev)
}

func (b *cowFrozenRawBatch) SetPointerViewWithRevision(k []byte, ptr page.ValuePtr, rev page.EntryRevision) error {
	b.ops = append(b.ops, cowFrozenRawOp{key: string(k), ptr: ptr, revision: rev})
	return b.Batch.SetPointerViewWithRevision(k, ptr, rev)
}

func (b *cowFrozenRawBatch) DeleteViewWithRevision(k []byte, rev page.EntryRevision) error {
	b.ops = append(b.ops, cowFrozenRawOp{key: string(k), revision: rev, deleted: true})
	return b.Batch.DeleteViewWithRevision(k, rev)
}

func (b *cowFrozenRawBatch) record(err error) error {
	if b.group != nil {
		b.owner.chunks = append(b.owner.chunks, cowFrozenRawChunk{ops: b.ops, final: b.final})
	}
	return err
}

func (b *cowFrozenRawBatch) Write() error     { return b.record(b.cowAcceptedReportBatch.Write()) }
func (b *cowFrozenRawBatch) WriteSync() error { return b.record(b.cowAcceptedReportBatch.WriteSync()) }

func TestCOWCheckpointMergesFrozenRawSources(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backendOpts := backenddb.Options{Dir: filepath.Join(dir, "backend")}
			cacheOpts := Options{
				DisableWAL: true, AllowUnsafe: true, MemtableMode: mode, FlushThreshold: 1 << 30,
				MemtableShards: 2, ValueLogPointerThreshold: 64,
				ValueLogCompression: uint8(vlogCompressionOff),
			}
			open := func() (*DB, *cowFrozenRawBackend) {
				t.Helper()
				disk, err := backenddb.Open(backendOpts)
				if err != nil {
					t.Fatal(err)
				}
				backend := &cowFrozenRawBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: disk}}
				db, err := Open(dir, backend, cacheOpts)
				if err != nil {
					t.Fatal(errors.Join(err, disk.Close()))
				}
				t.Cleanup(func() {
					if !db.closing.Load() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					}
				})
				return db, backend
			}
			db, backend := open()
			db.flushBackendMaxEntries = 2
			oldValue := bytes.Repeat([]byte("old"), 1024)
			newValue := bytes.Repeat([]byte("new"), 1024)
			check := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			write := func(rows ...batch.Entry) {
				t.Helper()
				b := db.NewBatch()
				for _, row := range rows {
					if row.Type == batch.OpDelete {
						check(b.DeleteWithRevision(row.Key, row.Revision))
					} else {
						check(b.SetWithRevision(row.Key, row.Value, row.Revision))
					}
				}
				check(b.Write())
				check(b.Close())
				check(db.rolloverCOWForFlush())
			}
			write(
				batch.Entry{Key: []byte("a"), Value: oldValue, Revision: 90},
				batch.Entry{Key: []byte("b"), Value: []byte("old-b"), Revision: 91},
				batch.Entry{Key: []byte("d"), Value: []byte("old-d"), Revision: 93},
				batch.Entry{Key: []byte("f"), Value: []byte("old-f"), Revision: 95},
			)
			held, err := db.acquireCOWSnapshotWithError()
			check(err)
			t.Cleanup(func() { check(held.Close()) })
			// The later source wins with a LOWER revision on both shared keys.
			write(
				batch.Entry{Key: []byte("a"), Value: newValue, Revision: 21},
				batch.Entry{Type: batch.OpDelete, Key: []byte("b"), Revision: 22},
				batch.Entry{Key: []byte("c"), Value: []byte("new-c"), Revision: 23},
				batch.Entry{Key: []byte("e"), Value: []byte("new-e"), Revision: 25},
			)
			if n := len(db.cow.cut.frozen); n < 3 {
				t.Fatalf("expected overlapping generations in multiple hash partitions, got %d sources", n)
			}
			var wantPtr page.ValuePtr
			for _, source := range db.cow.cut.frozen {
				_, ptr, flags, rev, ok := source.GetEntryWithRevision([]byte("a"))
				if ok && rev == 21 && flags&node.FlagPointer != 0 {
					wantPtr = ptr
				}
			}
			if wantPtr == (page.ValuePtr{}) {
				t.Fatal("newest a was not stored by pointer")
			}
			check(db.Set([]byte("d"), []byte("suffix")))
			before := backend.State().CommitSeq
			db.flushMu.Lock()
			err = db.flushCOWFrozen(true, nil)
			db.flushMu.Unlock()
			check(err)
			want := []cowFrozenRawOp{
				{key: "a", ptr: wantPtr, revision: 21},
				{key: "b", revision: 22, deleted: true},
				{key: "c", value: "new-c", revision: 23},
				{key: "d", value: "old-d", revision: 93},
				{key: "e", value: "new-e", revision: 25},
				{key: "f", value: "old-f", revision: 95},
			}
			if len(backend.chunks) != 3 {
				t.Fatalf("physical chunks=%+v", backend.chunks)
			}
			for i, chunk := range backend.chunks {
				if chunk.final != (i == len(backend.chunks)-1) || !slices.Equal(chunk.ops, want[2*i:2*i+2]) {
					t.Fatalf("chunk %d=%+v, want ordered ops %+v", i, chunk, want[2*i:2*i+2])
				}
			}
			if backend.publications.Load() != 1 || backend.State().CommitSeq != before+1 {
				t.Fatal("captured prefix did not publish exactly once")
			}
			checkVersion := func(get func([]byte) ([]byte, page.EntryRevision, error), key string, value []byte, revision page.EntryRevision) {
				t.Helper()
				got, rev, err := get([]byte(key))
				if err != nil || !bytes.Equal(got, value) || rev != revision {
					t.Fatalf("GetVersioned(%q) length=%d revision=%d err=%v, want length=%d revision=%d", key, len(got), rev, err, len(value), revision)
				}
			}
			checkVersion(held.GetVersioned, "a", oldValue, 90)
			checkVersion(held.GetVersioned, "b", []byte("old-b"), 91)
			checkVersion(db.GetVersioned, "a", newValue, 21)
			if got, err := backend.Get([]byte("d")); err != nil || string(got) != "old-d" {
				t.Fatalf("persisted prefix d=(%q, %v)", got, err)
			}
			suffix, suffixRev, err := db.GetVersioned([]byte("d"))
			if err != nil || string(suffix) != "suffix" {
				t.Fatalf("current d=(%q, %d, %v)", suffix, suffixRev, err)
			}
			if found, err := db.Has([]byte("b")); err != nil || found {
				t.Fatalf("tombstoned b found=%v err=%v", found, err)
			}
			check(db.Checkpoint())
			checkVersion(held.GetVersioned, "a", oldValue, 90)
			check(held.Close())
			check(db.Close())
			db, _ = open()
			checkVersion(db.GetVersioned, "a", newValue, 21)
			checkVersion(db.GetVersioned, "c", []byte("new-c"), 23)
			checkVersion(db.GetVersioned, "d", []byte("suffix"), suffixRev)
			if found, err := db.Has([]byte("b")); err != nil || found {
				t.Fatalf("reopened tombstoned b found=%v err=%v", found, err)
			}
			check(db.Close())
		})
	}
}

func TestCOWFrozenRawChunksFinalizeAfterCursorClose(t *testing.T) {
	iterErr := errors.New("frozen cursor refused")
	closeErr := errors.New("frozen cursor close failed")
	writeErr := errors.New("private chunk failed")
	newIterator := func(keys string) *oneStableTraceIterator {
		it := &oneStableTraceIterator{trace: new([]string)}
		for _, k := range keys {
			it.rows = append(it.rows, oneStableRawRow{key: []byte(string(k)), flags: node.FlagInline})
		}
		return it
	}
	for _, tc := range []struct {
		name      string
		olderKeys string
		newerKeys string
		iterErr   error
		closeErr  error
		writeErr  error
		chunks    []int
		final     bool
	}{
		{name: "exact_multiple", olderKeys: "ac", newerKeys: "bd", chunks: []int{2, 2}, final: true},
		{name: "empty", chunks: []int{0}, final: true},
		{name: "initially_empty_error", newerKeys: "abcd", iterErr: iterErr, chunks: []int{2}},
		{name: "shadowed_terminal_error", olderKeys: "a", newerKeys: "abcd", iterErr: iterErr, chunks: []int{2}},
		{name: "shadowed_close_error", olderKeys: "a", newerKeys: "abcd", closeErr: closeErr, chunks: []int{2}},
		{name: "nonfinal_write_error", olderKeys: "ac", newerKeys: "bd", writeErr: writeErr, chunks: []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			older, newer := newIterator(tc.olderKeys), newIterator(tc.newerKeys)
			older.closeErr = tc.closeErr
			older.actions = map[string]func(){"Error": func() {
				if !older.Valid() {
					older.iterErr = tc.iterErr
				}
			}}
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			before := budget.Stats()
			lease, err := budget.AcquireExternal(8)
			if err != nil {
				t.Fatal(err)
			}
			merge := cowFrozenMerge{
				cursors: []stableUnsafeOpIter{{UnsafeIterator: older}, {UnsafeIterator: newer}},
				lease:   lease,
			}
			for i := range merge.cursors {
				it := &merge.cursors[i]
				if it.Valid() {
					merge.heap = append(merge.heap, stableOpMergeItem{iter: it, priority: 1 - i, key: it.Key()})
				}
			}
			var chunks []int
			final := false
			err = writeCOWFrozenRawChunks(nil, &merge, 2, func(ops []batch.Entry, last bool) error {
				chunks = append(chunks, len(ops))
				if final {
					t.Fatal("write after final chunk")
				}
				if last {
					if older.closes != 1 || newer.closes != 1 {
						t.Fatal("final chunk preceded cursor closure")
					}
					final = true
				}
				return tc.writeErr
			})
			if want := errors.Join(tc.iterErr, tc.closeErr, tc.writeErr); (err == nil) != (want == nil) {
				t.Fatalf("err=%v, want %v", err, want)
			}
			for _, cause := range []error{tc.iterErr, tc.closeErr, tc.writeErr} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("err=%v, missing %v", err, cause)
				}
			}
			if !slices.Equal(chunks, tc.chunks) || final != tc.final || older.closes != 1 || newer.closes != 1 {
				t.Fatalf("chunks=%v final=%v closes=%d/%d", chunks, final, older.closes, newer.closes)
			}
			if after := budget.Stats(); before.TotalBytes != after.TotalBytes || before.ExternalLeases != after.ExternalLeases {
				t.Fatalf("merge workspace did not refund: before=%+v after=%+v", before, after)
			}
		})
	}
}
