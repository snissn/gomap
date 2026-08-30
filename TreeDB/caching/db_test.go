package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const retainedPruneNegativeAssertWait = 150 * time.Millisecond

// MockBackend implements BackendDB
type MockBackend struct {
	mu                           sync.RWMutex
	data                         map[string][]byte
	lastOps                      []batch.Entry
	writeCalls                   int
	writeSyncs                   int
	iteratorCalls                int
	iteratorStartedCh            chan struct{}
	iteratorBlockCh              chan struct{}
	writeErr                     error
	setOpsErr                    error
	setErr                       error
	deleteErr                    error
	registerValueLogErr          error
	markValueLogZombieErr        error
	markValueLogZombieID         uint32
	pointerEntries               map[string]page.ValuePtr
	setOpsInlineValueLimit       int
	lastSpanNativeFallbackReason db.FlushSpanRunFallbackReason
	spanNativeFallbackReasons    []db.FlushSpanRunFallbackReason
	fragReport                   map[string]string
	fragErr                      error
	vacuumErr                    error
	rootPublicationBuildGroups   int
	rootPublicationGroupBatches  int
	rootPublicationGroupFinals   int
}

func NewMockBackend() *MockBackend {
	return &MockBackend{data: make(map[string][]byte)}
}

func (m *MockBackend) SetWriteErr(err error) {
	m.mu.Lock()
	m.writeErr = err
	m.mu.Unlock()
}

func (m *MockBackend) getWriteErr() error {
	m.mu.RLock()
	err := m.writeErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) getSetErr() error {
	m.mu.RLock()
	err := m.setErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) getSetOpsErr() error {
	m.mu.RLock()
	err := m.setOpsErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) getDeleteErr() error {
	m.mu.RLock()
	err := m.deleteErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) RegisterValueLogSegment(path string, fileID uint32) error {
	m.mu.RLock()
	err := m.registerValueLogErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) MarkValueLogZombie(id uint32) error {
	m.mu.RLock()
	err := m.markValueLogZombieErr
	failID := m.markValueLogZombieID
	m.mu.RUnlock()
	if err != nil && (failID == 0 || failID == id) {
		return err
	}
	return nil
}

func (m *MockBackend) FragmentationReport() (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fragErr != nil {
		return nil, m.fragErr
	}
	if m.fragReport == nil {
		return nil, nil
	}
	out := make(map[string]string, len(m.fragReport))
	for k, v := range m.fragReport {
		out[k] = v
	}
	return out, nil
}

func (m *MockBackend) VacuumIndexOnline(ctx context.Context) error {
	m.mu.RLock()
	err := m.vacuumErr
	m.mu.RUnlock()
	return err
}

func setMutable(db *DB, key, value []byte) {
	shard := db.shardForKey(key)
	shard.mu.Lock()
	shard.mem.Set(key, value)
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	shard.mu.Unlock()
	db.mutableBytes.Add(delta)
}

func TestTailValueLogSegmentsByLane(t *testing.T) {
	segments := []logSegmentInfo{
		{path: "value-l0-000001.log", lane: 0, seq: 1, valueLog: true, size: 128},
		{path: "value-l0-000003.log", lane: 0, seq: 3, valueLog: true, size: 128},
		{path: "value-l0-000002.log", lane: 0, seq: 2, valueLog: true, size: 128},
		{path: "value-l1-000004.log", lane: 1, seq: 4, valueLog: true, size: 0},
		{path: "value-l1-000005.log", lane: 1, seq: 5, valueLog: true, size: 128},
		{path: "commit-1-000006.log", lane: 1, seq: 6, valueLog: false, size: 128},
	}

	got := tailValueLogSegmentsByLane(segments)
	if len(got) != 2 {
		t.Fatalf("tailValueLogSegmentsByLane len=%d want 2", len(got))
	}
	if got[0].lane != 0 || got[0].seq != 3 {
		t.Fatalf("lane 0 tail=%+v want seq=3", got[0])
	}
	if got[1].lane != 1 || got[1].seq != 5 {
		t.Fatalf("lane 1 tail=%+v want seq=5", got[1])
	}
}

func TestOpenSeedsRIDFromTailValueLogSegments(t *testing.T) {
	dir := t.TempDir()
	valueDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(valueDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	writeSegment := func(seq int, rid uint64) {
		t.Helper()
		fileID, err := valuelog.EncodeFileID(0, uint32(seq))
		if err != nil {
			t.Fatalf("EncodeFileID(%d): %v", seq, err)
		}
		path := filepath.Join(valueDir, valueLogName(0, seq))
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			t.Fatalf("NewWriter(%d): %v", seq, err)
		}
		if _, err := w.Append(0, nil, rid, []byte("v")); err != nil {
			_ = w.Close()
			t.Fatalf("Append(seq=%d rid=%d): %v", seq, rid, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%d): %v", seq, err)
		}
	}

	// RID allocation is monotonic, so Open only needs the newest non-empty
	// segment in each lane to recover the allocator high-watermark.
	const expectedMaxRID = uint64(100)
	writeSegment(1, 10)
	writeSegment(2, expectedMaxRID)

	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		DisableWAL:  true,
		RelaxedSync: true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if got := db.nextRID.Load(); got != expectedMaxRID {
		t.Fatalf("nextRID=%d want %d", got, expectedMaxRID)
	}
}

func foregroundMaintenanceCancelWait(t *testing.T) time.Duration {
	t.Helper()
	// Give maintenance cancellation several poll intervals to fire, but clamp the
	// wait to a small floor and at most a fraction of the remaining test budget.
	waitFor := 5 * foregroundMaintenancePollInterval()
	if waitFor < 50*time.Millisecond {
		waitFor = 50 * time.Millisecond
	}
	if ddl, ok := t.Deadline(); ok {
		if remaining := time.Until(ddl) / 10; remaining > 0 && remaining < waitFor {
			waitFor = remaining
		}
	}
	return waitFor
}

func TestIteratorTracksActiveForegroundIterators(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	cdb, err := Open(dir, backend, Options{
		AllowUnsafe:           true,
		DisableWAL:            true,
		ForceValueLogPointers: true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = cdb.Close() })
	cdb.testSkipVlogCheckpointKick = true

	b := cdb.NewBatch()
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := cdb.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	it, err := cdb.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	if got := cdb.activeForegroundIterators.Load(); got != 1 {
		t.Fatalf("activeForegroundIterators=%d want=1", got)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("close iterator: %v", err)
	}
	if got := cdb.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("activeForegroundIterators after close=%d want=0", got)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second close iterator: %v", err)
	}
	if got := cdb.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("activeForegroundIterators after second close=%d want=0", got)
	}
}

func TestBeginRawForegroundReadEndIsIdempotent(t *testing.T) {
	db := &DB{}
	end := db.beginRawForegroundRead()
	if got := db.activeForegroundIterators.Load(); got != 1 {
		t.Fatalf("activeForegroundIterators after begin=%d want=1", got)
	}
	if got := db.lastForegroundReadUnixNano.Load(); got == 0 {
		t.Fatal("raw foreground read did not update read activity")
	}
	end()
	end()
	if got := db.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("activeForegroundIterators after repeated end=%d want=0", got)
	}
}

func deleteMutable(db *DB, key []byte) {
	shard := db.shardForKey(key)
	shard.mu.Lock()
	shard.mem.Delete(key)
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	shard.mu.Unlock()
	db.mutableBytes.Add(delta)
}

func TestWrapForegroundIterator_AvoidsDoubleWrapAndCloseIsIdempotent(t *testing.T) {
	db := &DB{closeCh: make(chan struct{})}
	base := &MockIterator{}
	wrapped := db.wrapForegroundIterator(base)
	if got := db.activeForegroundIterators.Load(); got != 1 {
		t.Fatalf("activeForegroundIterators after first wrap=%d want=1", got)
	}
	wrappedAgain := db.wrapForegroundIterator(wrapped)
	if wrappedAgain != wrapped {
		t.Fatalf("double wrap returned different iterator")
	}
	if got := db.activeForegroundIterators.Load(); got != 1 {
		t.Fatalf("activeForegroundIterators after second wrap=%d want=1", got)
	}
	if err := wrappedAgain.Close(); err != nil {
		t.Fatalf("close wrapped iterator: %v", err)
	}
	if got := db.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("activeForegroundIterators after close=%d want=0", got)
	}
	_ = wrappedAgain.Close()
	if got := db.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("activeForegroundIterators after second close=%d want=0", got)
	}
}

func TestForegroundReadQuietFor_DoesNotDependOnActiveIterators(t *testing.T) {
	db := foregroundQuietTestDB()
	now := time.Unix(1_700_000_000, 0)
	quietWindow := 200 * time.Millisecond

	db.activeForegroundIterators.Store(1)
	db.lastForegroundReadUnixNano.Store(now.Add(-2 * quietWindow).UnixNano())
	if !db.foregroundReadQuietFor(now, quietWindow) {
		t.Fatalf("foregroundReadQuietFor=false want true when reads are old")
	}

	db.lastForegroundReadUnixNano.Store(now.Add(-quietWindow / 2).UnixNano())
	if db.foregroundReadQuietFor(now, quietWindow) {
		t.Fatalf("foregroundReadQuietFor=true want false when reads are recent")
	}
}

func foregroundQuietTestDB() *DB {
	return &DB{closeCh: make(chan struct{})}
}

func TestForegroundVlogMaintenanceQuietFor_IgnoresReadTraffic(t *testing.T) {
	db := foregroundQuietTestDB()
	now := time.Unix(1_700_000_000, 0)
	quietWindow := 200 * time.Millisecond

	db.activeForegroundIterators.Store(1)
	db.lastForegroundReadUnixNano.Store(now.UnixNano())
	db.lastForegroundWriteUnixNano.Store(now.Add(-2 * quietWindow).UnixNano())

	if !db.foregroundVlogMaintenanceQuietFor(now, quietWindow) {
		t.Fatalf("foregroundVlogMaintenanceQuietFor=false want true when writes are quiet")
	}

	db.lastForegroundWriteUnixNano.Store(now.Add(-quietWindow / 2).UnixNano())
	if db.foregroundVlogMaintenanceQuietFor(now, quietWindow) {
		t.Fatalf("foregroundVlogMaintenanceQuietFor=true want false when writes are recent")
	}
}

func TestForegroundMaintenanceContext_CancelsOnGetUnsafe(t *testing.T) {
	backend := NewMockBackend()
	backend.data["k"] = []byte("value")
	db := &DB{
		closeCh: make(chan struct{}),
		backend: backend,
	}
	ctx, cancel := db.foregroundMaintenanceContext(250 * time.Millisecond)
	defer cancel()

	if _, err := db.GetUnsafe([]byte("k")); err != nil {
		t.Fatalf("GetUnsafe: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(foregroundMaintenanceCancelWait(t)):
		t.Fatalf("foreground maintenance context did not cancel after GetUnsafe")
	}
}

func TestForegroundMaintenanceContext_CancelsOnActiveIterator(t *testing.T) {
	db := &DB{closeCh: make(chan struct{})}
	ctx, cancel := db.foregroundMaintenanceContext(250 * time.Millisecond)
	defer cancel()

	it := db.wrapForegroundIterator(&MockIterator{})
	defer it.Close()

	select {
	case <-ctx.Done():
	case <-time.After(foregroundMaintenanceCancelWait(t)):
		t.Fatalf("foreground maintenance context did not cancel with active iterator")
	}
}

func TestForegroundVlogMaintenanceResumedSince_FirstWriteAfterZeroBaseline(t *testing.T) {
	db := &DB{}
	if db.foregroundVlogMaintenanceResumedSince(0) {
		t.Fatalf("expected zero baseline with no writes to remain idle")
	}
	now := time.Unix(1_700_000_000, 0).UnixNano()
	db.lastForegroundWriteUnixNano.Store(now)
	if !db.foregroundVlogMaintenanceResumedSince(0) {
		t.Fatalf("expected first foreground write after open to count as resumed")
	}
}

func TestForegroundVlogMaintenanceContext_CancelsOnFirstWriteAfterOpen(t *testing.T) {
	db := &DB{closeCh: make(chan struct{})}
	ctx, cancel := db.foregroundVlogMaintenanceContextWithResumeGrace(250*time.Millisecond, 0)
	defer cancel()

	db.lastForegroundWriteUnixNano.Store(time.Unix(1_700_000_200, 0).UnixNano())

	select {
	case <-ctx.Done():
	case <-time.After(foregroundMaintenanceCancelWait(t)):
		t.Fatalf("value-log maintenance context did not cancel after first foreground write")
	}
}

func countSnapshotLeafEntries(t *testing.T, snap *db.Snapshot) int {
	t.Helper()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	state := snap.State()
	if state == nil {
		t.Fatalf("snapshot state nil")
	}
	p := snap.Pager()
	if p == nil {
		t.Fatalf("snapshot pager nil")
	}

	seen := make(map[uint64]struct{})
	var walk func(pageID uint64) (int, error)
	walk = func(pageID uint64) (int, error) {
		if _, ok := seen[pageID]; ok {
			return 0, nil
		}
		seen[pageID] = struct{}{}

		data, err := p.Get(pageID)
		if err != nil {
			return 0, err
		}
		n := node.NewNodeView(data)
		if !n.VerifyChecksum() {
			return 0, fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		switch n.Type() {
		case page.PageTypeLeaf:
			return int(n.Count()), nil
		case page.PageTypeInternal:
			total := 0
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				childID, err := n.GetInternalChildID(i)
				if err != nil {
					return 0, err
				}
				sub, err := walk(childID)
				if err != nil {
					return 0, err
				}
				total += sub
			}
			return total, nil
		default:
			return 0, fmt.Errorf("invalid page type %d at page %d", n.Type(), pageID)
		}
	}

	total, err := walk(state.RootPageID)
	if err != nil {
		t.Fatalf("count leaf entries: %v", err)
	}
	return total
}

func countCommitLogOpsInDir(t *testing.T, walDir string) (inlineOps int, ridOps int, deleteOps int) {
	t.Helper()
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "commit-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		path := filepath.Join(walDir, name)
		r, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatalf("commitlog.NewReader(%s): %v", name, err)
		}
		for {
			records, err := r.ReadBatch()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				_ = r.Close()
				t.Fatalf("ReadBatch(%s): %v", name, err)
			}
			for i := range records {
				switch records[i].Op {
				case commitlog.OpSetInline:
					inlineOps++
				case commitlog.OpSetRID:
					ridOps++
				case commitlog.OpDelete:
					deleteOps++
				}
			}
		}
		if err := r.Close(); err != nil {
			t.Fatalf("close reader %s: %v", name, err)
		}
	}
	return inlineOps, ridOps, deleteOps
}

func (m *MockBackend) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	// Mimic safe copy for Get
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret, nil
}

func (m *MockBackend) GetUnsafe(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (m *MockBackend) GetAppend(key, dst []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return dst, fmt.Errorf("mock: key not found") // Use error to match contract, though tests might not check type strictly
	}
	return append(dst, val...), nil
}

func (m *MockBackend) Has(key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[string(key)]
	return ok, nil
}

func (m *MockBackend) Set(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	m.data[string(key)] = valCopy
}

func (m *MockBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	m.mu.Lock()
	m.iteratorCalls++
	startedCh := m.iteratorStartedCh
	blockCh := m.iteratorBlockCh
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	m.mu.Unlock()
	if startedCh != nil {
		select {
		case <-startedCh:
		default:
			close(startedCh)
		}
	}
	if blockCh != nil {
		<-blockCh
	}
	sort.Strings(keys)
	it := &MockIterator{backend: m, keys: keys, idx: -1}
	it.Seek(start)
	return it, nil
}

type MockIterator struct {
	backend *MockBackend
	keys    []string
	idx     int
}

func (it *MockIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *MockIterator) Next() {
	it.idx++
}

func (it *MockIterator) Seek(key []byte) {
	it.idx = sort.SearchStrings(it.keys, string(key))
	// If not found, sort.Search returns insertion point.
	// If exact match or greater, that's what we want.
	if it.idx == len(it.keys) {
		// eof
	}
}

func (it *MockIterator) UnsafeKey() []byte {
	return []byte(it.keys[it.idx])
}

func (it *MockIterator) UnsafeValue() []byte {
	it.backend.mu.RLock()
	defer it.backend.mu.RUnlock()
	return it.backend.data[it.keys[it.idx]]
}

func (it *MockIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	it.backend.mu.RLock()
	ptr, ok := it.backend.pointerEntries[it.keys[it.idx]]
	it.backend.mu.RUnlock()
	if ok {
		return nil, ptr, node.FlagPointer
	}
	return it.UnsafeValue(), page.ValuePtr{}, 0
}

func (it *MockIterator) IsDeleted() bool           { return false }
func (it *MockIterator) Error() error              { return nil }
func (it *MockIterator) Close() error              { return nil }
func (it *MockIterator) Domain() ([]byte, []byte)  { return nil, nil }
func (it *MockIterator) Key() []byte               { return it.UnsafeKey() }
func (it *MockIterator) Value() []byte             { return it.UnsafeValue() }
func (it *MockIterator) KeyCopy(dst []byte) []byte { return append(dst[:0], it.UnsafeKey()...) }
func (it *MockIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func (m *MockBackend) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	m.mu.RLock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	m.mu.RUnlock()
	sort.Strings(keys)

	it := &MockReverseIterator{backend: m, keys: keys, idx: len(keys) - 1}
	it.Seek(end) // end is exclusive; start at first >= end, then step back via Next() if needed.
	return it, nil
}

func (m *MockBackend) Print() error             { return nil }
func (m *MockBackend) Stats() map[string]string { return nil }

func (m *MockBackend) BeginRootPublicationBuildGroup() (*db.RootPublicationBuildGroup, error) {
	m.mu.Lock()
	m.rootPublicationBuildGroups++
	m.mu.Unlock()
	return &db.RootPublicationBuildGroup{}, nil
}

// NewBatch returns a struct that satisfies BatchInterface
func (m *MockBackend) NewBatch() batch.Interface {
	return &MockBatch{mb: m}
}

type MockBatch struct {
	mb *MockBackend
}

func (b *MockBatch) Set(key, value []byte) error {
	if err := b.mb.getSetErr(); err != nil {
		return err
	}
	b.mb.Set(key, value)
	return nil
}
func (b *MockBatch) Delete(key []byte) error {
	if err := b.mb.getDeleteErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	delete(b.mb.data, string(key))
	b.mb.mu.Unlock()
	return nil
}
func (b *MockBatch) DeleteRange(start, end []byte) error {
	b.mb.mu.Lock()
	defer b.mb.mu.Unlock()
	for k := range b.mb.data {
		kb := []byte(k)
		if batch.DeleteRangeContainsKey(batch.DeleteRange{Start: start, End: end}, kb) {
			delete(b.mb.data, k)
		}
	}
	return nil
}
func (b *MockBatch) SetOps(ops []batch.Entry) error {
	if err := b.mb.getSetOpsErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	defer b.mb.mu.Unlock()
	b.mb.lastOps = append(b.mb.lastOps[:0], ops...)
	for _, op := range ops {
		if b.mb.setOpsInlineValueLimit > 0 && op.Type == batch.OpPut && !op.IsPtr && len(op.Value) > b.mb.setOpsInlineValueLimit {
			return batch.ErrValueTooLarge
		}
		if op.Type == batch.OpDeleteRange {
			for k := range b.mb.data {
				if batch.DeleteRangeContainsKey(batch.DeleteRange{Start: op.Key, End: op.Value}, []byte(k)) {
					delete(b.mb.data, k)
				}
			}
		} else if op.Type == batch.OpDelete {
			delete(b.mb.data, string(op.Key))
		} else {
			valCopy := make([]byte, len(op.Value))
			copy(valCopy, op.Value)
			b.mb.data[string(op.Key)] = valCopy
		}
	}
	return nil
}

func (b *MockBatch) Replay(fn func(batch.Entry) error) error {
	return nil
}

func (b *MockBatch) Write() error {
	if err := b.mb.getWriteErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	b.mb.writeCalls++
	b.mb.mu.Unlock()
	return nil
}

func (b *MockBatch) WriteSync() error {
	if err := b.mb.getWriteErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	b.mb.writeCalls++
	b.mb.writeSyncs++
	b.mb.mu.Unlock()
	return nil
}

func (b *MockBatch) SetFlushApplySpanNativeFallback(reason db.FlushSpanRunFallbackReason) {
	b.mb.mu.Lock()
	b.mb.lastSpanNativeFallbackReason = reason
	b.mb.spanNativeFallbackReasons = append(b.mb.spanNativeFallbackReasons, reason)
	b.mb.mu.Unlock()
}

func (b *MockBatch) SetCommandWALPublish(uint64, []db.CommandWALLSNRange) error { return nil }

func (b *MockBatch) SetRootPublicationBuildGroup(_ *db.RootPublicationBuildGroup, final bool) error {
	b.mb.mu.Lock()
	b.mb.rootPublicationGroupBatches++
	if final {
		b.mb.rootPublicationGroupFinals++
	}
	b.mb.mu.Unlock()
	return nil
}

func (b *MockBatch) Close() error              { return nil }
func (b *MockBatch) GetByteSize() (int, error) { return 0, nil }

func (m *MockBackend) Close() error { return nil }

func TestCachingDB_WriteAndFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Threshold 1 byte to trigger flush
	db, err := Open(dir, backend, Options{FlushThreshold: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write 10 keys (should fit in memtable or trigger flush)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := db.SetSync(k, v); err != nil {
			t.Fatalf("SetSync: %v", err)
		}
	}

	// Verify visibility (Get)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		val, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(val) != fmt.Sprintf("v%d", i) {
			t.Errorf("Get %s: got %q", k, val)
		}
	}

	// Close to flush everything
	db.Close()

	// Verify backend received data
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		if string(backend.data[k]) != fmt.Sprintf("v%d", i) {
			t.Errorf("Backend missing %s", k)
		}
	}
}

func TestCachingDB_FlushSyncsWhenWALDisabled(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Drain(); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	backend.mu.RLock()
	writeSyncs := backend.writeSyncs
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()

	if writeCalls == 0 {
		t.Fatalf("expected backend writes")
	}
	if writeSyncs != 0 {
		t.Fatalf("expected WAL-off flush to use non-sync backend writes; got %d syncs", writeSyncs)
	}
}

func TestCachingDB_ExternalCommandWALIsDistinctFromUnsafeDisableWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{ExternalCommandWAL: true, FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open external command WAL without AllowUnsafe: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.redo_log.mode"]; got != "external_command_wal" {
		t.Fatalf("redo_log.mode=%q, want external_command_wal", got)
	}
	if got := stats["treedb.cache.redo_log.enabled"]; got != "false" {
		t.Fatalf("redo_log.enabled=%q, want false", got)
	}
	if got := stats["treedb.cache.command_wal.external_durability"]; got != "true" {
		t.Fatalf("command_wal.external_durability=%q, want true", got)
	}
}

func TestCachingDB_ExternalCommandWALRejectsUnsafeDisableWALMix(t *testing.T) {
	_, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:         true,
		ExternalCommandWAL: true,
		AllowUnsafe:        true,
	})
	if err == nil {
		t.Fatal("Open with DisableWAL and ExternalCommandWAL unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("Open error=%v, want mutually exclusive", err)
	}
}

func TestCachingDB_FlushAllCombinesMemtables(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		FlushBuildConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k"), []byte("v2"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k2"), []byte("v3"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls != 3 {
		t.Fatalf("expected 3 backend batch commits (sequential flush), got %d", writeCalls)
	}

	got, err := db.backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("backend.Get(k): got %q want %q", got, "v2")
	}

	got, err = db.backend.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("backend.Get(k2): got %q want %q", got, "v3")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestBatchWriteSync_WALOffSmallNonOverlappingBypassesQueuedFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldKey := []byte("a/queued")
	oldVal := []byte("queued")
	db.mu.Lock()
	setMutable(db, oldKey, oldVal)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	queueLenBefore := len(db.queue)
	if queueLenBefore == 0 {
		t.Fatalf("queue len before=%d want > 0", queueLenBefore)
	}

	b := db.NewBatch()
	if b == nil {
		t.Fatalf("NewBatch returned nil")
	}
	newKey := []byte("z/new")
	newVal := []byte("fresh")
	if err := b.Set(newKey, newVal); err != nil {
		_ = b.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	_, oldPersisted := backend.data[string(oldKey)]
	newPersisted := backend.data[string(newKey)]
	backend.mu.RUnlock()

	if writeCalls != 1 {
		t.Fatalf("backend writeCalls=%d want 1", writeCalls)
	}
	if oldPersisted {
		t.Fatalf("queued key should not have been flushed by unrelated bypassed sync batch")
	}
	if string(newPersisted) != string(newVal) {
		t.Fatalf("backend new value=%q want %q", newPersisted, newVal)
	}
	if got := len(db.queue); got != queueLenBefore {
		t.Fatalf("queue len after=%d want %d", got, queueLenBefore)
	}

	gotOld, err := db.Get(oldKey)
	if err != nil {
		t.Fatalf("Get old: %v", err)
	}
	if string(gotOld) != string(oldVal) {
		t.Fatalf("Get old=%q want %q", gotOld, oldVal)
	}
	gotNew, err := db.Get(newKey)
	if err != nil {
		t.Fatalf("Get new: %v", err)
	}
	if string(gotNew) != string(newVal) {
		t.Fatalf("Get new=%q want %q", gotNew, newVal)
	}
}

func TestBatchWrite_WALOffSmallNonOverlappingDoesNotBypassQueuedFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldKey := []byte("a/queued")
	oldVal := []byte("queued")
	db.mu.Lock()
	setMutable(db, oldKey, oldVal)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	queueLenBefore := len(db.queue)
	if queueLenBefore == 0 {
		t.Fatalf("queue len before=%d want > 0", queueLenBefore)
	}

	b := db.NewBatch()
	if b == nil {
		t.Fatalf("NewBatch returned nil")
	}
	newKey := []byte("z/new")
	newVal := []byte("fresh")
	if err := b.Set(newKey, newVal); err != nil {
		_ = b.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	_, oldPersisted := backend.data[string(oldKey)]
	newPersisted := backend.data[string(newKey)]
	backend.mu.RUnlock()

	if writeCalls != 0 {
		t.Fatalf("backend writeCalls=%d want 0", writeCalls)
	}
	if oldPersisted {
		t.Fatalf("queued key should not have been flushed by unrelated small batch")
	}
	if len(newPersisted) != 0 {
		t.Fatalf("new key should not have been persisted eagerly, got %q", newPersisted)
	}
	if got := len(db.queue); got != queueLenBefore {
		t.Fatalf("queue len after=%d want %d", got, queueLenBefore)
	}

	gotOld, err := db.Get(oldKey)
	if err != nil {
		t.Fatalf("Get old: %v", err)
	}
	if string(gotOld) != string(oldVal) {
		t.Fatalf("Get old=%q want %q", gotOld, oldVal)
	}
	gotNew, err := db.Get(newKey)
	if err != nil {
		t.Fatalf("Get new: %v", err)
	}
	if string(gotNew) != string(newVal) {
		t.Fatalf("Get new=%q want %q", gotNew, newVal)
	}
}

func TestPrepareBypassValueLogOps_RewritesLargeValuesToPointers(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ops := []batch.Entry{{
		Type:  batch.OpPut,
		Key:   []byte("large/key"),
		Value: bytes.Repeat([]byte("v"), 8<<10),
	}}
	rewritten, err := db.prepareBypassValueLogOps(ops, true)
	if err != nil {
		t.Fatalf("prepareBypassValueLogOps: %v", err)
	}
	if len(rewritten) != 1 {
		t.Fatalf("rewritten len=%d want 1", len(rewritten))
	}
	if !rewritten[0].IsPtr {
		t.Fatalf("expected large value to be rewritten as pointer op")
	}
	if rewritten[0].Value != nil {
		t.Fatalf("expected pointer op value cleared, len=%d", len(rewritten[0].Value))
	}
	if rewritten[0].ValuePtr.FileID == 0 {
		t.Fatalf("expected non-zero value-log file id")
	}
}

func TestBatchWriteSync_WALOffLargeNonOverlappingBypassesAsPointers(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.setOpsInlineValueLimit = 1024

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldKey := []byte("a/queued")
	oldVal := []byte("queued")
	db.mu.Lock()
	setMutable(db, oldKey, oldVal)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	queueLenBefore := len(db.queue)
	if queueLenBefore == 0 {
		t.Fatalf("queue len before=%d want > 0", queueLenBefore)
	}

	b := db.NewBatch()
	if b == nil {
		t.Fatalf("NewBatch returned nil")
	}
	newKey := []byte("z/large")
	newVal := bytes.Repeat([]byte("x"), 8<<10)
	if err := b.Set(newKey, newVal); err != nil {
		_ = b.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	_, oldPersisted := backend.data[string(oldKey)]
	lastOps := append([]batch.Entry(nil), backend.lastOps...)
	backend.mu.RUnlock()

	if writeCalls != 1 {
		t.Fatalf("backend writeCalls=%d want 1", writeCalls)
	}
	if oldPersisted {
		t.Fatalf("queued key should not have been flushed by large bypassed sync batch")
	}
	if len(lastOps) != 1 {
		t.Fatalf("lastOps len=%d want 1", len(lastOps))
	}
	if !lastOps[0].IsPtr {
		t.Fatalf("expected bypassed large op to be committed as pointer")
	}
	if lastOps[0].Value != nil {
		t.Fatalf("expected committed pointer op value cleared, len=%d", len(lastOps[0].Value))
	}
	if lastOps[0].ValuePtr.FileID == 0 {
		t.Fatalf("expected committed pointer op to reference value log")
	}
	if got := len(db.queue); got != queueLenBefore {
		t.Fatalf("queue len after=%d want %d", got, queueLenBefore)
	}

	gotOld, err := db.Get(oldKey)
	if err != nil {
		t.Fatalf("Get old: %v", err)
	}
	if string(gotOld) != string(oldVal) {
		t.Fatalf("Get old=%q want %q", gotOld, oldVal)
	}
}

func TestBatchWriteBypass_FallbackReusesPreparedPointerOps(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.setOpsInlineValueLimit = 128

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	largeKey := []byte("large/key")
	largeVal := bytes.Repeat([]byte("v"), 8<<10)
	inlineKey := []byte("inline/key")
	inlineVal := bytes.Repeat([]byte("i"), 256)

	b := db.NewBatch()
	if err := b.Set(largeKey, largeVal); err != nil {
		t.Fatalf("Set large: %v", err)
	}
	if err := b.Set(inlineKey, inlineVal); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	if err := b.writeBypass(false); err != nil {
		t.Fatalf("writeBypass: %v", err)
	}
	if got := db.nextRID.Load(); got != 1 {
		t.Fatalf("nextRID=%d want 1", got)
	}
	gotLarge, err := db.Get(largeKey)
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	if !bytes.Equal(gotLarge, largeVal) {
		t.Fatalf("Get large len=%d want %d", len(gotLarge), len(largeVal))
	}
	gotInline, err := db.Get(inlineKey)
	if err != nil {
		t.Fatalf("Get inline: %v", err)
	}
	if !bytes.Equal(gotInline, inlineVal) {
		t.Fatalf("Get inline len=%d want %d", len(gotInline), len(inlineVal))
	}
}

func TestRotateValueLogMuHeld_RestoresUsableWriterAfterRegisterFailure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.registerValueLogErr = errors.New("test: register failed")

	oldSeq := 59
	oldPath := filepath.Join(dir, valueLogName(0, oldSeq))
	oldFileID, err := valuelog.EncodeFileID(0, uint32(oldSeq))
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	writer, err := valuelog.NewWriter(oldPath, oldFileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	l := &lane{
		id:       0,
		vlog:     writer,
		vlogSeq:  oldSeq,
		vlogPath: oldPath,
	}
	db := &DB{dir: dir, valueLogDir: dir, backend: backend}
	t.Cleanup(func() {
		if l.vlog != nil {
			_ = l.vlog.Close()
			l.vlog = nil
		}
		_ = db.removeFileRetry(oldPath)
		_ = db.removeFileRetry(filepath.Join(dir, valueLogName(0, oldSeq+1)))
	})

	err = db.rotateValueLogLocked(l)
	if err == nil || !strings.Contains(err.Error(), "register failed") {
		t.Fatalf("rotateValueLogLocked err=%v want register failure", err)
	}
	if got := l.vlogSeq; got != oldSeq {
		t.Fatalf("vlogSeq=%d want %d", got, oldSeq)
	}
	if got := l.vlogPath; got != oldPath {
		t.Fatalf("vlogPath=%q want %q", got, oldPath)
	}
	if l.vlog == nil {
		t.Fatalf("expected usable writer restored after register failure")
	}
	ptrs, err := db.appendValueLog(l, 0, nil, []valuelog.Record{{RID: 1, Value: []byte("value")}}, journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLog after rollback: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("ptrs len=%d want 1", len(ptrs))
	}
	if got := ptrs[0].FileID; got != page.ValueLogFileID(oldFileID) {
		t.Fatalf("ptr fileID=%d want %d", got, oldFileID)
	}
	putValueLogPtrs(ptrs)
}

func TestRegisterValueLogSegment_RollsBackReaderOnBackendFailure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.registerValueLogErr = errors.New("test: register failed")

	reader, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = reader.Close() }()

	db := &DB{
		dir:            dir,
		backend:        backend,
		valueLogReader: reader,
	}

	path := filepath.Join(dir, valueLogName(0, 1))
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err = db.registerValueLogSegment(path, fileID)
	if err == nil || !strings.Contains(err.Error(), "register failed") {
		t.Fatalf("registerValueLogSegment err=%v want register failure", err)
	}

	set := reader.CurrentSetNoRefresh()
	if _, ok := set.Files[fileID]; ok {
		t.Fatalf("segment %d remained registered after backend failure", fileID)
	}
}

func TestCheckpoint_IgnoresUnsupportedSparseVacuum(t *testing.T) {
	backend := NewMockBackend()
	backend.fragReport = map[string]string{
		"treedb.user.pages":                 strconv.FormatUint(checkpointSparseIndexMinPages, 10),
		"treedb.user.internal_fill_ppm_p50": strconv.FormatUint(checkpointSparseIndexMaxInternalFillP50PPM-1, 10),
		"treedb.user.internal_fill_ppm_avg": strconv.FormatUint(checkpointSparseIndexMaxInternalFillAvgPPM-1, 10),
	}
	backend.vacuumErr = db.ErrVacuumUnsupported

	db := &DB{
		backend:        backend,
		disableJournal: true,
	}
	db.checkpointRuns.Store(checkpointSparseIndexCheckEveryNoops)

	if err := db.maybeVacuumSparseIndexOnCheckpoint(); err != nil {
		t.Fatalf("maybeVacuumSparseIndexOnCheckpoint: %v", err)
	}
	if got := db.checkpointAutoVacuumRuns.Load(); got != 0 {
		t.Fatalf("checkpointAutoVacuumRuns=%d want 0", got)
	}
}

func TestCheckpoint_WALOffNoPendingStateSkipsRotationAndSyncsBackend(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if b == nil {
		t.Fatalf("NewBatch returned nil")
	}
	key := []byte("z/large")
	val := []byte("inline-visible")
	if err := b.Set(key, val); err != nil {
		_ = b.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := db.mutableBytes.Load(); got != 0 {
		t.Fatalf("mutableBytes=%d want 0", got)
	}
	if got := len(db.queue); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
	if db.hasDirtyValueLogLanes() {
		t.Fatalf("expected no dirty value-log lanes after sync bypass write")
	}

	seqBefore := make([]int, len(db.lanes))
	pathsBefore := make([]string, len(db.lanes))
	for i := range db.lanes {
		seqBefore[i] = db.currentValueLogSeq(&db.lanes[i])
		pathsBefore[i] = db.currentValueLogPath(&db.lanes[i])
	}
	backend.mu.RLock()
	writeCallsBefore := backend.writeCalls
	writeSyncsBefore := backend.writeSyncs
	backend.mu.RUnlock()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if got := db.checkpointNoopSkips.Load(); got == 0 {
		t.Fatalf("checkpointNoopSkips=%d want > 0", got)
	}
	for i := range db.lanes {
		if got := db.currentValueLogSeq(&db.lanes[i]); got != seqBefore[i] {
			t.Fatalf("lane %d seq=%d want %d", i, got, seqBefore[i])
		}
		if got := db.currentValueLogPath(&db.lanes[i]); got != pathsBefore[i] {
			t.Fatalf("lane %d path=%q want %q", i, got, pathsBefore[i])
		}
	}
	backend.mu.RLock()
	writeCallsAfter := backend.writeCalls
	writeSyncsAfter := backend.writeSyncs
	backend.mu.RUnlock()
	if writeCallsAfter != writeCallsBefore+1 {
		t.Fatalf("backend writeCalls after checkpoint=%d want %d", writeCallsAfter, writeCallsBefore+1)
	}
	if writeSyncsAfter != writeSyncsBefore+1 {
		t.Fatalf("backend writeSyncs after checkpoint=%d want %d", writeSyncsAfter, writeSyncsBefore+1)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("Get=%q want %q", got, val)
	}
}

func TestCachingDB_FlushAllCombinesMemtablesParallel(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		FlushBuildConcurrency: 4,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
		MemtableShards:        1,
		JournalLanes:          1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k"), []byte("v2"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k2"), []byte("v3"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls != 1 {
		t.Fatalf("expected 1 backend batch commit (combined flush), got %d", writeCalls)
	}

	got, err := db.backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("backend.Get(k): got %q want %q", got, "v2")
	}

	got, err = db.backend.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("backend.Get(k2): got %q want %q", got, "v3")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_DeleteRange_DisableWAL_CoversInMemoryDeletesBackend(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("a"), []byte("va"))
	backend.Set([]byte("b"), []byte("vb"))

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("d"), []byte("vd")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.DeleteRange(nil, nil); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if val != nil {
			t.Fatalf("expected %q to be deleted, got %q", key, val)
		}
	}

	if got := len(backend.data); got != 0 {
		t.Fatalf("expected backend to be empty, got %d keys", got)
	}
	if backend.writeCalls == 0 {
		t.Fatalf("expected backend batch write to be used")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_DeleteRange_DisableWAL_PartialRangeUsesTombstones(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("z"), []byte("vz")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.DeleteRange([]byte("a"), []byte("m")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	val, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != nil {
		t.Fatalf("expected %q to be deleted, got %q", "a", val)
	}

	val, err = db.Get([]byte("z"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "vz" {
		t.Fatalf("expected %q, got %q", "vz", val)
	}

	if backend.writeCalls != 0 {
		t.Fatalf("expected no backend writes, got %d", backend.writeCalls)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_FlushAllParallelBuildPreservesNewestWins(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 60, FlushBuildConcurrency: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const keys = 1000

	db.mu.Lock()
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		setMutable(db, k, []byte("v1"))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		setMutable(db, k, []byte("v2"))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if i%2 == 0 {
			deleteMutable(db, k)
		} else {
			setMutable(db, k, []byte("v3"))
		}
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	// Even keys should be deleted, odd keys should be v3 (newest memtable).
	got0, err := db.backend.Get([]byte("k0000"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if got0 != nil {
		t.Fatalf("expected k0000 deleted, got %q", got0)
	}
	got1, err := db.backend.Get([]byte("k0001"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got1) != "v3" {
		t.Fatalf("expected k0001=v3, got %q", got1)
	}
	got999, err := db.backend.Get([]byte("k0999"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got999) != "v3" {
		t.Fatalf("expected k0999=v3, got %q", got999)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type MockReverseIterator struct {
	backend *MockBackend
	keys    []string
	idx     int
}

func (it *MockReverseIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *MockReverseIterator) Next() {
	it.idx--
}

func (it *MockReverseIterator) Seek(key []byte) {
	if len(it.keys) == 0 {
		it.idx = -1
		return
	}
	if key == nil {
		it.idx = len(it.keys) - 1
		return
	}

	// Find first key >= target.
	pos := sort.SearchStrings(it.keys, string(key))
	if pos >= len(it.keys) {
		it.idx = len(it.keys) - 1
		return
	}
	it.idx = pos
}

func (it *MockReverseIterator) UnsafeKey() []byte {
	return []byte(it.keys[it.idx])
}

func (it *MockReverseIterator) UnsafeValue() []byte {
	it.backend.mu.RLock()
	defer it.backend.mu.RUnlock()
	return it.backend.data[it.keys[it.idx]]
}

func (it *MockReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.UnsafeValue(), page.ValuePtr{}, 0
}

func (it *MockReverseIterator) IsDeleted() bool           { return false }
func (it *MockReverseIterator) Error() error              { return nil }
func (it *MockReverseIterator) Close() error              { return nil }
func (it *MockReverseIterator) Domain() ([]byte, []byte)  { return nil, nil }
func (it *MockReverseIterator) Key() []byte               { return it.UnsafeKey() }
func (it *MockReverseIterator) Value() []byte             { return it.UnsafeValue() }
func (it *MockReverseIterator) KeyCopy(dst []byte) []byte { return append(dst[:0], it.UnsafeKey()...) }
func (it *MockReverseIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func TestCachingDB_IteratorIncludesBackendAfterStreamingBatch(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Large threshold so nothing flushes from memtable; we want the batch fast-path.
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	for i := 0; i < 64; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v := []byte("v")
		if err := b.Set(k, v); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	got := 0
	for it.Valid() {
		_ = it.Key()
		_ = it.Value()
		it.Next()
		got++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("Iterator.Error: %v", err)
	}
	if got != 64 {
		t.Fatalf("expected %d keys, got %d", 64, got)
	}
}

func TestCachingDB_NotifyErrorOnFlushFailure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.SetWriteErr(errors.New("write failed"))

	errCh := make(chan error, 1)
	db, err := Open(dir, backend, Options{
		FlushThreshold: 1,
		NotifyError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.flushAll(false)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected non-nil error callback")
		}
	default:
		t.Fatalf("expected NotifyError to be called")
	}

	backend.SetWriteErr(nil)
	if err := db.Close(); err == nil {
		t.Fatalf("expected Close to return background error")
	}
}

func TestCachingDB_IteratorDoesNotBlockOnWriteMu(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	done := make(chan error, 1)
	go func() {
		it, err := db.Iterator(nil, nil)
		if err != nil {
			done <- err
			return
		}
		done <- it.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Iterator: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("iterator creation blocked behind writeMu")
	}
}

func TestCachingDB_SetDoesNotBlockOnWriteMuRLock(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.writeMu.RLock()
	defer db.writeMu.RUnlock()

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("k2"), []byte("v2"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Set blocked behind writeMu RLock")
	}
}

func TestCachingDB_FlushPersistsValueLogPointer(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		MaxValueLogRetainedBytes: 1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)
	if err := cache.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	cache.flushAll(true)

	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	entry, err := snap.GetEntry(key)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		_ = snap.Close()
		t.Fatalf("expected backend to persist value-log pointer, got flags=%#x file_id=%#x", entry.Flags, entry.ValuePtr.FileID)
	}
	_ = snap.Close()

	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("backend Get mismatch")
	}
}

func TestCachingDB_ChunkedCheckpointPersistsGroupedValueLogPointersAfterReopen(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold:             1 << 60,
		ValueLogPointerThreshold:   1,
		FlushBackendMaxEntries:     2,
		FlushBackendMaxBatches:     -1,
		FlushSpanRunTargetPlanning: true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = cache.Close()
		}
	}()

	want := make(map[string][]byte, 5)
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("grouped-pointer-%02d", i))
		value := bytes.Repeat([]byte{byte('a' + i)}, page.DefaultInlineThreshold+64+i)
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
		want[string(key)] = value
	}
	beforeCommitSeq, err := strconv.ParseUint(backend.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse pre-checkpoint backend commit_seq: %v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	afterCommitSeq, err := strconv.ParseUint(backend.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse post-checkpoint backend commit_seq: %v", err)
	}
	if afterCommitSeq != beforeCommitSeq+1 {
		t.Fatalf("backend commit_seq advanced %d -> %d, want one complete grouped root", beforeCommitSeq, afterCommitSeq)
	}

	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("snapshot nil")
	}
	for key := range want {
		entry, err := snapshot.GetEntry([]byte(key))
		if err != nil {
			_ = snapshot.Close()
			t.Fatalf("GetEntry(%q): %v", key, err)
		}
		if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
			_ = snapshot.Close()
			t.Fatalf("GetEntry(%q) flags=%#x file_id=%#x, want value-log pointer", key, entry.Flags, entry.ValuePtr.FileID)
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("snapshot Close: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("cache Close: %v", err)
	}
	closed = true
	reopened, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer reopened.Close()
	for key, value := range want {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopened Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("reopened Get(%q) mismatch", key)
		}
	}
}

func TestCachingDB_CloseDeferredValueLogDoesNotFail(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		FlushThreshold:           1 << 60,
		ValueLogPointerThreshold: 1,
		DisableWAL:               true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}

	key := []byte("k1")
	valSize := 1 << 20
	val := bytes.Repeat([]byte("v"), valSize)
	for i := 0; i < 4; i++ {
		if err := cache.Set(key, val); err != nil {
			_ = cache.Close()
			t.Fatalf("Set: %v", err)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_ValueLogHardCapDisablesPointers(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                  true,
		FlushThreshold:               1 << 30,
		ValueLogPointerThreshold:     1,
		MaxValueLogRetainedBytesHard: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	val := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)
	if err := cache.Set([]byte("k1"), val); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	// Ensure buffered value-log writes are visible in retention stats.
	if err := cache.flushValueLog(); err != nil {
		t.Fatalf("flushValueLog: %v", err)
	}
	_, bytes1 := cache.valueLogRetainedStats()
	if bytes1 <= 0 {
		t.Fatalf("expected retained value-log bytes after first large value, got %d", bytes1)
	}

	if err := cache.Set([]byte("k2"), val); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	if err := cache.flushValueLog(); err != nil {
		t.Fatalf("flushValueLog: %v", err)
	}
	_, bytes2 := cache.valueLogRetainedStats()

	// Hard cap should disable *new* value-log pointers once retained bytes exceed
	// the cap; retained bytes should stop growing after the cap trips.
	if bytes2 != bytes1 {
		t.Fatalf("expected retained value-log bytes to stop growing after hard cap (before=%d after=%d)", bytes1, bytes2)
	}
}

func TestCachingDB_SetRejectsWhenShardNearHardCap(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	shard := &cache.mutableShards[0]
	shard.mu.Lock()
	shard.bytes = maxMemtableBytesPerShard - 8
	shard.mu.Unlock()

	err = cache.Set([]byte("k1"), bytes.Repeat([]byte("a"), 16))
	if !errors.Is(err, ErrMemtableFull) {
		t.Fatalf("Set(k1): expected ErrMemtableFull, got %v", err)
	}
}

func TestCachingDB_PrunesRetainedValueLog(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 1,
		MaxValueLogRetainedBytes: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	key := []byte("k1")
	large := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)

	// Flush without a durability boundary so WAL/value-log segments remain and
	// show up as retained.
	if err := cache.Set(key, large); err != nil {
		t.Fatalf("Set(large): %v", err)
	}
	cache.flushAll(false)
	if err := cache.rotateValueLogLocked(&cache.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	stats := cache.Stats()
	segments, err := strconv.Atoi(stats["treedb.cache.vlog_retained_segments"])
	if err != nil {
		t.Fatalf("parse retained segments: %v", err)
	}
	if segments == 0 {
		t.Fatalf("expected retained value-log segments after non-sync flush")
	}
	retainedBefore := cache.valueLogRetainedClosedBytes.Load()
	if retainedBefore <= 0 {
		t.Fatalf("expected retained closed bytes after rotate, got %d", retainedBefore)
	}

	// Delete the key and checkpoint. The prior durable slot can still select the
	// value-bearing root, so let the first prune retain that segment before
	// recommitting the current root to advance the recovery horizon. The active
	// segment stays open for writing, but it is not counted as retained closed.
	if err := cache.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	forceRetainedPruneIdle(cache)
	cache.waitForRetainedValueLogPrune()
	if err := backend.ForceCommit(backend.State().RootPageID); err != nil {
		t.Fatalf("commit durable-slot successor: %v", err)
	}
	cache.PruneRetainedValueLogsForMaintenance()
	stats = cache.Stats()
	segments, err = strconv.Atoi(stats["treedb.cache.vlog_retained_segments"])
	if err != nil {
		t.Fatalf("parse retained segments: %v", err)
	}
	if segments != 0 {
		t.Fatalf("expected no retained closed segments after checkpoint, got %d (before=%dB)", segments, retainedBefore)
	}
}

func TestOpen_InitializesRetainedClosedBytesFromExistingSegments(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	}

	backend1, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend1 open: %v", err)
	}
	cache1, err := Open(dir, backend1, opts)
	if err != nil {
		_ = backend1.Close()
		t.Fatalf("cache1 open: %v", err)
	}

	if err := cache1.Set([]byte("k"), bytes.Repeat([]byte("x"), page.DefaultInlineThreshold+256)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	cache1.flushAll(false)
	if err := cache1.rotateValueLogLocked(&cache1.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	if got := cache1.valueLogRetainedClosedBytes.Load(); got <= 0 {
		t.Fatalf("pre-close retained closed bytes=%d want >0", got)
	}
	if err := cache1.Close(); err != nil {
		t.Fatalf("cache1 close: %v", err)
	}

	backend2, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend2 open: %v", err)
	}
	cache2, err := Open(dir, backend2, opts)
	if err != nil {
		_ = backend2.Close()
		t.Fatalf("cache2 open: %v", err)
	}
	defer cache2.Close()

	if got := cache2.valueLogRetainedClosedBytes.Load(); got <= 0 {
		t.Fatalf("reopen retained closed bytes=%d want >0", got)
	}
}

func TestPruneRetainedValueLogs_SkipsLiveScanWhenAllRetainedPathsInUse(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		MaxValueLogRetainedBytes: 1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	if err := cache.Set([]byte("seed"), bytes.Repeat([]byte("v"), 64)); err != nil {
		t.Fatalf("seed value-log writer: %v", err)
	}
	retained := cache.currentValueLogPath(&cache.lanes[0])
	if retained == "" {
		t.Fatalf("expected current value-log path")
	}
	cache.markValueLogRetain(retained)

	pruneStats := cache.pruneRetainedValueLogs(false)

	backend.mu.RLock()
	iteratorCalls := backend.iteratorCalls
	backend.mu.RUnlock()
	if iteratorCalls != 0 {
		t.Fatalf("iteratorCalls=%d want 0", iteratorCalls)
	}
	if !cache.valueLogRetained(retained) {
		t.Fatalf("expected in-use retained path to remain retained")
	}
	if pruneStats.InUseSkippedSegments != 1 {
		t.Fatalf("InUseSkippedSegments=%d want 1", pruneStats.InUseSkippedSegments)
	}
	if pruneStats.CandidateSegments != 0 {
		t.Fatalf("CandidateSegments=%d want 0", pruneStats.CandidateSegments)
	}
}

func seedRetainedPrunePressure(cache *DB, retainedPath string, size int64) {
	l := &cache.lanes[0]
	l.vlogMu.Lock()
	if l.vlogClosedSizes == nil {
		l.vlogClosedSizes = make(map[string]int64)
	}
	prev := l.vlogClosedSizes[retainedPath]
	l.vlogClosedSizes[retainedPath] = size
	l.vlogClosedBytes.Add(size - prev)
	l.vlogMu.Unlock()
	cache.valueLogRetainedClosedBytes.Add(size - prev)
}

func seedRetainedPruneSegment(t *testing.T, cache *DB, seq uint32, retainedBytes int64) (string, uint32) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(0, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	valueDir := cache.valueLogDir
	if valueDir == "" {
		valueDir = filepath.Join(cache.dir, "value_vlog")
	}
	retainedPath := filepath.Join(valueDir, valueLogName(0, int(seq)))
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("r"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, retainedBytes)
	return retainedPath, fileID
}

func TestRetainedValueLogPruneDiagnostics_NoCandidates(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	pruneStats, ran := cache.runRetainedValueLogPruneInline(false, nil)
	if !ran {
		t.Fatalf("inline retained prune did not run")
	}
	if pruneStats.Mode != retainedPruneModeNoCandidates {
		t.Fatalf("Mode=%s want no_candidates", retainedPruneModeString(uint32(pruneStats.Mode)))
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "no_candidates" {
		t.Fatalf("last_status=%q want no_candidates", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "no_candidates" {
		t.Fatalf("last_mode=%q want no_candidates", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.no_candidate_runs"]; got != "1" {
		t.Fatalf("no_candidate_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.completed_runs"]; got != "1" {
		t.Fatalf("completed_runs=%q want 1", got)
	}
}

func TestRetainedValueLogPruneDiagnostics_FullLiveScan(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("a"), []byte("1"))
	backend.Set([]byte("b"), []byte("2"))

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	seedRetainedPruneSegment(t, cache, 401, 2<<30)
	pruneStats, ran := cache.runRetainedValueLogPruneInline(false, nil)
	if !ran {
		t.Fatalf("inline retained prune did not run")
	}
	if pruneStats.Mode != retainedPruneModeFullLiveIDScan {
		t.Fatalf("Mode=%s want full_live_id_scan", retainedPruneModeString(uint32(pruneStats.Mode)))
	}
	if pruneStats.ScanStats.Records != 2 {
		t.Fatalf("scan records=%d want 2", pruneStats.ScanStats.Records)
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "completed" {
		t.Fatalf("last_status=%q want completed", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "full_live_id_scan" {
		t.Fatalf("last_mode=%q want full_live_id_scan", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.full_live_id_scan_runs"]; got != "1" {
		t.Fatalf("full_live_id_scan_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.observed_source_fast_path_runs"]; got != "0" {
		t.Fatalf("observed_source_fast_path_runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_scan.records"]; got != "2" {
		t.Fatalf("last_scan.records=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.scan.records_total"]; got != "2" {
		t.Fatalf("scan.records_total=%q want 2", got)
	}
}

func TestRetainedValueLogPruneDiagnostics_ObservedSourceFastPath(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	_, fileID := seedRetainedPruneSegment(t, cache, 402, 2<<30)
	pruneStats, ran := cache.runRetainedValueLogPruneInline(true, map[uint32]struct{}{fileID: {}})
	if !ran {
		t.Fatalf("inline retained prune did not run")
	}
	if pruneStats.Mode != retainedPruneModeObservedSourceFastPath {
		t.Fatalf("Mode=%s want observed_source_fast_path", retainedPruneModeString(uint32(pruneStats.Mode)))
	}
	backend.mu.RLock()
	iteratorCalls := backend.iteratorCalls
	backend.mu.RUnlock()
	if iteratorCalls != 0 {
		t.Fatalf("iteratorCalls=%d want 0 for observed-source fast path", iteratorCalls)
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "completed" {
		t.Fatalf("last_status=%q want completed", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "observed_source_fast_path" {
		t.Fatalf("last_mode=%q want observed_source_fast_path", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_force"]; got != "true" {
		t.Fatalf("last_force=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.observed_source_fast_path_runs"]; got != "1" {
		t.Fatalf("observed_source_fast_path_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.full_live_id_scan_runs"]; got != "0" {
		t.Fatalf("full_live_id_scan_runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_scan.records"]; got != "0" {
		t.Fatalf("last_scan.records=%q want 0", got)
	}
}

func TestRetainedValueLogPruneDiagnostics_ForegroundAbort(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	seedRetainedPruneSegment(t, cache, 403, 2<<30)
	cache.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
	done := make(chan retainedValueLogPruneStats, 1)
	go func() {
		pruneStats, _ := cache.runRetainedValueLogPruneInline(false, nil)
		done <- pruneStats
	}()
	<-backend.iteratorStartedCh
	cache.lastForegroundWriteUnixNano.Add(1)
	close(backend.iteratorBlockCh)
	pruneStats := <-done
	if !pruneStats.AbortedForegroundWrites {
		t.Fatalf("AbortedForegroundWrites=false want true")
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "foreground_abort" {
		t.Fatalf("last_status=%q want foreground_abort", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "full_live_id_scan" {
		t.Fatalf("last_mode=%q want full_live_id_scan", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.foreground_abort_runs"]; got != "1" {
		t.Fatalf("foreground_abort_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.completed_runs"]; got != "0" {
		t.Fatalf("completed_runs=%q want 0", got)
	}
}

func TestRetainedValueLogPruneBudgetAbortSkipsUnprovenReclaim(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("ptr"), []byte("value"))

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	retainedPath, fileID := seedRetainedPruneSegment(t, cache, 406, 2<<30)
	backend.mu.Lock()
	backend.pointerEntries = map[string]page.ValuePtr{
		"ptr": {FileID: fileID, Length: 1},
	}
	backend.mu.Unlock()

	cache.retainedPruneFullLiveIDScanBudget = 50 * time.Millisecond
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	started := make(chan struct{})
	var startedOnce sync.Once
	cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
		if phase != "iterator_record" {
			return nil
		}
		startedOnce.Do(func() { close(started) })
		<-ctx.Done()
		return retainedPruneContextErr(ctx)
	}

	cache.scheduleRetainedValueLogPruneForce()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("retained prune did not enter budgeted full live-ID scan")
	}

	waitDone := make(chan struct{})
	go func() {
		cache.waitForRetainedValueLogPrune()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("budgeted retained prune did not finish")
	}

	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("retained path removed after budget-aborted prune: %v", err)
	}
	if !cache.valueLogRetained(retainedPath) {
		t.Fatalf("retained path unmarked after budget-aborted prune")
	}

	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "budget_abort" {
		t.Fatalf("last_status=%q want budget_abort", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "full_live_id_scan" {
		t.Fatalf("last_mode=%q want full_live_id_scan", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.budget_abort_runs"]; got != "1" {
		t.Fatalf("budget_abort_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.completed_runs"]; got != "0" {
		t.Fatalf("completed_runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.removed_segments"]; got != "0" {
		t.Fatalf("removed_segments=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.zombie_marked_segments"]; got != "0" {
		t.Fatalf("zombie_marked_segments=%q want 0", got)
	}
}

func TestRetainedValueLogPruneDiagnostics_CloseAbort(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	cache.closing.Store(true)
	_, ran := cache.runRetainedValueLogPruneInline(true, nil)
	cache.closing.Store(false)
	if ran {
		t.Fatalf("inline retained prune ran while closing")
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "close_abort" {
		t.Fatalf("last_status=%q want close_abort", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.close_abort_runs"]; got != "1" {
		t.Fatalf("close_abort_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_force"]; got != "true" {
		t.Fatalf("last_force=%q want true", got)
	}
}

func TestRetainedValueLogPruneCloseCancelsNestedLiveIDScan(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("ptr"), []byte("value"))

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}

	retainedPath, fileID := seedRetainedPruneSegment(t, cache, 404, 2<<30)
	backend.mu.Lock()
	backend.pointerEntries = map[string]page.ValuePtr{
		"ptr": {FileID: fileID, Length: 1},
	}
	backend.mu.Unlock()

	started := make(chan struct{})
	var startedOnce sync.Once
	cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
		if phase != "nested_outer_leaf_before_read" {
			return nil
		}
		startedOnce.Do(func() { close(started) })
		<-ctx.Done()
		return retainedPruneContextErr(ctx)
	}
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	cache.scheduleRetainedValueLogPruneForce()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		_ = cache.Close()
		t.Fatalf("retained prune did not enter nested live-ID scan")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- cache.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not cancel retained prune")
	}

	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("retained path removed after close-canceled prune: %v", err)
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "close_abort" {
		t.Fatalf("last_status=%q want close_abort", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "full_live_id_scan" {
		t.Fatalf("last_mode=%q want full_live_id_scan", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.close_abort_runs"]; got != "1" {
		t.Fatalf("close_abort_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.completed_runs"]; got != "0" {
		t.Fatalf("completed_runs=%q want 0", got)
	}
}

func TestRetainedValueLogPruneCancellationSkipsUnprovenReclaim(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("ptr"), []byte("value"))

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	retainedPath, fileID := seedRetainedPruneSegment(t, cache, 405, 2<<30)
	backend.mu.Lock()
	backend.pointerEntries = map[string]page.ValuePtr{
		"ptr": {FileID: fileID, Length: 1},
	}
	backend.mu.Unlock()

	started := make(chan struct{})
	var startedOnce sync.Once
	cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
		if phase != "nested_outer_leaf_before_read" {
			return nil
		}
		startedOnce.Do(func() { close(started) })
		<-ctx.Done()
		return retainedPruneContextErr(ctx)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct {
		stats retainedValueLogPruneStats
		ran   bool
	}, 1)
	go func() {
		stats, ran := cache.runRetainedValueLogPruneInlineWithContext(ctx, false, nil)
		done <- struct {
			stats retainedValueLogPruneStats
			ran   bool
		}{stats: stats, ran: ran}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel(errRetainedPruneCloseCanceled)
		t.Fatalf("retained prune did not enter nested live-ID scan")
	}
	cancel(errRetainedPruneCloseCanceled)

	var result struct {
		stats retainedValueLogPruneStats
		ran   bool
	}
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("retained prune did not exit after cancellation")
	}
	if !result.ran {
		t.Fatalf("inline retained prune did not run")
	}
	if !result.stats.AbortedClose {
		t.Fatalf("AbortedClose=false want true")
	}
	if result.stats.RemovedSegments != 0 || result.stats.ZombieMarkedSegments != 0 {
		t.Fatalf("canceled prune reclaimed segments: removed=%d zombie=%d", result.stats.RemovedSegments, result.stats.ZombieMarkedSegments)
	}
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("retained path removed after canceled prune: %v", err)
	}
	if !cache.valueLogRetained(retainedPath) {
		t.Fatalf("retained path unmarked after canceled prune")
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.last_status"]; got != "close_abort" {
		t.Fatalf("last_status=%q want close_abort", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.last_mode"]; got != "full_live_id_scan" {
		t.Fatalf("last_mode=%q want full_live_id_scan", got)
	}
}

func TestCheckpoint_SchedulesRetainedValueLogPruneAsynchronously(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 99)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000099.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("r"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	done := make(chan error, 1)
	go func() {
		done <- cache.Checkpoint()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Checkpoint blocked on retained prune")
	}

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("background prune did not start backend iterator")
	}
	close(backend.iteratorBlockCh)

	deadline := time.After(2 * time.Second)
	for {
		if !cache.retainedPruneActive() {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("background retained prune did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestCheckpoint_DefersRetainedValueLogPruneUntilForegroundQuiet(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 199)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000199.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("r"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	cache.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())

	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("background prune started during quiet-window defer")
	case <-time.After(retainedPruneNegativeAssertWait):
	}

	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("background prune did not start after quiet window elapsed")
	}
	close(backend.iteratorBlockCh)

	deadline := time.After(2 * time.Second)
	for {
		if !cache.retainedPruneActive() {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("background retained prune did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRetainedValueLogPruneQuietWaitDropsBelowPressure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		MaxValueLogRetainedBytes: 1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 198)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000198.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("q"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<20)
	cache.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())

	cache.scheduleRetainedValueLogPrune()

	deadline := time.Now().Add(2 * time.Second)
	for {
		cache.retainedPruneMu.Lock()
		waiting := cache.retainedPruneDone != nil
		running := cache.retainedPruneRunningDone != nil
		cache.retainedPruneMu.Unlock()
		if waiting && !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained prune did not enter quiet-wait state")
		}
		time.Sleep(10 * time.Millisecond)
	}

	seedRetainedPrunePressure(cache, retainedPath, 128)

	deadline = time.Now().Add(2 * time.Second)
	for {
		cache.retainedPruneMu.Lock()
		waiting := cache.retainedPruneDone != nil
		running := cache.retainedPruneRunningDone != nil
		cache.retainedPruneMu.Unlock()
		if !waiting && !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained prune stayed in-flight after dropping below pressure")
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("background prune started after retained bytes dropped below pressure")
	default:
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.schedule_requests"]; got != "1" {
		t.Fatalf("schedule_requests=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.schedule_skip.below_pressure"]; got != "1" {
		t.Fatalf("schedule_skip.below_pressure=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.inflight"]; got != "false" {
		t.Fatalf("inflight=%q want false", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.active"]; got != "false" {
		t.Fatalf("active=%q want false", got)
	}
}

func TestRetainedValueLogPruneQuietWaitPressureReboundRequiresFreshQuiet(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		MaxValueLogRetainedBytes: 1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 197)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000197.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("p"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<20)
	cache.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())

	reboundCh := make(chan struct{})
	cache.testRetainedPruneQuietWaitExitHook = func(result retainedPruneQuietWaitResult) {
		if result != retainedPruneQuietWaitBelowPressure {
			return
		}
		seedRetainedPrunePressure(cache, retainedPath, 2<<20)
		close(reboundCh)
	}

	cache.scheduleRetainedValueLogPrune()

	deadline := time.Now().Add(2 * time.Second)
	for {
		cache.retainedPruneMu.Lock()
		waiting := cache.retainedPruneDone != nil
		running := cache.retainedPruneRunningDone != nil
		cache.retainedPruneMu.Unlock()
		if waiting && !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained prune did not enter quiet-wait state")
		}
		time.Sleep(10 * time.Millisecond)
	}

	seedRetainedPrunePressure(cache, retainedPath, 128)

	select {
	case <-reboundCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("retained prune did not observe below-pressure quiet-wait exit")
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		cache.retainedPruneMu.Lock()
		waiting := cache.retainedPruneDone != nil
		running := cache.retainedPruneRunningDone != nil
		cache.retainedPruneMu.Unlock()
		if !waiting && !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained prune stayed in-flight after pressure rebound")
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("background prune started after pressure rebound without fresh quiet window")
	case <-time.After(retainedPruneNegativeAssertWait):
	}

	cache.testRetainedPruneQuietWaitExitHook = nil
	cache.scheduleRetainedValueLogPrune()
	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("retained prune started before fresh quiet window elapsed")
	case <-time.After(retainedPruneNegativeAssertWait):
	}

	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("retained prune did not start after fresh quiet window elapsed")
	}
	close(backend.iteratorBlockCh)
	cache.waitForRetainedValueLogPrune()

	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.schedule_requests"]; got != "2" {
		t.Fatalf("schedule_requests=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.schedule_skip.below_pressure"]; got != "1" {
		t.Fatalf("schedule_skip.below_pressure=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.runs"]; got != "1" {
		t.Fatalf("runs=%q want 1", got)
	}
}

func TestRetainedValueLogPrune_AbortsWhenForegroundWritesResume(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 211)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000211.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("q"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	cache.scheduleRetainedValueLogPrune()

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("background prune did not start")
	}

	lastWrite := cache.lastForegroundWriteUnixNano.Load()
	deadline := time.Now().Add(2 * time.Second)
	for !cache.foregroundWritesResumedSince(lastWrite) {
		if time.Now().After(deadline) {
			t.Fatalf("foreground write timestamp did not advance")
		}
		cache.noteWrite()
		time.Sleep(time.Millisecond)
	}
	close(backend.iteratorBlockCh)

	done := time.After(2 * time.Second)
	for {
		if !cache.retainedPruneActive() {
			break
		}
		select {
		case <-done:
			t.Fatalf("background retained prune did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("retained path removed after prune abort: %v", err)
	}
	if !cache.valueLogRetained(retainedPath) {
		t.Fatalf("retained path unmarked after prune abort")
	}
}

func TestRetainedValueLogPruneForce_AbortsWhenForegroundWritesResume(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 212)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000212.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("r"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	cache.scheduleRetainedValueLogPruneForce()

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("forced prune did not start")
	}

	lastWrite := cache.lastForegroundWriteUnixNano.Load()
	deadline := time.Now().Add(2 * time.Second)
	for !cache.foregroundWritesResumedSince(lastWrite) {
		if time.Now().After(deadline) {
			t.Fatalf("foreground write timestamp did not advance")
		}
		cache.noteWrite()
		time.Sleep(time.Millisecond)
	}
	close(backend.iteratorBlockCh)
	cache.waitForRetainedValueLogPrune()

	if !cache.valueLogRetained(retainedPath) {
		t.Fatalf("retained path unmarked after forced prune abort")
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.forced_runs"]; got != "1" {
		t.Fatalf("forced_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.foreground_abort_runs"]; got != "1" {
		t.Fatalf("foreground_abort_runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.write_gate_retries"]; got != "0" {
		t.Fatalf("write_gate_retries=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.write_gate_retry_successes"]; got != "0" {
		t.Fatalf("write_gate_retry_successes=%q want 0", got)
	}
}

func TestAllowValueLogPointers_HardCapRequestsForcedRetainedPrune(t *testing.T) {
	cache := &DB{}
	cache.testSkipRetainedPrune = true
	cache.maxValueLogRetainedBytesHard = 1024
	cache.valueLogRetainedClosedBytes.Store(2048)

	if cache.allowValueLogPointers() {
		t.Fatalf("allowValueLogPointers=true, want false when hard cap exceeded")
	}
	if got := cache.retainedValueLogPruneScheduleForcedRequests.Load(); got != 1 {
		t.Fatalf("schedule_forced_requests=%d want 1 after first hard-cap crossing", got)
	}

	// Re-check while still over cap should not repeatedly re-schedule until
	// retained bytes drop back below the hard cap.
	if cache.allowValueLogPointers() {
		t.Fatalf("allowValueLogPointers=true on repeated over-cap check, want false")
	}
	if got := cache.retainedValueLogPruneScheduleForcedRequests.Load(); got != 1 {
		t.Fatalf("schedule_forced_requests=%d want 1 after repeated over-cap check", got)
	}

	cache.valueLogRetainedClosedBytes.Store(0)
	if !cache.allowValueLogPointers() {
		t.Fatalf("allowValueLogPointers=false, want true after dropping below hard cap")
	}

	cache.valueLogRetainedClosedBytes.Store(4096)
	if cache.allowValueLogPointers() {
		t.Fatalf("allowValueLogPointers=true after second hard-cap crossing, want false")
	}
	if got := cache.retainedValueLogPruneScheduleForcedRequests.Load(); got != 2 {
		t.Fatalf("schedule_forced_requests=%d want 2 after second hard-cap crossing", got)
	}
}

func TestCheckpoint_RateLimitsRetainedValueLogPrune(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 233)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000233.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("p"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	cache.scheduleRetainedValueLogPrune()
	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("first prune did not start")
	}
	lastWrite := cache.lastForegroundWriteUnixNano.Load()
	deadline := time.Now().Add(2 * time.Second)
	for !cache.foregroundWritesResumedSince(lastWrite) {
		if time.Now().After(deadline) {
			t.Fatalf("foreground write timestamp did not advance")
		}
		cache.noteWrite()
		time.Sleep(time.Millisecond)
	}
	close(backend.iteratorBlockCh)

	done := time.After(2 * time.Second)
	for {
		if !cache.retainedPruneActive() {
			break
		}
		select {
		case <-done:
			t.Fatalf("first prune did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}

	backend.iteratorStartedCh = make(chan struct{})
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	cache.scheduleRetainedValueLogPrune()

	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("second prune started despite rate limit")
	case <-time.After(retainedPruneNegativeAssertWait):
	}
}

func TestCheckpoint_SkipsRetainedValueLogPruneBelowPressureThreshold(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		MaxValueLogRetainedBytes: 1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 244)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000244.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("s"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 128)
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	cache.scheduleRetainedValueLogPrune()

	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("background prune started below retained-byte pressure threshold")
	case <-time.After(retainedPruneNegativeAssertWait):
	}
	if cache.retainedPruneActive() {
		cache.waitForRetainedValueLogPrune()
	}
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.schedule_requests"]; got != "1" {
		t.Fatalf("schedule_requests=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.schedule_forced_requests"]; got != "0" {
		t.Fatalf("schedule_forced_requests=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.schedule_skip.below_pressure"]; got != "1" {
		t.Fatalf("schedule_skip.below_pressure=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.closed_bytes"]; got != "128" {
		t.Fatalf("closed_bytes=%q want 128", got)
	}
}

func TestRetainedValueLogPruneForce_BypassesPressureThreshold(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		MaxValueLogRetainedBytes: 1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 245)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000245.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("t"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 128)
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())

	cache.scheduleRetainedValueLogPruneForce()

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("forced retained prune did not start below pressure threshold")
	}
	close(backend.iteratorBlockCh)
	cache.waitForRetainedValueLogPrune()
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.schedule_forced_requests"]; got != "1" {
		t.Fatalf("schedule_forced_requests=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.forced_runs"]; got != "1" {
		t.Fatalf("forced_runs=%q want 1", got)
	}
}

func TestRetainedValueLogPruneForce_PreemptsQuietWait(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 246)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000246.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("u"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	cache.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())

	cache.scheduleRetainedValueLogPrune()
	select {
	case <-backend.iteratorStartedCh:
		t.Fatalf("retained prune started before quiet window elapsed")
	case <-time.After(retainedPruneNegativeAssertWait):
	}

	cache.scheduleRetainedValueLogPruneForce()

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("forced retained prune did not preempt quiet-window wait")
	}
	close(backend.iteratorBlockCh)
	cache.waitForRetainedValueLogPrune()
	stats := cache.Stats()
	if got := stats["treedb.cache.vlog_retained_prune.schedule_forced_requests"]; got != "1" {
		t.Fatalf("schedule_forced_requests=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_retained_prune.forced_runs"]; got != "1" {
		t.Fatalf("forced_runs=%q want 1", got)
	}
}

func TestCheckpoint_DoesNotWaitForPriorRetainedValueLogPrune(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 100)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000100.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("r"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("first Checkpoint: %v", err)
	}

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("background prune did not start backend iterator")
	}

	done := make(chan error, 1)
	go func() {
		done <- cache.Checkpoint()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second Checkpoint: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("second Checkpoint blocked on prior retained prune")
	}

	close(backend.iteratorBlockCh)

	deadline := time.After(2 * time.Second)
	for {
		if !cache.retainedPruneActive() {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("background retained prune did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestBackendMaintenance_DoesNotBlockOnRetainedValueLogPruneQuietWindow(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	cache, err := Open(dir, backend, Options{
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	fileID, err := valuelog.EncodeFileID(0, 321)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000321.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := valuelog.NewWriter(retainedPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("m"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	cache.markValueLogRetain(retainedPath)
	seedRetainedPrunePressure(cache, retainedPath, 2<<30)

	stopReads := make(chan struct{})
	var readWG sync.WaitGroup
	readWG.Add(1)
	go func() {
		defer readWG.Done()
		for {
			select {
			case <-stopReads:
				return
			default:
				cache.noteRead()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- cache.runWithBackendMaintenance(func() error { return nil })
	}()

	select {
	case err := <-done:
		if err != nil {
			close(stopReads)
			readWG.Wait()
			t.Fatalf("backend maintenance: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(stopReads)
		readWG.Wait()
		t.Fatalf("backend maintenance blocked on retained prune quiet-window")
	}

	close(stopReads)
	readWG.Wait()

	// Let the retained prune drain so Close() does not block.
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	cache.lastForegroundReadUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	cache.waitForRetainedValueLogPrune()
}

func TestAdvanceValueLogWriterPastObservedSeq_RollsBackToOriginalSeqOnRegisterFailure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	oldSeq := 59
	oldPath := filepath.Join(dir, valueLogName(0, oldSeq))
	oldFileID, err := valuelog.EncodeFileID(0, uint32(oldSeq))
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	writer, err := valuelog.NewWriter(oldPath, oldFileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	l := &lane{
		id:       0,
		vlog:     writer,
		vlogSeq:  oldSeq,
		vlogPath: oldPath,
	}
	db := &DB{dir: dir, valueLogDir: dir, backend: backend}
	t.Cleanup(func() {
		if l.vlog != nil {
			_ = l.vlog.Close()
			l.vlog = nil
		}
		_ = db.removeFileRetry(oldPath)
		_ = db.removeFileRetry(filepath.Join(dir, valueLogName(0, oldSeq+1)))
		_ = db.removeFileRetry(filepath.Join(dir, valueLogName(0, oldSeq+2)))
	})

	backend.registerValueLogErr = errors.New("test: register failed")
	err = db.advanceValueLogWriterPastObservedSeq(l, oldSeq+1)
	if err == nil || !strings.Contains(err.Error(), "register failed") {
		t.Fatalf("advanceValueLogWriterPastObservedSeq err=%v want register failure", err)
	}
	if got := l.vlogSeq; got != oldSeq {
		t.Fatalf("vlogSeq=%d want %d", got, oldSeq)
	}
	if got := l.vlogPath; got != oldPath {
		t.Fatalf("vlogPath=%q want %q", got, oldPath)
	}
	if l.vlog == nil {
		t.Fatalf("expected usable writer restored after register failure")
	}
	ptrs, err := db.appendValueLog(l, 0, nil, []valuelog.Record{{RID: 1, Value: []byte("value")}}, journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLog after rollback: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("ptrs len=%d want 1", len(ptrs))
	}
	if got := ptrs[0].FileID; got != page.ValueLogFileID(oldFileID) {
		t.Fatalf("ptr fileID=%d want %d", got, oldFileID)
	}
	putValueLogPtrs(ptrs)
}
