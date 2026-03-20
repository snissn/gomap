package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	iteriface "github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func writeValueLogSegment(t *testing.T, dir string, lane, seq uint32) (path string, fileID uint32) {
	t.Helper()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path = filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("x"), 256)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return path, fileID
}

func TestValueLogGC_DoesNotRescanWhenSetAlreadyPopulated(t *testing.T) {
	dir := t.TempDir()
	path1, id1 := writeValueLogSegment(t, dir, 0, 1)
	_, _ = writeValueLogSegment(t, dir, 0, 2)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	if !d.valueLogManager.HasSegment(id1) {
		t.Fatalf("expected segment %d to be registered at open", id1)
	}
	before := d.valueLogManager.RefreshScanCount()

	_, err = d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ValueLogGC triggered value-log refresh scan: before=%d after=%d", before, after)
	}
	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected GC to remove eligible segment %q, err=%v", path1, err)
	}
}

func TestValueLogRewritePlan_DoesNotRescanWhenSetAlreadyPopulated(t *testing.T) {
	dir := t.TempDir()
	_, _ = writeValueLogSegment(t, dir, 0, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	before := d.valueLogManager.RefreshScanCount()

	_, err = d.ValueLogRewritePlan(context.Background(), ValueLogRewriteOnlineOptions{})
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ValueLogRewritePlan triggered value-log refresh scan: before=%d after=%d", before, after)
	}
}

type projectionOnlyIterator struct {
	idx              int
	flags            []byte
	ptrs             []page.ValuePtr
	unsafeEntryCalls int
	projectionCalls  int
}

func (it *projectionOnlyIterator) Valid() bool { return it.idx >= 0 && it.idx < len(it.flags) }
func (it *projectionOnlyIterator) Next()       { it.idx++ }
func (it *projectionOnlyIterator) Seek(key []byte) {
	it.idx = 0
}
func (it *projectionOnlyIterator) UnsafeKey() []byte           { return nil }
func (it *projectionOnlyIterator) UnsafeValue() []byte         { return nil }
func (it *projectionOnlyIterator) Key() []byte                 { return nil }
func (it *projectionOnlyIterator) Value() []byte               { return nil }
func (it *projectionOnlyIterator) KeyCopy(dst []byte) []byte   { return dst[:0] }
func (it *projectionOnlyIterator) ValueCopy(dst []byte) []byte { return dst[:0] }
func (it *projectionOnlyIterator) IsDeleted() bool             { return false }
func (it *projectionOnlyIterator) Error() error                { return nil }
func (it *projectionOnlyIterator) Close() error                { return nil }
func (it *projectionOnlyIterator) Domain() (start, end []byte) { return nil, nil }
func (it *projectionOnlyIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	it.unsafeEntryCalls++
	return nil, it.ptrs[it.idx], it.flags[it.idx]
}
func (it *projectionOnlyIterator) UnsafePointerProjection() (page.ValuePtr, byte) {
	it.projectionCalls++
	return it.ptrs[it.idx], it.flags[it.idx]
}

var _ iteriface.PointerProjection = (*projectionOnlyIterator)(nil)

func TestCollectValueLogRefCounts_PrefersPointerProjection(t *testing.T) {
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	it := &projectionOnlyIterator{
		idx:   0,
		flags: []byte{node.FlagInline, node.FlagPointer},
		ptrs: []page.ValuePtr{
			{},
			{FileID: fileID, Offset: 123, Length: 9},
		},
	}
	refs := make(map[uint32]uint64)
	if err := collectValueLogRefCounts(context.Background(), nil, it, refs); err != nil {
		t.Fatalf("collectValueLogRefCounts: %v", err)
	}
	if it.unsafeEntryCalls != 0 {
		t.Fatalf("UnsafeEntry called %d times, want 0", it.unsafeEntryCalls)
	}
	if it.projectionCalls != 2 {
		t.Fatalf("UnsafePointerProjection called %d times, want 2", it.projectionCalls)
	}
	if refs[fileID] != 1 {
		t.Fatalf("refs[%d]=%d want 1", fileID, refs[fileID])
	}
}
