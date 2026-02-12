package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogRewriteOffline_RewritesAndShrinks(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	path1 := filepath.Join(walDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	w1.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptr1a, err := w1.Append(0, nil, 1, bytes.Repeat([]byte{0x01}, 128))
	if err != nil {
		t.Fatalf("append1a: %v", err)
	}
	_, err = w1.Append(0, nil, 2, bytes.Repeat([]byte{0x02}, 128))
	if err != nil {
		t.Fatalf("append1b: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	path2 := filepath.Join(walDir, "value-l0-000002.log")
	id2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	ptr2a, err := w2.Append(0, nil, 3, bytes.Repeat([]byte{0x03}, 128))
	if err != nil {
		t.Fatalf("append2a: %v", err)
	}
	_, err = w2.Append(0, nil, 4, bytes.Repeat([]byte{0x04}, 128))
	if err != nil {
		t.Fatalf("append2b: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1a); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2a); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.BytesAfter >= stats.BytesBefore {
		t.Fatalf("expected bytes to shrink, before=%d after=%d", stats.BytesBefore, stats.BytesAfter)
	}

	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old segment1 removed, err=%v", err)
	}
	if _, err := os.Stat(path2); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old segment2 removed, err=%v", err)
	}

	db, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if !bytes.Equal(val, bytes.Repeat([]byte{0x01}, 128)) {
		t.Fatalf("k1 mismatch")
	}
	val, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if !bytes.Equal(val, bytes.Repeat([]byte{0x03}, 128)) {
		t.Fatalf("k2 mismatch")
	}
}

func TestValueLogRewrite_HealthMetadata_PreservedAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 90_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}
	db = nil

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	healthPath := valueLogHealthPath(dir)
	beforeReopen, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health before reopen: %v", err)
	}
	if len(beforeReopen) == 0 {
		t.Fatalf("expected health metadata after rewrite")
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close reopen: %v", err)
	}

	afterReopen, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after reopen: %v", err)
	}
	if !reflect.DeepEqual(beforeReopen, afterReopen) {
		t.Fatalf("health metadata changed across reopen: before=%+v after=%+v", beforeReopen, afterReopen)
	}
}

func TestValueLogRewrite_BatchedPointerSwap_ReopenPreservesData(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 100_000, 4, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 512)
	})
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte{byte('a' + i)}, ptrs[i]); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := range ptrs {
		key := []byte{byte('a' + i)}
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		want := bytes.Repeat([]byte{byte(i + 1)}, 512)
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q", key)
		}
	}
}

func TestValueLogRewriteOnline_NoPointerKeys_DoesNotCreateNewSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 120_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("p"), 512)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()
	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete pointer key: %v", err)
	}

	segmentsBefore, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	var maxValueSeqBefore uint64
	for _, seg := range segmentsBefore {
		if seg.valueLog && seg.seq > maxValueSeqBefore {
			maxValueSeqBefore = seg.seq
		}
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 0 {
		t.Fatalf("expected no copied records, got %+v", stats)
	}

	segmentsAfter, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("list segments after rewrite: %v", err)
	}
	for _, seg := range segmentsAfter {
		if seg.valueLog && seg.seq > maxValueSeqBefore {
			t.Fatalf("unexpected new value-log segment created for no-op rewrite: %+v", seg)
		}
	}
}

func TestValueLogRewrite_BatchedPointerSwap_SnapshotIsolation(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 110_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(10 + i)}, 512)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	snap := db.AcquireSnapshot()
	defer snap.Close()
	oldSet := snap.state.ValueLogSet
	if oldSet == nil {
		t.Fatalf("snapshot missing ValueLogSet")
	}
	oldID := ptrs[0].FileID
	if _, ok := oldSet.Files[oldID]; !ok {
		t.Fatalf("snapshot missing old segment %d", oldID)
	}

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	gotSnap, err := snap.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("snapshot get k1: %v", err)
	}
	if !bytes.Equal(gotSnap, bytes.Repeat([]byte{10}, 512)) {
		t.Fatalf("snapshot value mismatch after rewrite")
	}

	state := db.State()
	if state == nil || state.ValueLogSet == nil {
		t.Fatalf("db state missing ValueLogSet after rewrite")
	}
	if _, ok := state.ValueLogSet.Files[oldID]; ok {
		t.Fatalf("old segment %d still visible in current state after rewrite", oldID)
	}
}

func TestValueLogRewrite_LocalityPolicy_PreservesGroupedAdjacency(t *testing.T) {
	candidates := []rewriteCandidate{
		{key: []byte("k3"), oldPtr: page.ValuePtr{FileID: 2, Offset: 400, Length: 1}},
		{key: []byte("k1"), oldPtr: page.ValuePtr{FileID: 1, Offset: 100, Length: 1}},
		{key: []byte("k2"), oldPtr: page.ValuePtr{FileID: 1, Offset: 120, Length: 1}},
		{key: []byte("k4"), oldPtr: page.ValuePtr{FileID: 2, Offset: 430, Length: 1}},
		{key: []byte("k0"), oldPtr: page.ValuePtr{FileID: 1, Offset: 80, Length: 1}},
	}

	orderRewriteCandidates(candidates, ValueLogRewriteLocalityGrouped)
	for i := 1; i < len(candidates); i++ {
		prev := candidates[i-1].oldPtr
		cur := candidates[i].oldPtr
		if prev.FileID > cur.FileID {
			t.Fatalf("file id order regressed at %d: prev=%d cur=%d", i, prev.FileID, cur.FileID)
		}
		if prev.FileID == cur.FileID && prev.Offset > cur.Offset {
			t.Fatalf("offset order regressed at %d: prev=%d cur=%d", i, prev.Offset, cur.Offset)
		}
	}
}

func TestValueLogRewrite_LocalityPolicy_NoWorseThanDefaultOnMixedSets(t *testing.T) {
	candidates := []rewriteCandidate{
		{key: []byte("a"), oldPtr: page.ValuePtr{FileID: 2, Offset: 1000, Length: 1}},
		{key: []byte("b"), oldPtr: page.ValuePtr{FileID: 1, Offset: 10, Length: 1}},
		{key: []byte("c"), oldPtr: page.ValuePtr{FileID: 2, Offset: 1040, Length: 1}},
		{key: []byte("d"), oldPtr: page.ValuePtr{FileID: 1, Offset: 30, Length: 1}},
		{key: []byte("e"), oldPtr: page.ValuePtr{FileID: 3, Offset: 7, Length: 1}},
		{key: []byte("f"), oldPtr: page.ValuePtr{FileID: 2, Offset: 1080, Length: 1}},
	}

	defaultOrdered := append([]rewriteCandidate(nil), candidates...)
	groupedOrdered := append([]rewriteCandidate(nil), candidates...)
	orderRewriteCandidates(defaultOrdered, ValueLogRewriteLocalityDefault)
	orderRewriteCandidates(groupedOrdered, ValueLogRewriteLocalityGrouped)

	defaultCost := rewriteLocalityTransitionCost(defaultOrdered)
	groupedCost := rewriteLocalityTransitionCost(groupedOrdered)
	if groupedCost > defaultCost {
		t.Fatalf("grouped locality cost regressed: grouped=%d default=%d", groupedCost, defaultCost)
	}
}

func TestValueLogRewrite_LocalityPolicy_DeterministicOrderingFixture(t *testing.T) {
	input := []rewriteCandidate{
		{key: []byte("k5"), oldPtr: page.ValuePtr{FileID: 2, Offset: 200, Length: 1}},
		{key: []byte("k2"), oldPtr: page.ValuePtr{FileID: 1, Offset: 30, Length: 1}},
		{key: []byte("k3"), oldPtr: page.ValuePtr{FileID: 1, Offset: 30, Length: 1}},
		{key: []byte("k1"), oldPtr: page.ValuePtr{FileID: 1, Offset: 10, Length: 1}},
	}
	orderRewriteCandidates(input, ValueLogRewriteLocalityGrouped)

	got := make([]string, 0, len(input))
	for _, c := range input {
		got = append(got, string(c.key))
	}
	want := []string{"k1", "k2", "k3", "k5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deterministic grouped order mismatch: got=%v want=%v", got, want)
	}
}

func rewriteLocalityTransitionCost(candidates []rewriteCandidate) int {
	if len(candidates) <= 1 {
		return 0
	}
	cost := 0
	for i := 1; i < len(candidates); i++ {
		prev := candidates[i-1].oldPtr
		cur := candidates[i].oldPtr
		if prev.FileID != cur.FileID {
			cost += 10
			continue
		}
		if cur.Offset < prev.Offset {
			cost += 5
			continue
		}
		delta := cur.Offset - prev.Offset
		if delta > 256 {
			cost++
		}
	}
	return cost
}
