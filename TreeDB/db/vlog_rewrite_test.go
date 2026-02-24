package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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

func TestValueLogRewriteOffline_LegacyGroupedFenceMarkerHint(t *testing.T) {
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

	path := filepath.Join(walDir, "value-l0-000001.log")
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid: %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	ptrs, err := w.AppendFrame(0, nil, []valuelog.Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
	})
	if err != nil {
		_ = w.Close()
		t.Fatalf("append grouped frame: %v", err)
	}
	if len(ptrs) != 2 || !page.ValuePtrIsGrouped(ptrs[0]) || !page.ValuePtrIsGrouped(ptrs[1]) {
		_ = w.Close()
		t.Fatalf("expected grouped pointers from frame append")
	}
	for i := 0; i < 4; i++ {
		if _, err := w.Append(0, nil, 10+uint64(i), bytes.Repeat([]byte{byte('x' + i)}, 256)); err != nil {
			_ = w.Close()
			t.Fatalf("append stale record %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	const legacyGroupedFenceMarkerBit = 0x00800000 // historical fence-marker bit in grouped length encoding
	legacyPtr := ptrs[0]
	legacyPtr.Length |= legacyGroupedFenceMarkerBit

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), legacyPtr); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptrs[1]); err != nil {
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
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite to copy records, got stats=%+v", stats)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()

	v1, err := reopen.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if !bytes.Equal(v1, []byte("alpha")) {
		t.Fatalf("k1 mismatch after rewrite: got=%q", v1)
	}
	v2, err := reopen.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if !bytes.Equal(v2, []byte("beta")) {
		t.Fatalf("k2 mismatch after rewrite: got=%q", v2)
	}
}

func TestValueLogRewrite_HealthMetadata_PreservedAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV1,
	})
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

	db, err := Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV1,
	})
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

func TestValueLogRewriteOnline_PreservesFenceOuterMarkerAndIteratorParity(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                 dir,
		IndexOuterLeafMode:  IndexOuterLeafModeV2FencePtr,
		IndexPackedValuePtr: false,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new valuelog writer: %v", err)
	}
	payload, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
		{Key: []byte("f010"), Value: []byte("v10")},
		{Key: []byte("f020"), Value: []byte("v20")},
		{Key: []byte("f030"), Value: []byte("v30")},
	}, uint8(ValueLogBlockSnappy), 8)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("encode outerleaf payload: %v", err)
	}
	buildRawRecord := func(rid uint64, value []byte) ([]byte, uint32) {
		raw := make([]byte, valuelog.HeaderSize+len(value))
		raw[4] = valuelog.Version
		raw[5] = 0 // non-grouped record
		raw[6] = 0
		raw[7] = 0
		binary.LittleEndian.PutUint64(raw[8:16], rid)
		binary.LittleEndian.PutUint32(raw[16:20], uint32(len(value)))
		copy(raw[valuelog.HeaderSize:], value)
		sum := crc.ChecksumParts(raw[4:valuelog.HeaderSize], raw[valuelog.HeaderSize:])
		binary.LittleEndian.PutUint32(raw[0:4], sum)
		length := uint32(valuelog.HeaderSize-4) + uint32(len(value))
		return raw, length
	}
	raw, rawLen := buildRawRecord(1, payload)
	rawLen = page.ValuePtrMarkFenceOuter(page.ValuePtr{Length: rawLen}).Length
	ptr, err := vw.AppendRawRecord(raw, rawLen)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append outerleaf payload: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	ptrUpperPayload, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
		{Key: []byte("f100"), Value: []byte("v100")},
		{Key: []byte("f110"), Value: []byte("v110")},
	}, uint8(ValueLogBlockSnappy), 8)
	if err != nil {
		t.Fatalf("encode upper outerleaf payload: %v", err)
	}
	fileIDUpper, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("encode upper file id: %v", err)
	}
	vwUpper, err := valuelog.NewWriter(filepath.Join(walDir, "value-l0-000002.log"), fileIDUpper)
	if err != nil {
		t.Fatalf("new upper valuelog writer: %v", err)
	}
	rawUpper, rawUpperLen := buildRawRecord(2, ptrUpperPayload)
	rawUpperLen = page.ValuePtrMarkFenceOuter(page.ValuePtr{Length: rawUpperLen}).Length
	ptrUpper, err := vwUpper.AppendRawRecord(rawUpper, rawUpperLen)
	if err != nil {
		_ = vwUpper.Close()
		t.Fatalf("append upper outerleaf payload: %v", err)
	}
	if err := vwUpper.Close(); err != nil {
		t.Fatalf("close upper writer: %v", err)
	}

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("f010"), ptr); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer(f010): %v", err)
	}
	if err := b.SetPointer([]byte("f100"), ptrUpper); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer(f100): %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()

	collectKeys := func(start, end []byte) ([]string, error) {
		it, err := db.Iterator(start, end)
		if err != nil {
			return nil, err
		}
		defer it.Close()
		keys := make([]string, 0, 4)
		for ; it.Valid(); it.Next() {
			keys = append(keys, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			return nil, err
		}
		return keys, nil
	}

	fencePtrForKey := func(target []byte) (page.ValuePtr, byte, bool, error) {
		it, err := db.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModePointerProjection})
		if err != nil {
			return page.ValuePtr{}, 0, false, err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			if !bytes.Equal(it.UnsafeKey(), target) {
				continue
			}
			_, p, flags := it.UnsafeEntry()
			return p, flags, true, nil
		}
		if err := it.Error(); err != nil {
			return page.ValuePtr{}, 0, false, err
		}
		return page.ValuePtr{}, 0, false, nil
	}

	val, err := db.Get([]byte("f020"))
	if err != nil {
		t.Fatalf("pre-rewrite Get(f020): %v", err)
	}
	if !bytes.Equal(val, []byte("v20")) {
		t.Fatalf("pre-rewrite Get(f020)=%q want=%q", val, []byte("v20"))
	}
	ptrBefore, flagsBefore, ok, err := fencePtrForKey([]byte("f010"))
	if err != nil {
		t.Fatalf("projection before rewrite: %v", err)
	}
	if !ok {
		t.Fatalf("projection did not include f010 before rewrite")
	}
	if flagsBefore&node.FlagPointer == 0 {
		t.Fatalf("f010 flags before rewrite missing pointer bit: %08b", flagsBefore)
	}
	if !page.ValuePtrIsFenceOuter(ptrBefore) {
		t.Fatalf("f010 pointer before rewrite missing fence marker: %+v", ptrBefore)
	}
	keysBefore, err := collectKeys([]byte("f015"), []byte("f115"))
	if err != nil {
		t.Fatalf("collect keys before rewrite: %v", err)
	}
	if !reflect.DeepEqual(keysBefore, []string{"f020", "f030", "f100", "f110"}) {
		t.Fatalf("keys before rewrite=%v want=[f020 f030 f100 f110]", keysBefore)
	}

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	val, err = db.Get([]byte("f020"))
	if err != nil {
		t.Fatalf("post-rewrite Get(f020): %v", err)
	}
	if !bytes.Equal(val, []byte("v20")) {
		t.Fatalf("post-rewrite Get(f020)=%q want=%q", val, []byte("v20"))
	}
	val, err = db.Get([]byte("f110"))
	if err != nil {
		t.Fatalf("post-rewrite Get(f110): %v", err)
	}
	if !bytes.Equal(val, []byte("v110")) {
		t.Fatalf("post-rewrite Get(f110)=%q want=%q", val, []byte("v110"))
	}
	keysAfter, err := collectKeys([]byte("f015"), []byte("f115"))
	if err != nil {
		t.Fatalf("collect keys after rewrite: %v", err)
	}
	if !reflect.DeepEqual(keysAfter, []string{"f020", "f030", "f100", "f110"}) {
		t.Fatalf("keys after rewrite=%v want=[f020 f030 f100 f110]", keysAfter)
	}

	ptrAfter, flagsAfter, ok, err := fencePtrForKey([]byte("f010"))
	if err != nil {
		t.Fatalf("projection after rewrite: %v", err)
	}
	if !ok {
		t.Fatalf("projection did not include f010 after rewrite")
	}
	if flagsAfter&node.FlagPointer == 0 {
		t.Fatalf("f010 flags after rewrite missing pointer bit: %08b", flagsAfter)
	}
	if !page.ValuePtrIsFenceOuter(ptrAfter) {
		t.Fatalf("f010 pointer after rewrite missing fence marker: before=%+v after=%+v", ptrBefore, ptrAfter)
	}
	if page.ValuePtrRecordLength(ptrAfter) != page.ValuePtrRecordLength(ptrBefore) {
		t.Fatalf("f010 pointer record length hint changed across rewrite: before=%d after=%d", page.ValuePtrRecordLength(ptrBefore), page.ValuePtrRecordLength(ptrAfter))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}
	db = nil

	db, err = Open(Options{
		Dir:                 dir,
		IndexOuterLeafMode:  IndexOuterLeafModeV2FencePtr,
		IndexPackedValuePtr: false,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	val, err = db.Get([]byte("f020"))
	if err != nil {
		t.Fatalf("reopen Get(f020): %v", err)
	}
	if !bytes.Equal(val, []byte("v20")) {
		t.Fatalf("reopen Get(f020)=%q want=%q", val, []byte("v20"))
	}
	keysAfterReopen, err := collectKeys([]byte("f015"), []byte("f115"))
	if err != nil {
		t.Fatalf("collect keys after reopen: %v", err)
	}
	if !reflect.DeepEqual(keysAfterReopen, []string{"f020", "f030", "f100", "f110"}) {
		t.Fatalf("keys after reopen=%v want=[f020 f030 f100 f110]", keysAfterReopen)
	}
	ptrAfterReopen, flagsAfterReopen, ok, err := fencePtrForKey([]byte("f010"))
	if err != nil {
		t.Fatalf("projection after reopen: %v", err)
	}
	if !ok {
		t.Fatalf("projection did not include f010 after reopen")
	}
	if flagsAfterReopen&node.FlagPointer == 0 {
		t.Fatalf("f010 flags after reopen missing pointer bit: %08b", flagsAfterReopen)
	}
	if !page.ValuePtrIsFenceOuter(ptrAfterReopen) {
		t.Fatalf("f010 pointer after reopen missing fence marker: %+v", ptrAfterReopen)
	}
}

func TestValueLogRewriteOnline_LegacyGroupedFenceMarkerHint_DoesNotUpgradeToFencePointer(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid: %v", err)
	}
	w, err := valuelog.NewWriter(filepath.Join(walDir, "value-l0-000001.log"), fileID)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	fakeOuterLeafPrefixValue := []byte("TOL2-not-a-valid-outerleaf-block")
	ptrs, err := w.AppendFrame(0, nil, []valuelog.Record{
		{RID: 1, Value: fakeOuterLeafPrefixValue},
		{RID: 2, Value: []byte("beta")},
	})
	if err != nil {
		_ = w.Close()
		t.Fatalf("append grouped frame: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if len(ptrs) != 2 {
		t.Fatalf("expected 2 grouped pointers, got %d", len(ptrs))
	}

	const legacyGroupedFenceMarkerBit = 0x00800000
	legacyPtr := ptrs[0]
	legacyPtr.Length |= legacyGroupedFenceMarkerBit

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), legacyPtr); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	ptrForKey := func(target []byte) (page.ValuePtr, byte, bool, error) {
		it, err := db.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModePointerProjection})
		if err != nil {
			return page.ValuePtr{}, 0, false, err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			if !bytes.Equal(it.UnsafeKey(), target) {
				continue
			}
			_, p, flags := it.UnsafeEntry()
			return p, flags, true, nil
		}
		if err := it.Error(); err != nil {
			return page.ValuePtr{}, 0, false, err
		}
		return page.ValuePtr{}, 0, false, nil
	}

	ptrBefore, flagsBefore, ok, err := ptrForKey([]byte("k1"))
	if err != nil {
		t.Fatalf("projection before rewrite: %v", err)
	}
	if !ok {
		t.Fatalf("k1 missing before rewrite")
	}
	if flagsBefore&node.FlagPointer == 0 {
		t.Fatalf("k1 before rewrite missing pointer flag: %08b", flagsBefore)
	}
	if !page.ValuePtrIsGrouped(ptrBefore) {
		t.Fatalf("expected grouped pointer before rewrite: %+v", ptrBefore)
	}
	if !page.ValuePtrIsFenceOuter(ptrBefore) {
		t.Fatalf("expected legacy grouped marker to be visible before rewrite: %+v", ptrBefore)
	}

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: false,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	ptrAfter, flagsAfter, ok, err := ptrForKey([]byte("k1"))
	if err != nil {
		t.Fatalf("projection after rewrite: %v", err)
	}
	if !ok {
		t.Fatalf("k1 missing after rewrite")
	}
	if flagsAfter&node.FlagPointer == 0 {
		t.Fatalf("k1 after rewrite missing pointer flag: %08b", flagsAfter)
	}
	if !page.ValuePtrIsGrouped(ptrAfter) {
		t.Fatalf("expected grouped pointer after rewrite: %+v", ptrAfter)
	}
	if page.ValuePtrIsFenceOuter(ptrAfter) {
		t.Fatalf("legacy grouped marker should not be upgraded into explicit fence pointer: %+v", ptrAfter)
	}

	v1, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1 after rewrite: %v", err)
	}
	if !bytes.Equal(v1, fakeOuterLeafPrefixValue) {
		t.Fatalf("k1 mismatch after rewrite: got=%q", v1)
	}
	v2, err := db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2 after rewrite: %v", err)
	}
	if !bytes.Equal(v2, []byte("beta")) {
		t.Fatalf("k2 mismatch after rewrite: got=%q", v2)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}
	db = nil

	db, err = Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV1,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	v1, err = db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1 after reopen: %v", err)
	}
	if !bytes.Equal(v1, fakeOuterLeafPrefixValue) {
		t.Fatalf("k1 mismatch after reopen: got=%q", v1)
	}
	v2, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2 after reopen: %v", err)
	}
	if !bytes.Equal(v2, []byte("beta")) {
		t.Fatalf("k2 mismatch after reopen: got=%q", v2)
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

func TestValueLogRewriteOffline_BlobRefOuterLeafPointerPreserved(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
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
	fileIDBlob, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode blob file id: %v", err)
	}
	blobPath := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(blobPath, fileIDBlob)
	if err != nil {
		t.Fatalf("new blob valuelog writer: %v", err)
	}

	blobValue := bytes.Repeat([]byte("nested-blob|"), 64)
	blobPtr, err := vw.Append(0, nil, 5000, blobValue)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append blob payload: %v", err)
	}

	key := []byte("k-blob-ref")
	outerPayload, err := outerleaf.EncodeSingleBlobRef(nil, key, blobPtr, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("EncodeSingleBlobRef: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close blob valuelog writer: %v", err)
	}

	fileIDOuter, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("encode outer file id: %v", err)
	}
	outerPath := filepath.Join(walDir, "value-l0-000002.log")
	vw, err = valuelog.NewWriter(outerPath, fileIDOuter)
	if err != nil {
		t.Fatalf("new outer valuelog writer: %v", err)
	}
	outerPtr, err := vw.Append(0, nil, 5001, outerPayload)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append outer payload: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close outer valuelog writer: %v", err)
	}

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer(key, outerPtr); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("close db before rewrite: %v", err)
	}
	db = nil

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	proj, err := reopen.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorWithOptions projection: %v", err)
	}
	defer proj.Close()

	found := false
	for ; proj.Valid(); proj.Next() {
		if !bytes.Equal(proj.Key(), key) {
			continue
		}
		_, gotPtr, flags := proj.UnsafeEntry()
		if flags&node.FlagPointer == 0 {
			t.Fatalf("expected pointer flag for %q", key)
		}
		if gotPtr != outerPtr {
			t.Fatalf("outer pointer rewritten unexpectedly: got=%+v want=%+v", gotPtr, outerPtr)
		}
		found = true
		break
	}
	if err := proj.Error(); err != nil {
		t.Fatalf("projection iterator error: %v", err)
	}
	if !found {
		t.Fatalf("projection iterator did not observe key %q", key)
	}

	got, err := reopen.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, blobValue) {
		t.Fatalf("Get(%q) mismatch: got len=%d want len=%d", key, len(got), len(blobValue))
	}

	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("expected nested blob segment to remain after rewrite: %v", err)
	}
	if _, err := os.Stat(outerPath); err != nil {
		t.Fatalf("expected outer block segment to remain after rewrite: %v", err)
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

func TestValueLogRewriteOnline_V1LeafLogRoute_PreservesNestedBlobRefSegments(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                dir,
		IndexOuterLeafMode: IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	blobPath := filepath.Join(walDir, "value-l0-000001.log")
	blobFileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("blob file id: %v", err)
	}
	blobWriter, err := valuelog.NewWriter(blobPath, blobFileID)
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	blobValue := bytes.Repeat([]byte("nested-online|"), 96)
	blobPtr, err := blobWriter.Append(0, nil, 21_000, blobValue)
	if err != nil {
		_ = blobWriter.Close()
		t.Fatalf("append blob: %v", err)
	}
	if err := blobWriter.Close(); err != nil {
		t.Fatalf("close blob writer: %v", err)
	}

	key := []byte("k-route-nested")
	outerPayload, err := outerleaf.EncodeSingleBlobRef(nil, key, blobPtr, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		t.Fatalf("EncodeSingleBlobRef: %v", err)
	}
	outerPath := filepath.Join(walDir, "value-l0-000002.log")
	outerFileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("outer file id: %v", err)
	}
	outerWriter, err := valuelog.NewWriter(outerPath, outerFileID)
	if err != nil {
		t.Fatalf("outer writer: %v", err)
	}
	outerPtr, err := outerWriter.Append(0, nil, 21_001, outerPayload)
	if err != nil {
		_ = outerWriter.Close()
		t.Fatalf("append outer: %v", err)
	}
	if err := outerWriter.Close(); err != nil {
		t.Fatalf("close outer writer: %v", err)
	}

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer(key, outerPtr); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write pointer batch: %v", err)
	}
	_ = b.Close()

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, blobValue) {
		t.Fatalf("Get(%q) mismatch: got len=%d want len=%d", key, len(got), len(blobValue))
	}

	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("expected nested blob segment to remain after online rewrite: %v", err)
	}
	if _, err := os.Stat(outerPath); err == nil {
		t.Fatalf("expected rewritten outer block source segment to be cleaned up")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat outer source segment: %v", err)
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

func TestValueLogRewriteOnline_SourceFileIDs_RestrictsRewriteSet(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 200_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 200_010, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs1[0].FileID},
		BatchSize:     8,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record, got %d", stats.RecordsCopied)
	}

	ptrK1, flagsK1 := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, flagsK2 := readProjectedPointerByKey(t, db, []byte("k2"))
	if flagsK1&node.FlagPointer == 0 || flagsK2&node.FlagPointer == 0 {
		t.Fatalf("expected pointer flags for rewritten keys: k1=%#x k2=%#x", flagsK1, flagsK2)
	}
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off source segment %d", ptrs1[0].FileID)
	}
	if ptrK2.FileID != ptrs2[0].FileID {
		t.Fatalf("expected k2 pointer to remain on non-selected segment %d, got %d", ptrs2[0].FileID, ptrK2.FileID)
	}
}

func TestValueLogRewriteOnline_SparseSelection_RewritesHighStaleSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Segment 1: two records (one referenced, one stale).
	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 210_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	// Segment 2: one referenced record.
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 210_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:            8,
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.30,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record from sparse segment, got %d", stats.RecordsCopied)
	}

	ptrK1, _ := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, _ := readProjectedPointerByKey(t, db, []byte("k2"))
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off stale source segment %d", ptrs1[0].FileID)
	}
	if ptrK2.FileID != ptrs2[0].FileID {
		t.Fatalf("expected k2 pointer to remain on segment %d, got %d", ptrs2[0].FileID, ptrK2.FileID)
	}
}

func TestValueLogRewriteOnline_SparseSelection_NoSelectedSources_IsNoOp(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// One fully-live segment: no stale bytes means sparse source selection should
	// select nothing and return a deterministic no-op stats result.
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 220_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:            8,
		MaxSourceSegments:    1,
		MinSegmentStaleRatio: 0.5,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 0 {
		t.Fatalf("expected no-op rewrite with zero copied records, got %d", stats.RecordsCopied)
	}
	if stats.SegmentsAfter != stats.SegmentsBefore {
		t.Fatalf("expected no-op segment count stats, before=%d after=%d", stats.SegmentsBefore, stats.SegmentsAfter)
	}
	if stats.BytesAfter != stats.BytesBefore {
		t.Fatalf("expected no-op byte stats, before=%d after=%d", stats.BytesBefore, stats.BytesAfter)
	}
}

func readProjectedPointerByKey(t *testing.T, db *DB, key []byte) (page.ValuePtr, byte) {
	t.Helper()
	it, err := db.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorWithOptions: %v", err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if !bytes.Equal(it.UnsafeKey(), key) {
			continue
		}
		_, ptr, flags := it.UnsafeEntry()
		return ptr, flags
	}
	if err := it.Error(); err != nil {
		t.Fatalf("projection iterator error: %v", err)
	}
	t.Fatalf("missing key %q in projection iterator", key)
	return page.ValuePtr{}, 0
}

func TestSelectRewriteSourceSegments_OversizeCandidates_SelectsOne(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "v1.log")
	path2 := filepath.Join(dir, "v2.log")
	if err := os.WriteFile(path1, bytes.Repeat([]byte{1}, 100), 0o644); err != nil {
		t.Fatalf("write path1: %v", err)
	}
	if err := os.WriteFile(path2, bytes.Repeat([]byte{2}, 100), 0o644); err != nil {
		t.Fatalf("write path2: %v", err)
	}

	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}
	active := map[uint32]struct{}{}
	liveByID := map[uint32]int64{
		1: 90, // stale 10
		2: 80, // stale 20 (preferred)
	}

	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		MaxSourceBytes:       32,
		MaxSourceSegments:    2,
		MinSegmentStaleBytes: 1,
	}, files, active, liveByID)

	if len(selected) != 1 {
		t.Fatalf("expected one selected segment when all candidates exceed byte budget, got %d", len(selected))
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("expected segment 2 selected by stale priority, got=%v", selected)
	}
}
