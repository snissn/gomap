package valuelog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestCOWGroupedFrameCache_VerifiedHitAndPolicyIsolation(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	w.SetBlockCompression(BlockCodecLZ4, true)
	const k = 8
	recs := make([]Record, k)
	want := make([][]byte, k)
	for i := 0; i < k; i++ {
		val := bytes.Repeat([]byte(fmt.Sprintf("cow-grouped-frame-value-%02d|", i)), 40)
		want[i] = val
		recs[i] = Record{RID: uint64(i + 1), Value: val}
	}
	ptrs, err := w.AppendFrame(0, nil, recs)
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	m.SetGroupedFrameCacheEntries(64)
	m.SetGroupedFrameCacheMaxBytes(4 << 20)

	f := m.files[fileID]
	if f == nil {
		t.Fatal("missing registered value-log file")
	}

	limits := COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}

	// 1. Cold cache misses cleanly without creating an idle cache.
	if out, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, nil, nil, false, false); hit || out != nil {
		t.Fatalf("cold cache read: out=%d hit=%t", len(out), hit)
	}
	if f.groupedFrameCache.Load() != nil {
		t.Fatal("cold COW cache miss unexpectedly allocated groupedFrameCache")
	}

	// 2. Non-COW plain groupedFrameCacheStore must NOT satisfy CopyVerifiedCOWGroupedValueCached.
	shape0, err := f.InspectCOWRecord(ptrs[0], limits)
	if err != nil || !shape0.Compressed || shape0.Codec != BlockCodecLZ4 {
		t.Fatalf("InspectCOWRecord: shape=%+v err=%v", shape0, err)
	}
	var plainOffsets [MaxFrameK + 1]uint32
	var plainRaw []byte
	for i := 0; i < k; i++ {
		plainOffsets[i] = uint32(len(plainRaw))
		plainRaw = append(plainRaw, want[i]...)
	}
	plainOffsets[k] = uint32(len(plainRaw))
	if !f.groupedFrameCacheStore(int64(ptrs[0].Offset-4), true, k, plainOffsets, plainRaw, false) {
		t.Fatal("plain groupedFrameCacheStore failed")
	}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, nil, nil, false, false); hit {
		t.Fatal("non-COW cache entry satisfied COW read")
	}

	// 3. Verified COW decode via ReadCOWRecordAndCache populates the COW certificate,
	// even when caller passes a zero-length rawScratch with sufficient capacity.
	dec, err := NewCOWBlockDecoder(shape0.Codec, shape0.RawBytes, shape0.PayloadBytes(), func(COWDecoderAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	input := make([]byte, shape0.PayloadBytes())
	raw := make([]byte, 0, shape0.RawBytes)
	outScratch := make([]byte, shape0.ValueBytes)
	got0, err := f.ReadCOWRecordAndCache(ptrs[0], shape0, input, raw, outScratch, dec.Decode)
	if err != nil || !bytes.Equal(got0, want[0]) {
		t.Fatalf("ReadCOWRecordAndCache: len=%d err=%v", len(got0), err)
	}

	// 4. All 8 sibling sub-records hit the cached frame (both useDst=true and useDst=false).
	dst := make([]byte, 0, 2048)
	for i := 0; i < k; i++ {
		gotDst, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[i], limits, nil, dst[:0], true, false)
		if !hit || !bytes.Equal(gotDst, want[i]) {
			t.Fatalf("cached subIndex %d (useDst): hit=%t len=%d", i, hit, len(gotDst))
		}
		dst = gotDst[:0]

		gotOwned, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[i], limits, nil, nil, false, false)
		if !hit || !bytes.Equal(gotOwned, want[i]) || len(gotOwned) != cap(gotOwned) {
			t.Fatalf("cached subIndex %d (owned): hit=%t len=%d cap=%d", i, hit, len(gotOwned), cap(gotOwned))
		}
	}
	st := m.GroupedFrameCacheDetailedStats()
	if st.Hits != 2*k {
		t.Fatalf("GroupedFrameCacheDetailedStats().Hits=%d want %d", st.Hits, 2*k)
	}

	// 4b. requireDstCap=true misses when cap(dst) < len(val) and hits when sufficient.
	smallDst := make([]byte, 0, len(want[0])-1)
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, nil, smallDst, true, true); hit {
		t.Fatal("requireDstCap=true hit with insufficient dst capacity")
	}
	fitDst := make([]byte, 0, len(want[0]))
	if gotFit, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, nil, fitDst, true, true); !hit || !bytes.Equal(gotFit, want[0]) {
		t.Fatalf("requireDstCap=true with sufficient dst: hit=%t len=%d", hit, len(gotFit))
	}

	// 5. Wrong FileID, leaf-log FileID, non-grouped ptr, wrong length hint, out-of-range subIndex miss cleanly.
	wrongFilePtr := ptrs[0]
	wrongFilePtr.FileID = fileID + 1
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(wrongFilePtr, limits, nil, nil, false, false); hit {
		t.Fatal("wrong FileID hit cache")
	}
	leafPtr := ptrs[0]
	leafID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	leafPtr.FileID = leafID
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(leafPtr, limits, nil, nil, false, false); hit {
		t.Fatal("leaf-log FileID hit value cache")
	}
	nonGroupedPtr := ptrs[0]
	nonGroupedPtr.Length = page.ValuePtrRecordLength(ptrs[0])
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(nonGroupedPtr, limits, nil, nil, false, false); hit {
		t.Fatal("non-grouped pointer hit grouped cache")
	}
	badHintPtr := ptrs[0]
	badHintPtr.Length = page.ValuePtrMarkGrouped(page.ValuePtrRecordLength(ptrs[0])+1, 0)
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(badHintPtr, limits, nil, nil, false, false); hit {
		t.Fatal("mismatched record-length hint hit cache")
	}
	oobSubPtr := ptrs[0]
	oobSubPtr.Length = page.ValuePtrMarkGrouped(page.ValuePtrRecordLength(ptrs[0]), k)
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(oobSubPtr, limits, nil, nil, false, false); hit {
		t.Fatal("out-of-range subIndex hit cache")
	}

	// 6. Stricter COWReadLimits miss cleanly (MaxRecordBytes, MaxRawBytes, MaxValueBytes).
	tightRec := COWReadLimits{MaxRecordBytes: shape0.RecordBytes - 1, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], tightRec, nil, nil, false, false); hit {
		t.Fatal("tight MaxRecordBytes hit cache")
	}
	tightRaw := COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: shape0.RawBytes - 1, MaxValueBytes: 1 << 20}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], tightRaw, nil, nil, false, false); hit {
		t.Fatal("tight MaxRawBytes hit cache")
	}
	tightVal := COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: shape0.ValueBytes - 1}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], tightVal, nil, nil, false, false); hit {
		t.Fatal("tight MaxValueBytes hit cache")
	}

	// 7. checkShape refusal misses cleanly.
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, func(COWRecordShape) bool { return false }, nil, false, false); hit {
		t.Fatal("checkShape refusal hit cache")
	}

	// 8. Dictionary credit dominance + stricter credit preservation on racing broader store.
	dictShape := shape0
	dictShape.DictID = 42
	dictShape.Codec = BlockCodecNone
	dictShape.limits = COWReadLimits{MaxRecordBytes: 512 << 10, MaxRawBytes: 512 << 10, MaxValueBytes: 512 << 10}
	if !f.groupedFrameCacheStoreCOWVerified(int64(ptrs[0].Offset-4), dictShape, plainOffsets, append([]byte(nil), plainRaw...), false) {
		t.Fatal("dict COW store failed")
	}
	// A racing broader-limit reader storing the same dict frame must not downgrade the 512 KiB credit.
	broaderDictShape := dictShape
	broaderDictShape.limits = COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: 2 << 20, MaxValueBytes: 2 << 20}
	if !f.groupedFrameCacheStoreCOWVerified(int64(ptrs[0].Offset-4), broaderDictShape, plainOffsets, append([]byte(nil), plainRaw...), false) {
		t.Fatal("broader dict COW store failed")
	}
	stricterThanDict := COWReadLimits{MaxRecordBytes: 256 << 10, MaxRawBytes: 256 << 10, MaxValueBytes: 256 << 10}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], stricterThanDict, nil, nil, false, false); hit {
		t.Fatal("stricter caller limits accepted broader dict credit")
	}
	exactDictLimits := COWReadLimits{MaxRecordBytes: 512 << 10, MaxRawBytes: 512 << 10, MaxValueBytes: 512 << 10}
	if gotDict, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], exactDictLimits, nil, nil, false, false); !hit || !bytes.Equal(gotDict, want[0]) {
		t.Fatalf("preserved 512 KiB dict credit missed: hit=%t len=%d", hit, len(gotDict))
	}

	// 9. Template-encoded and compact-leaf payloads in a cached frame miss cleanly so canonical path refuses.
	templPayload, err := templ.EncodePayload(7, [][]byte{[]byte("gap")})
	if err != nil {
		t.Fatal(err)
	}
	var templOffsets [MaxFrameK + 1]uint32
	templOffsets[0] = 0
	templOffsets[1] = uint32(len(templPayload))
	templShape := shape0
	templShape.frame.K = 1
	templShape.RawBytes = uint64(len(templPayload))
	templShape.ValueBytes = uint64(len(templPayload))
	if !f.groupedFrameCacheStoreCOWVerified(int64(ptrs[0].Offset-4), templShape, templOffsets, append([]byte(nil), templPayload...), false) {
		t.Fatal("template COW store failed")
	}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, nil, nil, false, false); hit {
		t.Fatal("template payload hit COW cache")
	}

	compactLeaf := make([]byte, compactLeafPagePayloadHeaderSize+16)
	copy(compactLeaf, compactLeafPagePayloadMagic[:])
	binary.LittleEndian.PutUint16(compactLeaf[len(compactLeafPagePayloadMagic):], 8)
	binary.LittleEndian.PutUint16(compactLeaf[len(compactLeafPagePayloadMagic)+2:], 8)
	var compactOffsets [MaxFrameK + 1]uint32
	compactOffsets[0] = 0
	compactOffsets[1] = uint32(len(compactLeaf))
	compactShape := shape0
	compactShape.frame.K = 1
	compactShape.RawBytes = uint64(len(compactLeaf))
	compactShape.ValueBytes = uint64(len(compactLeaf))
	if !f.groupedFrameCacheStoreCOWVerified(int64(ptrs[0].Offset-4), compactShape, compactOffsets, compactLeaf, false) {
		t.Fatal("compact-leaf COW store failed")
	}
	if _, hit := f.CopyVerifiedCOWGroupedValueCached(ptrs[0], limits, nil, nil, false, false); hit {
		t.Fatal("compact-leaf payload hit COW value cache")
	}
}

func TestGroupedFrameCache_CrossFileAndCrossShardEviction(t *testing.T) {
	dir := t.TempDir()
	id1, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := EncodeFileID(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []struct {
		id   uint32
		name string
	}{
		{id1, "value-l0-000001.log"},
		{id2, "value-l0-000002.log"},
	} {
		w, err := NewWriter(filepath.Join(dir, spec.name), spec.id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.AppendFrame(0, nil, []Record{{RID: 1, Value: bytes.Repeat([]byte{'a'}, 64)}}); err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	// Budget fits exactly 4 frames of 4096 bytes across ALL files and shards combined.
	const frameBytes = 4096
	m.SetGroupedFrameCacheEntries(64)
	m.SetGroupedFrameCacheMaxBytes(4 * frameBytes)

	f1 := m.files[id1]
	f2 := m.files[id2]
	offsets := groupedCacheOffsets(frameBytes)

	// 1. Fill the manager budget completely with file 1 across distinct shards,
	// then store more frames into OTHER cold shards of the SAME file 1.
	for i := 0; i < 8; i++ {
		raw := bytes.Repeat([]byte{byte('1' + i)}, frameBytes)
		if !f1.groupedFrameCacheStore(int64(100+i*100), true, 1, offsets, raw, false) {
			t.Fatalf("same-file cross-shard store %d starved under saturated budget", i)
		}
	}
	if st := m.GroupedFrameCacheDetailedStats(); st.RetainedBytes != 4*frameBytes || st.Entries != 4 {
		t.Fatalf("after file1 cross-shard fill: %+v", st)
	}

	// 2. File 2 must evict stale frames from file 1 rather than starving forever.
	for i := 0; i < 4; i++ {
		raw := bytes.Repeat([]byte{byte('A' + i)}, frameBytes)
		if !f2.groupedFrameCacheStore(int64(200+i*100), true, 1, offsets, raw, false) {
			t.Fatalf("file2 store %d starved under saturated peer budget", i)
		}
	}
	st := m.GroupedFrameCacheDetailedStats()
	if st.RetainedBytes > st.BudgetBytes {
		t.Fatalf("retained bytes %d exceeded budget %d", st.RetainedBytes, st.BudgetBytes)
	}
	if f2st := f2.groupedFrameCacheDetailedStats(); f2st.Entries == 0 {
		t.Fatalf("file2 cached 0 entries despite peer eviction: %+v", f2st)
	}

	// 3. Concurrent cross-file + cross-shard stores/reads/evictions must be race-free and deadlock-free.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			target := f1
			if id%2 == 1 {
				target = f2
			}
			for j := 0; j < 64; j++ {
				start := int64(1000 + ((id*17+j)%16)*64)
				raw := bytes.Repeat([]byte{byte(id + j)}, frameBytes)
				target.groupedFrameCacheStore(start, true, 1, offsets, raw, false)
				_, _, _, _ = target.groupedFrameCacheReadTo(start, true, 1, &offsets, frameBytes, 0, nil)
			}
		}(g)
	}
	wg.Wait()
	if st := m.GroupedFrameCacheDetailedStats(); st.RetainedBytes > st.BudgetBytes {
		t.Fatalf("post-concurrency retained bytes %d exceeded budget %d", st.RetainedBytes, st.BudgetBytes)
	}
}
