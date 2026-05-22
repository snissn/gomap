package colgranule

import (
	"encoding/binary"
	crc32 "github.com/snissn/go-crc32-asm"
	"strings"
	"testing"
	"time"
)

func TestTCS1ColumnPartAssetRoundTripsThroughMemoryStore(t *testing.T) {
	_, image := testColumnPartImageFixture(t, true)
	store := NewMemoryColumnAssetStore()
	ref, record, err := StoreTCS1ColumnPartImage(store, image)
	if err != nil {
		t.Fatalf("StoreTCS1ColumnPartImage: %v", err)
	}
	if record.PayloadBytes != image.TotalBytes() {
		t.Fatalf("payload bytes=%d want image bytes=%d", record.PayloadBytes, image.TotalBytes())
	}
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		t.Fatalf("asset kind=%s want %s", ref.Kind, ColumnAssetKindTCS1PartImage)
	}
	imagePart, loaded, err := ColumnPartFromTCS1Asset(store, ref)
	if err != nil {
		t.Fatalf("ColumnPartFromTCS1Asset: %v", err)
	}
	if loaded.PartID != image.PartID || loaded.Rows != image.Rows || loaded.PayloadBytes != image.TotalBytes() {
		t.Fatalf("loaded record=%+v image part/rows/bytes=(%d,%d,%d)", loaded, image.PartID, image.Rows, image.TotalBytes())
	}
	scan, err := imagePart.NewScanner().ScanProjected([]string{"id", "time_us", "value", "kind_code", "has_reply"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5})
	assertInt64s(t, "time_us", scan.Columns["time_us"], []int64{10, 20, 30, 40, 50})
	assertInt64s(t, "value", scan.Columns["value"], []int64{100, 200, 300, 400, 500})
	assertInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{0, 1, 1, 0, 2})
	assertInt64s(t, "has_reply", scan.Columns["has_reply"], []int64{0, 1, 1, 1, 0})
}

func TestMemoryColumnAssetStoreValidatesChecksumAfterLookup(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("payload"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ref.Checksum++
	if _, err := store.Read(ref); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Read err=%v want checksum mismatch", err)
	}
}

func TestMemoryColumnAssetStoreVerifyRecomputesChecksum(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	payload := []byte("payload")
	ref, err := store.PutOwned(ColumnAssetKindTCS1PartImage, payload)
	if err != nil {
		t.Fatalf("PutOwned: %v", err)
	}
	payload[0] = 'P'
	if err := store.Verify(ref); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Verify err=%v want checksum mismatch", err)
	}
}

func TestMemoryColumnAssetStoreValidatesLengthAfterAddressLookup(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("payload"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ref.Length--
	if _, err := store.Read(ref); err == nil || !strings.Contains(err.Error(), "asset length") {
		t.Fatalf("Read err=%v want length mismatch", err)
	}
}

func TestColumnAssetStoresRejectUnsupportedKind(t *testing.T) {
	memory := NewMemoryColumnAssetStore()
	ref, err := memory.Put(ColumnAssetKindTCS1PartImage, []byte("payload"))
	if err != nil {
		t.Fatalf("memory Put: %v", err)
	}
	ref.Kind = ColumnAssetKind("future_kind")
	if _, err := memory.Read(ref); err == nil || !strings.Contains(err.Error(), "unsupported column asset kind") {
		t.Fatalf("memory Read err=%v want unsupported kind", err)
	}

	segment, err := OpenSegmentColumnAssetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	defer segment.Close()
	ref, err = segment.Put(ColumnAssetKindTCS1PartImage, []byte("payload"))
	if err != nil {
		t.Fatalf("segment Put: %v", err)
	}
	ref.Kind = ColumnAssetKind("future_kind")
	if _, err := segment.Read(ref); err == nil || !strings.Contains(err.Error(), "unsupported column asset kind") {
		t.Fatalf("segment Read err=%v want unsupported kind", err)
	}
}

func TestTCS1ColumnPartAssetRoundTripsThroughSegmentStoreAfterReopen(t *testing.T) {
	_, image := testColumnPartImageFixture(t, true)
	dir := t.TempDir()
	store, err := OpenSegmentColumnAssetStore(dir)
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	ref, record, err := StoreTCS1ColumnPartImage(store, image)
	if err != nil {
		t.Fatalf("StoreTCS1ColumnPartImage: %v", err)
	}
	if record.AssetRef != ref {
		t.Fatalf("record ref=%+v want %+v", record.AssetRef, ref)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := OpenSegmentColumnAssetStore(dir)
	if err != nil {
		t.Fatalf("reopen segment store: %v", err)
	}
	defer reopened.Close()
	imagePart, loaded, err := ColumnPartFromTCS1Asset(reopened, ref)
	if err != nil {
		t.Fatalf("ColumnPartFromTCS1Asset after reopen: %v", err)
	}
	if loaded.AssetRef != ref {
		t.Fatalf("loaded ref=%+v want %+v", loaded.AssetRef, ref)
	}
	locator, ok := imagePart.LocatePrimaryID(4)
	if !ok {
		t.Fatal("missing locator for primary id 4")
	}
	value, err := imagePart.NewScanner().ValueAt(locator, "value")
	if err != nil {
		t.Fatalf("ValueAt: %v", err)
	}
	if value != 400 {
		t.Fatalf("value at reopened asset row=%d want 400", value)
	}
}

func TestSegmentColumnAssetStoreSyncsNewSegmentDirectoryEntry(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSegmentColumnAssetStore(dir)
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	if !store.dirSyncRequired {
		t.Fatal("new segment store did not require directory sync")
	}
	if _, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if store.dirSyncRequired {
		t.Fatal("segment store still requires directory sync after Sync")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenSegmentColumnAssetStore(dir)
	if err != nil {
		t.Fatalf("reopen segment store: %v", err)
	}
	defer reopened.Close()
	if !reopened.dirSyncRequired {
		t.Fatal("reopened segment store did not conservatively require directory sync")
	}
	if err := reopened.Sync(); err != nil {
		t.Fatalf("reopened Sync: %v", err)
	}
	if reopened.dirSyncRequired {
		t.Fatal("reopened segment store still requires directory sync after Sync")
	}
}

func TestSegmentColumnAssetStoreCloseWaitsForActiveVerifyIO(t *testing.T) {
	store, err := OpenSegmentColumnAssetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("payload"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, _, err := store.beginFileIO(); err != nil {
		t.Fatalf("beginFileIO: %v", err)
	}
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()
	waitForSegmentStoreClosing(t, store)
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before active file IO drained: %v", err)
	default:
	}
	if _, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("late payload")); err == nil || !strings.Contains(err.Error(), "closed segment asset store") {
		t.Fatalf("Put during pending Close err=%v want closed segment asset store", err)
	}
	if err := store.endFileIO(); err != nil {
		t.Fatalf("endFileIO: %v", err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after active file IO drained")
	}
	if err := store.Verify(ref); err == nil || !strings.Contains(err.Error(), "closed segment asset store") {
		t.Fatalf("Verify after Close err=%v want closed segment asset store", err)
	}
}

func TestSegmentColumnAssetStoreEndFileIORejectsWithoutBegin(t *testing.T) {
	store, err := OpenSegmentColumnAssetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	defer store.Close()
	if err := store.endFileIO(); err == nil || !strings.Contains(err.Error(), "without matching begin") {
		t.Fatalf("endFileIO without beginFileIO err=%v want without matching begin", err)
	}
}

func waitForSegmentStoreClosing(t *testing.T, store *SegmentColumnAssetStore) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if segmentStoreClosing(store) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("segment store Close did not enter closing state")
}

func segmentStoreClosing(store *SegmentColumnAssetStore) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.closing
}

func TestSegmentColumnAssetStoreRejectsOutOfRangeRefBeforeAllocation(t *testing.T) {
	store, err := OpenSegmentColumnAssetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	defer store.Close()
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("tiny"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ref.Length = 1 << 62
	if _, err := store.ReadTo(ref, nil); err == nil || !strings.Contains(err.Error(), "outside segment") {
		t.Fatalf("ReadTo err=%v want outside segment", err)
	}
}

func TestSegmentColumnAssetStoreRejectsMaxReadBeforeAllocation(t *testing.T) {
	store, err := OpenSegmentColumnAssetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	defer store.Close()
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, []byte("tiny"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ref.Length = maxColumnAssetReadBytes + 1
	store.size = ref.Length
	if _, err := store.ReadTo(ref, nil); err == nil || !strings.Contains(err.Error(), "exceeds max read bytes") {
		t.Fatalf("ReadTo err=%v want max read size", err)
	}
}

func TestTCS1ColumnPartAssetRejectsCorruption(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	payload, _, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	tests := []struct {
		name string
		edit func([]byte) []byte
		want string
	}{
		{
			name: "magic",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint32(in[tcs1MagicOffset:tcs1VersionOffset], 0)
				return in
			},
			want: "magic",
		},
		{
			name: "version",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint16(in[tcs1VersionOffset:tcs1KindOffset], 99)
				return in
			},
			want: "version",
		},
		{
			name: "flags",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint32(in[tcs1FlagsOffset:tcs1HeaderBytesOffset], 1)
				return in
			},
			want: "flags",
		},
		{
			name: "reserved",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint16(in[tcs1ReservedOffset:tcs1PayloadCRC32Offset], 1)
				return in
			},
			want: "reserved",
		},
		{
			name: "checksum",
			edit: func(in []byte) []byte {
				in[len(in)-1] ^= 1
				return in
			},
			want: "checksum",
		},
		{
			name: "truncated",
			edit: func(in []byte) []byte {
				return in[:len(in)-1]
			},
			want: "payload bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			corrupt := tt.edit(append([]byte(nil), payload...))
			if _, _, err := DecodeTCS1ColumnPartImage(corrupt); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("DecodeTCS1ColumnPartImage err=%v want contains %q", err, tt.want)
			}
		})
	}
}

func TestTCS1ColumnPartAssetRejectsImageHeaderMismatch(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	payload, _, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	binary.LittleEndian.PutUint64(payload[tcs1PartIDOffset:tcs1RowsOffset], image.PartID+1)
	if _, _, err := DecodeTCS1ColumnPartImage(payload); err == nil || !strings.Contains(err.Error(), "part id") {
		t.Fatalf("DecodeTCS1ColumnPartImage err=%v want part id mismatch", err)
	}
}

func TestTCS1ColumnPartAssetRejectsUnknownInnerEncoding(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	descriptor, dec := descriptorDecoderAtFirstColumnFirstBlock(t, image)
	corruptImage := append([]byte(nil), image.Bytes...)
	encodingOffset := descriptor.Offset + dec.offset + 4*8
	binary.LittleEndian.PutUint16(corruptImage[encodingOffset:], 0xffff)

	payload, _, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	copy(payload[tcs1PayloadOffset:], corruptImage)
	binary.LittleEndian.PutUint32(payload[tcs1PayloadCRC32Offset:tcs1PayloadOffset], crc32.ChecksumIEEE(corruptImage))

	store := NewMemoryColumnAssetStore()
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, err := ColumnPartFromTCS1Asset(store, ref); err == nil || !strings.Contains(err.Error(), "unsupported int64 encoding") {
		t.Fatalf("ColumnPartFromTCS1Asset err=%v want unsupported inner encoding", err)
	}
}

func TestTCS1ColumnPartAssetEncodeRejectsImageStructMismatch(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	mismatched := image
	mismatched.PartID++
	if _, _, err := EncodeTCS1ColumnPartImage(mismatched); err == nil || !strings.Contains(err.Error(), "part id") {
		t.Fatalf("EncodeTCS1ColumnPartImage part id err=%v want mismatch", err)
	}
	mismatched = image
	mismatched.Rows++
	if _, _, err := EncodeTCS1ColumnPartImage(mismatched); err == nil || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("EncodeTCS1ColumnPartImage rows err=%v want mismatch", err)
	}
}

func TestJSONBenchPartQueriesRunFromReopenedTCS1Asset(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(sample): %v", err)
	}
	rawTimings, err := RunJSONBenchQueries(ds, 1)
	if err != nil {
		t.Fatalf("RunJSONBenchQueries: %v", err)
	}
	rawByQuery := make(map[string]JSONBenchQueryTiming, len(rawTimings))
	for _, timing := range rawTimings {
		rawByQuery[timing.Query] = timing
	}
	part, err := BuildJSONBenchColumnPart(ds, 2)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	dir := t.TempDir()
	store, err := OpenSegmentColumnAssetStore(dir)
	if err != nil {
		t.Fatalf("OpenSegmentColumnAssetStore: %v", err)
	}
	_, assetRef, _, err := TCS1AssetBackedColumnPart(part, ds.Dictionaries, store)
	if err != nil {
		t.Fatalf("TCS1AssetBackedColumnPart: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopenedStore, err := OpenSegmentColumnAssetStore(dir)
	if err != nil {
		t.Fatalf("reopen segment store: %v", err)
	}
	defer reopenedStore.Close()
	reopenedPart, _, err := ColumnPartFromTCS1AssetWithOptions(reopenedStore, assetRef, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("ColumnPartFromTCS1Asset after reopen: %v", err)
	}
	if len(reopenedPart.Locators) != 0 {
		t.Fatalf("scan-only asset load decoded %d locators", len(reopenedPart.Locators))
	}

	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		run  jsonBenchPartQueryRunner
	}{
		{"Q1", runJSONBenchPartQ1},
		{"Q2", runJSONBenchPartQ2},
		{"Q3", runJSONBenchPartQ3},
		{"Q4", runJSONBenchPartQ4},
		{"Q5", runJSONBenchPartQ5},
	}
	for _, q := range queries {
		scratch := &jsonBenchPartQueryScratch{
			scanner:   reopenedPart.NewScanner(),
			projected: make(map[string][]int64, 6),
		}
		rows, digest, diagnostics, err := q.run(reopenedPart, codes, scratch)
		if err != nil {
			t.Fatalf("%s asset-backed query: %v", q.name, err)
		}
		raw := rawByQuery[q.name]
		if rows != raw.ResultRows || digest != raw.ResultDigest {
			t.Fatalf("%s rows/digest=(%d,%d) raw=(%d,%d)", q.name, rows, digest, raw.ResultRows, raw.ResultDigest)
		}
		if diagnostics.AggregateKernel == "" {
			t.Fatalf("%s missing aggregate kernel diagnostics: %+v", q.name, diagnostics)
		}
	}
}
