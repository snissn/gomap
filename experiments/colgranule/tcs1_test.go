package colgranule

import (
	"encoding/binary"
	"strings"
	"testing"
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
				binary.LittleEndian.PutUint32(in[0:4], 0)
				return in
			},
			want: "magic",
		},
		{
			name: "version",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint16(in[4:6], 99)
				return in
			},
			want: "version",
		},
		{
			name: "flags",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint32(in[8:12], 1)
				return in
			},
			want: "flags",
		},
		{
			name: "reserved",
			edit: func(in []byte) []byte {
				binary.LittleEndian.PutUint16(in[42:44], 1)
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
	binary.LittleEndian.PutUint64(payload[24:32], image.PartID+1)
	if _, _, err := DecodeTCS1ColumnPartImage(payload); err == nil || !strings.Contains(err.Error(), "part id") {
		t.Fatalf("DecodeTCS1ColumnPartImage err=%v want part id mismatch", err)
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
	reopenedPart, _, err := ColumnPartFromTCS1Asset(reopenedStore, assetRef)
	if err != nil {
		t.Fatalf("ColumnPartFromTCS1Asset after reopen: %v", err)
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
