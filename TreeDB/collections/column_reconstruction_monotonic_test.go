package collections

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkScanDocumentsFuncMonotonicReconstructionP3887(b *testing.B) {
	rows := 100000
	if raw := os.Getenv("P3887_BENCH_ROWS"); raw != "" {
		var err error
		rows, err = strconv.Atoi(raw)
		if err != nil || rows <= 0 {
			b.Fatalf("P3887_BENCH_ROWS=%q", raw)
		}
	}
	// P3887_BENCH_DIR can select fast storage; an empty base uses the portable
	// default temporary directory.
	base := os.Getenv("P3887_BENCH_DIR")
	dir, err := os.MkdirTemp(base, "gomap3887_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		b.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	// time_us is intentionally a typed-column-part-owned field so this measures
	// selective typed reconstruction, not only compatibility row assets.
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}, {Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true}, {Name: "operation", Path: "operation", ValueType: ColumnStoreValueString, Dictionary: true}, {Name: "collection", Path: "collection", ValueType: ColumnStoreValueString, Dictionary: true}, {Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true}}}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		b.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		b.Fatal(err)
	}
	ids, docs := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("e%09d", i))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"commit","operation":"create","collection":"app.bsky.feed.post","did":"did:%09d"}`, i, i))
	}
	const insertBatchRows = 100000
	for start := 0; start < rows; start += insertBatchRows {
		end := min(start+insertBatchRows, rows)
		if _, err := col.InsertBatch(ids[start:end], docs[start:end]); err != nil {
			b.Fatal(err)
		}
	}
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	d, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	col, err = NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(rows), "rows/op")
	b.ResetTimer()
	var scanStats CollectionDocumentScanStats
	for i := 0; i < b.N; i++ {
		count := 0
		truncated, err := col.ScanDocumentsFunc(rows, func(DocumentRecord) (bool, error) { count++; return true, nil })
		if err != nil || truncated || count != rows {
			b.Fatalf("scan err=%v truncated=%t rows=%d", err, truncated, count)
		}
		scanStats = col.LastDocumentScanStats()
		if !scanStats.CertifiedMonotonicPath || scanStats.GenericFallback || scanStats.PhysicalPasses != 2 || scanStats.PhysicalRows != uint64(rows*2) || scanStats.PhysicalDecodedBlocks != 2*uint64((rows+insertBatchRows-1)/insertBatchRows) || scanStats.MaxRecordWindow > uint64(columnReconstructionMonotonicBatchSize) || scanStats.MaxVisibleRowWindow > uint64(columnReconstructionMonotonicBatchSize) || scanStats.MaxTypedGenerations > 1 || scanStats.MaxRetainedBlocks > uint64(defaultColumnRetainedSemanticStreamV1DecodeCacheBlocks) {
			b.Fatalf("scan stats=%+v", scanStats)
		}
	}
	b.ReportMetric(float64(scanStats.MaxRecordWindow), "max_records/window")
	b.ReportMetric(float64(scanStats.MaxVisibleRowWindow), "max_visible_rows/window")
	b.ReportMetric(float64(scanStats.MaxTypedGenerations), "max_typed_generations/resident")
	b.ReportMetric(float64(scanStats.MaxTypedDecodedBytes), "max_typed_decoded_bytes/window")
	b.ReportMetric(float64(scanStats.MaxTypedSourcePartBytes), "max_typed_source_bytes/window")
	b.ReportMetric(float64(scanStats.MaxRetainedBlocks), "max_retained_blocks/scan")
	b.ReportMetric(float64(scanStats.PhysicalPasses), "physical_passes/op")
	b.ReportMetric(float64(scanStats.PhysicalDecodedBlocks), "physical_decoded_blocks/op")
}

func TestScanDocumentsFuncMonotonicReconstructionReopenCanonicalParityP3887(t *testing.T) {
	rows := 529
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{ID: fmt.Sprintf("e%04d", i), TimeUS: int64(i), Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: fmt.Sprintf("did:%04d", i)}
	}
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	want := sha256.New()
	for _, event := range events {
		fmt.Fprintf(want, "%s\x00{\"time_us\":%d,\"kind\":%q,\"operation\":%q,\"collection\":%q,\"did\":%q}\n", event.ID, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did)
	}
	got := sha256.New()
	count := 0
	truncated, err := col.ScanDocumentsFunc(rows, func(record DocumentRecord) (bool, error) {
		count++
		got.Write(record.ID)
		got.Write([]byte{0})
		got.Write(record.Document)
		got.Write([]byte{'\n'})
		return true, nil
	})
	if err != nil || truncated || count != rows {
		t.Fatalf("scan err=%v truncated=%t rows=%d want nil/false/%d", err, truncated, count, rows)
	}
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatalf("canonical reconstructed sha256=%x want %x", got.Sum(nil), want.Sum(nil))
	}
}

func TestScanDocumentsFuncMonotonicReconstructionCallbackAndTruncationP3887(t *testing.T) {
	events := make([]columnPhysicalJSONBenchParityEventP0, 4)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{ID: fmt.Sprintf("e%04d", i), TimeUS: int64(i), Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: fmt.Sprintf("did:%04d", i)}
	}
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	for _, tc := range []struct {
		name          string
		limit         int
		stopAt        int
		wantTruncated bool
	}{
		{name: "below", limit: 3, wantTruncated: true}, {name: "equal", limit: 4, wantTruncated: false}, {name: "above", limit: 5, wantTruncated: false}, {name: "early_false", limit: 4, stopAt: 2, wantTruncated: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			truncated, err := col.ScanDocumentsFunc(tc.limit, func(DocumentRecord) (bool, error) { calls++; return tc.stopAt == 0 || calls < tc.stopAt, nil })
			if err != nil || truncated != tc.wantTruncated {
				t.Fatalf("err=%v truncated=%t want nil/%t", err, truncated, tc.wantTruncated)
			}
			if tc.stopAt != 0 && calls != tc.stopAt {
				t.Fatalf("calls=%d want %d", calls, tc.stopAt)
			}
		})
	}
	wantErr := errors.New("callback error")
	_, err := col.ScanDocumentsFunc(4, func(DocumentRecord) (bool, error) { return false, wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("callback err=%v want %v", err, wantErr)
	}
	if stats := col.LastDocumentScanStats(); stats.ReconstructedRows != 1 {
		t.Fatalf("callback-error reconstructed rows=%d want 1", stats.ReconstructedRows)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionBoundsPreflightP3887(t *testing.T) {
	events := make([]columnPhysicalJSONBenchParityEventP0, 5)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{ID: fmt.Sprintf("e%04d", i), TimeUS: int64(i), Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: fmt.Sprintf("did:%04d", i)}
	}
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	var ids []string
	truncated, err := col.ScanDocumentsFunc(1, func(record DocumentRecord) (bool, error) {
		ids = append(ids, string(record.ID))
		return true, nil
	})
	if err != nil || !truncated || fmt.Sprint(ids) != "[e0000]" {
		t.Fatalf("scan err=%v truncated=%t ids=%v", err, truncated, ids)
	}
	if stats := col.LastDocumentScanStats(); stats.CertifiedMonotonicPath || !stats.GenericFallback || stats.PhysicalRows != 2 || stats.PhysicalRows >= uint64(len(events)) {
		t.Fatalf("scan stats=%+v want bounded preflight and honest generic result", stats)
	}
}

func TestMonotonicColumnReconstructionPreflightClassifierP3887(t *testing.T) {
	row := func(id string) columnPhysicalScanRowView {
		return columnPhysicalScanRowView{ID: []byte(id), Generation: 1, PartID: 1, Operation: ColumnPublishOperationInsert}
	}
	for _, tc := range []struct {
		name     string
		physical []columnPhysicalScanRowView
		primary  [][]byte
		wantCert bool
		wantErr  string
	}{
		{name: "ordered", physical: []columnPhysicalScanRowView{row("a"), row("b")}, primary: [][]byte{[]byte("a"), []byte("b")}, wantCert: true},
		{name: "shuffled_fallback", physical: []columnPhysicalScanRowView{row("b"), row("a")}, primary: [][]byte{[]byte("a"), []byte("b")}},
		{name: "duplicate_fail_closed", physical: []columnPhysicalScanRowView{row("a"), row("a")}, primary: [][]byte{[]byte("a"), []byte("a")}, wantErr: "duplicate"},
		{name: "orphan_fail_closed", physical: []columnPhysicalScanRowView{row("a"), row("b")}, primary: [][]byte{[]byte("a")}, wantErr: "mismatch"},
		{name: "missing_fail_closed", physical: []columnPhysicalScanRowView{row("a")}, primary: [][]byte{[]byte("a"), []byte("b")}, wantErr: "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := monotonicColumnReconstructionPreflight{eligible: true}
			for i, physical := range tc.physical {
				if i < len(tc.primary) {
					state.observe(physical, tc.primary[i], true)
				} else {
					state.observe(physical, nil, false)
				}
			}
			certified, err := state.finish(len(tc.primary) > len(tc.physical))
			if tc.wantErr != "" {
				if err == nil || !bytes.Contains([]byte(err.Error()), []byte(tc.wantErr)) {
					t.Fatalf("err=%v want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || certified != tc.wantCert {
				t.Fatalf("certified=%t err=%v want %t/nil", certified, err, tc.wantCert)
			}
		})
	}
}

func TestScanDocumentsFuncMonotonicReconstructionWindowBoundariesP3887(t *testing.T) {
	rows := columnReconstructionMonotonicBatchSize*2 + 17
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{ID: fmt.Sprintf("e%04d", i), TimeUS: int64(i), Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: fmt.Sprintf("did:%04d", i)}
	}
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	for _, limit := range []int{columnReconstructionMonotonicBatchSize - 1, columnReconstructionMonotonicBatchSize, columnReconstructionMonotonicBatchSize + 1} {
		calls := 0
		truncated, err := col.ScanDocumentsFunc(limit, func(DocumentRecord) (bool, error) { calls++; return true, nil })
		if err != nil || !truncated || calls != limit {
			t.Fatalf("limit=%d err=%v truncated=%t calls=%d", limit, err, truncated, calls)
		}
		if stats := col.LastDocumentScanStats(); stats.PhysicalRows != uint64(limit+1) || stats.CertifiedMonotonicPath || !stats.GenericFallback {
			t.Fatalf("limit=%d scan stats=%+v want bounded generic fallback", limit, stats)
		}
	}
	partialCalls := 0
	truncated, err := col.ScanDocumentsFunc(17, func(DocumentRecord) (bool, error) {
		partialCalls++
		return partialCalls < 3, nil
	})
	if err != nil || truncated || partialCalls != 3 {
		t.Fatalf("partial callback err=%v truncated=%t calls=%d", err, truncated, partialCalls)
	}
	for _, stopAt := range []int{1, columnReconstructionMonotonicBatchSize, rows} {
		calls := 0
		truncated, err := col.ScanDocumentsFunc(rows, func(DocumentRecord) (bool, error) { calls++; return calls < stopAt, nil })
		if err != nil || truncated || calls != stopAt {
			t.Fatalf("stopAt=%d err=%v truncated=%t calls=%d", stopAt, err, truncated, calls)
		}
		calls = 0
		wantErr := fmt.Errorf("stop-%d", stopAt)
		_, err = col.ScanDocumentsFunc(rows, func(DocumentRecord) (bool, error) {
			calls++
			if calls == stopAt {
				return false, wantErr
			}
			return true, nil
		})
		if !errors.Is(err, wantErr) || calls != stopAt {
			t.Fatalf("error stopAt=%d err=%v calls=%d", stopAt, err, calls)
		}
	}
}

func TestScanDocumentsFuncMonotonicReconstructionUpdateFallsBackP3887(t *testing.T) {
	events := []columnPhysicalJSONBenchParityEventP0{{ID: "e0000", TimeUS: 1, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:old"}}
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	if _, err := col.Replace([]byte("e0000"), []byte(`{"time_us":2,"kind":"commit","operation":"create","collection":"app.bsky.feed.post","did":"did:new"}`)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got []byte
	_, err := col.ScanDocumentsFunc(1, func(record DocumentRecord) (bool, error) {
		got = append([]byte(nil), record.Document...)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanDocumentsFunc: %v", err)
	}
	if stats := col.LastDocumentScanStats(); stats.CertifiedMonotonicPath || !stats.GenericFallback || stats.PhysicalPasses != 0 || stats.LocatorLookupBatches != 1 || stats.LocatorLookups != 1 || stats.PointRowFetches != 1 {
		t.Fatalf("scan stats=%+v want generic fallback", stats)
	}
	if want := `"did":"did:new"`; !bytes.Contains(got, []byte(want)) {
		t.Fatalf("reconstructed document=%s want %s", got, want)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionShuffledGenerationsFallBackP3887(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}}}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e0002")}, [][]byte{[]byte(`{"time_us":2}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e0001")}, [][]byte{[]byte(`{"time_us":1}`)}); err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	var ids []string
	truncated, err := col.ScanDocumentsFunc(2, func(record DocumentRecord) (bool, error) {
		callbacks++
		ids = append(ids, string(record.ID))
		return true, nil
	})
	if err != nil || truncated || fmt.Sprint(ids) != "[e0001 e0002]" {
		t.Fatalf("scan err=%v truncated=%t ids=%v", err, truncated, ids)
	}
	if stats := col.LastDocumentScanStats(); stats.CertifiedMonotonicPath || !stats.GenericFallback || callbacks != 2 {
		t.Fatalf("scan stats=%+v callbacks=%d want generic fallback", stats, callbacks)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionBoundsWindowsP3887(t *testing.T) {
	rows := columnReconstructionMonotonicBatchSize*2 + 17
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{ID: fmt.Sprintf("e%04d", i), TimeUS: int64(i), Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: fmt.Sprintf("did:%04d", i)}
	}
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	var ids []string
	truncated, err := col.ScanDocumentsFunc(rows, func(record DocumentRecord) (bool, error) { ids = append(ids, string(record.ID)); return true, nil })
	if err != nil || truncated || len(ids) != rows {
		t.Fatalf("scan err=%v truncated=%t rows=%d want nil/false/%d", err, truncated, len(ids), rows)
	}
	for i, id := range ids {
		if id != events[i].ID {
			t.Fatalf("callback order id[%d]=%q want %q", i, id, events[i].ID)
		}
	}
	if stats := col.LastDocumentScanStats(); !stats.CertifiedMonotonicPath || stats.GenericFallback || stats.PhysicalPasses != 2 || stats.PhysicalRows != uint64(rows*2) || stats.PhysicalDecodedBlocks != 2 || stats.PreflightProjectedColumns != 0 || stats.MaxRecordWindow != columnReconstructionMonotonicBatchSize || stats.MaxVisibleRowWindow != columnReconstructionMonotonicBatchSize || stats.ReconstructedRows != uint64(rows) {
		t.Fatalf("scan stats=%+v want certified bounded reconstruction", stats)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionSelectiveTypedColumnP3887(t *testing.T) {
	const rows = columnReconstructionMonotonicBatchSize + 3
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true, Owner: TypedStorageOwnerColumnPart},
	}}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("e%04d", i))
		kind := "generation-one"
		if i >= 200 {
			kind = "generation-two"
		}
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q}`, i, kind))
	}
	// Cross the 256-row reconstruction window with two generations: resident
	// typed state must stay at one part even when a window sees both.
	if _, err := col.InsertBatch(ids[:200], docs[:200]); err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch(ids[200:], docs[200:]); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	col, err = NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	truncated, err := col.ScanDocumentsFunc(rows, func(record DocumentRecord) (bool, error) {
		got = append(got, string(record.Document))
		return true, nil
	})
	if err != nil || truncated || len(got) != rows {
		t.Fatalf("scan err=%v truncated=%t rows=%d", err, truncated, len(got))
	}
	for i, document := range got {
		kind := "generation-one"
		if i >= 200 {
			kind = "generation-two"
		}
		want := fmt.Sprintf(`{"time_us":%d,"kind":%q}`, i, kind)
		if document != want {
			t.Fatalf("document[%d]=%s want %s", i, document, want)
		}
	}
	stats := col.LastDocumentScanStats()
	if !stats.CertifiedMonotonicPath || stats.MaxTypedGenerations != 1 || stats.MaxTypedDecodedBytes == 0 || stats.MaxTypedSourcePartBytes == 0 {
		t.Fatalf("scan stats=%+v want bounded selective typed reconstruction", stats)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionTypedSortKeyFallsBackP3887(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	cfg := &ColumnStoreConfig{Enabled: true,
		Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e0000"), []byte("e0001")}, [][]byte{[]byte(`{"time_us":2}`), []byte(`{"time_us":1}`)}); err != nil {
		t.Fatal(err)
	}
	var got []DocumentRecord
	truncated, err := col.ScanDocumentsFunc(2, func(record DocumentRecord) (bool, error) {
		got = append(got, record)
		return true, nil
	})
	if err != nil || truncated || len(got) != 2 {
		t.Fatalf("scan err=%v truncated=%t rows=%d", err, truncated, len(got))
	}
	if string(got[0].ID) != "e0000" || string(got[1].ID) != "e0001" {
		t.Fatalf("IDs=%q,%q want e0000,e0001", got[0].ID, got[1].ID)
	}
	assertJSONMapEqual1875(t, got[0].Document, map[string]any{"time_us": float64(2)})
	assertJSONMapEqual1875(t, got[1].Document, map[string]any{"time_us": float64(1)})
	stats := col.LastDocumentScanStats()
	if stats.CertifiedMonotonicPath || !stats.GenericFallback || stats.PhysicalPasses != 0 || stats.LocatorLookupBatches != 1 || stats.LocatorLookups != 2 || stats.PointRowFetches != 2 || stats.PhysicalRows != 0 {
		t.Fatalf("scan stats=%+v want generic fallback for typed sort-key rows", stats)
	}
}

func TestScanDocumentsFuncGenericLocatorReconstructionBoundsMultiAssetBatchesP3890(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:      "time_us",
			Path:      "time_us",
			ValueType: ColumnStoreValueInt64,
			Owner:     TypedStorageOwnerColumnPart,
		}},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}

	const (
		rows      = 513
		batchRows = 171
	)
	for start := 0; start < rows; start += batchRows {
		end := start + batchRows
		ids := make([][]byte, 0, end-start)
		docs := make([][]byte, 0, end-start)
		for i := start; i < end; i++ {
			ids = append(ids, []byte(fmt.Sprintf("e%04d", i)))
			docs = append(docs, []byte(fmt.Sprintf(`{"time_us":%d,"marker":%d}`, rows-i, i)))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("InsertBatch start=%d: %v", start, err)
		}
	}

	seen := 0
	truncated, err := col.ScanDocumentsFunc(rows, func(record DocumentRecord) (bool, error) {
		wantID := fmt.Sprintf("e%04d", seen)
		if string(record.ID) != wantID {
			return false, fmt.Errorf("row %d id=%q want %q", seen, record.ID, wantID)
		}
		seen++
		return true, nil
	})
	if err != nil || truncated || seen != rows {
		t.Fatalf("scan err=%v truncated=%t rows=%d want %d", err, truncated, seen, rows)
	}
	stats := col.LastDocumentScanStats()
	if stats.CertifiedMonotonicPath || !stats.GenericFallback || stats.PhysicalPasses != 0 ||
		stats.LocatorLookupBatches != 3 || stats.LocatorLookups != rows ||
		stats.PointRowFetches != rows || stats.MaxRecordWindow != 256 {
		t.Fatalf("scan stats=%+v want three bounded locator windows across immutable assets", stats)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionUnsupportedSelectedTypedFallsBackP3887(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "maybe_kind", Path: "maybe_kind", ValueType: ColumnStoreValueString, Dictionary: true, Nullable: true, Owner: TypedStorageOwnerColumnPart},
		{Name: "tags", Path: "tags", ValueType: ColumnStoreValueUint32List, Owner: TypedStorageOwnerColumnPart},
	}}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{
		[]byte(`{"maybe_kind":"present","tags":[1,2],"retained":"first"}`),
		[]byte(`{"tags":[3],"retained":"second"}`),
	}); err != nil {
		t.Fatal(err)
	}
	var got []DocumentRecord
	truncated, err := col.ScanDocumentsFunc(2, func(record DocumentRecord) (bool, error) {
		got = append(got, record)
		return true, nil
	})
	if err != nil || truncated || len(got) != 2 {
		t.Fatalf("scan err=%v truncated=%t rows=%d", err, truncated, len(got))
	}
	assertJSONMapEqual1875(t, got[0].Document, map[string]any{"maybe_kind": "present", "tags": []any{float64(1), float64(2)}, "retained": "first"})
	assertJSONMapEqual1875(t, got[1].Document, map[string]any{"tags": []any{float64(3)}, "retained": "second"})
	stats := col.LastDocumentScanStats()
	if stats.CertifiedMonotonicPath || !stats.GenericFallback || stats.PhysicalPasses != 0 || stats.LocatorLookupBatches != 1 || stats.LocatorLookups != 2 || stats.PointRowFetches != 2 {
		t.Fatalf("scan stats=%+v want generic fallback before selected-row typed decoding", stats)
	}
}

func TestScanDocumentsFuncMonotonicReconstructionBoundsRetainedSemanticCacheP3887(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(columnReconstructionMonotonicBatchSize + 3)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	count := 0
	truncated, err := col.ScanDocumentsFunc(len(ids), func(record DocumentRecord) (bool, error) {
		count++
		return true, nil
	})
	if err != nil || truncated || count != len(ids) {
		t.Fatalf("scan err=%v truncated=%t rows=%d", err, truncated, count)
	}
	stats := col.LastDocumentScanStats()
	if stats.MaxRetainedBlocks == 0 || stats.MaxRetainedBlocks > uint64(defaultColumnRetainedSemanticStreamV1DecodeCacheBlocks) {
		t.Fatalf("scan stats=%+v want retained cache within %d blocks", stats, defaultColumnRetainedSemanticStreamV1DecodeCacheBlocks)
	}
}
