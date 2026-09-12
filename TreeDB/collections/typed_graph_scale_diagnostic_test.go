package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type scaleDatasetManifest struct {
	Rows, Dimensions int
	QueryCount       int    `json:"query_count"`
	DocumentsSHA     string `json:"documents_sha256"`
	QueriesSHA       string `json:"queries_sha256"`
	TruthSHA         string `json:"truth_sha256"`
}

// This opt-in diagnostic uses real 768D Cohere vectors exported by
// benchmarks/vector_db_compare/prepare_cohere_scale.py. It measures the public
// collections interface, not transport or document materialization. Run build
// and measure separately so construction cannot contaminate query timings.
func TestTypedGraphScaleDiagnostic(t *testing.T) {
	data, dir, phase := os.Getenv("TREEDB_SCALE_DATA"), os.Getenv("TREEDB_SCALE_DB"), os.Getenv("TREEDB_SCALE_PHASE")
	if data == "" || dir == "" || phase == "" {
		t.Skip("opt-in: set TREEDB_SCALE_DATA, TREEDB_SCALE_DB, TREEDB_SCALE_PHASE=build|measure|write|write_schema|write_schema_roots")
	}
	if phase != "build" && phase != "measure" && phase != "write" && phase != "write_schema" && phase != "write_schema_roots" {
		t.Fatal("phase must be build, measure, write, write_schema, or write_schema_roots")
	}
	if phase == "write_schema_roots" && os.Getenv("TREEDB_SCALE_PRECEDING_RUN") == "" {
		t.Fatal("write_schema_roots requires TREEDB_SCALE_PRECEDING_RUN naming the completed eight-call schema run")
	}
	if (phase == "write" || phase == "write_schema" || phase == "write_schema_roots") && os.Getenv("TREEDB_SCALE_WRITE_COPY") != "1" {
		t.Fatal("write phases require TREEDB_SCALE_WRITE_COPY=1 acknowledging a disposable DB copy")
	}
	var manifest scaleDatasetManifest
	scaleReadJSON(t, filepath.Join(data, "manifest.json"), &manifest)
	if manifest.Dimensions != 768 || manifest.Rows < 4097 || manifest.Rows > 1000000 || manifest.QueryCount < 1 || manifest.QueryCount > 10000 {
		t.Fatal("invalid dataset manifest")
	}
	for a, b := manifest.Rows, 7919; b != 0; {
		a, b = b, a%b
		if b == 0 && a != 1 {
			t.Fatal("row count must be coprime to7919 for exact filter cardinalities")
		}
	}
	for _, input := range []struct {
		name, digest string
		size         int64
	}{{"documents.f32", manifest.DocumentsSHA, int64(manifest.Rows) * 768 * 4}, {"queries.f32", manifest.QueriesSHA, int64(manifest.QueryCount) * 768 * 4}, {"truth.json", manifest.TruthSHA, -1}} {
		if err := scaleVerifyFile(filepath.Join(data, input.name), input.size, input.digest); err != nil {
			t.Fatal(err)
		}
	}
	if phase != "build" {
		var ready scaleDatasetManifest
		scaleReadJSON(t, filepath.Join(dir, "scale-ready.json"), &ready)
		if ready.DocumentsSHA != manifest.DocumentsSHA || ready.Rows != manifest.Rows || ready.Dimensions != manifest.Dimensions {
			t.Fatal("DB fixture dataset digest or shape mismatch")
		}
	}
	emit := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(b))
	}
	emit(map[string]any{"phase": "provenance", "label": os.Getenv("TREEDB_SCALE_LABEL"), "source_commit": os.Getenv("TREEDB_SCALE_SOURCE_COMMIT"), "data": data, "db": dir, "go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "documents_sha256": manifest.DocumentsSHA})
	if phase == "build" {
		if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatalf("build requires a new DB directory; refusing to reuse existing evidence: %v", err)
		}
		if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
			t.Fatal(err)
		}
	}
	db := openTypedMinimaDB(t, dir)
	defer db.Close()
	manager := NewCollectionManager(db)
	if phase == "build" {
		meta := typedMinimaCollectionMeta()
		meta.Options.ColumnStore.Columns[0].VectorDims = 768
		meta.VectorIndexes[0].Dimensions, meta.VectorIndexes[0].M, meta.VectorIndexes[0].EfConstruction = 768, 16, 128
		if _, err := manager.CreateCollection(&meta); err != nil {
			t.Fatal(err)
		}
	}
	col, err := manager.OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	defer col.CloseVectorIndexPreparedSearchCache()
	if phase == "build" {
		file, err := os.Open(filepath.Join(data, "documents.f32"))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		started := time.Now()
		for start := 0; start < manifest.Rows; start += 1024 {
			n := min(1024, manifest.Rows-start)
			vectors := make([]float32, n*768)
			if err := binary.Read(file, binary.LittleEndian, vectors); err != nil {
				t.Fatal(err)
			}
			ids, payload := make([][]byte, n), make([][]byte, n)
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, n)}, {Name: "content", Strings: make([]string, n)}, {Name: "user", Strings: make([]string, n)}, {Name: "path", Strings: make([]string, n)}}
			for i := range n {
				row := start + i
				ids[i] = []byte(fmt.Sprintf("row-%06d", row))
				payload[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
				columns[0].Float32Vectors[i] = vectors[i*768 : (i+1)*768]
				columns[1].Strings[i] = "content"
				columns[2].Strings[i] = fmt.Sprintf("%06d", (row*7919)%manifest.Rows)
				columns[3].Strings[i] = "source"
			}
			if _, _, err := col.InsertTypedBatchWithStats(ids, payload, columns); err != nil {
				t.Fatal(err)
			}
			if start%51200 == 0 {
				emit(map[string]any{"phase": "ingest", "rows": start + n, "elapsed_ms": float64(time.Since(started).Microseconds()) / 1000})
			}
		}
		emit(map[string]any{"phase": "ingest_complete", "rows": manifest.Rows, "elapsed_ms": float64(time.Since(started).Microseconds()) / 1000})
		started = time.Now()
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		emit(map[string]any{"phase": "rebuild_complete", "elapsed_ms": float64(time.Since(started).Microseconds()) / 1000})
		ready, _ := json.Marshal(manifest)
		if err := os.WriteFile(filepath.Join(dir, "scale-ready.json"), ready, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	queriesRaw, err := os.ReadFile(filepath.Join(data, "queries.f32"))
	if err != nil {
		t.Fatal(err)
	}
	if len(queriesRaw) != manifest.QueryCount*768*4 {
		t.Fatal("query shape mismatch")
	}
	queries := make([][]float32, manifest.QueryCount)
	for q := range queries {
		queries[q] = make([]float32, 768)
		for d := range 768 {
			queries[q][d] = math.Float32frombits(binary.LittleEndian.Uint32(queriesRaw[(q*768+d)*4:]))
		}
	}
	var truth map[string][][]string
	scaleReadJSON(t, filepath.Join(data, "truth.json"), &truth)
	if err := scaleValidateTruth(manifest, truth); err != nil {
		t.Fatal(err)
	}
	opts := typedGraphPublicTestOptions()
	opts.Publication = ColumnGraphPublicationLimits{Rows: 4096, Tombstones: 4096, ValueSlots: 16384, OwnedBytes: 128 << 20, EncodedOutputBytes: 128 << 20}
	opts.Owners = ColumnGraphReadOwnerLimits{Owners: 16, States: 16, StateBytes: 2 << 30, AssetBytes: 16 << 30, Cold: ColumnGraphColdLimits{ManifestRecords: 32768, ManifestBytes: 64 << 20, AssetBytes: 8 << 30, DecodedTermBytes: 2 << 30}}
	opts.Filter = ColumnGraphFilterLimits{SourceIDs: manifest.Rows + 4096, SourceBytes: 64 << 20, RetainedBytes: 64 << 20, MappingWork: 128 << 20, InspectedEntries: manifest.Rows * 4}
	opts.CandidateOutput.Bytes = 8 << 30
	opts.FoldRows = manifest.Rows + 4096
	opts.SearchCandidates = manifest.Rows + 4096
	opts.Maintenance = ColumnGraphMaintenanceLimits{NativeEntries: 32768, ColumnSegments: 32768, ManifestRecords: 32768, LifecycleEntries: 32768, NativeBytes: 8 << 30, ColumnBytes: 8 << 30, ManifestBytes: 64 << 20, RetainedBytes: 2 << 30, PagerPages: 1 << 20}
	started := time.Now()
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", opts); err != nil {
		t.Fatal(err)
	}
	emit(map[string]any{"phase": "ensure", "elapsed_ms": float64(time.Since(started).Microseconds()) / 1000, "rows": manifest.Rows, "dimensions": 768, "queries": len(queries), "options": opts})
	if phase == "write" || phase == "write_schema" || phase == "write_schema_roots" {
		scaleConcurrentWrites(t, col, manifest.Rows, data, queries, phase, emit)
		return
	}
	counts := make([]int, 0, len(truth))
	for count := range truth {
		n, err := strconv.Atoi(count)
		if err != nil {
			t.Fatal(err)
		}
		counts = append(counts, n)
	}
	slices.Sort(counts)
	filterFor := func(lower, count int) *HybridScalarFilter {
		return &HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: fmt.Sprintf("%06d", lower), Inclusive: true}, Upper: IndexRangeBound{Value: fmt.Sprintf("%06d", lower+count-1), Inclusive: true}}}
	}
	type sample struct {
		Micros    float64              `json:"us"`
		Hits      int                  `json:"hits"`
		AcquireNS int64                `json:"acquire_ns"`
		Work      ColumnGraphQueryWork `json:"work"`
	}
	curve := func(label string, count int, run func(q, ef int) ([]VectorIndexSearchResult, ColumnGraphQueryWork, int64, error)) {
		for _, ef := range []int{128, 256, 512, 1024, 2048} {
			for q := range min(3, len(queries)) {
				if _, _, _, err := run(q, ef); err != nil {
					t.Fatal(err)
				}
			}
			samples := make([]sample, 0, len(queries))
			total, minHits := 0, 10
			for q := range queries {
				started := time.Now()
				results, work, acquire, err := run(q, ef)
				elapsed := time.Since(started)
				if err != nil || len(results) != 10 {
					t.Fatalf("%s eligible=%d ef=%d q=%d results=%d err=%v", label, count, ef, q, len(results), err)
				}
				if work.Filter.Attempted && (!work.Filter.Completed || work.Filter.EligibleRows != uint64(count)) {
					t.Fatalf("filter cardinality mismatch: want=%d work=%+v", count, work.Filter)
				}
				hits := 0
				for _, r := range results {
					if slices.Contains(truth[strconv.Itoa(count)][q], string(r.ID)) {
						hits++
					}
				}
				total += hits
				minHits = min(minHits, hits)
				samples = append(samples, sample{float64(elapsed.Nanoseconds()) / 1000, hits, acquire, work})
			}
			latencies := make([]float64, len(samples))
			for i, s := range samples {
				latencies[i] = s.Micros
			}
			slices.Sort(latencies)
			emit(map[string]any{"phase": label, "eligible": count, "ef": ef, "top_k": 10, "query_count": len(queries), "recall": float64(total) / float64(len(queries)*10), "min_recall": float64(minHits) / 10, "p50_us": latencies[len(latencies)/2], "p95_us": latencies[(len(latencies)-1)*95/100], "samples": samples})
		}
	}
	public := func(filter *HybridScalarFilter) func(int, int) ([]VectorIndexSearchResult, ColumnGraphQueryWork, int64, error) {
		var buffer VectorIndexSearchBuffer
		return func(q, ef int) ([]VectorIndexSearchResult, ColumnGraphQueryWork, int64, error) {
			response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: queries[q], TopK: 10, EfSearch: ef, DeclaredScalarFilter: filter, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
			if view != nil {
				if closeErr := view.Close(); err == nil {
					err = closeErr
				}
			}
			return response.Results, response.Stats.ColumnGraphWork, response.Stats.ColumnGraphOwnerAcquireNanos, err
		}
	}
	unfiltered := public(nil)
	started = time.Now()
	_, coldWork, _, err := unfiltered(0, 128)
	if err != nil {
		t.Fatal(err)
	}
	emit(map[string]any{"phase": "public_cold_unfiltered", "elapsed_us": float64(time.Since(started).Nanoseconds()) / 1000, "work": coldWork})
	curve("public_unfiltered", manifest.Rows, unfiltered)
	for _, count := range counts {
		filter := filterFor(0, count)
		owner, err := col.openTypedGraphReadOwner(opts.Owners)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		plan, err := prepareTypedGraphFilter(owner.overlay, *filter, opts.Filter)
		if err != nil {
			t.Fatal(err)
		}
		emit(map[string]any{"phase": "uncached_filter_prepare", "eligible": count, "elapsed_us": float64(time.Since(started).Nanoseconds()) / 1000, "work": plan.work(true)})
		var buffer VectorIndexSearchBuffer
		curve("prepared_global", count, func(q, ef int) ([]VectorIndexSearchResult, ColumnGraphQueryWork, int64, error) {
			results, stats, err := owner.overlay.searchPreparedFilter(plan, queries[q], 10, ef, opts.SearchCandidates, &buffer)
			return results, stats.work(), 0, err
		})
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		run := public(filter)
		started = time.Now()
		_, work, _, err := run(0, 512)
		if err != nil {
			t.Fatal(err)
		}
		emit(map[string]any{"phase": "public_cold_filter", "eligible": count, "ef": 512, "elapsed_us": float64(time.Since(started).Nanoseconds()) / 1000, "work": work})
		curve("public_warm_filter", count, run)
	}
	// Twelve distinct predicates expose the existing keeper's eight-entry limit.
	for i := range 12 {
		lower := i * 4097
		if lower+4097 > manifest.Rows {
			break
		}
		run := public(filterFor(lower, 4097))
		for repeat := range 2 {
			started := time.Now()
			_, work, _, err := run(0, 512)
			if err != nil {
				t.Fatal(err)
			}
			emit(map[string]any{"phase": "distinct_filter", "predicate": i, "repeat": repeat, "elapsed_us": float64(time.Since(started).Nanoseconds()) / 1000, "work": work})
		}
	}
}

// Run only after pristine baseline/candidate searches, on a separate DB copy.
// Updating content preserves vectors and scalar labels, hence exact truth.
// The same 256 IDs bound the live replacement set, not retained physical
// mutation history; subsequent writes still consume publication budgets.
func scaleConcurrentWrites(t *testing.T, col *Collection, rows int, data string, queries [][]float32, phase string, emit func(any)) {
	t.Helper()
	file, err := os.Open(filepath.Join(data, "documents.f32"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	vectors := make([]float32, 256*768)
	if err := binary.Read(file, binary.LittleEndian, vectors); err != nil {
		t.Fatal(err)
	}
	ids, payload := make([][]byte, 256), make([][]byte, 256)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%06d", i))
		payload[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
	}
	filter := &HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "000000", Inclusive: true}, Upper: IndexRangeBound{Value: "004096", Inclusive: true}}}
	var buffer VectorIndexSearchBuffer
	type readSample struct {
		US           float64              `json:"us"`
		AcquireNS    int64                `json:"acquire_ns"`
		WriterActive bool                 `json:"writer_active"`
		Work         ColumnGraphQueryWork `json:"work"`
	}
	read := func(q int, active bool) readSample {
		q %= len(queries)
		started := time.Now()
		response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: queries[q], TopK: 10, EfSearch: 512, DeclaredScalarFilter: filter, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
		if view != nil {
			if closeErr := view.Close(); err == nil {
				err = closeErr
			}
		}
		elapsed := time.Since(started)
		if err != nil || len(response.Results) != 10 {
			t.Fatalf("concurrent read: results=%d err=%v", len(response.Results), err)
		}
		// ANN recall is reported in the search curve; the concurrency gate checks
		// all returned IDs remain eligible and the coherent route completes.
		if !response.Stats.ColumnGraphWork.Completed || response.Stats.ColumnGraphWork.Filter.EligibleRows != 4097 {
			t.Fatalf("incoherent read work=%+v", response.Stats.ColumnGraphWork)
		}
		for _, r := range response.Results {
			row, err := scaleDocumentRow(string(r.ID), rows)
			if err != nil || (row*7919)%rows >= 4097 {
				t.Fatalf("ineligible result %q", r.ID)
			}
		}
		return readSample{float64(elapsed.Nanoseconds()) / 1000, response.Stats.ColumnGraphOwnerAcquireNanos, active, response.Stats.ColumnGraphWork}
	}
	for i := range 3 {
		read(i, false)
	}
	baseline := make([]readSample, 100)
	for i := range baseline {
		baseline[i] = read(i, false)
	}
	emit(map[string]any{"phase": "write_readonly", "samples": baseline})
	var schemaCreation time.Duration
	if phase != "write" {
		// Metadata-only schema creation does not attach a collection root. The
		// separate text-v2 phase exercises cold root attachment after the prior
		// eight-call run; neither changes the large collection's vector labels.
		meta := CollectionMeta{Name: "scale_aux", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}
		if phase == "write_schema_roots" {
			meta.Name = "scale_aux_text_v2"
			meta.TextIndexes = typedMinimaCollectionMeta().TextIndexes
		}
		started := time.Now()
		if _, alreadyExisted, err := NewCollectionManager(col.db).CreateCollectionWithPreparedCommandWALIntentStatus(meta, nil); err != nil {
			t.Fatal(err)
		} else if alreadyExisted {
			t.Fatalf("auxiliary schema %q already exists; refusing to reuse setup", meta.Name)
		}
		schemaCreation = time.Since(started)
	}
	emit(map[string]any{"phase": "write_state", "db_reopened": true, "unrelated_schema_creation": phase != "write", "schema_creates_text_v2_roots": phase == "write_schema_roots", "schema_creation_ns": schemaCreation.Nanoseconds(), "preceding_run": os.Getenv("TREEDB_SCALE_PRECEDING_RUN"), "after_prior_eight_replacements": phase == "write_schema_roots"})
	var done atomic.Bool
	writeResult := make(chan error, 1)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer done.Store(true)
		for batch := range 8 {
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, 256)}, {Name: "content", Strings: make([]string, 256)}, {Name: "user", Strings: make([]string, 256)}, {Name: "path", Strings: make([]string, 256)}}
			for i := range ids {
				columns[0].Float32Vectors[i] = vectors[i*768 : (i+1)*768]
				columns[1].Strings[i] = fmt.Sprintf("content update%d", batch)
				columns[2].Strings[i] = fmt.Sprintf("%06d", (i*7919)%rows)
				columns[3].Strings[i] = "source"
			}
			before := col.db.Stats()["treedb.command_wal.file_sync.calls_total"]
			started := time.Now()
			_, stats, err := col.UpsertTypedBatchWithStats(ids, payload, columns)
			elapsed := time.Since(started)
			emit(map[string]any{"phase": "concurrent_write", "batch": batch, "rows": 256, "wall_ns": elapsed.Nanoseconds(), "stats": stats, "wal_syncs_before": before, "wal_syncs_after": col.db.Stats()["treedb.command_wal.file_sync.calls_total"]})
			if err != nil {
				writeResult <- err
				return
			}
		}
		writeResult <- nil
	}()
	defer func() { <-writerDone }()
	var concurrent []readSample
	for q := 0; q < 100 || !done.Load(); q++ {
		concurrent = append(concurrent, read(q, !done.Load()))
	}
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	emit(map[string]any{"phase": "write_concurrent_reads", "samples": concurrent})
}

func scaleReadJSON(t testing.TB, path string, value any) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 16<<20 {
		t.Fatalf("oversized diagnostic JSON: %s", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, value); err != nil {
		t.Fatal(err)
	}
}

func scaleVerifyFile(path string, size int64, want string) error {
	digest, err := hex.DecodeString(want)
	if err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("invalid SHA256 for %s", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || (size >= 0 && info.Size() != size) || (size < 0 && info.Size() > 16<<20) {
		return fmt.Errorf("invalid dataset file shape: %s", path)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != want {
		return fmt.Errorf("dataset SHA256 mismatch: %s", path)
	}
	return nil
}

func scaleDocumentRow(id string, rows int) (int, error) {
	if len(id) != len("row-000000") || id[:4] != "row-" {
		return 0, fmt.Errorf("invalid diagnostic row ID %q", id)
	}
	row, err := strconv.Atoi(id[4:])
	if err != nil || row < 0 || row >= rows || fmt.Sprintf("row-%06d", row) != id {
		return 0, fmt.Errorf("invalid diagnostic row ID %q", id)
	}
	return row, nil
}

func scaleValidateTruth(manifest scaleDatasetManifest, truth map[string][][]string) error {
	counts := []int{4096, 4097, max(4097, manifest.Rows/100), max(4097, manifest.Rows/10), manifest.Rows}
	slices.Sort(counts)
	counts = slices.Compact(counts)
	if len(truth) != len(counts) {
		return fmt.Errorf("truth predicate count mismatch")
	}
	for _, count := range counts {
		queries := truth[strconv.Itoa(count)]
		if len(queries) != manifest.QueryCount {
			return fmt.Errorf("truth query count mismatch: eligible=%d", count)
		}
		for _, ids := range queries {
			if len(ids) != 10 {
				return fmt.Errorf("truth requires ten IDs per query")
			}
			for i, id := range ids {
				row, err := scaleDocumentRow(id, manifest.Rows)
				if err != nil || (row*7919)%manifest.Rows >= count || slices.Contains(ids[:i], id) {
					return fmt.Errorf("invalid, duplicate, or ineligible truth ID %q", id)
				}
			}
		}
	}
	return nil
}

func TestTypedGraphScaleDiagnosticFileValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors.f32")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("fixture"))
	for _, tc := range []struct {
		name, digest string
		size         int64
		valid        bool
	}{{"valid", hex.EncodeToString(digest[:]), 7, true}, {"truncated", hex.EncodeToString(digest[:]), 8, false}, {"missing_digest", "", 7, false}, {"wrong_digest", fmt.Sprintf("%064d", 0), 7, false}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := scaleVerifyFile(path, tc.size, tc.digest); (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
		})
	}
	manifest := scaleDatasetManifest{Rows: 4097, QueryCount: 1}
	truth := map[string][][]string{"4096": {{}}, "4097": {{}}}
	for row := 0; len(truth["4096"][0]) < 10; row++ {
		if row*7919%manifest.Rows < 4096 {
			id := fmt.Sprintf("row-%06d", row)
			truth["4096"][0] = append(truth["4096"][0], id)
			truth["4097"][0] = append(truth["4097"][0], id)
		}
	}
	if err := scaleValidateTruth(manifest, truth); err != nil {
		t.Fatal(err)
	}
	truth["4096"][0][1] = truth["4096"][0][0]
	if err := scaleValidateTruth(manifest, truth); err == nil {
		t.Fatal("duplicate truth accepted")
	}
	if _, err := scaleDocumentRow("x", manifest.Rows); err == nil {
		t.Fatal("short result ID accepted")
	}
}
