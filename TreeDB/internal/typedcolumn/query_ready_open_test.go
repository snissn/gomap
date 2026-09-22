package typedcolumn

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

func TestQueryReadyDeltaGenerationFileOpenIsMappedAndCloseIdempotent(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	delta, err := OpenQueryReadyDeltaGenerationFile(files.Deltas[0].Path, files.Deltas[0].Identity)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Base == nil || !delta.Base.Stats.Mapped || delta.Base.Stats.BytesMapped != int64(len(delta.Bytes())) {
		t.Fatalf("mapped delta stats=%+v bytes=%d", delta.Base.Stats, len(delta.Bytes()))
	}
	if err := delta.Close(); err != nil {
		t.Fatal(err)
	}
	if err := delta.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if delta.Bytes() != nil || delta.Base.Bytes() != nil {
		t.Fatal("close retained mapped byte views")
	}
}

func TestQueryReadyGenerationOpenMapsExactSegmentRanges(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	baseBytes := mustReadQueryReadyOpenFile(t, files.Base.Path)
	deltaBytes := mustReadQueryReadyOpenFile(t, files.Deltas[0].Path)
	prefix := make([]byte, 123)
	gap := make([]byte, 37)
	suffix := make([]byte, 91)
	segment := append(prefix, baseBytes...)
	deltaOffset := len(segment) + len(gap)
	segment = append(segment, gap...)
	segment = append(segment, deltaBytes...)
	segment = append(segment, suffix...)
	path := filepath.Join(t.TempDir(), "query-ready.segment")
	if err := os.WriteFile(path, segment, 0o600); err != nil {
		t.Fatal(err)
	}
	files.Base.Path, files.Base.Offset, files.Base.Length = path, int64(len(prefix)), int64(len(baseBytes))
	files.Deltas[0].Path, files.Deltas[0].Offset, files.Deltas[0].Length = path, int64(deltaOffset), int64(len(deltaBytes))
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	prepared, err := cache.Open(files)
	if err != nil {
		t.Fatalf("open ranged segment: %v", err)
	}
	if prepared.PartCount() != 2 {
		t.Fatalf("parts=%d", prepared.PartCount())
	}
	stats := cache.Stats()
	if stats.MappedFiles != 2 || stats.MappedBytes < int64(len(baseBytes)+len(deltaBytes)) {
		t.Fatalf("stats=%+v logical=%d", stats, len(baseBytes)+len(deltaBytes))
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryReadyGenerationFileRangeRejectsOutOfBoundsAndWrongSlice(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	data := mustReadQueryReadyOpenFile(t, files.Deltas[0].Path)
	path := filepath.Join(t.TempDir(), "delta.segment")
	prefix := make([]byte, 19)
	segment := append(prefix, data...)
	if err := os.WriteFile(path, segment, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name           string
		offset, length int64
	}{
		{name: "past-end", offset: int64(len(segment) + 1), length: 1},
		{name: "cross-end", offset: int64(len(prefix)), length: int64(len(data) + 1)},
		{name: "negative", offset: -1, length: int64(len(data))},
		{name: "wrong-slice", offset: int64(len(prefix) + 1), length: int64(len(data) - 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opened, err := OpenQueryReadyDeltaGenerationFileRange(path, tc.offset, tc.length, files.Deltas[0].Identity)
			if err == nil {
				_ = opened.Close()
				t.Fatal("expected range rejection")
			}
		})
	}
}

func TestQueryReadyGenerationOpenIsIndependentOfFirstQuery(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	orders := [][]string{
		{"shape-q1", "shape-q2", "shape-q3", "shape-q4", "shape-q5", "shape-qexpr"},
		{"shape-qexpr", "shape-q5", "shape-q3", "shape-q1", "shape-q4", "shape-q2"},
		{"shape-q3", "shape-q1", "shape-qexpr", "shape-q2", "shape-q5", "shape-q4"},
	}
	var want queryReadyOpenTestResult
	var wantStats QueryReadyGenerationOpenStats
	for i, order := range orders {
		cache := NewQueryReadyGenerationOpenCache(files.Key)
		prepared, err := cache.Open(files)
		if err != nil {
			t.Fatalf("order %d open: %v", i, err)
		}
		got := queryReadyOpenRunOrder(t, prepared, order)
		stats := cache.Stats()
		if i == 0 {
			want, wantStats = got, stats
		} else {
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("order %v result=%+v want %+v", order, got, want)
			}
			stats.OpenTime, stats.ValidationTime = 0, 0
			wantStats.OpenTime, wantStats.ValidationTime = 0, 0
			if !reflect.DeepEqual(stats, wantStats) {
				t.Fatalf("order %v stats=%+v want %+v", order, stats, wantStats)
			}
		}
		if gotStats := cache.Stats(); gotStats.PayloadBytesDecoded != stats.PayloadBytesDecoded || gotStats.DomainsConstructed != stats.DomainsConstructed || gotStats.RanksConstructed != stats.RanksConstructed || gotStats.OffsetsConstructed != stats.OffsetsConstructed {
			t.Fatalf("query mutated construction counters: before=%+v after=%+v", stats, gotStats)
		}
		if err := cache.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

func TestQueryReadyGenerationOpenReusesImmutableStateAcrossReaders(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })
	const readers = 24
	got := make([]*QueryReadyPreparedGeneration, readers)
	errCh := make(chan error, readers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var err error
			got[i], err = cache.Open(files)
			if err != nil {
				errCh <- err
				return
			}
			part, ok := got[i].Part(1)
			if !ok || part.Role != PartRoleDelta || part.Generation != 2 {
				errCh <- errors.New("unexpected concurrent direct part view")
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 1; i < len(got); i++ {
		if got[i] != got[0] {
			t.Fatalf("reader %d prepared=%p want shared %p", i, got[i], got[0])
		}
	}
	stats := cache.Stats()
	if stats.ColdOpens != 1 || stats.Published != 1 || stats.CacheHits != readers-1 {
		t.Fatalf("stats=%+v want one cold open, one publication, and cache reuse", stats)
	}
	if stats.State != QueryReadyOpenReady {
		t.Fatalf("state=%s", stats.State)
	}
}

func TestQueryReadyGenerationWarmOpenEnforcesStricterBound(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })
	prepared, err := cache.Open(files)
	if err != nil {
		t.Fatal(err)
	}

	strict := files
	strict.Bound = QueryReadyDeltaBoundPolicy{MaxRows: 1}
	if got, err := cache.Open(strict); err == nil || got != nil {
		t.Fatalf("warm strict open prepared=%p err=%v want bound rejection", got, err)
	} else {
		var boundErr *QueryReadyDeltaBoundError
		if !errors.As(err, &boundErr) || boundErr.Phase != "open" || !boundErr.Decision.Triggered {
			t.Fatalf("err=%v want open bound decision", err)
		}
		var openErr *QueryReadyGenerationOpenError
		if !errors.As(err, &openErr) || openErr.State != QueryReadyOpenUnsupportedOrStale {
			t.Fatalf("err=%v want unsupported/stale request", err)
		}
	}
	if cache.Stats().State != QueryReadyOpenReady {
		t.Fatalf("valid cached generation was poisoned: %+v", cache.Stats())
	}
	got, err := cache.Open(files)
	if err != nil || got != prepared {
		t.Fatalf("permissive reopen prepared=%p want=%p err=%v", got, prepared, err)
	}
}

func TestQueryReadyGenerationColdBoundMissDoesNotPoisonPermissiveReopen(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })

	strict := files
	strict.Bound = QueryReadyDeltaBoundPolicy{MaxRows: 1}
	if got, err := cache.Open(strict); err == nil || got != nil {
		t.Fatalf("cold strict open prepared=%p err=%v want bound rejection", got, err)
	} else {
		var boundErr *QueryReadyDeltaBoundError
		if !errors.As(err, &boundErr) || boundErr.Phase != "open" || !boundErr.Decision.Triggered {
			t.Fatalf("err=%v want open bound decision", err)
		}
	}
	if state := cache.Stats().State; state != QueryReadyOpenAbsentRebuildable {
		t.Fatalf("cold caller-local bound miss poisoned cache state=%s", state)
	}
	prepared, err := cache.Open(files)
	if err != nil || prepared == nil {
		t.Fatalf("permissive reopen prepared=%p err=%v", prepared, err)
	}
	stats := cache.Stats()
	if stats.State != QueryReadyOpenReady || stats.OpenAttempts != 2 || stats.ColdOpens != 1 || stats.Published != 1 {
		t.Fatalf("stats=%+v want retried cold open and one publication", stats)
	}
}

func TestQueryReadyGenerationOpenRejectsStaleSchemaOrGeneration(t *testing.T) {
	files := queryReadyOpenTestFiles(t)
	for _, mutate := range []func(*QueryReadyGenerationOpenFiles){
		func(f *QueryReadyGenerationOpenFiles) { f.Base.Identity.Generation++ },
		func(f *QueryReadyGenerationOpenFiles) {
			f.Base.Identity.SchemaHash = sha256.Sum256([]byte("other-schema"))
		},
		func(f *QueryReadyGenerationOpenFiles) { f.Key.ManifestHash = sha256.Sum256([]byte("other-manifest")) },
	} {
		bad := files
		bad.Deltas = append([]QueryReadyGenerationFile(nil), files.Deltas...)
		mutate(&bad)
		cache := NewQueryReadyGenerationOpenCache(files.Key)
		if _, err := cache.Open(bad); err == nil {
			t.Fatal("expected stale identity rejection")
		} else {
			var openErr *QueryReadyGenerationOpenError
			if !errors.As(err, &openErr) || openErr.State != QueryReadyOpenUnsupportedOrStale {
				t.Fatalf("err=%v want unsupported/stale", err)
			}
		}
		if state := cache.Stats().State; state != QueryReadyOpenUnsupportedOrStale {
			t.Fatalf("state=%s", state)
		}
	}
}

func TestQueryReadyGenerationOpenRejectsSnapshotGenerationOverclaim(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	files.Key.Identity.Generation++
	files.SnapshotGeneration = files.Key.Identity.Generation
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })

	prepared, err := cache.Open(files)
	if err == nil || prepared != nil {
		t.Fatalf("prepared=%p err=%v want selected-prefix generation overclaim rejection", prepared, err)
	}
	var openErr *QueryReadyGenerationOpenError
	if !errors.As(err, &openErr) || openErr.State != QueryReadyOpenUnsupportedOrStale {
		t.Fatalf("err=%v want unsupported/stale generation overclaim", err)
	}
	if stats := cache.Stats(); stats.Published != 0 || stats.State != QueryReadyOpenUnsupportedOrStale {
		t.Fatalf("stats=%+v want fail-closed without publication", stats)
	}
}

func TestQueryReadyGenerationOpenDoesNotDecodeWholePartsAfterReady(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })
	prepared, err := cache.Open(files)
	if err != nil {
		t.Fatal(err)
	}
	before := cache.Stats()
	if before.PartsDecoded != prepared.PartCount() || before.PayloadBytesCopied != 0 || before.DomainsConstructed != 0 || before.RanksConstructed != 0 || before.OffsetsConstructed != 0 || before.WholePartDecodesDuringOpen != 0 || before.WholePartDecodesAfterOpen != 0 {
		t.Fatalf("open stats=%+v", before)
	}
	for i := 0; i < 100; i++ {
		queryReadyOpenRunOrder(t, prepared, []string{"shape-q5", "shape-q3", "shape-qexpr", "shape-q1", "shape-q2", "shape-q4"})
	}
	after := cache.Stats()
	if after.PayloadBytesDecoded != before.PayloadBytesDecoded || after.PayloadBytesCopied != before.PayloadBytesCopied || after.DomainsConstructed != before.DomainsConstructed || after.RanksConstructed != before.RanksConstructed || after.OffsetsConstructed != before.OffsetsConstructed || after.WholePartDecodesAfterOpen != 0 {
		t.Fatalf("query-time reconstruction: before=%+v after=%+v", before, after)
	}
}

func TestQueryReadyGenerationOpenFailureDoesNotPublishPartialState(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	corrupt := append([]byte(nil), mustReadQueryReadyOpenFile(t, files.Deltas[0].Path)...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := os.WriteFile(files.Deltas[0].Path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	if prepared, err := cache.Open(files); err == nil || prepared != nil {
		t.Fatalf("prepared=%p err=%v want atomic failure", prepared, err)
	}
	stats := cache.Stats()
	if stats.State != QueryReadyOpenCorrupt || stats.Published != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if _, err := cache.Open(files); err == nil {
		t.Fatal("corrupt state must remain fail-closed")
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("partial cleanup close: %v", err)
	}
}

func TestQueryReadyGenerationOpenAbsentIsRebuildable(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	deltaBytes := mustReadQueryReadyOpenFile(t, files.Deltas[0].Path)
	if err := os.Remove(files.Deltas[0].Path); err != nil {
		t.Fatal(err)
	}
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })
	if _, err := cache.Open(files); err == nil {
		t.Fatal("expected absent asset")
	} else {
		var openErr *QueryReadyGenerationOpenError
		if !errors.As(err, &openErr) || openErr.State != QueryReadyOpenAbsentRebuildable {
			t.Fatalf("err=%v", err)
		}
	}
	if err := os.WriteFile(files.Deltas[0].Path, deltaBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Open(files); err != nil {
		t.Fatalf("reopen after rebuild: %v", err)
	}
	stats := cache.Stats()
	if stats.State != QueryReadyOpenReady || stats.Rebuilds != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestQueryReadyGenerationOpenConcurrentReadersRaceFree(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	files := queryReadyOpenTestFiles(t)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	t.Cleanup(func() { _ = cache.Close() })
	prepared, err := cache.Open(files)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				queryReadyOpenRunOrder(t, prepared, []string{"shape-q1", "shape-q2", "shape-q3", "shape-q4", "shape-q5", "shape-qexpr"})
			}
		}()
	}
	wg.Wait()
}

type queryReadyOpenTestResult struct {
	Generation uint64
	Parts      int
	Deltas     int
	Tombstones int
	Rows       int
	Schema     [sha256.Size]byte
}

func queryReadyOpenRunOrder(t testing.TB, prepared *QueryReadyPreparedGeneration, order []string) queryReadyOpenTestResult {
	t.Helper()
	var result queryReadyOpenTestResult
	for _, query := range order {
		switch query {
		case "shape-q1":
			result.Generation = prepared.Key().Identity.Generation
		case "shape-q2":
			result.Parts = prepared.PartCount()
		case "shape-q3":
			result.Deltas = prepared.DeltaCount()
		case "shape-q4":
			result.Tombstones = prepared.TombstoneCount()
		case "shape-q5":
			for i := 0; i < prepared.PartCount(); i++ {
				part, ok := prepared.Part(i)
				if !ok {
					t.Fatalf("part %d absent", i)
				}
				result.Rows += part.View.Dependency.Rows
			}
		case "shape-qexpr":
			result.Schema = prepared.Key().Identity.SchemaHash
		default:
			t.Fatalf("unknown query %q", query)
		}
	}
	return result
}

func queryReadyOpenTestFiles(t *testing.T) QueryReadyGenerationOpenFiles {
	t.Helper()
	dir := t.TempDir()
	schema := sha256.Sum256([]byte("query-ready-open-schema"))
	baseIdentity := QueryReadyBaseIdentity{Generation: 1, SchemaHash: schema}
	baseImage := queryReadyDeltaTestImage(t, 7101, map[int64]int64{1: 10, 2: 20})
	base, err := BuildQueryReadyBaseGeneration(baseIdentity, []QueryReadyBasePartInput{{SourceGeneration: 1, Image: baseImage}})
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	basePath := filepath.Join(dir, "base.qrbg")
	if err := os.WriteFile(basePath, base.Bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	deltaIdentity := QueryReadyBaseIdentity{Generation: 2, SchemaHash: schema}
	deltaImage := queryReadyDeltaTestImage(t, 7102, map[int64]int64{2: 22, 3: 30})
	delta, err := BuildQueryReadyDeltaGeneration(deltaIdentity, []QueryReadyBasePartInput{{SourceGeneration: 2, Image: deltaImage}}, []Tombstone{{PrimaryID: 1, GenerationID: 2}})
	if err != nil {
		t.Fatalf("build delta: %v", err)
	}
	deltaPath := filepath.Join(dir, "delta.qrdg")
	if err := os.WriteFile(deltaPath, delta.Bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return QueryReadyGenerationOpenFiles{
		Key:                QueryReadyGenerationOpenKey{Identity: deltaIdentity, ManifestHash: sha256.Sum256([]byte("manifest-generation-2"))},
		Base:               QueryReadyGenerationFile{Path: basePath, Identity: baseIdentity, Kind: QueryReadyGenerationBase},
		Deltas:             []QueryReadyGenerationFile{{Path: deltaPath, Identity: deltaIdentity, Kind: QueryReadyGenerationDelta}},
		SnapshotGeneration: 2,
		Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8},
	}
}

func mustReadQueryReadyOpenFile(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func BenchmarkQueryReadyGenerationColdOpen(b *testing.B) {
	files := queryReadyOpenBenchmarkFiles(b)
	sample := NewQueryReadyGenerationOpenCache(files.Key)
	if _, err := sample.Open(files); err != nil {
		b.Fatal(err)
	}
	stats := sample.Stats()
	_ = sample.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache := NewQueryReadyGenerationOpenCache(files.Key)
		prepared, err := cache.Open(files)
		if err != nil || prepared == nil {
			b.Fatalf("open: prepared=%p err=%v", prepared, err)
		}
		if err := cache.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(stats.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(stats.PayloadBytesDecoded), "payload_decoded_bytes/op")
	b.ReportMetric(float64(stats.PayloadBytesCopied), "payload_copied_bytes/op")
	b.ReportMetric(float64(stats.WholePartDecodesDuringOpen), "whole_part_decodes/op")
}

func BenchmarkQueryReadyGenerationWarmOpen(b *testing.B) {
	files := queryReadyOpenBenchmarkFiles(b)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	want, err := cache.Open(files)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cache.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := cache.Open(files)
		if err != nil || got != want {
			b.Fatalf("warm open=%p want %p err=%v", got, want, err)
		}
	}
}

// These access-order probes exercise the M3 direct-view surface only. They are
// named after downstream JSONBench shapes for routing clarity, but they are not
// canonical q1-q5/qexpr operators or correctness/performance evidence for M4.
func BenchmarkQueryReadyGenerationDirectViewAccessOrders(b *testing.B) {
	files := queryReadyOpenBenchmarkFiles(b)
	orders := map[string][]string{
		"shape-q1-probe-first":    {"shape-q1", "shape-q2", "shape-q3", "shape-q4", "shape-q5", "shape-qexpr"},
		"shape-q2-probe-first":    {"shape-q2", "shape-q4", "shape-q1", "shape-q5", "shape-qexpr", "shape-q3"},
		"shape-q3-probe-first":    {"shape-q3", "shape-q5", "shape-q1", "shape-qexpr", "shape-q2", "shape-q4"},
		"shape-q4-probe-first":    {"shape-q4", "shape-q2", "shape-qexpr", "shape-q3", "shape-q1", "shape-q5"},
		"shape-q5-probe-first":    {"shape-q5", "shape-q1", "shape-q4", "shape-q2", "shape-q3", "shape-qexpr"},
		"shape-qexpr-probe-first": {"shape-qexpr", "shape-q4", "shape-q2", "shape-q5", "shape-q3", "shape-q1"},
	}
	for name, order := range orders {
		b.Run(name, func(b *testing.B) {
			cache := NewQueryReadyGenerationOpenCache(files.Key)
			prepared, err := cache.Open(files)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = cache.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queryReadyOpenRunOrder(b, prepared, order)
			}
			b.StopTimer()
			stats := cache.Stats()
			b.ReportMetric(float64(stats.PayloadBytesDecoded), "payload_decoded_bytes")
			b.ReportMetric(float64(stats.DomainsConstructed), "domains_constructed")
			b.ReportMetric(float64(stats.RanksConstructed), "ranks_constructed")
			b.ReportMetric(float64(stats.OffsetsConstructed), "offsets_constructed")
		})
	}
}

func BenchmarkQueryReadyGenerationConcurrentWarmOpen(b *testing.B) {
	files := queryReadyOpenBenchmarkFiles(b)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	want, err := cache.Open(files)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cache.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got, err := cache.Open(files)
			if err != nil || got != want {
				b.Errorf("warm open=%p want %p err=%v", got, want, err)
				return
			}
		}
	})
}

func BenchmarkQueryReadyGenerationOpenLiveHeap(b *testing.B) {
	b.StopTimer()
	files := queryReadyOpenBenchmarkFiles(b)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	cache := NewQueryReadyGenerationOpenCache(files.Key)
	prepared, err := cache.Open(files)
	if err != nil || prepared == nil {
		b.Fatalf("open: %v", err)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "live_heap_bytes")
	b.ReportMetric(float64(cache.Stats().MappedBytes), "mapped_bytes")
	b.Cleanup(func() { _ = cache.Close() })
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if prepared.PartCount() == 0 {
			b.Fatal("no prepared parts")
		}
	}
}

func queryReadyOpenBenchmarkFiles(tb testing.TB) QueryReadyGenerationOpenFiles {
	tb.Helper()
	requireQueryReadyGenerationFileOpen(tb)
	base, deltas := queryReadyDeltaBenchmarkFixture(tb, 1, "low_cardinality")
	if len(deltas) != 1 {
		tb.Fatalf("deltas=%d", len(deltas))
	}
	dir := tb.TempDir()
	basePath := filepath.Join(dir, "base.qrbg")
	deltaPath := filepath.Join(dir, "delta.qrdg")
	if err := os.WriteFile(basePath, base.Bytes(), 0o600); err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(deltaPath, deltas[0].Bytes(), 0o600); err != nil {
		tb.Fatal(err)
	}
	key := QueryReadyGenerationOpenKey{Identity: deltas[0].Identity, ManifestHash: sha256.Sum256([]byte("benchmark-manifest"))}
	return QueryReadyGenerationOpenFiles{
		Key:                key,
		Base:               QueryReadyGenerationFile{Path: basePath, Identity: base.Identity, Kind: QueryReadyGenerationBase},
		Deltas:             []QueryReadyGenerationFile{{Path: deltaPath, Identity: deltas[0].Identity, Kind: QueryReadyGenerationDelta}},
		SnapshotGeneration: deltas[0].Identity.Generation,
		Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8},
	}
}

func requireQueryReadyGenerationFileOpen(tb testing.TB) {
	tb.Helper()
	if !QueryReadyGenerationFileOpenSupported() {
		tb.Skip("query-ready generation file open requires read-only mmap support")
	}
}
