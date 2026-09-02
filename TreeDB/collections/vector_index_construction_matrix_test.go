package collections

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestColumnGraphRebuildConstructionMatrixLifecycle4438(t *testing.T) {
	if runtime.GOOS == "windows" || !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("rebuild-local construction mmap is unsupported on Windows")
	}
	const dimensions = 64
	rows := columnGraphRebuildSyntheticRowsV2A(48, dimensions)
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dimensions, 16, rows)
	baseline := make([]columnVectorGraphAssetRow, len(rows))
	for i := range rows {
		invNorm, err := columnVectorGraphInvNorm(rows[i].vector)
		if err != nil {
			_ = d.Close()
			t.Fatalf("baseline inverse norm row %d: %v", i, err)
		}
		baseline[i] = columnVectorGraphAssetRow{ID: []byte(rows[i].id), Vector: append([]float32(nil), rows[i].vector...), InvNorm: invNorm}
	}
	if err := buildColumnVectorGraphAdjacency(baseline, def); err != nil {
		_ = d.Close()
		t.Fatalf("baseline adjacency: %v", err)
	}
	bound := false
	restore := setColumnVectorGraphConstructionMatrixBoundTestHook(func(index *VectorIndex) {
		bound = true
		if matches, err := filepath.Glob(filepath.Join(d.ColumnAssetRootDir(), columnVectorGraphConstructionMatrixPattern)); err != nil || len(matches) != 0 {
			t.Fatalf("live construction matrix remains linked matches=%v err=%v", matches, err)
		}
		if !index.constructionRowsFixed || index.frozenPrefixHeapRowStores != 0 || index.frozenPrefixBatches == 0 || len(index.vectorRows) != len(rows)*dimensions {
			t.Fatalf("construction index fixed=%t heap_stores=%d batches=%d row_floats=%d", index.constructionRowsFixed, index.frozenPrefixHeapRowStores, index.frozenPrefixBatches, len(index.vectorRows))
		}
		for i := range index.nodes {
			if len(index.nodes[i].vector) != dimensions || &index.nodes[i].vector[0] != &index.vectorRows[i*dimensions] {
				t.Fatalf("construction node %d does not alias fixed matrix", i)
			}
		}
	})
	defer restore()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	if !bound {
		_ = d.Close()
		t.Fatal("typed rebuild did not bind the construction matrix")
	}
	if matches, err := filepath.Glob(filepath.Join(d.ColumnAssetRootDir(), columnVectorGraphConstructionMatrixPattern)); err != nil || len(matches) != 0 {
		_ = d.Close()
		t.Fatalf("construction matrix after publication matches=%v err=%v", matches, err)
	}

	_, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	wantIDs := make([]string, len(baseline))
	for i := range baseline {
		wantIDs[i] = string(baseline[i].ID)
	}
	if got := columnGraphRebuildScannedIDsV2A(scanned); !slices.Equal(got, wantIDs) {
		_ = d.Close()
		t.Fatalf("published locality ids=%v want %v", got, wantIDs)
	}
	for i := range scanned {
		if !slices.Equal(scanned[i].vector, baseline[i].Vector) || !slices.Equal(scanned[i].adjacency, baseline[i].Adjacency) {
			_ = d.Close()
			t.Fatalf("published row[%d]=%+v want vector=%v adjacency=%v", i, scanned[i], baseline[i].Vector, baseline[i].Adjacency)
		}
	}
	query := append([]float32(nil), rows[0].vector...)
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &VectorIndexSearchBuffer{})
	if err != nil {
		_ = searcher.Close()
		_ = d.Close()
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	if got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackFallbacks != 0 {
		_ = searcher.Close()
		_ = d.Close()
		t.Fatalf("search stats=%+v want optimized HNSW search-pack route", got.Stats)
	}
	if err := searcher.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("searcher close: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	reopenedSearcher, err := reopenedCol.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher reopen: %v", err)
	}
	defer func() { _ = reopenedSearcher.Close() }()
	reopenedGot, err := reopenedSearcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &VectorIndexSearchBuffer{})
	if err != nil {
		t.Fatalf("SearchWithBuffer reopen: %v", err)
	}
	if reopenedGot.Stats.SearchRouteHNSWSearchPack != 1 || reopenedGot.Stats.HNSWSearchPackFallbacks != 0 || len(reopenedGot.Results) != len(got.Results) {
		t.Fatalf("reopened search=%+v want durable optimized parity with %+v", reopenedGot, got)
	}
	for i := range got.Results {
		if !slices.Equal(reopenedGot.Results[i].ID, got.Results[i].ID) || reopenedGot.Results[i].Score != got.Results[i].Score {
			t.Fatalf("reopened result[%d]=%+v want %+v", i, reopenedGot.Results[i], got.Results[i])
		}
	}

	t.Run("staging error removes temporary file", func(t *testing.T) {
		root := t.TempDir()
		_, err := stageColumnVectorGraphConstructionMatrix(root, []columnVectorGraphAssetRow{
			{Vector: []float32{1, 2, 3}},
			{Vector: []float32{4, 5}},
		}, 3)
		if err == nil || !errors.Is(err, errColumnVectorGraphConstructionMatrixShape) {
			t.Fatalf("stage error=%v want shape error", err)
		}
		matches, globErr := filepath.Glob(filepath.Join(root, columnVectorGraphConstructionMatrixPattern))
		if globErr != nil || len(matches) != 0 {
			t.Fatalf("failed staging leaked files=%v glob_err=%v", matches, globErr)
		}
		entries, readErr := os.ReadDir(root)
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("failed staging root entries=%v read_err=%v", entries, readErr)
		}
	})
}

func TestColumnGraphRebuildReleasesConstructionMatrixBeforePublication4542(t *testing.T) {
	if runtime.GOOS == "windows" || !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("rebuild-local construction mmap is unsupported on Windows")
	}
	const dimensions = 64
	rows := columnGraphRebuildSyntheticRowsV2A(48, dimensions)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dimensions, 16, rows)
	defer func() { _ = d.Close() }()

	type observedState struct {
		aliasesEmpty     bool
		closed           bool
		handleGone       bool
		activeHandles    int
		activeMappedByte int64
	}
	stagedHandles := 0
	var stagedMappedBytes int64
	restoreBound := setColumnVectorGraphConstructionMatrixBoundTestHook(func(*VectorIndex) {
		for _, pin := range mappedresource.GlobalPinSummary() {
			if pin.Root == d.ColumnAssetRootDir() && pin.Key.Kind == "column_graph_construction_matrix" {
				stagedHandles++
				if pin.Source == mappedresource.SourceMapped {
					stagedMappedBytes += pin.Bytes
				}
			}
		}
	})
	defer restoreBound()
	reached := make(chan observedState, 1)
	resume := make(chan struct{})
	restore := setColumnVectorGraphConstructionMatrixLastUseTestHook(func(rows []columnVectorGraphAssetRow, matrix *columnVectorGraphConstructionMatrix) error {
		state := observedState{
			aliasesEmpty: true,
			closed:       matrix != nil && matrix.closed && len(matrix.values) == 0,
			handleGone:   matrix != nil && matrix.handle.Released() && len(matrix.handle.Bytes()) == 0,
		}
		for i := range rows {
			if len(rows[i].Vector) != 0 {
				state.aliasesEmpty = false
				break
			}
		}
		for _, pin := range mappedresource.GlobalPinSummary() {
			if pin.Root == d.ColumnAssetRootDir() && pin.Key.Kind == "column_graph_construction_matrix" {
				state.activeHandles++
				if pin.Source == mappedresource.SourceMapped {
					state.activeMappedByte += pin.Bytes
				}
			}
		}
		reached <- state
		<-resume
		return nil
	})
	defer restore()

	done := make(chan error, 1)
	go func() {
		_, err := col.RebuildVectorIndex(def.Name)
		done <- err
	}()
	var state observedState
	select {
	case state = <-reached:
	case <-time.After(collectionTestTimeout(t, 10*time.Second)):
		t.Fatal("timed out waiting for post-preparation publication boundary")
	}
	close(resume)
	wantMappedBytes := int64(len(rows) * dimensions * 4)
	if stagedHandles != 1 || stagedMappedBytes != wantMappedBytes {
		t.Fatalf("staged construction matrix handles=%d mapped_bytes=%d want 1/%d", stagedHandles, stagedMappedBytes, wantMappedBytes)
	}
	if !state.aliasesEmpty || !state.closed || !state.handleGone || state.activeHandles != 0 || state.activeMappedByte != 0 {
		t.Fatalf("post-preparation matrix state=%+v want empty aliases and zero active handles/mapped bytes before publication", state)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 10*time.Second)):
		t.Fatal("timed out waiting for rebuild publication")
	}
}

func TestColumnGraphConstructionMatrixCloseRowsIsIdempotent4542(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{Vector: []float32{1, 2, 3}},
		{Vector: []float32{4, 5, 6}},
	}
	matrix, err := stageColumnVectorGraphConstructionMatrix(t.TempDir(), rows, 3)
	if errors.Is(err, mappedresource.ErrMmapUnsupported) {
		t.Skip("rebuild-local construction mmap is unsupported")
	}
	if err != nil {
		t.Fatalf("stage construction matrix: %v", err)
	}
	defer func() { _ = matrix.CloseRows(rows) }()
	before := mappedresource.GlobalStats()
	if err := matrix.CloseRows(rows); err != nil {
		t.Fatalf("first CloseRows: %v", err)
	}
	afterFirst := mappedresource.GlobalStats()
	if err := matrix.CloseRows(rows); err != nil {
		t.Fatalf("second CloseRows: %v", err)
	}
	afterSecond := mappedresource.GlobalStats()
	for i := range rows {
		if len(rows[i].Vector) != 0 {
			t.Fatalf("row[%d] vector remains aliased after close", i)
		}
	}
	if !matrix.closed || len(matrix.values) != 0 || !matrix.handle.Released() || len(matrix.handle.Bytes()) != 0 {
		t.Fatalf("closed matrix=%+v released=%t bytes=%d", matrix, matrix.handle.Released(), len(matrix.handle.Bytes()))
	}
	if got := afterFirst.TotalReleases - before.TotalReleases; got != 1 {
		t.Fatalf("first close release delta=%d want 1", got)
	}
	if got := afterSecond.TotalReleases - afterFirst.TotalReleases; got != 0 {
		t.Fatalf("idempotent close release delta=%d want 0", got)
	}
}

func TestColumnGraphRebuildPreparationFailureReleasesConstructionMatrix4542(t *testing.T) {
	if runtime.GOOS == "windows" || !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("rebuild-local construction mmap is unsupported on Windows")
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	const dimensions = 64
	rows := columnGraphRebuildSyntheticRowsV2A(48, dimensions)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dimensions, 16, rows)
	defer func() { _ = d.Close() }()
	injected := errors.New("injected preparation failure before matrix release")
	restorePrepare := setColumnVectorGraphStableAuthorityTestHook(func(*rootpublication.StableResourceSet, []columnVectorIndexStateAssetSnapshot) error {
		return injected
	})
	defer restorePrepare()
	lastUseCalls := 0
	restoreLastUse := setColumnVectorGraphConstructionMatrixLastUseTestHook(func([]columnVectorGraphAssetRow, *columnVectorGraphConstructionMatrix) error {
		lastUseCalls++
		return nil
	})
	defer restoreLastUse()

	if _, err := col.RebuildVectorIndex(def.Name); !errors.Is(err, injected) {
		t.Fatalf("RebuildVectorIndex error=%v want injected preparation failure", err)
	}
	if lastUseCalls != 0 {
		t.Fatalf("last-use hook calls=%d want 0 before successful preparation", lastUseCalls)
	}
	for _, pin := range mappedresource.GlobalPinSummary() {
		if pin.Root == d.ColumnAssetRootDir() && pin.Key.Kind == "column_graph_construction_matrix" {
			t.Fatalf("failed preparation retained construction matrix pin=%+v", pin)
		}
	}
}

func TestColumnGraphRebuildPublicationFailureReplayAfterMatrixRelease4542(t *testing.T) {
	if runtime.GOOS == "windows" || !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("rebuild-local construction mmap is unsupported on Windows")
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	const dimensions = 64
	rows := columnGraphRebuildSyntheticRowsV2A(48, dimensions)
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dimensions, 16, rows)
	injected := errors.New("injected publication failure after matrix release")
	failPublish := true
	restorePublish := setColumnVectorGraphStablePublishTestHook(func(*columnVectorGraphPreparedPhysicalAsset) error {
		if failPublish {
			failPublish = false
			return injected
		}
		return nil
	})
	defer restorePublish()
	lastUseCalls := 0
	restoreLastUse := setColumnVectorGraphConstructionMatrixLastUseTestHook(func(gotRows []columnVectorGraphAssetRow, matrix *columnVectorGraphConstructionMatrix) error {
		lastUseCalls++
		for i := range gotRows {
			if len(gotRows[i].Vector) != 0 {
				return errors.New("row vector remains aliased at publication failure boundary")
			}
		}
		if matrix == nil || !matrix.closed || !matrix.handle.Released() {
			return errors.New("construction matrix remains live at publication failure boundary")
		}
		return nil
	})
	defer restoreLastUse()

	if _, err := col.RebuildVectorIndex(def.Name); !errors.Is(err, injected) {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex error=%v want injected publication failure", err)
	}
	if lastUseCalls != 1 {
		_ = d.Close()
		t.Fatalf("last-use hook calls after failed publication=%d want 1", lastUseCalls)
	}
	for _, pin := range mappedresource.GlobalPinSummary() {
		if pin.Root == d.ColumnAssetRootDir() && pin.Key.Kind == "column_graph_construction_matrix" {
			_ = d.Close()
			t.Fatalf("failed publication retained construction matrix pin=%+v", pin)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close after publication failure: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection after replay: %v", err)
	}
	status, err := reopenedCol.columnGraphVectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("columnGraphVectorIndexStatus after replay: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	if lastUseCalls != 2 {
		t.Fatalf("last-use hook calls after replay=%d want 2", lastUseCalls)
	}
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     append([]float32(nil), rows[0].vector...),
		TopK:      2,
		EfSearch:  len(rows),
		StatsMode: VectorIndexSearchStatsModeProduction,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex after replay: %v", err)
	}
	if len(got.Results) != 2 || got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackFallbacks != 0 {
		t.Fatalf("search after replay=%+v want two results on optimized pack route", got)
	}
}
