package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/page"
)

type columnServingMaterializerCacheFixture struct {
	*columnServingLeaseTestFixture
	payload                []byte
	rowBase, rowSuffix     ColumnAssetRef
	typedBase, typedSuffix ColumnAssetRef
}

func newColumnServingMaterializerCacheFixture(t testing.TB) *columnServingMaterializerCacheFixture {
	t.Helper()
	base := &columnServingLeaseTestFixture{
		root:       t.TempDir(),
		namespace:  "docs_assets",
		collection: "docs",
		db:         new(backenddb.DB),
		ledger:     new(typedGraphPhysicalResourceLedger),
		limits: typedGraphPhysicalResourceLimits{
			Segments: 8, Descriptors: 8, MappedBytes: 8 << 20,
			FallbackBytes: 8 << 20, InventoryBytes: 1 << 20,
		},
	}
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(1 + (i*37)%251)
	}
	makeRef := func(kind ColumnAssetKind, generation, partID uint64, offset, length int64) ColumnAssetRef {
		return ColumnAssetRef{
			Kind: kind, Namespace: base.namespace, Generation: generation,
			PartID: partID, FileID: 1, Offset: offset, Length: length,
			Checksum: page.Checksum(payload[offset : offset+length]),
		}
	}
	fixture := &columnServingMaterializerCacheFixture{
		columnServingLeaseTestFixture: base,
		payload:                       payload,
		rowBase:                       makeRef(ColumnAssetKindTCS1PartImage, 1, 11, 96, 160),
		// Both unlisted refs lie wholly inside the authorized mapped prefix.
		// Their changed exact identities must still force the local path.
		rowSuffix:   makeRef(ColumnAssetKindTCS1PartImage, 2, 12, 112, 80),
		typedBase:   makeRef(ColumnAssetKindTCS1TypedColumnPart, 1, 21, 512, 192),
		typedSuffix: makeRef(ColumnAssetKindTCS1TypedColumnPart, 2, 22, 536, 96),
	}
	base.refs = []ColumnAssetRef{fixture.rowBase, fixture.typedBase}
	sort.Slice(base.refs, func(i, j int) bool { return compareColumnAssetRefs(base.refs[i], base.refs[j]) < 0 })
	path, err := columnAssetSegmentPath(base.root, fixture.rowBase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := digestTypedGraphServingBaseRefs(base.refs)
	if err != nil {
		t.Fatal(err)
	}
	base.key = columnVectorGraphSharedPreparedSearchKey{
		family: columnVectorGraphSharedPreparedSearchKeyServing, db: base.db,
		assetRoot: base.root, collection: base.collection, namespace: base.namespace,
		logical: "test-materializer-holder", refsDigest: digest, refsCount: len(base.refs),
	}
	base.guardian = &ColumnAssetLifecyclePinSet{
		source: ColumnAssetLifecyclePinSourcePreparedQuery,
		owner:  "test-materializer-holder-guardian",
		refs:   slices.Clone(base.refs),
	}
	base.set, err = newColumnServingSegmentLeaseSet(base.key, base.refs, base.guardian, base.ledger, base.limits)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f *columnServingMaterializerCacheFixture) bytesFor(ref ColumnAssetRef) []byte {
	return f.payload[ref.Offset : ref.Offset+ref.Length]
}

func newColumnServingMaterializerReadCache(t testing.TB, fixture *columnServingMaterializerCacheFixture, integrity ColumnAssetReadIntegrity, forceReadAt bool, serving bool) (*columnPhysicalAssetReadCache, *mappedresource.Manager) {
	t.Helper()
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(fixture.root, fixture.namespace, integrity)
	if err != nil {
		t.Fatal(err)
	}
	cache.returnViews = true
	cache.forceReadAtFallback = forceReadAt
	cache.trustCachedVerifyFileIdentity = true
	manager := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeCollectionReadView, ID: "test-materializer-view", Collection: fixture.collection, Namespace: fixture.namespace}
	if err := cache.useMappedResourceManager(manager, scope, "test materializer read"); err != nil {
		t.Fatal(err)
	}
	if serving {
		access, err := newColumnVectorGraphServingSourceAccess(context.Background(), fixture.set)
		if err != nil {
			t.Fatal(err)
		}
		if err := cache.useServingSourceAccess(access); err != nil {
			t.Fatal(err)
		}
	}
	return &cache, manager
}

func TestColumnServingMaterializerCachesExactAuthorityBeforeSameFileCache(t *testing.T) {
	modes := []struct {
		name   string
		mapErr error
	}{
		{name: "mapped"},
		{name: "ordinary_mmap_error", mapErr: errColumnServingLeaseInjected},
		{name: "mmap_unsupported", mapErr: mappedresource.ErrMmapUnsupported},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			if mode.mapErr == nil && !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
				t.Skip("exact-prefix mmap is unsupported")
			}
			fixture := newColumnServingMaterializerCacheFixture(t)
			if mode.mapErr != nil {
				installColumnServingLeaseHooks(t, func() {
					columnServingSegmentLeaseHooks.Lock()
					columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, mode.mapErr }
					columnServingSegmentLeaseHooks.Unlock()
				})
			}

			families := []struct {
				name         string
				base, suffix ColumnAssetRef
			}{
				{name: "row_asset_cache", base: fixture.rowBase, suffix: fixture.rowSuffix},
				{name: "typed_column_asset_cache", base: fixture.typedBase, suffix: fixture.typedSuffix},
			}
			for _, family := range families {
				for _, order := range []struct {
					name string
					refs []ColumnAssetRef
				}{
					{name: "base_suffix_base", refs: []ColumnAssetRef{family.base, family.suffix, family.base}},
					{name: "suffix_base_suffix", refs: []ColumnAssetRef{family.suffix, family.base, family.suffix}},
				} {
					t.Run(family.name+"/"+order.name, func(t *testing.T) {
						cache, manager := newColumnServingMaterializerReadCache(t, fixture, ColumnAssetReadIntegrityVerify, false, true)
						var local *columnPhysicalAssetSegmentReader
						for i, ref := range order.refs {
							before := cache.lifecycleStats()
							raw, err := cache.read(ref, nil)
							if err != nil || !bytes.Equal(raw, fixture.bytesFor(ref)) {
								t.Fatalf("read[%d] ref=%+v bytes=%d err=%v", i, ref, len(raw), err)
							}
							after := cache.lifecycleStats()
							if ref == family.base {
								if after.ServingBorrows != before.ServingBorrows+1 || after.MmapHits != before.MmapHits || after.ReadAtFallbacks != before.ReadAtFallbacks || after.FileOpens != before.FileOpens || after.FileCloses != before.FileCloses {
									t.Fatalf("base read changed local counters before=%+v after=%+v", before, after)
								}
								if cache.file != local {
									t.Fatal("base borrow changed the request-local same-FileID reader")
								}
							} else {
								if after.ServingBorrows != before.ServingBorrows {
									t.Fatal("unlisted same-file suffix borrowed serving authority")
								}
								if cache.file == nil || cache.file.file == nil {
									t.Fatal("unlisted suffix did not retain its request-local reader")
								}
								if local == nil {
									local = cache.file
								} else if cache.file != local {
									t.Fatal("same-file suffix changed its local reader")
								}
							}
						}
						if local == nil || cache.file != local || len(cache.files) != 0 {
							t.Fatal("cache did not retain exactly the local suffix reader")
						}
						if manager.Stats().ActiveHandles == 0 {
							t.Fatal("materializer logical handles were not request-owned")
						}
						if err := cache.close(); err != nil {
							t.Fatal(err)
						}
						if manager.Stats().ActiveHandles != 0 || local.file != nil || local.mmap != nil {
							t.Fatalf("cache close did not release only local state: manager=%+v local=%+v", manager.Stats(), local)
						}
						physical := fixture.ledger.snapshot()
						if mode.mapErr == nil {
							if physical.mappedBackings != 1 || physical.descriptorsLive != 0 {
								t.Fatalf("cache close changed mapped holder backing: %+v", physical)
							}
						} else if physical.fallbackSegments != 1 || physical.descriptorsLive != 1 || physical.fallbackBytes != 0 {
							t.Fatalf("cache close changed descriptor holder backing or populated parent fallback: %+v", physical)
						}
					})
				}
			}
			if err := fixture.set.Close(); err != nil {
				t.Fatal(err)
			}
			requireColumnServingLedgerEmpty(t, fixture.ledger)
		})
	}
}

func TestColumnServingMaterializerForceReadAtAndGenericRemainLocal(t *testing.T) {
	for _, tc := range []struct {
		name        string
		serving     bool
		forceReadAt bool
	}{
		{name: "forced_read_at", serving: true, forceReadAt: true},
		{name: "non_serving", serving: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newColumnServingMaterializerCacheFixture(t)
			cache, manager := newColumnServingMaterializerReadCache(t, fixture, ColumnAssetReadIntegrityVerify, tc.forceReadAt, tc.serving)
			raw, err := cache.read(fixture.rowBase, nil)
			if err != nil || !bytes.Equal(raw, fixture.bytesFor(fixture.rowBase)) {
				t.Fatalf("local read bytes=%d err=%v", len(raw), err)
			}
			stats := cache.lifecycleStats()
			if stats.ServingBorrows != 0 || stats.FileOpens != 1 {
				t.Fatalf("local route stats=%+v", stats)
			}
			if tc.forceReadAt && (stats.ReadAtFallbacks != 1 || stats.MmapHits != 0) {
				t.Fatalf("forced ReadAt route stats=%+v", stats)
			}
			if fixture.set.segments[0].state != columnServingSegmentUnopened {
				t.Fatal("local route acquired the serving pool")
			}
			if err := cache.close(); err != nil {
				t.Fatal(err)
			}
			if manager.Stats().ActiveHandles != 0 {
				t.Fatal("local logical handles survived cache close")
			}
			if err := fixture.set.Close(); err != nil {
				t.Fatal(err)
			}
			requireColumnServingLedgerEmpty(t, fixture.ledger)
		})
	}
}

func TestColumnServingMaterializerAuthorizedPoolFailureIsFailClosed(t *testing.T) {
	fixture := newColumnServingMaterializerCacheFixture(t)
	cache, _ := newColumnServingMaterializerReadCache(t, fixture, ColumnAssetReadIntegrityVerify, false, true)
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(fixture.rowBase, nil); !errors.Is(err, errColumnServingSegmentClosed) {
		t.Fatalf("authorized read after pool close error=%v", err)
	}
	if cache.fileOpens != 0 || cache.file != nil || len(cache.files) != 0 {
		t.Fatal("authorized pool failure silently opened a local reader")
	}
	if err := cache.close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

func TestColumnServingMaterializerCachedVerifyUsesExactRefAndStableIdentity(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	fixture := newColumnServingMaterializerCacheFixture(t)
	cache, _ := newColumnServingMaterializerReadCache(t, fixture, ColumnAssetReadIntegrityCachedVerify, false, true)
	if _, err := cache.read(fixture.rowBase, nil); err != nil {
		t.Fatal(err)
	}
	identity := fixture.set.segments[0].identity
	if identity.valid && !columnAssetVerifiedChecksumCacheContains(fixture.root, fixture.rowBase, identity) {
		t.Fatal("pooled verified read did not retain its exact captured identity proof")
	}
	changed := fixture.rowBase
	changed.Checksum++
	if authorized, err := cache.servingSourceAccess.authorizesMaterializerAsset(fixture.root, changed); err != nil || authorized {
		t.Fatalf("changed exact ref authority=%t err=%v", authorized, err)
	}
	if _, err := cache.read(changed, nil); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("same-file changed checksum reused pool verification: %v", err)
	}
	if cache.servingBorrows != 1 || cache.fileOpens != 1 {
		t.Fatalf("exact-ref/local integrity routing borrows=%d opens=%d", cache.servingBorrows, cache.fileOpens)
	}
	if err := cache.close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)

	resetColumnAssetVerifiedChecksumCacheForTest(t)
	corrupt := newColumnServingMaterializerCacheFixture(t)
	bad := corrupt.rowBase
	bad.Checksum++
	corrupt.refs = []ColumnAssetRef{bad, corrupt.typedBase}
	sort.Slice(corrupt.refs, func(i, j int) bool { return compareColumnAssetRefs(corrupt.refs[i], corrupt.refs[j]) < 0 })
	if err := corrupt.set.Close(); err != nil {
		t.Fatal(err)
	}
	digest, err := digestTypedGraphServingBaseRefs(corrupt.refs)
	if err != nil {
		t.Fatal(err)
	}
	corrupt.key.refsDigest = digest
	corrupt.guardian = &ColumnAssetLifecyclePinSet{source: ColumnAssetLifecyclePinSourcePreparedQuery, owner: "test-corrupt-materializer-guardian", refs: slices.Clone(corrupt.refs)}
	corrupt.set, err = newColumnServingSegmentLeaseSet(corrupt.key, corrupt.refs, corrupt.guardian, corrupt.ledger, corrupt.limits)
	if err != nil {
		t.Fatal(err)
	}
	badCache, _ := newColumnServingMaterializerReadCache(t, corrupt, ColumnAssetReadIntegrityCachedVerify, false, true)
	for range 2 {
		if _, err := badCache.read(bad, nil); err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("cached first corrupt pooled read error=%v", err)
		}
	}
	if badCache.hasVerifiedRowIndexKey {
		t.Fatal("failed pooled checksum authorized a row-index memo")
	}
	if err := badCache.close(); err != nil {
		t.Fatal(err)
	}
	if err := corrupt.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, corrupt.ledger)
}

func TestTypedGraphPublicMaterializerBorrowsServingHolder(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: len(ids), EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	documents, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	access, err := view.materializerServingSourceAccess()
	if err != nil || access == nil || view.rowAssetReadCache == nil || view.typedColumnAssetReadCache == nil || view.rowAssetReadCache.servingSourceAccess != access || view.typedColumnAssetReadCache.servingSourceAccess != access {
		t.Fatalf("materializer caches do not share holder access: access=%p row=%p typed=%p err=%v", access, view.rowAssetReadCache, view.typedColumnAssetReadCache, err)
	}
	if documents.Stats.AssetServingBorrows < 2 || documents.Stats.AssetMmapHits != 0 || documents.Stats.AssetReadAtFallbacks != 0 || documents.Stats.AssetFileOpens != 0 {
		t.Fatalf("base-only materializer physical routing stats=%+v", documents.Stats)
	}
	manager := view.assetManager
	beforeClose := access.pool.ledger.snapshot()
	if manager == nil || manager.Stats().ActiveHandles == 0 {
		t.Fatal("serving materializer did not retain request logical handles")
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Stats().ActiveHandles != 0 {
		t.Fatal("view close retained materializer logical handles")
	}
	afterClose := access.pool.ledger.snapshot()
	if afterClose.mappedBackings != beforeClose.mappedBackings || afterClose.descriptorsLive != beforeClose.descriptorsLive || afterClose.fallbackBytes != beforeClose.fallbackBytes {
		t.Fatalf("request close changed holder physical backing before=%+v after=%+v", beforeClose, afterClose)
	}
	if len(response.Results) == 0 || len(documents.Results) != len(ids) {
		t.Fatal("serving materializer returned incomplete results")
	}

	plain, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	want, err := plain.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want.Stats.AssetServingBorrows != 0 || want.Stats.AssetFileOpens == 0 || !reflect.DeepEqual(documents.Results, want.Results) {
		t.Fatalf("generic materializer route/results stats=%+v equal=%t", want.Stats, reflect.DeepEqual(documents.Results, want.Results))
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	var forcedBuffer VectorIndexSearchBuffer
	_, forced, err := col.SearchVectorIndexWithBufferReadView(query, &forcedBuffer)
	if err != nil {
		t.Fatal(err)
	}
	forced.forceAssetReadAtFallbackForTest = true
	forcedDocs, err := forced.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if forcedDocs.Stats.AssetServingBorrows != 0 || forcedDocs.Stats.AssetReadAtFallbacks == 0 || forcedDocs.Stats.AssetFileOpens == 0 || !reflect.DeepEqual(documents.Results, forcedDocs.Results) {
		t.Fatalf("forced local route/results stats=%+v equal=%t", forcedDocs.Stats, reflect.DeepEqual(documents.Results, forcedDocs.Results))
	}
	if err := forced.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedGraphPublicMaterializerMixedBaseSuffixParityAndOldView(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]},
		{Name: "content", Strings: []string{"first-suffix"}},
		{Name: "user", Strings: []string{"suffix-user"}},
		{Name: "path", Strings: []string{"suffix-path"}},
	}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}

	query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}
	var buffer VectorIndexSearchBuffer
	_, view, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	got, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		_ = view.Close()
		t.Fatal(err)
	}
	access, err := view.materializerServingSourceAccess()
	if err != nil || access == nil {
		_ = view.Close()
		t.Fatalf("mixed materializer serving access=%p err=%v", access, err)
	}
	assertTypedGraphMaterializerReuse(t, view, true)
	for name, cache := range map[string]*columnPhysicalAssetReadCache{
		"row": view.rowAssetReadCache, "typed": view.typedColumnAssetReadCache,
	} {
		if cache == nil || cache.servingSourceAccess != access || cache.servingBorrows == 0 || cache.fileOpens == 0 {
			_ = view.Close()
			t.Fatalf("%s cache did not split base borrows from suffix-local reads: %+v", name, cache)
		}
	}
	if got.Stats.AssetServingBorrows == 0 || got.Stats.AssetMmapHits == 0 {
		_ = view.Close()
		t.Fatalf("mixed route stats=%+v", got.Stats)
	}

	plain, err := col.OpenCollectionReadView()
	if err != nil {
		_ = view.Close()
		t.Fatal(err)
	}
	want, wantErr := plain.FetchDocumentsByID(ids, DocumentFetchOptions{})
	plainCloseErr := plain.Close()
	if wantErr != nil || plainCloseErr != nil || want.Stats.AssetServingBorrows != 0 || !reflect.DeepEqual(got.Results, want.Results) {
		_ = view.Close()
		t.Fatalf("mixed serving/local parity fetch=%v close=%v generic_stats=%+v equal=%t", wantErr, plainCloseErr, want.Stats, reflect.DeepEqual(got.Results, want.Results))
	}

	// A later publication must not change either the borrowed base mapping or
	// suffix-local reader owned by this already-admitted view.
	changed[1].Strings[0] = "second-suffix"
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		_ = view.Close()
		t.Fatal(err)
	}
	held, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || !reflect.DeepEqual(held.Results, got.Results) {
		_ = view.Close()
		t.Fatalf("old mixed view changed after publication err=%v equal=%t", err, reflect.DeepEqual(held.Results, got.Results))
	}

	beforeClose := access.pool.ledger.snapshot()
	manager := view.assetManager
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	afterClose := access.pool.ledger.snapshot()
	closed := view.assetCounters()
	var activeHandles int64 = -1
	if manager != nil {
		activeHandles = manager.Stats().ActiveHandles
	}
	if activeHandles != 0 || closed.fileCloses != closed.fileOpens {
		t.Fatalf("mixed request cleanup active_handles=%d counters=%+v", activeHandles, closed)
	}
	if afterClose.mappedBackings != beforeClose.mappedBackings || afterClose.descriptorsLive != beforeClose.descriptorsLive || afterClose.fallbackBytes != beforeClose.fallbackBytes {
		t.Fatalf("request close changed current holder backing before=%+v after=%+v", beforeClose, afterClose)
	}
}

func TestTypedGraphPublicConcurrentMaterializersShareOnlyHolderBacking(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}
	var anchorBuffer VectorIndexSearchBuffer
	_, anchor, err := col.SearchVectorIndexWithBufferReadView(query, &anchorBuffer)
	if err != nil {
		t.Fatal(err)
	}
	want, err := anchor.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		_ = anchor.Close()
		t.Fatal(err)
	}
	access, err := anchor.materializerServingSourceAccess()
	if err != nil || access == nil {
		_ = anchor.Close()
		t.Fatalf("anchor access=%p err=%v", access, err)
	}
	warm := access.pool.ledger.snapshot()

	const callers = 8
	type result struct {
		documents                  DocumentFetchResponse
		rowCache, typedColumnCache *columnPhysicalAssetReadCache
		access                     *columnVectorGraphSourceAccess
		activeAfterClose           int64
		err                        error
	}
	results := make(chan result, callers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			var searchBuffer VectorIndexSearchBuffer
			_, read, openErr := col.SearchVectorIndexWithBufferReadView(query, &searchBuffer)
			if openErr != nil {
				results <- result{err: openErr}
				return
			}
			documents, fetchErr := read.FetchDocumentsByID(ids, DocumentFetchOptions{})
			borrowed, borrowErr := read.materializerServingSourceAccess()
			rowCache, typedCache, manager := read.rowAssetReadCache, read.typedColumnAssetReadCache, read.assetManager
			closeErr := read.Close()
			var active int64
			if manager != nil {
				active = manager.Stats().ActiveHandles
			}
			results <- result{documents: documents, rowCache: rowCache, typedColumnCache: typedCache, access: borrowed, activeAfterClose: active, err: errors.Join(fetchErr, borrowErr, closeErr)}
		}()
	}
	ready.Wait()
	close(start)
	rowCaches := make(map[*columnPhysicalAssetReadCache]struct{}, callers)
	typedCaches := make(map[*columnPhysicalAssetReadCache]struct{}, callers)
	for range callers {
		got := <-results
		if got.err != nil || got.access != access || got.rowCache == nil || got.typedColumnCache == nil || got.rowCache == got.typedColumnCache || got.activeAfterClose != 0 || got.documents.Stats.AssetServingBorrows < 2 || got.documents.Stats.AssetFileOpens != 0 || !reflect.DeepEqual(got.documents.Results, want.Results) {
			_ = anchor.Close()
			t.Fatalf("concurrent materializer result=%+v err=%v access=%p active=%d equal=%t", got.documents.Stats, got.err, got.access, got.activeAfterClose, reflect.DeepEqual(got.documents.Results, want.Results))
		}
		rowCaches[got.rowCache] = struct{}{}
		typedCaches[got.typedColumnCache] = struct{}{}
	}
	if len(rowCaches) != callers || len(typedCaches) != callers {
		_ = anchor.Close()
		t.Fatalf("request-local caches were shared: row=%d typed=%d", len(rowCaches), len(typedCaches))
	}
	after := access.pool.ledger.snapshot()
	if after.mappedBackings != warm.mappedBackings || after.descriptorsLive != warm.descriptorsLive || after.fallbackBytes != warm.fallbackBytes {
		_ = anchor.Close()
		t.Fatalf("independent request closes changed holder backing warm=%+v after=%+v", warm, after)
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedGraphPublicMaterializerDBCloseRetainsHolderUntilViewClose(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	beforeGlobal := ColumnGraphClosedDBPhysicalQuarantineSnapshot()
	col, base, ids, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}
	var buffer VectorIndexSearchBuffer
	_, view, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{}); err != nil {
		_ = view.Close()
		t.Fatal(err)
	}
	access, err := view.materializerServingSourceAccess()
	if err != nil || access == nil || view.assetManager == nil || view.assetManager.Stats().ActiveHandles == 0 {
		_ = view.Close()
		t.Fatalf("active materializer access=%p manager=%+v err=%v", access, view.assetManager, err)
	}
	dir := col.db.Dir()
	if err := col.db.Close(); !errors.Is(err, errColumnServingSegmentCleanupRetained) {
		_ = view.Close()
		t.Fatalf("DB close with materializer holder error=%v", err)
	}
	duringGlobal := ColumnGraphClosedDBPhysicalQuarantineSnapshot()
	if len(duringGlobal) != len(beforeGlobal)+1 || !duringGlobal[len(duringGlobal)-1].Physical.ClosedDB {
		_ = view.Close()
		t.Fatalf("active materializer holder was not transferred at DB close before=%d during=%+v", len(beforeGlobal), duringGlobal)
	}
	if view.assetManager.Stats().ActiveHandles == 0 {
		_ = view.Close()
		t.Fatal("DB close released request logical handles before view close")
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if view.assetManager.Stats().ActiveHandles != 0 || !access.pool.ledger.snapshot().empty() {
		t.Fatalf("late view close did not release materializer/pool manager=%+v physical=%+v", view.assetManager.Stats(), access.pool.ledger.snapshot())
	}
	afterGlobal := ColumnGraphClosedDBPhysicalQuarantineSnapshot()
	if len(afterGlobal) != len(beforeGlobal) {
		t.Fatalf("late materializer close did not prune closed-DB record before=%d after=%+v", len(beforeGlobal), afterGlobal)
	}

	reopenedDB := openTypedMinimaDB(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	var reopenedBuffer VectorIndexSearchBuffer
	_, reopenedView, err := reopened.SearchVectorIndexWithBufferReadView(query, &reopenedBuffer)
	if err != nil {
		t.Fatal(err)
	}
	documents, fetchErr := reopenedView.FetchDocumentsByID(ids, DocumentFetchOptions{})
	closeErr := reopenedView.Close()
	if fetchErr != nil || closeErr != nil || len(documents.Results) != len(ids) || documents.Stats.AssetServingBorrows == 0 {
		t.Fatalf("reopened materializer results=%d stats=%+v fetch=%v close=%v", len(documents.Results), documents.Stats, fetchErr, closeErr)
	}
}
