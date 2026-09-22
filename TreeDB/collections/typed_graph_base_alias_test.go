package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestTypedGraphBaseCapturePagerPagesDisjoint(t *testing.T) {
	col, reader, _, _, _, _ := openTypedGraphQualityFixture(t, 512)
	defer reader.Close()
	snap := col.db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	base := catalog.typedGraphBase
	if base == nil {
		t.Fatal("missing captured base")
	}
	currentPages := make(map[uint64]string)
	multiPage := false
	for name := range base.roots {
		pages, err := tree.New(snap.Pager(), nil, catalog.rootID(name)).CollectPageIDs()
		if err != nil {
			t.Fatal(err)
		}
		multiPage = multiPage || len(pages) > 1
		for _, id := range pages {
			currentPages[id] = name
		}
	}
	if !multiPage {
		t.Fatal("fixture did not exercise multiple pager pages")
	}
	// CollectPageIDs intentionally excludes immutable leaf-log references.
	// All mutable pager descendants, not only root IDs, must be independent.
	for name, root := range base.roots {
		pages, err := tree.New(snap.Pager(), nil, root).CollectPageIDs()
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range pages {
			if current, shared := currentPages[id]; shared {
				t.Errorf("captured %s shares mutable pager page %d with current %s", name, id, current)
			}
		}
	}
}

func TestTypedGraphBaseControlBoundsAndIdentity(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	meta := col.Meta()
	raw, err := encodeTypedGraphBaseControl(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeTypedGraphBaseControl(raw, meta.Name)
	if err != nil || !collectionMetaValuesEqual(meta, decoded) {
		t.Fatalf("roundtrip: %v", err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"short":            func(b []byte) []byte { return b[:typedGraphBaseControlHeader-1] },
		"magic":            func(b []byte) []byte { b[0]++; return b },
		"version":          func(b []byte) []byte { b[4]++; return b },
		"shared-root-v1":   func(b []byte) []byte { b[4] = 1; return b },
		"zero_roots":       func(b []byte) []byte { binary.LittleEndian.PutUint16(b[6:8], 0); return b },
		"too_many_roots":   func(b []byte) []byte { binary.LittleEndian.PutUint16(b[6:8], typedGraphBaseMaxRoots+1); return b },
		"wrong_root_count": func(b []byte) []byte { b[6]++; return b },
		"length":           func(b []byte) []byte { b[8]++; return b },
		"trailing":         func(b []byte) []byte { return append(b, 0) },
		"oversized":        func(b []byte) []byte { return append(b, make([]byte, typedGraphBaseControlMaxBytes)...) },
		"unknown_metadata": func(b []byte) []byte {
			b = append(b[:len(b)-1], []byte(`,"aliases":{"root":123}}`)...)
			binary.LittleEndian.PutUint32(b[8:12], uint32(len(b)-typedGraphBaseControlHeader))
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeTypedGraphBaseControl(mutate(bytes.Clone(raw)), meta.Name); err == nil {
				t.Fatal("accepted malformed control")
			}
		})
	}
	if _, err := decodeTypedGraphBaseControl(raw, "foreign"); err == nil {
		t.Fatal("accepted foreign collection")
	}
	for count := typedGraphBaseMaxRoots - 3; count <= typedGraphBaseMaxRoots-2; count++ {
		candidate := meta
		candidate.Indexes = make([]IndexDefinition, count)
		for i := range candidate.Indexes {
			candidate.Indexes[i] = IndexDefinition{Name: fmt.Sprintf("user%d", i), Field: "meta.user_id", ValueType: IndexValueString}
		}
		_, err := encodeTypedGraphBaseControl(candidate)
		if (err == nil) != (count == typedGraphBaseMaxRoots-3) {
			t.Fatalf("root count %d: %v", count+3, err)
		}
	}
	large := meta
	large.Name = strings.Repeat("x", typedGraphBaseControlMaxBytes)
	if _, err := encodeTypedGraphBaseControl(large); err == nil {
		t.Fatal("accepted oversized metadata")
	}
	current := meta
	cfg := *meta.Options.ColumnStore
	current.Options.ColumnStore = &cfg
	identity := *cfg.ActiveManifest
	identity.Generation++
	identity.Checksum++
	cfg.ActiveManifest, cfg.RecoveryAuthoritativeManifest = &identity, &identity
	cfg.RecoveryAuthoritativeAppliedCommandLSN++
	cfg.PhysicalMutationParts++
	if !typedGraphBaseSchemaMatches(meta, current) {
		t.Fatal("rejected forward physical frontier")
	}
	if typedGraphBaseSchemaMatches(current, meta) {
		t.Fatal("accepted reversed frontier")
	}
	current.Indexes = append([]IndexDefinition(nil), meta.Indexes...)
	current.Indexes[0].Field = "content"
	if typedGraphBaseSchemaMatches(meta, current) {
		t.Fatal("accepted changed scalar predicate schema")
	}
}

func TestTypedGraphBaseAliasControlledFixtureReopen(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) { testTypedGraphBaseAliasControlledFixtureReopen(t, direct) })
	}
}

func TestTypedGraphBaseCaptureRejectsCrossManagerStaleBeforeWAL(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	var afterWriteFrames int
	restore := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
		if _, _, err := other.InsertTypedBatchWithStats([][]byte{[]byte("new")}, [][]byte{[]byte(`{"id":"new"}`)}, columns); err != nil {
			t.Fatal(err)
		}
		if err := other.Flush(); err != nil {
			t.Fatal(err)
		}
		afterWriteFrames = countCollectionCommandWALFrames(t, dir)
	})
	defer restore()
	_, err = col.RebuildVectorIndex("embedding_graph")
	if err == nil || errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("stale build must fail before WAL admission: %v", err)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != afterWriteFrames {
		t.Fatalf("stale build appended command frame: before=%d after=%d", afterWriteFrames, got)
	}
	if document, err := other.Get([]byte("new")); err != nil || len(document) == 0 {
		t.Fatalf("intervening authoritative write missing: %s %v", document, err)
	}
	restore()
	meta := typedMinimaCollectionMeta()
	meta.Name = "unrelated"
	manager := NewCollectionManager(db)
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	unrelated, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	restoreUnrelated := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
		if _, _, err := unrelated.InsertTypedBatchWithStats([][]byte{[]byte("elsewhere")}, [][]byte{[]byte(`{"id":"elsewhere"}`)}, columns); err != nil {
			t.Fatal(err)
		}
		if err := unrelated.Flush(); err != nil {
			t.Fatal(err)
		}
	})
	defer restoreUnrelated()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatalf("unrelated collection publication rejected valid source: %v", err)
	}
}

func TestTypedGraphBaseAutomaticCapture(t *testing.T) {
	for _, fixture := range []struct{ populated, quantized bool }{{}, {populated: true}, {populated: true, quantized: true}} {
		t.Run(fmt.Sprintf("populated=%v/quantized=%v", fixture.populated, fixture.quantized), func(t *testing.T) {
			populated := fixture.populated
			meta := typedMinimaCollectionMeta()
			if fixture.quantized {
				meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.fast"}}
			}
			_, db, col := openTypedMinimaCollectionMeta(t, meta)
			defer db.Close()
			if populated {
				columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
				if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			snap := db.AcquireSnapshot()
			defer snap.Close()
			catalog, err := loadCollectionCatalog(snap, col.Name())
			if err != nil || catalog == nil || catalog.typedGraphBase == nil {
				t.Fatalf("rebuild did not atomically capture base: %v", err)
			}
			if !collectionMetaValuesEqual(catalog.meta, catalog.typedGraphBase.meta) {
				t.Fatal("capture identity differs from rebuilt current graph")
			}
			records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, db, col.Name())
			def := col.Meta().VectorIndexes[0]
			state := columnVectorIndexStateFromRecords1987(t, records, def)
			assertTypedGraphSelectedMetadataInventory(t, state.Assets, state.RowCount, len(def.QuantizedIndexes))
			_, _, pack := loadColumnHNSWSearchPackForTest2313(t, db, def, graphManifestFromRecords1918(t, records, def), state)
			if state.AdjacencyLayerCount != pack.Header.AdjacencyLayerCount {
				t.Errorf("selected TVIS layers=%d pack=%d", state.AdjacencyLayerCount, pack.Header.AdjacencyLayerCount)
			}
			if _, err := catalog.typedGraphBase.catalog(col, snap); err != nil {
				t.Fatal(err)
			}
			status, err := col.VectorIndexStatus(def.Name)
			if err != nil || !status.Loaded {
				t.Errorf("selected status=%+v err=%v", status, err)
			}
			if populated {
				opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}
				modes := []VectorIndexQueryMode{VectorIndexQueryModeExact}
				if fixture.quantized {
					modes = append(modes, VectorIndexQueryModeQuantizedOnly, VectorIndexQueryModeQuantizedRerank)
				}
				for _, mode := range modes {
					opts.QueryMode = mode
					if mode != VectorIndexQueryModeExact {
						opts.QuantizedIndexName = def.QuantizedIndexes[0].Name
					}
					if mode == VectorIndexQueryModeQuantizedRerank {
						opts.QuantizedRerankCandidates = 1
					}
					var buffer VectorIndexSearchBuffer
					response, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
					if err != nil || len(response.Results) != 1 || string(response.Results[0].ID) != "base" || response.Results[0].Score < .99 {
						t.Errorf("selected buffered mode=%s results=%+v err=%v", mode, response.Results, err)
					}
				}
			}
			if rootpublication.StableRelativeNamespaceSupported() {
				calls := 0
				restore := setColumnVectorGraphStableAuthorityTestHook(func(resources *rootpublication.StableResourceSet, assets []columnVectorIndexStateAssetSnapshot) error {
					calls++
					assertTypedGraphSelectedMetadataInventory(t, assets, state.RowCount, len(def.QuantizedIndexes))
					var obligations int
					for _, descriptor := range resources.Descriptors() {
						obligations += len(descriptor.LogicalObligations())
					}
					if obligations != len(assets) {
						t.Errorf("stable obligations=%d actual assets=%d", obligations, len(assets))
					}
					return nil
				})
				defer restore()
				closure, err := col.PrepareVectorIndexStableClosure(def.Name)
				if err != nil {
					t.Fatal(err)
				}
				closure.Release()
				if calls != 1 {
					t.Fatalf("selected stable preparation calls=%d", calls)
				}
			}
		})
	}
}

func assertTypedGraphSelectedMetadataInventory(t *testing.T, assets []columnVectorIndexStateAssetSnapshot, rows, quantized int) {
	t.Helper()
	want := map[string]int{columnVectorIndexStateAssetRoleHNSWSearchPack: 1}
	if rows > 0 {
		want[columnVectorIndexStateAssetRoleInverseNorm] = 1
		want[columnVectorIndexStateAssetRoleRowRefs] = 1
		if quantized > 0 {
			want[columnVectorIndexStateAssetRoleQuantizedCodes] = quantized
		}
	}
	got := make(map[string]int)
	for _, asset := range assets {
		got[asset.Role]++
		if asset.Role == columnVectorIndexStateAssetRoleRowRefs && asset.AssetID != columnVectorGraphRowRefStateAssetID(columnVectorGraphRowRefStateFieldOrdinalByPhysicalRow) {
			t.Errorf("selected writer emitted duplicate forward ref %q", asset.AssetID)
		}
	}
	if !maps.Equal(got, want) {
		t.Errorf("selected asset inventory=%v want=%v", got, want)
	}
}

func TestTypedGraphBaseCaptureInlineAdmissionBeforeWAL(t *testing.T) {
	dir, db, _ := openTypedMinimaCollection(t)
	defer db.Close()
	meta := typedMinimaCollectionMeta() // No active/recovery manifest yet.
	meta.Name = "wide-control"
	found := false
	for n := 0; n < 64; n++ {
		name := fmt.Sprintf("extra_%02d", n)
		column := meta.Options.ColumnStore.Columns[1]
		column.Name, column.Path = name, name
		meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, column)
		normalized, err := normalizeCollectionMeta(meta)
		if err != nil {
			t.Fatalf("invalid inline-boundary fixture: %v", err)
		}
		meta = normalized
		if _, err := typedGraphBaseCaptureAdmission(meta); err != nil {
			if !strings.Contains(err.Error(), "inline publication budget") {
				t.Fatalf("unexpected capture admission failure: %v", err)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("failed to construct bounded inline rejection")
	}
	manager := NewCollectionManager(db)
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatalf("original metadata should fit before larger capture control: %v", err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	before := countCollectionCommandWALFrames(t, dir)
	if _, err := col.RebuildVectorIndex("embedding_graph"); err == nil || errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("oversized capture must reject before admission: %v", err)
	}
	if after := countCollectionCommandWALFrames(t, dir); after != before {
		t.Fatalf("oversized capture appended frame: before=%d after=%d", before, after)
	}
}

func testTypedGraphBaseAliasControlledFixtureReopen(t *testing.T, direct bool) {
	meta := typedMinimaCollectionMeta()
	meta.Options.DisableIndexedWriteMemtables = direct
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer func() { _ = db.Close() }()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer snap.Close()
	_, found, err := getSystemValue(snap, "collections/typed-graph-base/v1/minima")
	if err != nil || !found {
		t.Fatalf("rebuild did not capture base: found=%v err=%v", found, err)
	}
	catalog, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	control, err := encodeTypedGraphBaseControl(catalog.meta)
	if err != nil {
		t.Fatal(err)
	}
	names, err := typedGraphBaseRootNames(catalog.meta)
	if err != nil {
		t.Fatal(err)
	}
	updates := map[string][]byte{typedGraphBaseControlPrefix + col.Name(): control}
	updates["typed-graph-base-fixture/unrelated"] = []byte("preserved")
	for _, name := range names {
		updates[systemCollectionRootKey(typedGraphBaseAliasRootName(col.Name(), name))] = encodeRootID(catalog.typedGraphBase.roots[name])
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, catalog.rootID(collectionColumnManifestRootName(col.Name())))
	if err != nil {
		t.Fatal(err)
	}
	cfg := catalog.meta.Options.ColumnStore
	requirements, err := stableColumnManifestDurableRequirements(records, cfg.ActiveManifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	// This test installs an explicit fixture through the ordinary root publisher.
	// Its empty update intent is NOT a replayable capture command: checkpoint
	// and normal reopen below test the cold loader, not automatic lifecycle/WAL.
	installFixture := func(requirements rootpublication.StableLogicalObligationRequirements) error {
		intent, err := col.newCollectionUpdateCommandWALIntent(nil, nil)
		if err != nil {
			return err
		}
		_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(ctx backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
			if err := ctx.RegisterDurableLogicalObligationRequirements(requirements); err != nil {
				return nil, err
			}
			return buildSystemDeltaIterator(updates)
		})
		return err
	}
	err = installFixture(requirements)
	_ = snap.Close()
	if err != nil {
		t.Fatal(err)
	}
	columns[0].Float32Vectors[0] = []float32{0, 1, 0, 0, 0, 0, 0, 0}
	columns[1].Strings[0], columns[2].Strings[0] = "changed", "new-tenant"
	if _, err := col.ReplaceTypedBatch([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	// Do not rebuild here: automatic rebuild now replaces the captured base.
	// This fixture exercises the unchanged-base alias source independently.
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	snap = db.AcquireSnapshot()
	defer snap.Close()
	current, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	base, err := loadTypedGraphBaseAlias(snap, current.meta)
	if err != nil || base == nil {
		t.Fatalf("load base after reopen: %v", err)
	}
	old, err := base.catalog(col, snap)
	if err != nil {
		t.Fatal(err)
	}
	currentRecords, err := loadColumnManifestRecordsFromRoot(snap, current.rootID(collectionColumnManifestRootName(col.Name())))
	if err != nil {
		t.Fatal(err)
	}
	currentCfg := current.meta.Options.ColumnStore
	currentRequirements, err := stableColumnManifestDurableRequirements(currentRecords, currentCfg.ActiveManifest.Generation, currentCfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	var capturedRefs []ColumnAssetRef
	for _, original := range requirements.Obligations {
		capturedRefs = append(capturedRefs, ColumnAssetRef{Kind: ColumnAssetKind(original.Kind), Namespace: original.Namespace, Generation: original.Generation, PartID: original.PartID, FileID: uint32(original.FileID), Offset: original.Offset, Length: original.Length, Checksum: original.Checksum})
	}
	if len(capturedRefs) == 0 {
		t.Fatal("fixture has no captured external references")
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true, CandidateRefs: capturedRefs})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range capturedRefs {
		protected := false
		for _, entry := range plan.Entries {
			if entry.Ref == ref && entry.Status == ColumnAssetReachabilityProtected && slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePinnedSnapshot) {
				protected = true
			}
		}
		if !protected {
			t.Fatalf("captured reference missing independent alias protection after reopen: %+v", ref)
		}
	}
	for _, pair := range []struct {
		catalog *collectionCatalog
		content string
	}{{old, "original"}, {current, "changed"}} {
		view := newCollectionReadViewAtSnapshot(col, snap, pair.catalog, false, "")
		response, err := view.FetchDocumentsByID([][]byte{[]byte("base")}, DocumentFetchOptions{})
		_ = view.Close()
		if err != nil || len(response.Results) != 1 || !response.Results[0].Found || !bytes.Contains(response.Results[0].Document, []byte(pair.content)) {
			t.Fatalf("captured/current materialization %q: %+v err=%v", pair.content, response.Results, err)
		}
	}
	for _, captured := range []bool{true, false} {
		lookupCatalog := current
		if captured {
			lookupCatalog = old
		}
		lookup := hybridScalarLookupView{snapshot: snap, catalog: lookupCatalog}
		for _, tenant := range []string{"tenant", "new-tenant"} {
			set, _, truncated, err := lookup.leafProbe(HybridScalarFilter{IndexName: "user", Value: tenant}, 2)
			want := captured == (tenant == "tenant")
			_, found := set["base"]
			if err != nil || truncated || found != want {
				t.Fatalf("captured=%v tenant=%q: ids=%v truncated=%v err=%v", captured, tenant, set, truncated, err)
			}
		}
	}
	for _, role := range names {
		broken := *base
		broken.roots = make(map[string]uint64, len(base.roots))
		for name, root := range base.roots {
			broken.roots[name] = root
		}
		broken.roots[role] = 0
		if _, err := broken.catalog(col, snap); err == nil {
			t.Fatalf("accepted missing nonempty base root %q", role)
		}
	}
	// Exact destructive plans must not claim removal of a reference still in
	// the alias union. Append preparation must not eagerly run its fallback.
	// Remove captured obligations from this synthetic current requirement set:
	// the lower exact-union seam must supply them solely from persistent aliases.
	exactCurrent := currentRequirements
	exactCurrent.Obligations = nil
	for _, obligation := range currentRequirements.Obligations {
		if !slices.Contains(requirements.Obligations, obligation) {
			exactCurrent.Obligations = append(exactCurrent.Obligations, obligation)
		}
	}
	exact := &columnPublishPlanLease{collection: col, plan: ColumnPublishPlan{durableResourceRequirements: exactCurrent, durableResourceMutation: rootpublication.StableLogicalObligationMutation{Removed: requirements.Obligations}}}
	if err := col.bindTypedGraphBasePlanClosure(exact, base); err != nil {
		t.Fatal(err)
	}
	if len(exact.plan.durableResourceMutation.Removed) != 0 {
		t.Fatal("retained-base removal evidence survived exact union")
	}
	for _, required := range requirements.Obligations {
		found := false
		for _, actual := range exact.plan.durableResourceRequirements.Obligations {
			if actual == required {
				found = true
			}
		}
		if !found {
			t.Fatal("exact union omitted captured reference")
		}
	}
	fallbackCalls := 0
	appendPlan := &columnPublishPlanLease{collection: col, plan: ColumnPublishPlan{durableResourceRequirementsFallback: func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		fallbackCalls++
		return currentRequirements, rootpublication.StableResourceClosureWork{}, nil
	}}}
	if err := col.bindTypedGraphBasePlanClosure(appendPlan, base); err != nil {
		t.Fatal(err)
	}
	if fallbackCalls != 0 {
		t.Fatal("append preparation eagerly scanned exact closure")
	}
	union, _, err := appendPlan.plan.durableResourceRequirementsFallback()
	if err != nil || fallbackCalls != 1 || len(union.Obligations) != len(exact.plan.durableResourceRequirements.Obligations) {
		t.Fatalf("append fallback union: %v", err)
	}
	// A metadata owner is not a page-ID lease. Even deliberately stale cached
	// root IDs are ignored by cold closure refresh through current descriptors.
	staleRoots := *base
	staleRoots.roots = map[string]uint64{}
	if _, _, err := col.typedGraphBaseRequirements(&staleRoots); err != nil {
		t.Fatalf("cold descriptor refresh reused stale owner roots: %v", err)
	}
	_ = snap.Close() // No process read-view pin may be the reason GC retains refs.
	gc, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{Detailed: true, CandidateRefs: capturedRefs})
	if err != nil || gc.SegmentsDeleted != 0 {
		t.Fatalf("captured-ref GC: %+v err=%v", gc, err)
	}
	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{Detailed: true, CandidateRefs: capturedRefs})
	if err != nil || rewrite.RefsEligible != 0 {
		t.Fatalf("captured-ref rewrite: %+v err=%v", rewrite, err)
	}
	if _, err := col.DropVectorIndex("embedding_graph"); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("captured schema drop crossed existing barrier: %v", err)
	}
	verify := db.AcquireSnapshot()
	defer verify.Close()
	unchanged, err := loadCollectionCatalog(verify, col.Name())
	if err != nil || unchanged == nil || unchanged.typedGraphBase == nil {
		t.Fatalf("rejected drop changed captured catalog: %v", err)
	}
	if _, err := unchanged.typedGraphBase.catalog(col, verify); err != nil {
		t.Fatalf("maintenance/drop invalidated captured graph: %v", err)
	}
	_ = verify.Close()
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("appended%d", i)
		if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte(id)}, [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, id))}, columns); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		work := col.LastInsertStats().ColumnPublishFinalizeCandidateResourceWork
		t.Logf("captured-base append %d closure work: %+v", i, work)
		// Buffered LastInsertStats covers admission, not the subsequent flush;
		// the direct supported debug route exposes actual publication work.
		if direct && i == 2 && (work.FinalRequirementProofFastPath != 1 || work.FinalRequirementProofFallbacks != 0 || work.FinalRequirementObligationsMaterialized != 0) {
			t.Fatalf("unchanged-base append did not regain mutation-local certification: %+v", work)
		}
	}
	testTypedGraphBaseSchemaCleanup(t, col, direct)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	reopened := db.AcquireSnapshot()
	defer reopened.Close()
	after, err := loadCollectionCatalog(reopened, col.Name())
	if err != nil || after == nil || after.typedGraphBase != nil {
		t.Fatalf("schema cleanup reopen: catalog=%v err=%v", after, err)
	}
}

func testTypedGraphBaseSchemaCleanup(t *testing.T, col *Collection, publishBackfill bool) {
	t.Helper()
	snap := col.db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	changed := copyCollectionMeta(catalog.meta)
	changed.VectorIndexes = nil
	encoded, err := encodeCollectionMeta(changed)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{typedGraphBaseControlPrefix + col.Name()}
	for root := range catalog.typedGraphBase.roots {
		keys = append(keys, systemCollectionRootKey(typedGraphBaseAliasRootName(col.Name(), root)))
	}
	for _, backfill := range []bool{false, true} {
		var it iterator.UnsafeIterator
		if backfill {
			it, err = col.buildSchemaAndRootDescriptorSystemIterator(catalog.meta, changed, nil, nil, nil)
		} else {
			it, err = col.buildSchemaOnlySystemDeltaIterator(catalog.meta, encoded, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			it.Seek([]byte(key))
			if !it.Valid() || string(it.UnsafeKey()) != key || !it.IsDeleted() {
				t.Fatalf("schema backfill=%v did not tombstone %q", backfill, key)
			}
		}
		_ = it.Close()
	}
	// The schema operation itself remains unsupported by public command WAL.
	// Publish only this controlled fixture through the existing command context
	// to prove actual deletion and catalog invalidation, not replay semantics.
	intent, err := col.newCollectionUpdateCommandWALIntent(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, catalog.rootID(collectionColumnManifestRootName(col.Name())))
	if err != nil {
		t.Fatal(err)
	}
	cfg := catalog.meta.Options.ColumnStore
	requirements, err := stableColumnManifestDurableRequirements(records, cfg.ActiveManifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	systemRoot, _, err := col.db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(ctx backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		if err := ctx.RegisterDurableLogicalObligationRequirements(requirements); err != nil {
			return nil, err
		}
		if publishBackfill {
			return col.buildSchemaAndRootDescriptorSystemIterator(catalog.meta, changed, nil, nil, nil)
		}
		return col.buildSchemaOnlySystemDeltaIterator(catalog.meta, encoded, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	col.meta = changed
	next := cloneCatalogAfterSchemaChange(catalog, changed, nil, nil)
	if next.typedGraphBase != nil || catalog.typedGraphBase == nil {
		t.Fatal("schema clone retained owner or mutated pinned source")
	}
	col.rememberCatalogAtSystemRoot(systemRoot, next)
	col.noteWriteDomainCatalog(systemRoot, next)
	current := col.db.AcquireSnapshot()
	defer current.Close()
	if value, found, err := getSystemValue(current, "typed-graph-base-fixture/unrelated"); err != nil || !found || string(value) != "preserved" {
		t.Fatalf("schema cleanup lost unrelated system key: found=%v value=%q err=%v", found, value, err)
	}
	for _, key := range keys {
		if _, found, err := getSystemValue(current, key); err != nil || found {
			t.Fatalf("schema cleanup left key %q found=%v err=%v", key, found, err)
		}
	}
	for _, load := range []func(*backenddb.Snapshot) (*collectionCatalog, error){col.catalogForSnapshot, func(s *backenddb.Snapshot) (*collectionCatalog, error) { return loadCollectionCatalog(s, col.Name()) }} {
		after, err := load(current)
		if err != nil || after == nil || after.typedGraphBase != nil {
			t.Fatalf("schema cleanup catalog: %v", err)
		}
		for root, id := range catalog.roots {
			if after.rootID(root) != id {
				t.Fatalf("schema cleanup changed unrelated root %q", root)
			}
		}
	}
}
