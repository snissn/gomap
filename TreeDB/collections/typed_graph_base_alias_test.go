package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

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
	if err != nil || found {
		t.Fatalf("automatic capture must remain gated until closure integration: found=%v err=%v", found, err)
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
	for _, name := range names {
		updates[systemCollectionRootKey(typedGraphBaseAliasRootName(col.Name(), name))] = encodeRootID(catalog.rootID(name))
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
	intent, err := col.newCollectionUpdateCommandWALIntent(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(ctx backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		if err := ctx.RegisterDurableLogicalObligationRequirements(requirements); err != nil {
			return nil, err
		}
		return buildSystemDeltaIterator(updates)
	})
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
	// Rebuilding current graph state retires prior graph/TVIS references from
	// the current manifest; the captured base remains their independent owner.
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
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
	var baseOnly []ColumnAssetRef
	for _, original := range requirements.Obligations {
		shared := false
		for _, live := range currentRequirements.Obligations {
			if original == live {
				shared = true
				break
			}
		}
		if !shared {
			baseOnly = append(baseOnly, ColumnAssetRef{Kind: ColumnAssetKind(original.Kind), Namespace: original.Namespace, Generation: original.Generation, PartID: original.PartID, FileID: uint32(original.FileID), Offset: original.Offset, Length: original.Length, Checksum: original.Checksum})
		}
	}
	if len(baseOnly) == 0 {
		t.Fatal("fixture has no base-only external references")
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true, CandidateRefs: baseOnly})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range baseOnly {
		protected := false
		for _, entry := range plan.Entries {
			if entry.Ref == ref && entry.Status == ColumnAssetReachabilityProtected {
				protected = true
			}
		}
		if !protected {
			t.Fatalf("captured base-only reference is not persistently protected after reopen: %+v", ref)
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
	exact := &columnPublishPlanLease{collection: col, plan: ColumnPublishPlan{durableResourceRequirements: currentRequirements, durableResourceMutation: rootpublication.StableLogicalObligationMutation{Removed: requirements.Obligations}}}
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
	gc, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{Detailed: true, CandidateRefs: baseOnly})
	if err != nil || gc.SegmentsDeleted != 0 {
		t.Fatalf("base-only GC: %+v err=%v", gc, err)
	}
	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{Detailed: true, CandidateRefs: baseOnly})
	if err != nil || rewrite.RefsEligible != 0 {
		t.Fatalf("base-only rewrite: %+v err=%v", rewrite, err)
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
}
