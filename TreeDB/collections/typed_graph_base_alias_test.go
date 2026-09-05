package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
	dir, db, col := openTypedMinimaCollection(t)
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
}
