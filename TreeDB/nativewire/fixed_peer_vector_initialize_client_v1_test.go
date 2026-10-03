package nativewire

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestFixedPeerVectorFixturePhysicalCreateV1(t *testing.T) {
	intent := &FixedPeerTCPVectorInitializationV1{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}, IndexDefinition: collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, Strategy: collections.VectorIndexStrategyColumnGraph}}
	meta, err := fixtureMetaV1(intent)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCollectionMeta(raw)
	if err != nil {
		t.Fatal(err)
	}
	cfg := decoded.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.ActiveManifest != nil || cfg.RecoveryAuthoritativeManifest != nil || len(cfg.Columns) != 2 {
		t.Fatalf("physical create schema %+v", cfg)
	}
	found := false
	for _, column := range cfg.Columns {
		if column.Name == "embedding" {
			found = column.VectorDims == 2 && column.Owner == collections.TypedStorageOwnerColumnPart && column.ValueType == collections.ColumnStoreValueFloat32Vector
		}
	}
	if !found {
		t.Fatal("missing actual typed physical vector column")
	}
	entry, err := fixtureEntryV1(iwire.CommandCreateCollection, []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("fixture/create")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 0)},
		{ID: iwire.SectionCollectionMeta, Bytes: raw}, ackSection(AckRaftCommitted)})
	if err != nil || len(entry) == 0 {
		t.Fatalf("production deterministic create: %v", err)
	}
}
func TestFixedPeerVectorFixtureBoundsBeforeNetworkV1(t *testing.T) {
	config := initializationTestConfigsV1(t)[0]
	client, err := NewFixedPeerTCPClientV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := validateFixedPeerFixtureV1(client.config, "run_1"); err != nil {
		t.Fatalf("normalized FP32 fixture rejected: %v", err)
	}
	for _, id := range []string{"", "slash/id", " space", "é"} {
		if validateFixedPeerFixtureV1(config, id) == nil {
			t.Fatalf("accepted %q", id)
		}
	}
	for _, bound := range []uint64{3, 4, 512} {
		config.VectorInitialization.MaxSourceRows = bound
		if err := validateFixedPeerFixtureV1(config, "run_1"); err != nil {
			t.Fatalf("bound=%d: %v", bound, err)
		}
	}
	for _, bound := range []uint64{2, 513} {
		config.VectorInitialization.MaxSourceRows = bound
		if validateFixedPeerFixtureV1(config, "run_1") == nil {
			t.Fatalf("accepted preparation bound %d", bound)
		}
	}
	called := false
	if fixturePollV1(context.Background(), func() (bool, error) { called = true; return true, nil }) == nil || called {
		t.Fatal("unbounded operation reached work")
	}
}

func TestFixedPeerVectorFixtureCanonicalIntentBeforeNetworkV1(t *testing.T) {
	config := initializationTestConfigsV1(t)[0]
	// These valid production intents must still be refused by both standalone
	// fixture modes. A canceled context prevents network activity on regression;
	// checking the fixture error and empty stage proves admission happened first.
	for _, tc := range []struct {
		name   string
		change func(*FixedPeerTCPVectorInitializationV1)
	}{
		{"generation", func(v *FixedPeerTCPVectorInitializationV1) { v.Generation = 2 }},
		{"collection", func(v *FixedPeerTCPVectorInitializationV1) { v.Collection.Collection = "other" }},
		{"name", func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.Name = "other" }},
		{"field", func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.Field = "other" }},
		{"dimensions", func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.Dimensions = 3 }},
		{"m", func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.M = 3 }},
		{"ef-construction", func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.EfConstruction = 9 }},
		{"ef-search", func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.EfSearch = 9 }},
		{"source-bound", func(v *FixedPeerTCPVectorInitializationV1) { v.MaxSourceRows = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := config
			changed.VectorInitialization = cloneFixedPeerVectorInitializationV1(config.VectorInitialization)
			tc.change(changed.VectorInitialization)
			client, err := NewFixedPeerTCPClientV1(changed)
			if err != nil {
				t.Fatalf("valid production intent rejected before fixture admission: %v", err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			initialized, err := client.InitializeVectorFixtureV1(ctx, "canonical")
			if err == nil || !strings.Contains(err.Error(), "fixture requires canonical") || initialized.Stage != "" {
				t.Fatalf("initialize reached work: stage=%q err=%v", initialized.Stage, err)
			}
			if _, err := client.QualifyVectorFixtureV1(ctx, "canonical"); err == nil || !strings.Contains(err.Error(), "fixture requires canonical") {
				t.Fatalf("qualify reached work: %v", err)
			}
		})
	}
	for _, change := range []func(*FixedPeerTCPVectorInitializationV1){
		func(v *FixedPeerTCPVectorInitializationV1) { v.CatalogEpoch = 2 },
		func(v *FixedPeerTCPVectorInitializationV1) { v.Collection.Database = "other" },
		func(v *FixedPeerTCPVectorInitializationV1) { v.Collection.Catalog = "other" },
		func(v *FixedPeerTCPVectorInitializationV1) { v.SourceGroupID = "other" },
		func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.Metric = collections.VectorMetricL2 },
		func(v *FixedPeerTCPVectorInitializationV1) { v.IndexDefinition.Strategy = "" },
	} {
		changed := config
		changed.VectorInitialization = cloneFixedPeerVectorInitializationV1(config.VectorInitialization)
		change(changed.VectorInitialization)
		if validateFixedPeerFixtureV1(changed, "canonical") == nil {
			t.Fatalf("accepted noncanonical intent: %+v", changed.VectorInitialization)
		}
	}
}

func TestFixedPeerVectorFixtureRealRaftV1(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("vector partition namespace persistence unsupported on this platform")
	}
	configs := initializationTestConfigsV1(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	nodes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	closeAll := func() {
		for i, node := range nodes {
			if node != nil {
				if err := node.Close(); err != nil {
					t.Errorf("close node %d: %v", i, err)
				}
				nodes[i] = nil
			}
		}
	}
	defer closeAll()
	openAll := func() {
		t.Helper()
		for i := range configs {
			node, err := fixedPeerOpenTestRuntimeV1(t, configs[i])
			if err != nil {
				t.Fatal(err)
			}
			nodes[i] = node
		}
	}
	openAll()
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	initialized, err := client.InitializeVectorFixtureV1(ctx, "operator-fixture")
	client.Close()
	if err != nil || initialized.Stage != "prepared-restart-required" {
		t.Fatalf("initialize=%+v err=%v", initialized, err)
	}
	closeAll()
	if t.Failed() {
		return
	}
	openAll()
	client, err = NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	qualified, err := client.QualifyVectorFixtureV1(ctx, "operator-fixture")
	if err != nil || len(qualified.Readiness) != 3 {
		t.Fatalf("qualify=%+v err=%v", qualified, err)
	}
}

func TestFixedPeerVectorFixtureNativeGraphSearchV1(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		selected, hnsw, exact uint64
		valid                 bool
	}{
		{"native", 1, 1, 0, true},
		{"multiple-native", 2, 2, 0, true},
		{"empty", 0, 0, 0, false},
		{"exact-fallback", 1, 0, 1, false},
		{"mixed", 2, 2, 1, false},
		{"missing-native", 2, 1, 0, false},
		{"excess-native", 1, 2, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := public.SearchResponseV1{Counters: public.SearchCountersV1{SelectedPartitions: tc.selected, HNSWServedPartitions: tc.hnsw, ExactScanPartitions: tc.exact}, Neighbors: []public.NeighborV1{{ID: "seed-x", Score: 1}}}
			if err := fixtureNativeGraphSearchV1(response); (err == nil) != tc.valid {
				t.Fatalf("valid=%v response=%+v err=%v", tc.valid, response, err)
			}
		})
	}
}
