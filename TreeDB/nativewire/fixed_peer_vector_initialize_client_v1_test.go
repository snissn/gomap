package nativewire

import (
	"context"
	"encoding/binary"
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
	config := FixedPeerTCPConfigV1{Credentials: &PeerCredentialsV1{}, Nodes: make([]FixedPeerTCPNodeV1, 3), Groups: make([]FixedPeerTCPGroupV1, 1), VectorInitialization: &FixedPeerTCPVectorInitializationV1{MaxSourceRows: 3, IndexDefinition: collections.VectorIndexDefinition{Field: "embedding", Dimensions: 2}}}
	for _, id := range []string{"", "slash/id", " space", "é"} {
		if validateFixedPeerFixtureV1(config, id) == nil {
			t.Fatalf("accepted %q", id)
		}
	}
	if err := validateFixedPeerFixtureV1(config, "run_1"); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 2, 4, 5, 6} {
		config.Nodes = make([]FixedPeerTCPNodeV1, count)
		err := validateFixedPeerFixtureV1(config, "run_1")
		if (err == nil) != (count == 4) {
			t.Fatalf("fixture node count %d: %v", count, err)
		}
	}
	config.Nodes = make([]FixedPeerTCPNodeV1, 3)
	config.VectorInitialization.MaxSourceRows = 2
	if validateFixedPeerFixtureV1(config, "run_1") == nil {
		t.Fatal("accepted insufficient preparation bound")
	}
	called := false
	if fixturePollV1(context.Background(), func() (bool, error) { called = true; return true, nil }) == nil || called {
		t.Fatal("unbounded operation reached work")
	}
}

func TestFixedPeerVectorFixtureRealRaftV1(t *testing.T) {
	runFixedPeerVectorFixtureRealRaftV1(t, 3)
}

func TestFixedPeerVectorFixtureRF4RealRaftV1(t *testing.T) {
	runFixedPeerVectorFixtureRealRaftV1(t, 4)
}

func runFixedPeerVectorFixtureRealRaftV1(t *testing.T, replicas int) {
	t.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("vector partition namespace persistence unsupported on this platform")
	}
	var configs []FixedPeerTCPConfigV1
	if replicas == 4 {
		configs = fourNodeInitializationTestConfigsV1(t)
	} else {
		configs = initializationTestConfigsV1(t)
	}
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
	if err != nil || len(qualified.Readiness) != len(configs) {
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
