package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This contract uses JSON rather than a not-yet-existing Go field so its
// baseline failure proves silently discarded initialization identity.
func TestFixedPeerVectorInitializationJSONIdentityV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.ClusterID = "vector-initialization"
	ca := newPeerCAFixtureV1(t)
	config.Credentials = ca.issue(t, config.ClusterID, string(config.NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	intent := map[string]any{
		"Collection":      raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
		"IndexDefinition": collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph},
		"CatalogEpoch":    uint64(1), "Generation": uint64(1), "MaxSourceRows": uint64(8),
	}
	object["VectorInitialization"], err = json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	var decoded FixedPeerTCPConfigV1
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var retained map[string]json.RawMessage
	if err := json.Unmarshal(roundTrip, &retained); err != nil {
		t.Fatal(err)
	}
	if len(retained["VectorInitialization"]) == 0 || string(retained["VectorInitialization"]) == "null" {
		t.Fatal("JSON discarded VectorInitialization: a fresh vector initialization intent is not retained or bound to persistent configuration identity")
	}
}

func initializationTestConfigsV1(t testing.TB) []FixedPeerTCPConfigV1 {
	t.Helper()
	configs := fixedPeerTestConfigsV1(t)
	group := configs[0].Groups[0]
	group.Peers = append(append([]raftcluster.Peer(nil), group.Peers...), configs[0].Groups[1].Peers...)
	features := cloneFixedPeerGroupV1(configs[0].Catalog).Features
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.Version{Major: 1}})
	catalog := cloneFixedPeerGroupV1(configs[0].Catalog)
	catalog.Features = features
	for i := range catalog.Peers {
		catalog.Peers[i].Capabilities = features
	}
	// Retain every role until the runtime adopts its exact reserved socket.
	address := func() string { return fixedPeerReserveTestAddressV1(t, nil) }
	intent := &FixedPeerTCPVectorInitializationV1{SourceGroupID: group.ID,
		Collection:      raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
		IndexDefinition: collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph},
		CatalogEpoch:    1, Generation: 1, MaxSourceRows: 8,
		PublicAddresses: map[raftcluster.NodeID]string{}, ShardAddresses: map[raftcluster.GroupID]map[raftcluster.NodeID]string{group.ID: {}},
	}
	for _, node := range configs[0].Nodes {
		intent.PublicAddresses[node.ID] = address()
		intent.ShardAddresses[group.ID][node.ID] = address()
	}
	ca := newPeerCAFixtureV1(t)
	for i := range configs {
		configs[i].Catalog = catalog
		configs[i].Groups = []FixedPeerTCPGroupV1{group}
		configs[i].RaftListen = map[raftcluster.GroupID]string{catalog.ID: configs[i].RaftListen[catalog.ID]}
		for _, peer := range group.Peers {
			if peer.ID == configs[i].NodeID {
				configs[i].RaftListen[group.ID] = peer.Address
			}
		}
		configs[i].ClusterID = "initialization-intent"
		configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		configs[i].VectorInitialization = cloneFixedPeerVectorInitializationV1(intent)
	}
	return configs
}

func TestFixedPeerVectorInitializationCloneDigestAndValidationV1(t *testing.T) {
	configs := initializationTestConfigsV1(t)
	normalized, digest, err := validateFixedPeerConfigV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs[1:] {
		_, shared, err := validateFixedPeerConfigV1(config)
		if err != nil || shared != digest {
			t.Fatalf("different shared identity: %s %s %v", shared, digest, err)
		}
	}
	explicit := configs[0]
	explicit.VectorInitialization = cloneFixedPeerVectorInitializationV1(explicit.VectorInitialization)
	explicit.VectorInitialization.IndexDefinition.Encoding = collections.VectorIndexEncodingFloat32
	_, same, err := validateFixedPeerConfigV1(explicit)
	if err != nil || same != digest {
		t.Fatalf("default encoding not canonical: %s %s %v", same, digest, err)
	}
	changed := configs[0]
	changed.VectorInitialization = cloneFixedPeerVectorInitializationV1(changed.VectorInitialization)
	changed.VectorInitialization.Generation++
	_, different, err := validateFixedPeerConfigV1(changed)
	if err != nil || different == digest {
		t.Fatalf("intent missing from digest: %v", err)
	}
	reordered := normalized
	reordered.Catalog = cloneFixedPeerGroupV1(normalized.Catalog)
	reordered.Groups = []FixedPeerTCPGroupV1{cloneFixedPeerGroupV1(normalized.Groups[0])}
	for i, j := 0, len(reordered.Catalog.Peers)-1; i < j; i, j = i+1, j-1 {
		reordered.Catalog.Peers[i], reordered.Catalog.Peers[j] = reordered.Catalog.Peers[j], reordered.Catalog.Peers[i]
	}
	for i, j := 0, len(reordered.Groups[0].Peers)-1; i < j; i, j = i+1, j-1 {
		reordered.Groups[0].Peers[i], reordered.Groups[0].Peers[j] = reordered.Groups[0].Peers[j], reordered.Groups[0].Peers[i]
	}
	_, canonical, err := validateFixedPeerConfigV1(reordered)
	if err != nil || canonical != digest {
		t.Fatalf("membership order changed identity: %v", err)
	}
	originalAddress := normalized.VectorInitialization.PublicAddresses[configs[0].NodeID]
	originalShard := normalized.VectorInitialization.ShardAddresses[normalized.VectorInitialization.SourceGroupID][configs[0].NodeID]
	configs[0].VectorInitialization.PublicAddresses[configs[0].NodeID] = "caller-mutated"
	configs[0].VectorInitialization.ShardAddresses[normalized.VectorInitialization.SourceGroupID][configs[0].NodeID] = "caller-mutated"
	configs[0].VectorInitialization.IndexDefinition.Dimensions = 9
	if normalized.VectorInitialization.PublicAddresses[normalized.NodeID] != originalAddress || normalized.VectorInitialization.ShardAddresses[normalized.VectorInitialization.SourceGroupID][normalized.NodeID] != originalShard || normalized.VectorInitialization.IndexDefinition.Dimensions != 2 {
		t.Fatal("caller mutation changed owned intent")
	}
	cases := map[string]func(*FixedPeerTCPConfigV1){
		"unauthenticated":        func(c *FixedPeerTCPConfigV1) { c.Credentials = nil },
		"ready-vector":           func(c *FixedPeerTCPConfigV1) { c.Vector = &FixedPeerTCPVectorConfigV1{} },
		"dormant-node":           func(c *FixedPeerTCPConfigV1) { c.RaftListen = nil },
		"missing-source":         func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.SourceGroupID = "absent" },
		"zero-generation":        func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.Generation = 0 },
		"wrong-epoch":            func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.CatalogEpoch = 2 },
		"zero-source-bound":      func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.MaxSourceRows = 0 },
		"oversized-source-bound": func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.MaxSourceRows = 513 },
		"quantized-definition": func(c *FixedPeerTCPConfigV1) {
			c.VectorInitialization.IndexDefinition.QuantizedIndexes = []collections.QuantizedVectorIndexDefinition{{Name: "unsupported"}}
		},
		"bad-field": func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.IndexDefinition.Field = "" },
		"bad-metric": func(c *FixedPeerTCPConfigV1) {
			c.VectorInitialization.IndexDefinition.Metric = collections.VectorMetric(255)
		},
		"bad-build-option":        func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.IndexDefinition.M = -1 },
		"extra-public-node":       func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.PublicAddresses["absent"] = "127.0.0.1:1" },
		"nonlocal-public-address": func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.PublicAddresses[c.NodeID] = "192.168.0.185:5555" },
		"duplicate-shard-address": func(c *FixedPeerTCPConfigV1) {
			c.VectorInitialization.ShardAddresses[c.VectorInitialization.SourceGroupID][c.NodeID] = c.ListenAddress
		},
		"bad-collection":      func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.Collection.Collection = "" },
		"nondefault-database": func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.Collection.Database = "tenant" },
		"nondefault-catalog":  func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.Collection.Catalog = "tenant" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			config := normalized
			config.VectorInitialization = cloneFixedPeerVectorInitializationV1(normalized.VectorInitialization)
			mutate(&config)
			if _, _, err := validateFixedPeerConfigV1(config); err == nil {
				t.Fatal("malformed intent admitted")
			}
		})
	}
	catalog := fixedPeerVectorInitializationCatalogV1(normalized)
	catalog.Groups[0].Members = catalog.Groups[0].Members[:2]
	if err := validateFixedPeerCatalogWithAuthorityV1(normalized, catalog, nil); err == nil {
		t.Fatal("noncanonical membership admitted")
	}
}

func TestFixedPeerVectorInitializationAuthenticatedShardAddressesV1(t *testing.T) {
	base := initializationTestConfigsV1(t)[0]
	cases := []struct {
		name          string
		shardAddress  string
		publicAddress string
		wantValid     bool
	}{
		{name: "private-ipv4", shardAddress: "192.168.0.185:20102", wantValid: true},
		{name: "private-ula", shardAddress: "[fd00::185]:20102", wantValid: true},
		{name: "loopback-ipv4", shardAddress: "127.0.0.2:20102", wantValid: true},
		{name: "loopback-ipv6", shardAddress: "[::1]:20102", wantValid: true},
		{name: "public-ipv4", shardAddress: "8.8.8.8:20102"},
		{name: "public-ipv6", shardAddress: "[2001:4860:4860::8888]:20102"},
		{name: "unspecified-ipv4", shardAddress: "0.0.0.0:20102"},
		{name: "unspecified-ipv6", shardAddress: "[::]:20102"},
		{name: "mapped-private", shardAddress: "[::ffff:192.168.0.185]:20102"},
		{name: "mapped-loopback", shardAddress: "[::ffff:127.0.0.1]:20102"},
		{name: "zero-port", shardAddress: "192.168.0.185:0"},
		{name: "noncanonical-ula", shardAddress: "[FD00::185]:20102"},
		{name: "duplicate-control", shardAddress: base.ListenAddress},
		{name: "duplicate-public", shardAddress: base.VectorInitialization.PublicAddresses[base.NodeID]},
		{name: "private-public-ipv4", shardAddress: "192.168.0.185:20102", publicAddress: "192.168.0.185:20101"},
		{name: "private-public-ula", shardAddress: "[fd00::185]:20102", publicAddress: "[fd00::185]:20101"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			config.VectorInitialization = cloneFixedPeerVectorInitializationV1(base.VectorInitialization)
			config.VectorInitialization.ShardAddresses[config.VectorInitialization.SourceGroupID][config.NodeID] = tc.shardAddress
			if tc.publicAddress != "" {
				config.VectorInitialization.PublicAddresses[config.NodeID] = tc.publicAddress
			}
			normalized, _, err := validateFixedPeerConfigV1(config)
			if tc.wantValid {
				if err != nil {
					t.Fatalf("authenticated private or loopback shard refused: %v", err)
				}
				if got := normalized.VectorInitialization.ShardAddresses[config.VectorInitialization.SourceGroupID][config.NodeID]; got != tc.shardAddress {
					t.Fatalf("shard address changed: got %q want %q", got, tc.shardAddress)
				}
			} else if err == nil {
				t.Fatal("invalid shard or nonloopback public address admitted")
			}
		})
	}
}

func TestFixedPeerVectorInitializationRootIdentityV1(t *testing.T) {
	configs := initializationTestConfigsV1(t)
	config, _, err := validateFixedPeerConfigV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	bind := func(c FixedPeerTCPConfigV1) error {
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		return preparePeerStorageV1(c, raw)
	}
	if err := bind(config); err != nil {
		t.Fatal(err)
	}
	if err := bind(config); err != nil {
		t.Fatalf("identical intent reopen: %v", err)
	}
	for _, change := range []func(*FixedPeerTCPConfigV1){
		func(c *FixedPeerTCPConfigV1) { c.VectorInitialization.Generation++ },
		func(c *FixedPeerTCPConfigV1) { c.VectorInitialization = nil },
	} {
		changed := config
		changed.VectorInitialization = cloneFixedPeerVectorInitializationV1(config.VectorInitialization)
		change(&changed)
		if err := bind(changed); err == nil {
			t.Fatal("changed intent adopted marked roots")
		}
	}
	unmarked := config
	unmarked.DataRoot = filepath.Join(t.TempDir(), "data")
	unmarked.RaftRoot = filepath.Join(t.TempDir(), "raft")
	if err := os.MkdirAll(unmarked.DataRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unmarked.DataRoot, "existing"), []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := bind(unmarked); err == nil {
		t.Fatal("unmarked nonempty root adopted")
	}
	legacy, _, err := validateFixedPeerConfigV1(fixedPeerTestConfigsV1(t)[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := bind(legacy); err != nil {
		t.Fatal(err)
	}
	legacy.VectorInitialization = config.VectorInitialization
	if err := bind(legacy); err == nil {
		t.Fatal("nil-intent marked root retrofitted")
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var mark peerStorageIdentityV1
	marker, err := os.ReadFile(filepath.Join(config.DataRoot, peerStorageMarkerV1))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(marker, &mark); err != nil {
		t.Fatal(err)
	}
	identity, err := InspectFixedPeerTCPConfigV1(config)
	if err != nil {
		t.Fatal(err)
	}
	if identity.LocalSHA256 != mark.LocalConfigSHA256 || !bytes.Contains(raw, []byte("VectorInitialization")) {
		t.Fatal("paired root omitted initialization identity")
	}
}

func TestFixedPeerVectorInitializationRealRaftCreateIngestReopenV1(t *testing.T) {
	configs := initializationTestConfigsV1(t)
	nodes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	reserved := make([]map[string]net.Listener, len(configs))
	open := func() {
		for i := range configs {
			var err error
			nodes[i], err = fixedPeerOpenTestRuntimeV1(t, configs[i])
			if err != nil {
				t.Fatal(err)
			}
			reserved[i] = make(map[string]net.Listener)
			for _, address := range []string{configs[i].VectorInitialization.PublicAddresses[configs[i].NodeID], configs[i].VectorInitialization.ShardAddresses[configs[i].Groups[0].ID][configs[i].NodeID]} {
				reserved[i][address] = fixedPeerReservedTestListenerV1(nodes[i], address)
				if reserved[i][address] == nil || reserved[i][address].Addr().String() != address {
					t.Fatalf("initialization did not reserve vector address %s", address)
				}
			}
		}
	}
	closeAll := func() {
		for i, node := range nodes {
			if node != nil {
				if err := node.Close(); err != nil {
					t.Error(err)
				}
				nodes[i] = nil
			}
		}
	}
	defer closeAll()
	open()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := nodes[0].client
	var leader raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			status, err := node.Status(ctx)
			if err == nil && status.CatalogRaft.State == "Leader" {
				leader = node.config.NodeID
				return true
			}
		}
		return false
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, fixedPeerVectorInitializationCatalogV1(nodes[0].config))
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, command); err != nil {
		t.Fatal(err)
	}
	group := configs[0].Groups[0]
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			status, err := node.Status(ctx)
			if err != nil || status.Catalog.Epoch != 1 || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	// A valid newer catalog still cannot alter the immutable initialization
	// epoch. Refusal must leave every replica on the committed epoch 1 record.
	nextRecord, err := raftplacement.NewCatalogMetaRecordV1(2, record.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	nextCommand, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: 1, Record: nextRecord})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, nextCommand); !errors.Is(err, raftplacement.ErrCatalogMetaStaleEpoch) {
		t.Fatalf("initialization epoch publication error: %v", err)
	}
	for _, node := range nodes {
		status, err := node.Status(ctx)
		if err != nil || status.Catalog.Epoch != 1 || status.Catalog.Digest != record.Digest {
			t.Fatalf("rejected publication changed catalog: %+v %v", status.Catalog, err)
		}
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "docs", Shape: ClusterRouteShapeCollection}
	submit := func(command iwire.CommandID, sections []iwire.Section) raftcluster.SubmitResultV1 {
		t.Helper()
		sections = append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})}}, sections...)
		validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := iwire.AppendDeterministicEntry(nil, validated)
		if err != nil {
			t.Fatal(err)
		}
		route, err := client.Route(ctx, configs[0].NodeID, request)
		if err != nil {
			t.Fatal(err)
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		result, err := client.Submit(ctx, configs[0].NodeID, entry, metadata)
		if err != nil || !result.CommittedRecoverable || !result.CommittedApplied {
			t.Fatalf("real Raft command %v result=%+v err=%v", command, result, err)
		}
		return result
	}
	owner, err := client.leader(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Status(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	meta := collections.CollectionMeta{Name: "docs", VectorIndexes: []collections.VectorIndexDefinition{nodes[0].config.VectorInitialization.IndexDefinition}}
	submit(iwire.CommandCreateCollection, raftClusterCreateCollectionSectionsWithMeta(meta, status.Groups[0].CatalogVersion, AckRaftCommitted))
	status, err = client.Status(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"embedding":[1,0],"kind":"real-raft-initialization"}`)
	submit(iwire.CommandInsertBatch, []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("initialization-fresh-insert")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, status.Groups[0].CatalogVersion)},
		collectionNameRef("docs"), documentFormatSection(collections.DocumentFormatJSON),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("fresh-document"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, document)}, ackSection(AckRaftCommitted),
	})
	check := func(reopened bool) {
		t.Helper()
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, node := range nodes {
				status, err := node.Status(ctx)
				if err != nil || len(status.Groups) != 1 || !status.Groups[0].Applied.HasApplied {
					return false
				}
				collection, err := collections.OpenCollectionForRaftSourceV1(node.data[group.ID].db, "docs")
				if err != nil {
					return false
				}
				got, err := collection.Get([]byte("fresh-document"))
				if err != nil || !bytes.Equal(got, document) {
					return false
				}
			}
			return true
		})
		for i, node := range nodes {
			status, err := node.Status(ctx)
			if err != nil || status.VectorPhase != FixedPeerVectorPhaseInitializingV1 || reopened && status.RecoveryState != "reopened" {
				t.Fatalf("initializing status: %+v %v", status, err)
			}
			report, err := client.ReadinessV1(ctx, node.config.NodeID)
			if err == nil || !report.Live || report.Ready || report.VectorPhase != FixedPeerVectorPhaseInitializingV1 {
				t.Fatalf("initialization became ready: %+v %v", report, err)
			}
			if node.vector != nil {
				t.Fatal("initialization opened vector runtime")
			}
			for _, address := range []string{node.config.VectorInitialization.PublicAddresses[node.config.NodeID], node.config.VectorInitialization.ShardAddresses[group.ID][node.config.NodeID]} {
				if fixedPeerReservedTestListenerV1(node, address) != reserved[i][address] {
					t.Fatalf("initialization replaced reserved vector socket %s", address)
				}
				fixedPeerAssertDormantRefusalV1(t, address)
			}
			for _, action := range []fixedPeerVectorLifecycleActionV1{fixedPeerVectorLifecycleEnsureImmutableV1, fixedPeerVectorLifecycleStageImmutableV1, fixedPeerVectorLifecycleWarmImmutableV1, fixedPeerVectorLifecycleCaptureSourceV1} {
				if _, err := client.call(ctx, node.config.NodeID, "vector-lifecycle", fixedPeerRequestV1{VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: action}}, true); err == nil {
					t.Fatalf("lifecycle %s admitted", action)
				}
			}
			for _, operation := range []struct {
				name string
				body fixedPeerRequestV1
			}{
				{"vector-forward", fixedPeerRequestV1{VectorInsert: &VectorPartitionRoutedInsertV1{}}},
				{"vector-search", fixedPeerRequestV1{VectorSearch: &public.SearchRequestV1{}}},
				{"vector-lifecycle", fixedPeerRequestV1{}},
				{"vector-catalog-read", fixedPeerRequestV1{VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorCatalogActiveV1}}},
				{"vector-split-project", fixedPeerRequestV1{}},
				{"vector-split-source-proof", fixedPeerRequestV1{}},
				{"vector-split-receipt", fixedPeerRequestV1{}},
			} {
				if _, err := client.call(ctx, node.config.NodeID, operation.name, operation.body, true); err == nil {
					t.Fatalf("%s admitted during initialization", operation.name)
				}
			}
			if _, err := node.applyVectorInsertV1(ctx, VectorPartitionRoutedInsertV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
				t.Fatalf("vector insert not unavailable: %v", err)
			}
		}
	}
	check(false)
	closeAll()
	open()
	client = nodes[0].client
	check(true)
}

// Build the previously admitted two-group shape without opening a runtime.
func sixNodeInitializationTestConfigV1(t *testing.T) FixedPeerTCPConfigV1 {
	t.Helper()
	first, second := initializationTestConfigsV1(t), initializationTestConfigsV1(t)
	config := first[0]
	config.Catalog = cloneFixedPeerGroupV1(config.Catalog)
	config.Nodes = append([]FixedPeerTCPNodeV1(nil), config.Nodes...)
	config.Groups = append([]FixedPeerTCPGroupV1(nil), config.Groups...)
	config.VectorInitialization = cloneFixedPeerVectorInitializationV1(config.VectorInitialization)
	other := cloneFixedPeerGroupV1(second[0].Groups[0])
	other.ID = "group-b"
	other.BootstrapNode += "-ann"
	addresses := map[raftcluster.NodeID]string{}
	for _, node := range second[0].Nodes {
		original := node.ID
		node.ID += "-ann"
		config.Nodes = append(config.Nodes, node)
		config.VectorInitialization.PublicAddresses[node.ID] = second[0].VectorInitialization.PublicAddresses[original]
		addresses[node.ID] = second[0].VectorInitialization.ShardAddresses[second[0].Groups[0].ID][original]
	}
	for i := range other.Peers {
		other.Peers[i].ID += "-ann"
	}
	config.Groups = append(config.Groups, other)
	config.VectorInitialization.ShardAddresses[other.ID] = addresses
	for _, peer := range second[0].Catalog.Peers {
		peer.ID += "-ann"
		config.Catalog.Peers = append(config.Catalog.Peers, peer)
	}
	// This test never opens a runtime. Use the existing static loopback
	// inventory pattern for the entire assembled layout, rather than combine
	// two independently released ephemeral-port reservation sets.
	for i, node := range config.Nodes {
		host := fmt.Sprintf("127.6.0.%d", i+1)
		config.Nodes[i].Address = host + ":19000"
		config.VectorInitialization.PublicAddresses[node.ID] = host + ":19003"
		for j := range config.Catalog.Peers {
			if config.Catalog.Peers[j].ID == node.ID {
				config.Catalog.Peers[j].Address = host + ":19001"
				if node.ID == config.NodeID {
					config.RaftListen[config.Catalog.ID] = host + ":19001"
				}
			}
		}
		for j := range config.Groups {
			for k := range config.Groups[j].Peers {
				if config.Groups[j].Peers[k].ID == node.ID {
					config.Groups[j].Peers[k].Address = host + ":19002"
					config.VectorInitialization.ShardAddresses[config.Groups[j].ID][node.ID] = host + ":19004"
					if node.ID == config.NodeID {
						config.RaftListen[config.Groups[j].ID] = host + ":19002"
					}
				}
			}
		}
		if node.ID == config.NodeID {
			config.ListenAddress = config.Nodes[i].Address
		}
	}
	return config
}

func TestFixedPeerVectorInitializationSixNodeLayoutV1(t *testing.T) {
	config := sixNodeInitializationTestConfigV1(t)
	if _, _, err := validateFixedPeerConfigV1(config); err == nil {
		t.Fatal("two-group initialization admitted before prepare and serving support it")
	}
	config.VectorInitialization = nil
	config.Catalog.Features = raftcluster.DefaultFeatureSet()
	config.Catalog.Features.Required = append(config.Catalog.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.Version{Major: 1}})
	for i := range config.Catalog.Peers {
		config.Catalog.Peers[i].Capabilities = config.Catalog.Features
	}
	if _, _, err := validateFixedPeerConfigV1(config); err != nil {
		t.Fatalf("ordinary six-node configuration rejected: %v", err)
	}
}

func TestFixedPeerVectorInitializationRefusesReplicaReplacementV1(t *testing.T) {
	config := sixNodeInitializationTestConfigV1(t)
	intent := config.VectorInitialization
	catalog := cloneFixedPeerGroupV1(config.Catalog)
	config.VectorInitialization = nil
	config.Catalog.Features = raftcluster.DefaultFeatureSet()
	config.Catalog.Features.Required = append(config.Catalog.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.Version{Major: 1}})
	for i := range config.Catalog.Peers {
		config.Catalog.Peers[i].Capabilities = config.Catalog.Features
	}
	ordinary, digest, err := validateFixedPeerConfigV1(config)
	if err != nil {
		t.Fatal(err)
	}
	group := ordinary.Groups[0]
	foreign := ordinary.Groups[1].Peers[0]
	command := replacementReceiverTestBeginV1(t)
	command.ConfigDigest = digest
	command.GroupID = group.ID
	command.OldNodeID = group.Peers[0].ID
	command.NewPeer = raftcluster.Peer{ID: foreign.ID, Address: foreign.Address}
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		t.Fatal(err)
	}
	r := &FixedPeerTCPRuntimeV1{
		config: ordinary,
		client: &FixedPeerTCPClientV1{digest: digest, addresses: map[raftcluster.NodeID]string{foreign.ID: foreign.Address}},
		data:   make(map[raftcluster.GroupID]*fixedPeerDataV1),
	}
	if _, err := r.validateReplacementBeginV1(command); err != nil {
		t.Fatalf("ordinary preauthorized replacement fixture rejected: %v", err)
	}
	// Retain the formerly admitted init shape to exercise runtime refusal
	// independently of the new two-group config validation gate.
	r.config.VectorInitialization = intent
	r.config.Catalog = catalog
	if _, err := r.validateReplacementBeginV1(command); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
		t.Fatalf("initialization replacement admitted: %v", err)
	}
	ctx := context.Background()
	for _, operation := range []string{"begin", "prepare", "enroll", "promotion-intent", "promote", "removal-proof", "removal-intent", "remove", "complete", "reconcile"} {
		t.Run(operation, func(t *testing.T) {
			var reply fixedPeerReplyV1
			if err := r.handleReplacementV1(ctx, "/v1/replacement-"+operation, raw, &reply); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
				t.Fatalf("initialization %s: %v", operation, err)
			}
			if reply.ReplacementState != nil || reply.Membership != nil || len(r.data) != 0 || len(r.transports) != 0 {
				t.Fatalf("initialization %s published replacement state: %+v", operation, reply)
			}
		})
	}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"prepare", func() error { return r.prepareReplacementV1(ctx, command) }},
		{"remove", func() error { return r.removeReplacementV1(ctx, command, &fixedPeerReplyV1{}) }},
		{"complete", func() error { return r.completeReplacementV1(ctx, command, &fixedPeerReplyV1{}) }},
		{"reconcile", func() error { return r.reconcileReplacementV1(ctx, command) }},
	} {
		t.Run("direct-"+tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
				t.Fatalf("direct initialization %s: %v", tc.name, err)
			}
			if len(r.data) != 0 || len(r.transports) != 0 {
				t.Fatalf("direct initialization %s changed local runtime", tc.name)
			}
		})
	}
}
