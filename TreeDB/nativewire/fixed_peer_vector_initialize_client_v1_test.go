package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
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
	config = client.config
	if err := validateFixedPeerFixtureV1(client.config, "run_1"); err != nil {
		t.Fatalf("normalized FP32 fixture rejected: %v", err)
	}
	for _, id := range []string{"", "slash/id", " space", "é"} {
		if validateFixedPeerFixtureV1(config, id) == nil {
			t.Fatalf("accepted %q", id)
		}
	}
	for _, count := range []int{1, 2, 4, 5, 6} {
		changed := config
		changed.Nodes = make([]FixedPeerTCPNodeV1, count)
		if err := validateFixedPeerFixtureV1(changed, "run_1"); (err == nil) != (count == 4) {
			t.Fatalf("fixture node count %d: %v", count, err)
		}
	}
	for _, bound := range []uint64{3, 4, 512, 10003, 16384} {
		config.VectorInitialization.MaxSourceRows = bound
		if err := validateFixedPeerFixtureV1(config, "run_1"); err != nil {
			t.Fatalf("bound=%d: %v", bound, err)
		}
	}
	for _, bound := range []uint64{2, 16385} {
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
	runFixedPeerVectorFixtureRealRaftV1(t, "three-row", 3)
}

func TestFixedPeerVectorDatasetChunkAmbiguityStopsRealRaftV1(t *testing.T) {
	runFixedPeerVectorFixtureRealRaftV1(t, "dataset-ambiguous", 3)
}

func TestFixedPeerVectorDatasetSameCountReplacementRefusedRealRaftV1(t *testing.T) {
	runFixedPeerVectorFixtureRealRaftV1(t, "dataset-replaced", 4)
}
func TestFixedPeerVectorDatasetRF4RealRaftReopenV1(t *testing.T) {
	runFixedPeerVectorFixtureRealRaftV1(t, "dataset", 4)
}

func TestFixedPeerVectorFixtureRF4RealRaftV1(t *testing.T) {
	runFixedPeerVectorFixtureRealRaftV1(t, "three-row", 4)
}

func TestFixedPeerVectorFixturePreparedRowsRealRaftV1(t *testing.T) {
	for _, mode := range []string{"initialize-four-row", "standalone-four-row"} {
		t.Run(mode, func(t *testing.T) { runFixedPeerVectorFixtureRealRaftV1(t, mode, 3) })
	}
}

func fixtureStatusClientV1(t *testing.T, client *FixedPeerTCPClientV1, edit func(*fixedPeerReplyV1)) *FixedPeerTCPClientV1 {
	t.Helper()
	copyClient := *client
	copyHTTP := *client.readHTTP
	base, ok := client.readHTTP.Transport.(*http.Transport)
	if !ok {
		t.Fatal("fixture requires actual authenticated HTTP transport")
	}
	copyHTTP.Transport = splitInsertRoundTripperV1{Transport: base, roundTrip: func(request *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(request)
		if err != nil || request.URL.Path != "/v1/vector-prepare-status" {
			return response, err
		}
		raw, err := io.ReadAll(response.Body)
		if closeErr := response.Body.Close(); err != nil || closeErr != nil {
			t.Fatalf("read actual preparation status: %v %v", err, closeErr)
		}
		var reply fixedPeerReplyV1
		if err := json.Unmarshal(raw, &reply); err != nil {
			t.Fatal(err)
		}
		edit(&reply)
		raw, err = json.Marshal(reply)
		if err != nil {
			t.Fatal(err)
		}
		response.Body = io.NopCloser(bytes.NewReader(raw))
		response.ContentLength = int64(len(raw))
		response.Header.Set("Content-Length", fmt.Sprint(len(raw)))
		return response, nil
	}}
	copyClient.readHTTP = &copyHTTP
	return &copyClient
}

func fixtureExtraRowV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1) {
	t.Helper()
	group := client.config.Groups[0]
	owner, err := client.leader(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	state, err := client.Status(ctx, owner)
	if err != nil || len(state.Groups) != 1 {
		t.Fatalf("source status: %v %+v", err, state)
	}
	entry, err := fixtureEntryV1(iwire.CommandInsertBatch, []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("foreign-extra")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, state.Groups[0].CatalogVersion)},
		collectionNameRef("docs"), documentFormatSection(collections.DocumentFormatJSON),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("foreign-extra"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"embedding":[-1,-1],"kind":"foreign"}`))}, ackSection(AckRaftCommitted)})
	if err != nil {
		t.Fatal(err)
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "docs", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, client.config.NodeID, request)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	result, err := client.Submit(ctx, client.config.NodeID, entry, metadata)
	if err != nil || !result.CommittedApplied || !result.CommittedRecoverable || !result.Evidence.ProvesProductionConsensus() {
		t.Fatalf("actual extra-row commit: %+v %v", result, err)
	}
	if err := client.fixturePrefixV1(ctx, result.Evidence.Index); err != nil {
		t.Fatal(err)
	}
}

func runFixedPeerVectorFixtureRealRaftV1(t *testing.T, mode string, replicas int) {
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
	datasetPath := ""
	expectedRows := uint64(3)
	if strings.HasPrefix(mode, "dataset") {
		datasetPath = writeFixedPeerDatasetTestV1(t, 600, 128)
		expectedRows = 603
		for i := range configs {
			configs[i].RequestTimeout = time.Minute
			// The 600x128 dataset exercises more work than the small fixture.
			// Use a one-second Raft budget to avoid elections during preparation
			// under the race detector.
			configs[i].RaftTimeout = time.Second
			v := configs[i].VectorInitialization
			v.MaxSourceRows = 640
			v.IndexDefinition.Dimensions = 128
			v.IndexDefinition.M = 16
			v.IndexDefinition.EfConstruction = 128
			v.IndexDefinition.EfSearch = 128
		}
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
	defer client.Close()
	initializeCtx, initializeCancel := context.WithCancel(ctx)
	defer initializeCancel()
	initializing := client
	injected := false
	if mode == "initialize-four-row" || mode == "standalone-four-row" {
		initializing = fixtureStatusClientV1(t, client, func(reply *fixedPeerReplyV1) {
			if injected {
				return
			}
			injected = true
			// The first Prepare status read follows the actual all-voter seed
			// prefix. Insert through authenticated routing, never local DB edits.
			fixtureExtraRowV1(t, ctx, client)
			if mode == "standalone-four-row" {
				initializeCancel()
			}
		})
	}
	datasetManifestSHA := ""
	if strings.HasPrefix(mode, "dataset") {
		d, e := readFixedPeerVectorDatasetV1(datasetPath, configs[0])
		if e != nil {
			t.Fatal(e)
		}
		datasetManifestSHA = d.identity.ManifestSHA256
	}
	targetCalls := 0
	if mode == "dataset-ambiguous" {
		copyClient := *client
		copyHTTP := *client.http
		base, ok := client.http.Transport.(*http.Transport)
		if !ok {
			t.Fatal("actual authenticated transport required")
		}
		copyHTTP.Transport = splitInsertRoundTripperV1{Transport: base, roundTrip: func(request *http.Request) (*http.Response, error) {
			drop := false
			if request.URL.Path == "/v1/submit" {
				raw, e := io.ReadAll(request.Body)
				if e != nil {
					return nil, e
				}
				request.Body = io.NopCloser(bytes.NewReader(raw))
				var body fixedPeerRequestV1
				if e = json.Unmarshal(raw, &body); e != nil {
					return nil, e
				}
				drop = bytes.Contains(body.Entry, []byte(fixedPeerVectorDatasetRequestIDV1("operator-fixture", datasetManifestSHA)+"/dataset/000001"))
			}
			response, e := base.RoundTrip(request)
			if e != nil || !drop {
				return response, e
			}
			targetCalls++
			raw, e := io.ReadAll(response.Body)
			response.Body.Close()
			if e != nil {
				return nil, e
			}
			var reply fixedPeerReplyV1
			if e = json.Unmarshal(raw, &reply); e != nil {
				return nil, e
			}
			if !reply.Submit.CommittedApplied || !reply.Submit.Evidence.ProvesProductionConsensus() {
				t.Fatalf("cut did not follow real commit: %+v", reply)
			}
			return nil, fmt.Errorf("injected lost committed dataset chunk reply")
		}}
		copyClient.http = &copyHTTP
		initializing = &copyClient
	}
	var initialized FixedPeerVectorBootstrapV1
	if strings.HasPrefix(mode, "dataset") {
		initialized, err = initializing.InitializeVectorDatasetV1(initializeCtx, "operator-fixture", datasetPath)
	} else {
		initialized, err = initializing.InitializeVectorFixtureV1(initializeCtx, "operator-fixture")
	}
	if mode == "dataset-ambiguous" {
		if err == nil || targetCalls != 1 || initialized.Stage != "dataset-seed" || initialized.Prepare.Command.Term != 0 || len(initialized.Chunks) != 5 {
			t.Fatalf("ambiguous chunk did not stop: %+v calls=%d err=%v", initialized, targetCalls, err)
		}
		if initialized.Chunks[0].Outcome != "committed-applied" || initialized.Chunks[1].Outcome != "unknown" || initialized.Chunks[1].Error == "" {
			t.Fatalf("partial accounting lost: %+v", initialized.Chunks)
		}
		for _, chunk := range initialized.Chunks[2:] {
			if chunk.Outcome != "unissued" {
				t.Fatalf("continued after ambiguous commit: %+v", chunk)
			}
		}
		return
	}
	if mode == "initialize-four-row" {
		if !injected || err == nil || !strings.Contains(err.Error(), "exactly3") ||
			initialized.Stage != "prepare" || initialized.Prepare.Command.SourceRowCount != 4 {
			t.Fatalf("four-row initialization accepted/lost completion: %+v err=%v", initialized, err)
		}
		return
	}
	if mode == "standalone-four-row" {
		if !injected || err == nil || initialized.Prepare.Command.Term != 0 {
			t.Fatalf("independent pre-Prepare cancellation: %+v err=%v", initialized, err)
		}
		// Prepare genuinely through the ordinary API, independently of the
		// initializer's post-Prepare fixture gate; its valid bound still permits4.
		completion, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "operator-fixture")
		if err != nil || completion.Command.SourceRowCount != 4 {
			t.Fatalf("genuine four-row preparation: %+v err=%v", completion, err)
		}
	} else if err != nil || initialized.Stage != "prepared-restart-required" || initialized.Prepare.Command.SourceRowCount != expectedRows {
		t.Fatalf("initialize=%+v err=%v", initialized, err)
	}
	client.Close()
	closeAll()
	if t.Failed() {
		return
	}
	if mode == "dataset-replaced" {
		// A distinct fully eligible same-count corpus must not inherit the original
		// durable preparation identity. Keep its hashes honest so pure input
		// admission succeeds and the authoritative preparation check is exercised.
		vectorsPath := filepath.Join(datasetPath, "documents.f32")
		vectors, e := os.ReadFile(vectorsPath)
		if e != nil {
			t.Fatal(e)
		}
		vectors[11] ^= 0x80 // Row0 coordinate2: +1 -> -1, same norm and oracle plane.
		if e = os.WriteFile(vectorsPath, vectors, 0600); e != nil {
			t.Fatal(e)
		}
		manifestPath := filepath.Join(datasetPath, "manifest.json")
		raw, e := os.ReadFile(manifestPath)
		if e != nil {
			t.Fatal(e)
		}
		var manifest fixedPeerVectorDatasetManifestV1
		if e = json.Unmarshal(raw, &manifest); e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(vectors)
		entry := manifest.Files["documents.f32"]
		entry.SHA256 = hex.EncodeToString(sum[:])
		manifest.Files["documents.f32"] = entry
		raw, e = json.Marshal(manifest)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(manifestPath, raw, 0600); e != nil {
			t.Fatal(e)
		}
		replacement, e := readFixedPeerVectorDatasetV1(datasetPath, configs[0])
		if e != nil || replacement.identity.SourceRows != expectedRows || replacement.identity.ManifestSHA256 == initialized.Dataset.ManifestSHA256 {
			t.Fatalf("replacement must remain eligible and distinct: %+v %v", replacement, e)
		}
	}
	openAll()
	client, err = NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if mode == "standalone-four-row" {
		qualified, err := client.QualifyVectorFixtureV1(ctx, "operator-fixture")
		if err == nil || !strings.Contains(err.Error(), "exactly3") || qualified.Prepare.Command.SourceRowCount != 4 ||
			qualified.Before.Counters.SelectedPartitions != 0 || qualified.Insert.CommitIndex != 0 || qualified.Retry.CommitIndex != 0 {
			t.Fatalf("standalone qualification reached public work/lost completion: %+v err=%v", qualified, err)
		}
		for _, node := range nodes {
			group := node.config.Groups[0]
			col, _, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx,
				raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			row, err := col.Get([]byte("operator-fixture-fresh-y"))
			if err != nil || row != nil {
				t.Fatalf("qualification wrote a fresh row: %q err=%v", row, err)
			}
		}
		return
	}
	if mode == "dataset-replaced" {
		qualified, e := client.QualifyVectorDatasetV1(ctx, "operator-fixture", datasetPath)
		if e == nil || !strings.Contains(e.Error(), "matching request") || qualified.Before.Counters.SelectedPartitions != 0 || qualified.Insert.CommitIndex != 0 || qualified.Retry.CommitIndex != 0 {
			t.Fatalf("substituted corpus reached public qualification: %+v %v", qualified, e)
		}
		return
	}
	if mode == "dataset" {
		qualified, e := client.QualifyVectorDatasetV1(ctx, "operator-fixture", datasetPath)
		if e != nil || qualified.Dataset == nil || qualified.Prepare.Command.SourceRowCount != expectedRows || len(qualified.Readiness) != 4 || qualified.Prepare.Command != initialized.Prepare.Command {
			t.Fatalf("dataset reopen qualification: %+v %v", qualified, e)
		}
		if len(initialized.Chunks) != 5 {
			t.Fatalf("bounded dataset chunks=%d", len(initialized.Chunks))
		}
		for _, chunk := range initialized.Chunks {
			if chunk.Outcome != "committed-applied" || !chunk.Result.Evidence.ProvesProductionConsensus() {
				t.Fatalf("chunk lost proof: %+v", chunk)
			}
		}
		return
	}
	for _, name := range []string{"absent", "incomplete", "malformed", "request", "row-count", "incomplete-with-wrong-request", "command-disagreement", "asset-disagreement"} {
		t.Run(name, func(t *testing.T) {
			probe := fixtureStatusClientV1(t, client, func(reply *fixedPeerReplyV1) {
				state := reply.VectorPreparation
				if reply.Error != "" || state == nil || state.Completion == nil {
					t.Fatalf("actual status unavailable: %+v", reply)
				}
				switch name {
				case "absent":
					reply.VectorPreparation = nil
				case "incomplete":
					state.Completion = nil
				case "malformed":
					state.Completion.Command.Version = 0
				case "request":
					state.RequestID = "foreign/prepare"
				case "row-count":
					state.Completion.Command.SourceRowCount = 4
				case "incomplete-with-wrong-request":
					if reply.NodeID == client.config.Groups[0].Peers[0].ID {
						state.Completion = nil
					} else {
						state.RequestID = "foreign/prepare"
					}
				case "command-disagreement":
					if reply.NodeID == client.config.Groups[0].Peers[1].ID {
						state.Completion.Command.CommandDigest = strings.Repeat("a", 64)
					}
				case "asset-disagreement":
					if reply.NodeID == client.config.Groups[0].Peers[1].ID {
						state.Completion.AssetSetDigest = strings.Repeat("a", 64)
					}
				}
			})
			probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			report, err := probe.QualifyVectorFixtureV1(probeCtx, "operator-fixture")
			if name == "incomplete-with-wrong-request" && (err == nil || !strings.Contains(err.Error(), "matching request")) {
				t.Fatalf("pending voter masked completed mismatch: %v", err)
			}
			if err == nil || !strings.Contains(err.Error(), "fixture preparation") || report.Before.Counters.SelectedPartitions != 0 || report.Insert.CommitIndex != 0 {
				t.Fatalf("invalid preparation reached public qualification: %+v err=%v", report, err)
			}
		})
	}
	qualified, err := client.QualifyVectorFixtureV1(ctx, "operator-fixture")
	if err != nil || len(qualified.Readiness) != len(configs) || qualified.Prepare.Command.SourceRowCount != 3 ||
		qualified.Prepare.Command != initialized.Prepare.Command || qualified.Prepare.AssetSetDigest != initialized.Prepare.AssetSetDigest {
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
