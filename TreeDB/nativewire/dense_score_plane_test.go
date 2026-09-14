package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDenseScorePlaneCodecOwnedAndStrict(t *testing.T) {
	proof := collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, Completed: true,
		RequestedMode: collections.VectorIndexQueryModeQuantizedRerank,
		EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank,
		Route:         "quantized_rerank", QuantizedIndexName: "embedding.scalar_u8.public",
		QuantizedCodec: collections.QuantizedVectorCodecScalarU8, QuantizedVersion: 1,
		RequestedTopK: 2, RequestedEFSearch: 8, RequestedRerankCandidates: 0,
		QuantizedScoreCalls: 4, ActualRerankCandidates: 2,
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true, SchemaHash: 11, SchemaGeneration: 3,
			BaseManifest:    collections.ColumnGraphManifestWork{Generation: 5, Format: "tcs1", Version: 1, Checksum: 7},
			CurrentManifest: collections.ColumnGraphManifestWork{Generation: 6, Format: "tcs1", Version: 1, Checksum: 8}},
	}
	raw, err := appendDenseScorePlane(nil, proof, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeDenseScorePlane(raw, iwire.DefaultLimits())
	if err != nil || decoded != proof {
		t.Fatalf("round trip decoded=%+v err=%v", decoded, err)
	}
	expectedName := proof.QuantizedIndexName
	clear(raw)
	if decoded.QuantizedIndexName != expectedName || decoded.Snapshot.BaseManifest.Format != "tcs1" {
		t.Fatal("score-plane proof borrowed encoded bytes")
	}
	for size := range len(raw) {
		// raw was cleared above; use a fresh encoding for truncation.
		candidate, err := appendDenseScorePlane(nil, proof, iwire.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		candidate = candidate[:size]
		if _, err := decodeDenseScorePlane(candidate, iwire.DefaultLimits()); err == nil {
			t.Fatalf("truncated score-plane proof accepted at %d", size)
		}
	}
	bad, err := appendDenseScorePlane(nil, proof, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	bad = append(bad, 0)
	if _, err := decodeDenseScorePlane(bad, iwire.DefaultLimits()); err == nil {
		t.Fatal("trailing score-plane proof accepted")
	}
	if !bytes.Equal(raw, make([]byte, len(raw))) {
		t.Fatal("clear should only affect the encoded buffer")
	}
	incomplete := proof
	incomplete.Completed = false
	incomplete.Snapshot = collections.ColumnGraphQuerySnapshot{}
	incompleteRaw, err := appendDenseScorePlane(nil, incomplete, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	incompleteDecoded, err := decodeDenseScorePlane(incompleteRaw, iwire.DefaultLimits())
	if err != nil || !incompleteDecoded.Available || incompleteDecoded.Completed || incompleteDecoded.Snapshot.Available {
		t.Fatalf("incomplete score-plane flags were not preserved: %+v err=%v", incompleteDecoded, err)
	}
	noQuantizedWork := proof
	noQuantizedWork.QuantizedScoreCalls = 0
	if _, err := appendDenseScorePlane(nil, noQuantizedWork, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed quantized rerank proof without score calls accepted")
	}
}

func TestDenseQuantizedScorePlaneResponseRejectsUnsupportedRoute(t *testing.T) {
	proof := &collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, Completed: true,
		RequestedMode: collections.VectorIndexQueryModeQuantizedRerank,
		EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank,
		Route:         "quantized_rerank", QuantizedIndexName: "embedding.scalar_u8.public",
		RequestedTopK: 1, RequestedEFSearch: 8, QuantizedScoreCalls: 1,
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true},
	}
	work := documentservice.DenseSearchWork{Completed: true, Graph: collections.ColumnGraphQueryWork{Available: true, Completed: true, Route: "typed_hnsw", BaseANNScored: 1}}
	work.Graph.Snapshot = proof.Snapshot
	request := DenseVectorSearchRequest{QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: proof.QuantizedIndexName, TopK: 1, EfSearch: 8}
	if err := validateDenseQuantizedScorePlaneResponse(work, proof, request, 0); err != nil {
		t.Fatalf("valid public proof rejected: %v", err)
	}
	for _, mutate := range []func(*collections.ColumnGraphScorePlaneWork){
		func(p *collections.ColumnGraphScorePlaneWork) { p.Route = "typed_hnsw" },
		func(p *collections.ColumnGraphScorePlaneWork) { p.Available = false },
		func(p *collections.ColumnGraphScorePlaneWork) { p.Completed = false },
		func(p *collections.ColumnGraphScorePlaneWork) { p.Snapshot.Available = false },
	} {
		candidate := *proof
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(work, &candidate, request, 0); err == nil {
			t.Fatalf("invalid proof accepted: %+v", candidate)
		}
	}
	wrongGraph := work
	wrongGraph.Graph.Route = "typed_exact"
	if err := validateDenseQuantizedScorePlaneResponse(wrongGraph, proof, request, 0); err == nil {
		t.Fatal("score-plane proof accepted with a contradictory dense-work route")
	}
	wrongSnapshot := work
	wrongSnapshot.Graph.Snapshot.SchemaHash++
	if err := validateDenseQuantizedScorePlaneResponse(wrongSnapshot, proof, request, 0); err == nil {
		t.Fatal("score-plane proof accepted with a contradictory snapshot")
	}
	counterMismatch := *proof
	counterMismatch.QuantizedScoreCalls++
	if err := validateDenseQuantizedScorePlaneResponse(work, &counterMismatch, request, 0); err == nil {
		t.Fatal("score-plane proof accepted with contradictory graph counters")
	}
	emptyProof := *proof
	emptyProof.Route = "typed_empty"
	emptyWork := work
	emptyWork.Graph.Route = "typed_empty"
	if err := validateDenseQuantizedScorePlaneResponse(emptyWork, &emptyProof, request, 1); err == nil {
		t.Fatal("typed-empty score-plane proof accepted nonempty results")
	}
	cappedProof := *proof
	cappedProof.RequestedRerankCandidates = 2
	cappedProof.RerankCandidateCap = 3
	cappedProof.ActualRerankCandidates = 3
	cappedRequest := request
	cappedRequest.QuantizedRerankCandidates = 2
	if err := validateDenseQuantizedScorePlaneResponse(work, &cappedProof, cappedRequest, 0); err == nil {
		t.Fatal("score-plane proof exceeded the explicit rerank cap")
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphScorePlaneWork){
		"shortlist exceeds retained": func(p *collections.ColumnGraphScorePlaneWork) {
			p.LiveShortlistCandidates = p.RawRetainedCandidates + 1
		},
		"retained exceeds width": func(p *collections.ColumnGraphScorePlaneWork) { p.RawRetainedCandidates = p.RawCandidateWidth + 1 },
		"actual exceeds shortlist": func(p *collections.ColumnGraphScorePlaneWork) {
			p.ActualRerankCandidates = p.LiveShortlistCandidates + 1
		},
	} {
		candidate := *proof
		candidate.RawCandidateWidth, candidate.RawRetainedCandidates, candidate.LiveShortlistCandidates, candidate.ActualRerankCandidates = 4, 3, 2, 1
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(work, &candidate, request, 0); err == nil {
			t.Fatalf("invalid rerank counts accepted (%s): %+v", name, candidate)
		}
	}
}

func TestDenseTypedQuantizedNativePublicPath(t *testing.T) {
	// The selected typed serving path relies on the exact retained-parent
	// namespace contract. Windows intentionally does not advertise that
	// contract; keep this integration test aligned with the service-level
	// serving tests instead of turning the platform limitation into a failure.
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("selected serving requires exact relative namespace support")
	}
	var native [2]byte
	binary.NativeEndian.PutUint16(native[:], 1)
	if native[0] != 1 {
		t.Skip("selected serving requires little-endian mmap-direct prepared views")
	}
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, DocumentService: svc})
	t.Cleanup(func() { _ = server.Close(); _ = svc.Close(); _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	info, err := svc.CreateIndex(ctx, documentservice.CreateIndexRequest{Name: "typed-q", Dimension: 2, TypedInput: true,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []documentservice.QuantizedIndexInfo{{Name: "embedding.scalar_u8.public", Codec: collections.QuantizedVectorCodecScalarU8}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpsertDocuments(ctx, info.Name, documentservice.UpsertDocumentsRequest{DeferVectorIndexRebuild: true, Documents: []documentservice.Document{{ID: "a", Content: "a", Embedding: []float32{1, 0}}, {ID: "b", Content: "b", Embedding: []float32{0, 1}}}}); err != nil {
		t.Fatal(err)
	}
	options := collections.ColumnGraphServingOptions{
		Publication:     collections.ColumnGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 4096, OwnedBytes: 16 << 20, EncodedOutputBytes: 16 << 20},
		Owners:          collections.ColumnGraphReadOwnerLimits{Owners: 8, States: 8, StateBytes: 128 << 20, AssetBytes: 128 << 20, Cold: collections.ColumnGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 8 << 20, AssetBytes: 64 << 20, DecodedTermBytes: 64 << 20}},
		CandidateOutput: collections.ColumnGraphCandidateOutputLimits{Bytes: 1 << 30, AppenderAttempts: 4096},
		Maintenance:     collections.ColumnGraphMaintenanceLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 128 << 20, ColumnBytes: 64 << 20, ManifestBytes: 8 << 20, RetainedBytes: 256 << 20, PagerPages: 32768},
		Filter:          collections.ColumnGraphFilterLimits{SourceIDs: 4096, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 100000, InspectedEntries: 4096}, FoldRows: 4096, SearchCandidates: 4096,
	}
	if _, err := svc.OptimizeIndex(ctx, info.Name, documentservice.OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	response, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{TypedColumnGraph: true, Index: info.Name, Query: []float32{1, 0}, TopK: 1, QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.public", ExpectedGeneration: info.Generation})
	if err != nil {
		t.Fatal(err)
	}
	if !response.TypedColumnGraph || response.ScorePlane == nil || !response.ScorePlane.Completed || response.ScorePlane.QuantizedIndexName != "embedding.scalar_u8.public" || len(response.Results) != 1 {
		t.Fatalf("native v3 response=%+v", response)
	}
	owned := response.ScorePlane
	response2, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{TypedColumnGraph: true, Index: info.Name, Query: []float32{0, 1}, TopK: 1, QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.public", ExpectedGeneration: info.Generation})
	if err != nil {
		t.Fatal(err)
	}
	if owned == response2.ScorePlane || owned.QuantizedIndexName != "embedding.scalar_u8.public" {
		t.Fatal("score-plane proof was reused or borrowed")
	}
	request := DenseVectorSearchRequest{TypedColumnGraph: true, Index: info.Name, Query: []float32{1, 0}, TopK: 1, QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.public", ExpectedGeneration: info.Generation}
	payload, err := appendDenseVectorSearchRequest(nil, request, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	sections := []iwire.Section{{ID: iwire.SectionDenseSearchRequest, Bytes: payload}, {ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))}}
	qoptions, err := appendDenseQuantizedOptions(nil, request, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	sections = append(sections, iwire.Section{ID: iwire.SectionDenseSearchQuantizedOptions, Bytes: qoptions})
	serverLimits := server.limits
	server.limits.MaxByteVectorBytes = 1
	partial, err := server.handleVersionedDenseVectorSearch(ctx, &connState{}, iwire.DenseVectorSearchTypedQuantizedVersion, sections, nil)
	server.limits = serverLimits
	var observed *denseWorkError
	if err == nil || len(partial) != 0 || !errors.As(err, &observed) || observed.scorePlane == nil || !observed.scorePlane.Completed {
		t.Fatalf("quantized encoding error lost proof: partial=%d err=%v", len(partial), err)
	}
	var frame bytes.Buffer
	if err := server.writeError(&frame, iwire.Header{}, err); err != nil {
		t.Fatal(err)
	}
	_, body, err := readFrame(&frame, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	remote, ok := decodeWireErrorVersion(body, iwire.DefaultLimits(), iwire.DenseVectorSearchTypedQuantizedVersion).(*WireError)
	if !ok || remote.ScorePlane == nil || remote.ScorePlane.QuantizedIndexName != "embedding.scalar_u8.public" {
		t.Fatalf("wire error lost score-plane proof: %+v", remote)
	}
}
