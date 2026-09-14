package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
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
		NormalizedCandidateWidth: 2, RawCandidateWidth: 2, RerankCandidateCap: 2, RawRetainedCandidates: 2, LiveShortlistCandidates: 2,
		QuantizedScoreCalls: 4, ActualRerankCandidates: 2, ExactBaseRerankScoreCalls: 2,
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
	legacyHash := proof
	legacyHash.QuantizedConfigHash = 1
	if _, err := appendDenseScorePlane(nil, legacyHash, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed legacy score-plane proof with nonzero config hash accepted")
	}
	inconsistentRerank := proof
	inconsistentRerank.ExactBaseRerankScoreCalls = 1
	if _, err := appendDenseScorePlane(nil, inconsistentRerank, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed quantized rerank proof with inconsistent exact counters accepted")
	}
	efBounded := proof
	efBounded.RequestedTopK, efBounded.RequestedEFSearch = 1, 1
	if _, err := appendDenseScorePlane(nil, efBounded, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed quantized rerank proof exceeded the explicit EF width")
	}
	underCap := proof
	underCap.RerankCandidateCap = 1
	if _, err := appendDenseScorePlane(nil, underCap, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed quantized rerank proof accepted a producer-inconsistent cap")
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
	proof.QuantizedCodeBytesRead = 2
	request := DenseVectorSearchRequest{Query: []float32{1, 0}, QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: proof.QuantizedIndexName, TopK: 1, EfSearch: 8}
	if err := validateDenseQuantizedScorePlaneResponse(work, proof, request, 0); err != nil {
		t.Fatalf("valid public proof rejected: %v", err)
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphScorePlaneWork){
		"quantized bytes":    func(p *collections.ColumnGraphScorePlaneWork) { p.QuantizedCodeBytesRead = 0 },
		"exact base bytes":   func(p *collections.ColumnGraphScorePlaneWork) { p.ExactBaseVectorBytesRead = 1 },
		"exact suffix bytes": func(p *collections.ColumnGraphScorePlaneWork) { p.ExactSuffixVectorBytesRead = 1 },
	} {
		candidate := *proof
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(work, &candidate, request, 0); err == nil {
			t.Fatalf("invalid score-plane byte counters accepted (%s): %+v", name, candidate)
		}
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
	if err := validateDenseQuantizedScorePlaneResponse(work, proof, request, 2); err == nil {
		t.Fatal("score-plane proof accepted more results than exact score calls")
	}
	inconsistentRerank := *proof
	inconsistentRerank.ExactBaseRerankScoreCalls = 0
	inconsistentRerank.ExactSmallFilterScoreCalls = 1
	if err := validateDenseQuantizedScorePlaneResponse(work, &inconsistentRerank, request, 0); err == nil {
		t.Fatal("score-plane proof accepted inconsistent rerank counters")
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
	retainedWithoutScore := *proof
	retainedWithoutScore.QuantizedScoreCalls, retainedWithoutScore.RawRetainedCandidates, retainedWithoutScore.RawCandidateWidth = 1, 2, 2
	if err := validateDenseQuantizedScorePlaneResponse(work, &retainedWithoutScore, request, 0); err == nil {
		t.Fatal("score-plane proof retained more candidates than quantized score calls")
	}
	exactProof := *proof
	exactProof.Route, exactProof.QuantizedScoreCalls, exactProof.QuantizedCodeBytesRead = "typed_exact", 0, 0
	exactProof.ExactSuffixScoreCalls, exactProof.ExactSuffixVectorBytesRead = 1, 8
	exactWork := work
	exactWork.Graph.Route, exactWork.Graph.BaseANNScored, exactWork.Graph.DeltaScored = "typed_exact", 0, 1
	if err := validateDenseQuantizedScorePlaneResponse(exactWork, &exactProof, request, 0); err == nil {
		t.Fatal("typed-exact proof accepted fewer rows than exact score calls")
	}
	quantizedUnderfill := *proof
	quantizedUnderfill.NormalizedCandidateWidth, quantizedUnderfill.RawCandidateWidth = 1, 1
	quantizedUnderfill.RerankCandidateCap, quantizedUnderfill.RawRetainedCandidates, quantizedUnderfill.LiveShortlistCandidates = 1, 1, 1
	quantizedUnderfill.ActualRerankCandidates, quantizedUnderfill.QuantizedScoreCalls = 1, 1
	quantizedUnderfill.QuantizedCodeBytesRead, quantizedUnderfill.ExactBaseRerankScoreCalls, quantizedUnderfill.ExactBaseVectorBytesRead = 2, 1, 8
	quantizedWork := work
	quantizedWork.Graph.ExactBaseScored, quantizedWork.Graph.BaseResultIDs = 1, 1
	if err := validateDenseQuantizedScorePlaneResponse(quantizedWork, &quantizedUnderfill, request, 0); err == nil {
		t.Fatal("quantized proof accepted fewer rows than exact score calls")
	}
	baseResultMismatch := work
	baseResultMismatch.Graph.BaseResultIDs = 1
	if err := validateDenseQuantizedScorePlaneResponse(baseResultMismatch, proof, request, 0); err == nil {
		t.Fatal("score-plane proof accepted base-result IDs unrelated to exact scoring")
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphScorePlaneWork){
		"cap exceeds normalized width":       func(p *collections.ColumnGraphScorePlaneWork) { p.RerankCandidateCap = 2 },
		"shortlist exceeds normalized width": func(p *collections.ColumnGraphScorePlaneWork) { p.LiveShortlistCandidates = 2 },
		"normalized exceeds raw width":       func(p *collections.ColumnGraphScorePlaneWork) { p.NormalizedCandidateWidth = 2 },
	} {
		candidate := *proof
		candidate.NormalizedCandidateWidth, candidate.RawCandidateWidth = 1, 1
		candidate.RerankCandidateCap, candidate.RawRetainedCandidates, candidate.LiveShortlistCandidates = 1, 1, 1
		candidate.ActualRerankCandidates, candidate.ExactBaseRerankScoreCalls = 1, 1
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(work, &candidate, request, 0); err == nil {
			t.Fatalf("invalid candidate widths accepted (%s): %+v", name, candidate)
		}
	}
	strictEmptyProof := *proof
	strictEmptyProof.Route = "typed_empty"
	strictEmptyWork := work
	strictEmptyWork.Graph.Route = "typed_empty"
	for name, mutate := range map[string]func(*collections.ColumnGraphScorePlaneWork){
		"quantized work":      func(p *collections.ColumnGraphScorePlaneWork) { p.QuantizedScoreCalls = 1 },
		"exact suffix work":   func(p *collections.ColumnGraphScorePlaneWork) { p.ExactSuffixScoreCalls = 1 },
		"small-filter work":   func(p *collections.ColumnGraphScorePlaneWork) { p.ExactSmallFilterScoreCalls = 1 },
		"retained candidates": func(p *collections.ColumnGraphScorePlaneWork) { p.RawRetainedCandidates = 1 },
		"live shortlist":      func(p *collections.ColumnGraphScorePlaneWork) { p.LiveShortlistCandidates = 1 },
	} {
		candidate := strictEmptyProof
		candidate.QuantizedScoreCalls, candidate.ExactSuffixScoreCalls, candidate.ExactSmallFilterScoreCalls = 0, 0, 0
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(strictEmptyWork, &candidate, request, 0); err == nil {
			t.Fatalf("typed-empty proof accepted %s: %+v", name, candidate)
		}
	}
	strictExactProof := *proof
	strictExactProof.Route = "typed_exact"
	strictExactProof.QuantizedScoreCalls = 0
	strictExactProof.QuantizedCodeBytesRead = 0
	strictExactProof.ActualRerankCandidates = 0
	strictExactProof.ExactBaseRerankScoreCalls = 0
	strictExactProof.ExactSuffixScoreCalls = 1
	strictExactProof.ExactSuffixVectorBytesRead = 8
	strictExactWork := work
	strictExactWork.Graph.Route = "typed_exact"
	strictExactWork.Graph.BaseANNScored = 0
	strictExactWork.Graph.DeltaScored = 1
	for name, mutate := range map[string]func(*collections.ColumnGraphScorePlaneWork){
		"retained candidates": func(p *collections.ColumnGraphScorePlaneWork) { p.RawRetainedCandidates = 1 },
		"live shortlist":      func(p *collections.ColumnGraphScorePlaneWork) { p.LiveShortlistCandidates = 1 },
	} {
		candidate := strictExactProof
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(strictExactWork, &candidate, request, 1); err == nil {
			t.Fatalf("typed-exact proof accepted %s: %+v", name, candidate)
		}
	}
}

func TestDenseV3CandidateCountMatchesRows(t *testing.T) {
	for _, candidate := range [][3]int{{1, 1, 1}, {0, 0, 1}, {2, 1, 0}, {-1, 0, 0}, {0, -1, 0}} {
		if got := denseV3CandidateCountMatchesRows(candidate[0], candidate[1]); (got && candidate[2] == 0) || (!got && candidate[2] == 1) {
			t.Fatalf("candidate count match (%d,%d)=%v, want %v", candidate[0], candidate[1], got, candidate[2] == 1)
		}
	}
}

func TestDenseV3ResultsHaveUniqueIDs(t *testing.T) {
	if !denseV3ResultsHaveUniqueIDs([]DenseVectorSearchResult{{ID: []byte("a")}, {ID: []byte("b")}}) {
		t.Fatal("unique result IDs rejected")
	}
	if denseV3ResultsHaveUniqueIDs([]DenseVectorSearchResult{{ID: []byte("a")}, {ID: []byte("a")}}) {
		t.Fatal("duplicate result IDs accepted")
	}
}

func TestDenseV3ResultsOrdered(t *testing.T) {
	for name, results := range map[string][]DenseVectorSearchResult{
		"descending score": {{ID: []byte("b"), Score: 0.9}, {ID: []byte("a"), Score: 0.1}},
		"ascending ID tie": {{ID: []byte("a"), Score: 0.5}, {ID: []byte("b"), Score: 0.5}},
	} {
		if !denseV3ResultsOrdered(results) {
			t.Fatalf("valid %s order rejected", name)
		}
	}
	for name, results := range map[string][]DenseVectorSearchResult{
		"ascending score":   {{ID: []byte("a"), Score: 0.1}, {ID: []byte("b"), Score: 0.9}},
		"descending ID tie": {{ID: []byte("b"), Score: 0.5}, {ID: []byte("a"), Score: 0.5}},
	} {
		if denseV3ResultsOrdered(results) {
			t.Fatalf("invalid %s order accepted", name)
		}
	}
}

func TestDenseV3ResultDecodeErrorsPreserveOwnedProofs(t *testing.T) {
	proof := collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, Completed: true,
		RequestedMode: collections.VectorIndexQueryModeQuantizedRerank,
		EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank,
		Route:         "quantized_rerank", QuantizedIndexName: "embedding.scalar_u8.public",
		QuantizedCodec: collections.QuantizedVectorCodecScalarU8, QuantizedVersion: 1,
		RequestedTopK: 1, RequestedEFSearch: 8,
		NormalizedCandidateWidth: 1, RawCandidateWidth: 1, RerankCandidateCap: 1,
		RawRetainedCandidates: 1, LiveShortlistCandidates: 1, ActualRerankCandidates: 1,
		QuantizedScoreCalls: 1, QuantizedCodeBytesRead: 2,
		ExactBaseRerankScoreCalls: 1, ExactBaseVectorBytesRead: 8,
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true, SchemaHash: 7, SchemaGeneration: 2},
	}
	work := documentservice.DenseSearchWork{
		Version: 1, Completed: true,
		Graph: collections.ColumnGraphQueryWork{
			Available: true, Completed: true, Route: "typed_hnsw", BaseANNScored: 1, BaseResultIDs: 1,
			Snapshot: proof.Snapshot,
		},
		Output: documentservice.DenseSearchOutputWork{Attempted: true, Completed: true, Requested: 1, Fetched: 1, OutputBytes: 2},
	}
	workRaw, err := appendDenseWork(nil, work)
	if err != nil {
		t.Fatal(err)
	}
	proofRaw, err := appendDenseScorePlane(nil, proof, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	responseFor := func(meta []byte) []byte {
		t.Helper()
		var body []byte
		for _, section := range []iwire.Section{
			{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
			{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte("{}"))},
			{ID: iwire.SectionDenseSearchResponse, Bytes: meta},
			{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
			{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
		} {
			body, err = iwire.AppendSection(body, section)
			if err != nil {
				t.Fatal(err)
			}
		}
		return body
	}
	validScore := binary.LittleEndian.AppendUint64(nil, math.Float64bits(1))
	for name, meta := range map[string][]byte{
		"candidate count": append([]byte{3, 0, 0, 0, 1}, validScore...),
		"nonfinite score": append([]byte{3, 1, 0, 0, 1}, binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.NaN()))...),
	} {
		t.Run(name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			client := NewClient(clientConn)
			client.denseTypedQuantizedNegotiated = true
			response := responseFor(meta)
			errCh := make(chan error, 1)
			go func() {
				header, _, serveErr := readFrame(serverConn, iwire.DefaultLimits())
				if serveErr == nil {
					serveErr = writeFrame(serverConn, iwire.Header{Type: iwire.FrameResponse, RequestID: header.RequestID}, response)
				}
				errCh <- serveErr
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, gotErr := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{
				TypedColumnGraph: true, Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8,
				QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: proof.QuantizedIndexName,
			})
			cancel()
			_ = client.Close()
			_ = serverConn.Close()
			if serveErr := <-errCh; serveErr != nil {
				t.Fatal(serveErr)
			}
			var decodeErr *DenseVectorSearchDecodeError
			if !errors.As(gotErr, &decodeErr) || decodeErr.DenseWork == nil || decodeErr.ScorePlane == nil {
				t.Fatalf("decode error lost proofs: %v", gotErr)
			}
			if *decodeErr.DenseWork != work || *decodeErr.ScorePlane != proof {
				t.Fatalf("decode error proofs changed: work=%+v score_plane=%+v", decodeErr.DenseWork, decodeErr.ScorePlane)
			}
		})
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
