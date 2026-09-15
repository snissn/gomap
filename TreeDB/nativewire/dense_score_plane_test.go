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
			CurrentManifest: collections.ColumnGraphManifestWork{Generation: 6, Format: "tcs1", Version: 1, Checksum: 8},
			BaseCoverageLSN: 5, CurrentCoverageLSN: 6},
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
	completedWithReason := proof
	completedWithReason.Reason = "stale error"
	if _, err := appendDenseScorePlane(nil, completedWithReason, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed score-plane proof with an error reason accepted")
	}
	zeroPlan := proof
	zeroPlan.NormalizedCandidateWidth, zeroPlan.RawCandidateWidth, zeroPlan.RerankCandidateCap = 0, 0, 0
	zeroPlan.RawRetainedCandidates, zeroPlan.LiveShortlistCandidates = 0, 0
	zeroPlan.ActualRerankCandidates, zeroPlan.ExactBaseRerankScoreCalls = 0, 0
	if _, err := appendDenseScorePlane(nil, zeroPlan, iwire.DefaultLimits()); err == nil {
		t.Fatal("completed quantized rerank proof with a zero plan accepted")
	}
	reversedCoverage := proof
	reversedCoverage.Snapshot.BaseCoverageLSN, reversedCoverage.Snapshot.CurrentCoverageLSN = 100, 1
	if _, err := appendDenseScorePlane(nil, reversedCoverage, iwire.DefaultLimits()); err == nil {
		t.Fatal("score-plane proof with reversed snapshot coverage accepted")
	}
	reversedManifest := proof
	reversedManifest.Snapshot.BaseManifest.Generation = reversedManifest.Snapshot.CurrentManifest.Generation + 1
	if _, err := appendDenseScorePlane(nil, reversedManifest, iwire.DefaultLimits()); err == nil {
		t.Fatal("score-plane proof with reversed snapshot manifest generation accepted")
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphQuerySnapshot){
		"schema generation": func(s *collections.ColumnGraphQuerySnapshot) { s.SchemaGeneration = 0 },
		"base coverage LSN": func(s *collections.ColumnGraphQuerySnapshot) { s.BaseCoverageLSN = 0 },
	} {
		t.Run("missing "+name, func(t *testing.T) {
			candidate := proof
			mutate(&candidate.Snapshot)
			if _, err := appendDenseScorePlane(nil, candidate, iwire.DefaultLimits()); err == nil {
				t.Fatalf("score-plane proof with zero %s accepted", name)
			}
		})
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphManifestWork){
		"generation": func(m *collections.ColumnGraphManifestWork) { m.Generation = 0 },
		"version":    func(m *collections.ColumnGraphManifestWork) { m.Version = 0 },
		"checksum":   func(m *collections.ColumnGraphManifestWork) { m.Checksum = 0 },
	} {
		t.Run("incomplete manifest "+name, func(t *testing.T) {
			candidate := proof
			mutate(&candidate.Snapshot.BaseManifest)
			if _, err := appendDenseScorePlane(nil, candidate, iwire.DefaultLimits()); err == nil {
				t.Fatalf("score-plane proof with zero manifest %s accepted", name)
			}
		})
	}
	unsupportedManifestVersion := proof
	unsupportedManifestVersion.Snapshot.BaseManifest.Version = 2
	if _, err := appendDenseScorePlane(nil, unsupportedManifestVersion, iwire.DefaultLimits()); err == nil {
		t.Fatal("score-plane proof with unsupported manifest version accepted")
	}
}

func TestDenseQuantizedScorePlaneResponseRejectsUnsupportedRoute(t *testing.T) {
	proof := &collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, Completed: true,
		RequestedMode: collections.VectorIndexQueryModeQuantizedRerank,
		EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank,
		Route:         "quantized_rerank", QuantizedIndexName: "embedding.scalar_u8.public",
		RequestedTopK: 1, RequestedEFSearch: 8,
		NormalizedCandidateWidth: 1, RawCandidateWidth: 1, RerankCandidateCap: 1,
		RawRetainedCandidates: 1, QuantizedScoreCalls: 1,
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true, SchemaGeneration: 1,
			BaseManifest:    collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
			CurrentManifest: collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
			BaseCoverageLSN: 1, CurrentCoverageLSN: 1},
	}
	work := documentservice.DenseSearchWork{Completed: true, Graph: collections.ColumnGraphQueryWork{Available: true, Completed: true, Route: "typed_hnsw", BaseANNScored: 1, BaseShadowed: 1}}
	work.Graph.Snapshot = proof.Snapshot
	proof.QuantizedCodeBytesRead = 2
	request := DenseVectorSearchRequest{Query: []float32{1, 0}, QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: proof.QuantizedIndexName, TopK: 1, EfSearch: 8}
	if err := validateDenseQuantizedScorePlaneResponse(work, proof, request, 0); err != nil {
		t.Fatalf("valid public proof rejected: %v", err)
	}
	newerAggregate := request
	newerAggregate.ExpectedGeneration = proof.Snapshot.SchemaGeneration + 1
	if err := validateDenseQuantizedScorePlaneResponse(work, proof, newerAggregate, 0); err != nil {
		t.Fatalf("aggregate generation newer than vector generation rejected: %v", err)
	}
	newerSnapshot := *proof
	newerSnapshot.Snapshot.SchemaGeneration++
	newerSnapshotWork := work
	newerSnapshotWork.Graph.Snapshot = newerSnapshot.Snapshot
	request.ExpectedGeneration = proof.Snapshot.SchemaGeneration
	if err := validateDenseQuantizedScorePlaneResponse(newerSnapshotWork, &newerSnapshot, request, 0); err == nil {
		t.Fatal("snapshot newer than the admitted generation was accepted")
	}
	request.ExpectedGeneration = 0
	for name, candidate := range map[string]documentservice.DenseSearchWork{
		"base edge work": func() documentservice.DenseSearchWork {
			candidate := work
			candidate.Graph.BaseEdges = 1
			return candidate
		}(),
		"shadow/shortlist mismatch": func() documentservice.DenseSearchWork {
			candidate := work
			candidate.Graph.BaseShadowed = 0
			return candidate
		}(),
	} {
		if err := validateDenseQuantizedScorePlaneResponse(candidate, proof, request, 0); err == nil {
			t.Fatalf("quantized route accepted %s: %+v", name, candidate.Graph)
		}
	}
	filteredRequest := request
	filteredRequest.Filter = new(documentservice.Filter)
	filteredProof := *proof
	filteredProof.NormalizedCandidateWidth, filteredProof.RawCandidateWidth, filteredProof.RerankCandidateCap = 1, 1, 1
	filteredProof.RawRetainedCandidates, filteredProof.LiveShortlistCandidates, filteredProof.ActualRerankCandidates = 1, 1, 1
	filteredProof.ExactBaseRerankScoreCalls, filteredProof.ExactBaseVectorBytesRead = 1, 8
	filteredWork := work
	filteredWork.Graph.BaseShadowed = 0
	filteredWork.Graph.ExactBaseScored, filteredWork.Graph.BaseResultIDs = 1, 1
	filteredWork.Graph.Filter = collections.ColumnGraphFilterWork{Attempted: true, Completed: true, EligibleRows: denseTypedScalarExactLimit + 1}
	if err := validateDenseQuantizedScorePlaneResponse(filteredWork, &filteredProof, filteredRequest, 1); err != nil {
		t.Fatalf("valid filtered public proof rejected: %v", err)
	}
	if err := validateDenseQuantizedScorePlaneResponse(work, proof, filteredRequest, 0); err == nil {
		t.Fatal("filtered request accepted without filter work")
	}
	if err := validateDenseQuantizedScorePlaneResponse(filteredWork, &filteredProof, request, 1); err == nil {
		t.Fatal("unfiltered request accepted with filter work")
	}
	underfilledRequest := filteredRequest
	underfilledRequest.TopK = 2
	underfilledProof := filteredProof
	underfilledProof.RequestedTopK = 2
	underfilledProof.NormalizedCandidateWidth, underfilledProof.RawCandidateWidth, underfilledProof.RerankCandidateCap = 2, 2, 2
	underfilledWork := filteredWork
	underfilledWork.Graph.Filter.EligibleRows = 2
	if err := validateDenseQuantizedScorePlaneResponse(underfilledWork, &underfilledProof, underfilledRequest, 1); err == nil {
		t.Fatal("filtered response underfilled captured eligible rows")
	}
	overScoredProof := filteredProof
	overScoredProof.Route = "typed_exact"
	overScoredProof.RawRetainedCandidates, overScoredProof.LiveShortlistCandidates, overScoredProof.ActualRerankCandidates = 0, 0, 0
	overScoredProof.QuantizedScoreCalls, overScoredProof.QuantizedCodeBytesRead = 0, 0
	overScoredProof.ExactBaseRerankScoreCalls, overScoredProof.ExactSmallFilterScoreCalls = 0, 2
	overScoredProof.ExactBaseVectorBytesRead = 16
	overScoredWork := filteredWork
	overScoredWork.Graph.Route = "typed_exact"
	overScoredWork.Graph.BaseANNScored, overScoredWork.Graph.ExactBaseScored, overScoredWork.Graph.BaseResultIDs = 0, 2, 2
	overScoredWork.Graph.Filter.EligibleRows = 1
	if err := validateDenseQuantizedScorePlaneResponse(overScoredWork, &overScoredProof, filteredRequest, 1); err == nil {
		t.Fatal("filtered response exact-scored beyond captured eligible rows")
	}
	underScoredExactProof := filteredProof
	underScoredExactProof.Route = "typed_exact"
	underScoredExactProof.QuantizedScoreCalls, underScoredExactProof.QuantizedCodeBytesRead = 0, 0
	underScoredExactProof.RawRetainedCandidates, underScoredExactProof.LiveShortlistCandidates = 0, 0
	underScoredExactProof.ActualRerankCandidates, underScoredExactProof.ExactBaseRerankScoreCalls = 0, 0
	underScoredExactProof.ExactBaseVectorBytesRead = 0
	underScoredExactProof.ExactSuffixScoreCalls, underScoredExactProof.ExactSuffixVectorBytesRead = 1, 8
	underScoredExactWork := filteredWork
	underScoredExactWork.Graph.Route = "typed_exact"
	underScoredExactWork.Graph.BaseANNScored, underScoredExactWork.Graph.ExactBaseScored = 0, 0
	underScoredExactWork.Graph.BaseResultIDs, underScoredExactWork.Graph.DeltaScored = 0, 1
	underScoredExactWork.Graph.Filter.EligibleRows = 2
	if err := validateDenseQuantizedScorePlaneResponse(underScoredExactWork, &underScoredExactProof, filteredRequest, 1); err == nil {
		t.Fatal("filtered typed-exact response did not score every eligible row")
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
	candidateMismatch := work
	candidateMismatch.Graph.BaseCandidates = proof.QuantizedScoreCalls + 1
	if err := validateDenseQuantizedScorePlaneResponse(candidateMismatch, proof, request, 0); err == nil {
		t.Fatal("score-plane proof accepted more graph candidates than quantized score calls")
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
	strictEmptyProof.QuantizedScoreCalls = 0
	strictEmptyProof.QuantizedCodeBytesRead = 0
	strictEmptyProof.NormalizedCandidateWidth = 1
	strictEmptyProof.RawCandidateWidth = 1
	strictEmptyProof.RerankCandidateCap = 1
	strictEmptyProof.RawRetainedCandidates = 0
	strictEmptyWork := work
	strictEmptyWork.Graph.Route = "typed_empty"
	strictEmptyWork.Graph.BaseANNScored = 0
	strictEmptyWork.Graph.BaseShadowed = 0
	strictEmptyWork.Graph.Filter = collections.ColumnGraphFilterWork{Attempted: true, Completed: true}
	emptyRequest := request
	emptyRequest.Filter = new(documentservice.Filter)
	if err := validateDenseQuantizedScorePlaneResponse(strictEmptyWork, &strictEmptyProof, emptyRequest, 0); err != nil {
		t.Fatalf("valid typed-empty proof rejected: %v", err)
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphFilterWork){
		"unattempted filter": func(f *collections.ColumnGraphFilterWork) { f.Attempted = false; f.Completed = false },
		"incomplete filter":  func(f *collections.ColumnGraphFilterWork) { f.Completed = false },
		"eligible rows":      func(f *collections.ColumnGraphFilterWork) { f.EligibleRows = 1 },
	} {
		candidate := strictEmptyWork
		mutate(&candidate.Graph.Filter)
		if err := validateDenseQuantizedScorePlaneResponse(candidate, &strictEmptyProof, emptyRequest, 0); err == nil {
			t.Fatalf("typed-empty proof accepted %s: %+v", name, candidate.Graph.Filter)
		}
	}
	for name, mutate := range map[string]func(*collections.ColumnGraphScorePlaneWork){
		"quantized work":      func(p *collections.ColumnGraphScorePlaneWork) { p.QuantizedScoreCalls = 1 },
		"exact suffix work":   func(p *collections.ColumnGraphScorePlaneWork) { p.ExactSuffixScoreCalls = 1 },
		"small-filter work":   func(p *collections.ColumnGraphScorePlaneWork) { p.ExactSmallFilterScoreCalls = 1 },
		"retained candidates": func(p *collections.ColumnGraphScorePlaneWork) { p.RawRetainedCandidates = 1 },
		"live shortlist":      func(p *collections.ColumnGraphScorePlaneWork) { p.LiveShortlistCandidates = 1 },
	} {
		candidate := strictEmptyProof
		mutate(&candidate)
		if err := validateDenseQuantizedScorePlaneResponse(strictEmptyWork, &candidate, emptyRequest, 0); err == nil {
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
	strictExactProof.NormalizedCandidateWidth = 0
	strictExactProof.RawCandidateWidth = 0
	strictExactProof.RerankCandidateCap = 0
	strictExactProof.RawRetainedCandidates = 0
	strictExactProof.LiveShortlistCandidates = 0
	strictExactWork := work
	strictExactWork.Graph.Route = "typed_exact"
	strictExactWork.Graph.BaseANNScored = 0
	strictExactWork.Graph.BaseShadowed = 0
	strictExactWork.Graph.DeltaScored = 1
	if err := validateDenseQuantizedScorePlaneResponse(strictExactWork, &strictExactProof, request, 1); err != nil {
		t.Fatalf("valid unfiltered typed-exact proof rejected: %v", err)
	}
	for name, mutate := range map[string]func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork){
		"planning widths": func(_ *documentservice.DenseSearchWork, proof *collections.ColumnGraphScorePlaneWork) {
			proof.NormalizedCandidateWidth, proof.RawCandidateWidth, proof.RerankCandidateCap = 1, 1, 1
		},
		"small-filter scoring": func(work *documentservice.DenseSearchWork, proof *collections.ColumnGraphScorePlaneWork) {
			proof.ExactSmallFilterScoreCalls, proof.ExactBaseVectorBytesRead = 1, 8
			work.Graph.ExactBaseScored, work.Graph.BaseResultIDs = 1, 1
		},
		"base shadowing": func(work *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			work.Graph.BaseShadowed = 1
		},
		"base edge work": func(work *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			work.Graph.BaseEdges = 1
		},
	} {
		candidateWork, candidateProof := strictExactWork, strictExactProof
		mutate(&candidateWork, &candidateProof)
		if err := validateDenseQuantizedScorePlaneResponse(candidateWork, &candidateProof, request, 1); err == nil {
			t.Fatalf("unfiltered typed-exact route accepted %s", name)
		}
	}
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
	for name, candidate := range map[string]collections.ColumnGraphScorePlaneWork{
		"typed-empty": strictEmptyProof,
		"typed-exact": strictExactProof,
	} {
		candidate.RequestedTopK, candidate.RequestedEFSearch = 1, 1
		candidate.NormalizedCandidateWidth, candidate.RawCandidateWidth, candidate.RerankCandidateCap = 100, 100, 100
		if denseScorePlaneRerankCountersMatch(&candidate) {
			t.Fatalf("%s proof accepted impossible shortcut planning widths: %+v", name, candidate)
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

func TestDenseWorkResultsBindOutputDiagnostics(t *testing.T) {
	results := []DenseVectorSearchResult{{ID: []byte("a"), Document: []byte("{}")}}
	work := documentservice.DenseSearchWork{
		Completed: true,
		Output: documentservice.DenseSearchOutputWork{
			Fetched: 1, OutputBytes: 2, RetainedPayloadFetches: 1,
			JSONReconstructionRows: 1, TypedColumnRows: 1,
		},
	}
	if err := validateDenseWorkResults(work, results); err != nil {
		t.Fatalf("valid dense output diagnostics rejected: %v", err)
	}
	for name, mutate := range map[string]func(*documentservice.DenseSearchOutputWork){
		"retained payload fetches": func(output *documentservice.DenseSearchOutputWork) { output.RetainedPayloadFetches = 0 },
		"JSON reconstruction rows": func(output *documentservice.DenseSearchOutputWork) { output.JSONReconstructionRows = 0 },
		"typed column rows":        func(output *documentservice.DenseSearchOutputWork) { output.TypedColumnRows = 2 },
	} {
		candidate := work
		mutate(&candidate.Output)
		if err := validateDenseWorkResults(candidate, results); err == nil {
			t.Fatalf("dense output accepted inconsistent %s", name)
		}
	}
}

func TestDenseV3ResultsHaveValidIDs(t *testing.T) {
	if !denseV3ResultsHaveValidIDs([]DenseVectorSearchResult{{ID: []byte("a")}, {ID: []byte("b")}, {ID: []byte("\x1cc")}}) {
		t.Fatal("valid unique result IDs rejected")
	}
	for name, results := range map[string][]DenseVectorSearchResult{
		"duplicate":           {{ID: []byte("a")}, {ID: []byte("a")}},
		"empty":               {{ID: nil}},
		"whitespace only":     {{ID: []byte(" \t")}},
		"leading whitespace":  {{ID: []byte(" a")}},
		"trailing whitespace": {{ID: []byte("a ")}},
		"invalid UTF-8":       {{ID: []byte{0xff}}},
	} {
		if denseV3ResultsHaveValidIDs(results) {
			t.Fatalf("invalid %s result ID accepted", name)
		}
	}
}

func TestDenseV3ResultsHaveCosineScores(t *testing.T) {
	if !denseV3ResultsHaveCosineScores([]DenseVectorSearchResult{
		{Score: -1 - denseCosineScoreTolerance},
		{Score: 1 + denseCosineScoreTolerance},
	}) {
		t.Fatal("bounded cosine rounding tolerance rejected")
	}
	for _, score := range []float64{-100, 100, math.NaN(), math.Inf(1)} {
		if denseV3ResultsHaveCosineScores([]DenseVectorSearchResult{{Score: score}}) {
			t.Fatalf("invalid cosine score accepted: %v", score)
		}
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
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true, SchemaHash: 7, SchemaGeneration: 2,
			BaseManifest:    collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
			CurrentManifest: collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
			BaseCoverageLSN: 1, CurrentCoverageLSN: 1},
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
	responseFor := func(meta, id []byte) []byte {
		t.Helper()
		var body []byte
		for _, section := range []iwire.Section{
			{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, id)},
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
	for name, candidate := range map[string]struct{ meta, id []byte }{
		"candidate count":    {append([]byte{3, 0, 0, 0, 1}, validScore...), []byte("a")},
		"nonfinite score":    {append([]byte{3, 1, 0, 0, 1}, binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.NaN()))...), []byte("a")},
		"out-of-range score": {append([]byte{3, 1, 0, 0, 1}, binary.LittleEndian.AppendUint64(nil, math.Float64bits(100))...), []byte("a")},
		"empty ID":           {append([]byte{3, 1, 0, 0, 1}, validScore...), nil},
		"leading-space ID":   {append([]byte{3, 1, 0, 0, 1}, validScore...), []byte(" a")},
		"trailing-space ID":  {append([]byte{3, 1, 0, 0, 1}, validScore...), []byte("a ")},
	} {
		t.Run(name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			client := NewClient(clientConn)
			client.denseTypedQuantizedNegotiated = true
			response := responseFor(candidate.meta, candidate.id)
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

func TestDenseV3WireErrorMalformedProofPreservesSibling(t *testing.T) {
	snapshot := collections.ColumnGraphQuerySnapshot{
		Available: true, SchemaGeneration: 1,
		BaseManifest: collections.ColumnGraphManifestWork{
			Generation: 1, Format: "tcs1", Version: 1, Checksum: 3,
		},
		CurrentManifest: collections.ColumnGraphManifestWork{
			Generation: 1, Format: "tcs1", Version: 1, Checksum: 3,
		},
		BaseCoverageLSN: 1, CurrentCoverageLSN: 1,
	}
	work := documentservice.DenseSearchWork{
		Version: 1,
		Graph: collections.ColumnGraphQueryWork{
			Available: true,
			Snapshot:  snapshot,
		},
	}
	workRaw, err := appendDenseWork(nil, work)
	if err != nil {
		t.Fatal(err)
	}
	proof := collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true,
		RequestedMode:      collections.VectorIndexQueryModeQuantizedRerank,
		EffectiveMode:      collections.VectorIndexQueryModeQuantizedRerank,
		Reason:             "incomplete",
		QuantizedIndexName: "embedding.scalar_u8.public",
		QuantizedCodec:     collections.QuantizedVectorCodecScalarU8,
		QuantizedVersion:   1,
	}
	proofRaw, err := appendDenseScorePlane(nil, proof, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name                string
		workRaw, proofRaw   []byte
		wantWork, wantProof bool
	}{
		{"malformed score plane", workRaw, []byte{1}, true, false},
		{"malformed dense work", []byte{1}, proofRaw, false, true},
		{"both proofs malformed", []byte{1}, []byte{1}, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			for _, section := range []iwire.Section{
				{ID: iwire.SectionError, Bytes: appendErrorPayload(nil, iwire.ErrInternal, false, "original")},
				{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: tt.workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: tt.proofRaw},
			} {
				body, err = iwire.AppendSection(body, section)
				if err != nil {
					t.Fatal(err)
				}
			}
			got := decodeWireErrorVersion(body, iwire.DefaultLimits(), iwire.DenseVectorSearchTypedQuantizedVersion)
			var decodeErr *DenseVectorSearchDecodeError
			if !errors.As(got, &decodeErr) || (decodeErr.DenseWork != nil) != tt.wantWork || (decodeErr.ScorePlane != nil) != tt.wantProof {
				t.Fatalf("malformed proof lost valid sibling: %v", got)
			}
			if tt.wantWork && *decodeErr.DenseWork != work {
				t.Fatalf("dense work changed: %+v", decodeErr.DenseWork)
			}
			if tt.wantProof && *decodeErr.ScorePlane != proof {
				t.Fatalf("score plane changed: %+v", decodeErr.ScorePlane)
			}
			if nativeCodeOf(got) != iwire.ErrMalformedFrame {
				t.Fatalf("malformed proof code=%d", nativeCodeOf(got))
			}
		})
	}
	malformedRetry := binary.AppendUvarint(nil, uint64(iwire.ErrInternal))
	malformedRetry = append(malformedRetry, 2, 0)
	validError := appendErrorPayload(nil, iwire.ErrInternal, false, "original")
	for _, tt := range []struct {
		name     string
		sections []iwire.Section
		wantCode iwire.ErrorCode
	}{
		{
			name: "missing error metadata",
			sections: []iwire.Section{
				{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
			},
			wantCode: iwire.ErrMalformedFrame,
		},
		{
			name: "duplicate error metadata",
			sections: []iwire.Section{
				{ID: iwire.SectionError, Bytes: validError},
				{ID: iwire.SectionError, Bytes: validError},
				{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
			},
			wantCode: iwire.ErrInvalidCommand,
		},
		{
			name: "malformed error metadata",
			sections: []iwire.Section{
				{ID: iwire.SectionError, Bytes: malformedRetry},
				{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
			},
			wantCode: iwire.ErrMalformedFrame,
		},
		{
			name: "unknown critical sibling",
			sections: []iwire.Section{
				{ID: iwire.SectionError, Bytes: validError},
				{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
				{ID: 999, Flags: iwire.SectionFlagCritical},
			},
			wantCode: iwire.ErrUnsupportedFeature,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			for _, section := range tt.sections {
				var err error
				body, err = iwire.AppendSection(body, section)
				if err != nil {
					t.Fatal(err)
				}
			}
			got := decodeWireErrorVersion(body, iwire.DefaultLimits(), iwire.DenseVectorSearchTypedQuantizedVersion)
			var decodeErr *DenseVectorSearchDecodeError
			if !errors.As(got, &decodeErr) || decodeErr.DenseWork == nil || decodeErr.ScorePlane == nil {
				t.Fatalf("malformed error envelope lost proofs: %v", got)
			}
			if *decodeErr.DenseWork != work || *decodeErr.ScorePlane != proof || nativeCodeOf(got) != tt.wantCode {
				t.Fatalf("malformed error envelope changed proofs or code: %v", got)
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
