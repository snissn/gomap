package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"strings"
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
	incomplete.Reason = "incomplete"
	incomplete.Snapshot = collections.ColumnGraphQuerySnapshot{}
	incompleteRaw, err := appendDenseScorePlane(nil, incomplete, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	incompleteDecoded, err := decodeDenseScorePlane(incompleteRaw, iwire.DefaultLimits())
	if err != nil || !incompleteDecoded.Available || incompleteDecoded.Completed || incompleteDecoded.Snapshot.Available {
		t.Fatalf("incomplete score-plane flags were not preserved: %+v err=%v", incompleteDecoded, err)
	}
	incomplete.Reason = ""
	if err := validateDenseScorePlane(incomplete); err == nil {
		t.Fatal("available incomplete score-plane proof without a reason accepted")
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
		"schema hash":       func(s *collections.ColumnGraphQuerySnapshot) { s.SchemaHash = 0 },
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
		QuantizedCodec: collections.QuantizedVectorCodecScalarU8, QuantizedVersion: 1,
		RequestedTopK: 1, RequestedEFSearch: 8,
		NormalizedCandidateWidth: 1, RawCandidateWidth: 1, RerankCandidateCap: 1,
		RawRetainedCandidates: 1, QuantizedScoreCalls: 1,
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true, SchemaHash: 1, SchemaGeneration: 1,
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
	unfilteredCandidates := *proof
	unfilteredCandidates.NormalizedCandidateWidth, unfilteredCandidates.RawCandidateWidth = 2, 2
	unfilteredCandidates.RerankCandidateCap, unfilteredCandidates.RawRetainedCandidates = 2, 2
	unfilteredCandidates.LiveShortlistCandidates, unfilteredCandidates.ActualRerankCandidates = 1, 1
	unfilteredCandidates.QuantizedScoreCalls, unfilteredCandidates.QuantizedCodeBytesRead = 2, 4
	unfilteredCandidates.ExactBaseRerankScoreCalls, unfilteredCandidates.ExactBaseVectorBytesRead = 1, 8
	unfilteredCandidateWork := work
	unfilteredCandidateWork.Graph.BaseANNScored = 2
	unfilteredCandidateWork.Graph.ExactBaseScored, unfilteredCandidateWork.Graph.BaseResultIDs = 1, 1
	if err := validateDenseQuantizedScorePlaneResponse(unfilteredCandidateWork, &unfilteredCandidates, request, 1); err != nil {
		t.Fatalf("valid expanded unfiltered public proof rejected: %v", err)
	}
	unfilteredCandidateWork.Graph.BaseCandidates = 1
	if err := validateDenseQuantizedScorePlaneResponse(unfilteredCandidateWork, &unfilteredCandidates, request, 1); err == nil {
		t.Fatal("unfiltered quantized proof accepted a suppressed base-candidate count")
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
	filteredWork.Graph.BaseCandidates, filteredWork.Graph.BaseShadowed = 1, 0
	filteredWork.Graph.ExactBaseScored, filteredWork.Graph.BaseResultIDs = 1, 1
	filteredWork.Graph.Filter = collections.ColumnGraphFilterWork{Attempted: true, Completed: true, EligibleRows: denseTypedScalarExactLimit + 1}
	if err := validateDenseQuantizedScorePlaneResponse(filteredWork, &filteredProof, filteredRequest, 1); err != nil {
		t.Fatalf("valid filtered public proof rejected: %v", err)
	}
	filteredUnderreported := filteredWork
	filteredUnderreported.Graph.BaseCandidates = 0
	if err := validateDenseQuantizedScorePlaneResponse(filteredUnderreported, &filteredProof, filteredRequest, 1); err == nil {
		t.Fatal("filtered quantized proof retained more candidates than graph work reported")
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
	zeroWidthExactProof := underScoredExactProof
	zeroWidthExactProof.NormalizedCandidateWidth, zeroWidthExactProof.RawCandidateWidth, zeroWidthExactProof.RerankCandidateCap = 0, 0, 0
	zeroWidthExactWork := underScoredExactWork
	zeroWidthExactWork.Graph.BaseCandidates = 0
	zeroWidthExactWork.Graph.Filter.EligibleRows = 1
	if err := validateDenseQuantizedScorePlaneResponse(zeroWidthExactWork, &zeroWidthExactProof, filteredRequest, 1); err != nil {
		t.Fatalf("valid filtered zero-width suffix-only proof rejected: %v", err)
	}
	for name, mutate := range map[string]func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork){
		"raw candidate width": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RawCandidateWidth = 1
		},
		"base scoring": func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.DeltaScored, w.Graph.ExactBaseScored, w.Graph.BaseResultIDs = 0, 1, 1
			p.ExactSuffixScoreCalls, p.ExactSuffixVectorBytesRead = 0, 0
			p.ExactSmallFilterScoreCalls, p.ExactBaseVectorBytesRead = 1, 8
		},
		"base shadowing": func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseShadowed = 1
		},
	} {
		candidateWork, candidateProof := zeroWidthExactWork, zeroWidthExactProof
		mutate(&candidateWork, &candidateProof)
		if name != "base shadowing" {
			if _, err := appendDenseScorePlane(nil, candidateProof, iwire.DefaultLimits()); err == nil {
				t.Fatalf("score-plane encoder accepted zero-width %s", name)
			}
		}
		if err := validateDenseQuantizedScorePlaneResponse(candidateWork, &candidateProof, filteredRequest, 1); err == nil {
			t.Fatalf("filtered zero-width typed-exact proof accepted %s", name)
		}
		remote := &WireError{Code: iwire.ErrInternal, Message: name, DenseWork: &candidateWork, ScorePlane: &candidateProof}
		if got := validateDenseQuantizedFailureProof(remote, filteredRequest); got == remote {
			t.Fatalf("filtered zero-width failure proof accepted %s", name)
		}
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

func TestDenseV3ResultDocumentsMatchRequest(t *testing.T) {
	withoutEmbedding := DenseVectorSearchRequest{Query: []float32{1, 0}}
	withEmbedding := withoutEmbedding
	withEmbedding.ReturnEmbedding = true
	filtered := withoutEmbedding
	filtered.Filter = &documentservice.Filter{Operator: "AND", Conditions: []documentservice.Filter{
		{Field: "meta.tenant", Operator: "==", Value: "a"},
		{Field: "meta.rank", Operator: ">=", Value: int64(2)},
	}}
	for name, candidate := range map[string]struct {
		result  DenseVectorSearchResult
		request DenseVectorSearchRequest
		valid   bool
	}{
		"omitted embedding":     {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","content":"alpha","meta":{"kind":"test"}}`)}, withoutEmbedding, true},
		"requested embedding":   {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding":[1,0]}`)}, withEmbedding, true},
		"matching filter":       {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","meta":{"tenant":"a","rank":2}}`)}, filtered, true},
		"mismatched filter":     {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","meta":{"tenant":"b","rank":2}}`)}, filtered, false},
		"missing filter field":  {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","meta":{"tenant":"a"}}`)}, filtered, false},
		"mismatched ID":         {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"b"}`)}, withoutEmbedding, false},
		"null content":          {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","content":null}`)}, withoutEmbedding, false},
		"malformed JSON":        {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":`)}, withoutEmbedding, false},
		"invalid UTF-8":         {DenseVectorSearchResult{ID: []byte("a"), Document: []byte{'{', '"', 'i', 'd', '"', ':', '"', 0xff, '"', '}'}}, withoutEmbedding, false},
		"unknown field":         {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","future":1}`)}, withoutEmbedding, false},
		"case-variant ID":       {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"ID":"a"}`)}, withoutEmbedding, false},
		"duplicate ID":          {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"b","id":"a"}`)}, withoutEmbedding, false},
		"hidden embedding":      {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding":[1,0],"embedding":null}`)}, withoutEmbedding, false},
		"inner score":           {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","score":1}`)}, withoutEmbedding, false},
		"compact embedding":     {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding_f32_le_b64":"AACAPwAAAAA="}`)}, withoutEmbedding, false},
		"unrequested embedding": {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding":[1,0]}`)}, withoutEmbedding, false},
		"empty embedding":       {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding":[]}`)}, withoutEmbedding, false},
		"missing embedding":     {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a"}`)}, withEmbedding, false},
		"wrong embedding size":  {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding":[1]}`)}, withEmbedding, false},
		"overflowing embedding": {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a","embedding":[3.5e38,0]}`)}, withEmbedding, false},
		"trailing JSON value":   {DenseVectorSearchResult{ID: []byte("a"), Document: []byte(`{"id":"a"}{}`)}, withoutEmbedding, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := denseV3ResultDocumentsMatchRequest([]DenseVectorSearchResult{candidate.result}, candidate.request); got != candidate.valid {
				t.Fatalf("document validation=%v, want %v", got, candidate.valid)
			}
		})
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

func TestDenseWorkRequiresScorePlaneCoversEveryExecutionSignal(t *testing.T) {
	preOwner := documentservice.DenseSearchWork{Version: 1, Graph: collections.ColumnGraphQueryWork{
		Available: true,
		Filter: collections.ColumnGraphFilterWork{
			Attempted: true, Completed: true, EligibleRows: 1, SourceIDs: 1, SourceBytes: 1,
			InspectedEntries: 1, MappingWorkCharged: 1, RetainedBytes: 1, ScratchIDBytes: 1,
			ScratchRows: 1, OrdinalGrowthPeakBytes: 1,
		},
		Snapshot: collections.ColumnGraphQuerySnapshot{
			Available: true, SchemaHash: 1, SchemaGeneration: 1,
			BaseManifest:    collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 1},
			CurrentManifest: collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 1},
			BaseCoverageLSN: 1, CurrentCoverageLSN: 1,
		},
	}}
	if denseWorkRequiresScorePlane(&preOwner) {
		t.Fatal("snapshot/filter-only pre-owner work requires a score-plane proof")
	}
	mutations := []struct {
		name   string
		mutate func(*documentservice.DenseSearchWork)
	}{
		{"service completed", func(w *documentservice.DenseSearchWork) { w.Completed = true }},
		{"graph completed", func(w *documentservice.DenseSearchWork) { w.Graph.Completed = true }},
		{"typed empty route", func(w *documentservice.DenseSearchWork) { w.Graph.Route = "typed_empty" }},
		{"typed exact route", func(w *documentservice.DenseSearchWork) { w.Graph.Route = "typed_exact" }},
		{"typed hnsw route", func(w *documentservice.DenseSearchWork) { w.Graph.Route = "typed_hnsw" }},
		{"base ANN scored", func(w *documentservice.DenseSearchWork) { w.Graph.BaseANNScored = 1 }},
		{"base candidates", func(w *documentservice.DenseSearchWork) { w.Graph.BaseCandidates = 1 }},
		{"base edges", func(w *documentservice.DenseSearchWork) { w.Graph.BaseEdges = 1 }},
		{"delta scored", func(w *documentservice.DenseSearchWork) { w.Graph.DeltaScored = 1 }},
		{"exact base scored", func(w *documentservice.DenseSearchWork) { w.Graph.ExactBaseScored = 1 }},
		{"base shadowed", func(w *documentservice.DenseSearchWork) { w.Graph.BaseShadowed = 1 }},
		{"base result IDs", func(w *documentservice.DenseSearchWork) { w.Graph.BaseResultIDs = 1 }},
		{"output attempted", func(w *documentservice.DenseSearchWork) { w.Output.Attempted = true }},
		{"output completed", func(w *documentservice.DenseSearchWork) { w.Output.Completed = true }},
		{"output requested", func(w *documentservice.DenseSearchWork) { w.Output.Requested = 1 }},
		{"output fetched", func(w *documentservice.DenseSearchWork) { w.Output.Fetched = 1 }},
		{"output missing", func(w *documentservice.DenseSearchWork) { w.Output.Missing = 1 }},
		{"output bytes", func(w *documentservice.DenseSearchWork) { w.Output.OutputBytes = 1 }},
		{"retained payload fetches", func(w *documentservice.DenseSearchWork) { w.Output.RetainedPayloadFetches = 1 }},
		{"JSON reconstruction rows", func(w *documentservice.DenseSearchWork) { w.Output.JSONReconstructionRows = 1 }},
		{"typed column rows", func(w *documentservice.DenseSearchWork) { w.Output.TypedColumnRows = 1 }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := preOwner
			mutation.mutate(&candidate)
			if !denseWorkRequiresScorePlane(&candidate) {
				t.Fatal("execution signal did not require a score-plane proof")
			}
		})
	}
}

func denseV3FrameErrorForTest(t *testing.T, request DenseVectorSearchRequest, sections ...iwire.Section) error {
	t.Helper()
	frameSections := append([]iwire.Section{{ID: iwire.SectionError, Bytes: appendErrorPayload(nil, iwire.ErrInternal, false, "original")}}, sections...)
	var frameError []byte
	for _, section := range frameSections {
		var err error
		frameError, err = iwire.AppendSection(frameError, section)
		if err != nil {
			t.Fatal(err)
		}
	}
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn)
	client.denseTypedQuantizedNegotiated = true
	errCh := make(chan error, 1)
	go func() {
		header, _, err := readFrame(serverConn, iwire.DefaultLimits())
		if err == nil {
			err = writeFrame(serverConn, iwire.Header{Type: iwire.FrameError, RequestID: header.RequestID}, frameError)
		}
		errCh <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, got := client.DenseVectorSearch(ctx, request)
	cancel()
	_ = client.Close()
	_ = serverConn.Close()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDenseV3FailureProofBindsRequestWithoutRetainingRemoteError(t *testing.T) {
	request := DenseVectorSearchRequest{
		TypedColumnGraph: true, Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8,
		QueryMode:          collections.VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName: "embedding.scalar_u8.public", QuantizedRerankCandidates: 8,
		ExpectedGeneration: 2,
	}
	snapshot := collections.ColumnGraphQuerySnapshot{Available: true, SchemaHash: 1, SchemaGeneration: 2,
		BaseManifest:    collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
		CurrentManifest: collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
		BaseCoverageLSN: 1, CurrentCoverageLSN: 1}
	proof := collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, RequestedMode: request.QueryMode,
		EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank, Route: "quantized_rerank",
		Reason: "incomplete", QuantizedIndexName: request.QuantizedIndexName,
		QuantizedCodec: collections.QuantizedVectorCodecScalarU8, QuantizedVersion: 1,
		RequestedTopK: uint64(request.TopK), RequestedEFSearch: uint64(request.EfSearch),
		RequestedRerankCandidates: uint64(request.QuantizedRerankCandidates), Snapshot: snapshot,
	}
	work := documentservice.DenseSearchWork{Version: 1, Graph: collections.ColumnGraphQueryWork{
		Available: true, Filter: collections.ColumnGraphFilterWork{}, Snapshot: snapshot,
	}}
	remote := &WireError{Code: iwire.ErrInternal, Message: "original", DenseWork: &work, ScorePlane: &proof}
	if got := validateDenseQuantizedFailureProof(remote, request); got != remote {
		t.Fatalf("matching failure proof changed: %v", got)
	}
	partialProof := proof
	partialProof.QuantizedScoreCalls = 1 // Prefix counters need not yet have matching byte charges.
	partialWork := work
	partialWork.Graph.BaseANNScored = 1
	partial := &WireError{Code: iwire.ErrInternal, Message: "partial", DenseWork: &partialWork, ScorePlane: &partialProof}
	if got := validateDenseQuantizedFailureProof(partial, request); got != partial {
		t.Fatalf("valid partial failure proof changed: %v", got)
	}
	filteredBeforeOwner := request
	filteredBeforeOwner.Filter = &documentservice.Filter{Field: "meta.user_id", Operator: "==", Value: "u"}
	preOwnerWork := work
	preOwnerWork.Graph.Filter = collections.ColumnGraphFilterWork{
		Attempted: true, Completed: true, EligibleRows: 1, SourceIDs: 1, SourceBytes: 1,
		InspectedEntries: 1, MappingWorkCharged: 1, RetainedBytes: 1, ScratchIDBytes: 1,
		ScratchRows: 1, OrdinalGrowthPeakBytes: 1,
	}
	preOwner := &WireError{Code: iwire.ErrInternal, Message: "pre-owner", DenseWork: &preOwnerWork}
	if got := validateDenseQuantizedFailureProof(preOwner, filteredBeforeOwner); got != preOwner {
		t.Fatalf("filtered failure before score-plane ownership changed: %v", got)
	}
	workOnly := &WireError{Code: iwire.ErrInternal, Message: "work-only", DenseWork: &work}
	if got := validateDenseQuantizedFailureProof(workOnly, request); got != workOnly {
		t.Fatalf("unfiltered work-only failure changed: %v", got)
	}
	proofOnly := &WireError{Code: iwire.ErrInternal, Message: "proof-only", ScorePlane: &proof}
	got := validateDenseQuantizedFailureProof(proofOnly, request)
	var proofOnlyDecode *DenseVectorSearchDecodeError
	var proofOnlyRemote *WireError
	if !errors.As(got, &proofOnlyDecode) || proofOnlyDecode.DenseWork != nil || proofOnlyDecode.ScorePlane == nil || *proofOnlyDecode.ScorePlane != proof ||
		!strings.Contains(got.Error(), "failure proof does not match the request") || errors.As(got, &proofOnlyRemote) {
		t.Fatalf("unfiltered proof-only failure was not rejected with diagnostic plane: %v", got)
	}

	typedEmptyWork, typedExactWork, typedHNSWWork := work, work, work
	typedEmptyWork.Graph.Route = "typed_empty"
	typedExactWork.Graph.Route = "typed_exact"
	typedHNSWWork.Graph.Route = "typed_hnsw"
	scoredWork, outputWork := work, work
	scoredWork.Graph.BaseANNScored = 1
	outputWork.Output.Attempted = true
	executedWork := []struct {
		name string
		work documentservice.DenseSearchWork
	}{
		{"typed_empty", typedEmptyWork},
		{"typed_exact", typedExactWork},
		{"typed_hnsw", typedHNSWWork},
		{"route-empty score", scoredWork},
		{"output attempted", outputWork},
	}
	for _, candidate := range executedWork {
		t.Run("direct work-only "+candidate.name, func(t *testing.T) {
			remote := &WireError{Code: iwire.ErrInternal, Message: "work-only", DenseWork: &candidate.work}
			got := validateDenseQuantizedFailureProof(remote, request)
			var decoded *DenseVectorSearchDecodeError
			var retainedRemote *WireError
			if !errors.As(got, &decoded) || decoded.DenseWork == nil || *decoded.DenseWork != candidate.work || decoded.ScorePlane != nil ||
				!strings.Contains(got.Error(), "failure proof does not match the request") || errors.As(got, &retainedRemote) {
				t.Fatalf("executed work-only proof was not de-authenticated with retained work: %v", got)
			}
		})
	}

	mutations := map[string]func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork){
		"mode": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RequestedMode = collections.VectorIndexQueryModeQuantizedOnly
		},
		"proof unavailable": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.Available = false
		},
		"graph unavailable": func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph = collections.ColumnGraphQueryWork{}
		},
		"proof snapshot unavailable": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.Snapshot = collections.ColumnGraphQuerySnapshot{}
		},
		"graph snapshot unavailable": func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Snapshot = collections.ColumnGraphQuerySnapshot{}
		},
		"name": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.QuantizedIndexName = "other"
		},
		"top k": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) { p.RequestedTopK++ },
		"EF": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RequestedEFSearch++
		},
		"rerank": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RequestedRerankCandidates++
		},
		"generation": func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Snapshot.SchemaGeneration++
			p.Snapshot.SchemaGeneration++
		},
		"snapshot": func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.Snapshot.SchemaHash++
		},
		"filter": func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Filter.Attempted, w.Graph.Filter.Completed = true, true
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidateWork, candidateProof := work, proof
			mutate(&candidateWork, &candidateProof)
			candidate := &WireError{Code: iwire.ErrInternal, Message: "original", DenseWork: &candidateWork, ScorePlane: &candidateProof}
			got := validateDenseQuantizedFailureProof(candidate, request)
			var decoded *DenseVectorSearchDecodeError
			if !errors.As(got, &decoded) || decoded.DenseWork == nil || decoded.ScorePlane == nil || !strings.Contains(got.Error(), "failure proof does not match the request") {
				t.Fatalf("mismatched proof was not rejected with retained diagnostics: %v", got)
			}
			var retainedRemote *WireError
			if errors.As(got, &retainedRemote) {
				t.Fatalf("rejected proof retained the remote service error: %v", got)
			}
		})
	}
	filtered := request
	filtered.Filter = &documentservice.Filter{Field: "meta.user_id", Operator: "==", Value: "u"}
	got = validateDenseQuantizedFailureProof(remote, filtered)
	if !strings.Contains(got.Error(), "failure proof does not match the request") {
		t.Fatalf("proof-bearing filtered failure omitted completed filter evidence: %v", got)
	}

	workRaw, err := appendDenseWork(nil, work)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedProof := proof
	mismatchedProof.QuantizedIndexName = "embedding.scalar_u8.other"
	proofRaw, err := appendDenseScorePlane(nil, mismatchedProof, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	validProofRaw, err := appendDenseScorePlane(nil, proof, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeDenseScorePlaneSection([]iwire.Section{{ID: iwire.SectionDenseSearchScorePlaneProof, Bytes: validProofRaw}}, true, iwire.DefaultLimits()); err == nil {
		t.Fatal("noncritical dense score-plane proof accepted")
	}
	if decoded, err := decodeDenseScorePlaneSection([]iwire.Section{{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: validProofRaw}}, true, iwire.DefaultLimits()); err != nil || *decoded != proof {
		t.Fatalf("critical dense score-plane proof rejected: %+v err=%v", decoded, err)
	}
	for name, sections := range map[string][]iwire.Section{
		"mismatched pair": {
			{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
			{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
		},
		"proof only": {
			{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: validProofRaw},
		},
	} {
		t.Run("FrameError "+name, func(t *testing.T) {
			got := denseV3FrameErrorForTest(t, request, sections...)
			var decoded *DenseVectorSearchDecodeError
			var retainedRemote *WireError
			if !strings.Contains(got.Error(), "failure proof does not match the request") || !errors.As(got, &decoded) ||
				decoded.ScorePlane == nil || (name == "proof only" && (decoded.DenseWork != nil || *decoded.ScorePlane != proof)) || errors.As(got, &retainedRemote) {
				t.Fatalf("public FrameError did not reject %s evidence with diagnostics: %v", name, got)
			}
		})
	}
	for _, name := range []string{
		"proof unavailable", "graph unavailable", "proof snapshot unavailable", "graph snapshot unavailable",
	} {
		t.Run("FrameError "+name, func(t *testing.T) {
			candidateWork, candidateProof := work, proof
			mutations[name](&candidateWork, &candidateProof)
			candidateWorkRaw, err := appendDenseWork(nil, candidateWork)
			if err != nil {
				t.Fatal(err)
			}
			candidateProofRaw, err := appendDenseScorePlane(nil, candidateProof, iwire.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			got := denseV3FrameErrorForTest(t, request,
				iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: candidateWorkRaw},
				iwire.Section{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: candidateProofRaw})
			var decoded *DenseVectorSearchDecodeError
			var retainedRemote *WireError
			if !errors.As(got, &decoded) || decoded.DenseWork == nil || decoded.ScorePlane == nil ||
				!strings.Contains(got.Error(), "failure proof does not match the request") || errors.As(got, &retainedRemote) {
				t.Fatalf("public FrameError retained impossible proof-bearing availability state: %v", got)
			}
		})
	}
	preOwnerRaw, err := appendDenseWork(nil, preOwnerWork)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("FrameError work-only pre-owner", func(t *testing.T) {
		got := denseV3FrameErrorForTest(t, filteredBeforeOwner, iwire.Section{
			ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: preOwnerRaw,
		})
		var retainedRemote *WireError
		if !errors.As(got, &retainedRemote) || retainedRemote.DenseWork == nil || *retainedRemote.DenseWork != preOwnerWork || retainedRemote.ScorePlane != nil {
			t.Fatalf("public FrameError changed valid pre-owner work-only evidence: %v", got)
		}
	})
	for _, candidate := range executedWork {
		t.Run("FrameError work-only "+candidate.name, func(t *testing.T) {
			workRaw, err := appendDenseWork(nil, candidate.work)
			if err != nil {
				t.Fatal(err)
			}
			got := denseV3FrameErrorForTest(t, request, iwire.Section{
				ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw,
			})
			var decoded *DenseVectorSearchDecodeError
			var retainedRemote *WireError
			if !errors.As(got, &decoded) || decoded.DenseWork == nil || *decoded.DenseWork != candidate.work || decoded.ScorePlane != nil ||
				!strings.Contains(got.Error(), "failure proof does not match the request") || errors.As(got, &retainedRemote) {
				t.Fatalf("public FrameError retained authenticated executed work-only evidence: %v", got)
			}
		})
	}
}

func TestDenseV3FailureProofCompletedGraphPrefixes(t *testing.T) {
	request := DenseVectorSearchRequest{
		TypedColumnGraph: true, Index: "docs", Query: []float32{1, 0}, TopK: 2, EfSearch: 8,
		QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.public",
		QuantizedRerankCandidates: 8, ExpectedGeneration: 2,
	}
	snapshot := collections.ColumnGraphQuerySnapshot{
		Available: true, SchemaHash: 7, SchemaGeneration: 2,
		BaseManifest:    collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
		CurrentManifest: collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
		BaseCoverageLSN: 1, CurrentCoverageLSN: 1,
	}
	proof := collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, Completed: true,
		RequestedMode: collections.VectorIndexQueryModeQuantizedRerank, EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank,
		Route: "quantized_rerank", QuantizedIndexName: request.QuantizedIndexName,
		QuantizedCodec: collections.QuantizedVectorCodecScalarU8, QuantizedVersion: 1,
		RequestedTopK: 2, RequestedEFSearch: 8, RequestedRerankCandidates: 8,
		NormalizedCandidateWidth: 2, RawCandidateWidth: 2, RerankCandidateCap: 2,
		RawRetainedCandidates: 2, LiveShortlistCandidates: 2, ActualRerankCandidates: 2,
		QuantizedScoreCalls: 2, QuantizedCodeBytesRead: 4,
		ExactBaseRerankScoreCalls: 2, ExactBaseVectorBytesRead: 16,
		Snapshot: snapshot,
	}
	work := documentservice.DenseSearchWork{Version: 1, Graph: collections.ColumnGraphQueryWork{
		Available: true, Completed: true, Route: "typed_hnsw", BaseANNScored: 2,
		ExactBaseScored: 2, BaseResultIDs: 2, Snapshot: snapshot,
	}}
	partialFetch := work
	partialFetch.Output = documentservice.DenseSearchOutputWork{
		Attempted: true, Requested: 2, Fetched: 1, OutputBytes: 2,
		RetainedPayloadFetches: 1, JSONReconstructionRows: 1, TypedColumnRows: 1,
	}
	completedFetchWithMissing := partialFetch
	completedFetchWithMissing.Output.Completed = true
	completedFetchWithMissing.Output.Missing = 1

	for name, candidate := range map[string]documentservice.DenseSearchWork{
		"post-search before fetch": work,
		"partial fetch":            partialFetch,
		"completed fetch missing":  completedFetchWithMissing,
	} {
		t.Run("valid "+name, func(t *testing.T) {
			remote := &WireError{Code: iwire.ErrInternal, Message: name, DenseWork: &candidate, ScorePlane: &proof}
			if got := validateDenseQuantizedFailureProof(remote, request); got != remote {
				t.Fatalf("valid outer-incomplete prefix changed: %v", got)
			}
		})
	}

	reject := func(t *testing.T, candidateWork documentservice.DenseSearchWork, candidateProof collections.ColumnGraphScorePlaneWork) {
		t.Helper()
		remote := &WireError{Code: iwire.ErrInternal, Message: "invalid", DenseWork: &candidateWork, ScorePlane: &candidateProof}
		got := validateDenseQuantizedFailureProof(remote, request)
		var decoded *DenseVectorSearchDecodeError
		var retainedRemote *WireError
		if !errors.As(got, &decoded) || decoded.DenseWork == nil || decoded.ScorePlane == nil ||
			*decoded.DenseWork != candidateWork || *decoded.ScorePlane != candidateProof || errors.As(got, &retainedRemote) {
			t.Fatalf("invalid completion prefix retained authenticated error or lost diagnostics: %v", got)
		}
	}

	invalid := []struct {
		name   string
		mutate func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork)
	}{
		{"proof complete graph incomplete", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Completed = false
		}},
		{"graph complete proof incomplete", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.Completed = false
			p.Reason = "incomplete"
		}},
		{"route", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Route = "typed_exact"
		}},
		{"snapshot", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.Snapshot.SchemaHash++
		}},
		{"filter ownership", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Filter = collections.ColumnGraphFilterWork{Attempted: true, Completed: true, EligibleRows: 2}
		}},
		{"graph counters", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseANNScored++
		}},
		{"planning counters", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RerankCandidateCap--
		}},
		{"byte counters", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.QuantizedCodeBytesRead--
		}},
		{"output requested", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Requested: 1}
		}},
		{"fetched beyond retained", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Requested: 2, Fetched: 1, JSONReconstructionRows: 1}
		}},
		{"retained plus missing", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Requested: 2, Missing: 1, RetainedPayloadFetches: 2}
		}},
		{"JSON rows", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Requested: 2, Fetched: 1, RetainedPayloadFetches: 1}
		}},
		{"typed rows", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Requested: 2, TypedColumnRows: 1}
		}},
		{"bytes before fetch", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Requested: 2, OutputBytes: 1}
		}},
		{"completed partial output", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Output = documentservice.DenseSearchOutputWork{Attempted: true, Completed: true, Requested: 2, Fetched: 1, RetainedPayloadFetches: 1, JSONReconstructionRows: 1}
		}},
		{"outer complete before output", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) { w.Completed = true }},
	}
	for _, mutation := range invalid {
		t.Run("invalid "+mutation.name, func(t *testing.T) {
			candidateWork, candidateProof := work, proof
			mutation.mutate(&candidateWork, &candidateProof)
			reject(t, candidateWork, candidateProof)
		})
	}

	matchingIncompleteWork, matchingIncompleteProof := work, proof
	matchingIncompleteWork.Graph.Completed = false
	matchingIncompleteProof.Completed = false
	matchingIncompleteProof.Reason = "scoring interrupted"
	prefixPair := func(graphRoute string) (documentservice.DenseSearchWork, collections.ColumnGraphScorePlaneWork) {
		candidateWork, candidateProof := matchingIncompleteWork, matchingIncompleteProof
		candidateWork.Graph.Route = graphRoute
		switch graphRoute {
		case "", "typed_empty":
			candidateWork.Graph.BaseANNScored, candidateWork.Graph.ExactBaseScored = 0, 0
			candidateWork.Graph.BaseResultIDs, candidateWork.Graph.DeltaScored = 0, 0
			candidateProof.QuantizedScoreCalls, candidateProof.ExactBaseRerankScoreCalls = 0, 0
			candidateProof.ExactSmallFilterScoreCalls, candidateProof.ExactSuffixScoreCalls = 0, 0
			candidateProof.RawRetainedCandidates, candidateProof.LiveShortlistCandidates = 0, 0
			candidateProof.ActualRerankCandidates = 0
			candidateProof.QuantizedCodeBytesRead, candidateProof.ExactBaseVectorBytesRead = 0, 0
			candidateProof.Route = "typed_empty"
			if graphRoute == "" {
				candidateProof.Route = "quantized_rerank"
			}
		case "typed_exact":
			candidateWork.Graph.BaseANNScored, candidateWork.Graph.ExactBaseScored = 0, 0
			candidateWork.Graph.BaseResultIDs, candidateWork.Graph.DeltaScored = 0, 1
			candidateProof.QuantizedScoreCalls, candidateProof.ExactBaseRerankScoreCalls = 0, 0
			candidateProof.ExactSmallFilterScoreCalls, candidateProof.ExactSuffixScoreCalls = 0, 1
			candidateProof.RawRetainedCandidates, candidateProof.LiveShortlistCandidates = 0, 0
			candidateProof.ActualRerankCandidates = 0
			candidateProof.QuantizedCodeBytesRead, candidateProof.ExactBaseVectorBytesRead = 0, 0
			candidateProof.ExactSuffixVectorBytesRead = 8
			candidateProof.Route = "typed_exact"
		case "typed_hnsw":
			candidateProof.Route = "quantized_rerank"
		}
		return candidateWork, candidateProof
	}
	prefixes := map[string]struct {
		work  documentservice.DenseSearchWork
		proof collections.ColumnGraphScorePlaneWork
	}{}
	for _, graphRoute := range []string{"", "typed_empty", "typed_exact", "typed_hnsw"} {
		candidateWork, candidateProof := prefixPair(graphRoute)
		prefixes[graphRoute] = struct {
			work  documentservice.DenseSearchWork
			proof collections.ColumnGraphScorePlaneWork
		}{candidateWork, candidateProof}
		t.Run("valid incomplete route "+graphRoute, func(t *testing.T) {
			matching := &WireError{Code: iwire.ErrInternal, Message: "partial", DenseWork: &candidateWork, ScorePlane: &candidateProof}
			if got := validateDenseQuantizedFailureProof(matching, request); got != matching {
				t.Fatalf("matching incomplete route prefix changed: %v", got)
			}
		})
	}
	matchingIncompleteWork, matchingIncompleteProof = prefixes["typed_hnsw"].work, prefixes["typed_hnsw"].proof
	counterMutations := []struct {
		name   string
		mutate func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork)
	}{
		{"quantized", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.QuantizedScoreCalls++
		}},
		{"exact base", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.ExactBaseScored++
		}},
		{"base result IDs", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseResultIDs++
		}},
		{"suffix", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.ExactSuffixScoreCalls++
		}},
		{"exact sum overflow", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.ExactBaseRerankScoreCalls, p.ExactSmallFilterScoreCalls = ^uint64(0), 1
		}},
	}
	for _, graphRoute := range []string{"", "typed_exact", "typed_hnsw"} {
		for _, mutation := range counterMutations {
			candidateWork, candidateProof := prefixes[graphRoute].work, prefixes[graphRoute].proof
			mutation.mutate(&candidateWork, &candidateProof)
			t.Run("invalid incomplete counters "+graphRoute+" "+mutation.name, func(t *testing.T) {
				reject(t, candidateWork, candidateProof)
			})
		}
	}
	prefixShapeMutations := []struct {
		name   string
		mutate func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork)
	}{
		{"base candidates exceed quantized calls", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseCandidates = p.QuantizedScoreCalls + 1
		}},
		{"rerank cap does not match plan", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RerankCandidateCap--
		}},
		{"normalized width exceeds explicit EF", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.NormalizedCandidateWidth, p.RawCandidateWidth, p.RerankCandidateCap = 9, 9, 8
		}},
		{"normalized width exceeds raw width", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RawCandidateWidth--
		}},
		{"raw retained exceeds quantized calls", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RawCandidateWidth, p.RawRetainedCandidates = 3, 3
		}},
		{"raw retained exceeds raw width", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseANNScored, p.QuantizedScoreCalls, p.RawRetainedCandidates = 3, 3, 3
		}},
		{"live shortlist exceeds raw retained", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.NormalizedCandidateWidth, p.RawCandidateWidth, p.RerankCandidateCap = 3, 3, 3
			p.LiveShortlistCandidates = 3
		}},
		{"live shortlist exceeds normalized width", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseANNScored, p.QuantizedScoreCalls = 3, 3
			p.RawCandidateWidth, p.RawRetainedCandidates, p.LiveShortlistCandidates = 3, 3, 3
		}},
		{"actual rerank exceeds shortlist", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseANNScored, w.Graph.ExactBaseScored, w.Graph.BaseResultIDs = 3, 3, 3
			p.NormalizedCandidateWidth, p.RawCandidateWidth, p.RerankCandidateCap = 3, 3, 3
			p.RawRetainedCandidates, p.QuantizedScoreCalls = 3, 3
			p.ActualRerankCandidates, p.ExactBaseRerankScoreCalls = 3, 3
		}},
		{"actual rerank exceeds cap", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.ExactBaseScored, w.Graph.BaseResultIDs = 3, 3
			p.ActualRerankCandidates, p.ExactBaseRerankScoreCalls = 3, 3
		}},
		{"actual rerank differs from exact base calls", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.ExactBaseScored, w.Graph.BaseResultIDs = 1, 1
			p.ExactBaseRerankScoreCalls = 1
		}},
	}
	for _, mutation := range prefixShapeMutations {
		candidateWork, candidateProof := prefixes["typed_hnsw"].work, prefixes["typed_hnsw"].proof
		mutation.mutate(&candidateWork, &candidateProof)
		t.Run("invalid incomplete prefix shape "+mutation.name, func(t *testing.T) {
			reject(t, candidateWork, candidateProof)
		})
	}
	zeroWidthPrefixWork, zeroWidthPrefixProof := prefixes["typed_exact"].work, prefixes["typed_exact"].proof
	zeroWidthPrefixProof.NormalizedCandidateWidth, zeroWidthPrefixProof.RawCandidateWidth, zeroWidthPrefixProof.RerankCandidateCap = 0, 0, 0
	zeroWidthRemote := &WireError{Code: iwire.ErrInternal, DenseWork: &zeroWidthPrefixWork, ScorePlane: &zeroWidthPrefixProof}
	if got := validateDenseQuantizedFailureProof(zeroWidthRemote, request); got != zeroWidthRemote {
		t.Fatalf("valid zero-width incomplete prefix rejected: %v", got)
	}
	zeroWidthPrefixMutations := []struct {
		name   string
		mutate func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork)
	}{
		{"raw candidate width", func(_ *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			p.RawCandidateWidth = 1
		}},
		{"base scoring", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.DeltaScored, w.Graph.ExactBaseScored, w.Graph.BaseResultIDs = 0, 1, 1
			p.ExactSuffixScoreCalls, p.ExactSmallFilterScoreCalls = 0, 1
		}},
		{"base shadowing", func(w *documentservice.DenseSearchWork, _ *collections.ColumnGraphScorePlaneWork) {
			w.Graph.BaseShadowed = 1
		}},
	}
	for _, mutation := range zeroWidthPrefixMutations {
		candidateWork, candidateProof := zeroWidthPrefixWork, zeroWidthPrefixProof
		mutation.mutate(&candidateWork, &candidateProof)
		t.Run("invalid incomplete zero-width "+mutation.name, func(t *testing.T) { reject(t, candidateWork, candidateProof) })
	}
	mismatchedIncompleteWork := matchingIncompleteWork
	mismatchedIncompleteWork.Graph.Route = "typed_exact"
	reject(t, mismatchedIncompleteWork, matchingIncompleteProof)
	for _, route := range []string{"typed_empty", "typed_exact"} {
		candidateWork, candidateProof := matchingIncompleteWork, matchingIncompleteProof
		candidateWork.Graph.Route, candidateProof.Route = "", route
		reject(t, candidateWork, candidateProof)
	}

	frameCase := func(t *testing.T, candidateWork documentservice.DenseSearchWork, candidateProof collections.ColumnGraphScorePlaneWork, valid bool) {
		t.Helper()
		workRaw, err := appendDenseWork(nil, candidateWork)
		if err != nil {
			t.Fatal(err)
		}
		proofRaw, err := appendDenseScorePlane(nil, candidateProof, iwire.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		got := denseV3FrameErrorForTest(t, request,
			iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
			iwire.Section{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw})
		if valid {
			var retainedRemote *WireError
			if !errors.As(got, &retainedRemote) {
				t.Fatalf("valid FrameError prefix was de-authenticated: %v", got)
			}
			return
		}
		var decoded *DenseVectorSearchDecodeError
		var retainedRemote *WireError
		if !errors.As(got, &decoded) || decoded.DenseWork == nil || decoded.ScorePlane == nil || errors.As(got, &retainedRemote) {
			t.Fatalf("invalid FrameError prefix retained authenticated error or lost diagnostics: %v", got)
		}
	}
	for name, candidate := range map[string]documentservice.DenseSearchWork{
		"post-search before fetch": work,
		"partial fetch":            partialFetch,
		"completed fetch missing":  completedFetchWithMissing,
	} {
		t.Run("FrameError valid "+name, func(t *testing.T) { frameCase(t, candidate, proof, true) })
	}
	for _, mutation := range []struct {
		name   string
		mutate func(*documentservice.DenseSearchWork, *collections.ColumnGraphScorePlaneWork)
	}{
		invalid[0], invalid[1], invalid[2], invalid[3], invalid[5], invalid[7], invalid[8],
		{"incomplete route", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Completed = false
			w.Graph.Route = "typed_exact"
			p.Completed = false
			p.Reason = "scoring interrupted"
		}},
		{"empty graph typed-empty proof", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Completed, w.Graph.Route = false, ""
			p.Completed, p.Route, p.Reason = false, "typed_empty", "scoring interrupted"
		}},
		{"empty graph typed-exact proof", func(w *documentservice.DenseSearchWork, p *collections.ColumnGraphScorePlaneWork) {
			w.Graph.Completed, w.Graph.Route = false, ""
			p.Completed, p.Route, p.Reason = false, "typed_exact", "scoring interrupted"
		}},
	} {
		t.Run("FrameError invalid "+mutation.name, func(t *testing.T) {
			candidateWork, candidateProof := work, proof
			mutation.mutate(&candidateWork, &candidateProof)
			frameCase(t, candidateWork, candidateProof, false)
		})
	}
	for _, example := range []struct {
		name     string
		route    string
		mutation int
	}{
		{"empty quantized counter", "", 0},
		{"typed exact exact-base counter", "typed_exact", 1},
		{"typed hnsw suffix counter", "typed_hnsw", 3},
		{"typed hnsw exact-sum overflow", "typed_hnsw", 4},
	} {
		t.Run("FrameError invalid "+example.name, func(t *testing.T) {
			candidateWork, candidateProof := prefixes[example.route].work, prefixes[example.route].proof
			counterMutations[example.mutation].mutate(&candidateWork, &candidateProof)
			frameCase(t, candidateWork, candidateProof, false)
		})
	}
	for _, mutation := range prefixShapeMutations {
		t.Run("FrameError invalid incomplete prefix shape "+mutation.name, func(t *testing.T) {
			candidateWork, candidateProof := prefixes["typed_hnsw"].work, prefixes["typed_hnsw"].proof
			mutation.mutate(&candidateWork, &candidateProof)
			frameCase(t, candidateWork, candidateProof, false)
		})
	}
	for _, mutation := range zeroWidthPrefixMutations {
		t.Run("FrameError invalid incomplete zero-width "+mutation.name, func(t *testing.T) {
			candidateWork, candidateProof := zeroWidthPrefixWork, zeroWidthPrefixProof
			mutation.mutate(&candidateWork, &candidateProof)
			frameCase(t, candidateWork, candidateProof, false)
		})
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
			Available: true, Completed: true, Route: "typed_hnsw", BaseANNScored: 1,
			ExactBaseScored: 1, BaseResultIDs: 1,
			Snapshot: proof.Snapshot,
		},
		Output: documentservice.DenseSearchOutputWork{
			Attempted: true, Completed: true, Requested: 1, Fetched: 1,
			RetainedPayloadFetches: 1, JSONReconstructionRows: 1, TypedColumnRows: 1,
		},
	}
	var err error
	responseFor := func(meta, id, document []byte, responseWork documentservice.DenseSearchWork, responseProof collections.ColumnGraphScorePlaneWork) ([]byte, documentservice.DenseSearchWork) {
		t.Helper()
		candidateWork := responseWork
		candidateWork.Output.OutputBytes = uint64(len(document))
		workRaw, workErr := appendDenseWork(nil, candidateWork)
		if workErr != nil {
			t.Fatal(workErr)
		}
		candidateProofRaw, proofErr := appendDenseScorePlane(nil, responseProof, iwire.DefaultLimits())
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		var body []byte
		for _, section := range []iwire.Section{
			{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, id)},
			{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, document)},
			{ID: iwire.SectionDenseSearchResponse, Bytes: meta},
			{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
			{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: candidateProofRaw},
		} {
			body, err = iwire.AppendSection(body, section)
			if err != nil {
				t.Fatal(err)
			}
		}
		return body, candidateWork
	}
	validScore := binary.LittleEndian.AppendUint64(nil, math.Float64bits(1))
	validMeta := append([]byte{3, 1, 0, 0, 1}, validScore...)
	baseRequest := DenseVectorSearchRequest{
		TypedColumnGraph: true, Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8,
		QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: proof.QuantizedIndexName,
	}
	roundTripWithEvidence := func(meta, id, document []byte, request DenseVectorSearchRequest, responseWork documentservice.DenseSearchWork, responseProof collections.ColumnGraphScorePlaneWork) (DenseVectorSearchResponse, error, documentservice.DenseSearchWork) {
		t.Helper()
		clientConn, serverConn := net.Pipe()
		client := NewClient(clientConn)
		client.denseTypedQuantizedNegotiated = true
		response, expectedWork := responseFor(meta, id, document, responseWork, responseProof)
		errCh := make(chan error, 1)
		go func() {
			header, _, serveErr := readFrame(serverConn, iwire.DefaultLimits())
			if serveErr == nil {
				serveErr = writeFrame(serverConn, iwire.Header{Type: iwire.FrameResponse, RequestID: header.RequestID}, response)
			}
			errCh <- serveErr
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		out, gotErr := client.DenseVectorSearch(ctx, request)
		cancel()
		_ = client.Close()
		_ = serverConn.Close()
		if serveErr := <-errCh; serveErr != nil {
			t.Fatal(serveErr)
		}
		return out, gotErr, expectedWork
	}
	roundTrip := func(meta, id, document []byte, request DenseVectorSearchRequest) (DenseVectorSearchResponse, error, documentservice.DenseSearchWork) {
		return roundTripWithEvidence(meta, id, document, request, work, proof)
	}
	assertOwnedProofs := func(gotErr error, expectedWork documentservice.DenseSearchWork) {
		t.Helper()
		var decodeErr *DenseVectorSearchDecodeError
		if !errors.As(gotErr, &decodeErr) || decodeErr.DenseWork == nil || decodeErr.ScorePlane == nil {
			t.Fatalf("decode error lost proofs: %v", gotErr)
		}
		if *decodeErr.DenseWork != expectedWork || *decodeErr.ScorePlane != proof {
			t.Fatalf("decode error proofs changed: work=%+v score_plane=%+v", decodeErr.DenseWork, decodeErr.ScorePlane)
		}
	}
	for name, candidate := range map[string]struct{ meta, id []byte }{
		"candidate count":    {append([]byte{3, 0, 0, 0, 1}, validScore...), []byte("a")},
		"nonfinite score":    {append([]byte{3, 1, 0, 0, 1}, binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.NaN()))...), []byte("a")},
		"out-of-range score": {append([]byte{3, 1, 0, 0, 1}, binary.LittleEndian.AppendUint64(nil, math.Float64bits(100))...), []byte("a")},
		"empty ID":           {append([]byte{3, 1, 0, 0, 1}, validScore...), nil},
		"leading-space ID":   {append([]byte{3, 1, 0, 0, 1}, validScore...), []byte(" a")},
		"trailing-space ID":  {append([]byte{3, 1, 0, 0, 1}, validScore...), []byte("a ")},
	} {
		t.Run(name, func(t *testing.T) {
			_, gotErr, expectedWork := roundTrip(candidate.meta, candidate.id, []byte(`{"id":"a"}`), baseRequest)
			assertOwnedProofs(gotErr, expectedWork)
		})
	}

	for name, candidate := range map[string]struct {
		document        []byte
		returnEmbedding bool
		valid           bool
	}{
		"omitted embedding":      {[]byte(`{"id":"a","content":"alpha"}`), false, true},
		"requested embedding":    {[]byte(`{"id":"a","embedding":[1,0]}`), true, true},
		"mismatched ID":          {[]byte(`{"id":"b"}`), false, false},
		"unrequested embedding":  {[]byte(`{"id":"a","embedding":[1,0]}`), false, false},
		"missing embedding":      {[]byte(`{"id":"a"}`), true, false},
		"wrong embedding size":   {[]byte(`{"id":"a","embedding":[1]}`), true, false},
		"overflowing embedding":  {[]byte(`{"id":"a","embedding":[3.5e38,0]}`), true, false},
		"compact embedding":      {[]byte(`{"id":"a","embedding_f32_le_b64":"AACAPwAAAAA="}`), false, false},
		"duplicate ID":           {[]byte(`{"id":"b","id":"a"}`), false, false},
		"hidden embedding":       {[]byte(`{"id":"a","embedding":[1,0],"embedding":null}`), false, false},
		"case-variant embedding": {[]byte(`{"id":"a","Embedding":[1,0]}`), false, false},
	} {
		t.Run("document "+name, func(t *testing.T) {
			request := baseRequest
			request.ReturnEmbedding = candidate.returnEmbedding
			out, gotErr, expectedWork := roundTrip(validMeta, []byte("a"), candidate.document, request)
			if candidate.valid {
				if gotErr != nil || len(out.Results) != 1 || !bytes.Equal(out.Results[0].Document, candidate.document) {
					t.Fatalf("valid document rejected: response=%+v err=%v", out, gotErr)
				}
				return
			}
			assertOwnedProofs(gotErr, expectedWork)
		})
	}

	filteredRequest := baseRequest
	filteredRequest.Filter = &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "a"}
	filteredWork := work
	filteredWork.Graph.BaseCandidates = 1
	filteredWork.Graph.Filter = collections.ColumnGraphFilterWork{
		Attempted: true, Completed: true, EligibleRows: 4097,
	}
	for name, document := range map[string][]byte{
		"matching":    []byte(`{"id":"a","meta":{"tenant":"a"}}`),
		"mismatching": []byte(`{"id":"a","meta":{"tenant":"b"}}`),
		"missing":     []byte(`{"id":"a"}`),
	} {
		t.Run("filtered document "+name, func(t *testing.T) {
			out, gotErr, expectedWork := roundTripWithEvidence(
				validMeta, []byte("a"), document, filteredRequest, filteredWork, proof,
			)
			if name == "matching" {
				if gotErr != nil || len(out.Results) != 1 || !bytes.Equal(out.Results[0].Document, document) {
					t.Fatalf("matching filtered document rejected: response=%+v err=%v", out, gotErr)
				}
				return
			}
			assertOwnedProofs(gotErr, expectedWork)
		})
	}

	mismatchedRequest := baseRequest
	mismatchedRequest.EfSearch = 9
	_, gotErr, _ := roundTrip(append([]byte{3, 0, 0, 0, 1}, validScore...), []byte("a"), []byte(`{"id":"a"}`), mismatchedRequest)
	if !strings.Contains(gotErr.Error(), "failure proof does not match the request") {
		t.Fatalf("malformed success retained request-mismatched proof: %v", gotErr)
	}
}

func TestDenseV3WireErrorMalformedProofPreservesSibling(t *testing.T) {
	snapshot := collections.ColumnGraphQuerySnapshot{
		Available: true, SchemaHash: 1, SchemaGeneration: 1,
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
			name: "noncritical dense work",
			sections: []iwire.Section{
				{ID: iwire.SectionError, Bytes: validError},
				{ID: iwire.SectionDenseSearchWork, Bytes: workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: proofRaw},
			},
			wantCode: iwire.ErrMalformedFrame,
		},
		{
			name: "noncritical score plane",
			sections: []iwire.Section{
				{ID: iwire.SectionError, Bytes: validError},
				{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: workRaw},
				{ID: iwire.SectionDenseSearchScorePlaneProof, Bytes: proofRaw},
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
	if _, err := server.handleVersionedDenseVectorSearch(ctx, &connState{}, iwire.DenseVectorSearchTypedQuantizedVersion, sections, nil); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("noncritical quantized options accepted: %v", err)
	}
	sections[len(sections)-1].Flags = iwire.SectionFlagCritical
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
