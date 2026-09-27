package nativewire

import (
	"context"
	"encoding/binary"
	"math"
	"time"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const (
	denseNormalizedOptionsVersion     = uint64(1)
	denseNormalizedDiagnosticsVersion = uint64(1)
	denseNormalizedResponseVersion    = uint64(1)
	denseNormalizedRouteVersion       = uint64(1)
	denseNormalizedRepresentationTag  = uint64(1)
)

func denseNormalizedModeTag(mode collections.VectorIndexQueryMode) uint64 {
	switch mode {
	case collections.VectorIndexQueryModeExact:
		return 1
	case collections.VectorIndexQueryModeQuantizedRerank:
		return 2
	default:
		return 0
	}
}

func denseNormalizedMode(tag uint64) (collections.VectorIndexQueryMode, bool) {
	switch tag {
	case 1:
		return collections.VectorIndexQueryModeExact, true
	case 2:
		return collections.VectorIndexQueryModeQuantizedRerank, true
	default:
		return "", false
	}
}

func appendDenseNormalizedOptions(dst []byte, request DenseVectorSearchRequest, limits iwire.Limits) ([]byte, error) {
	limits = denseDefaultLimits(limits)
	mode := denseNormalizedModeTag(request.QueryMode)
	if request.VectorRepresentation != collections.VectorIndexRepresentationCosineNormalizedF32V1 || mode == 0 {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "unsupported normalized dense representation or mode")
	}
	if !utf8.ValidString(request.QuantizedIndexName) || uint64(len(request.QuantizedIndexName)) > limits.MaxDeterministicNameBytes {
		return nil, protocolError(iwire.ErrResourceExhausted, "normalized dense quantized index name exceeds limit")
	}
	if request.QueryMode == collections.VectorIndexQueryModeExact {
		if request.QuantizedIndexName != "" || request.QuantizedRerankCandidates != 0 {
			return nil, protocolError(iwire.ErrInvalidCommand, "normalized exact options carry quantized state")
		}
	} else if request.QuantizedIndexName == "" || request.QuantizedRerankCandidates < request.TopK || request.QuantizedRerankCandidates <= 0 || request.QuantizedRerankCandidates > limits.MaxByteVectorItems {
		return nil, protocolError(iwire.ErrInvalidCommand, "normalized quantized options require a bounded rerank width at least top_k")
	}
	dst = binary.AppendUvarint(dst, denseNormalizedOptionsVersion)
	dst = binary.AppendUvarint(dst, denseNormalizedRepresentationTag)
	dst = binary.AppendUvarint(dst, mode)
	dst = binary.AppendUvarint(dst, uint64(len(request.QuantizedIndexName)))
	dst = append(dst, request.QuantizedIndexName...)
	dst = binary.AppendUvarint(dst, uint64(request.QuantizedRerankCandidates))
	if uint64(len(dst)) > limits.MaxSectionLen {
		return nil, protocolError(iwire.ErrResourceExhausted, "normalized dense options exceed section limit")
	}
	return dst, nil
}

func decodeDenseNormalizedOptions(raw []byte, limits iwire.Limits) (DenseVectorSearchRequest, error) {
	limits = denseDefaultLimits(limits)
	var request DenseVectorSearchRequest
	off := 0
	version, err := readUvarintField(raw, &off, "normalized dense options version")
	if err != nil || version != denseNormalizedOptionsVersion {
		return request, denseDecodeError(err, "unsupported normalized dense options version")
	}
	representation, err := readUvarintField(raw, &off, "normalized dense representation")
	if err != nil || representation != denseNormalizedRepresentationTag {
		return request, denseDecodeError(err, "unsupported normalized dense representation")
	}
	modeTag, err := readUvarintField(raw, &off, "normalized dense query mode")
	if err != nil {
		return request, err
	}
	mode, ok := denseNormalizedMode(modeTag)
	if !ok {
		return request, protocolError(iwire.ErrUnsupportedFeature, "unsupported normalized dense query mode")
	}
	name, err := readDenseString(raw, &off, limits.MaxDeterministicNameBytes, "normalized dense quantized index name")
	if err != nil || !utf8.ValidString(name) {
		return request, denseDecodeError(err, "normalized dense quantized index name is invalid")
	}
	rerank, err := readDenseInt(raw, &off, "normalized dense rerank candidates")
	if err != nil || rerank > limits.MaxByteVectorItems || off != len(raw) {
		return request, denseDecodeError(err, "normalized dense options are invalid")
	}
	if mode == collections.VectorIndexQueryModeExact {
		if name != "" || rerank != 0 {
			return request, protocolError(iwire.ErrInvalidCommand, "normalized exact options carry quantized state")
		}
	} else if name == "" || rerank <= 0 {
		return request, protocolError(iwire.ErrInvalidCommand, "normalized quantized options are incomplete")
	}
	request.VectorRepresentation = collections.VectorIndexRepresentationCosineNormalizedF32V1
	request.QueryMode = mode
	request.QuantizedIndexName = name
	request.QuantizedRerankCandidates = rerank
	return request, nil
}

func denseNormalizedDiagnosticsRequested(sections []iwire.Section) (bool, error) {
	raw, found, err := singletonSection(sections, iwire.SectionDenseSearchDiagnostics)
	if err != nil || !found {
		return false, err
	}
	for _, section := range sections {
		if section.ID == iwire.SectionDenseSearchDiagnostics && section.Flags != iwire.SectionFlagCritical {
			return false, protocolError(iwire.ErrInvalidCommand, "normalized dense diagnostics section must be critical")
		}
	}
	off := 0
	version, err := readUvarintField(raw, &off, "normalized dense diagnostics version")
	if err != nil || version != denseNormalizedDiagnosticsVersion || off != len(raw) {
		return false, denseDecodeError(err, "normalized dense diagnostics section is invalid")
	}
	return true, nil
}

func appendDenseNormalizedResponse(dst []byte, response documentservice.RawDenseVectorSearchResponse) []byte {
	dst = binary.AppendUvarint(dst, denseNormalizedResponseVersion)
	dst = binary.AppendUvarint(dst, uint64(len(response.Results)))
	for i := range response.Results {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(response.Results[i].Score))
	}
	return dst
}

func denseNormalizedRouteTag(route string) uint64 {
	switch route {
	case "typed_empty":
		return 1
	case "typed_exact":
		return 2
	case "typed_hnsw":
		return 3
	default:
		return 0
	}
}

func denseNormalizedRoute(tag uint64) (string, bool) {
	switch tag {
	case 1:
		return "typed_empty", true
	case 2:
		return "typed_exact", true
	case 3:
		return "typed_hnsw", true
	default:
		return "", false
	}
}

func appendDenseNormalizedRouteIdentity(dst []byte, identity documentservice.DenseSearchRouteIdentity, limits iwire.Limits) ([]byte, error) {
	limits = denseDefaultLimits(limits)
	mode := denseNormalizedModeTag(identity.QueryMode)
	route := denseNormalizedRouteTag(identity.ExecutionRoute)
	if identity.Version != uint16(denseNormalizedRouteVersion) || identity.Representation != collections.VectorIndexRepresentationCosineNormalizedF32V1 || mode == 0 || route == 0 {
		return nil, protocolError(iwire.ErrConsistencyUnavailable, "normalized dense route identity is invalid")
	}
	codec := uint64(0)
	if identity.QuantizedCodec != "" {
		if identity.QuantizedCodec != collections.QuantizedVectorCodecScalarU8 {
			return nil, protocolError(iwire.ErrUnsupportedFeature, "normalized dense route codec is unsupported")
		}
		codec = 1
	}
	flags := uint64(0)
	if identity.ReturnEmbedding {
		flags |= 1
	}
	if identity.Diagnostics {
		flags |= 2
	}
	if identity.Filter {
		flags |= 4
	}
	for _, value := range []uint64{
		denseNormalizedRouteVersion, denseNormalizedRepresentationTag, mode, route, codec, uint64(identity.QuantizedVersion), flags,
		identity.SchemaHash, identity.SchemaGeneration, identity.BaseManifestGeneration, identity.BaseManifestChecksum,
		identity.CurrentManifestGeneration, identity.CurrentManifestChecksum, identity.CurrentCoverageLSN,
		identity.TopK, identity.EfSearch, identity.RerankCandidates, identity.ResultCount,
		identity.QuantizedScoreCalls, identity.QuantizedCodeBytesRead, identity.FP32ScoreCalls, identity.FP32VectorBytesRead,
		identity.PackedScoreCalls, identity.PackedScoreCandidates, identity.PackedVectorBytesRead,
		identity.EmbeddingVectorReads, identity.EmbeddingVectorBytes, identity.EmbeddingOutputBytes,
	} {
		dst = binary.AppendUvarint(dst, value)
	}
	if !utf8.ValidString(identity.QuantizedIndexName) || uint64(len(identity.QuantizedIndexName)) > limits.MaxDeterministicNameBytes {
		return nil, protocolError(iwire.ErrResourceExhausted, "normalized dense route index name exceeds limit")
	}
	dst = binary.AppendUvarint(dst, uint64(len(identity.QuantizedIndexName)))
	dst = append(dst, identity.QuantizedIndexName...)
	if uint64(len(dst)) > limits.MaxSectionLen {
		return nil, protocolError(iwire.ErrResourceExhausted, "normalized dense route identity exceeds section limit")
	}
	return dst, nil
}

func decodeDenseNormalizedRouteIdentity(raw []byte, limits iwire.Limits) (documentservice.DenseSearchRouteIdentity, error) {
	limits = denseDefaultLimits(limits)
	var identity documentservice.DenseSearchRouteIdentity
	var values [28]uint64
	off := 0
	for i := range values {
		value, err := readUvarintField(raw, &off, "normalized dense route identity")
		if err != nil {
			return identity, err
		}
		values[i] = value
	}
	if values[0] != denseNormalizedRouteVersion || values[1] != denseNormalizedRepresentationTag || values[4] > 1 || values[5] > math.MaxUint16 || values[6] > 7 {
		return identity, protocolError(iwire.ErrMalformedFrame, "normalized dense route identity fields are invalid")
	}
	mode, ok := denseNormalizedMode(values[2])
	if !ok {
		return identity, protocolError(iwire.ErrMalformedFrame, "normalized dense route mode is invalid")
	}
	route, ok := denseNormalizedRoute(values[3])
	if !ok {
		return identity, protocolError(iwire.ErrMalformedFrame, "normalized dense execution route is invalid")
	}
	name, err := readDenseString(raw, &off, limits.MaxDeterministicNameBytes, "normalized dense route index name")
	if err != nil || !utf8.ValidString(name) || off != len(raw) {
		return identity, denseDecodeError(err, "normalized dense route index name is invalid")
	}
	identity = documentservice.DenseSearchRouteIdentity{
		Version: uint16(values[0]), Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		QueryMode: mode, ExecutionRoute: route, QuantizedVersion: uint16(values[5]), QuantizedIndexName: name,
		ReturnEmbedding: values[6]&1 != 0, Diagnostics: values[6]&2 != 0, Filter: values[6]&4 != 0,
		SchemaHash: values[7], SchemaGeneration: values[8], BaseManifestGeneration: values[9], BaseManifestChecksum: values[10],
		CurrentManifestGeneration: values[11], CurrentManifestChecksum: values[12], CurrentCoverageLSN: values[13],
		TopK: values[14], EfSearch: values[15], RerankCandidates: values[16], ResultCount: values[17],
		QuantizedScoreCalls: values[18], QuantizedCodeBytesRead: values[19], FP32ScoreCalls: values[20], FP32VectorBytesRead: values[21],
		PackedScoreCalls: values[22], PackedScoreCandidates: values[23], PackedVectorBytesRead: values[24],
		EmbeddingVectorReads: values[25], EmbeddingVectorBytes: values[26], EmbeddingOutputBytes: values[27],
	}
	if values[4] == 1 {
		identity.QuantizedCodec = collections.QuantizedVectorCodecScalarU8
	}
	return identity, nil
}

func denseNormalizedProduct(values ...uint64) (uint64, bool) {
	product := uint64(1)
	for _, value := range values {
		if value != 0 && product > math.MaxUint64/value {
			return 0, false
		}
		product *= value
	}
	return product, true
}

func validateDenseNormalizedRouteIdentity(identity documentservice.DenseSearchRouteIdentity, request DenseVectorSearchRequest, resultCount int) error {
	if identity.Version != uint16(denseNormalizedRouteVersion) || identity.Representation != request.VectorRepresentation ||
		identity.QueryMode != request.QueryMode || identity.ReturnEmbedding != request.ReturnEmbedding || identity.Diagnostics != request.Diagnostics ||
		identity.Filter != (request.Filter != nil) || identity.TopK != uint64(request.TopK) || identity.EfSearch != uint64(request.EfSearch) ||
		identity.RerankCandidates != uint64(request.QuantizedRerankCandidates) || identity.ResultCount != uint64(resultCount) ||
		identity.SchemaHash == 0 || identity.SchemaGeneration == 0 || identity.SchemaGeneration > request.ExpectedGeneration ||
		identity.BaseManifestGeneration == 0 || identity.BaseManifestChecksum == 0 || identity.CurrentManifestGeneration < identity.BaseManifestGeneration || identity.CurrentManifestChecksum == 0 ||
		identity.CurrentCoverageLSN == 0 ||
		(identity.CurrentManifestGeneration == identity.BaseManifestGeneration && identity.CurrentManifestChecksum != identity.BaseManifestChecksum) {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense route identity does not match the request")
	}
	if request.QueryMode == collections.VectorIndexQueryModeExact {
		if identity.QuantizedCodec != "" || identity.QuantizedIndexName != "" || identity.QuantizedVersion != 0 || identity.QuantizedScoreCalls != 0 || identity.QuantizedCodeBytesRead != 0 {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized exact route carries quantized work")
		}
	} else if identity.QuantizedCodec != collections.QuantizedVectorCodecScalarU8 || identity.QuantizedIndexName != request.QuantizedIndexName || identity.QuantizedVersion != 1 {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized route identity does not match the request")
	}
	if identity.ExecutionRoute == "typed_empty" && resultCount != 0 {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized empty route returned results")
	}
	dimension := uint64(len(request.Query))
	wantFP32, ok := denseNormalizedProduct(identity.FP32ScoreCalls, dimension, 4)
	if !ok || identity.FP32VectorBytesRead != wantFP32 ||
		(resultCount != 0 && identity.FP32ScoreCalls == 0) || identity.PackedScoreCandidates > identity.FP32ScoreCalls {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense FP32 byte accounting is invalid")
	}
	wantCodes, ok := denseNormalizedProduct(identity.QuantizedScoreCalls, dimension)
	if !ok || identity.QuantizedCodeBytesRead != wantCodes {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense quantized byte accounting is invalid")
	}
	wantPacked, ok := denseNormalizedProduct(identity.PackedScoreCandidates, dimension, 4)
	if !ok || identity.PackedVectorBytesRead != wantPacked || (identity.PackedScoreCandidates == 0) != (identity.PackedScoreCalls == 0) || identity.PackedScoreCalls > identity.PackedScoreCandidates {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense packed rerank accounting is invalid")
	}
	if !request.ReturnEmbedding {
		if identity.EmbeddingVectorReads != 0 || identity.EmbeddingVectorBytes != 0 || identity.EmbeddingOutputBytes != 0 {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense embedding projection performed forbidden work")
		}
	} else {
		wantEmbedding, ok := denseNormalizedProduct(uint64(resultCount), dimension, 4)
		if !ok || identity.EmbeddingVectorReads != uint64(resultCount) || identity.EmbeddingVectorBytes != wantEmbedding || (resultCount != 0 && identity.EmbeddingOutputBytes == 0) {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense embedding projection accounting is invalid")
		}
	}
	if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
		if identity.PackedScoreCalls != min(identity.PackedScoreCandidates, 1) ||
			(identity.CurrentManifestGeneration == identity.BaseManifestGeneration && identity.FP32ScoreCalls != identity.PackedScoreCandidates) {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized base/suffix score accounting is invalid")
		}
		if identity.ExecutionRoute == "typed_hnsw" && identity.QuantizedScoreCalls == 0 {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized HNSW route omitted candidate work")
		}
	}
	if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank && identity.ExecutionRoute != "typed_hnsw" &&
		(identity.QuantizedScoreCalls != 0 || identity.QuantizedCodeBytesRead != 0) {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized exact shortcut carried candidate-code work")
	}
	if identity.ExecutionRoute == "typed_empty" &&
		(identity.QuantizedScoreCalls != 0 || identity.QuantizedCodeBytesRead != 0 || identity.FP32ScoreCalls != 0 || identity.FP32VectorBytesRead != 0 ||
			identity.PackedScoreCalls != 0 || identity.PackedScoreCandidates != 0 || identity.PackedVectorBytesRead != 0) {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized empty route carried score work")
	}
	return nil
}

func validateDenseNormalizedDiagnosticIdentity(identity documentservice.DenseSearchRouteIdentity, work documentservice.DenseSearchWork, proof *collections.ColumnGraphScorePlaneWork, request DenseVectorSearchRequest, resultCount int) error {
	graph, snapshot, output := work.Graph, work.Graph.Snapshot, work.Output
	if !work.Completed || !graph.Completed || !snapshot.Available || !output.Completed ||
		identity.ExecutionRoute != graph.Route || identity.Filter != graph.Filter.Attempted ||
		identity.SchemaHash != snapshot.SchemaHash || identity.SchemaGeneration != snapshot.SchemaGeneration ||
		identity.BaseManifestGeneration != snapshot.BaseManifest.Generation || identity.BaseManifestChecksum != snapshot.BaseManifest.Checksum ||
		identity.CurrentManifestGeneration != snapshot.CurrentManifest.Generation || identity.CurrentManifestChecksum != snapshot.CurrentManifest.Checksum ||
		identity.CurrentCoverageLSN != snapshot.CurrentCoverageLSN ||
		identity.ResultCount != uint64(resultCount) || output.Requested != uint64(resultCount) || output.Fetched != uint64(resultCount) || output.Missing != 0 {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized dense route identity and diagnostic work proof disagree")
	}
	if request.QueryMode == collections.VectorIndexQueryModeExact {
		if proof != nil || graph.BaseANNScored > math.MaxUint64-graph.DeltaScored || identity.FP32ScoreCalls != graph.BaseANNScored+graph.DeltaScored || identity.PackedScoreCandidates > graph.BaseANNScored {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized exact route identity and diagnostic counters disagree")
		}
		return nil
	}
	if proof == nil || !proof.Available || !proof.Completed || !proof.Snapshot.Available ||
		!denseScorePlaneRequestMatches(proof, request, true) || !denseScorePlaneRerankCountersMatch(proof) ||
		!denseScorePlaneRouteMatchesGraph(graph.Route, proof.Route) || proof.Snapshot != snapshot ||
		graph.Filter.Attempted != (request.Filter != nil) || (graph.Filter.Attempted && !graph.Filter.Completed) ||
		(request.Filter != nil && uint64(resultCount) != minUint64(uint64(request.TopK), graph.Filter.EligibleRows)) ||
		!denseExactResultCountMatchesTopK(proof, request.TopK, resultCount) ||
		proof.ExactBaseRerankScoreCalls > math.MaxUint64-proof.ExactSmallFilterScoreCalls {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized diagnostic counters are unavailable")
	}
	baseExact := proof.ExactBaseRerankScoreCalls + proof.ExactSmallFilterScoreCalls
	if baseExact > math.MaxUint64-proof.ExactSuffixScoreCalls ||
		identity.QuantizedScoreCalls != proof.QuantizedScoreCalls ||
		identity.FP32ScoreCalls != baseExact+proof.ExactSuffixScoreCalls ||
		identity.PackedScoreCalls != proof.PackedScoreBatchCalls || identity.PackedScoreCandidates != proof.PackedScoreCandidates ||
		identity.PackedVectorBytesRead != proof.PackedVectorBytesRead ||
		graph.ExactBaseScored != baseExact || graph.DeltaScored != proof.ExactSuffixScoreCalls {
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized route identity and diagnostic counters disagree")
	}
	switch proof.Route {
	case "typed_empty":
		if graph.BaseANNScored != 0 || baseExact != 0 || proof.ExactSuffixScoreCalls != 0 || resultCount != 0 {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized empty diagnostics carried score work")
		}
	case "typed_exact":
		// Dense-work v1's BaseANNScored field is sourced from PreparedScoreCalls.
		// On the canonical exact shortcut that is the packed FP32 base batch,
		// while score-plane v2 separately proves zero scalar-u8 candidate work.
		if proof.QuantizedScoreCalls != 0 || graph.BaseANNScored != baseExact {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized exact diagnostics disagree on packed base work")
		}
	case "quantized_rerank":
		if graph.BaseANNScored != proof.QuantizedScoreCalls || proof.QuantizedScoreCalls == 0 {
			return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized traversal diagnostics disagree on candidate work")
		}
	default:
		return protocolError(iwire.ErrConsistencyUnavailable, "normalized quantized diagnostics used an unsupported route")
	}
	return nil
}

func decodeDenseNormalizedMetaInto(raw []byte, topK int, results []DenseVectorSearchResult) ([]DenseVectorSearchResult, error) {
	off := 0
	version, err := readUvarintField(raw, &off, "normalized dense response version")
	if err != nil || version != denseNormalizedResponseVersion {
		return nil, denseDecodeError(err, "unsupported normalized dense response version")
	}
	count, err := readUvarintField(raw, &off, "normalized dense result count")
	if err != nil || count > uint64(topK) || count > uint64((len(raw)-off)/8) || len(raw)-off != int(count)*8 {
		return nil, denseDecodeError(err, "normalized dense response lengths do not match")
	}
	if int(count) <= cap(results) {
		clear(results)
		results = results[:int(count)]
	} else {
		results = make([]DenseVectorSearchResult, int(count))
	}
	for i := range results {
		score := math.Float64frombits(binary.LittleEndian.Uint64(raw[off+i*8:]))
		if math.IsNaN(score) || math.IsInf(score, 0) || score < -1-denseCosineScoreTolerance || score > 1+denseCosineScoreTolerance {
			return nil, protocolError(iwire.ErrConsistencyUnavailable, "normalized dense response score is invalid")
		}
		results[i].Score = score
	}
	return results, nil
}

func denseNormalizedDocumentsMatchRequest(results []DenseVectorSearchResult, request DenseVectorSearchRequest) bool {
	if err := request.Filter.Validate(); err != nil {
		return false
	}
	for _, result := range results {
		document, ok := decodeDenseV3ResultDocument(result.Document)
		if !ok || document.ID != string(result.ID) {
			return false
		}
		if !request.ReturnEmbedding {
			if document.Embedding != nil {
				return false
			}
		} else {
			if len(document.Embedding) != len(request.Query) {
				return false
			}
			for _, value := range document.Embedding {
				if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
					return false
				}
			}
		}
		matches, err := request.Filter.MatchesDocument(document)
		if err != nil || !matches {
			return false
		}
	}
	return true
}

func decodeDenseNormalizedResponse(sections []iwire.Section, request DenseVectorSearchRequest, limits iwire.Limits, ids, docs [][]byte, results []DenseVectorSearchResult) (DenseVectorSearchResponse, [][]byte, [][]byte, []DenseVectorSearchResult, error) {
	var response DenseVectorSearchResponse
	wantSections := 4
	if request.Diagnostics {
		wantSections++
		if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
			wantSections++
		}
	}
	if len(sections) != wantSections {
		return response, ids, docs, results, protocolError(iwire.ErrMalformedFrame, "normalized dense response section inventory is invalid")
	}
	for _, section := range sections {
		allowed := section.ID == iwire.SectionDocumentIDs || section.ID == iwire.SectionDocuments || section.ID == iwire.SectionDenseSearchResponse || section.ID == iwire.SectionDenseSearchRouteIdentity ||
			(request.Diagnostics && section.ID == iwire.SectionDenseSearchWork) ||
			(request.Diagnostics && request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank && section.ID == iwire.SectionDenseSearchScorePlaneProof)
		if !allowed {
			return response, ids, docs, results, protocolError(iwire.ErrUnsupportedFeature, "unexpected normalized dense response section")
		}
		wantFlags := uint64(0)
		if section.ID == iwire.SectionDenseSearchRouteIdentity || section.ID == iwire.SectionDenseSearchWork || section.ID == iwire.SectionDenseSearchScorePlaneProof {
			wantFlags = iwire.SectionFlagCritical
		}
		if section.Flags != wantFlags {
			return response, ids, docs, results, protocolError(iwire.ErrMalformedFrame, "normalized dense response section flags are invalid")
		}
	}
	meta, ok, err := singletonSection(sections, iwire.SectionDenseSearchResponse)
	if err != nil || !ok {
		return response, ids, docs, results, denseDecodeError(err, "normalized dense response metadata is missing")
	}
	results, err = decodeDenseNormalizedMetaInto(meta, request.TopK, results)
	if err != nil {
		return response, ids, docs, results, err
	}
	idsRaw, ok, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil || !ok {
		return response, ids, docs, results, denseDecodeError(err, "normalized dense result IDs are missing")
	}
	docsRaw, ok, err := singletonSection(sections, iwire.SectionDocuments)
	if err != nil || !ok {
		return response, ids, docs, results, denseDecodeError(err, "normalized dense result documents are missing")
	}
	ids, err = iwire.DecodeByteVectorItemsInto(ids[:0], idsRaw, limits)
	if err != nil {
		return response, ids, docs, results, err
	}
	docs, err = iwire.DecodeByteVectorItemsInto(docs[:0], docsRaw, limits)
	if err != nil || len(ids) != len(results) || len(docs) != len(results) {
		return response, ids, docs, results, denseDecodeError(err, "normalized dense result lengths do not match")
	}
	for i := range results {
		results[i].ID, results[i].Document = ids[i], docs[i]
	}
	routeRaw, ok, err := singletonSection(sections, iwire.SectionDenseSearchRouteIdentity)
	if err != nil || !ok {
		return response, ids, docs, results, denseDecodeError(err, "normalized dense route identity is missing")
	}
	identity, err := decodeDenseNormalizedRouteIdentity(routeRaw, limits)
	if err != nil || validateDenseNormalizedRouteIdentity(identity, request, len(results)) != nil {
		if err == nil {
			err = validateDenseNormalizedRouteIdentity(identity, request, len(results))
		}
		return response, ids, docs, results, err
	}
	response = DenseVectorSearchResponse{TypedColumnGraph: true, Results: results, Route: documentservice.RouteAnn, Candidates: len(results), RouteIdentity: &identity}
	if !denseV3ResultsHaveValidIDs(results) || !denseV3ResultsHaveCosineScores(results) || !denseV3ResultsOrdered(results) || !denseNormalizedDocumentsMatchRequest(results, request) {
		return response, ids, docs, results, protocolError(iwire.ErrConsistencyUnavailable, "normalized dense results do not match the request")
	}
	if request.Diagnostics {
		workRaw, found, workErr := singletonSection(sections, iwire.SectionDenseSearchWork)
		if workErr != nil || !found {
			return response, ids, docs, results, denseDecodeError(workErr, "normalized dense work proof is missing")
		}
		response.DenseWork, err = decodeDenseWork(workRaw)
		if err != nil || !response.DenseWork.Completed || validateDenseWorkResults(response.DenseWork, results) != nil {
			if err == nil {
				err = protocolError(iwire.ErrConsistencyUnavailable, "normalized dense work proof is invalid")
			}
			return response, ids, docs, results, err
		}
		if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
			response.ScorePlane, err = decodeDenseScorePlaneV2Section(sections, true, limits)
			if err != nil {
				return response, ids, docs, results, err
			}
		}
		if err = validateDenseNormalizedDiagnosticIdentity(identity, response.DenseWork, response.ScorePlane, request, len(results)); err != nil {
			return response, ids, docs, results, err
		}
	}
	return response, ids, docs, results, nil
}

func validateDenseNormalizedFailure(err error, request DenseVectorSearchRequest) error {
	work, scorePlane := denseFailureProofs(err)
	if !request.Diagnostics {
		if work == nil && scorePlane == nil {
			return err
		}
		return &DenseVectorSearchDecodeError{Err: protocolError(iwire.ErrMalformedFrame, "normalized production error carried diagnostics"), DenseWork: work, ScorePlane: scorePlane}
	}
	if request.QueryMode == collections.VectorIndexQueryModeExact && scorePlane != nil {
		return &DenseVectorSearchDecodeError{Err: protocolError(iwire.ErrMalformedFrame, "normalized exact error carried score-plane diagnostics"), DenseWork: work, ScorePlane: scorePlane}
	}
	if request.QueryMode == collections.VectorIndexQueryModeExact {
		return validateDenseNormalizedExactFailureProof(err, request)
	}
	if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
		return validateDenseQuantizedFailureProof(err, request)
	}
	return err
}

func validateDenseNormalizedExactFailureProof(err error, _ DenseVectorSearchRequest) error {
	work, scorePlane := denseFailureProofs(err)
	if work == nil && scorePlane == nil {
		return err
	}
	// Dense-work v1 intentionally has no index/query/filter-value digest. Keep
	// structurally decoded evidence for debugging, but never classify it as an
	// authenticated exact-request error. Quantized diagnostics have the sibling
	// score-plane request fields and use validateDenseQuantizedFailureProof.
	return &DenseVectorSearchDecodeError{
		Err:       protocolError(iwire.ErrConsistencyUnavailable, "normalized exact failure proof cannot be bound to the exact request"),
		DenseWork: work, ScorePlane: scorePlane,
	}
}

func (s *Server) handleDenseNormalizedVectorSearch(ctx context.Context, state *connState, sections []iwire.Section, dst []byte) (_ []byte, err error) {
	defer clearDenseVectorSearchScratch(state)
	diagnostics, err := denseNormalizedDiagnosticsRequested(sections)
	if err != nil {
		return nil, err
	}
	var proof *documentservice.DenseSearchWork
	var scorePlane *collections.ColumnGraphScorePlaneWork
	defer func() {
		if err == nil || !diagnostics {
			return
		}
		if proof == nil {
			proof = denseServiceWork(err)
		}
		if scorePlane == nil {
			scorePlane = denseServiceScorePlane(err)
		}
		if proof != nil {
			err = &denseWorkError{error: err, work: *proof, scorePlane: scorePlane, version: iwire.DenseVectorSearchNormalizedVersion}
		}
	}()
	deadline, err := deadlineUnixNanosFromSections(sections)
	if err != nil || deadline <= 0 {
		return nil, denseDecodeError(err, "positive normalized dense deadline is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, deadline))
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.documentService == nil {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "dense document service is not configured")
	}
	raw, ok, err := singletonSection(sections, iwire.SectionDenseSearchRequest)
	if err != nil || !ok {
		return nil, denseDecodeError(err, "normalized dense search request is missing")
	}
	request, leaves, err := decodeDenseVectorSearchRequest(raw, s.limits, state.vectorQuery[:0], &state.denseFilter, state.denseFilters[:0])
	if err != nil {
		return nil, err
	}
	state.vectorQuery, state.denseFilters = request.Query[:0], leaves[:0]
	optionsRaw, ok, err := singletonSection(sections, iwire.SectionDenseSearchNormalizedOptions)
	if err != nil || !ok {
		return nil, denseDecodeError(err, "normalized dense options are missing")
	}
	for _, section := range sections {
		if section.ID == iwire.SectionDenseSearchNormalizedOptions && section.Flags != iwire.SectionFlagCritical {
			return nil, protocolError(iwire.ErrInvalidCommand, "normalized dense options section must be critical")
		}
	}
	options, err := decodeDenseNormalizedOptions(optionsRaw, s.limits)
	if err != nil {
		return nil, err
	}
	request.QueryMode, request.VectorRepresentation = options.QueryMode, options.VectorRepresentation
	request.QuantizedIndexName, request.QuantizedRerankCandidates = options.QuantizedIndexName, options.QuantizedRerankCandidates
	request.Diagnostics = diagnostics
	if request.ExpectedGeneration == 0 || request.EfSearch <= 0 || (request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank && request.QuantizedRerankCandidates < request.TopK) {
		return nil, protocolError(iwire.ErrInvalidCommand, "normalized dense request requires positive generation, E, and an admitted R")
	}
	responseSections := 4
	if diagnostics {
		responseSections++
		if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
			responseSections++
		}
	}
	if err := s.checkResponseSectionCount(responseSections); err != nil {
		return nil, err
	}
	response, err := s.documentService.SearchDenseVectorNativeRawInto(ctx, request.Index, documentservice.DenseVectorSearchRequest{
		ExpectedGeneration: request.ExpectedGeneration, QueryEmbedding: request.Query, TopK: request.TopK, EfSearch: request.EfSearch,
		QueryMode: request.QueryMode, QuantizedIndexName: request.QuantizedIndexName, QuantizedRerankCandidates: request.QuantizedRerankCandidates,
		Route: documentservice.RouteAnn, Filter: request.Filter, ReturnEmbedding: request.ReturnEmbedding, Diagnostics: diagnostics,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1, RequireVectorRepresentation: true,
	}, state.denseResults[:0])
	if err != nil {
		return nil, err
	}
	proof, scorePlane = response.DenseWork, response.ScorePlane
	state.denseResults = response.Results[:0]
	if !response.TypedColumnGraph || response.RouteIdentity == nil || response.Diagnostics != diagnostics {
		return nil, protocolError(iwire.ErrConsistencyUnavailable, "normalized dense service response route is invalid")
	}
	if err := validateDenseNormalizedRouteIdentity(*response.RouteIdentity, request, len(response.Results)); err != nil {
		return nil, err
	}
	state.idsScratch = resizeByteSlices(state.idsScratch, len(response.Results))
	state.docsScratch = resizeByteSlices(state.docsScratch, len(response.Results))
	for i := range response.Results {
		state.idsScratch[i], state.docsScratch[i] = response.Results[i].ID, response.Results[i].Document
	}
	meta := appendDenseNormalizedResponse(state.denseMeta[:0], response)
	state.denseMeta = meta
	route, err := appendDenseNormalizedRouteIdentity(nil, *response.RouteIdentity, s.limits)
	if err != nil {
		return nil, err
	}
	var work, score []byte
	if diagnostics {
		if proof == nil || !proof.Completed {
			return nil, protocolError(iwire.ErrConsistencyUnavailable, "normalized dense work proof is unavailable")
		}
		work, err = appendDenseWork(nil, *proof)
		if err != nil {
			return nil, err
		}
		if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
			if scorePlane == nil || !scorePlane.Completed {
				return nil, protocolError(iwire.ErrConsistencyUnavailable, "normalized dense score-plane proof is unavailable")
			}
			score, err = appendDenseScorePlaneV2(nil, *scorePlane, s.limits)
			if err != nil {
				return nil, err
			}
		}
	}
	idLen := iwire.ByteVectorEncodedLen(state.idsScratch)
	docLen := iwire.ByteVectorEncodedLen(state.docsScratch)
	idBytes, err := denseByteVectorPayloadLen(state.idsScratch)
	if err != nil {
		return nil, err
	}
	docBytes, err := denseByteVectorPayloadLen(state.docsScratch)
	if err != nil {
		return nil, err
	}
	if err := s.checkResponseByteVectorLen("normalized dense ids", idBytes); err != nil {
		return nil, err
	}
	if err := s.checkResponseByteVectorLen("normalized dense documents", docBytes); err != nil {
		return nil, err
	}
	for _, checked := range [...]struct {
		label  string
		length int
	}{
		{label: "normalized dense ids", length: idLen},
		{label: "normalized dense documents", length: docLen},
		{label: "normalized dense scores", length: len(meta)},
		{label: "normalized dense route", length: len(route)},
		{label: "normalized dense work", length: len(work)},
		{label: "normalized dense score-plane", length: len(score)},
	} {
		if err := s.checkResponseSectionLen(checked.label, checked.length); err != nil {
			return nil, err
		}
	}
	bodyLen := uint64(0)
	for _, section := range [...]struct {
		id     iwire.SectionID
		length int
	}{
		{id: iwire.SectionDocumentIDs, length: idLen},
		{id: iwire.SectionDocuments, length: docLen},
		{id: iwire.SectionDenseSearchResponse, length: len(meta)},
		{id: iwire.SectionDenseSearchRouteIdentity, length: len(route)},
		{id: iwire.SectionDenseSearchWork, length: len(work)},
		{id: iwire.SectionDenseSearchScorePlaneProof, length: len(score)},
	} {
		if section.length == 0 {
			continue
		}
		sectionLen, sectionErr := responseSectionBodyLen(section.id, section.length)
		if sectionErr != nil {
			return nil, sectionErr
		}
		bodyLen, err = addResponseLen(bodyLen, sectionLen)
		if err != nil {
			return nil, err
		}
	}
	if err := s.checkResponseBodyLen(bodyLen); err != nil {
		return nil, err
	}
	for _, section := range [...]struct {
		id      iwire.SectionID
		flags   uint64
		length  int
		payload []byte
	}{
		{id: iwire.SectionDocumentIDs, length: idLen},
		{id: iwire.SectionDocuments, length: docLen},
		{id: iwire.SectionDenseSearchResponse, length: len(meta), payload: meta},
		{id: iwire.SectionDenseSearchRouteIdentity, flags: iwire.SectionFlagCritical, length: len(route), payload: route},
		{id: iwire.SectionDenseSearchWork, flags: iwire.SectionFlagCritical, length: len(work), payload: work},
		{id: iwire.SectionDenseSearchScorePlaneProof, flags: iwire.SectionFlagCritical, length: len(score), payload: score},
	} {
		if section.length == 0 {
			continue
		}
		dst, err = iwire.AppendSectionHeader(dst, section.id, section.flags, section.length)
		if err != nil {
			return nil, err
		}
		switch section.id {
		case iwire.SectionDocumentIDs:
			dst = iwire.AppendByteVectorWithEncodedLen(dst, idLen, state.idsScratch...)
		case iwire.SectionDocuments:
			dst = iwire.AppendByteVectorWithEncodedLen(dst, docLen, state.docsScratch...)
		default:
			dst = append(dst, section.payload...)
		}
	}
	return dst, nil
}
