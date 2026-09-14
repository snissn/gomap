package nativewire

import (
	"encoding/binary"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const denseScorePlaneVersion = uint64(1)

func denseScorePlaneModeTag(mode collections.VectorIndexQueryMode) uint64 {
	switch mode {
	case collections.VectorIndexQueryModeExact:
		return 1
	case collections.VectorIndexQueryModeQuantizedRerank:
		return 2
	case collections.VectorIndexQueryModeQuantizedOnly:
		return 3
	default:
		return 0
	}
}

func denseScorePlaneMode(tag uint64) (collections.VectorIndexQueryMode, bool) {
	switch tag {
	case 1:
		return collections.VectorIndexQueryModeExact, true
	case 2:
		return collections.VectorIndexQueryModeQuantizedRerank, true
	case 3:
		return collections.VectorIndexQueryModeQuantizedOnly, true
	default:
		return "", false
	}
}

func denseScorePlaneRouteTag(route string) uint64 {
	switch route {
	case "":
		return 0
	case "typed_empty":
		return 1
	case "typed_exact":
		return 2
	case "quantized_rerank":
		return 3
	case "typed_hnsw":
		return 4
	default:
		return 0
	}
}

func denseScorePlaneRoute(tag uint64) (string, bool) {
	switch tag {
	case 0:
		return "", true
	case 1:
		return "typed_empty", true
	case 2:
		return "typed_exact", true
	case 3:
		return "quantized_rerank", true
	case 4:
		return "typed_hnsw", true
	default:
		return "", false
	}
}

func appendDenseScorePlaneString(dst []byte, value string, limit uint64, label string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, protocolError(iwire.ErrInvalidCommand, "dense score-plane %s must be valid UTF-8", label)
	}
	if uint64(len(value)) > limit {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense score-plane %s exceeds limit", label)
	}
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...), nil
}

func readDenseScorePlaneString(raw []byte, off *int, limit uint64, label string) (string, error) {
	value, err := readDenseString(raw, off, limit, "dense score-plane "+label)
	if err != nil {
		return "", err
	}
	if !utf8.ValidString(value) {
		return "", protocolError(iwire.ErrMalformedFrame, "dense score-plane %s must be valid UTF-8", label)
	}
	return value, nil
}

func validateDenseScorePlane(proof collections.ColumnGraphScorePlaneWork) error {
	if proof.Version != uint16(denseScorePlaneVersion) {
		return protocolError(iwire.ErrUnsupportedVersion, "unsupported dense score-plane version %d", proof.Version)
	}
	if denseScorePlaneModeTag(proof.RequestedMode) == 0 || denseScorePlaneModeTag(proof.EffectiveMode) == 0 {
		return protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane query mode")
	}
	if proof.RequestedMode == collections.VectorIndexQueryModeQuantizedRerank || proof.EffectiveMode == collections.VectorIndexQueryModeQuantizedRerank {
		if proof.QuantizedIndexName == "" || proof.QuantizedCodec != collections.QuantizedVectorCodecScalarU8 || proof.QuantizedVersion != 1 || proof.QuantizedConfigHash != 0 {
			return protocolError(iwire.ErrUnsupportedFeature, "dense score-plane proof requires the legacy scalar_u8/v1 codec")
		}
	}
	if denseScorePlaneRouteTag(proof.Route) == 0 && proof.Route != "" {
		return protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane route")
	}
	for _, manifest := range []collections.ColumnGraphManifestWork{proof.Snapshot.BaseManifest, proof.Snapshot.CurrentManifest} {
		if manifest.Format != "" && manifest.Format != "tcs1" {
			return protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane manifest format")
		}
		if proof.Snapshot.Available && !denseManifestWorkComplete(manifest) {
			return protocolError(iwire.ErrConsistencyUnavailable, "dense score-plane snapshot manifest is incomplete")
		}
	}
	if (!proof.Snapshot.Available && proof.Snapshot != (collections.ColumnGraphQuerySnapshot{})) ||
		(proof.Snapshot.Available && proof.Snapshot.CurrentCoverageLSN < proof.Snapshot.BaseCoverageLSN) {
		return protocolError(iwire.ErrConsistencyUnavailable, "dense score-plane snapshot is inconsistent")
	}
	if proof.Completed && (!proof.Available || !proof.Snapshot.Available || proof.Route == "" || proof.Reason != "") {
		return protocolError(iwire.ErrConsistencyUnavailable, "completed dense score-plane proof is incomplete")
	}
	if proof.Completed && proof.Route == "quantized_rerank" && proof.QuantizedScoreCalls == 0 {
		return protocolError(iwire.ErrConsistencyUnavailable, "completed quantized rerank proof has no quantized score calls")
	}
	if proof.Completed && !denseScorePlaneRerankCountersMatch(&proof) {
		return protocolError(iwire.ErrConsistencyUnavailable, "completed dense score-plane rerank counters are inconsistent")
	}
	return nil
}

func denseScorePlaneRerankCountersMatch(proof *collections.ColumnGraphScorePlaneWork) bool {
	if proof == nil || !proof.Completed {
		return true
	}
	if proof.RequestedEFSearch != 0 {
		effectiveWidth := proof.RequestedTopK
		if proof.RequestedEFSearch > effectiveWidth {
			effectiveWidth = proof.RequestedEFSearch
		}
		if proof.NormalizedCandidateWidth > effectiveWidth {
			return false
		}
	}
	expectedCap := proof.NormalizedCandidateWidth
	if proof.RequestedRerankCandidates != 0 && proof.RequestedRerankCandidates < expectedCap {
		expectedCap = proof.RequestedRerankCandidates
	}
	if proof.RerankCandidateCap != expectedCap ||
		proof.LiveShortlistCandidates > proof.NormalizedCandidateWidth ||
		proof.NormalizedCandidateWidth > proof.RawCandidateWidth {
		return false
	}
	switch proof.Route {
	case "quantized_rerank":
		return proof.NormalizedCandidateWidth != 0 &&
			proof.RawCandidateWidth != 0 &&
			proof.RerankCandidateCap != 0 &&
			proof.RawRetainedCandidates <= proof.QuantizedScoreCalls &&
			proof.ExactSmallFilterScoreCalls == 0 &&
			proof.ActualRerankCandidates == proof.ExactBaseRerankScoreCalls &&
			proof.ActualRerankCandidates == minUint64(proof.LiveShortlistCandidates, proof.RerankCandidateCap)
	case "typed_empty":
		return proof.QuantizedScoreCalls == 0 &&
			proof.ExactSuffixScoreCalls == 0 &&
			proof.ExactSmallFilterScoreCalls == 0 &&
			proof.RawRetainedCandidates == 0 &&
			proof.LiveShortlistCandidates == 0 &&
			proof.ActualRerankCandidates == 0 &&
			proof.ExactBaseRerankScoreCalls == 0
	case "typed_exact":
		return proof.QuantizedScoreCalls == 0 &&
			proof.RawRetainedCandidates == 0 &&
			proof.LiveShortlistCandidates == 0 &&
			proof.ActualRerankCandidates == 0 &&
			proof.ExactBaseRerankScoreCalls == 0
	default:
		return true
	}
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// appendDenseScorePlane encodes the versioned sibling proof. Dense work v1 is
// intentionally not extended; all strings and counters are owned by the
// returned value and bounded by the negotiated section limit.
func appendDenseScorePlane(dst []byte, proof collections.ColumnGraphScorePlaneWork, limits iwire.Limits) ([]byte, error) {
	limits = denseDefaultLimits(limits)
	if err := validateDenseScorePlane(proof); err != nil {
		return nil, err
	}
	flags := uint64(0)
	if proof.Available {
		flags |= 1
	}
	if proof.Completed {
		flags |= 2
	}
	if proof.Snapshot.Available {
		flags |= 4
	}
	for _, value := range []uint64{
		denseScorePlaneVersion, flags, denseScorePlaneModeTag(proof.RequestedMode), denseScorePlaneModeTag(proof.EffectiveMode), denseScorePlaneRouteTag(proof.Route),
		uint64(proof.QuantizedVersion), proof.QuantizedConfigHash,
		proof.RequestedTopK, proof.RequestedEFSearch, proof.RequestedRerankCandidates, proof.NormalizedCandidateWidth, proof.RawCandidateWidth, proof.RerankCandidateCap,
		proof.RawRetainedCandidates, proof.LiveShortlistCandidates, proof.ActualRerankCandidates, proof.QuantizedScoreCalls, proof.QuantizedCodeBytesRead,
		proof.ExactBaseRerankScoreCalls, proof.ExactSuffixScoreCalls, proof.ExactSmallFilterScoreCalls, proof.ExactBaseVectorBytesRead, proof.ExactSuffixVectorBytesRead,
		proof.Snapshot.SchemaHash, proof.Snapshot.SchemaGeneration, proof.Snapshot.BaseCoverageLSN, proof.Snapshot.CurrentCoverageLSN,
	} {
		dst = binary.AppendUvarint(dst, value)
	}
	for _, value := range []string{proof.Reason, proof.QuantizedIndexName, proof.QuantizedCodec} {
		var err error
		dst, err = appendDenseScorePlaneString(dst, value, limits.MaxSectionLen, "string")
		if err != nil {
			return nil, err
		}
	}
	for _, manifest := range []collections.ColumnGraphManifestWork{proof.Snapshot.BaseManifest, proof.Snapshot.CurrentManifest} {
		format := uint64(0)
		if manifest.Format == "tcs1" {
			format = 1
		}
		for _, value := range []uint64{manifest.Generation, format, uint64(manifest.Version), manifest.Checksum} {
			dst = binary.AppendUvarint(dst, value)
		}
	}
	if uint64(len(dst)) > limits.MaxSectionLen {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense score-plane proof exceeds section limit")
	}
	return dst, nil
}

func decodeDenseScorePlane(raw []byte, limits iwire.Limits) (collections.ColumnGraphScorePlaneWork, error) {
	limits = denseDefaultLimits(limits)
	var proof collections.ColumnGraphScorePlaneWork
	values := make([]uint64, 0, 27)
	off := 0
	for len(values) < 27 {
		value, err := readUvarintField(raw, &off, "dense score-plane")
		if err != nil {
			return proof, err
		}
		values = append(values, value)
	}
	if values[0] != denseScorePlaneVersion || values[1] > 7 || values[2] == 0 || values[3] == 0 || values[4] > 4 || values[5] > 65535 {
		return proof, protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane fields")
	}
	requested, ok := denseScorePlaneMode(values[2])
	if !ok {
		return proof, protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane requested mode")
	}
	effective, ok := denseScorePlaneMode(values[3])
	if !ok {
		return proof, protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane effective mode")
	}
	route, ok := denseScorePlaneRoute(values[4])
	if !ok {
		return proof, protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane route")
	}
	proof.Version = uint16(values[0])
	proof.Available, proof.Completed = values[1]&1 != 0, values[1]&2 != 0
	proof.RequestedMode, proof.EffectiveMode, proof.Route = requested, effective, route
	proof.QuantizedVersion, proof.QuantizedConfigHash = uint16(values[5]), values[6]
	proof.RequestedTopK, proof.RequestedEFSearch, proof.RequestedRerankCandidates = values[7], values[8], values[9]
	proof.NormalizedCandidateWidth, proof.RawCandidateWidth, proof.RerankCandidateCap = values[10], values[11], values[12]
	proof.RawRetainedCandidates, proof.LiveShortlistCandidates, proof.ActualRerankCandidates = values[13], values[14], values[15]
	proof.QuantizedScoreCalls, proof.QuantizedCodeBytesRead = values[16], values[17]
	proof.ExactBaseRerankScoreCalls, proof.ExactSuffixScoreCalls, proof.ExactSmallFilterScoreCalls = values[18], values[19], values[20]
	proof.ExactBaseVectorBytesRead, proof.ExactSuffixVectorBytesRead = values[21], values[22]
	proof.Snapshot.Available = values[1]&4 != 0
	proof.Snapshot.SchemaHash, proof.Snapshot.SchemaGeneration = values[23], values[24]
	proof.Snapshot.BaseCoverageLSN, proof.Snapshot.CurrentCoverageLSN = values[25], values[26]
	for _, target := range [](*string){&proof.Reason, &proof.QuantizedIndexName, &proof.QuantizedCodec} {
		value, err := readDenseScorePlaneString(raw, &off, limits.MaxSectionLen, "string")
		if err != nil {
			return proof, err
		}
		*target = value
	}
	formats := [...]string{"", "tcs1"}
	for _, target := range []*collections.ColumnGraphManifestWork{&proof.Snapshot.BaseManifest, &proof.Snapshot.CurrentManifest} {
		var m [4]uint64
		for j := range m {
			var err error
			m[j], err = readUvarintField(raw, &off, "dense score-plane manifest")
			if err != nil {
				return proof, err
			}
		}
		if m[1] > 1 || m[2] > 65535 {
			return proof, protocolError(iwire.ErrMalformedFrame, "invalid dense score-plane manifest")
		}
		*target = collections.ColumnGraphManifestWork{Generation: m[0], Format: formats[m[1]], Version: uint16(m[2]), Checksum: m[3]}
	}
	if off != len(raw) {
		return proof, protocolError(iwire.ErrMalformedFrame, "dense score-plane proof has trailing bytes")
	}
	return proof, validateDenseScorePlane(proof)
}

func decodeDenseScorePlaneSection(sections []iwire.Section, required bool, limits iwire.Limits) (*collections.ColumnGraphScorePlaneWork, error) {
	raw, found, err := singletonSection(sections, iwire.SectionDenseSearchScorePlaneProof)
	if err != nil {
		return nil, err
	}
	if !found {
		if required {
			return nil, protocolError(iwire.ErrMalformedFrame, "dense score-plane proof section missing")
		}
		return nil, nil
	}
	proof, err := decodeDenseScorePlane(raw, limits)
	if err != nil {
		return nil, err
	}
	return &proof, nil
}
