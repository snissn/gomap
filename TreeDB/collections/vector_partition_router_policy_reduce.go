package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
)

const vectorPartitionRankingDiagnosticMethodV1 = "all_level_representatives_frequency_first_w_E_C_v2"

// This private error is mapped to the existing public typed coverage error by
// the owner-pinned diagnostic entry point, not exposed as a new public contract.
var errVectorPartitionPolicyCoverageV1 = errors.New("router policy candidate coverage shortfall")

type vectorPartitionPolicyCandidateV1 struct {
	Ordinal       int
	Domain        uint32
	SourceOrdinal uint64
	Score         float64
}

type vectorPartitionPolicyContextV1 struct {
	ModelDigest         string
	QueryDigest         string
	Mode                string
	RepresentativeCount int
	DomainCount         int
	CandidateBudget     int
	ReturnedWidth       int
	BeamWidth           int
	Probes              int
}

type vectorPartitionPolicyDomainV1 struct {
	Domain                uint32  `json:"domain"`
	Distance              float64 `json:"distance"`
	Frequency             int     `json:"frequency"`
	WinningRepresentative int     `json:"winning_representative"`
	WinningSourceOrdinal  uint64  `json:"winning_source_ordinal"`
}

type vectorPartitionPolicyReductionV1 struct {
	Method                  string                          `json:"method"`
	CandidateSetSHA256      string                          `json:"candidate_set_sha256"`
	CandidateSequenceSHA256 string                          `json:"candidate_sequence_sha256"`
	InputCount              int                             `json:"input_count"`
	UniqueReturned          int                             `json:"unique_returned"`
	Distance                []vectorPartitionPolicyDomainV1 `json:"distance"`
	Frequency               []vectorPartitionPolicyDomainV1 `json:"frequency"`
	Hybrid                  []vectorPartitionPolicyDomainV1 `json:"hybrid"`
}

func vectorPartitionPolicyDistanceV1(score float64) float64 {
	distance := 1 - score
	if distance < 0 && distance > -1e-6 {
		return 0
	}
	return distance
}

// Hash through one bounded row buffer. V2 binds independent beam and score work.
// Detailed diagnostic hashing is never called by the ordinary route.
func vectorPartitionPolicyDigestV1(ctx context.Context, meta vectorPartitionPolicyContextV1, kind string, candidates []vectorPartitionPolicyCandidateV1) (string, error) {
	h := sha256.New()
	var buf [32]byte
	writeUint := func(v uint64) { binary.LittleEndian.PutUint64(buf[:8], v); _, _ = h.Write(buf[:8]) }
	for _, value := range []string{vectorPartitionRankingDiagnosticMethodV1, kind, meta.ModelDigest, meta.QueryDigest, meta.Mode, "cosine_1_minus_score_small_negative_clamp_v1"} {
		writeUint(uint64(len(value)))
		_, _ = h.Write([]byte(value))
	}
	// Probes is not candidate identity: the same set supports all prefixes.
	for _, value := range []int{meta.RepresentativeCount, meta.DomainCount, meta.CandidateBudget, meta.ReturnedWidth, meta.BeamWidth, len(candidates)} {
		writeUint(uint64(value))
	}
	for i, c := range candidates {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		binary.LittleEndian.PutUint64(buf[0:8], uint64(c.Ordinal))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(c.Domain))
		binary.LittleEndian.PutUint64(buf[16:24], c.SourceOrdinal)
		binary.LittleEndian.PutUint64(buf[24:32], math.Float64bits(c.Score))
		_, _ = h.Write(buf[:])
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var sum [sha256.Size]byte
	return hex.EncodeToString(h.Sum(sum[:0])), nil
}

// reduceVectorPartitionRouterPoliciesV1 compares reducers only. Input must be
// a bounded candidate collection from ONE model/query/retrieval operation. No
// representative search, retry, graph access, or fallback occurs in this helper.
// All output storage is owned; neither the input nor its order is modified.
func reduceVectorPartitionRouterPoliciesV1(ctx context.Context, meta vectorPartitionPolicyContextV1, input []vectorPartitionPolicyCandidateV1) (vectorPartitionPolicyReductionV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	for _, digest := range []string{meta.ModelDigest, meta.QueryDigest} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
			return vectorPartitionPolicyReductionV1{}, errors.New("invalid policy model/query digest")
		}
	}
	if meta.RepresentativeCount < 1 || meta.DomainCount < 1 || meta.DomainCount > meta.RepresentativeCount || meta.Probes < 1 || meta.Probes > meta.DomainCount ||
		meta.ReturnedWidth < 1 || meta.ReturnedWidth > meta.RepresentativeCount || meta.CandidateBudget < 1 || len(input) > meta.CandidateBudget {
		return vectorPartitionPolicyReductionV1{}, errors.New("invalid router policy shape/budget")
	}
	switch meta.Mode {
	case VectorPartitionRouterModeExactV1:
		if meta.CandidateBudget < meta.RepresentativeCount {
			return vectorPartitionPolicyReductionV1{}, errors.New("exact policy scan lacks full representative budget")
		}
	case VectorPartitionRouterModeApproxV1:
	default:
		return vectorPartitionPolicyReductionV1{}, errors.New("unsupported policy retrieval mode")
	}
	// This map is bounded by the collected input, never an array indexed by
	// the full corpus. Ordinary production search does not call this helper.
	unique := make(map[int]vectorPartitionPolicyCandidateV1, len(input))
	for i, candidate := range input {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return vectorPartitionPolicyReductionV1{}, err
			}
		}
		if candidate.Ordinal < 0 || candidate.Ordinal >= meta.RepresentativeCount || uint64(candidate.Domain) >= uint64(meta.DomainCount) ||
			math.IsNaN(candidate.Score) || math.IsInf(candidate.Score, 0) {
			return vectorPartitionPolicyReductionV1{}, errors.New("invalid returned representative")
		}
		if old, present := unique[candidate.Ordinal]; present {
			if old.Domain != candidate.Domain || old.SourceOrdinal != candidate.SourceOrdinal || math.Float64bits(old.Score) != math.Float64bits(candidate.Score) {
				return vectorPartitionPolicyReductionV1{}, errors.New("conflicting duplicate representative")
			}
			continue
		}
		unique[candidate.Ordinal] = candidate
	}
	if meta.Mode == VectorPartitionRouterModeExactV1 && len(unique) != meta.RepresentativeCount {
		return vectorPartitionPolicyReductionV1{}, errors.New("exact policy receipt omits model representatives")
	}
	candidates := make([]vectorPartitionPolicyCandidateV1, 0, len(unique))
	for _, candidate := range unique {
		if len(candidates)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return vectorPartitionPolicyReductionV1{}, err
			}
		}
		candidates = append(candidates, candidate)
	}
	var err error
	candidates, err = nearestVectorPartitionPolicyCandidatesV1(ctx, candidates, meta.ReturnedWidth)
	if err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	// Nearest-w selection precedes voting; identity order canonicalizes SET
	// hashing without changing score order or hiding a distinct sequence hash.
	if err := sortVectorPartitionSliceWithContextV1(ctx, candidates, func(a, b vectorPartitionPolicyCandidateV1) bool { return a.Ordinal < b.Ordinal }); err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	best := make(map[uint32]vectorPartitionPolicyDomainV1, min(len(candidates), meta.DomainCount))
	for i, candidate := range candidates {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return vectorPartitionPolicyReductionV1{}, err
			}
		}
		distance := vectorPartitionPolicyDistanceV1(candidate.Score)
		row, present := best[candidate.Domain]
		if !present {
			row.Domain = candidate.Domain
			row.Distance = distance
			row.WinningRepresentative = candidate.Ordinal
			row.WinningSourceOrdinal = candidate.SourceOrdinal
		} else if distance < row.Distance || (distance == row.Distance && candidate.Ordinal < row.WinningRepresentative) {
			row.Distance = distance
			row.WinningRepresentative = candidate.Ordinal
			row.WinningSourceOrdinal = candidate.SourceOrdinal
		}
		row.Frequency++
		best[candidate.Domain] = row
	}
	setDigest, err := vectorPartitionPolicyDigestV1(ctx, meta, "selected_candidate_set", candidates)
	if err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	sequenceDigest, err := vectorPartitionPolicyDigestV1(ctx, meta, "collected_candidate_sequence", input)
	if err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	result := vectorPartitionPolicyReductionV1{Method: vectorPartitionRankingDiagnosticMethodV1,
		CandidateSetSHA256: setDigest, CandidateSequenceSHA256: sequenceDigest, InputCount: len(input), UniqueReturned: len(candidates)}
	if len(best) < meta.Probes {
		// Complete scalar candidate receipt, but never a partial route.
		return result, fmt.Errorf("%w: reached=%d requested=%d", errVectorPartitionPolicyCoverageV1, len(best), meta.Probes)
	}

	base := make([]vectorPartitionPolicyDomainV1, 0, len(best))
	for _, row := range best {
		if len(base)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return vectorPartitionPolicyReductionV1{}, err
			}
		}
		base = append(base, row)
	}
	distanceLess := func(a, b vectorPartitionPolicyDomainV1) bool {
		if a.Distance != b.Distance {
			return a.Distance < b.Distance
		}
		return a.Domain < b.Domain
	}
	if err := sortVectorPartitionSliceWithContextV1(ctx, base, distanceLess); err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	frequency := make([]vectorPartitionPolicyDomainV1, len(base))
	for start := 0; start < len(base); start += 1024 {
		if err := ctx.Err(); err != nil {
			return vectorPartitionPolicyReductionV1{}, err
		}
		copy(frequency[start:min(start+1024, len(base))], base[start:min(start+1024, len(base))])
	}
	if err := sortVectorPartitionSliceWithContextV1(ctx, frequency, func(a, b vectorPartitionPolicyDomainV1) bool {
		if a.Frequency != b.Frequency {
			return a.Frequency > b.Frequency
		}
		return distanceLess(a, b)
	}); err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	// Return only the requested prefixes, not slices retaining full-domain
	// temporary arrays. All three slices share one owned, disjoint capped buffer.
	routes := make([]vectorPartitionPolicyDomainV1, 3*meta.Probes)
	result.Distance = routes[:meta.Probes:meta.Probes]
	result.Frequency = routes[meta.Probes : 2*meta.Probes : 2*meta.Probes]
	result.Hybrid = routes[2*meta.Probes : 3*meta.Probes : 3*meta.Probes]
	copy(result.Distance, base[:meta.Probes])
	copy(result.Frequency, frequency[:meta.Probes])
	result.Hybrid[0] = frequency[0]
	j := 1
	for i, row := range base {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return vectorPartitionPolicyReductionV1{}, err
			}
		}
		if j == meta.Probes {
			break
		}
		if row.Domain != frequency[0].Domain {
			result.Hybrid[j] = row
			j++
		}
	}
	if err := ctx.Err(); err != nil {
		return vectorPartitionPolicyReductionV1{}, err
	}
	return result, nil
}

// nearestVectorPartitionPolicyCandidatesV1 orders a caller-owned work buffer.
// It must observe cancellation during ordering, not only before and after it.
func nearestVectorPartitionPolicyCandidatesV1(ctx context.Context, candidates []vectorPartitionPolicyCandidateV1, width int) ([]vectorPartitionPolicyCandidateV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if width < 1 {
		return nil, errors.New("invalid nearest representative width")
	}
	if err := sortVectorPartitionSliceWithContextV1(ctx, candidates, func(a, b vectorPartitionPolicyCandidateV1) bool {
		da, db := vectorPartitionPolicyDistanceV1(a.Score), vectorPartitionPolicyDistanceV1(b.Score)
		if da != db {
			return da < db
		}
		return a.Ordinal < b.Ordinal
	}); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return candidates[:min(width, len(candidates))], nil
}
