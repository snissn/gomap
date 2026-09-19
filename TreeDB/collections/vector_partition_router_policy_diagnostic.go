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

// VectorPartitionRouterPolicyDiagnosticMethodV1 identifies an offline comparison,
// not a new serving policy or a promise of improved recall.
const VectorPartitionRouterPolicyDiagnosticMethodV1 = vectorPartitionRankingDiagnosticMethodV1

// VectorPartitionRouterPolicyDiagnosticOptionsV1 compares policies using the
// current V2 collector. It cannot reproduce frozen V1-policy receipts: the model
// identity, hierarchy and actual-score budget semantics have changed.
type VectorPartitionRouterPolicyDiagnosticOptionsV1 struct {
	Mode            string
	ScoreBudget     int
	ReturnedWidth   int
	BeamWidth       int
	PartitionProbes int
}

// VectorPartitionRouterPolicyDomainV1 owns scalar routing evidence only. Source
// ordinal is an anchor, not representative identity and not an independent vote.
type VectorPartitionRouterPolicyDomainV1 = vectorPartitionPolicyDomainV1

// VectorPartitionRouterPolicyComparisonV1 contains deterministic, owned evidence.
// There are no borrowed model/vector slices, and no nondeterministic timing fields.
// On failure all routes are empty; collection metadata/work may remain available.
type VectorPartitionRouterPolicyComparisonV1 struct {
	Method                  string                                `json:"method"`
	Generation              uint64                                `json:"generation"`
	SourceGeneration        uint64                                `json:"source_generation"`
	ModelSHA256             string                                `json:"model_sha256"`
	QuerySHA256             string                                `json:"query_sha256"`
	Mode                    string                                `json:"mode"`
	RepresentativeCount     int                                   `json:"representative_count"`
	DomainCount             int                                   `json:"domain_count"`
	ScoreBudget             int                                   `json:"score_budget"`
	ReturnedWidth           int                                   `json:"returned_width"`
	BeamWidth               int                                   `json:"beam_width"`
	ScoreCalls              uint64                                `json:"score_calls"`
	Probes                  int                                   `json:"probes"`
	CollectionComplete      bool                                  `json:"collection_complete"`
	Collected               int                                   `json:"collected"`
	UniqueReturned          int                                   `json:"unique_returned"`
	Candidates              uint64                                `json:"scored_candidates"`
	Edges                   uint64                                `json:"edges"`
	CandidateSetSHA256      string                                `json:"candidate_set_sha256,omitempty"`
	CandidateSequenceSHA256 string                                `json:"candidate_sequence_sha256,omitempty"`
	Distance                []VectorPartitionRouterPolicyDomainV1 `json:"distance,omitempty"`
	Frequency               []VectorPartitionRouterPolicyDomainV1 `json:"frequency,omitempty"`
	Hybrid                  []VectorPartitionRouterPolicyDomainV1 `json:"hybrid,omitempty"`
}

func vectorPartitionPolicyQueryDigestV1(query []float32) string {
	h := sha256.New()
	_, _ = h.Write([]byte("treedb/representative-policy/query-f32/v1\x00"))
	var b [4]byte
	for _, x := range query {
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(x))
		_, _ = h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// CompareRankingPoliciesForDiagnosticsV1 collects once under the existing read
// owner, then replays distance, frequency and frequency-first/distance-rest. It
// neither alters ordinary search counters nor changes graphs, public policy,
// persistent identity or search effort. It does not fetch documents. The caller
// must preflight any multi-query experiment; each invocation is bounded by the
// admitted immutable model and explicit candidate/returned counts.
func (r *VectorPartitionRouterV1) CompareRankingPoliciesForDiagnosticsV1(ctx context.Context, query []float32, opts VectorPartitionRouterPolicyDiagnosticOptionsV1) (out VectorPartitionRouterPolicyComparisonV1, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if r == nil {
		return out, errors.New("collections: nil router policy diagnostic")
	}
	r.closeMu.RLock()
	defer r.closeMu.RUnlock()
	if r.closed.Load() || r.view == nil {
		return out, errors.New("collections: closed router policy diagnostic")
	}
	if opts.Mode == "" {
		opts.Mode = VectorPartitionRouterModeApproxV1
	}
	n, d := len(r.model.Representatives), r.model.Metrics.Partitions
	if n < 1 || d < 1 || d > n || opts.PartitionProbes < 1 || opts.PartitionProbes > d || opts.ReturnedWidth < 1 || opts.ReturnedWidth > opts.BeamWidth || opts.BeamWidth > n || opts.ScoreBudget < 1 || opts.ScoreBudget > MaxVectorPartitionRouterScoreBudgetV2 {
		return out, errors.New("collections: invalid router policy diagnostic shape")
	}
	switch opts.Mode {
	case VectorPartitionRouterModeExactV1:
		if opts.ScoreBudget < n {
			return out, errors.New("collections: exact router diagnostic requires full representative scan")
		}
	case VectorPartitionRouterModeApproxV1:
	default:
		return out, errors.New("collections: unknown router policy diagnostic mode")
	}
	normalized, err := normalizeVectorPartitionRouterQueryV1(query, r.model.Dimensions)
	if err != nil {
		return out, err
	}
	out = VectorPartitionRouterPolicyComparisonV1{Method: VectorPartitionRouterPolicyDiagnosticMethodV1,
		Generation: r.manifest.Generation, SourceGeneration: r.manifest.SourceGeneration,
		ModelSHA256: r.modelDigest, QuerySHA256: vectorPartitionPolicyQueryDigestV1(query), Mode: opts.Mode,
		RepresentativeCount: n, DomainCount: d, ScoreBudget: opts.ScoreBudget, ReturnedWidth: opts.ReturnedWidth, Probes: opts.PartitionProbes}
	meta := vectorPartitionPolicyContextV1{ModelDigest: out.ModelSHA256, QueryDigest: out.QuerySHA256, Mode: opts.Mode,
		RepresentativeCount: n, DomainCount: d, ScoreBudget: opts.ScoreBudget, ReturnedWidth: opts.ReturnedWidth, Probes: opts.PartitionProbes}
	out.BeamWidth = opts.BeamWidth
	meta.BeamWidth = opts.BeamWidth
	width, beam := opts.ReturnedWidth, opts.BeamWidth
	if opts.Mode == VectorPartitionRouterModeExactV1 {
		// The diagnostic reducer owns exact-reference width trimming and proves
		// the full source population before comparing policies on that prefix.
		width, beam = n, n
	}
	candidates, work, err := r.collectVectorPartitionRouterCandidatesLockedV1(ctx, query, normalized, VectorPartitionRouterSearchOptionsV2{Mode: opts.Mode, ScoreBudget: opts.ScoreBudget, ReturnedWidth: width, BeamWidth: beam, PartitionProbes: opts.PartitionProbes})
	out.ScoreCalls = work.ScoreCalls
	if err != nil {
		if errors.Is(err, errTypedGraphSearchBudget) {
			err = ErrVectorPartitionRouterScoreBudget
		}
		return out, err
	}
	out.CollectionComplete = true
	out.Collected = len(candidates)
	out.Candidates = work.Candidates
	out.Edges = work.Edges
	input := make([]vectorPartitionPolicyCandidateV1, len(candidates))
	for i, c := range candidates {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return out, err
			}
		}
		if c.ordinal < 0 || c.ordinal >= n {
			return out, errors.New("collections: invalid diagnostic representative ordinal")
		}
		rep := r.model.Representatives[c.ordinal]
		input[i] = vectorPartitionPolicyCandidateV1{Ordinal: c.ordinal, Domain: rep.PartitionID, SourceOrdinal: rep.SourceOrdinal, Score: c.score}
	}
	reduced, err := reduceVectorPartitionRouterPoliciesV1(ctx, meta, input)
	if err == nil || errors.Is(err, errVectorPartitionPolicyCoverageV1) {
		out.UniqueReturned = reduced.UniqueReturned
		out.CandidateSetSHA256 = reduced.CandidateSetSHA256
		out.CandidateSequenceSHA256 = reduced.CandidateSequenceSHA256
	}
	if err != nil {
		if errors.Is(err, errVectorPartitionPolicyCoverageV1) {
			return out, fmt.Errorf("%w: diagnostic nearest representatives do not cover %d domains", ErrVectorPartitionRouterCandidateCoverageV1, opts.PartitionProbes)
		}
		return out, err
	}
	out.Distance = reduced.Distance
	out.Frequency = reduced.Frequency
	out.Hybrid = reduced.Hybrid
	return out, nil
}
