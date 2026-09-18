package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	VectorPartitionRouterEffortLegacyV1           = "legacy_l0_distinct"
	VectorPartitionRouterEffortHierarchicalV1     = "hierarchical_all_scores"
	VectorPartitionRouterEffortDiagnosticMethodV1 = "bounded_router_effort_v1"
	// This limits invocations, not model rows. Hierarchical traversal may score
	// one row repeatedly. Population-wide work/memory must also be preflighted.
	VectorPartitionRouterEffortMaxScoreCallsV1 = 1 << 24
)

// ErrVectorPartitionRouterEffortBudgetV1 is a diagnostic refusal, never a
// successful partial route. It does not change public serving error semantics.
var ErrVectorPartitionRouterEffortBudgetV1 = errors.New("router effort diagnostic score budget exhausted")

// VectorPartitionRouterEffortOptionsV1 selects one explicit offline treatment.
// The zero value disables no guard and is invalid; ordinary Search is unchanged.
type VectorPartitionRouterEffortOptionsV1 struct {
	Mode          string `json:"mode"`
	ReturnedWidth int    `json:"returned_width"`
	Beam          int    `json:"beam"`
	ScoreBudget   int    `json:"score_budget"`
}

func ValidateVectorPartitionRouterEffortOptionsV1(o VectorPartitionRouterEffortOptionsV1, representatives int) error {
	if representatives < 1 || o.ReturnedWidth < 1 || o.ReturnedWidth > o.Beam || o.Beam > representatives || o.ScoreBudget < o.Beam || o.ScoreBudget > VectorPartitionRouterEffortMaxScoreCallsV1 {
		return errors.New("invalid router effort w/E/C: require 1<=w<=E<=N and E<=C<=hard call cap")
	}
	switch o.Mode {
	case VectorPartitionRouterEffortLegacyV1:
		if o.ScoreBudget > representatives {
			return errors.New("legacy distinct-score budget exceeds model rows")
		}
	case VectorPartitionRouterEffortHierarchicalV1:
		// C>N is legal. Do not silently clamp total invocations to distinct rows.
	default:
		return errors.New("unknown router effort entry/budget mode")
	}
	return nil
}

func VectorPartitionRouterEffortIdentityV1(o VectorPartitionRouterEffortOptionsV1) string {
	h := sha256.New()
	_, _ = h.Write([]byte(VectorPartitionRouterEffortDiagnosticMethodV1 + "\x00" + o.Mode + "\x00"))
	var b [8]byte
	for _, x := range []int{o.ReturnedWidth, o.Beam, o.ScoreBudget} {
		binary.LittleEndian.PutUint64(b[:], uint64(x))
		_, _ = h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

type VectorPartitionRouterEffortWorkV1 struct {
	EntryMode           string `json:"entry_mode"`
	BudgetUnit          string `json:"budget_unit"`
	InitialEntryOrdinal int    `json:"initial_entry_ordinal"`
	Layer0EntryOrdinal  int    `json:"layer0_entry_ordinal"`
	UpperLayers         int    `json:"upper_layers_entered"`
	UpperScoreCalls     uint64 `json:"upper_score_calls"`
	Layer0Distinct      uint64 `json:"layer0_distinct_scores"`
	TotalScoreCalls     uint64 `json:"total_score_calls"`
	VectorBytes         uint64 `json:"vector_bytes_read"`
	Edges               uint64 `json:"edges"`
	Returned            int    `json:"returned"`
	ReachedScoreCap     bool   `json:"reached_score_cap"`
	BudgetExhausted     bool   `json:"budget_exhausted"`
}

// Aliased owned scalar results from R1. This is not a second ranking algorithm.
type VectorPartitionRouterEffortPoliciesV1 = vectorPartitionPolicyReductionV1

type VectorPartitionRouterEffortComparisonV1 struct {
	Method              string                                 `json:"method"`
	Generation          uint64                                 `json:"generation"`
	SourceGeneration    uint64                                 `json:"source_generation"`
	ModelSHA256         string                                 `json:"model_sha256"`
	QuerySHA256         string                                 `json:"query_sha256"`
	RepresentativeCount int                                    `json:"representative_count"`
	DomainCount         int                                    `json:"domain_count"`
	Probes              int                                    `json:"probes"`
	Options             VectorPartitionRouterEffortOptionsV1   `json:"options"`
	OptionsSHA256       string                                 `json:"options_sha256"`
	CollectionComplete  bool                                   `json:"collection_complete"`
	Work                VectorPartitionRouterEffortWorkV1      `json:"work"`
	Policies            *VectorPartitionRouterEffortPoliciesV1 `json:"policies,omitempty"`
}

// A fixed-size opt-in observation at traversal phase boundaries. No detailed
// trace or model-sized storage is enabled by collecting this observation.
type columnHNSWSearchPackEffortObservationV1 struct {
	initialEntry, layer0Entry, upperLayers int
}

// CompareRankingPoliciesWithEffortForDiagnosticsV1 runs existing prepared HNSW
// with explicit return, beam and scoring coordinates, then the unchanged R1
// reducers. It owns no serving counters and never retries or invokes exact scan.
func (r *VectorPartitionRouterV1) CompareRankingPoliciesWithEffortForDiagnosticsV1(ctx context.Context, query []float32, o VectorPartitionRouterEffortOptionsV1, probes int) (out VectorPartitionRouterEffortComparisonV1, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if r == nil {
		return out, errors.New("nil router effort owner")
	}
	r.closeMu.RLock()
	defer r.closeMu.RUnlock()
	if r.closed.Load() || r.view == nil {
		return out, errors.New("closed router effort owner")
	}
	n, d := len(r.model.Representatives), r.model.Metrics.Partitions
	if err := ValidateVectorPartitionRouterEffortOptionsV1(o, n); err != nil {
		return out, err
	}
	if d < 1 || d > n || probes < 1 || probes > d || r.view.Header.Rows != n || len(r.viewToModel) != n || r.view.Header.ExternalNormalizedVectors {
		return out, errors.New("invalid router effort model/probes or unsupported score plane")
	}
	if _, err := normalizeVectorPartitionRouterQueryV1(query, r.model.Dimensions); err != nil {
		return out, err
	}
	out = VectorPartitionRouterEffortComparisonV1{Method: VectorPartitionRouterEffortDiagnosticMethodV1, Generation: r.manifest.Generation, SourceGeneration: r.manifest.SourceGeneration, ModelSHA256: r.modelDigest, QuerySHA256: vectorPartitionPolicyQueryDigestV1(query), RepresentativeCount: n, DomainCount: d, Probes: probes, Options: o, OptionsSHA256: VectorPartitionRouterEffortIdentityV1(o)}
	out.Work.EntryMode = "layer0_only"
	out.Work.BudgetUnit = "distinct_layer0_scores"
	strict := o.Mode == VectorPartitionRouterEffortHierarchicalV1
	if strict {
		out.Work.EntryMode = "upper_greedy_then_layer0"
		out.Work.BudgetUnit = "all_actual_vector_score_invocations"
	}
	observation := columnHNSWSearchPackEffortObservationV1{initialEntry: r.view.Header.EntryOrdinal, layer0Entry: -1}
	scratch := r.scratch.Get().(*columnVectorGraphNativeSearchScratch)
	defer r.scratch.Put(scratch)
	native, stats, err := r.view.searchCosineWithContext(ctx, query, columnVectorGraphNativeSearchOptions{TopK: o.ReturnedWidth, EfSearch: o.Beam, CandidateLimit: o.ScoreBudget, StrictScoreBudget: strict, OmitResultMaterialization: true, effortObservation: &observation}, scratch)
	out.Work.InitialEntryOrdinal = observation.initialEntry
	out.Work.Layer0EntryOrdinal = observation.layer0Entry
	out.Work.UpperLayers = observation.upperLayers
	out.Work.TotalScoreCalls = stats.PreparedScoreCalls
	out.Work.Layer0Distinct = stats.Candidates
	out.Work.VectorBytes = stats.VectorBytesRead
	out.Work.Edges = stats.Edges
	if stats.Candidates > stats.PreparedScoreCalls {
		return out, errors.New("router effort score accounting underflow")
	}
	out.Work.UpperScoreCalls = stats.PreparedScoreCalls - stats.Candidates
	out.Work.ReachedScoreCap = stats.PreparedScoreCalls >= uint64(o.ScoreBudget)
	if stats.PreparedScoreCalls > uint64(o.ScoreBudget) {
		return out, errors.New("router effort exceeded actual score budget")
	}
	if err != nil {
		if errors.Is(err, errTypedGraphSearchBudget) {
			out.Work.BudgetExhausted = true
			return out, fmt.Errorf("%w: %v", ErrVectorPartitionRouterEffortBudgetV1, err)
		}
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	out.CollectionComplete = true
	out.Work.Returned = len(native)
	if len(native) > o.ReturnedWidth {
		return out, errors.New("router effort returned more than w")
	}
	candidates := make([]vectorPartitionPolicyCandidateV1, len(native))
	for i, c := range native {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return out, err
			}
		}
		if c.Ordinal < 0 || c.Ordinal >= n {
			return out, errors.New("invalid router effort native ordinal")
		}
		ordinal := r.viewToModel[c.Ordinal]
		if ordinal < 0 || ordinal >= n {
			return out, errors.New("invalid router effort model ordinal")
		}
		rep := r.model.Representatives[ordinal]
		candidates[i] = vectorPartitionPolicyCandidateV1{Ordinal: ordinal, Domain: rep.PartitionID, SourceOrdinal: rep.SourceOrdinal, Score: c.Score}
	}
	// R1's CandidateBudget bounds the supplied returned slice here, not total
	// scores. The distinct retrieval digest binds w/E/C and entry policy explicitly.
	meta := vectorPartitionPolicyContextV1{ModelDigest: out.ModelSHA256, QueryDigest: out.QuerySHA256, Mode: "approximate", RepresentativeCount: n, DomainCount: d, CandidateBudget: o.ReturnedWidth, ReturnedWidth: o.ReturnedWidth, Probes: probes, RetrievalDigest: out.OptionsSHA256}
	reduced, err := reduceVectorPartitionRouterPoliciesV1(ctx, meta, candidates)
	if err == nil || errors.Is(err, errVectorPartitionPolicyCoverageV1) {
		out.Policies = &reduced
	}
	if errors.Is(err, errVectorPartitionPolicyCoverageV1) {
		return out, ErrVectorPartitionRouterCandidateCoverageV1
	}
	return out, err
}
