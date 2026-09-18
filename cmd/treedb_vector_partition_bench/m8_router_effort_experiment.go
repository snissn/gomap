package main

// R2 changes only offline representative retrieval. Timed coordinator queries
// still use their original routing contract; no diagnostic result is a fallback.
import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"

	"github.com/snissn/gomap/TreeDB/collections"
)

const m8RouterEffortMethodV1 = "static_router_effort_comparison_v1"

type m8RouterEffortDeltaV1 struct {
	Lost   uint16 `json:"lost"`
	Gained uint16 `json:"gained"`
}
type m8RouterEffortQueryV1 struct {
	QuerySHA256  string                                              `json:"query_sha256"`
	TruthSHA256  string                                              `json:"truth_sha256"`
	Status       string                                              `json:"status"`
	Comparison   collections.VectorPartitionRouterEffortComparisonV1 `json:"comparison"`
	Coverage     *m8RouterPolicyCoverageV1                           `json:"coverage,omitempty"`
	VersusLegacy *m8RouterEffortDeltaV1                              `json:"distance_versus_legacy,omitempty"`
	VersusExact  *m8RouterEffortDeltaV1                              `json:"distance_versus_exact,omitempty"`
}
type m8RouterEffortEvidenceV1 struct {
	Method  string                                           `json:"method"`
	Options collections.VectorPartitionRouterEffortOptionsV1 `json:"options"`
	Queries []m8RouterEffortQueryV1                          `json:"queries"`
}
type m8RouterEffortCacheV1 struct {
	options  collections.VectorPartitionRouterEffortOptionsV1
	byProbes map[int]*m8RouterEffortEvidenceV1
}

func m8RouterEffortConfigV1(cfg config) *collections.VectorPartitionRouterEffortOptionsV1 {
	if cfg.m8RouterEffort.Mode == "" {
		return nil
	}
	copy := cfg.m8RouterEffort
	return &copy
}
func m8ValidateRouterEffortConfigV1(cfg config) error {
	o := cfg.m8RouterEffort
	if o.Mode == "" {
		if o != (collections.VectorPartitionRouterEffortOptionsV1{}) {
			return errors.New("router effort coordinates require explicit diagnostic mode")
		}
		return nil
	}
	if !cfg.m8QualityDiagnostics || !cfg.m8RouterPolicyDiagnostics {
		return errors.New("router effort requires quality and same-candidate policy diagnostics")
	}
	// Actual N is validated after immutable assets open, before topology/timing.
	if err := collections.ValidateVectorPartitionRouterEffortOptionsV1(o, collections.VectorPartitionRouterEffortMaxScoreCallsV1); err != nil {
		return err
	}
	width := cfg.m8RouterPolicyWidth
	if width == 0 {
		width = cfg.routerCandidates
	}
	if o.ReturnedWidth != width {
		return errors.New("router effort w must match the explicitly frozen R1 returned width")
	}
	return nil
}
func (h *m8AttributionHarnessV1) enableRouterEffortV1(o collections.VectorPartitionRouterEffortOptionsV1) error {
	if h == nil || h.assets == nil || h.assets.router == nil || h.quality == nil || h.policies == nil || h.effort != nil {
		return errors.New("effort requires one initialized quality/policy owner")
	}
	if err := collections.ValidateVectorPartitionRouterEffortOptionsV1(o, int(h.assets.status.Representatives)); err != nil {
		return err
	}
	if o.ReturnedWidth != h.policies.width {
		return errors.New("effort width differs from R1 exact/legacy control")
	}
	h.effort = &m8RouterEffortCacheV1{options: o, byProbes: make(map[int]*m8RouterEffortEvidenceV1)}
	return nil
}

func m8EffortCoverageV1(masks []uint16, costs []int64, p *collections.VectorPartitionRouterEffortPoliciesV1) (*m8RouterPolicyCoverageV1, error) {
	c := &m8RouterPolicyCoverageV1{}
	var err error
	c.DistanceMask, c.DistancePacks, err = m8RouteCoverageV1(masks, costs, m8RouterPolicyDomainsV1(p.Distance))
	if err != nil {
		return nil, err
	}
	c.FrequencyMask, c.FrequencyPacks, err = m8RouteCoverageV1(masks, costs, m8RouterPolicyDomainsV1(p.Frequency))
	if err != nil {
		return nil, err
	}
	c.HybridMask, c.HybridPacks, err = m8RouteCoverageV1(masks, costs, m8RouterPolicyDomainsV1(p.Hybrid))
	if err != nil {
		return nil, err
	}
	c.HybridLost = c.DistanceMask &^ c.HybridMask
	c.HybridGained = c.HybridMask &^ c.DistanceMask
	c.FrequencyLost = c.DistanceMask &^ c.FrequencyMask
	c.FrequencyGained = c.FrequencyMask &^ c.DistanceMask
	return c, nil
}
func m8EffortDeltaV1(before, after uint16) *m8RouterEffortDeltaV1 {
	return &m8RouterEffortDeltaV1{Lost: before &^ after, Gained: after &^ before}
}

func (h *m8AttributionHarnessV1) routerEffortEvidenceV1(ctx context.Context, queries [][]float64, truth [][]m8CanonicalResultV1, probes, approximateBudget int) (*m8RouterEffortEvidenceV1, error) {
	if h.effort == nil {
		return nil, nil
	}
	// R1 rechecks query/truth identity on EVERY lookup. The R2 resource plan
	// charges these scans, even if both candidate receipts are already cached.
	controls, err := h.routerPolicyEvidenceV1(ctx, queries, truth, probes, approximateBudget)
	if err != nil {
		return nil, err
	}
	if controls == nil || controls.EffectiveWidth != h.effort.options.ReturnedWidth {
		return nil, errors.New("effort control coordinate missing")
	}
	if cached := h.effort.byProbes[probes]; cached != nil {
		return cached, nil
	}
	e := &m8RouterEffortEvidenceV1{Method: m8RouterEffortMethodV1, Options: h.effort.options, Queries: make([]m8RouterEffortQueryV1, len(queries))}
	for i, q := range queries {
		c, err := h.assets.router.CompareRankingPoliciesWithEffortForDiagnosticsV1(ctx, m8Query32V1(q), e.Options, probes)
		r := &e.Queries[i]
		r.QuerySHA256 = h.quality.queries[i].querySHA256
		r.TruthSHA256 = h.quality.queries[i].truthSHA256
		r.Comparison = c
		switch {
		case errors.Is(err, collections.ErrVectorPartitionRouterEffortBudgetV1):
			r.Status = "score_budget_exhausted"
			continue
		case errors.Is(err, collections.ErrVectorPartitionRouterCandidateCoverageV1):
			r.Status = "candidate_coverage_shortfall"
			continue
		case err != nil:
			return nil, err
		}
		r.Status = "pass"
		r.Coverage, err = m8EffortCoverageV1(h.quality.queries[i].masks, h.quality.packCosts, c.Policies)
		if err != nil {
			return nil, err
		}
		if old := controls.Queries[i].Approximate.Coverage; old != nil {
			r.VersusLegacy = m8EffortDeltaV1(old.DistanceMask, r.Coverage.DistanceMask)
		}
		if old := controls.Queries[i].Exact.Coverage; old != nil {
			r.VersusExact = m8EffortDeltaV1(old.DistanceMask, r.Coverage.DistanceMask)
		}
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	h.effort.byProbes[probes] = e
	return e, nil
}

// Shape checks never replace independent retained-asset replay. In particular,
// budget failures carry real static identities/work, never partial routes.
func m8RouterEffortEvidenceSelectionV1(cfg m8ProductionConfigEvidenceV1, row m8ProductionRowV1) error {
	e, q, p := row.Attribution.RouterEffort, row.Attribution.Quality, row.Attribution.RouterPolicies
	if cfg.RouterEffort == nil {
		if e != nil {
			return errors.New("unselected router effort evidence")
		}
		return nil
	}
	if !cfg.QualityDiagnostics || !cfg.RouterPolicyDiagnostics {
		return errors.New("effort evidence missing controls")
	}
	if row.Status != "pass" && row.Status != "fail" && row.Status != "candidate_coverage_shortfall" {
		if e != nil {
			return errors.New("unexecuted row has effort observations")
		}
		return nil
	}
	if e == nil || q == nil || p == nil || e.Method != m8RouterEffortMethodV1 || e.Options != *cfg.RouterEffort || e.Options.ReturnedWidth != p.EffectiveWidth || len(e.Queries) != row.Samples || len(q.Queries) != row.Samples || len(p.Queries) != row.Samples || row.Samples < 1 {
		return errors.New("invalid effort selection/population")
	}
	for i, r := range e.Queries {
		c, w := r.Comparison, r.Comparison.Work
		if err := collections.ValidateVectorPartitionRouterEffortOptionsV1(c.Options, c.RepresentativeCount); err != nil {
			return err
		}
		if c.Method != collections.VectorPartitionRouterEffortDiagnosticMethodV1 || c.Options != e.Options || c.OptionsSHA256 != collections.VectorPartitionRouterEffortIdentityV1(e.Options) || c.Generation != q.Generation || c.SourceGeneration != q.SourceGeneration || c.ModelSHA256 != q.ModelSHA256 || c.QuerySHA256 != p.Queries[i].Exact.Comparison.QuerySHA256 || r.QuerySHA256 != q.Queries[i].QuerySHA256 || r.TruthSHA256 != q.Queries[i].TruthSHA256 || c.DomainCount != q.Domains || c.RepresentativeCount != p.Queries[i].Exact.Comparison.RepresentativeCount || c.Probes != row.Probes {
			return errors.New("effort static identity mismatch")
		}
		if w.TotalScoreCalls > uint64(e.Options.ScoreBudget) || w.Layer0Distinct > uint64(c.RepresentativeCount) || w.UpperScoreCalls > w.TotalScoreCalls || w.TotalScoreCalls-w.UpperScoreCalls != w.Layer0Distinct || w.InitialEntryOrdinal < 0 || w.InitialEntryOrdinal >= c.RepresentativeCount || w.Layer0EntryOrdinal < -1 || w.Layer0EntryOrdinal >= c.RepresentativeCount || w.UpperLayers < 0 || w.Returned < 0 || w.Returned > e.Options.ReturnedWidth || w.ReachedScoreCap != (w.TotalScoreCalls == uint64(e.Options.ScoreBudget)) {
			return errors.New("effort work/budget mismatch")
		}
		strict := e.Options.Mode == collections.VectorPartitionRouterEffortHierarchicalV1
		if strict {
			if w.EntryMode != "upper_greedy_then_layer0" || w.BudgetUnit != "all_actual_vector_score_invocations" {
				return errors.New("hierarchical score units changed")
			}
		} else if w.EntryMode != "layer0_only" || w.BudgetUnit != "distinct_layer0_scores" || w.UpperScoreCalls != 0 || w.UpperLayers != 0 || w.BudgetExhausted {
			return errors.New("legacy entry/units changed")
		}
		if r.Status == "score_budget_exhausted" {
			if !strict || !w.BudgetExhausted || c.CollectionComplete || c.Policies != nil || w.Returned != 0 || r.Coverage != nil || r.VersusLegacy != nil || r.VersusExact != nil {
				return errors.New("strict failure contains a partial route")
			}
			continue
		}
		if w.BudgetExhausted || !c.CollectionComplete || c.Policies == nil || w.Layer0EntryOrdinal < 0 || w.Returned > int(w.Layer0Distinct) {
			return errors.New("effort completion inconsistent")
		}
		reduced := c.Policies
		if reduced.Method != collections.VectorPartitionRouterPolicyDiagnosticMethodV1 || reduced.InputCount != w.Returned || reduced.UniqueReturned < 1 || reduced.UniqueReturned > w.Returned || !m8SHA256V1(reduced.CandidateSetSHA256) || !m8SHA256V1(reduced.CandidateSequenceSHA256) {
			return errors.New("effort returned candidates incomplete")
		}
		if r.Status == "candidate_coverage_shortfall" {
			if len(reduced.Distance)+len(reduced.Frequency)+len(reduced.Hybrid) != 0 || r.Coverage != nil || r.VersusLegacy != nil || r.VersusExact != nil {
				return errors.New("effort shortfall returned route")
			}
			continue
		}
		if r.Status != "pass" || r.Coverage == nil {
			return errors.New("unknown effort outcome")
		}
		cv := r.Coverage
		full := uint16(1<<uint(cfg.TopK)) - 1
		for kind, arm := range []struct {
			routes []collections.VectorPartitionRouterPolicyDomainV1
			mask   uint16
			cost   int64
		}{{reduced.Distance, cv.DistanceMask, cv.DistancePacks}, {reduced.Frequency, cv.FrequencyMask, cv.FrequencyPacks}, {reduced.Hybrid, cv.HybridMask, cv.HybridPacks}} {
			if len(arm.routes) != row.Probes || arm.mask&^full != 0 {
				return errors.New("effort route shape")
			}
			seen := make(map[uint32]bool, len(arm.routes))
			var cost int64
			votes := 0
			for j, d := range arm.routes {
				if int(d.Domain) >= q.Domains || seen[d.Domain] || math.IsNaN(d.Distance) || math.IsInf(d.Distance, 0) || d.Frequency < 1 || d.WinningRepresentative < 0 || d.WinningRepresentative >= c.RepresentativeCount {
					return errors.New("invalid effort route member")
				}
				seen[d.Domain] = true
				votes += d.Frequency
				var err error
				cost, err = memoryAdd(cost, q.PackCosts[d.Domain])
				if err != nil {
					return err
				}
				if j > 0 && (kind != 2 || j > 1) && m8RouterPolicyLessV1(d, arm.routes[j-1], kind == 1) {
					return errors.New("effort route order")
				}
			}
			if votes > reduced.UniqueReturned || cost != arm.cost {
				return errors.New("effort votes/physical expansion mismatch")
			}
			hd, err := q.Queries[i].DomainCost.hitsAtBudget(int64(row.Probes))
			if err != nil {
				return err
			}
			hp, err := q.Queries[i].PackCost.hitsAtBudget(cost)
			if err != nil || bits.OnesCount16(arm.mask) > min(hd, hp) {
				return errors.New("effort exceeds coverage ceiling")
			}
		}
		if reduced.Hybrid[0] != reduced.Frequency[0] || cv.HybridLost != cv.DistanceMask&^cv.HybridMask || cv.HybridGained != cv.HybridMask&^cv.DistanceMask || cv.FrequencyLost != cv.DistanceMask&^cv.FrequencyMask || cv.FrequencyGained != cv.FrequencyMask&^cv.DistanceMask {
			return errors.New("effort policy delta mismatch")
		}
		var old, exact *m8RouterEffortDeltaV1
		if before := p.Queries[i].Approximate.Coverage; before != nil {
			old = m8EffortDeltaV1(before.DistanceMask, cv.DistanceMask)
		}
		if before := p.Queries[i].Exact.Coverage; before != nil {
			exact = m8EffortDeltaV1(before.DistanceMask, cv.DistanceMask)
		}
		if !reflect.DeepEqual(old, r.VersusLegacy) || !reflect.DeepEqual(exact, r.VersusExact) {
			return errors.New("effort versus-control delta mismatch")
		}
	}
	return nil
}

func m8PlanRouterEffortV1(cfg config, m fixtureManifest, domains []int, capWork, capBytes int64) (int64, int64, error) {
	if err := m8ValidateRouterEffortConfigV1(cfg); err != nil {
		return 0, 0, err
	}
	if cfg.m8RouterEffort.Mode == "" {
		return 0, 0, nil
	}
	if len(domains) == 0 || m.Queries < 1 || m.Dimensions < 1 || m.Dimensions > maxDimensions || m.Vectors < 1 || len(cfg.probes) == 0 || len(cfg.efSearch) == 0 || len(cfg.concurrency) == 0 || capWork < 1 || capBytes < 1 {
		return 0, 0, errors.New("invalid effort work shape")
	}
	o := cfg.m8RouterEffort
	calls, err := memoryMul(int64(m.Queries), int64(len(cfg.probes)))
	if err != nil {
		return 0, 0, err
	}
	records, err := memoryMul(calls, int64(len(cfg.efSearch)), int64(len(cfg.concurrency)))
	if err != nil {
		return 0, 0, err
	}
	var total, peak int64
	for variant, d := range domains {
		n, err := m8RouterPolicyRepresentativeBoundV1(cfg, variant, d, m.Vectors)
		if err != nil {
			return 0, 0, err
		}
		if err := collections.ValidateVectorPartitionRouterEffortOptionsV1(o, int(n)); err != nil {
			return 0, 0, err
		}
		scores, err := memoryMul(int64(o.ScoreBudget), int64(m.Dimensions)+n+16)
		if err != nil {
			return 0, 0, err
		}
		reducing, err := memoryMul(int64(o.ReturnedWidth), 8*int64(bits.Len(uint(o.ReturnedWidth)))+256)
		if err != nil {
			return 0, 0, err
		}
		perCall, err := memoryAdd(scores, reducing, 16*int64(m.Dimensions))
		if err != nil {
			return 0, 0, err
		}
		work, err := memoryMul(calls, perCall)
		if err != nil {
			return 0, 0, err
		}
		// Both the producer and independent replay can revalidate cached population
		// and scalar rows. Charge the larger execution conservatively, not zero.
		bookkeeping, err := memoryMul(records, 4, 8*int64(m.Dimensions)+int64(cfg.topK)*(documentIDStorageBytes+16)+4096+int64(d)*1024)
		if err != nil {
			return 0, 0, err
		}
		total, err = memoryAdd(total, work, bookkeeping)
		if err != nil {
			return 0, 0, err
		}
		scratch, err := memoryMul(n, 512)
		if err != nil {
			return 0, 0, err
		}
		retained, err := memoryMul(records, 4, 4096+int64(d)*3*256)
		if err != nil {
			return 0, 0, err
		}
		bytes, err := memoryAdd(scratch, retained, 16*int64(m.Dimensions))
		if err != nil {
			return 0, 0, err
		}
		peak = max(peak, bytes)
	}
	if total > capWork || peak > capBytes {
		return total, peak, fmt.Errorf("router effort exceeds work/memory envelope: work=%d bytes=%d", total, peak)
	}
	return total, peak, nil
}
