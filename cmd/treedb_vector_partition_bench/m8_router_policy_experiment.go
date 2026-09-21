package main

// Offline, opt-in replay of ranking rules on a single owner-pinned candidate set.
// Measured coordinator requests continue using the ordinary distance-only route.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const m8RouterPolicyExperimentMethodV1 = "static_same_candidates_router_policies_w_E_C_v2"

type m8RouterPolicyCoverageV1 struct {
	DistanceMask    uint16 `json:"distance_mask"`
	FrequencyMask   uint16 `json:"frequency_mask"`
	HybridMask      uint16 `json:"hybrid_mask"`
	DistancePacks   int64  `json:"distance_packs"`
	FrequencyPacks  int64  `json:"frequency_packs"`
	HybridPacks     int64  `json:"hybrid_packs"`
	HybridLost      uint16 `json:"hybrid_lost"`
	HybridGained    uint16 `json:"hybrid_gained"`
	FrequencyLost   uint16 `json:"frequency_lost"`
	FrequencyGained uint16 `json:"frequency_gained"`
}

type m8RouterPolicyOutcomeV1 struct {
	Status     string                                              `json:"status"`
	Comparison collections.VectorPartitionRouterPolicyComparisonV1 `json:"comparison"`
	Coverage   *m8RouterPolicyCoverageV1                           `json:"coverage,omitempty"`
}

type m8RouterPolicyQueryV1 struct {
	QuerySHA256 string                  `json:"query_sha256"`
	TruthSHA256 string                  `json:"truth_sha256"`
	Exact       m8RouterPolicyOutcomeV1 `json:"exact"`
	Approximate m8RouterPolicyOutcomeV1 `json:"approximate"`
}

type m8RouterPolicyEvidenceV1 struct {
	Method            string                  `json:"method"`
	RequestedWidth    int                     `json:"requested_width"`
	EffectiveWidth    int                     `json:"effective_width"`
	RouterScoreBudget int                     `json:"router_score_budget"`
	Queries           []m8RouterPolicyQueryV1 `json:"queries"`
}

type m8RouterPolicyCacheV1 struct {
	requestedWidth, width, beam, approximateBudget int
	// One entry per probe count; neither local EF nor serving concurrency changes
	// the representative candidate operation. Evidence is immutable once cached.
	byProbes map[int]*m8RouterPolicyEvidenceV1
}

func m8RouterPolicyDiagnosticShapeV1(representatives, requestedWidth, approximateBudget int) (width, beam int) {
	beam = min(representatives, defaultRouterPolicyDiagnosticBeamV1)
	if requestedWidth == 0 {
		beam = min(beam, approximateBudget)
		width = min(beam, defaultRouterPolicyDiagnosticWidthV1)
	} else {
		width = requestedWidth
		beam = min(beam, max(approximateBudget, width))
	}
	return width, beam
}

func (h *m8AttributionHarnessV1) enableRouterPoliciesV1(width, approximateBudget int) error {
	if h == nil || h.quality == nil || h.policies != nil || h.assets == nil || h.assets.router == nil {
		return errors.New("router policy experiment requires one initialized quality owner")
	}
	n := int(h.assets.status.Representatives)
	effective, beam := m8RouterPolicyDiagnosticShapeV1(n, width, approximateBudget)
	if approximateBudget < 1 || width < 0 || effective < 1 || effective > beam || beam > n {
		return errors.New("invalid policy experiment effective width/budget")
	}
	h.policies = &m8RouterPolicyCacheV1{requestedWidth: width, width: effective, beam: beam, approximateBudget: approximateBudget, byProbes: make(map[int]*m8RouterPolicyEvidenceV1)}
	return nil
}

func m8RouterPolicyDomainsV1(rows []collections.VectorPartitionRouterPolicyDomainV1) []uint32 {
	out := make([]uint32, len(rows))
	for i, r := range rows {
		out[i] = r.Domain
	}
	return out
}

func (h *m8AttributionHarnessV1) routerPolicyOutcomeV1(ctx context.Context, i, probes int, query []float32, mode string) (m8RouterPolicyOutcomeV1, error) {
	p := h.policies
	budget := p.approximateBudget
	if mode == collections.VectorPartitionRouterModeExactV1 {
		budget = int(h.assets.status.Representatives)
	}
	c, err := h.assets.router.CompareRankingPoliciesForDiagnosticsV1(ctx, query, collections.VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: mode, ScoreBudget: budget, ReturnedWidth: p.width, BeamWidth: p.beam, PartitionProbes: probes})
	out := m8RouterPolicyOutcomeV1{Status: "pass", Comparison: c}
	if err != nil {
		if errors.Is(err, collections.ErrVectorPartitionRouterCandidateCoverageV1) {
			out.Status = m8ProductionCandidateCoverageShortfallV1
			return out, nil
		}
		if errors.Is(err, collections.ErrVectorPartitionRouterScoreBudget) {
			out.Status = m8ProductionRouterScoreBudgetExhaustedV1
			return out, nil
		}
		return out, err
	}
	masks, costs := h.quality.queries[i].masks, h.quality.packCosts
	coverage := &m8RouterPolicyCoverageV1{}
	coverage.DistanceMask, coverage.DistancePacks, err = m8RouteCoverageV1(masks, costs, m8RouterPolicyDomainsV1(c.Distance))
	if err != nil {
		return out, err
	}
	coverage.FrequencyMask, coverage.FrequencyPacks, err = m8RouteCoverageV1(masks, costs, m8RouterPolicyDomainsV1(c.Frequency))
	if err != nil {
		return out, err
	}
	coverage.HybridMask, coverage.HybridPacks, err = m8RouteCoverageV1(masks, costs, m8RouterPolicyDomainsV1(c.Hybrid))
	if err != nil {
		return out, err
	}
	coverage.HybridLost = coverage.DistanceMask &^ coverage.HybridMask
	coverage.HybridGained = coverage.HybridMask &^ coverage.DistanceMask
	coverage.FrequencyLost = coverage.DistanceMask &^ coverage.FrequencyMask
	coverage.FrequencyGained = coverage.FrequencyMask &^ coverage.DistanceMask
	out.Coverage = coverage
	return out, nil
}

func (h *m8AttributionHarnessV1) routerPolicyEvidenceV1(ctx context.Context, queries [][]float64, truth [][]m8CanonicalResultV1, probes, approximateBudget int) (*m8RouterPolicyEvidenceV1, error) {
	if h.policies == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h.quality == nil || len(queries) != len(h.quality.queries) || len(truth) != len(queries) || probes < 1 || probes > len(h.quality.packCosts) || approximateBudget != h.policies.approximateBudget {
		return nil, errors.New("router policy cache coordinate mismatch")
	}
	// Identity is checked even on a cache hit. No changed query/truth population
	// can borrow another probe/EF cell's deterministic receipt.
	for i, q := range queries {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if m8QualityQueryDigestV1(m8Query32V1(q)) != h.quality.queries[i].querySHA256 || m8QualityTruthDigestV1(truth[i]) != h.quality.queries[i].truthSHA256 {
			return nil, errors.New("router policy query/truth changed")
		}
	}
	if cached := h.policies.byProbes[probes]; cached != nil {
		return cached, nil
	}
	result := &m8RouterPolicyEvidenceV1{Method: m8RouterPolicyExperimentMethodV1, RequestedWidth: h.policies.requestedWidth, EffectiveWidth: h.policies.width, RouterScoreBudget: approximateBudget, Queries: make([]m8RouterPolicyQueryV1, len(queries))}
	for i, q64 := range queries {
		q := m8Query32V1(q64)
		record := &result.Queries[i]
		record.QuerySHA256 = h.quality.queries[i].querySHA256
		record.TruthSHA256 = h.quality.queries[i].truthSHA256
		var err error
		record.Exact, err = h.routerPolicyOutcomeV1(ctx, i, probes, q, collections.VectorPartitionRouterModeExactV1)
		if err != nil {
			return nil, err
		}
		record.Approximate, err = h.routerPolicyOutcomeV1(ctx, i, probes, q, collections.VectorPartitionRouterModeApproxV1)
		if err != nil {
			return nil, err
		}
	}
	h.policies.byProbes[probes] = result
	return result, nil
}

func m8RouterPolicyLessV1(a, b collections.VectorPartitionRouterPolicyDomainV1, frequency bool) bool {
	if frequency && a.Frequency != b.Frequency {
		return a.Frequency > b.Frequency
	}
	if a.Distance != b.Distance {
		return a.Distance < b.Distance
	}
	return a.Domain < b.Domain
}

// Shape and internal equations are checked here; authoritative source/model,
// candidate scores, votes and route masks are checked by fresh-owner replay.
func m8ValidateRouterPolicyEvidenceV1(e *m8RouterPolicyEvidenceV1, q *m8QualityAttributionV1, k, probes, samples int) error {
	if e == nil || q == nil || samples < 1 || q.Domains < 1 || q.Domains > maxPartitions || len(q.PackCosts) != q.Domains || e.Method != m8RouterPolicyExperimentMethodV1 || len(e.Queries) != samples || len(q.Queries) != samples || k < 1 || k > 10 || probes < 1 || probes > q.Domains || e.RequestedWidth < 0 || e.EffectiveWidth < 1 || e.RouterScoreBudget < 1 || e.RequestedWidth != 0 && e.RequestedWidth != e.EffectiveWidth {
		return errors.New("invalid policy experiment shape/identity")
	}
	full := uint16(1<<uint(k)) - 1
	for i, row := range e.Queries {
		base := q.Queries[i]
		if row.QuerySHA256 != base.QuerySHA256 || row.TruthSHA256 != base.TruthSHA256 || row.Exact.Comparison.QuerySHA256 != row.Approximate.Comparison.QuerySHA256 {
			return errors.New("policy experiment query/truth mismatch")
		}
		for _, which := range []struct {
			mode string
			out  m8RouterPolicyOutcomeV1
		}{{"exact", row.Exact}, {"approximate", row.Approximate}} {
			out, c := which.out, which.out.Comparison
			if c.Method != collections.VectorPartitionRouterPolicyDiagnosticMethodV1 || c.Mode != which.mode || c.Generation != q.Generation || c.SourceGeneration != q.SourceGeneration || c.ModelSHA256 != q.ModelSHA256 || !m8SHA256V1(c.QuerySHA256) || c.DomainCount != q.Domains || c.RepresentativeCount < c.DomainCount || c.RepresentativeCount > vectorpartition.DefaultRouterConfigV1().MaxRepresentatives || c.ScoreBudget < 1 || c.ReturnedWidth != e.EffectiveWidth || c.ReturnedWidth > c.BeamWidth || c.BeamWidth > c.RepresentativeCount || c.ScoreCalls > uint64(c.ScoreBudget) || c.ScoreCalls < c.Candidates || c.Probes != probes {
				return errors.New("policy candidate identity/work mismatch")
			}
			if which.mode == "exact" {
				if c.ScoreBudget != c.RepresentativeCount || c.Collected != c.RepresentativeCount || c.Candidates != uint64(c.RepresentativeCount) || c.Edges != 0 {
					return errors.New("policy exact scan work incomplete")
				}
			} else if c.ScoreBudget != e.RouterScoreBudget {
				return errors.New("policy approximate budget mismatch")
			}
			if out.Status == m8ProductionRouterScoreBudgetExhaustedV1 {
				if which.mode != collections.VectorPartitionRouterModeApproxV1 || out.Coverage != nil || c.CollectionComplete || c.ScoreCalls != uint64(c.ScoreBudget) || c.Collected != 0 || c.Candidates != 0 || c.Edges != 0 || c.UniqueReturned != 0 || c.CandidateSetSHA256 != "" || c.CandidateSequenceSHA256 != "" || len(c.Distance)+len(c.Frequency)+len(c.Hybrid) != 0 {
					return errors.New("policy score-budget refusal contains partial results")
				}
				continue
			}
			if !c.CollectionComplete || c.Collected < 1 || c.Collected > c.RepresentativeCount || c.Collected > c.ScoreBudget || c.Candidates < uint64(c.Collected) || c.Candidates > uint64(c.ScoreBudget) {
				return errors.New("policy candidate collection is incomplete")
			}
			if out.Status == m8ProductionCandidateCoverageShortfallV1 {
				if out.Coverage != nil || len(c.Distance)+len(c.Frequency)+len(c.Hybrid) != 0 || !m8SHA256V1(c.CandidateSetSHA256) || !m8SHA256V1(c.CandidateSequenceSHA256) || c.UniqueReturned < 1 || c.UniqueReturned > c.ReturnedWidth || c.UniqueReturned > c.Collected {
					return errors.New("policy shortfall contains partial results")
				}
				continue
			}
			if out.Status != "pass" || out.Coverage == nil || c.UniqueReturned < probes || c.UniqueReturned > c.ReturnedWidth || c.UniqueReturned > c.Collected || !m8SHA256V1(c.CandidateSetSHA256) || !m8SHA256V1(c.CandidateSequenceSHA256) || len(c.Distance) != probes || len(c.Frequency) != probes || len(c.Hybrid) != probes {
				return errors.New("invalid policy success")
			}
			// Shared domain facts may occur in different prefixes but cannot disagree.
			facts := make(map[uint32]collections.VectorPartitionRouterPolicyDomainV1, 3*probes)
			for routeIndex, route := range [][]collections.VectorPartitionRouterPolicyDomainV1{c.Distance, c.Frequency, c.Hybrid} {
				seen := make(map[uint32]bool, len(route))
				for j, d := range route {
					if int(d.Domain) >= q.Domains || seen[d.Domain] || d.Frequency < 1 || d.Frequency > c.UniqueReturned || d.WinningRepresentative < 0 || d.WinningRepresentative >= c.RepresentativeCount || math.IsNaN(d.Distance) || math.IsInf(d.Distance, 0) {
						return errors.New("invalid policy domain record")
					}
					seen[d.Domain] = true
					if old, ok := facts[d.Domain]; ok && old != d {
						return errors.New("policy reducers disagree on same candidate facts")
					}
					facts[d.Domain] = d
					if j > 0 && routeIndex < 2 && !m8RouterPolicyLessV1(route[j-1], d, routeIndex == 1) {
						return errors.New("noncanonical policy order")
					}
				}
			}
			expected := []collections.VectorPartitionRouterPolicyDomainV1{c.Frequency[0]}
			for _, d := range c.Distance {
				if len(expected) == probes {
					break
				}
				if d.Domain != expected[0].Domain {
					expected = append(expected, d)
				}
			}
			if !slices.Equal(expected, c.Hybrid) {
				return errors.New("hybrid swapped instead of prepended winner")
			}
			cv := out.Coverage
			for _, r := range []struct {
				route []collections.VectorPartitionRouterPolicyDomainV1
				mask  uint16
				cost  int64
			}{{c.Distance, cv.DistanceMask, cv.DistancePacks}, {c.Frequency, cv.FrequencyMask, cv.FrequencyPacks}, {c.Hybrid, cv.HybridMask, cv.HybridPacks}} {
				if r.mask&^full != 0 {
					return errors.New("policy truth mask out of range")
				}
				// Route identity/uniqueness was checked above. Sum its real pack
				// costs directly; no domain-sized zero-mask allocation is needed.
				var cost int64
				for _, d := range r.route {
					if q.PackCosts[d.Domain] < 1 {
						return errors.New("invalid policy physical pack cost")
					}
					next, err := memoryAdd(cost, q.PackCosts[d.Domain])
					if err != nil {
						return err
					}
					cost = next
				}
				if cost != r.cost {
					return errors.New("policy omitted physical pack cost")
				}
				hd, err := base.DomainCost.hitsAtBudget(int64(probes))
				if err != nil {
					return err
				}
				hp, err := base.PackCost.hitsAtBudget(cost)
				if err != nil || bits.OnesCount16(r.mask) > min(hd, hp) {
					return errors.New("policy route exceeds independent coverage ceiling")
				}
			}
			if cv.HybridLost != cv.DistanceMask&^cv.HybridMask || cv.HybridGained != cv.HybridMask&^cv.DistanceMask || cv.FrequencyLost != cv.DistanceMask&^cv.FrequencyMask || cv.FrequencyGained != cv.FrequencyMask&^cv.DistanceMask {
				return errors.New("policy signed changes mismatch")
			}
		}
	}
	return nil
}

func m8RouterPolicyEvidenceSelectionV1(cfg m8ProductionConfigEvidenceV1, routerRepresentatives int, row m8ProductionRowV1) error {
	e := row.Attribution.RouterPolicies
	if !cfg.RouterPolicyDiagnostics {
		if cfg.RouterPolicyWidth != 0 || e != nil {
			return errors.New("unselected router policy evidence")
		}
		return nil
	}
	effectiveWidth, diagnosticBeam := m8RouterPolicyDiagnosticShapeV1(routerRepresentatives, cfg.RouterPolicyWidth, row.Attribution.ApproximateRouterScoreBudget)
	if !cfg.QualityDiagnostics || routerRepresentatives < 1 || cfg.RouterPolicyWidth < 0 || effectiveWidth < 1 || effectiveWidth > diagnosticBeam {
		return errors.New("invalid router policy configuration")
	}
	if row.Accounting == nil && row.Status != "pass" && row.Status != "fail" && !m8ProductionRouterRefusalStatusV1(row.Status) {
		if e != nil {
			return errors.New("unexecuted row has policy observations")
		}
		return nil
	}
	if err := m8ValidateRouterPolicyEvidenceV1(e, row.Attribution.Quality, cfg.TopK, row.Probes, row.Samples); err != nil {
		return err
	}
	if e.RequestedWidth != cfg.RouterPolicyWidth || e.EffectiveWidth != effectiveWidth || e.RouterScoreBudget != row.Attribution.ApproximateRouterScoreBudget {
		return errors.New("policy requested/effective config mismatch")
	}
	for _, query := range e.Queries {
		if query.Exact.Comparison.RepresentativeCount != routerRepresentatives ||
			query.Approximate.Comparison.RepresentativeCount != routerRepresentatives ||
			query.Exact.Comparison.BeamWidth != diagnosticBeam || query.Approximate.Comparison.BeamWidth != diagnosticBeam {
			return errors.New("policy beam config mismatch")
		}
	}
	return nil
}

func m8RouterPolicyReplayEqualV1(a, b *m8RouterPolicyEvidenceV1) bool { return reflect.DeepEqual(a, b) }

// Charge diagnostic work independently from serving and without charging a full
// Q*N corpus clone. Exact representative scans and bounded reducer sorting use
// a conservative per-model upper bound; no new public request work is included.
func m8PlanRouterPolicyDiagnosticsV1(cfg config, m fixtureManifest, domainCounts []int, capUnits, capBytes int64) (int64, int64, error) {
	if !cfg.m8RouterPolicyDiagnostics {
		if cfg.m8RouterPolicyWidth != 0 {
			return 0, 0, errors.New("policy width requires diagnostics")
		}
		return 0, 0, nil
	}
	if len(cfg.m8RouterPolicyRepresentativeCounts) > 0 && len(cfg.m8RouterPolicyRepresentativeCounts) != len(domainCounts) {
		return 0, 0, errors.New("policy model-count preflight cardinality mismatch")
	}
	if !cfg.m8QualityDiagnostics || cfg.topK < 1 || cfg.topK > 10 || cfg.routerCandidates < 1 || cfg.m8RouterPolicyWidth < 0 || cfg.m8RouterPolicyWidth > defaultRouterPolicyDiagnosticBeamV1 || m.Vectors < 1 || m.Queries < 1 || m.Dimensions < 1 || m.Dimensions > maxDimensions || len(cfg.probes) < 1 || len(cfg.efSearch) < 1 || len(cfg.concurrency) < 1 || capUnits < 1 || capBytes < 1 {
		return 0, 0, errors.New("invalid policy diagnostic work shape")
	}
	if len(domainCounts) == 0 {
		return 0, 0, errors.New("selected router policy diagnostics have no model shape")
	}
	// Collector/reducer results are cached per probe, but routerPolicyEvidenceV1
	// revalidates the whole query/truth population BEFORE every cache lookup.
	// Producer: P*E checks. Strict replay: at most 2*P*E*C checks, from the
	// explicit policy replay and m8BuildAttributionV1 (including failed rows).
	// Bound the larger traversal, not their sum: these are separate executions.
	// Count conversion/scalar work plus hash input bytes conservatively; these
	// units are an admission envelope, not a claim about CPU cycles.
	populationChecks, err := memoryMul(2, int64(m.Queries), int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(len(cfg.concurrency)), int64(max(1, cfg.m8MeasuredRepetitions)))
	if err != nil {
		return 0, 0, err
	}
	queryIdentityWork, err := memoryMul(8, int64(m.Dimensions))
	if err != nil {
		return 0, 0, err
	}
	truthIdentityWork, err := memoryMul(int64(cfg.topK), documentIDStorageBytes+12)
	if err != nil {
		return 0, 0, err
	}
	identityWork, err := memoryAdd(256, queryIdentityWork, truthIdentityWork)
	if err != nil {
		return 0, 0, err
	}
	populationWork, err := memoryMul(populationChecks, identityWork)
	if err != nil {
		return 0, 0, err
	}
	// Include dimension-sized conversion/normalization scratch independently
	// of model N; a small model can still have a large source dimension.
	queryScratch, err := memoryMul(16, int64(m.Dimensions))
	if err != nil {
		return 0, 0, err
	}
	var work, peak int64
	for variant, d := range domainCounts {
		if d < 1 || d > cfg.partitions {
			return 0, 0, errors.New("invalid policy domain count")
		}
		n, err := m8RouterPolicyRepresentativeBoundV1(cfg, variant, d, m.Vectors)
		if err != nil {
			return 0, 0, err
		}
		calls, err := memoryMul(2, int64(m.Queries), int64(len(cfg.probes)))
		if err != nil {
			return 0, 0, err
		}
		// O(n log n) comparisons, candidate hashing, normalization and vector scores.
		// Legacy capped routing skips upper layers. At most C distinct L0 rows
		// expand, each bounded by N native plus at most nine auxiliary edges.
		// Use this conservative bound for retained graphs rather than assuming
		// a particular M that their actual header may not select.
		logN := int64(bits.Len64(uint64(n)))
		perCall, err := memoryMul(n, 8*logN+int64(m.Dimensions)+64)
		if err != nil {
			return 0, 0, err
		}
		edgeWork, err := memoryMul(min(n, int64(cfg.routerCandidates)), n+9)
		if err != nil {
			return 0, 0, err
		}
		perCall, err = memoryAdd(perCall, edgeWork)
		if err != nil {
			return 0, 0, err
		}
		w, err := memoryMul(calls, perCall)
		if err != nil {
			return 0, 0, err
		}
		work, err = memoryAdd(work, w, populationWork)
		if err != nil {
			return 0, 0, err
		}
		scratch, err := memoryMul(n, 1024)
		if err != nil {
			return 0, 0, err
		}
		// Conservative resident/cache plus repeated JSON report/row copies. Cached
		// values are shared read-only, but encoding can own its own byte buffers.
		records, err := memoryMul(int64(m.Queries), int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(len(cfg.concurrency)), int64(max(1, cfg.m8MeasuredRepetitions)))
		if err != nil {
			return 0, 0, err
		}
		// Preserve the per-row validation/copy/serialization bound separately
		// from the population identity rechecks above. A cached candidate set
		// removes search work, not retained receipt validation or encoding.
		truthWork, err := memoryMul(16, int64(cfg.topK), documentIDStorageBytes+8)
		if err != nil {
			return 0, 0, err
		}
		routeWork, err := memoryMul(512, int64(d))
		if err != nil {
			return 0, 0, err
		}
		recordWork, err := memoryAdd(truthWork, routeWork, 4096)
		if err != nil {
			return 0, 0, err
		}
		recordWork, err = memoryMul(records, recordWork)
		if err != nil {
			return 0, 0, err
		}
		work, err = memoryAdd(work, recordWork)
		if err != nil {
			return 0, 0, err
		}
		recordBytes, err := memoryMul(records, 4, 4096+int64(d)*3*256)
		if err != nil {
			return 0, 0, err
		}
		bytes, err := memoryAdd(scratch, queryScratch, recordBytes)
		if err != nil {
			return 0, 0, err
		}
		peak = max(peak, bytes)
	}
	if work > capUnits || peak > capBytes {
		return work, peak, fmt.Errorf("router policy diagnostics exceed resource cap: work=%d bytes=%d", work, peak)
	}
	return work, peak, nil
}

func m8RouterPolicyRepresentativeBoundV1(cfg config, variant, domains, sourceRows int) (int64, error) {
	if domains < 1 || sourceRows < 1 {
		return 0, errors.New("invalid policy model bound")
	}
	if len(cfg.m8RouterPolicyRepresentativeCounts) > 0 {
		if variant < 0 || variant >= len(cfg.m8RouterPolicyRepresentativeCounts) {
			return 0, errors.New("missing retained policy model count")
		}
		n := cfg.m8RouterPolicyRepresentativeCounts[variant]
		if n < domains || n > vectorpartition.DefaultRouterConfigV1().MaxRepresentatives {
			return 0, errors.New("invalid retained policy model count")
		}
		return int64(n), nil
	}
	if cfg.m8ExistingDB != "" || len(cfg.m8VariantDBs) > 0 {
		return 0, errors.New("retained policy diagnostics require actual representative counts")
	}
	// The new-assets M8 builder uses the validated CLI router configuration.
	// Derive its exact bound; never assume a configured bound for a retained model.
	bound := int64(cfg.routerConfig.RepresentativeBudget)
	return min(bound, 2*int64(sourceRows)-int64(domains)), nil
}
