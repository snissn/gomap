package main

// R3 exact offline model comparisons share frozen membership/source and the R1
// ranking reducers. They are not timings or recalls for a deployed new policy.
import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"

	"github.com/snissn/gomap/TreeDB/collections"
)

const m8RouterRepresentationMethodV1 = "static_router_representation_comparison_v1"

type m8RouterRepresentationArmV1 struct {
	Comparison    collections.VectorPartitionRouterRepresentationResultV1 `json:"comparison"`
	Coverage      *m8RouterPolicyCoverageV1                               `json:"coverage,omitempty"`
	VersusControl *m8RouterEffortDeltaV1                                  `json:"versus_control,omitempty"`
	VersusExact   *m8RouterEffortDeltaV1                                  `json:"versus_original_exact,omitempty"`
}
type m8RouterRepresentationTruthDomainV1 struct {
	Domain             uint32  `json:"domain"`
	TruthMask          uint16  `json:"truth_mask"`
	NearestMemberID    string  `json:"nearest_member_id"`
	NearestMemberScore float32 `json:"nearest_member_score"`
	NearestMemberRank  int     `json:"nearest_member_rank"`
}
type m8RouterRepresentationQueryV1 struct {
	QuerySHA256  string                                `json:"query_sha256"`
	TruthSHA256  string                                `json:"truth_sha256"`
	TruthDomains []m8RouterRepresentationTruthDomainV1 `json:"truth_domains"`
	Arms         []m8RouterRepresentationArmV1         `json:"arms"`
}
type m8RouterRepresentationEvidenceV1 struct {
	Method  string                                                `json:"method"`
	Info    collections.VectorPartitionRouterRepresentationInfoV1 `json:"model_info"`
	Queries []m8RouterRepresentationQueryV1                       `json:"queries"`
}
type m8RouterRepresentationCacheV1 struct {
	owner    *collections.VectorPartitionRouterRepresentationV1
	byProbes map[int]*m8RouterRepresentationEvidenceV1
}

func m8RouterRepresentationConfigV1(cfg config) *collections.VectorPartitionRouterRepresentationOptionsV1 {
	if cfg.m8RouterRepresentation.Arm == "" {
		return nil
	}
	o := cfg.m8RouterRepresentation
	return &o
}
func m8ValidateRouterRepresentationConfigV1(cfg config) error {
	o := cfg.m8RouterRepresentation
	if o.Arm == "" {
		if o != (collections.VectorPartitionRouterRepresentationOptionsV1{}) {
			return errors.New("representation coordinates without selected arm")
		}
		return nil
	}
	if cfg.stage != m8ProductionMultiGroupModeV1 || !cfg.m8QualityDiagnostics || !cfg.m8RouterPolicyDiagnostics {
		return errors.New("representation requires production M8 quality and policy diagnostics")
	}
	if err := collections.ValidateVectorPartitionRouterRepresentationOptionsV1(o); err != nil {
		return err
	}
	width := cfg.m8RouterPolicyWidth
	if width == 0 {
		width = cfg.routerCandidates
	}
	if o.ReturnedWidth != width {
		return errors.New("representation width differs from frozen R1 control")
	}
	return nil
}
func (h *m8AttributionHarnessV1) enableRouterRepresentationV1(ctx context.Context, o collections.VectorPartitionRouterRepresentationOptionsV1) error {
	if h == nil || h.assets == nil || h.assets.router == nil || h.quality == nil || h.policies == nil || h.representation != nil {
		return errors.New("representation requires one initialized quality/policy owner")
	}
	if o.ReturnedWidth != h.policies.width {
		return errors.New("representation effective width differs from R1")
	}
	owner, err := h.assets.router.BuildRepresentationForDiagnosticsV1(ctx, h.assets.collection, o)
	if err != nil {
		return err
	}
	if err := collections.ValidateVectorPartitionRouterRepresentationInfoV1(owner.InfoV1()); err != nil {
		return err
	}
	h.representation = &m8RouterRepresentationCacheV1{owner: owner, byProbes: make(map[int]*m8RouterRepresentationEvidenceV1)}
	return nil
}
func (h *m8AttributionHarnessV1) routerRepresentationEvidenceV1(ctx context.Context, queries [][]float64, truth [][]m8CanonicalResultV1, probes, budget int) (*m8RouterRepresentationEvidenceV1, error) {
	if h.representation == nil {
		return nil, nil
	}
	// Revalidate the immutable population on each producer/replay cache lookup.
	// The work model charges these scans even when exact candidate results cache.
	controls, err := h.routerPolicyEvidenceV1(ctx, queries, truth, probes, budget)
	if err != nil {
		return nil, err
	}
	info := h.representation.owner.InfoV1()
	if controls == nil || controls.EffectiveWidth != info.Options.ReturnedWidth {
		return nil, errors.New("representation control coordinate changed")
	}
	if prior := h.representation.byProbes[probes]; prior != nil {
		return prior, nil
	}
	e := &m8RouterRepresentationEvidenceV1{Method: m8RouterRepresentationMethodV1, Info: info, Queries: make([]m8RouterRepresentationQueryV1, len(queries))}
	for i, q := range queries {
		results, err := h.representation.owner.CompareWithContextV1(ctx, m8Query32V1(q), probes)
		if err != nil {
			return nil, err
		}
		r := &e.Queries[i]
		r.QuerySHA256 = h.quality.queries[i].querySHA256
		r.TruthSHA256 = h.quality.queries[i].truthSHA256
		qc := &h.quality.queries[i]
		if len(qc.noCoarseningBest) != len(qc.masks) || len(qc.noCoarsening) != len(qc.masks) {
			return nil, errors.New("representation requires existing exact all-pack observations")
		}
		ranks := make([]int, len(qc.masks))
		for rank, d := range qc.noCoarsening {
			ranks[d] = rank + 1
		}
		for d, mask := range qc.masks {
			if mask == 0 {
				continue
			}
			best := qc.noCoarseningBest[d]
			if !best.Present {
				return nil, errors.New("truth domain lacks exact member observation")
			}
			r.TruthDomains = append(r.TruthDomains, m8RouterRepresentationTruthDomainV1{Domain: uint32(d), TruthMask: mask, NearestMemberID: best.ID, NearestMemberScore: best.Score, NearestMemberRank: ranks[d]})
		}

		r.Arms = make([]m8RouterRepresentationArmV1, len(results))
		for j, c := range results {
			a := &r.Arms[j]
			a.Comparison = c
			if c.Status != "pass" {
				continue
			}
			a.Coverage, err = m8EffortCoverageV1(h.quality.queries[i].masks, h.quality.packCosts, c.Policies)
			if err != nil {
				return nil, err
			}
			if base := r.Arms[0].Coverage; base != nil {
				a.VersusControl = m8EffortDeltaV1(base.DistanceMask, a.Coverage.DistanceMask)
			}
			if base := controls.Queries[i].Exact.Coverage; base != nil {
				a.VersusExact = m8EffortDeltaV1(base.DistanceMask, a.Coverage.DistanceMask)
			}
		}
		// The legacy-centroid control is required to reproduce the captured original
		// exact route, including its refusal. Do not compare model-specific hashes.
		c := r.Arms[0].Comparison
		old := controls.Queries[i].Exact
		if c.Status != old.Status {
			return nil, errors.New("representation control refusal differs from exact router")
		}
		if c.Status == "pass" && (!reflect.DeepEqual(c.Policies.Distance, old.Comparison.Distance) || !reflect.DeepEqual(c.Policies.Frequency, old.Comparison.Frequency) || !reflect.DeepEqual(c.Policies.Hybrid, old.Comparison.Hybrid)) {
			return nil, errors.New("representation control ranking differs from exact router")
		}
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	h.representation.byProbes[probes] = e
	return e, nil
}
func m8RepresentationCoverageSelectionV1(cfg m8ProductionConfigEvidenceV1, q *m8QualityAttributionV1, query, probes, n int, c *collections.VectorPartitionRouterEffortPoliciesV1, cv *m8RouterPolicyCoverageV1) error {
	if cv == nil || c == nil || c.Method != collections.VectorPartitionRouterPolicyDiagnosticMethodV1 || !m8SHA256V1(c.CandidateSetSHA256) || !m8SHA256V1(c.CandidateSequenceSHA256) {
		return errors.New("missing representation candidate/coverage record")
	}
	full := uint16(1<<uint(cfg.TopK)) - 1
	for kind, a := range []struct {
		r    []collections.VectorPartitionRouterPolicyDomainV1
		mask uint16
		cost int64
	}{{c.Distance, cv.DistanceMask, cv.DistancePacks}, {c.Frequency, cv.FrequencyMask, cv.FrequencyPacks}, {c.Hybrid, cv.HybridMask, cv.HybridPacks}} {
		if len(a.r) != probes || a.mask&^full != 0 {
			return errors.New("representation route shape")
		}
		seen := make(map[uint32]bool, len(a.r))
		var cost int64
		votes := 0
		for j, d := range a.r {
			if int(d.Domain) >= q.Domains || seen[d.Domain] || math.IsNaN(d.Distance) || math.IsInf(d.Distance, 0) || d.Frequency < 1 || d.WinningRepresentative < 0 || d.WinningRepresentative >= n {
				return errors.New("invalid representation route member")
			}
			seen[d.Domain] = true
			votes += d.Frequency
			var err error
			cost, err = memoryAdd(cost, q.PackCosts[d.Domain])
			if err != nil {
				return err
			}
			if j > 0 && (kind != 2 || j > 1) && m8RouterPolicyLessV1(d, a.r[j-1], kind == 1) {
				return errors.New("representation route order")
			}
		}
		if cost != a.cost || votes > c.UniqueReturned {
			return errors.New("representation votes/pack cost")
		}
		dh, err := q.Queries[query].DomainCost.hitsAtBudget(int64(probes))
		if err != nil {
			return err
		}
		ph, err := q.Queries[query].PackCost.hitsAtBudget(cost)
		if err != nil || bits.OnesCount16(a.mask) > min(dh, ph) {
			return errors.New("representation exceeds available coverage ceiling")
		}
	}
	if c.Hybrid[0] != c.Frequency[0] || cv.HybridLost != cv.DistanceMask&^cv.HybridMask || cv.HybridGained != cv.HybridMask&^cv.DistanceMask || cv.FrequencyLost != cv.DistanceMask&^cv.FrequencyMask || cv.FrequencyGained != cv.FrequencyMask&^cv.DistanceMask {
		return errors.New("representation policy delta mismatch")
	}
	return nil
}
func m8RouterRepresentationEvidenceSelectionV1(cfg m8ProductionConfigEvidenceV1, row m8ProductionRowV1) error {
	e, q, p := row.Attribution.RouterRepresentation, row.Attribution.Quality, row.Attribution.RouterPolicies
	if cfg.RouterRepresentation == nil {
		if e != nil {
			return errors.New("unselected representation evidence")
		}
		return nil
	}
	if !cfg.QualityDiagnostics || !cfg.RouterPolicyDiagnostics {
		return errors.New("representation evidence without controls")
	}
	if row.Status != "pass" && row.Status != "fail" && row.Status != "candidate_coverage_shortfall" {
		if e != nil {
			return errors.New("unexecuted representation observations")
		}
		return nil
	}
	if e == nil || q == nil || p == nil || e.Method != m8RouterRepresentationMethodV1 || e.Info.Options != *cfg.RouterRepresentation || e.Info.Options.ReturnedWidth != p.EffectiveWidth || len(e.Queries) != row.Samples || len(q.Queries) != row.Samples || len(p.Queries) != row.Samples || row.Samples < 1 || cfg.TopK < 1 || cfg.TopK > 10 {
		return errors.New("representation selection/population mismatch")
	}
	if err := collections.ValidateVectorPartitionRouterRepresentationInfoV1(e.Info); err != nil {
		return err
	}
	if e.Info.Generation != q.Generation || e.Info.SourceGeneration != q.SourceGeneration || e.Info.OriginalModelSHA256 != q.ModelSHA256 || len(e.Info.Quotas) != q.Domains || len(q.PackCosts) != q.Domains {
		return errors.New("representation source owner mismatch")
	}
	for i, r := range e.Queries {
		if r.QuerySHA256 != q.Queries[i].QuerySHA256 || r.TruthSHA256 != q.Queries[i].TruthSHA256 || len(r.Arms) != len(e.Info.Models) {
			return errors.New("representation query/model population mismatch")
		}
		if len(r.TruthDomains) < 1 || len(r.TruthDomains) > q.Domains {
			return errors.New("missing truth-domain explanation")
		}
		full := uint16(1<<uint(cfg.TopK)) - 1
		var covered uint16
		for j, d := range r.TruthDomains {
			if int(d.Domain) >= q.Domains || j > 0 && d.Domain <= r.TruthDomains[j-1].Domain || d.TruthMask == 0 || d.TruthMask&^full != 0 || d.NearestMemberID == "" || len(d.NearestMemberID) > documentIDStorageBytes || math.IsNaN(float64(d.NearestMemberScore)) || math.IsInf(float64(d.NearestMemberScore), 0) || d.NearestMemberRank < 1 || d.NearestMemberRank > q.Domains {
				return errors.New("invalid truth-domain explanation")
			}
			covered |= d.TruthMask
		}
		if covered != full {
			return errors.New("truth-domain explanation omits canonical truth")
		}
		for j, a := range r.Arms {
			c, m := a.Comparison, e.Info.Models[j]
			if c.ModelSHA256 != m.ModelSHA256 || c.Geometry != m.Geometry || c.QuerySHA256 != p.Queries[i].Exact.Comparison.QuerySHA256 || c.ReturnedWidth != p.EffectiveWidth || c.Probes != row.Probes {
				return errors.New("representation comparison identity mismatch")
			}
			if c.Status == "returned_width_exceeds_model" {
				if c.ReturnedWidth <= m.Representatives || c.Policies != nil || len(c.FullExactDomainPriorities) != 0 || c.ScoreInvocations != 0 || c.VectorBytes != 0 || a.Coverage != nil || a.VersusControl != nil || a.VersusExact != nil {
					return errors.New("underfilled representation hides work/partial result")
				}
				continue
			}
			if c.ReturnedWidth > m.Representatives || c.ScoreInvocations != uint64(m.Representatives) || c.VectorBytes != uint64(m.Representatives)*uint64(m.Dimensions)*8 || c.Policies == nil || c.Policies.InputCount != m.Representatives || c.Policies.UniqueReturned != c.ReturnedWidth || !m8SHA256V1(c.Policies.CandidateSetSHA256) || !m8SHA256V1(c.Policies.CandidateSequenceSHA256) {
				return errors.New("representation full-scan work/candidate identity mismatch")
			}
			if len(c.FullExactDomainPriorities) != q.Domains {
				return errors.New("missing full exact representative domain ranks")
			}
			seen := make(map[uint32]bool, q.Domains)
			for rank, d := range c.FullExactDomainPriorities {
				if int(d.Domain) >= q.Domains || seen[d.Domain] || d.WinningRepresentative < 0 || d.WinningRepresentative >= m.Representatives || math.IsNaN(d.Distance) || math.IsInf(d.Distance, 0) || d.Frequency != 0 || rank > 0 && m8RouterPolicyLessV1(d, c.FullExactDomainPriorities[rank-1], false) {
					return errors.New("invalid full exact representative rank")
				}
				node := m.RepresentedNodes[d.WinningRepresentative]
				if node.Domain != d.Domain || node.SourceAnchor != d.WinningSourceOrdinal {
					return errors.New("winning representative metadata mismatch")
				}
				seen[d.Domain] = true
			}
			if c.Status == "candidate_coverage_shortfall" {
				if len(c.Policies.Distance)+len(c.Policies.Frequency)+len(c.Policies.Hybrid) != 0 || a.Coverage != nil || a.VersusControl != nil || a.VersusExact != nil {
					return errors.New("representation refusal returned partial route")
				}
				continue
			}
			if c.Status != "pass" {
				return errors.New("unknown representation outcome")
			}
			if err := m8RepresentationCoverageSelectionV1(cfg, q, i, row.Probes, m.Representatives, c.Policies, a.Coverage); err != nil {
				return err
			}
			var base, exact *m8RouterEffortDeltaV1
			if old := r.Arms[0].Coverage; old != nil {
				base = m8EffortDeltaV1(old.DistanceMask, a.Coverage.DistanceMask)
			}
			if old := p.Queries[i].Exact.Coverage; old != nil {
				exact = m8EffortDeltaV1(old.DistanceMask, a.Coverage.DistanceMask)
			}
			if !reflect.DeepEqual(base, a.VersusControl) || !reflect.DeepEqual(exact, a.VersusExact) {
				return errors.New("representation versus-control deltas changed")
			}
		}
		if r.Arms[0].Comparison.Status != p.Queries[i].Exact.Status {
			return errors.New("representation control outcome changed")
		}
	}
	return nil
}

// Build has its own named, explicit work cap per opened variant; it is not
// silently admitted under the existing query-pass cap. Runtime validates the
// source-derived bound before copying rows. Query/replay work still contributes
// to the ordinary aggregate M8 diagnostic cap. All builds are serial.
func m8PlanRouterRepresentationV1(cfg config, m fixtureManifest, domains []int, capWork, capBytes int64) (work, peak, build int64, err error) {
	if err = m8ValidateRouterRepresentationConfigV1(cfg); err != nil {
		return
	}
	if cfg.m8RouterRepresentation.Arm == "" {
		return
	}
	if len(domains) == 0 || m.Queries < 1 || m.Dimensions < 1 || m.Dimensions > maxDimensions || m.Vectors < 1 || len(cfg.probes) == 0 || len(cfg.efSearch) == 0 || len(cfg.concurrency) == 0 || capWork < 1 || capBytes < 1 {
		return 0, 0, 0, errors.New("invalid representation work shape")
	}
	o := cfg.m8RouterRepresentation
	arms := int64(2)
	if o.Arm == "centroid_geometry" {
		arms = 3
	}
	build, err = memoryMul(int64(o.MaxBuildWork), int64(len(domains)))
	if err != nil {
		return
	}
	calls, err := memoryMul(int64(m.Queries), int64(len(cfg.probes)))
	if err != nil {
		return
	}
	records, err := memoryMul(calls, int64(len(cfg.efSearch)), int64(len(cfg.concurrency)))
	if err != nil {
		return
	}
	for variant, d := range domains {
		n, e := m8RouterPolicyRepresentativeBoundV1(cfg, variant, d, m.Vectors)
		if e != nil {
			return 0, 0, 0, e
		}
		if int64(o.ReturnedWidth) > n {
			return 0, 0, 0, errors.New("representation control width exceeds model bound")
		}
		// Centroid norm/dot/priority and nearest-width/reducer/hash work. Geometry
		// arms all pay a full exact scan; underfill refusal cannot increase this bound.
		per, e := memoryMul(arms, n, 8*int64(m.Dimensions)+16*int64(bits.Len(uint(n)))+512)
		if e != nil {
			return 0, 0, 0, e
		}
		full, e := memoryMul(calls, per)
		if e != nil {
			return 0, 0, 0, e
		}
		bookkeeping, e := memoryMul(records, 4, 16*int64(m.Dimensions)+int64(cfg.topK)*(documentIDStorageBytes+16)+arms*(8192+int64(d)*2048))
		if e != nil {
			return 0, 0, 0, e
		}
		infoCopies, e := memoryMul(int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(len(cfg.concurrency)), 4, arms, n, 1024)
		if e != nil {
			return 0, 0, 0, e
		}
		work, e = memoryAdd(work, full, bookkeeping, infoCopies)
		if e != nil {
			return 0, 0, 0, e
		}
		retained, e := memoryMul(records, 4, arms*(8192+int64(d)*2048))
		if e != nil {
			return 0, 0, 0, e
		}
		scratch, e := memoryMul(n, 1024)
		if e != nil {
			return 0, 0, 0, e
		}
		bytes, e := memoryAdd(int64(o.MaxBuildBytes), retained, scratch, infoCopies)
		if e != nil {
			return 0, 0, 0, e
		}
		peak = max(peak, bytes)
	}
	if work > capWork || peak > capBytes {
		return work, peak, build, fmt.Errorf("representation exceeds query/heap envelope: query_work=%d bytes=%d separately_declared_build_work=%d", work, peak, build)
	}
	return
}
