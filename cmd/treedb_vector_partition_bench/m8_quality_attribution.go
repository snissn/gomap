package main

// This file extends the existing, static-generation M8 attribution pass. It is
// deliberately not a serving router or a second source-of-truth implementation.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/bits"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	m8QualityAttributionMethodV1       = "static_truth_cost_no_coarsening_v1"
	m8QualityTraceMaxQueriesV1         = 8
	m8QualityTraceMaxBytesV1     int64 = 64 << 20
	m8QualityTraceMaxEventsV1    int64 = 1 << 20
)

// A nil Actual means the local stage was not executed. Within a measured Actual,
// nil Scored/Retained means it was not sampled, rather than zero observed hits.
type m8QualityQueryV1 struct {
	QuerySHA256               string                  `json:"query_sha256"`
	TruthSHA256               string                  `json:"truth_sha256"`
	DomainCost                m8CoverageCurveV1       `json:"domain_cost"`
	PackCost                  m8CoverageCurveV1       `json:"pack_cost"`
	NoCoarseningDomains       []uint32                `json:"no_coarsening_domains"`
	NoCoarseningMask          uint16                  `json:"no_coarsening_mask"`
	NoCoarseningPacks         int64                   `json:"no_coarsening_packs"`
	ExactDomains              []uint32                `json:"exact_representative_domains"`
	ExactMask                 uint16                  `json:"exact_representative_mask"`
	ExactPacks                int64                   `json:"exact_representative_packs"`
	ApproximateDomains        []uint32                `json:"approximate_representative_domains,omitempty"`
	ApproximateMask           *uint16                 `json:"approximate_representative_mask,omitempty"`
	ApproximatePacks          int64                   `json:"approximate_representative_packs"`
	NoCoarseningToExactLost   uint16                  `json:"no_coarsening_to_exact_lost"`
	NoCoarseningToExactGained uint16                  `json:"no_coarsening_to_exact_gained"`
	ExactToApproximateLost    *uint16                 `json:"exact_to_approximate_lost,omitempty"`
	ExactToApproximateGained  *uint16                 `json:"exact_to_approximate_gained,omitempty"`
	Actual                    *m8ObservedTruthMasksV1 `json:"actual_local,omitempty"`
	CoordinatorReturned       *uint16                 `json:"coordinator_returned_mask,omitempty"`
}

type m8QualityAttributionV1 struct {
	Method           string             `json:"method"`
	Generation       uint64             `json:"generation"`
	SourceGeneration uint64             `json:"source_generation"`
	ModelSHA256      string             `json:"model_sha256"`
	Domains          int                `json:"domains"`
	PackCosts        []int64            `json:"packs_per_domain"`
	TraceQueries     int                `json:"trace_queries"`
	Queries          []m8QualityQueryV1 `json:"queries"`
}

type m8QualityCachedQueryV1 struct {
	querySHA256          string
	truthSHA256          string
	masks                []uint16
	primaryPrefix        []int
	domainCost, packCost m8CoverageCurveV1
	noCoarsening         []uint32
}

type m8QualityCacheV1 struct {
	queries        []m8QualityCachedQueryV1
	packDomains    []uint32
	packCosts      []int64
	traceQueries   int
	packIDs        [][]string
	traceValidated []bool
	limits         m8CoverageLimitsV1
}

func m8QualityQueryDigestV1(q []float32) string {
	h := sha256.New()
	h.Write([]byte("treedb/m8/static-quality/query/v1\x00"))
	var b [4]byte
	for _, x := range q {
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(x))
		h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func m8QualityTruthDigestV1(truth []m8CanonicalResultV1) string {
	h := sha256.New()
	h.Write([]byte(m8CanonicalResultContractV1 + "/quality-truth/v1\x00"))
	var b [8]byte
	for _, r := range truth {
		binary.LittleEndian.PutUint64(b[:], uint64(len(r.ID)))
		h.Write(b[:])
		h.Write([]byte(r.ID))
		binary.LittleEndian.PutUint32(b[:4], math.Float32bits(r.Score))
		h.Write(b[:4])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// m8QualityPackOwnersV1 validates the complete relation before any query masks
// are prepared. A logical domain may own several packs, but a pack has one owner.
func m8QualityPackOwnersV1(manifest collections.VectorPartitionManifestV1) ([]uint32, []int64, error) {
	p, d := int(manifest.PartitionCount), int(manifest.DomainCount)
	if p < 1 || p > maxPartitions || d < 1 || d > p || len(manifest.DomainPacks) != p {
		return nil, nil, errors.New("quality attribution requires complete domain-pack layout")
	}
	owners := make([]uint32, p)
	seen := make([]bool, p)
	costs := make([]int64, d)
	for _, m := range manifest.DomainPacks {
		if int(m.PackID) >= p || int(m.DomainID) >= d || seen[m.PackID] {
			return nil, nil, errors.New("duplicate or invalid quality pack owner")
		}
		seen[m.PackID] = true
		owners[m.PackID] = m.DomainID
		costs[m.DomainID]++
	}
	for _, c := range costs {
		if c == 0 {
			return nil, nil, errors.New("quality domain has no pack")
		}
	}
	return owners, costs, nil
}

func (h *m8AttributionHarnessV1) enableQualityV1(ctx context.Context, queries [][]float64, truth [][]m8CanonicalResultV1, homes map[string]uint32, members map[string][]uint32, traceQueries int, limits m8CoverageLimitsV1) error {
	if h == nil || h.assets == nil || len(queries) == 0 || len(queries) != len(truth) || h.quality != nil || traceQueries < 0 || traceQueries > m8QualityTraceMaxQueriesV1 || traceQueries > len(queries) {
		return errors.New("invalid or repeated quality attribution initialization")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Check matrix-independent cache bounds before creating ownership maps.
	if _, _, err := m8QualityOwnedBoundsV1(len(truth[0]), int(h.assets.manifest.DomainCount), int(h.assets.manifest.PartitionCount), len(queries), 1, traceQueries, limits); err != nil {
		return err
	}
	owners, costs, err := m8QualityPackOwnersV1(h.assets.manifest)
	if err != nil {
		return err
	}
	domainHomes, domainMembers, domains, err := m8OracleDomainMembershipsV1(homes, members, h.assets.manifest)
	if err != nil {
		return err
	}
	if domains != len(costs) {
		return errors.New("quality membership/layout disagreement")
	}
	cache := &m8QualityCacheV1{packDomains: owners, packCosts: costs, traceQueries: traceQueries, limits: limits, queries: make([]m8QualityCachedQueryV1, len(queries))}
	if traceQueries > 0 {
		cache.packIDs = make([][]string, len(owners))
		cache.traceValidated = make([]bool, len(owners))
	}
	unit := make([]int64, domains)
	for i := range unit {
		unit[i] = 1
	}
	var scratch m8CoverageScratchV1
	for i := range queries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(queries[i]) == 0 || len(queries[i]) != len(queries[0]) {
			return errors.New("quality query dimensions differ")
		}
		for _, value := range queries[i] {
			if math.IsNaN(value) || math.IsInf(value, 0) || math.IsInf(float64(float32(value)), 0) {
				return errors.New("nonfinite quality query value")
			}
		}
		if len(truth[i]) != len(truth[0]) {
			return errors.New("quality truth width differs across queries")
		}
		ids := m8CanonicalIDsV1(truth[i])
		masks, err := m8CoverageMasksV1(ids, domainMembers, domains)
		if err != nil {
			return err
		}
		for _, r := range truth[i] {
			if math.IsNaN(float64(r.Score)) || math.IsInf(float64(r.Score), 0) {
				return errors.New("nonfinite quality truth score")
			}
		}
		dc, err := m8CoverageCostCurveV1(ctx, len(ids), masks, unit, limits, &scratch)
		if err != nil {
			return err
		}
		pc, err := m8CoverageCostCurveV1(ctx, len(ids), masks, costs, limits, &scratch)
		if err != nil {
			return err
		}
		counts := make([]int, domains)
		for _, id := range ids {
			d, ok := domainHomes[id]
			if !ok || int(d) >= domains {
				return errors.New("missing quality primary home")
			}
			counts[d]++
		}
		slices.SortFunc(counts, func(a, b int) int { return b - a })
		prefix := make([]int, domains+1)
		for d, c := range counts {
			prefix[d+1] = prefix[d] + c
		}
		cache.queries[i] = m8QualityCachedQueryV1{querySHA256: m8QualityQueryDigestV1(m8Query32V1(queries[i])), truthSHA256: m8QualityTruthDigestV1(truth[i]), masks: masks, primaryPrefix: prefix, domainCost: dc, packCost: pc}
	}
	h.quality = cache
	return nil
}

func (h *m8AttributionHarnessV1) membershipOraclesV1(truth [][]m8CanonicalResultV1, homes map[string]uint32, members map[string][]uint32, probes int) ([]m8MembershipOracleRecallV1, error) {
	if h.quality == nil {
		return m8MembershipOracleRecallCacheV1(truth, homes, members, h.assets.manifest, probes)
	}
	if probes < 1 || probes > len(h.quality.packCosts) || len(truth) != len(h.quality.queries) {
		return nil, errors.New("quality oracle cache shape mismatch")
	}
	out := make([]m8MembershipOracleRecallV1, len(truth))
	for i, q := range h.quality.queries {
		if q.truthSHA256 != m8QualityTruthDigestV1(truth[i]) {
			return nil, errors.New("quality oracle truth identity changed")
		}
		hits, err := q.domainCost.hitsAtBudget(int64(probes))
		if err != nil {
			return nil, err
		}
		out[i] = m8MembershipOracleRecallV1{primary: float64(q.primaryPrefix[probes]) / float64(len(truth[i])), final: float64(hits) / float64(len(truth[i]))}
	}
	return out, nil
}

func (h *m8AttributionHarnessV1) exactQualityUnionV1(ctx context.Context, i int, query []float32, truth []m8CanonicalResultV1, k int) ([]m8CanonicalResultV1, error) {
	if h.quality == nil || i < 0 || i >= len(h.quality.queries) {
		return nil, errors.New("quality exact pass without cache")
	}
	q := &h.quality.queries[i]
	if q.querySHA256 != m8QualityQueryDigestV1(query) || q.truthSHA256 != m8QualityTruthDigestV1(truth) {
		return nil, errors.New("quality exact pass changed query/truth")
	}
	packs := make([]uint32, len(h.searchers))
	for p := range packs {
		packs[p] = uint32(p)
	}
	bests := make([]m8ExactPackBestV1, len(packs))
	got, _, err := h.searchWithMetricsAndBestsV1(ctx, query, packs, k, k, true, bests)
	if err != nil {
		return nil, err
	}
	if id, score := m8CanonicalParityV1(truth, got); !id || !score {
		return nil, errors.New("quality exact pack union differs from canonical IDs/score bits")
	}
	order, costs, err := m8NoCoarseningDomainsV1(bests, h.quality.packDomains, len(h.quality.packCosts))
	if err != nil {
		return nil, err
	}
	if !slices.Equal(costs, h.quality.packCosts) {
		return nil, errors.New("quality exact pass physical cost mismatch")
	}
	q.noCoarsening = order
	return got, nil
}

func (c *m8QualityCacheV1) domainsForPacksV1(packs []uint32) ([]uint32, error) {
	seenP := make([]bool, len(c.packDomains))
	seenD := make([]bool, len(c.packCosts))
	out := make([]uint32, 0, min(len(packs), len(c.packCosts)))
	counts := make([]int64, len(c.packCosts))
	for _, p := range packs {
		if int(p) >= len(seenP) || seenP[p] {
			return nil, errors.New("invalid or repeated quality routed pack")
		}
		seenP[p] = true
		d := c.packDomains[p]
		counts[d]++
		if !seenD[d] {
			seenD[d] = true
			out = append(out, d)
		}
	}
	for _, d := range out {
		if counts[d] != c.packCosts[d] {
			return nil, errors.New("quality route omits a required physical pack")
		}
	}
	return out, nil
}

func m8QualityResultMaskV1(truth []m8CanonicalResultV1, results []m8CanonicalResultV1) uint16 {
	var mask uint16
	for _, r := range results {
		for i, t := range truth {
			if r.ID == t.ID {
				mask |= uint16(1) << uint(i)
			}
		}
	}
	return mask
}

func (h *m8AttributionHarnessV1) qualityQueryV1(i, probes int, truth []m8CanonicalResultV1, exactPacks, approxPacks []uint32, local []m8CanonicalResultV1, observed *m8ObservedTruthMasksV1, localExecuted bool) (m8QualityQueryV1, error) {
	if h.quality == nil || i < 0 || i >= len(h.quality.queries) {
		return m8QualityQueryV1{}, errors.New("missing quality query")
	}
	c := h.quality
	q := c.queries[i]
	if probes < 1 || probes > len(q.noCoarsening) {
		return m8QualityQueryV1{}, errors.New("missing no-coarsening exact pass")
	}
	out := m8QualityQueryV1{QuerySHA256: q.querySHA256, TruthSHA256: q.truthSHA256, DomainCost: q.domainCost, PackCost: q.packCost, NoCoarseningDomains: slices.Clone(q.noCoarsening[:probes])}
	var err error
	out.NoCoarseningMask, out.NoCoarseningPacks, err = m8RouteCoverageV1(q.masks, c.packCosts, out.NoCoarseningDomains)
	if err != nil {
		return out, err
	}
	out.ExactDomains, err = c.domainsForPacksV1(exactPacks)
	if err != nil {
		return out, err
	}
	out.ExactMask, out.ExactPacks, err = m8RouteCoverageV1(q.masks, c.packCosts, out.ExactDomains)
	if err != nil {
		return out, err
	}
	out.NoCoarseningToExactLost = out.NoCoarseningMask &^ out.ExactMask
	out.NoCoarseningToExactGained = out.ExactMask &^ out.NoCoarseningMask
	if len(approxPacks) > 0 {
		out.ApproximateDomains, err = c.domainsForPacksV1(approxPacks)
		if err != nil {
			return out, err
		}
		mask, cost, err := m8RouteCoverageV1(q.masks, c.packCosts, out.ApproximateDomains)
		if err != nil {
			return out, err
		}
		out.ApproximateMask = &mask
		out.ApproximatePacks = cost
		lost, gained := out.ExactMask&^mask, mask&^out.ExactMask
		out.ExactToApproximateLost = &lost
		out.ExactToApproximateGained = &gained
		if localExecuted {
			if observed == nil {
				observed = &m8ObservedTruthMasksV1{}
			}
			observed.Available = mask
			observed.Returned = m8QualityResultMaskV1(truth, local)
			if err := observed.validate(len(truth)); err != nil {
				return out, err
			}
			out.Actual = observed
		}
	} else if localExecuted {
		return out, errors.New("local quality search executed without a route")
	}
	return out, nil
}

func (h *m8AttributionHarnessV1) qualityEvidenceV1() *m8QualityAttributionV1 {
	if h.quality == nil {
		return nil
	}
	return &m8QualityAttributionV1{Method: m8QualityAttributionMethodV1, Generation: h.assets.manifest.Generation, SourceGeneration: h.assets.manifest.SourceGeneration, ModelSHA256: h.assets.status.ModelDigest, Domains: len(h.quality.packCosts), PackCosts: slices.Clone(h.quality.packCosts), TraceQueries: h.quality.traceQueries, Queries: make([]m8QualityQueryV1, len(h.quality.queries))}
}

// Validate trace storage from actual pack structure BEFORE calling the existing
// detailed trace path. One trace is live at a time; ordinary requests never use it.
func (h *m8AttributionHarnessV1) prepareQualityTraceV1(ctx context.Context, p uint32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c := h.quality
	if c == nil || c.traceQueries == 0 || int(p) >= len(h.searchers) {
		return errors.New("invalid quality trace selection")
	}
	if c.traceValidated[p] {
		return nil
	}
	d, err := h.searchers[p].PackDiagnosticsV1()
	if err != nil {
		return err
	}
	var edges uint64 = d.AuxiliaryEdges
	for _, e := range d.EdgesByLayer {
		if e > math.MaxUint64-edges {
			return errors.New("trace edge bound overflow")
		}
		edges += e
	}
	// Greedy upper descent expands each improving node at most once per layer.
	// Charge edge inspection, score IDs, internal+exported events and seed arrays.
	if edges > uint64(m8QualityTraceMaxEventsV1) || d.Rows > uint64(m8QualityTraceMaxEventsV1) {
		return errors.New("quality trace structural event cap exceeded")
	}
	bound := int64(edges)*256 + int64(d.Rows)*256
	if bound > m8QualityTraceMaxBytesV1 || bound > c.limits.Bytes {
		return errors.New("quality trace structural memory cap exceeded")
	}
	ids, err := h.searchers[p].PackDocumentIDsForOfflineTraceWithContextV1(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.packIDs[p] = ids
	c.traceValidated[p] = true
	return nil
}

func (h *m8AttributionHarnessV1) searchQualityObservedV1(ctx context.Context, query []float32, packs []uint32, truth []m8CanonicalResultV1, k, ef int) ([]m8CanonicalResultV1, collections.VectorPartitionSearchMetricsV1, *m8ObservedTruthMasksV1, error) {
	var metrics collections.VectorPartitionSearchMetricsV1
	var scored, retained uint16
	merged := make([]m8CanonicalResultV1, 0, len(packs)*k)
	for _, p := range packs {
		if err := ctx.Err(); err != nil {
			return nil, metrics, nil, err
		}
		if err := h.prepareQualityTraceV1(ctx, p); err != nil {
			return nil, metrics, nil, err
		}
		results, m, a, err := h.searchers[p].SearchWithAttributionV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: k, EfSearch: ef})
		if err != nil {
			return nil, metrics, nil, err
		}
		if a.TerminationReason == "" {
			return nil, metrics, nil, errors.New("missing sampled quality trace")
		}
		metrics.Candidates += m.Candidates
		metrics.Edges += m.Edges
		for _, ordinal := range a.ScoreOrdinals {
			if int(ordinal) >= len(h.quality.packIDs[p]) {
				return nil, metrics, nil, errors.New("quality trace score ordinal out of range")
			}
			id := h.quality.packIDs[p][ordinal]
			for bit, t := range truth {
				if id == t.ID {
					scored |= uint16(1) << uint(bit)
				}
			}
		}
		for _, r := range results {
			merged = append(merged, m8CanonicalResultV1{ID: r.ID, Score: r.Score})
			for bit, t := range truth {
				if r.ID == t.ID {
					retained |= uint16(1) << uint(bit)
				}
			}
		}
	}
	return m8CanonicalResultsV1(merged, k), metrics, &m8ObservedTruthMasksV1{Scored: &scored, Retained: &retained}, nil
}

func m8ValidateQualityEvidenceV1(a *m8QualityAttributionV1, k, probes, samples int, complete bool) error {
	if a == nil {
		return errors.New("missing selected quality evidence")
	}
	if a.Method != m8QualityAttributionMethodV1 || a.Generation == 0 || a.SourceGeneration == 0 || !m8SHA256V1(a.ModelSHA256) || a.Domains < 1 || a.Domains > maxPartitions || len(a.PackCosts) != a.Domains || len(a.Queries) != samples || k < 1 || k > 10 || probes < 1 || probes > a.Domains || a.TraceQueries < 0 || a.TraceQueries > m8QualityTraceMaxQueriesV1 || a.TraceQueries > samples {
		return errors.New("invalid quality evidence identity or shape")
	}
	var totalPacks int64
	for _, c := range a.PackCosts {
		if c < 1 || c > int64(maxPartitions)-totalPacks {
			return errors.New("invalid quality physical cost")
		}
		totalPacks += c
	}
	full := uint16(1<<uint(k)) - 1
	for i, q := range a.Queries {
		if !m8SHA256V1(q.QuerySHA256) || !m8SHA256V1(q.TruthSHA256) || q.DomainCost.Method != m8CoverageCostMethodV1 || q.PackCost.Method != m8CoverageCostMethodV1 || q.DomainCost.TruthCount != k || q.PackCost.TruthCount != k {
			return errors.New("invalid quality query method/identity")
		}
		for _, c := range []m8CoverageCurveV1{q.DomainCost, q.PackCost} {
			if _, err := c.hitsAtBudget(0); err != nil {
				return err
			}
			if c.WorkBound <= 0 || c.ScratchBytes <= 0 {
				return errors.New("missing quality resource bound")
			}
		}
		for _, r := range []struct {
			domains []uint32
			mask    uint16
			cost    int64
		}{{q.NoCoarseningDomains, q.NoCoarseningMask, q.NoCoarseningPacks}, {q.ExactDomains, q.ExactMask, q.ExactPacks}} {
			if len(r.domains) != probes || r.mask&^full != 0 {
				return errors.New("invalid quality route width/mask")
			}
			_, cost, err := m8RouteCoverageV1(make([]uint16, a.Domains), a.PackCosts, r.domains)
			if err != nil || cost != r.cost {
				return errors.New("quality physical route cost mismatch")
			}
			hits, err := q.DomainCost.hitsAtBudget(int64(probes))
			if err != nil || bits.OnesCount16(r.mask) > hits {
				return errors.New("route exceeds domain oracle ceiling")
			}
			hits, err = q.PackCost.hitsAtBudget(r.cost)
			if err != nil || bits.OnesCount16(r.mask) > hits {
				return errors.New("route exceeds physical oracle ceiling")
			}
		}
		if q.NoCoarseningToExactLost != q.NoCoarseningMask&^q.ExactMask || q.NoCoarseningToExactGained != q.ExactMask&^q.NoCoarseningMask {
			return errors.New("quality signed route changes mismatch")
		}
		if q.ApproximateMask == nil {
			if complete || len(q.ApproximateDomains) != 0 || q.ApproximatePacks != 0 || q.ExactToApproximateLost != nil || q.ExactToApproximateGained != nil || q.Actual != nil {
				return errors.New("quality missing approximate route misreported")
			}
			continue
		}
		if *q.ApproximateMask&^full != 0 || len(q.ApproximateDomains) != probes || q.ExactToApproximateLost == nil || q.ExactToApproximateGained == nil || *q.ExactToApproximateLost != q.ExactMask&^*q.ApproximateMask || *q.ExactToApproximateGained != *q.ApproximateMask&^q.ExactMask {
			return errors.New("quality approximate masks mismatch")
		}
		_, cost, err := m8RouteCoverageV1(make([]uint16, a.Domains), a.PackCosts, q.ApproximateDomains)
		if err != nil || cost != q.ApproximatePacks {
			return errors.New("quality approximate pack cost mismatch")
		}
		domainHits, domainErr := q.DomainCost.hitsAtBudget(int64(probes))
		packHits, packErr := q.PackCost.hitsAtBudget(q.ApproximatePacks)
		if domainErr != nil || packErr != nil || bits.OnesCount16(*q.ApproximateMask) > min(domainHits, packHits) {
			return errors.New("approximate route exceeds coverage oracle ceiling")
		}
		if complete {
			if q.Actual == nil || q.Actual.Available != *q.ApproximateMask {
				return errors.New("missing quality local observation")
			}
			if err := q.Actual.validate(k); err != nil {
				return err
			}
			traced := i < a.TraceQueries
			if (q.Actual.Scored != nil) != traced || (q.Actual.Retained != nil) != traced {
				return errors.New("quality trace sample mislabeled")
			}
		} else if q.Actual != nil {
			return errors.New("unexecuted quality local cell has observations")
		}
	}
	return nil
}

func m8QualityEvidenceSelectionV1(cfg m8ProductionConfigEvidenceV1, row m8ProductionRowV1) error {
	if !cfg.QualityDiagnostics {
		if cfg.QualityTraceQueries != 0 || row.Attribution.Quality != nil {
			return errors.New("unselected quality diagnostics in report")
		}
		return nil
	}
	if cfg.TopK < 1 || cfg.TopK > 10 || cfg.QualityTraceQueries < 0 || cfg.QualityTraceQueries > m8QualityTraceMaxQueriesV1 {
		return errors.New("invalid quality config")
	}
	if row.Status != "pass" && row.Status != "fail" && row.Status != "candidate_coverage_shortfall" {
		if row.Attribution.Quality != nil {
			return errors.New("unsupported row has quality evidence")
		}
		return nil
	}
	q := row.Attribution.Quality
	if err := m8ValidateQualityEvidenceV1(q, cfg.TopK, row.Probes, row.Samples, row.Attribution.ApproximateRouterPartitionCoverageComplete); err != nil {
		return err
	}
	d, packs, ok := m8ProductionDomainLayoutV1(cfg)
	if !ok || d != q.Domains || q.TraceQueries != cfg.QualityTraceQueries {
		return errors.New("quality config/layout mismatch")
	}
	for _, observed := range q.Queries {
		if row.Attribution.ApproximateRouterPartitionCoverageComplete {
			if observed.CoordinatorReturned == nil || *observed.CoordinatorReturned & ^uint16((1<<uint(cfg.TopK))-1) != 0 {
				return errors.New("missing or invalid measured coordinator truth mask")
			}
			if row.Attribution.CoordinatorMergeIDParity && (observed.Actual == nil || *observed.CoordinatorReturned != observed.Actual.Returned) {
				return errors.New("coordinator/local truth masks disagree with claimed ID parity")
			}
		} else if observed.CoordinatorReturned != nil {
			return errors.New("unexecuted coordinator has truth observation")
		}
	}
	for i, c := range q.PackCosts {
		if c != int64(packs[i]) {
			return errors.New("quality pack counts do not match config")
		}
	}
	return nil
}

// Called by retained replay after independently executing the same static
// generation. This comparison is intentionally deep: changing a cost, query,
// mask, trace coverage or method cannot be accepted via an aggregate recall.
func m8QualityReplayEqualV1(want, got *m8QualityAttributionV1) bool {
	return reflect.DeepEqual(want, got)
}

// Attach only the actual measured coordinator results. The static local trace
// remains distinct; neither timing nor a successful coordinator mask is inferred
// from the offline search. Clone per-row fields so other concurrency cells keep
// their own observation population.
func m8QualityAttachCoordinatorV1(a *m8QualityAttributionV1, truth, results [][]m8CanonicalResultV1) (*m8QualityAttributionV1, error) {
	if a == nil {
		return nil, nil
	}
	if len(truth) != len(a.Queries) || len(results) != len(truth) {
		return nil, errors.New("quality coordinator truth/result cardinality mismatch")
	}
	out := *a
	out.Queries = slices.Clone(a.Queries)
	for i := range truth {
		if m8QualityTruthDigestV1(truth[i]) != out.Queries[i].TruthSHA256 {
			return nil, errors.New("quality coordinator truth identity changed")
		}
		mask := m8QualityResultMaskV1(truth[i], results[i])
		out.Queries[i].CoordinatorReturned = &mask
	}
	return &out, nil
}
