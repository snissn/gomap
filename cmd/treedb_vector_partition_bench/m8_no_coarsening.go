package main

import (
	"errors"
	"math"
	"sort"
)

// m8ExactPackBestV1 is the first eligible result from an exhaustive pack scan.
// Present distinguishes an empty eligible pack from a real zero-score result.
type m8ExactPackBestV1 struct {
	Present bool
	ID      string
	Score   float32
}

// m8NoCoarseningDomainsV1 reduces all physical packs, not only packs containing
// top-k truth. packDomains is indexed by physical pack, with one logical owner.
// Every domain must have a physical pack; an eligible-empty domain is legal.
func m8NoCoarseningDomainsV1(best []m8ExactPackBestV1, packDomains []uint32, domains int) ([]uint32, []int64, error) {
	if domains < 1 || domains > len(best) || len(best) != len(packDomains) {
		return nil, nil, errors.New("incomplete no-coarsening pack/domain shape")
	}
	for p, d := range packDomains {
		if uint64(d) >= uint64(domains) {
			return nil, nil, errors.New("invalid no-coarsening domain owner")
		}
		b := best[p]
		if (b.Present && (b.ID == "" || math.IsNaN(float64(b.Score)) || math.IsInf(float64(b.Score), 0))) ||
			(!b.Present && (b.ID != "" || math.Float32bits(b.Score) != 0)) {
			return nil, nil, errors.New("invalid exact pack best or eligible-empty marker")
		}
	}
	domainBest := make([]m8ExactPackBestV1, domains)
	costs := make([]int64, domains)
	for p, d := range packDomains {
		costs[d]++
		b, old := best[p], domainBest[d]
		if b.Present && (!old.Present || b.Score > old.Score || (b.Score == old.Score && b.ID < old.ID)) {
			domainBest[d] = b
		}
	}
	order := make([]uint32, domains)
	for d := range order {
		if costs[d] == 0 {
			return nil, nil, errors.New("logical domain has no physical packs")
		}
		order[d] = uint32(d)
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := domainBest[order[i]], domainBest[order[j]]
		if a.Present != b.Present {
			return a.Present
		}
		if a.Present && a.Score != b.Score {
			return a.Score > b.Score
		}
		// Domain priorities use domain ID as the tie breaker, not which
		// member happened to supply an equally good score.
		return order[i] < order[j]
	})
	return order, costs, nil
}

func m8RouteCoverageV1(masks []uint16, packCosts []int64, route []uint32) (uint16, int64, error) {
	if len(masks) == 0 || len(masks) != len(packCosts) {
		return 0, 0, errors.New("invalid route coverage shape")
	}
	seen := make(map[uint32]struct{}, len(route))
	var mask uint16
	var cost int64
	for _, domain := range route {
		if uint64(domain) >= uint64(len(masks)) || packCosts[domain] <= 0 {
			return 0, 0, errors.New("invalid routed domain/cost")
		}
		if _, duplicate := seen[domain]; duplicate {
			return 0, 0, errors.New("duplicate routed domain")
		}
		seen[domain] = struct{}{}
		var err error
		cost, err = m8CoverageCheckedAddV1(cost, packCosts[domain])
		if err != nil {
			return 0, 0, err
		}
		mask |= masks[domain]
	}
	return mask, cost, nil
}

// m8ObservedTruthMasksV1 keeps missing observations explicitly absent.
// Returned is local output here; the coordinator output needs its own receipt.
type m8ObservedTruthMasksV1 struct {
	Available uint16  `json:"available"`
	Scored    *uint16 `json:"scored,omitempty"`
	Retained  *uint16 `json:"retained,omitempty"`
	Returned  uint16  `json:"returned"`
}

func (m m8ObservedTruthMasksV1) validate(k int) error {
	if k < 1 || k > 10 {
		return errors.New("invalid observed truth width")
	}
	full := uint16(1<<uint(k)) - 1
	if m.Available & ^full != 0 || m.Returned & ^m.Available != 0 {
		return errors.New("returned truth is outside available eligible truth")
	}
	ceiling := m.Available
	for _, observation := range []*uint16{m.Scored, m.Retained} {
		if observation == nil {
			continue
		}
		if *observation & ^ceiling != 0 || m.Returned & ^*observation != 0 {
			return errors.New("observed eligible truth pipeline is not nested")
		}
		ceiling = *observation
	}
	return nil
}
