package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
)

const m8CoverageCostMethodV1 = "truth_mask_min_cost_v1"

// m8CoverageLimitsV1 is supplied by the benchmark's work preflight, not a
// serving request. Work charges conservative state visits, including copies.
type m8CoverageLimitsV1 struct {
	WorkUnits int64
	Bytes     int64
}

type m8CoverageScratchV1 struct {
	current []int64
	next    []int64
}

// m8CoverageCurveV1 records the least cost needed for AT LEAST h truth hits.
// -1 denotes unreachable. Its storage depends on truth width, not cost size.
type m8CoverageCurveV1 struct {
	Method       string  `json:"method"`
	TruthCount   int     `json:"truth_count"`
	MinimumCosts []int64 `json:"minimum_costs_for_hits"`
	WorkBound    int64   `json:"work_bound"`
	ScratchBytes int64   `json:"scratch_bytes"`
}

func (c m8CoverageCurveV1) hitsAtBudget(budget int64) (int, error) {
	if budget < 0 || c.TruthCount < 1 || c.TruthCount > 10 || len(c.MinimumCosts) != c.TruthCount+1 || c.MinimumCosts[0] != 0 {
		return 0, errors.New("invalid coverage curve or budget")
	}
	hits, last, unreachable := 0, int64(0), false
	for h, cost := range c.MinimumCosts {
		if cost < -1 || (cost >= 0 && (unreachable || cost < last)) {
			return 0, errors.New("nonmonotone coverage curve")
		}
		if cost == -1 {
			unreachable = true
			continue
		}
		last = cost
		if cost <= budget {
			hits = h
		}
	}
	return hits, nil
}

func m8CoverageCheckedAddV1(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, errors.New("coverage cost/work overflow")
	}
	return a + b, nil
}

func m8CoverageCheckedMulV1(a, b int64) (int64, error) {
	if a < 0 || b < 0 || (b != 0 && a > math.MaxInt64/b) {
		return 0, errors.New("coverage shape overflow")
	}
	return a * b, nil
}

func m8CoverageShapeV1(k int, masks []uint16, costs []int64) (int, int64, error) {
	if k < 1 || k > 10 || len(masks) == 0 || len(masks) != len(costs) {
		return 0, 0, errors.New("coverage requires 1..10 truth IDs and matching nonempty masks/costs")
	}
	states := 1 << uint(k)
	var total int64
	for d, mask := range masks {
		if int(mask) >= states || costs[d] <= 0 {
			return 0, 0, fmt.Errorf("invalid coverage mask/cost at domain %d", d)
		}
		var err error
		total, err = m8CoverageCheckedAddV1(total, costs[d])
		if err != nil {
			return 0, 0, err
		}
	}
	if total == math.MaxInt64 {
		return 0, 0, errors.New("coverage cost leaves no finite infinity sentinel")
	}
	return states, total + 1, nil
}

// m8CoveragePlanV1 bounds each loop before allocation. Inputs are caller-owned;
// bytes here include both reusable buffers and the owned result cost vector.
func m8CoveragePlanV1(k, domains, states int, limits m8CoverageLimitsV1) (int64, int64, error) {
	if limits.WorkUnits < 1 || limits.Bytes < 1 {
		return 0, 0, errors.New("coverage requires explicit positive resource limits")
	}
	passes, err := m8CoverageCheckedAddV1(int64(domains), int64(k)+1)
	if err != nil {
		return 0, 0, err
	}
	passes, err = m8CoverageCheckedMulV1(passes, 2)
	if err != nil {
		return 0, 0, err
	}
	work, err := m8CoverageCheckedMulV1(passes, int64(states))
	if err != nil {
		return 0, 0, err
	}
	memory := int64(2*states+k+1) * 8
	if work > limits.WorkUnits || memory > limits.Bytes {
		return work, memory, fmt.Errorf("coverage preflight exceeds limits: work=%d bytes=%d", work, memory)
	}
	return work, memory, nil
}

// m8CoverageCostCurveV1 solves one cost interpretation. It does not combine
// independent domain-count and physical-pack optima into a joint witness.
func m8CoverageCostCurveV1(ctx context.Context, k int, masks []uint16, costs []int64, limits m8CoverageLimitsV1, scratch *m8CoverageScratchV1) (m8CoverageCurveV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return m8CoverageCurveV1{}, err
	}
	if k < 1 || k > 10 || len(masks) == 0 || len(masks) != len(costs) {
		return m8CoverageCurveV1{}, errors.New("invalid coverage input shape")
	}
	states := 1 << uint(k)
	work, memory, err := m8CoveragePlanV1(k, len(masks), states, limits)
	if err != nil {
		return m8CoverageCurveV1{}, err
	}
	_, inf, err := m8CoverageShapeV1(k, masks, costs)
	if err != nil {
		return m8CoverageCurveV1{}, err
	}
	if scratch == nil {
		return m8CoverageCurveV1{}, errors.New("coverage scratch is required")
	}
	// Release an oversized retained buffer; the supplied cap covers retention
	// as well as length, even if this scratch came from a wider earlier query.
	if cap(scratch.current) < states || cap(scratch.current) > states {
		scratch.current = make([]int64, states)
	}
	if cap(scratch.next) < states || cap(scratch.next) > states {
		scratch.next = make([]int64, states)
	}
	current, next := scratch.current[:states], scratch.next[:states]
	for i := range current {
		current[i] = inf
	}
	current[0] = 0
	for d, mask := range masks {
		if err := ctx.Err(); err != nil {
			return m8CoverageCurveV1{}, err
		}
		copy(next, current)
		for old, cost := range current {
			if cost == inf {
				continue
			}
			// A state before processing d contains only earlier domains.
			// Its sum plus this cost is bounded by the validated total.
			union := old | int(mask)
			if candidate := cost + costs[d]; candidate < next[union] {
				next[union] = candidate
			}
		}
		current, next = next, current
	}
	result := m8CoverageCurveV1{Method: m8CoverageCostMethodV1, TruthCount: k,
		MinimumCosts: make([]int64, k+1), WorkBound: work, ScratchBytes: memory}
	for h := range result.MinimumCosts {
		result.MinimumCosts[h] = -1
	}
	for mask, cost := range current {
		if cost == inf {
			continue
		}
		for h := 0; h <= bits.OnesCount16(uint16(mask)); h++ {
			if result.MinimumCosts[h] == -1 || cost < result.MinimumCosts[h] {
				result.MinimumCosts[h] = cost
			}
		}
	}
	return result, ctx.Err()
}

// m8CoverageJointHitsV1 is an explicitly bounded optional oracle: at most
// maxDomains AND at most packBudget. Descending count prevents domain reuse.
func m8CoverageJointHitsV1(ctx context.Context, k int, masks []uint16, packCosts []int64, maxDomains int, packBudget int64, limits m8CoverageLimitsV1) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	states, inf, err := m8CoverageShapeV1(k, masks, packCosts)
	if err != nil {
		return 0, err
	}
	if maxDomains < 0 || maxDomains > len(masks) || packBudget < 0 || limits.WorkUnits < 1 || limits.Bytes < 1 {
		return 0, errors.New("invalid joint coverage bounds")
	}
	cells, err := m8CoverageCheckedMulV1(int64(maxDomains)+1, int64(states))
	if err != nil {
		return 0, err
	}
	memory, err := m8CoverageCheckedMulV1(cells, 8)
	if err != nil {
		return 0, err
	}
	work, err := m8CoverageCheckedMulV1(cells, int64(len(masks))+2)
	if err != nil {
		return 0, err
	}
	if work > limits.WorkUnits || memory > limits.Bytes || cells > int64(math.MaxInt) {
		return 0, errors.New("joint coverage preflight exceeds resource limits")
	}
	joint := make([]int64, int(cells))
	for i := range joint {
		joint[i] = inf
	}
	joint[0] = 0
	for d, mask := range masks {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		for count := min(maxDomains, d+1); count > 0; count-- {
			previous, current := joint[(count-1)*states:count*states], joint[count*states:(count+1)*states]
			for old, cost := range previous {
				if cost == inf {
					continue
				}
				union := old | int(mask)
				if candidate := cost + packCosts[d]; candidate < current[union] {
					current[union] = candidate
				}
			}
		}
	}
	hits := 0
	for cell, cost := range joint {
		if cost != inf && cost <= packBudget {
			hits = max(hits, bits.OnesCount16(uint16(cell%states)))
		}
	}
	return hits, ctx.Err()
}

// m8CoverageMasksV1 accepts only a complete, duplicate-free truth membership
// relation in logical-domain space, with strictly ascending domain lists.
// Pack ownership and input preparation are validated/charged by its caller.
func m8CoverageMasksV1(truth []string, memberships map[string][]uint32, domains int) ([]uint16, error) {
	if len(truth) < 1 || len(truth) > 10 || domains < 1 || domains > 1<<20 {
		return nil, errors.New("invalid coverage truth/domain shape")
	}
	seenIDs := make(map[string]struct{}, len(truth))
	// Validate before allocating domain-sized storage.
	for _, id := range truth {
		if id == "" {
			return nil, errors.New("empty truth ID")
		}
		if _, duplicate := seenIDs[id]; duplicate {
			return nil, errors.New("duplicate truth ID")
		}
		seenIDs[id] = struct{}{}
		parts := memberships[id]
		if len(parts) == 0 || len(parts) > domains {
			return nil, errors.New("missing or oversized truth membership")
		}
		for i, d := range parts {
			if uint64(d) >= uint64(domains) {
				return nil, errors.New("truth membership outside logical domains")
			}
			if i > 0 && d <= parts[i-1] {
				return nil, errors.New("noncanonical or duplicate truth-domain membership")
			}
		}
	}
	masks := make([]uint16, domains)
	for rank, id := range truth {
		for _, domain := range memberships[id] {
			masks[domain] |= uint16(1) << uint(rank)
		}
	}
	return masks, nil
}
