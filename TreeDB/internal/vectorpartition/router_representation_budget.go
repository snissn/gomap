package vectorpartition

import (
	"context"
	"errors"
	"math"
)

// ApportionRouterRepresentationBudgetV1 is an explicit alternative quota planner,
// NOT selected by the M8 equal-control-budget arms. One token/domain is reserved;
// remaining tokens use integer largest remainders with source-population caps.
// A work cap bounds repeated redistribution when small domains saturate.
func ApportionRouterRepresentationBudgetV1(ctx context.Context, populations []uint64, budget int, maxWork uint64) ([]RouterRepresentationQuotaV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(populations) == 0 || len(populations) > routerMaxPartitions || budget < len(populations) || budget > routerMaxRepresentatives || maxWork == 0 {
		return nil, errors.New("invalid representation apportionment bounds")
	}
	var total uint64
	for _, n := range populations {
		if n == 0 || n > routerMaxVectors || total > math.MaxUint64-n {
			return nil, errors.New("invalid/overflow population")
		}
		total += n
	}
	if uint64(budget) > total {
		return nil, errors.New("budget exceeds structural population cap")
	}
	out := make([]RouterRepresentationQuotaV1, len(populations))
	for i := range out {
		out[i] = RouterRepresentationQuotaV1{Domain: uint32(i), Tokens: 1}
	}
	remaining := budget - len(out)
	var work uint64
	type remainder struct {
		domain int
		value  uint64
	}
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		charge := uint64(len(populations)) * uint64(len(populations)+8)
		if charge > maxWork || work > maxWork-charge {
			return nil, errors.New("apportionment work cap")
		}
		work += uint64(len(populations)) * uint64(len(populations)+8)
		var weight uint64
		for i, n := range populations {
			if uint64(out[i].Tokens) < n {
				weight += n
			}
		}
		if weight == 0 {
			return nil, errors.New("apportionment has no remaining capacity")
		}
		original := remaining
		rs := make([]remainder, 0, len(populations))
		for i, n := range populations {
			if uint64(out[i].Tokens) >= n {
				continue
			}
			product, err := representationMulV1(uint64(original), n)
			if err != nil {
				return nil, err
			}
			share := minU64RepresentationV1(product/weight, n-uint64(out[i].Tokens))
			out[i].Tokens += int(share)
			remaining -= int(share)
			rs = append(rs, remainder{domain: i, value: product % weight})
		}
		if err := routerRepresentationSortV1(ctx, rs, func(a, b remainder) bool {
			if a.value != b.value {
				return a.value > b.value
			}
			return a.domain < b.domain
		}); err != nil {
			return nil, err
		}
		for _, r := range rs {
			if remaining == 0 {
				break
			}
			if uint64(out[r.domain].Tokens) < populations[r.domain] {
				out[r.domain].Tokens++
				remaining--
			}
		}
		if remaining == original {
			return nil, errors.New("apportionment made no progress")
		}
	}
	return out, nil
}
func minU64RepresentationV1(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
