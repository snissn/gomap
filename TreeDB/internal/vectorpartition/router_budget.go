package vectorpartition

import (
	"errors"
	"sort"
)

// ApportionRouterBudgetV2 reserves one root per population, then uses integer
// largest remainders with canonical input-order ties. A non-unary tree with n
// members has at most 2*n-1 nodes. Unusable tokens remain unallocated.
func ApportionRouterBudgetV2(populations []int, budget int) ([]int, error) {
	eligible := make([]bool, len(populations))
	for i := range eligible {
		eligible[i] = true
	}
	return apportionRouterSubtreeBudgetsV3(populations, eligible, budget)
}

// apportionRouterSubtreeBudgetsV3 reserves the already-emitted centroid in
// every bucket, then assigns residual tokens only to buckets large and deep
// enough to recurse. Callers provide eligibility because leaf size and depth
// are part of the versioned builder contract.
func apportionRouterSubtreeBudgetsV3(populations []int, eligible []bool, budget int) ([]int, error) {
	if len(populations) == 0 || budget < len(populations) || budget > routerMaxRepresentatives {
		return nil, errors.New("vectorpartition: invalid global router budget")
	}
	if len(eligible) != len(populations) {
		return nil, errors.New("vectorpartition: invalid router subtree eligibility")
	}
	out := make([]int, len(populations))
	var capacity int64
	for i, n := range populations {
		if n < 1 || n > routerMaxVectors {
			return nil, errors.New("vectorpartition: invalid router population")
		}
		if eligible[i] {
			capacity += int64(2*n - 1)
		} else {
			capacity++
		}
		out[i] = 1
	}
	available := budget
	if capacity < int64(available) {
		available = int(capacity)
	}
	remaining := available - len(out)
	type remainder struct {
		index int
		value int64
	}
	for remaining > 0 {
		var weight int64
		for i, n := range populations {
			if eligible[i] && out[i] < 2*n-1 {
				weight += int64(n)
			}
		}
		if weight == 0 {
			break
		}
		pending := remaining
		remainders := make([]remainder, 0, len(out))
		for i, n := range populations {
			if !eligible[i] {
				continue
			}
			room := 2*n - 1 - out[i]
			if room == 0 {
				continue
			}
			// Both factors are independently bounded above; their product fits int64.
			product := int64(pending) * int64(n)
			share := min(room, int(product/weight))
			out[i] += share
			remaining -= share
			remainders = append(remainders, remainder{i, product % weight})
		}
		sort.Slice(remainders, func(i, j int) bool {
			a, b := remainders[i], remainders[j]
			if a.value != b.value {
				return a.value > b.value
			}
			return a.index < b.index
		})
		for _, r := range remainders {
			if remaining == 0 {
				break
			}
			if out[r.index] < 2*populations[r.index]-1 {
				out[r.index]++
				remaining--
			}
		}
	}
	return out, nil
}

func routerIdenticalMembersV2(vectors []routerBuildVectorV1, members []int) bool {
	first := vectors[members[0]].values
	for _, member := range members[1:] {
		for i, value := range vectors[member].values {
			if value != first[i] {
				return false
			}
		}
	}
	return true
}
