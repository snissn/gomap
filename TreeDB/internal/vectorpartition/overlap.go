package vectorpartition

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// OverlapConfig controls M3's derived memberships.  Ratio is deliberately
// explicit: no product default is selected by this package.
type OverlapConfig struct{ Ratio float64 }

// Membership is a stable vector ordinal paired with one logical partition.
// Home is false for derived memberships and true for the immutable M2 home.
type Membership struct {
	VectorOrdinal int
	Partition     int
	Home          bool
}

// OverlapResult is canonical (ordinal, partition order) and records budget
// left unused when the capacity/affinity constraints prevent another move.
type OverlapResult struct {
	Memberships []Membership
	Budget      int
	Used        int
	Unspent     int
	Loads       []int
}

// BuildOverlap derives bounded non-home memberships from a validated disjoint
// artifact.  Each round observes one immutable membership snapshot, ranks all
// candidates by cut-edge reduction then stable identity, and applies the
// winning proposals in that order.  Consequently scheduler timing cannot
// affect the result.
func BuildOverlap(a Artifact, cfg OverlapConfig) (OverlapResult, error) {
	if err := ValidateArtifact(a); err != nil {
		return OverlapResult{}, err
	}
	if math.IsNaN(cfg.Ratio) || math.IsInf(cfg.Ratio, 0) || cfg.Ratio < 0 || cfg.Ratio > 1 {
		return OverlapResult{}, errors.New("overlap ratio must be finite in [0,1]")
	}
	n := len(a.IDs)
	budget := int(math.Floor(cfg.Ratio * float64(n)))
	loads := make([]int, a.Config.Partitions)
	members := make([]map[int]struct{}, n)
	for i, p := range a.Assignment {
		loads[p]++
		members[i] = map[int]struct{}{p: {}}
	}
	used := 0
	type proposal struct {
		node, part, gain int
		id               string
	}
	for used < budget {
		// This proposal set is the bulk-synchronous round snapshot.
		ps := make([]proposal, 0, n)
		for i, ns := range a.Graph.Neighbors {
			counts := make([]int, a.Config.Partitions)
			for _, j := range ns {
				for p := range members[j] {
					counts[p]++
				}
			}
			best, gain := -1, 0
			for p, count := range counts {
				if _, exists := members[i][p]; exists || count == 0 {
					continue
				}
				if count > gain || count == gain && (best < 0 || p < best) {
					best, gain = p, count
				}
			}
			if best >= 0 {
				// A plurality alone is insufficient: score only directed cut
				// edges newly satisfied by this membership.
				reduction := overlapCutReduction(members, i, best, ns)
				if reduction > 0 {
					ps = append(ps, proposal{i, best, reduction, a.IDs[i]})
				}
			}
		}
		sort.Slice(ps, func(i, j int) bool {
			if ps[i].gain != ps[j].gain {
				return ps[i].gain > ps[j].gain
			}
			if ps[i].id != ps[j].id {
				return ps[i].id < ps[j].id
			}
			return ps[i].part < ps[j].part
		})
		if len(ps) == 0 {
			break
		}
		applied := 0
		for _, p := range ps {
			if used == budget {
				break
			}
			if loads[p.part] >= a.Metrics.Cap {
				continue
			}
			if _, exists := members[p.node][p.part]; exists {
				continue
			}
			if overlapCutReduction(members, p.node, p.part, a.Graph.Neighbors[p.node]) == 0 {
				continue
			}
			members[p.node][p.part] = struct{}{}
			loads[p.part]++
			used++
			applied++
		}
		if applied == 0 {
			break
		}
	}
	out := OverlapResult{Budget: budget, Used: used, Unspent: budget - used, Loads: loads}
	for i, set := range members {
		for p := range set {
			out.Memberships = append(out.Memberships, Membership{VectorOrdinal: i, Partition: p, Home: p == a.Assignment[i]})
		}
	}
	sort.Slice(out.Memberships, func(i, j int) bool {
		if out.Memberships[i].VectorOrdinal != out.Memberships[j].VectorOrdinal {
			return out.Memberships[i].VectorOrdinal < out.Memberships[j].VectorOrdinal
		}
		return out.Memberships[i].Partition < out.Memberships[j].Partition
	})
	if err := ValidateOverlap(a, cfg, out); err != nil {
		return OverlapResult{}, err
	}
	return out, nil
}

// ValidateOverlap is intentionally reusable by manifest/materialization code.
func overlapCutReduction(members []map[int]struct{}, node, partition int, neighbors []int) int {
	reduction := 0
	for _, neighbor := range neighbors {
		already := false
		for p := range members[node] {
			if _, ok := members[neighbor][p]; ok {
				already = true
				break
			}
		}
		if !already {
			if _, ok := members[neighbor][partition]; ok {
				reduction++
			}
		}
	}
	return reduction
}

// ValidateOverlap verifies accounting against the caller's actual configured
// ratio. It never trusts a self-declared budget from serialized output.
func ValidateOverlap(a Artifact, cfg OverlapConfig, r OverlapResult) error {
	if err := ValidateArtifact(a); err != nil {
		return err
	}
	if math.IsNaN(cfg.Ratio) || math.IsInf(cfg.Ratio, 0) || cfg.Ratio < 0 || cfg.Ratio > 1 {
		return errors.New("overlap ratio must be finite in [0,1]")
	}
	wantBudget := int(math.Floor(cfg.Ratio * float64(len(a.IDs))))
	if r.Budget != wantBudget || r.Budget < 0 || r.Used < 0 || r.Used > r.Budget || r.Unspent != r.Budget-r.Used || len(r.Loads) != a.Config.Partitions || len(r.Memberships) != len(a.IDs)+r.Used {
		return errors.New("invalid overlap accounting")
	}
	seen := make(map[[2]int]struct{}, len(r.Memberships))
	homes := make([]int, len(a.IDs))
	loads := make([]int, a.Config.Partitions)
	perVector := make([]int, len(a.IDs))
	for i, m := range r.Memberships {
		if m.VectorOrdinal < 0 || m.VectorOrdinal >= len(a.IDs) || m.Partition < 0 || m.Partition >= a.Config.Partitions || (i > 0 && (m.VectorOrdinal < r.Memberships[i-1].VectorOrdinal || m.VectorOrdinal == r.Memberships[i-1].VectorOrdinal && m.Partition <= r.Memberships[i-1].Partition)) {
			return errors.New("noncanonical overlap membership")
		}
		key := [2]int{m.VectorOrdinal, m.Partition}
		if _, ok := seen[key]; ok {
			return errors.New("duplicate overlap membership")
		}
		seen[key] = struct{}{}
		if m.Home != (m.Partition == a.Assignment[m.VectorOrdinal]) {
			return fmt.Errorf("home assignment moved for vector %d", m.VectorOrdinal)
		}
		if m.Home {
			homes[m.VectorOrdinal]++
		}
		loads[m.Partition]++
		if !m.Home {
			perVector[m.VectorOrdinal]++
			if perVector[m.VectorOrdinal] > 16 {
				return errors.New("overlap membership per-vector cap exceeded")
			}
		}
	}
	for i, n := range homes {
		if n != 1 {
			return fmt.Errorf("vector %d has %d home memberships", i, n)
		}
	}
	for p, n := range loads {
		if n != r.Loads[p] || n > a.Metrics.Cap {
			return errors.New("overlap partition capacity exceeded")
		}
	}
	return nil
}
