package vectorpartition

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// OverlapConfig controls M3's derived memberships. Ratio is a global extra-
// membership ratio: floor(Ratio * source rows) memberships are requested.
// Capacity is the declared per-partition total-membership cap; zero retains
// the disjoint artifact cap. RequireExact makes any shortfall fail closed.
// UsefulOnly stops when cut-reducing proposals are exhausted and forbids
// filler replicas. UsefulOnly and RequireExact are mutually exclusive.
type OverlapConfig struct {
	Ratio        float64
	Capacity     int
	RequireExact bool
	UsefulOnly   bool
}

// Membership is a stable vector ordinal paired with one logical partition.
// Home is false for derived memberships and true for the immutable M2 home.
type Membership struct {
	VectorOrdinal int
	Partition     int
	Home          bool
}

const overlapReplicaPolicyV1 = "boundary_utility_v1"

// ReplicaUtilityClassV1 is the authoritative utility classification for a
// non-home membership.
type ReplicaUtilityClassV1 string

const (
	ReplicaUtilityPositiveGainV1 ReplicaUtilityClassV1 = "positive_gain"
	ReplicaUtilityZeroUtilityV1  ReplicaUtilityClassV1 = "zero_utility"
)

// Replica records the construction reason for one non-home membership. It is
// diagnostic construction metadata only: no held-out query data contributes to
// Gain or Class.
type Replica struct {
	VectorOrdinal int
	Partition     int
	Policy        string
	Gain          int
	Class         ReplicaUtilityClassV1
}

// OverlapResult is canonical (ordinal, partition order) and records budget
// left unused when the capacity/affinity constraints prevent another move.
type OverlapResult struct {
	Memberships          []Membership
	Budget               int
	Used                 int
	Unspent              int
	Capacity             int
	Loads                []int
	EdgeCutBefore        int
	EdgeCutAfter         int
	Useful               int // memberships applied with a positive directed cut reduction
	Filler               int // exact-target memberships added after useful proposals ended
	Replicas             []Replica
	CumulativeUtility    int
	DestinationDiversity []int
}

// OverlapShortfallError reports an exact-build request that could not be
// realized. Its accounting is deliberately machine-readable through fields
// rather than requiring callers to parse an error string.
type OverlapShortfallError struct {
	Requested, Realized, Rejected, Capacity int
}

func (e *OverlapShortfallError) Error() string {
	return fmt.Sprintf("exact overlap shortfall: requested=%d realized=%d rejected=%d capacity=%d", e.Requested, e.Realized, e.Rejected, e.Capacity)
}

// MaxOverlapMembershipsPerVector matches the durable M1 manifest cap. The
// builder treats the cap as a reason to leave budget unspent, never as a
// post-build validation failure.
const MaxOverlapMembershipsPerVector = 16

// BuildOverlap derives bounded non-home memberships from a validated disjoint
// artifact.  Each round observes one immutable membership snapshot, ranks all
// candidates by cut-edge reduction then stable identity, and applies the
// winning proposals in that order.  Consequently scheduler timing cannot
// affect the result.
func BuildOverlap(a Artifact, cfg OverlapConfig) (OverlapResult, error) {
	if cfg.UsefulOnly && cfg.RequireExact {
		return OverlapResult{}, errors.New("useful-only overlap cannot require exact fill")
	}
	if err := ValidateArtifact(a); err != nil {
		return OverlapResult{}, err
	}
	if math.IsNaN(cfg.Ratio) || math.IsInf(cfg.Ratio, 0) || cfg.Ratio < 0 || cfg.Ratio > 1 {
		return OverlapResult{}, errors.New("overlap ratio must be finite in [0,1]")
	}
	n := len(a.IDs)
	budget := int(math.Floor(cfg.Ratio * float64(n)))
	capacity := cfg.Capacity
	if capacity == 0 {
		capacity = a.Metrics.Cap
	}
	if capacity < a.Metrics.Cap {
		return OverlapResult{}, fmt.Errorf("overlap capacity %d below immutable home load cap %d", capacity, a.Metrics.Cap)
	}
	loads := make([]int, a.Config.Partitions)
	members := make([]map[int]struct{}, n)
	incoming := make([][]int, n)
	for i, p := range a.Assignment {
		loads[p]++
		members[i] = map[int]struct{}{p: {}}
		for _, neighbor := range a.Graph.Neighbors[i] {
			incoming[neighbor] = append(incoming[neighbor], i)
		}
	}
	used, useful, filler := 0, 0, 0
	replicas := make([]Replica, 0, budget)
	type proposal struct {
		node, part, gain int
		id               string
	}
	for used < budget {
		// This proposal set is the bulk-synchronous round snapshot.
		ps := make([]proposal, 0, n)
		for i, ns := range a.Graph.Neighbors {
			if len(members[i])-1 >= MaxOverlapMembershipsPerVector {
				continue
			}
			counts := make([]int, a.Config.Partitions)
			for _, j := range ns {
				for p := range members[j] {
					counts[p]++
				}
			}
			for _, j := range incoming[i] {
				for p := range members[j] {
					counts[p]++
				}
			}
			// Rank destinations by the directed cut edges each one newly
			// satisfies, not by raw neighbor-membership count. A plurality
			// destination can have zero marginal reduction once earlier
			// replicas already colocated those neighbors, and picking it first
			// would discard a lower-count destination that still reduces the
			// cut. Only partitions holding at least one neighbor can reduce
			// anything, so counts still bounds the candidate set.
			best, gain := -1, 0
			for p, count := range counts {
				if _, exists := members[i][p]; exists || count == 0 || loads[p] >= capacity {
					continue
				}
				reduction := overlapCutReduction(members, i, p, ns, incoming[i])
				if reduction > gain || reduction == gain && reduction > 0 && (best < 0 || p < best) {
					best, gain = p, reduction
				}
			}
			if best >= 0 {
				ps = append(ps, proposal{i, best, gain, a.IDs[i]})
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
			// Exact M3 experiments declare a global membership target, not a
			// "beneficial edges only" target. Once every cut-reducing proposal
			// is exhausted, deterministically fill remaining legal non-home
			// slots. Adding memberships cannot increase the overlap edge cut.
			if cfg.RequireExact {
				filled, usefulFilled, fillerFilled := fillExactOverlapSlotsWithReplicas(a, members, incoming, loads, capacity, budget-used, &replicas)
				used += filled
				useful += usefulFilled
				filler += fillerFilled
			}
			break
		}
		applied := 0
		for _, p := range ps {
			if used == budget {
				break
			}
			if loads[p.part] >= capacity {
				continue
			}
			if len(members[p.node])-1 >= MaxOverlapMembershipsPerVector {
				continue
			}
			if _, exists := members[p.node][p.part]; exists {
				continue
			}
			gain := overlapCutReduction(members, p.node, p.part, a.Graph.Neighbors[p.node], incoming[p.node])
			if gain == 0 {
				continue
			}
			members[p.node][p.part] = struct{}{}
			loads[p.part]++
			used++
			useful++
			replicas = append(replicas, Replica{VectorOrdinal: p.node, Partition: p.part, Policy: overlapReplicaPolicyV1, Gain: gain, Class: ReplicaUtilityPositiveGainV1})
			applied++
		}
		if applied == 0 {
			if cfg.RequireExact {
				filled, usefulFilled, fillerFilled := fillExactOverlapSlotsWithReplicas(a, members, incoming, loads, capacity, budget-used, &replicas)
				used += filled
				useful += usefulFilled
				filler += fillerFilled
			}
			break
		}
	}
	out := OverlapResult{
		Budget:        budget,
		Used:          used,
		Unspent:       budget - used,
		Capacity:      capacity,
		Loads:         loads,
		EdgeCutBefore: a.Metrics.EdgeCut,
		Useful:        useful,
		Filler:        filler,
		Replicas:      replicas,
	}
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
	sort.Slice(out.Replicas, func(i, j int) bool {
		if out.Replicas[i].VectorOrdinal != out.Replicas[j].VectorOrdinal {
			return out.Replicas[i].VectorOrdinal < out.Replicas[j].VectorOrdinal
		}
		return out.Replicas[i].Partition < out.Replicas[j].Partition
	})
	out.DestinationDiversity = make([]int, a.Config.Partitions)
	destinations := make([]map[int]struct{}, a.Config.Partitions)
	for partition := range destinations {
		destinations[partition] = make(map[int]struct{})
	}
	for _, replica := range out.Replicas {
		out.CumulativeUtility += replica.Gain
		destinations[replica.Partition][a.Assignment[replica.VectorOrdinal]] = struct{}{}
	}
	for partition := range destinations {
		out.DestinationDiversity[partition] = len(destinations[partition])
	}
	out.EdgeCutAfter = overlapEdgeCut(a, out.Memberships)
	if cfg.RequireExact && out.Unspent != 0 {
		return OverlapResult{}, &OverlapShortfallError{Requested: out.Budget, Realized: out.Used, Rejected: out.Unspent, Capacity: out.Capacity}
	}
	if cfg.UsefulOnly && out.Filler != 0 {
		return OverlapResult{}, errors.New("useful-only overlap realized filler replicas")
	}
	if err := ValidateOverlap(a, cfg, out); err != nil {
		return OverlapResult{}, err
	}
	return out, nil
}

// fillExactOverlapSlots deterministically completes an exact global target
// after affinity has no further cut-reducing proposal. IDs are canonical in a
// validated artifact; partition order is stable. A vector's durable cap and
// the declared total-membership cap are both rechecked at application time.
func fillExactOverlapSlots(a Artifact, members []map[int]struct{}, incoming [][]int, loads []int, capacity, remaining int) (used, useful, filler int) {
	return fillExactOverlapSlotsWithReplicas(a, members, incoming, loads, capacity, remaining, nil)
}

func fillExactOverlapSlotsWithReplicas(a Artifact, members []map[int]struct{}, incoming [][]int, loads []int, capacity, remaining int, replicas *[]Replica) (used, useful, filler int) {
	for i := range a.IDs {
		if used == remaining || len(members[i])-1 >= MaxOverlapMembershipsPerVector {
			continue
		}
		for partition := 0; partition < a.Config.Partitions && used < remaining; partition++ {
			if loads[partition] >= capacity || len(members[i])-1 >= MaxOverlapMembershipsPerVector {
				continue
			}
			if _, exists := members[i][partition]; exists {
				continue
			}
			gain := overlapCutReduction(members, i, partition, a.Graph.Neighbors[i], incoming[i])
			class := ReplicaUtilityZeroUtilityV1
			if gain > 0 {
				useful++
				class = ReplicaUtilityPositiveGainV1
			} else {
				filler++
			}
			if replicas != nil {
				*replicas = append(*replicas, Replica{VectorOrdinal: i, Partition: partition, Policy: overlapReplicaPolicyV1, Gain: gain, Class: class})
			}
			members[i][partition] = struct{}{}
			loads[partition]++
			used++
			if used == remaining {
				return used, useful, filler
			}
		}
	}
	return used, useful, filler
}

// overlapCutReduction scores every directed cut edge that membership in
// partition would newly satisfy for node, including edges directed into it.
func overlapCutReduction(members []map[int]struct{}, node, partition int, outgoing, incoming []int) int {
	reduction := 0
	for _, neighbor := range outgoing {
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
	for _, neighbor := range incoming {
		already := false
		for p := range members[neighbor] {
			if _, ok := members[node][p]; ok {
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
	capacity := cfg.Capacity
	if capacity == 0 {
		capacity = a.Metrics.Cap
	}
	if capacity < a.Metrics.Cap {
		return fmt.Errorf("overlap capacity %d below immutable home load cap %d", capacity, a.Metrics.Cap)
	}
	if r.Budget != wantBudget || r.Budget < 0 || r.Used < 0 || r.Used > r.Budget || r.Useful < 0 || r.Filler < 0 || r.Useful+r.Filler != r.Used || r.Unspent != r.Budget-r.Used || r.Capacity != capacity || len(r.Loads) != a.Config.Partitions || len(r.Memberships) != len(a.IDs)+r.Used || len(r.Replicas) != r.Used || len(r.DestinationDiversity) != a.Config.Partitions || r.EdgeCutBefore != a.Metrics.EdgeCut {
		return errors.New("invalid overlap accounting")
	}
	if cfg.UsefulOnly && cfg.RequireExact {
		return errors.New("useful-only overlap cannot require exact fill")
	}
	if cfg.RequireExact && r.Unspent != 0 {
		return &OverlapShortfallError{Requested: r.Budget, Realized: r.Used, Rejected: r.Unspent, Capacity: r.Capacity}
	}
	if cfg.UsefulOnly && (r.Filler != 0 || r.Useful != r.Used) {
		return errors.New("useful-only overlap realized filler replicas")
	}
	seen := make(map[[2]int]struct{}, len(r.Memberships))
	replicas := make(map[[2]int]Replica, len(r.Replicas))
	utility, useful, filler := 0, 0, 0
	destinations := make([]map[int]struct{}, a.Config.Partitions)
	for partition := range destinations {
		destinations[partition] = make(map[int]struct{})
	}
	for i, replica := range r.Replicas {
		if replica.VectorOrdinal < 0 || replica.VectorOrdinal >= len(a.IDs) || replica.Partition < 0 || replica.Partition >= a.Config.Partitions || replica.Policy != overlapReplicaPolicyV1 || replica.Gain < 0 || (replica.Gain == 0 && replica.Class != ReplicaUtilityZeroUtilityV1) || (replica.Gain > 0 && replica.Class != ReplicaUtilityPositiveGainV1) || (i > 0 && (replica.VectorOrdinal < r.Replicas[i-1].VectorOrdinal || replica.VectorOrdinal == r.Replicas[i-1].VectorOrdinal && replica.Partition <= r.Replicas[i-1].Partition)) {
			return errors.New("invalid overlap replica")
		}
		key := [2]int{replica.VectorOrdinal, replica.Partition}
		if _, exists := replicas[key]; exists {
			return errors.New("duplicate overlap replica")
		}
		replicas[key] = replica
		utility += replica.Gain
		if replica.Class == ReplicaUtilityPositiveGainV1 {
			useful++
		} else {
			filler++
		}
		destinations[replica.Partition][a.Assignment[replica.VectorOrdinal]] = struct{}{}
	}
	if utility != r.CumulativeUtility || useful != r.Useful || filler != r.Filler {
		return errors.New("overlap utility accounting mismatch")
	}
	for partition := range destinations {
		if r.DestinationDiversity[partition] != len(destinations[partition]) {
			return errors.New("overlap destination diversity mismatch")
		}
	}
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
			if _, exists := replicas[key]; !exists {
				return errors.New("overlap replica membership is missing")
			}
			perVector[m.VectorOrdinal]++
			if perVector[m.VectorOrdinal] > MaxOverlapMembershipsPerVector {
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
		if n != r.Loads[p] || n > capacity {
			return errors.New("overlap partition capacity exceeded")
		}
	}
	if got := overlapEdgeCut(a, r.Memberships); got != r.EdgeCutAfter || got > r.EdgeCutBefore {
		return errors.New("invalid overlap edge-cut accounting")
	} else if r.CumulativeUtility != r.EdgeCutBefore-got {
		return errors.New("overlap utility does not match edge-cut reduction")
	}
	return nil
}

func overlapEdgeCut(a Artifact, memberships []Membership) int {
	sets := make([]map[int]struct{}, len(a.IDs))
	for i := range sets {
		sets[i] = make(map[int]struct{})
	}
	for _, membership := range memberships {
		if membership.VectorOrdinal >= 0 && membership.VectorOrdinal < len(sets) {
			sets[membership.VectorOrdinal][membership.Partition] = struct{}{}
		}
	}
	cut := 0
	for i, neighbors := range a.Graph.Neighbors {
		for _, neighbor := range neighbors {
			shared := false
			for partition := range sets[i] {
				if _, ok := sets[neighbor][partition]; ok {
					shared = true
					break
				}
			}
			if !shared {
				cut++
			}
		}
	}
	return cut
}
