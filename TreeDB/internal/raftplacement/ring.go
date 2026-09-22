package raftplacement

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const maxTokenV1 = ^uint64(0)

var (
	ErrInvalidTokenRing           = errors.New("raftplacement: invalid token ring")
	ErrInvalidTokenPartitionCount = errors.New("raftplacement: invalid token partition count")
	ErrMissingTokenPartition      = errors.New("raftplacement: missing token partition")
	ErrInvalidTokenPartition      = errors.New("raftplacement: invalid token partition")
	ErrDuplicateTokenPartition    = errors.New("raftplacement: duplicate token partition")
	ErrInvalidTokenRange          = errors.New("raftplacement: invalid token range")
	ErrTokenRangeGap              = errors.New("raftplacement: token range gap")
	ErrTokenRangeOverlap          = errors.New("raftplacement: token range overlap")
	ErrTokenRingNotFull           = errors.New("raftplacement: token ring not full")
	ErrTokenUnassigned            = errors.New("raftplacement: token unassigned")
)

type TokenPartitionID string

type TokenRingPlanOptions struct {
	PartitionCount int
}

type TokenPartitionV1 struct {
	ID      TokenPartitionID
	GroupID raftcluster.GroupID
	Start   uint64
	End     uint64
}

type TokenRingPlanV1 struct {
	Partitions []TokenPartitionV1
}

type ResolvedTokenPartitionV1 struct {
	ID      TokenPartitionID
	GroupID raftcluster.GroupID
	Start   uint64
	End     uint64
}

type ResolvedTokenRingPlanV1 struct {
	Partitions []ResolvedTokenPartitionV1

	partitions []ResolvedTokenPartitionV1
}

// PlanTokenRing builds a deterministic simulation-only token ring over the
// full uint64 token space. It does not attach the plan to a catalog or enable
// token/ring production request routing.
func PlanTokenRing(c ResolvedCatalogV1, opts TokenRingPlanOptions) (ResolvedTokenRingPlanV1, error) {
	if opts.PartitionCount <= 0 {
		return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrInvalidTokenPartitionCount, fmt.Errorf("partition count %d", opts.PartitionCount))
	}
	groups := resolvedGroupIDs(c)
	if len(groups) == 0 {
		return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrMissingGroup, fmt.Errorf("at least one resolved group is required"))
	}

	partitions := make([]TokenPartitionV1, opts.PartitionCount)
	start := uint64(0)
	if opts.PartitionCount == 1 {
		partitions[0] = TokenPartitionV1{
			ID:      tokenPartitionID(0),
			GroupID: groups[0],
			Start:   0,
			End:     maxTokenV1,
		}
	} else {
		baseWidth, remainder := bits.Div64(1, 0, uint64(opts.PartitionCount))
		for i := range partitions {
			width := baseWidth
			if uint64(i) < remainder {
				width++
			}
			end := start + width - 1
			partitions[i] = TokenPartitionV1{
				ID:      tokenPartitionID(i),
				GroupID: groups[i%len(groups)],
				Start:   start,
				End:     end,
			}
			if end != maxTokenV1 {
				start = end + 1
			}
		}
	}

	return ValidateTokenRingPlan(c, TokenRingPlanV1{Partitions: partitions})
}

func ValidateTokenRingPlan(c ResolvedCatalogV1, plan TokenRingPlanV1) (ResolvedTokenRingPlanV1, error) {
	return validateTokenRingPartitions(plan.Partitions, func(id raftcluster.GroupID) bool {
		_, ok := c.Group(id)
		return ok
	})
}

func validateTokenRingPartitions(partitions []TokenPartitionV1, groupExists func(raftcluster.GroupID) bool) (ResolvedTokenRingPlanV1, error) {
	if len(partitions) == 0 {
		return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrMissingTokenPartition, fmt.Errorf("at least one token partition is required"))
	}

	out := make([]ResolvedTokenPartitionV1, 0, len(partitions))
	seenIDs := make(map[TokenPartitionID]struct{}, len(partitions))
	for i, partition := range partitions {
		if err := validateID("token partition id", string(partition.ID)); err != nil {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrInvalidTokenPartition, fmt.Errorf("partition[%d]: %w", i, err))
		}
		if _, exists := seenIDs[partition.ID]; exists {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrDuplicateTokenPartition, fmt.Errorf("partition %q appears more than once", partition.ID))
		}
		seenIDs[partition.ID] = struct{}{}
		if err := validateID("token partition group id", string(partition.GroupID)); err != nil {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrUnknownGroup, fmt.Errorf("partition[%d]: %w", i, err))
		}
		if !groupExists(partition.GroupID) {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrUnknownGroup, fmt.Errorf("partition[%d] references group %q", i, partition.GroupID))
		}
		if partition.Start > partition.End {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrInvalidTokenRange, fmt.Errorf("partition[%d] start=%d end=%d", i, partition.Start, partition.End))
		}
		out = append(out, ResolvedTokenPartitionV1{
			ID:      partition.ID,
			GroupID: partition.GroupID,
			Start:   partition.Start,
			End:     partition.End,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		return out[i].ID < out[j].ID
	})
	if out[0].Start != 0 {
		return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrTokenRangeGap, fmt.Errorf("first partition starts at %d", out[0].Start))
	}
	for i := 1; i < len(out); i++ {
		prev, cur := out[i-1], out[i]
		if prev.End == maxTokenV1 || cur.Start <= prev.End {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrTokenRangeOverlap, fmt.Errorf("partition %q [%d,%d] overlaps %q [%d,%d]", cur.ID, cur.Start, cur.End, prev.ID, prev.Start, prev.End))
		}
		if cur.Start != prev.End+1 {
			return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrTokenRangeGap, fmt.Errorf("partition %q starts at %d after %q ends at %d", cur.ID, cur.Start, prev.ID, prev.End))
		}
	}
	if out[len(out)-1].End != maxTokenV1 {
		return ResolvedTokenRingPlanV1{}, errors.Join(ErrInvalidTokenRing, ErrTokenRingNotFull, fmt.Errorf("last partition ends at %d", out[len(out)-1].End))
	}

	resolvedPartitions := cloneResolvedTokenPartitions(out)
	return ResolvedTokenRingPlanV1{
		Partitions: cloneResolvedTokenPartitions(resolvedPartitions),
		partitions: resolvedPartitions,
	}, nil
}

func (p ResolvedTokenRingPlanV1) ResolveToken(token uint64) (ResolvedTokenPartitionV1, error) {
	partitions := p.partitions
	if len(partitions) == 0 {
		return ResolvedTokenPartitionV1{}, errors.Join(ErrInvalidTokenRing, ErrTokenUnassigned, fmt.Errorf("token %d", token))
	}
	i := sort.Search(len(partitions), func(i int) bool {
		return partitions[i].End >= token
	})
	if i >= len(partitions) || token < partitions[i].Start {
		return ResolvedTokenPartitionV1{}, errors.Join(ErrInvalidTokenRing, ErrTokenUnassigned, fmt.Errorf("token %d", token))
	}
	return partitions[i], nil
}

func (p ResolvedTokenRingPlanV1) GroupPartitionCounts() map[raftcluster.GroupID]int {
	partitions := p.partitions
	counts := make(map[raftcluster.GroupID]int, len(partitions))
	for _, partition := range partitions {
		counts[partition.GroupID]++
	}
	return counts
}

func resolvedGroupIDs(c ResolvedCatalogV1) []raftcluster.GroupID {
	groups := make([]raftcluster.GroupID, 0, len(c.groups))
	for id := range c.groups {
		groups = append(groups, id)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i] < groups[j]
	})
	return groups
}

func tokenPartitionID(i int) TokenPartitionID {
	return TokenPartitionID(fmt.Sprintf("token-%06d", i))
}

func cloneResolvedTokenPartitions(partitions []ResolvedTokenPartitionV1) []ResolvedTokenPartitionV1 {
	return append([]ResolvedTokenPartitionV1(nil), partitions...)
}

func cloneResolvedTokenRingPlan(plan ResolvedTokenRingPlanV1) ResolvedTokenRingPlanV1 {
	partitions := cloneResolvedTokenPartitions(plan.partitions)
	return ResolvedTokenRingPlanV1{
		Partitions: cloneResolvedTokenPartitions(partitions),
		partitions: partitions,
	}
}
