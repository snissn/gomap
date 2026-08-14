package vectorpartition

import (
	"errors"
	"fmt"
	"math"
)

const (
	// FP32BytesPerDimensionV1 is the current exact-FP32 traversal-row width.
	FP32BytesPerDimensionV1 = 4
	// DefaultFP32DimensionsV1 is the selected 128-d FP32 traversal plane.
	DefaultFP32DimensionsV1 = 128
	// GraphIdentityOverheadPerRowV1 is a fixed per-row charge for HNSW
	// adjacency, level metadata, and stable identity. It is not derived
	// from host LLC or runtime pack inspection.
	GraphIdentityOverheadPerRowV1 = 512
	// SelectedOverlapRatioV1 is the #4142 useful-only overlap cap.
	SelectedOverlapRatioV1 = 0.2
	// SelectedRouterCandidatesV1 is the #4142 c256 router budget.
	SelectedRouterCandidatesV1 = 256
	// SelectedPartitionProbesV1 is the #4142 bounded probe policy.
	SelectedPartitionProbesV1 = 2
	// SelectedSearchableRowsPerPackV1 is the #4142 per-pack membership cap.
	SelectedSearchableRowsPerPackV1 = 7500
	// DefaultTargetHotBytesV1 is the portable #4142 per-pack budget:
	// 7500 rows * (128-dim FP32 + fixed per-row overhead).
	DefaultTargetHotBytesV1 = uint64(SelectedSearchableRowsPerPackV1 * (DefaultFP32DimensionsV1*FP32BytesPerDimensionV1 + GraphIdentityOverheadPerRowV1))
)

// Aliases keep earlier contract names compiling against the selected defaults.
const (
	GraphIdentityOverheadBytesV1 = GraphIdentityOverheadPerRowV1
	SelectedTargetHotBytesV1     = DefaultTargetHotBytesV1
)

// ShardPlanInputV1 is the portable, explicit planner input. Callers must
// supply TargetHotBytes when they want a non-default budget; the planner
// never inspects serving-host LLC.
type ShardPlanInputV1 struct {
	Vectors        int
	Dimensions     int
	OverlapRatio   float64
	Imbalance      float64
	TargetHotBytes uint64
	MaxPartitions  int
}

// ShardPlanRequestV1 is a compatibility alias for ShardPlanInputV1.
type ShardPlanRequestV1 = ShardPlanInputV1

// ShardPlanV1 is the deterministic byte-bounded partition/capacity contract.
type ShardPlanV1 struct {
	Partitions            int     `json:"partitions"`
	HomeCapacity          int     `json:"home_capacity"`
	OverlapCapacity       int     `json:"overlap_capacity"`
	MaxMembershipsPerPack int     `json:"max_memberships_per_pack"`
	RequestedOverlap      int     `json:"requested_overlap"`
	PlannedMemberships    int     `json:"planned_memberships"`
	TargetHotBytes        uint64  `json:"target_hot_bytes"`
	TraversalRowBytes     int     `json:"traversal_row_bytes"`
	GraphIdentityOverhead int     `json:"graph_identity_overhead"`
	PackFixedOverhead     int     `json:"pack_fixed_overhead"`
	MaxPackBytes          uint64  `json:"max_pack_bytes"`
	OverlapRatio          float64 `json:"overlap_ratio"`
	Imbalance             float64 `json:"imbalance"`
	Vectors               int     `json:"vectors"`
	Dimensions            int     `json:"dimensions"`
}

// HomeCap and OverlapRequested are compatibility aliases used by some tests.
func (p ShardPlanV1) HomeCap() int { return p.HomeCapacity }

// DefaultShardPlanInputV1 returns the portable #4142 defaults for one corpus.
func DefaultShardPlanInputV1(vectors, dimensions int) ShardPlanInputV1 {
	return ShardPlanInputV1{
		Vectors:        vectors,
		Dimensions:     dimensions,
		OverlapRatio:   SelectedOverlapRatioV1,
		Imbalance:      DefaultConfig().Imbalance,
		TargetHotBytes: DefaultTargetHotBytesV1,
	}
}

// SelectedShardPlanRequestV1 is a compatibility wrapper around the selected input.
func SelectedShardPlanRequestV1(sourceRows, dimensions int) ShardPlanInputV1 {
	return DefaultShardPlanInputV1(sourceRows, dimensions)
}

// PlanByteBoundedShardsV1 derives partition count and capacities from an
// explicit hot-byte target. It fails closed before any allocation on overflow,
// an undersized target, or an impossible balance.
func PlanByteBoundedShardsV1(in ShardPlanInputV1) (ShardPlanV1, error) {
	if in.MaxPartitions == 0 {
		in.MaxPartitions = maxPartitions
	}
	if in.TargetHotBytes == 0 {
		in.TargetHotBytes = DefaultTargetHotBytesV1
	}
	if in.Vectors < 1 || in.Vectors > maxVectors || in.Dimensions < 1 || in.Dimensions > maxDimensions || in.MaxPartitions < 1 || in.MaxPartitions > maxPartitions {
		return ShardPlanV1{}, errors.New("vectorpartition: invalid shard plan vector/dimension bounds")
	}
	if math.IsNaN(in.OverlapRatio) || math.IsInf(in.OverlapRatio, 0) || in.OverlapRatio < 0 || in.OverlapRatio > 1 {
		return ShardPlanV1{}, errors.New("vectorpartition: overlap ratio must be finite in [0,1]")
	}
	if math.IsNaN(in.Imbalance) || math.IsInf(in.Imbalance, 0) || in.Imbalance < 0 || in.Imbalance > 1 {
		return ShardPlanV1{}, errors.New("vectorpartition: imbalance must be finite in [0,1]")
	}
	if in.Dimensions > math.MaxInt/FP32BytesPerDimensionV1 {
		return ShardPlanV1{}, errors.New("vectorpartition: traversal-row byte overflow")
	}
	traversal := in.Dimensions * FP32BytesPerDimensionV1
	rowBytes, ok := checkedAddInt(traversal, GraphIdentityOverheadPerRowV1)
	if !ok {
		return ShardPlanV1{}, errors.New("vectorpartition: row-byte accounting overflow")
	}
	if in.TargetHotBytes < uint64(rowBytes) {
		return ShardPlanV1{}, fmt.Errorf("vectorpartition: target-hot-bytes %d undersized versus one row %d", in.TargetHotBytes, rowBytes)
	}
	maxMemberships := in.TargetHotBytes / uint64(rowBytes)
	if maxMemberships < 1 || maxMemberships > uint64(math.MaxInt) {
		return ShardPlanV1{}, errors.New("vectorpartition: target-hot-bytes undersized")
	}
	requestedFloat := math.Floor(in.OverlapRatio * float64(in.Vectors))
	if requestedFloat > float64(math.MaxInt) {
		return ShardPlanV1{}, errors.New("vectorpartition: overlap request overflows int")
	}
	requested := int(requestedFloat)
	total, ok := checkedAddInt(in.Vectors, requested)
	if !ok {
		return ShardPlanV1{}, errors.New("vectorpartition: planned memberships overflow")
	}
	maxRows := int(maxMemberships)
	partitions := (total + maxRows - 1) / maxRows
	if partitions < 1 {
		partitions = 1
	}
	for {
		if partitions > in.Vectors || partitions > in.MaxPartitions {
			return ShardPlanV1{}, fmt.Errorf("vectorpartition: impossible shard balance rows=%d target_hot_bytes=%d", in.Vectors, in.TargetHotBytes)
		}
		homeCap := partitionCap(in.Vectors, partitions, in.Imbalance)
		if homeCap < 1 {
			return ShardPlanV1{}, errors.New("vectorpartition: home capacity underflow")
		}
		overlapCap, err := overlapCapacityForRequestedV1(in.Vectors, requested, partitions, homeCap)
		if err != nil {
			return ShardPlanV1{}, err
		}
		if overlapCap <= maxRows && homeCap <= maxRows {
			return ShardPlanV1{
				Partitions:            partitions,
				HomeCapacity:          homeCap,
				OverlapCapacity:       overlapCap,
				MaxMembershipsPerPack: maxRows,
				RequestedOverlap:      requested,
				PlannedMemberships:    total,
				TargetHotBytes:        in.TargetHotBytes,
				TraversalRowBytes:     traversal,
				GraphIdentityOverhead: GraphIdentityOverheadPerRowV1,
				MaxPackBytes:          in.TargetHotBytes,
				OverlapRatio:          in.OverlapRatio,
				Imbalance:             in.Imbalance,
				Vectors:               in.Vectors,
				Dimensions:            in.Dimensions,
			}, nil
		}
		if partitions == in.Vectors || partitions == in.MaxPartitions {
			return ShardPlanV1{}, fmt.Errorf("vectorpartition: impossible shard balance rows=%d target_hot_bytes=%d", in.Vectors, in.TargetHotBytes)
		}
		partitions++
	}
}

func (p ShardPlanV1) input() ShardPlanInputV1 {
	return ShardPlanInputV1{
		Vectors:        p.Vectors,
		Dimensions:     p.Dimensions,
		OverlapRatio:   p.OverlapRatio,
		Imbalance:      p.Imbalance,
		TargetHotBytes: p.TargetHotBytes,
	}
}

func (p ShardPlanV1) request() ShardPlanInputV1 { return p.input() }

// ShardPackSummaryV1 records realized memberships and charged hot bytes for
// one partition pack. Bytes use the planner's portable row charge, not host LLC.
type ShardPackSummaryV1 struct {
	Partition int    `json:"partition"`
	Rows      int    `json:"rows"`
	Bytes     uint64 `json:"bytes"`
}

// SelectedOverlapConfigV1 is the #4143 useful-only overlap contract. Callers
// that need a non-selected ratio overwrite Ratio after construction.
func SelectedOverlapConfigV1(capacity int) OverlapConfig {
	return OverlapConfig{Ratio: SelectedOverlapRatioV1, Capacity: capacity, UsefulOnly: true}
}

// AccountShardPacksV1 converts realized partition loads into per-pack byte
// summaries and fails closed when any pack exceeds the planned budget.
func AccountShardPacksV1(plan ShardPlanV1, loads []int) ([]ShardPackSummaryV1, error) {
	if plan.MaxMembershipsPerPack < 1 || plan.TraversalRowBytes < 1 || len(loads) == 0 || len(loads) > maxPartitions {
		return nil, errors.New("vectorpartition: invalid shard pack accounting inputs")
	}
	rowBytes, ok := checkedAddInt(plan.TraversalRowBytes, plan.GraphIdentityOverhead)
	if !ok {
		return nil, errors.New("vectorpartition: pack row-byte overflow")
	}
	out := make([]ShardPackSummaryV1, len(loads))
	for i, rows := range loads {
		if rows < 0 || rows > plan.MaxMembershipsPerPack || rows > plan.OverlapCapacity {
			return nil, fmt.Errorf("vectorpartition: pack %d rows=%d exceed planned capacity", i, rows)
		}
		hotBytes, ok := mulUint64(uint64(rows), uint64(rowBytes))
		if !ok || hotBytes > plan.TargetHotBytes {
			return nil, fmt.Errorf("vectorpartition: pack %d hot-bytes overflow or exceed target", i)
		}
		out[i] = ShardPackSummaryV1{Partition: i, Rows: rows, Bytes: hotBytes}
	}
	return out, nil
}

// RouterPartitionsFromMembershipsV1 rebuilds router input from final
// membership only. Every realized membership appears once.
func RouterPartitionsFromMembershipsV1(memberships []Membership, values [][]float32, partitions int) ([]RouterPartitionV1, error) {
	if partitions < 1 || partitions > routerMaxPartitions || len(values) == 0 {
		return nil, errors.New("vectorpartition: invalid router membership inputs")
	}
	parts := make([]RouterPartitionV1, partitions)
	for i := range parts {
		parts[i].PartitionID = uint32(i)
	}
	seen := make(map[[2]int]struct{}, len(memberships))
	for _, membership := range memberships {
		if membership.VectorOrdinal < 0 || membership.VectorOrdinal >= len(values) || membership.Partition < 0 || membership.Partition >= partitions {
			return nil, errors.New("vectorpartition: router membership is outside realized bounds")
		}
		key := [2]int{membership.VectorOrdinal, membership.Partition}
		if _, dup := seen[key]; dup {
			return nil, errors.New("vectorpartition: duplicate router membership")
		}
		seen[key] = struct{}{}
		kind := "overlap"
		if membership.Home {
			kind = "home"
		}
		parts[membership.Partition].Vectors = append(parts[membership.Partition].Vectors, RouterVectorV1{
			Ordinal:        uint64(membership.VectorOrdinal),
			Values:         append([]float32(nil), values[membership.VectorOrdinal]...),
			MembershipKind: kind,
		})
	}
	for i, part := range parts {
		if len(part.Vectors) == 0 {
			return nil, fmt.Errorf("vectorpartition: router partition %d has no realized membership", i)
		}
	}
	return parts, nil
}

func overlapCapacityForRequestedV1(rows, requested, partitions, baseCapacity int) (int, error) {
	if rows < 0 || requested < 0 || partitions < 1 || baseCapacity < 0 || rows > math.MaxInt-requested {
		return 0, errors.New("vectorpartition: overlap capacity overflows int")
	}
	total := rows + requested
	capacity := total / partitions
	if total%partitions != 0 {
		if capacity == math.MaxInt {
			return 0, errors.New("vectorpartition: overlap capacity overflows int")
		}
		capacity++
	}
	if baseCapacity > capacity {
		return baseCapacity, nil
	}
	return capacity, nil
}

func checkedAddInt(a, b int) (int, bool) {
	if a < 0 || b < 0 || a > math.MaxInt-b {
		return 0, false
	}
	return a + b, true
}

func mulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
