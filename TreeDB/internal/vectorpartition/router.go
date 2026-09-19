package vectorpartition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
)

const (
	// A 1M-source V1 build can carry its exact 20% overlap membership into
	// router construction without relaxing the bounded envelope.
	routerMaxVectors         = 1_200_000
	routerMaxDimensions      = 4096
	routerMaxPartitions      = 16384
	routerMaxRepresentatives = 1_000_000
	routerMaxDepth           = 64
	routerMaxIterations      = 1024
	routerDefaultScalarWork  = int64(20_000_000_000)
	routerMaxScalarWork      = int64(50_000_000_000)
	routerMaxBytes           = uint64(1 << 30)
)

// RouterConfigV1 contains every control that can change the deterministic
// hierarchical k-means representative set.
type RouterConfigV1 struct {
	Seed                 int64  `json:"seed"`
	BranchFactor         int    `json:"branch_factor"`
	LeafSize             int    `json:"leaf_size"`
	RepresentativeBudget int    `json:"representative_budget"`
	MaxDepth             int    `json:"max_depth"`
	MaxIterations        int    `json:"max_iterations"`
	MaxVectors           int    `json:"max_vectors"`
	MaxDimensions        int    `json:"max_dimensions"`
	MaxRepresentatives   int    `json:"max_representatives"`
	MaxScalarWork        int64  `json:"max_scalar_work"`
	MaxRouterBytes       uint64 `json:"max_router_bytes"`
}

func DefaultRouterConfigV1() RouterConfigV1 {
	return RouterConfigV1{
		Seed:                 1,
		BranchFactor:         4,
		LeafSize:             64,
		RepresentativeBudget: 256,
		MaxDepth:             8,
		MaxIterations:        16,
		MaxVectors:           routerMaxVectors,
		MaxDimensions:        routerMaxDimensions,
		MaxRepresentatives:   routerMaxRepresentatives,
		MaxScalarWork:        routerDefaultScalarWork,
		MaxRouterBytes:       routerMaxBytes,
	}
}

type RouterVectorV1 struct {
	Ordinal        uint64    `json:"ordinal"`
	Values         []float32 `json:"values"`
	MembershipKind string    `json:"membership_kind,omitempty"`
}

type RouterPartitionV1 struct {
	PartitionID uint32           `json:"partition_id"`
	Vectors     []RouterVectorV1 `json:"vectors"`
}

// RouterHierarchyNodeV1 describes one persisted node. Every node, including
// retained internal centers, has exactly one representative.
type RouterHierarchyNodeV1 struct {
	NodeID       uint32 `json:"node_id"`
	ParentNodeID uint32 `json:"parent_node_id,omitempty"`
	PartitionID  uint32 `json:"partition_id"`
	Depth        uint16 `json:"depth"`
	MemberCount  uint32 `json:"member_count"`
	Leaf         bool   `json:"leaf"`
	Budget       uint32 `json:"budget"`
}

type RouterRepresentativeV1 struct {
	PartitionID   uint32    `json:"partition_id"`
	SourceOrdinal uint64    `json:"source_ordinal"`
	NodeID        uint32    `json:"node_id"`
	Depth         uint16    `json:"depth"`
	MemberCount   uint32    `json:"member_count"`
	Path          []uint32  `json:"path"`
	Values        []float32 `json:"values"`
}

type RouterBuildMetricsV1 struct {
	Partitions      int `json:"partitions"`
	Vectors         int `json:"vectors"`
	Representatives int `json:"representatives"`
	HierarchyNodes  int `json:"hierarchy_nodes"`
	LloydIterations int `json:"lloyd_iterations"`
	EmptyRepairs    int `json:"empty_repairs"`
	StoppedLeafSize int `json:"stopped_leaf_size"`
	StoppedMaxDepth int `json:"stopped_max_depth"`
	StoppedNoSplit  int `json:"stopped_no_split"`
	UnusedBudget    int `json:"unused_budget"`
}

type RouterModelV1 struct {
	Format          string                   `json:"format"`
	Config          RouterConfigV1           `json:"config"`
	Dimensions      int                      `json:"dimensions"`
	Nodes           []RouterHierarchyNodeV1  `json:"nodes"`
	Representatives []RouterRepresentativeV1 `json:"representatives"`
	Metrics         RouterBuildMetricsV1     `json:"metrics"`
}

type RouterPartitionScoreV1 struct {
	PartitionID           uint32  `json:"partition_id"`
	Distance              float64 `json:"distance"`
	WinningRepresentative int     `json:"winning_representative"`
	WinningSourceOrdinal  uint64  `json:"winning_source_ordinal"`
}

type RouterRouteResultV1 struct {
	Partitions       []RouterPartitionScoreV1 `json:"partitions"`
	CandidatesScored int                      `json:"candidates_scored"`
}

type routerBuildVectorV1 struct {
	ordinal uint64
	values  []float32
}

type routerBuildNodeV1 struct {
	record  RouterHierarchyNodeV1
	members []int
	path    []uint32
	center  []float32
}

func ValidateRouterConfigV1(cfg RouterConfigV1) error {
	switch {
	case cfg.BranchFactor < 2:
		return errors.New("vectorpartition: router branch factor must be at least 2")
	case cfg.LeafSize < 1:
		return errors.New("vectorpartition: router leaf size must be positive")
	case cfg.RepresentativeBudget < 1 || cfg.RepresentativeBudget > cfg.MaxRepresentatives:
		return errors.New("vectorpartition: router representative budget must be positive")
	case cfg.MaxDepth < 1 || cfg.MaxDepth > routerMaxDepth:
		return fmt.Errorf("vectorpartition: router max depth must be in [1,%d]", routerMaxDepth)
	case cfg.MaxIterations < 1 || cfg.MaxIterations > routerMaxIterations:
		return fmt.Errorf("vectorpartition: router max iterations must be in [1,%d]", routerMaxIterations)
	case cfg.MaxVectors < 1 || cfg.MaxVectors > routerMaxVectors:
		return fmt.Errorf("vectorpartition: router max vectors must be in [1,%d]", routerMaxVectors)
	case cfg.MaxDimensions < 1 || cfg.MaxDimensions > routerMaxDimensions:
		return fmt.Errorf("vectorpartition: router max dimensions must be in [1,%d]", routerMaxDimensions)
	case cfg.MaxRepresentatives < 1 || cfg.MaxRepresentatives > routerMaxRepresentatives:
		return fmt.Errorf("vectorpartition: router max representatives must be in [1,%d]", routerMaxRepresentatives)
	case cfg.MaxScalarWork < 1 || cfg.MaxScalarWork > routerMaxScalarWork:
		return fmt.Errorf("vectorpartition: router max scalar work must be in [1,%d]", routerMaxScalarWork)
	case cfg.MaxRouterBytes < 1 || cfg.MaxRouterBytes > routerMaxBytes:
		return fmt.Errorf("vectorpartition: router max bytes must be in [1,%d]", routerMaxBytes)
	default:
		return nil
	}
}

// BuildRouterV1 deterministically coarsens each partition's canonical final
// membership set into a bounded hierarchy of cosine representatives. A source
// ordinal may occur in more than one partition, but never twice in one.
func BuildRouterV1(partitions []RouterPartitionV1, cfg RouterConfigV1) (RouterModelV1, error) {
	var model RouterModelV1
	if err := ValidateRouterConfigV1(cfg); err != nil {
		return model, err
	}
	if len(partitions) == 0 {
		return model, errors.New("vectorpartition: router requires at least one partition")
	}
	if len(partitions) > routerMaxPartitions {
		return model, fmt.Errorf("vectorpartition: router partitions=%d exceeds limit=%d", len(partitions), routerMaxPartitions)
	}
	if cfg.RepresentativeBudget < len(partitions) {
		return model, errors.New("vectorpartition: global representative budget must reserve one root per domain")
	}
	input := append([]RouterPartitionV1(nil), partitions...)
	sort.Slice(input, func(i, j int) bool { return input[i].PartitionID < input[j].PartitionID })
	for i := 1; i < len(input); i++ {
		if input[i].PartitionID == input[i-1].PartitionID {
			return model, fmt.Errorf("vectorpartition: duplicate router partition %d", input[i].PartitionID)
		}
	}

	dimensions := 0
	totalVectors := 0
	populations := make([]int, len(input))
	normalized := make([][]routerBuildVectorV1, len(input))
	for partitionOrdinal, partition := range input {
		if len(partition.Vectors) == 0 {
			return model, fmt.Errorf("vectorpartition: router partition %d is empty", partition.PartitionID)
		}
		if len(partition.Vectors) > cfg.MaxVectors-totalVectors {
			return model, fmt.Errorf("vectorpartition: router final memberships exceed vector limit=%d", cfg.MaxVectors)
		}
		vectors := append([]RouterVectorV1(nil), partition.Vectors...)
		sort.Slice(vectors, func(i, j int) bool { return vectors[i].Ordinal < vectors[j].Ordinal })
		normalized[partitionOrdinal] = make([]routerBuildVectorV1, len(vectors))
		for vectorOrdinal, vector := range vectors {
			if vectorOrdinal > 0 && vector.Ordinal == vectors[vectorOrdinal-1].Ordinal {
				return model, fmt.Errorf("vectorpartition: duplicate router vector ordinal %d", vector.Ordinal)
			}
			if dimensions == 0 {
				dimensions = len(vector.Values)
				if dimensions == 0 || dimensions > cfg.MaxDimensions {
					return model, fmt.Errorf("vectorpartition: router dimensions=%d outside limit=%d", dimensions, cfg.MaxDimensions)
				}
			}
			if len(vector.Values) != dimensions {
				return model, fmt.Errorf("vectorpartition: router vector ordinal %d dimensions=%d want %d", vector.Ordinal, len(vector.Values), dimensions)
			}
			values, err := normalizeRouterVectorV1(vector.Values)
			if err != nil {
				return model, fmt.Errorf("vectorpartition: router vector ordinal %d: %w", vector.Ordinal, err)
			}
			normalized[partitionOrdinal][vectorOrdinal] = routerBuildVectorV1{ordinal: vector.Ordinal, values: values}
		}
		totalVectors += len(vectors)
		if totalVectors > cfg.MaxVectors {
			return model, fmt.Errorf("vectorpartition: router vectors=%d exceeds limit=%d", totalVectors, cfg.MaxVectors)
		}
		populations[partitionOrdinal] = len(vectors)
	}
	quotas, err := ApportionRouterBudgetV2(populations, cfg.RepresentativeBudget)
	if err != nil {
		return model, err
	}
	work, ok := CheckedRouterScalarWorkV1(populations, dimensions, cfg)
	if !ok {
		return model, errors.New("vectorpartition: router scalar-work bound overflows")
	}
	if work > cfg.MaxScalarWork {
		return model, fmt.Errorf("vectorpartition: router scalar work=%d exceeds limit=%d", work, cfg.MaxScalarWork)
	}

	model = RouterModelV1{
		Format:     "treedb_vector_partition_router_v2",
		Config:     cfg,
		Dimensions: dimensions,
		Metrics: RouterBuildMetricsV1{
			Partitions: len(input),
			Vectors:    totalVectors,
		},
	}
	var nextNodeID uint32 = 1
	for partitionOrdinal, partition := range input {
		vectors := normalized[partitionOrdinal]
		members := make([]int, len(vectors))
		for i := range members {
			members[i] = i
		}
		root := &routerBuildNodeV1{
			record: RouterHierarchyNodeV1{
				NodeID:      nextNodeID,
				PartitionID: partition.PartitionID,
				MemberCount: uint32(len(members)),
				Leaf:        true,
				Budget:      uint32(quotas[partitionOrdinal]),
			},
			members: members,
			path:    []uint32{nextNodeID},
		}
		nextNodeID++
		root.center = routerSphericalCenterV2(vectors, members)
		nodes := []*routerBuildNodeV1{root}
		for cursor := 0; cursor < len(nodes); cursor++ {
			parent := nodes[cursor]
			remaining := int(parent.record.Budget) - 1
			if len(parent.members) <= cfg.LeafSize || int(parent.record.Depth) >= cfg.MaxDepth || remaining < 2 || routerIdenticalMembersV2(vectors, parent.members) {
				continue
			}
			k := min(cfg.BranchFactor, min(len(parent.members), remaining))
			children, iterations, repairs, err := routerSplitNodeV1(vectors, parent, k, cfg, &nextNodeID)
			model.Metrics.LloydIterations += iterations
			model.Metrics.EmptyRepairs += repairs
			if err != nil {
				return RouterModelV1{}, fmt.Errorf("vectorpartition: router partition %d: %w", partition.PartitionID, err)
			}
			if len(children) < 2 {
				continue
			}
			parent.record.Leaf = false
			counts := make([]int, len(children))
			for i, child := range children {
				counts[i] = len(child.members)
			}
			childBudgets, err := ApportionRouterBudgetV2(counts, remaining)
			if err != nil {
				return RouterModelV1{}, err
			}
			for i, child := range children {
				child.record.Budget = uint32(childBudgets[i])
			}
			nodes = append(nodes, children...)
		}
		for _, leaf := range nodes {
			if leaf.record.Leaf {
				switch {
				case len(leaf.members) <= cfg.LeafSize:
					model.Metrics.StoppedLeafSize++
				case int(leaf.record.Depth) >= cfg.MaxDepth:
					model.Metrics.StoppedMaxDepth++
				default:
					model.Metrics.StoppedNoSplit++
				}
			}
			sourceOrdinal := routerMedoidOrdinalV1(vectors, leaf.members, leaf.center)
			model.Representatives = append(model.Representatives, RouterRepresentativeV1{
				PartitionID:   partition.PartitionID,
				SourceOrdinal: sourceOrdinal,
				NodeID:        leaf.record.NodeID,
				Depth:         leaf.record.Depth,
				MemberCount:   leaf.record.MemberCount,
				Path:          append([]uint32(nil), leaf.path...),
				Values:        append([]float32(nil), leaf.center...),
			})
		}
		for _, node := range nodes {
			model.Nodes = append(model.Nodes, node.record)
		}
	}
	sort.Slice(model.Nodes, func(i, j int) bool { return model.Nodes[i].NodeID < model.Nodes[j].NodeID })
	sort.Slice(model.Representatives, func(i, j int) bool {
		if model.Representatives[i].PartitionID != model.Representatives[j].PartitionID {
			return model.Representatives[i].PartitionID < model.Representatives[j].PartitionID
		}
		return model.Representatives[i].NodeID < model.Representatives[j].NodeID
	})
	model.Metrics.Representatives = len(model.Representatives)
	model.Metrics.HierarchyNodes = len(model.Nodes)
	model.Metrics.UnusedBudget = cfg.RepresentativeBudget - len(model.Nodes)
	return model, ValidateRouterModelV1(model)
}

func ValidateRouterModelV1(model RouterModelV1) error {
	return ValidateRouterModelWithContextV1(context.Background(), model)
}

func ValidateRouterModelWithContextV1(ctx context.Context, model RouterModelV1) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if model.Format != "treedb_vector_partition_router_v2" {
		return errors.New("vectorpartition: invalid router format")
	}
	if err := ValidateRouterConfigV1(model.Config); err != nil {
		return err
	}
	if model.Dimensions < 1 || model.Dimensions > model.Config.MaxDimensions {
		return fmt.Errorf("vectorpartition: invalid router dimensions %d", model.Dimensions)
	}
	if len(model.Representatives) == 0 || len(model.Representatives) > model.Config.RepresentativeBudget {
		return fmt.Errorf("vectorpartition: invalid router representative count %d", len(model.Representatives))
	}
	if len(model.Nodes) == 0 || len(model.Nodes) > 2*model.Config.MaxRepresentatives {
		return fmt.Errorf("vectorpartition: invalid router hierarchy node count %d", len(model.Nodes))
	}
	if model.Metrics.Partitions < 1 ||
		model.Metrics.Vectors < model.Metrics.Partitions ||
		model.Metrics.Vectors > model.Config.MaxVectors ||
		model.Metrics.Representatives != len(model.Representatives) ||
		model.Metrics.HierarchyNodes != len(model.Nodes) ||
		model.Metrics.LloydIterations < 0 ||
		model.Metrics.EmptyRepairs < 0 ||
		model.Metrics.StoppedLeafSize < 0 ||
		model.Metrics.StoppedMaxDepth < 0 ||
		model.Metrics.StoppedNoSplit < 0 ||
		model.Metrics.UnusedBudget != model.Config.RepresentativeBudget-len(model.Representatives) {
		return errors.New("vectorpartition: invalid router build metrics")
	}
	nodes := make(map[uint32]RouterHierarchyNodeV1, len(model.Nodes))
	roots := make(map[uint32]uint32)
	leaves := make(map[uint32]struct{})
	childCounts := make(map[uint32]uint32)
	childMembers := make(map[uint32]uint64)
	childBudgets := make(map[uint32]uint64)
	children := make(map[uint32][]RouterHierarchyNodeV1)
	totalVectors := uint64(0)
	internalNodes := 0
	for ordinal, node := range model.Nodes {
		if ordinal&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if node.NodeID != uint32(ordinal+1) || node.MemberCount == 0 || node.Budget < 1 || node.Budget > uint32(model.Config.RepresentativeBudget) ||
			int(node.Depth) > model.Config.MaxDepth {
			return errors.New("vectorpartition: invalid router hierarchy node")
		}
		if _, exists := nodes[node.NodeID]; exists {
			return fmt.Errorf("vectorpartition: duplicate router hierarchy node %d", node.NodeID)
		}
		if node.ParentNodeID == 0 {
			if node.Depth != 0 {
				return fmt.Errorf("vectorpartition: router root %d has nonzero depth", node.NodeID)
			}
			if _, exists := roots[node.PartitionID]; exists {
				return fmt.Errorf("vectorpartition: duplicate router partition root %d", node.PartitionID)
			}
			roots[node.PartitionID] = node.NodeID
			totalVectors += uint64(node.MemberCount)
		} else {
			parent, exists := nodes[node.ParentNodeID]
			if !exists || parent.PartitionID != node.PartitionID || node.Depth != parent.Depth+1 {
				return fmt.Errorf("vectorpartition: router hierarchy node %d has invalid parent", node.NodeID)
			}
			childCounts[node.ParentNodeID]++
			childMembers[node.ParentNodeID] += uint64(node.MemberCount)
			childBudgets[node.ParentNodeID] += uint64(node.Budget)
		}
		children[node.ParentNodeID] = append(children[node.ParentNodeID], node)
		if node.Leaf {
			leaves[node.NodeID] = struct{}{}
		} else {
			internalNodes++
		}
		nodes[node.NodeID] = node
	}
	// Validate the same allocation at roots and every split. A valid digest must
	// not bless altered quotas that happen to stay below the global ceiling.
	for parentID, siblings := range children {
		if err := ctx.Err(); err != nil {
			return err
		}
		populations := make([]int, len(siblings))
		for i, child := range siblings {
			populations[i] = int(child.MemberCount)
		}
		budget := model.Config.RepresentativeBudget
		if parentID != 0 {
			budget = int(nodes[parentID].Budget) - 1
		}
		quotas, err := ApportionRouterBudgetV2(populations, budget)
		if err != nil {
			return err
		}
		for i, child := range siblings {
			if int(child.Budget) != quotas[i] {
				return errors.New("vectorpartition: hierarchy budget differs from canonical apportionment")
			}
		}
	}
	for ordinal, node := range model.Nodes {
		if ordinal&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if node.Leaf {
			if childCounts[node.NodeID] != 0 {
				return fmt.Errorf("vectorpartition: router leaf %d has children", node.NodeID)
			}
			continue
		}
		if childCounts[node.NodeID] < 2 || childCounts[node.NodeID] > uint32(model.Config.BranchFactor) || childMembers[node.NodeID] != uint64(node.MemberCount) || childBudgets[node.NodeID] >= uint64(node.Budget) {
			return fmt.Errorf("vectorpartition: router hierarchy node %d has invalid child totals", node.NodeID)
		}
	}
	maxIterations := int64(internalNodes) * int64(model.Config.MaxIterations)
	maxRepairs := maxIterations * int64(model.Config.BranchFactor-1)
	if int64(model.Metrics.LloydIterations) < int64(internalNodes) ||
		int64(model.Metrics.LloydIterations) > maxIterations ||
		int64(model.Metrics.EmptyRepairs) > maxRepairs {
		return errors.New("vectorpartition: router iteration metrics do not match hierarchy")
	}
	if len(roots) != model.Metrics.Partitions || totalVectors != uint64(model.Metrics.Vectors) ||
		len(model.Nodes) != len(model.Representatives) || model.Metrics.StoppedLeafSize+model.Metrics.StoppedMaxDepth+model.Metrics.StoppedNoSplit != len(leaves) {
		return errors.New("vectorpartition: router hierarchy does not match build metrics")
	}
	var previousPartition uint32
	var previousNode uint32
	representativesPerPartition := make(map[uint32]int, len(roots))
	representedNodes := make(map[uint32]struct{}, len(model.Representatives))
	for i, representative := range model.Representatives {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if i > 0 && (representative.PartitionID < previousPartition ||
			representative.PartitionID == previousPartition && representative.NodeID <= previousNode) {
			return errors.New("vectorpartition: router representatives are not canonically ordered")
		}
		previousPartition, previousNode = representative.PartitionID, representative.NodeID
		node, exists := nodes[representative.NodeID]
		if !exists || node.PartitionID != representative.PartitionID ||
			node.MemberCount != representative.MemberCount || node.Depth != representative.Depth {
			return fmt.Errorf("vectorpartition: representative node %d is inconsistent", representative.NodeID)
		}
		if _, exists := representedNodes[representative.NodeID]; exists {
			return fmt.Errorf("vectorpartition: duplicate representative node %d", representative.NodeID)
		}
		representedNodes[representative.NodeID] = struct{}{}
		representativesPerPartition[representative.PartitionID]++
		if representativesPerPartition[representative.PartitionID] > int(nodes[roots[representative.PartitionID]].Budget) {
			return fmt.Errorf("vectorpartition: partition %d exceeds representative budget", representative.PartitionID)
		}
		if len(representative.Path) != int(representative.Depth)+1 ||
			len(representative.Path) == 0 ||
			representative.Path[len(representative.Path)-1] != representative.NodeID {
			return fmt.Errorf("vectorpartition: representative node %d has invalid hierarchy path", representative.NodeID)
		}
		for pathIndex, nodeID := range representative.Path {
			pathNode, ok := nodes[nodeID]
			if !ok || pathNode.PartitionID != representative.PartitionID || int(pathNode.Depth) != pathIndex {
				return fmt.Errorf("vectorpartition: representative node %d has invalid hierarchy node %d", representative.NodeID, nodeID)
			}
			if pathIndex > 0 && pathNode.ParentNodeID != representative.Path[pathIndex-1] {
				return fmt.Errorf("vectorpartition: representative node %d has disconnected hierarchy path", representative.NodeID)
			}
		}
		if len(representative.Values) != model.Dimensions {
			return fmt.Errorf("vectorpartition: representative node %d dimensions=%d want %d", representative.NodeID, len(representative.Values), model.Dimensions)
		}
		normalized, err := normalizeRouterVectorV1(representative.Values)
		if err != nil {
			return fmt.Errorf("vectorpartition: representative node %d: %w", representative.NodeID, err)
		}
		for dimension := range normalized {
			if dimension&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if math.Abs(float64(normalized[dimension]-representative.Values[dimension])) > 1e-5 {
				return fmt.Errorf("vectorpartition: representative node %d is not cosine-normalized", representative.NodeID)
			}
		}
	}
	if len(representativesPerPartition) != len(roots) {
		return errors.New("vectorpartition: router partition lacks a representative")
	}
	return ctx.Err()
}

func CanonicalRouterJSONV1(model RouterModelV1) ([]byte, error) {
	return CanonicalRouterJSONWithContextV1(context.Background(), model)
}

func CanonicalRouterJSONWithContextV1(ctx context.Context, model RouterModelV1) ([]byte, error) {
	var raw bytes.Buffer
	if err := writeCanonicalRouterJSONWithContextV1(ctx, &raw, model); err != nil {
		return nil, err
	}
	return raw.Bytes(), nil
}

func RouterDigestV1(model RouterModelV1) (string, error) {
	return RouterDigestWithContextV1(context.Background(), model)
}

func RouterDigestWithContextV1(ctx context.Context, model RouterModelV1) (string, error) {
	hash := sha256.New()
	if err := writeCanonicalRouterJSONWithContextV1(ctx, hash, model); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeCanonicalRouterJSONWithContextV1(ctx context.Context, dst io.Writer, model RouterModelV1) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ValidateRouterModelWithContextV1(ctx, model); err != nil {
		return err
	}
	writeString := func(value string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := io.WriteString(dst, value)
		return err
	}
	writeValue := func(value any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err = dst.Write(raw)
		return err
	}
	if err := writeString(`{"format":`); err != nil {
		return err
	}
	if err := writeValue(model.Format); err != nil {
		return err
	}
	if err := writeString(`,"config":`); err != nil {
		return err
	}
	if err := writeValue(model.Config); err != nil {
		return err
	}
	if err := writeString(`,"dimensions":` + strconv.Itoa(model.Dimensions) + `,"nodes":[`); err != nil {
		return err
	}
	for ordinal, node := range model.Nodes {
		if ordinal > 0 {
			if err := writeString(","); err != nil {
				return err
			}
		}
		if err := writeValue(node); err != nil {
			return err
		}
	}
	if err := writeString(`],"representatives":[`); err != nil {
		return err
	}
	for ordinal, representative := range model.Representatives {
		if ordinal > 0 {
			if err := writeString(","); err != nil {
				return err
			}
		}
		if err := writeValue(representative); err != nil {
			return err
		}
	}
	if err := writeString(`],"metrics":`); err != nil {
		return err
	}
	if err := writeValue(model.Metrics); err != nil {
		return err
	}
	if err := writeString("}"); err != nil {
		return err
	}
	return ctx.Err()
}

// RouteExactV1 is the correctness oracle. Because it scores every persisted
// representative, the explicit candidate budget must cover the entire model.
func RouteExactV1(model RouterModelV1, query []float32, candidateBudget, partitionProbes int) (RouterRouteResultV1, error) {
	var result RouterRouteResultV1
	if err := ValidateRouterModelV1(model); err != nil {
		return result, err
	}
	if candidateBudget < len(model.Representatives) {
		return result, fmt.Errorf("vectorpartition: exact router candidate budget=%d is smaller than representatives=%d", candidateBudget, len(model.Representatives))
	}
	if partitionProbes < 1 {
		return result, errors.New("vectorpartition: partition probes must be positive")
	}
	if partitionProbes > model.Metrics.Partitions {
		return result, fmt.Errorf("vectorpartition: partition probes=%d exceeds partitions=%d", partitionProbes, model.Metrics.Partitions)
	}
	normalized, err := normalizeRouterVectorV1(query)
	if err != nil {
		return result, fmt.Errorf("vectorpartition: router query: %w", err)
	}
	if len(normalized) != model.Dimensions {
		return result, fmt.Errorf("vectorpartition: router query dimensions=%d want %d", len(normalized), model.Dimensions)
	}
	best := make(map[uint32]RouterPartitionScoreV1)
	for ordinal, representative := range model.Representatives {
		distance := routerCosineDistanceNormalizedV1(normalized, representative.Values)
		current, exists := best[representative.PartitionID]
		if !exists || distance < current.Distance ||
			distance == current.Distance && ordinal < current.WinningRepresentative {
			best[representative.PartitionID] = RouterPartitionScoreV1{
				PartitionID:           representative.PartitionID,
				Distance:              distance,
				WinningRepresentative: ordinal,
				WinningSourceOrdinal:  representative.SourceOrdinal,
			}
		}
	}
	for _, score := range best {
		result.Partitions = append(result.Partitions, score)
	}
	sort.Slice(result.Partitions, func(i, j int) bool {
		if result.Partitions[i].Distance != result.Partitions[j].Distance {
			return result.Partitions[i].Distance < result.Partitions[j].Distance
		}
		return result.Partitions[i].PartitionID < result.Partitions[j].PartitionID
	})
	if len(result.Partitions) > partitionProbes {
		result.Partitions = result.Partitions[:partitionProbes]
	}
	result.CandidatesScored = len(model.Representatives)
	return result, nil
}

// CheckedRouterScalarWorkV1 bounds every coordinate evaluated by a cosine
// distance during construction. Memberships are disjoint within a hierarchy
// depth. Along a root-to-leaf path, a width-k split consumes at least k quota
// tokens and k-1 members before any child can continue.
func CheckedRouterScalarWorkV1(populations []int, dimensions int, cfg RouterConfigV1) (int64, bool) {
	if len(populations) == 0 || dimensions < 1 || cfg.BranchFactor < 2 || cfg.LeafSize < 1 || cfg.MaxDepth < 1 || cfg.MaxIterations < 1 {
		return 0, false
	}
	quotas, err := ApportionRouterBudgetV2(populations, cfg.RepresentativeBudget)
	if err != nil {
		return 0, false
	}
	multiply := func(left, right uint64) (uint64, bool) {
		if right != 0 && left > math.MaxInt64/right {
			return 0, false
		}
		return left * right, true
	}
	add := func(left, right uint64) (uint64, bool) {
		if right > math.MaxInt64 || left > math.MaxInt64-right {
			return 0, false
		}
		return left + right, true
	}
	splitCost := func(branch int) (uint64, bool) {
		width := uint64(branch)
		initialization, ok := multiply(width, width+1)
		if !ok {
			return 0, false
		}
		// Include a possible final scan where distinct FP32 centers have zero
		// cosine distance and farthest-first stops before its requested width.
		initialization /= 2 // 1 + ... + branch farthest-first distances.
		perIteration, ok := multiply(2*width-1, uint64(cfg.MaxIterations))
		if !ok {
			return 0, false
		}
		return add(initialization, perIteration) // assignment plus empty repair.
	}
	addSplitCosts := func(total uint64, count, branch int) (uint64, bool) {
		if count == 0 {
			return total, true
		}
		cost, ok := splitCost(branch)
		if !ok {
			return 0, false
		}
		cost, ok = multiply(uint64(count), cost)
		if !ok {
			return 0, false
		}
		return add(total, cost)
	}
	var work uint64
	for i, population := range populations {
		quota := quotas[i]
		maxSplits := min(cfg.MaxDepth, min((quota-1)/2, max(0, population-cfg.LeafSize)))
		maxBranch := min(cfg.BranchFactor, population)
		terminalSelection := uint64(0)
		if maxSplits > 0 {
			terminalSelection = 1 // possible failed split after the last represented level.
		}
		distancesPerVector := uint64(1) + terminalSelection // root medoid pass.
		for splits := 1; splits <= maxSplits; splits++ {
			candidate := uint64(splits+1) + terminalSelection // one medoid pass per represented level.
			extraCapacity := maxBranch - 2
			prefixExtraCapacity := population - cfg.LeafSize - splits
			if splits == 1 {
				prefixExtraCapacity = 0
			} else if extraCapacity <= prefixExtraCapacity/(splits-1) {
				prefixExtraCapacity = (splits - 1) * extraCapacity
			}
			extraWidth := min(quota-1-2*splits, min(population-1-splits, extraCapacity+prefixExtraCapacity))
			// Split cost is convex in width. Subject to widths in [2,maxBranch]
			// and quota/member-feasible sums, its maximum saturates all but at
			// most one split at an endpoint. Put a saturated split last to respect
			// the tighter continuation requirement on every preceding split.
			if maxBranch == 2 {
				var ok bool
				candidate, ok = addSplitCosts(candidate, splits, 2)
				if !ok {
					return 0, false
				}
			} else {
				saturated := extraWidth / extraCapacity
				remainder := extraWidth % extraCapacity
				var ok bool
				candidate, ok = addSplitCosts(candidate, saturated, maxBranch)
				if !ok {
					return 0, false
				}
				remaining := splits - saturated
				if remainder > 0 {
					candidate, ok = addSplitCosts(candidate, 1, 2+remainder)
					if !ok {
						return 0, false
					}
					remaining--
				}
				candidate, ok = addSplitCosts(candidate, remaining, 2)
				if !ok {
					return 0, false
				}
			}
			if candidate > distancesPerVector {
				distancesPerVector = candidate
			}
		}
		domainWork, ok := multiply(uint64(population), distancesPerVector)
		if !ok {
			return 0, false
		}
		domainWork, ok = multiply(domainWork, uint64(dimensions))
		if !ok {
			return 0, false
		}
		work, ok = add(work, domainWork)
		if !ok {
			return 0, false
		}
	}
	return int64(work), true
}

func normalizeRouterVectorV1(values []float32) ([]float32, error) {
	if len(values) == 0 {
		return nil, errors.New("vector is empty")
	}
	var normSquared float64
	for _, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("vector contains a non-finite value")
		}
		normSquared += float64(value) * float64(value)
	}
	if normSquared == 0 || math.IsInf(normSquared, 0) {
		return nil, errors.New("vector norm is not finite and positive")
	}
	inverseNorm := float32(1 / math.Sqrt(normSquared))
	if math.IsInf(float64(inverseNorm), 0) || inverseNorm == 0 {
		return nil, errors.New("vector norm cannot be represented safely")
	}
	normalized := make([]float32, len(values))
	for i, value := range values {
		normalized[i] = value * inverseNorm
	}
	return normalized, nil
}

func routerSplitNodeV1(vectors []routerBuildVectorV1, parent *routerBuildNodeV1, k int, cfg RouterConfigV1, nextNodeID *uint32) ([]*routerBuildNodeV1, int, int, error) {
	if k < 2 || k > len(parent.members) {
		return nil, 0, 0, errors.New("invalid hierarchical split width")
	}
	centers := routerInitialCentersV1(vectors, parent, k, cfg.Seed)
	k = len(centers)
	if k < 2 {
		return nil, 0, 0, nil
	}
	assignments := make([]int, len(parent.members))
	for i := range assignments {
		assignments[i] = -1
	}
	iterations := 0
	repairs := 0
	for iterations < cfg.MaxIterations {
		iterations++
		changed := false
		counts := make([]int, k)
		for memberOrdinal, vectorIndex := range parent.members {
			bestCenter := 0
			bestDistance := routerCosineDistanceNormalizedV1(vectors[vectorIndex].values, centers[0])
			for centerOrdinal := 1; centerOrdinal < k; centerOrdinal++ {
				distance := routerCosineDistanceNormalizedV1(vectors[vectorIndex].values, centers[centerOrdinal])
				if distance < bestDistance {
					bestCenter, bestDistance = centerOrdinal, distance
				}
			}
			if assignments[memberOrdinal] != bestCenter {
				assignments[memberOrdinal] = bestCenter
				changed = true
			}
			counts[bestCenter]++
		}
		for empty := 0; empty < k; empty++ {
			if counts[empty] != 0 {
				continue
			}
			donorMember := -1
			var donorDistance float64
			for memberOrdinal, assigned := range assignments {
				if counts[assigned] <= 1 {
					continue
				}
				distance := routerCosineDistanceNormalizedV1(vectors[parent.members[memberOrdinal]].values, centers[assigned])
				if donorMember < 0 || distance > donorDistance ||
					distance == donorDistance && vectors[parent.members[memberOrdinal]].ordinal < vectors[parent.members[donorMember]].ordinal {
					donorMember, donorDistance = memberOrdinal, distance
				}
			}
			if donorMember < 0 {
				return nil, iterations, repairs, errors.New("unable to repair empty router cluster")
			}
			counts[assignments[donorMember]]--
			assignments[donorMember] = empty
			counts[empty]++
			repairs++
			changed = true
		}
		for centerOrdinal := range centers {
			clusterMembers := make([]int, 0, counts[centerOrdinal])
			for memberOrdinal, assigned := range assignments {
				if assigned == centerOrdinal {
					clusterMembers = append(clusterMembers, parent.members[memberOrdinal])
				}
			}
			centers[centerOrdinal] = routerSphericalCenterV2(vectors, clusterMembers)
		}
		if !changed {
			break
		}
	}
	children := make([]*routerBuildNodeV1, k)
	for centerOrdinal := 0; centerOrdinal < k; centerOrdinal++ {
		members := make([]int, 0)
		for memberOrdinal, assigned := range assignments {
			if assigned == centerOrdinal {
				members = append(members, parent.members[memberOrdinal])
			}
		}
		sort.Slice(members, func(i, j int) bool { return vectors[members[i]].ordinal < vectors[members[j]].ordinal })
		center := routerSphericalCenterV2(vectors, members)
		children[centerOrdinal] = &routerBuildNodeV1{
			record: RouterHierarchyNodeV1{
				ParentNodeID: parent.record.NodeID,
				PartitionID:  parent.record.PartitionID,
				Depth:        parent.record.Depth + 1,
				MemberCount:  uint32(len(members)),
				Leaf:         true,
			},
			members: members,
			center:  center,
		}
	}
	sort.Slice(children, func(i, j int) bool {
		left := vectors[children[i].members[0]].ordinal
		right := vectors[children[j].members[0]].ordinal
		return left < right
	})
	// Assign identity after canonical sibling ordering. The allocator and reopen
	// validator use node order for largest-remainder ties, not seed-center order.
	for _, child := range children {
		child.record.NodeID = *nextNodeID
		*nextNodeID++
		child.path = append(append([]uint32(nil), parent.path...), child.record.NodeID)
	}
	return children, iterations, repairs, nil
}

func routerInitialCentersV1(vectors []routerBuildVectorV1, node *routerBuildNodeV1, k int, seed int64) [][]float32 {
	centers := make([][]float32, 0, k)
	mixed := routerMix64V1(uint64(seed) ^ uint64(node.record.PartitionID)<<32 ^ uint64(node.record.NodeID))
	first := int(mixed % uint64(len(node.members)))
	centers = append(centers, append([]float32(nil), vectors[node.members[first]].values...))
	selected := map[int]struct{}{node.members[first]: {}}
	for len(centers) < k {
		bestMember := -1
		var bestDistance float64
		for _, member := range node.members {
			if _, exists := selected[member]; exists {
				continue
			}
			duplicate := false
			for _, center := range centers {
				if slices.Equal(vectors[member].values, center) {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			minDistance := routerCosineDistanceNormalizedV1(vectors[member].values, centers[0])
			for _, center := range centers[1:] {
				distance := routerCosineDistanceNormalizedV1(vectors[member].values, center)
				if distance < minDistance {
					minDistance = distance
				}
			}
			if bestMember < 0 || minDistance > bestDistance ||
				minDistance == bestDistance && vectors[member].ordinal < vectors[bestMember].ordinal {
				bestMember, bestDistance = member, minDistance
			}
		}
		if bestMember < 0 || bestDistance <= 0 {
			break
		}
		selected[bestMember] = struct{}{}
		centers = append(centers, append([]float32(nil), vectors[bestMember].values...))
	}
	return centers
}

func routerSphericalCenterV2(vectors []routerBuildVectorV1, members []int) []float32 {
	center := make([]float32, len(vectors[members[0]].values))
	// Accumulate admitted FP32 inputs in FP64 before normalization, especially
	// for nearly cancelling directions. Member order is canonical.
	sums := make([]float64, len(center))
	for _, member := range members {
		for dimension, value := range vectors[member].values {
			sums[dimension] += float64(value)
		}
	}
	var normSquared float64
	for _, value := range sums {
		normSquared += value * value
	}
	if normSquared == 0 || math.IsInf(normSquared, 0) {
		copy(center, vectors[members[0]].values)
	} else {
		inverseNorm := 1 / math.Sqrt(normSquared)
		for dimension := range center {
			center[dimension] = float32(sums[dimension] * inverseNorm)
		}
	}
	return center
}

func routerMedoidOrdinalV1(vectors []routerBuildVectorV1, members []int, center []float32) uint64 {
	best := members[0]
	bestDistance := routerCosineDistanceNormalizedV1(vectors[best].values, center)
	for _, member := range members[1:] {
		distance := routerCosineDistanceNormalizedV1(vectors[member].values, center)
		if distance < bestDistance || distance == bestDistance && vectors[member].ordinal < vectors[best].ordinal {
			best, bestDistance = member, distance
		}
	}
	return vectors[best].ordinal
}

func routerCosineDistanceNormalizedV1(left, right []float32) float64 {
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	distance := 1 - dot
	if distance < 0 && distance > -1e-6 {
		return 0
	}
	return distance
}

func routerMix64V1(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
