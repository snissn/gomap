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
	Seed                        int64  `json:"seed"`
	BranchFactor                int    `json:"branch_factor"`
	LeafSize                    int    `json:"leaf_size"`
	RepresentativesPerPartition int    `json:"representatives_per_partition"`
	MaxDepth                    int    `json:"max_depth"`
	MaxIterations               int    `json:"max_iterations"`
	MaxVectors                  int    `json:"max_vectors"`
	MaxDimensions               int    `json:"max_dimensions"`
	MaxRepresentatives          int    `json:"max_representatives"`
	MaxScalarWork               int64  `json:"max_scalar_work"`
	MaxRouterBytes              uint64 `json:"max_router_bytes"`
}

func DefaultRouterConfigV1() RouterConfigV1 {
	return RouterConfigV1{
		Seed:                        1,
		BranchFactor:                4,
		LeafSize:                    64,
		RepresentativesPerPartition: 16,
		MaxDepth:                    8,
		MaxIterations:               16,
		MaxVectors:                  routerMaxVectors,
		MaxDimensions:               routerMaxDimensions,
		MaxRepresentatives:          routerMaxRepresentatives,
		MaxScalarWork:               routerDefaultScalarWork,
		MaxRouterBytes:              routerMaxBytes,
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

// RouterHierarchyNodeV1 describes one persisted node. A representative points
// at a leaf and carries its root-to-leaf node path.
type RouterHierarchyNodeV1 struct {
	NodeID       uint32 `json:"node_id"`
	ParentNodeID uint32 `json:"parent_node_id,omitempty"`
	PartitionID  uint32 `json:"partition_id"`
	Depth        uint16 `json:"depth"`
	MemberCount  uint32 `json:"member_count"`
	Leaf         bool   `json:"leaf"`
}

type RouterRepresentativeV1 struct {
	PartitionID   uint32    `json:"partition_id"`
	SourceOrdinal uint64    `json:"source_ordinal"`
	LeafNodeID    uint32    `json:"leaf_node_id"`
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
	error   float64
}

func ValidateRouterConfigV1(cfg RouterConfigV1) error {
	switch {
	case cfg.BranchFactor < 2:
		return errors.New("vectorpartition: router branch factor must be at least 2")
	case cfg.LeafSize < 1:
		return errors.New("vectorpartition: router leaf size must be positive")
	case cfg.RepresentativesPerPartition < 1:
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
	input := append([]RouterPartitionV1(nil), partitions...)
	sort.Slice(input, func(i, j int) bool { return input[i].PartitionID < input[j].PartitionID })
	for i := 1; i < len(input); i++ {
		if input[i].PartitionID == input[i-1].PartitionID {
			return model, fmt.Errorf("vectorpartition: duplicate router partition %d", input[i].PartitionID)
		}
	}

	dimensions := 0
	totalVectors := 0
	totalRepresentativeBudget := 0
	normalized := make([][]routerBuildVectorV1, len(input))
	for partitionOrdinal, partition := range input {
		if len(partition.Vectors) == 0 {
			return model, fmt.Errorf("vectorpartition: router partition %d is empty", partition.PartitionID)
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
		budget := min(cfg.RepresentativesPerPartition, len(vectors))
		totalRepresentativeBudget += budget
		if totalRepresentativeBudget > cfg.MaxRepresentatives {
			return model, fmt.Errorf("vectorpartition: router representatives=%d exceeds limit=%d", totalRepresentativeBudget, cfg.MaxRepresentatives)
		}
	}
	work, ok := checkedRouterWorkV1(normalized, dimensions, cfg)
	if !ok || work > cfg.MaxScalarWork {
		return model, fmt.Errorf("vectorpartition: router scalar work=%d exceeds limit=%d", work, cfg.MaxScalarWork)
	}

	model = RouterModelV1{
		Format:     "treedb_vector_partition_router_v1",
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
			},
			members: members,
			path:    []uint32{nextNodeID},
		}
		nextNodeID++
		root.center, root.error = routerCenterAndErrorV1(vectors, members)
		nodes := []*routerBuildNodeV1{root}
		leaves := []*routerBuildNodeV1{root}
		budget := min(cfg.RepresentativesPerPartition, len(vectors))
		for len(leaves) < budget {
			splitIndex := routerNextSplitV1(leaves, cfg)
			if splitIndex < 0 {
				break
			}
			remaining := budget - len(leaves)
			k := min(cfg.BranchFactor, len(leaves[splitIndex].members))
			k = min(k, remaining+1)
			children, iterations, repairs, err := routerSplitNodeV1(vectors, leaves[splitIndex], k, cfg, &nextNodeID)
			model.Metrics.LloydIterations += iterations
			model.Metrics.EmptyRepairs += repairs
			if err != nil {
				return RouterModelV1{}, fmt.Errorf("vectorpartition: router partition %d: %w", partition.PartitionID, err)
			}
			if len(children) < 2 {
				break
			}
			parent := leaves[splitIndex]
			parent.record.Leaf = false
			leaves = append(leaves[:splitIndex], leaves[splitIndex+1:]...)
			leaves = append(leaves, children...)
			nodes = append(nodes, children...)
		}
		sort.Slice(leaves, func(i, j int) bool { return leaves[i].record.NodeID < leaves[j].record.NodeID })
		for _, leaf := range leaves {
			switch {
			case len(leaf.members) <= cfg.LeafSize:
				model.Metrics.StoppedLeafSize++
			case int(leaf.record.Depth) >= cfg.MaxDepth:
				model.Metrics.StoppedMaxDepth++
			default:
				model.Metrics.StoppedNoSplit++
			}
			sourceOrdinal := routerMedoidOrdinalV1(vectors, leaf.members, leaf.center)
			model.Representatives = append(model.Representatives, RouterRepresentativeV1{
				PartitionID:   partition.PartitionID,
				SourceOrdinal: sourceOrdinal,
				LeafNodeID:    leaf.record.NodeID,
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
		return model.Representatives[i].LeafNodeID < model.Representatives[j].LeafNodeID
	})
	model.Metrics.Representatives = len(model.Representatives)
	model.Metrics.HierarchyNodes = len(model.Nodes)
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
	if model.Format != "treedb_vector_partition_router_v1" {
		return errors.New("vectorpartition: invalid router format")
	}
	if err := ValidateRouterConfigV1(model.Config); err != nil {
		return err
	}
	if model.Dimensions < 1 || model.Dimensions > model.Config.MaxDimensions {
		return fmt.Errorf("vectorpartition: invalid router dimensions %d", model.Dimensions)
	}
	if len(model.Representatives) == 0 || len(model.Representatives) > model.Config.MaxRepresentatives {
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
		model.Metrics.StoppedLeafSize+model.Metrics.StoppedMaxDepth+model.Metrics.StoppedNoSplit != len(model.Representatives) {
		return errors.New("vectorpartition: invalid router build metrics")
	}
	nodes := make(map[uint32]RouterHierarchyNodeV1, len(model.Nodes))
	roots := make(map[uint32]uint32)
	leaves := make(map[uint32]struct{})
	childCounts := make(map[uint32]uint32)
	childMembers := make(map[uint32]uint64)
	totalVectors := uint64(0)
	internalNodes := 0
	for ordinal, node := range model.Nodes {
		if ordinal&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if node.NodeID != uint32(ordinal+1) || node.MemberCount == 0 ||
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
		}
		if node.Leaf {
			leaves[node.NodeID] = struct{}{}
		} else {
			internalNodes++
		}
		nodes[node.NodeID] = node
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
		if childCounts[node.NodeID] < 2 || childMembers[node.NodeID] != uint64(node.MemberCount) {
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
		len(leaves) != len(model.Representatives) {
		return errors.New("vectorpartition: router hierarchy does not match build metrics")
	}
	var previousPartition uint32
	var previousLeaf uint32
	representativesPerPartition := make(map[uint32]int, len(roots))
	representedLeaves := make(map[uint32]struct{}, len(model.Representatives))
	representedSources := make(map[[2]uint64]struct{}, len(model.Representatives))
	for i, representative := range model.Representatives {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if i > 0 && (representative.PartitionID < previousPartition ||
			representative.PartitionID == previousPartition && representative.LeafNodeID <= previousLeaf) {
			return errors.New("vectorpartition: router representatives are not canonically ordered")
		}
		previousPartition, previousLeaf = representative.PartitionID, representative.LeafNodeID
		node, exists := nodes[representative.LeafNodeID]
		if !exists || !node.Leaf || node.PartitionID != representative.PartitionID ||
			node.MemberCount != representative.MemberCount || node.Depth != representative.Depth {
			return fmt.Errorf("vectorpartition: representative leaf %d is inconsistent", representative.LeafNodeID)
		}
		if _, exists := representedLeaves[representative.LeafNodeID]; exists {
			return fmt.Errorf("vectorpartition: duplicate representative leaf %d", representative.LeafNodeID)
		}
		representedLeaves[representative.LeafNodeID] = struct{}{}
		sourceKey := [2]uint64{uint64(representative.PartitionID), representative.SourceOrdinal}
		if _, exists := representedSources[sourceKey]; exists {
			return fmt.Errorf("vectorpartition: duplicate representative source ordinal %d in partition %d", representative.SourceOrdinal, representative.PartitionID)
		}
		representedSources[sourceKey] = struct{}{}
		representativesPerPartition[representative.PartitionID]++
		if representativesPerPartition[representative.PartitionID] > model.Config.RepresentativesPerPartition {
			return fmt.Errorf("vectorpartition: partition %d exceeds representative budget", representative.PartitionID)
		}
		if len(representative.Path) != int(representative.Depth)+1 ||
			len(representative.Path) == 0 ||
			representative.Path[len(representative.Path)-1] != representative.LeafNodeID {
			return fmt.Errorf("vectorpartition: representative leaf %d has invalid hierarchy path", representative.LeafNodeID)
		}
		for pathIndex, nodeID := range representative.Path {
			pathNode, ok := nodes[nodeID]
			if !ok || pathNode.PartitionID != representative.PartitionID || int(pathNode.Depth) != pathIndex {
				return fmt.Errorf("vectorpartition: representative leaf %d has invalid hierarchy node %d", representative.LeafNodeID, nodeID)
			}
			if pathIndex > 0 && pathNode.ParentNodeID != representative.Path[pathIndex-1] {
				return fmt.Errorf("vectorpartition: representative leaf %d has disconnected hierarchy path", representative.LeafNodeID)
			}
		}
		if len(representative.Values) != model.Dimensions {
			return fmt.Errorf("vectorpartition: representative leaf %d dimensions=%d want %d", representative.LeafNodeID, len(representative.Values), model.Dimensions)
		}
		normalized, err := normalizeRouterVectorV1(representative.Values)
		if err != nil {
			return fmt.Errorf("vectorpartition: representative leaf %d: %w", representative.LeafNodeID, err)
		}
		for dimension := range normalized {
			if dimension&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if math.Abs(float64(normalized[dimension]-representative.Values[dimension])) > 1e-5 {
				return fmt.Errorf("vectorpartition: representative leaf %d is not cosine-normalized", representative.LeafNodeID)
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

func checkedRouterWorkV1(partitions [][]routerBuildVectorV1, dimensions int, cfg RouterConfigV1) (int64, bool) {
	var vectorRepresentativePairs int64
	for _, partition := range partitions {
		budget := min(cfg.RepresentativesPerPartition, len(partition))
		if len(partition) != 0 && int64(budget) > math.MaxInt64/int64(len(partition)) {
			return 0, false
		}
		vectorRepresentativePairs += int64(len(partition)) * int64(budget)
	}
	work := vectorRepresentativePairs
	for _, multiplier := range []int{cfg.BranchFactor, cfg.MaxIterations, dimensions} {
		if multiplier != 0 && work > math.MaxInt64/int64(multiplier) {
			return 0, false
		}
		work *= int64(multiplier)
	}
	return work, true
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

func routerNextSplitV1(leaves []*routerBuildNodeV1, cfg RouterConfigV1) int {
	best := -1
	for i, leaf := range leaves {
		if len(leaf.members) <= cfg.LeafSize || len(leaf.members) < 2 || int(leaf.record.Depth) >= cfg.MaxDepth {
			continue
		}
		if best < 0 || len(leaf.members) > len(leaves[best].members) ||
			len(leaf.members) == len(leaves[best].members) && leaf.error > leaves[best].error ||
			len(leaf.members) == len(leaves[best].members) && leaf.error == leaves[best].error && leaf.record.NodeID < leaves[best].record.NodeID {
			best = i
		}
	}
	return best
}

func routerSplitNodeV1(vectors []routerBuildVectorV1, parent *routerBuildNodeV1, k int, cfg RouterConfigV1, nextNodeID *uint32) ([]*routerBuildNodeV1, int, int, error) {
	if k < 2 || k > len(parent.members) {
		return nil, 0, 0, errors.New("invalid hierarchical split width")
	}
	centers := routerInitialCentersV1(vectors, parent, k, cfg.Seed)
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
			centers[centerOrdinal], _ = routerCenterAndErrorV1(vectors, clusterMembers)
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
		center, errorSum := routerCenterAndErrorV1(vectors, members)
		nodeID := *nextNodeID
		*nextNodeID++
		path := append(append([]uint32(nil), parent.path...), nodeID)
		children[centerOrdinal] = &routerBuildNodeV1{
			record: RouterHierarchyNodeV1{
				NodeID:       nodeID,
				ParentNodeID: parent.record.NodeID,
				PartitionID:  parent.record.PartitionID,
				Depth:        parent.record.Depth + 1,
				MemberCount:  uint32(len(members)),
				Leaf:         true,
			},
			members: members,
			path:    path,
			center:  center,
			error:   errorSum,
		}
	}
	sort.Slice(children, func(i, j int) bool {
		left := vectors[children[i].members[0]].ordinal
		right := vectors[children[j].members[0]].ordinal
		return left < right
	})
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
		selected[bestMember] = struct{}{}
		centers = append(centers, append([]float32(nil), vectors[bestMember].values...))
	}
	return centers
}

func routerCenterAndErrorV1(vectors []routerBuildVectorV1, members []int) ([]float32, float64) {
	center := make([]float32, len(vectors[members[0]].values))
	for _, member := range members {
		for dimension, value := range vectors[member].values {
			center[dimension] += value
		}
	}
	var normSquared float64
	for _, value := range center {
		normSquared += float64(value) * float64(value)
	}
	if normSquared == 0 || math.IsInf(normSquared, 0) {
		copy(center, vectors[members[0]].values)
	} else {
		inverseNorm := float32(1 / math.Sqrt(normSquared))
		for dimension := range center {
			center[dimension] *= inverseNorm
		}
	}
	var errorSum float64
	for _, member := range members {
		errorSum += routerCosineDistanceNormalizedV1(vectors[member].values, center)
	}
	return center, errorSum
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
