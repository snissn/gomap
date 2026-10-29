package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

// columnGraphHNSWBuilder owns temporary graph construction and bounded domain
// graph primitives. It has no collection, registry, public search, persistence,
// scalar cache or runtime lease.
type columnGraphHNSWBuilder struct {
	name                             string
	field                            string
	metric                           VectorMetric
	encoding                         VectorIndexEncoding
	dimensions                       int
	m                                int
	efConstruction                   int
	mu                               sync.RWMutex
	nodes                            []vectorIndexNode
	vectorRows                       []float32
	constructionRowsFixed            bool
	currentNode                      map[string]int
	entry                            int
	maxLevel                         int
	insertScratch                    vectorIndexSearchScratch
	searchScratch                    sync.Pool
	frozenPrefixBatches              uint64
	frozenPrefixReciprocalPrunes     uint64
	frozenPrefixReciprocalPruneEdges uint64
	frozenPrefixIndexedDotBatches    uint64
	frozenPrefixHeapRowStores        uint64
	constructionWorkers              int
	parallelReciprocalLinks          bool
	insertQuantScratch               []int8
	constructionTrace                *vectorIndexConstructionTraceV1
	decisionObserver                 *vectorIndexConstructionDecisionObserverV1
	layer0ConstructionPolicy         *vectorIndexLayer0ConstructionPolicyV1
	qualityPostfillCandidates        map[int]map[int]struct{}
	mutationObserver                 columnGraphHNSWMutationObserver
}

type columnGraphHNSWBuildOptions struct {
	Name           string
	Field          string
	Metric         VectorMetric
	Encoding       VectorIndexEncoding
	Dimensions     int
	M              int
	EfConstruction int
}

type columnGraphHNSWMutationObserver interface {
	prepareSearchViewForMutationLocked()
	capturePartitionLiveDomainCurrentNodeV2Locked([]byte)
	capturePartitionLiveDomainNodeV2Locked(int)
	markVectorNodeDirtyLocked(int)
	markVectorDocDirtyLocked([]byte)
	markVectorMetaDirtyLocked()
	markGraphChangedLocked()
}

func newColumnGraphHNSWBuilder(opts columnGraphHNSWBuildOptions) (*columnGraphHNSWBuilder, error) {
	if opts.Metric != VectorMetricCosine || opts.Encoding != VectorIndexEncodingFloat32 {
		return nil, errors.New("collections: ColumnGraph HNSW construction requires cosine float32 vectors")
	}
	if opts.Dimensions < 0 {
		return nil, errors.New("collections: graph dimensions cannot be negative")
	}
	if opts.M <= 0 {
		opts.M = defaultVectorIndexM
	}
	if opts.EfConstruction <= 0 {
		opts.EfConstruction = defaultVectorIndexEfConstruction
	}
	if opts.EfConstruction < opts.M {
		opts.EfConstruction = opts.M
	}
	return &columnGraphHNSWBuilder{name: opts.Name, field: opts.Field, metric: opts.Metric, encoding: opts.Encoding, dimensions: opts.Dimensions, m: opts.M, efConstruction: opts.EfConstruction, currentNode: make(map[string]int), entry: -1, maxLevel: -1}, nil
}

func columnGraphHNSWBuildOptionsFromDefinition(def VectorIndexDefinition) columnGraphHNSWBuildOptions {
	return columnGraphHNSWBuildOptions{Name: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction}
}

func (idx *columnGraphHNSWBuilder) insertBatch(ctx context.Context, ids [][]byte, vectors [][]float32) error {
	return idx.insertVectorBatchWithContextLocked(ctx, ids, vectors)
}

// snapshotAdjacency borrows the completed graph until the builder closes.
func (idx *columnGraphHNSWBuilder) snapshotAdjacency() ([]vectorIndexNode, int, int) {
	return idx.nodes, idx.entry, idx.maxLevel
}

func (idx *columnGraphHNSWBuilder) close() {
	if idx == nil {
		return
	}
	idx.nodes = nil
	idx.vectorRows = nil
	idx.currentNode = nil
	idx.insertScratch = vectorIndexSearchScratch{}
	idx.searchScratch = sync.Pool{}
	idx.qualityPostfillCandidates = nil
	idx.mutationObserver = nil
}

func (idx *columnGraphHNSWBuilder) capturePartitionLiveDomainCurrentNodeV2Locked(value []byte) {
	if idx.mutationObserver != nil {
		idx.mutationObserver.capturePartitionLiveDomainCurrentNodeV2Locked(value)
	}
}

func (idx *columnGraphHNSWBuilder) capturePartitionLiveDomainNodeV2Locked(value int) {
	if idx.mutationObserver != nil {
		idx.mutationObserver.capturePartitionLiveDomainNodeV2Locked(value)
	}
}

func (idx *columnGraphHNSWBuilder) markGraphChangedLocked() {
	if idx.mutationObserver != nil {
		idx.mutationObserver.markGraphChangedLocked()
	}
}

func (idx *columnGraphHNSWBuilder) markVectorDocDirtyLocked(value []byte) {
	if idx.mutationObserver != nil {
		idx.mutationObserver.markVectorDocDirtyLocked(value)
	}
}

func (idx *columnGraphHNSWBuilder) markVectorMetaDirtyLocked() {
	if idx.mutationObserver != nil {
		idx.mutationObserver.markVectorMetaDirtyLocked()
	}
}

func (idx *columnGraphHNSWBuilder) markVectorNodeDirtyLocked(value int) {
	if idx.mutationObserver != nil {
		idx.mutationObserver.markVectorNodeDirtyLocked(value)
	}
}

func (idx *columnGraphHNSWBuilder) prepareSearchViewForMutationLocked() {
	if idx.mutationObserver != nil {
		idx.mutationObserver.prepareSearchViewForMutationLocked()
	}
}

func (idx *columnGraphHNSWBuilder) admitRedundantConstructionNeighborhoodLocked(query []float32, queryNorm float64, prepared *preparedFloat32CosineQuery, candidates []vectorIndexCandidate, sourceNodeID, oldCurrent int, scratch *vectorIndexSearchScratch, context *vectorIndexConstructionDecisionContextV1) []vectorIndexCandidate {
	idx.sortVectorIndexCandidatesByDistanceLocked(candidates)
	if len(candidates) == 0 || !idx.constructionCandidatesHaveIdenticalVectorsLocked(candidates) {
		return candidates
	}
	// Classify this bounded pool only when an admission needs to evict. Node
	// ordinals remain immutable even when their candidate slots are overwritten.
	// ponytail: initial classification is quadratic in EfConstruction; hash only if profiling warrants it.
	var classNodesStack, classCountsStack, membershipsStack [128]int
	var classNodes, classCounts, memberships []int
	classFor := func(nodeID int) int {
		free := -1
		for class, representative := range classNodes {
			if classCounts[class] == 0 {
				if free < 0 {
					free = class
				}
				continue
			}
			if idx.constructionNodesHaveIdenticalVectorsLocked(nodeID, representative) {
				return class
			}
		}
		classNodes[free] = nodeID
		return free
	}
	admit := func(nodeID int) {
		if nodeID < 0 || nodeID >= len(idx.nodes) || nodeID == sourceNodeID {
			return
		}
		for _, candidate := range candidates {
			if candidate.nodeID == nodeID {
				return
			}
		}
		replace := -1
		if len(candidates) >= idx.efConstruction {
			if memberships == nil {
				n := len(candidates)
				if n <= len(membershipsStack) {
					classNodes = classNodesStack[:n]
					classCounts = classCountsStack[:n]
					memberships = membershipsStack[:n]
				} else {
					classNodes = make([]int, n)
					classCounts = make([]int, n)
					memberships = make([]int, n)
				}
				for i, candidate := range candidates {
					class := classFor(candidate.nodeID)
					memberships[i] = class
					classCounts[class]++
				}
			}
			for i, candidate := range candidates {
				if classCounts[memberships[i]] <= 1 {
					continue
				}
				if replace < 0 {
					replace = i
				} else {
					deleted := idx.nodes[candidate.nodeID].deleted
					replacedDeleted := idx.nodes[candidates[replace].nodeID].deleted
					if (deleted && !replacedDeleted) || (deleted == replacedDeleted && vectorIndexConstructionOrdinalOffset(candidate.nodeID, sourceNodeID) > vectorIndexConstructionOrdinalOffset(candidates[replace].nodeID, sourceNodeID)) {
						replace = i
					}
				}
			}
			if replace < 0 {
				return
			}
		}
		if context != nil {
			context.recordRowFrom(sourceNodeID, nodeID, false)
		}
		distance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNorm, prepared, nodeID, scratch)
		if !ok || math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 0) {
			return
		}
		candidate := vectorIndexCandidate{nodeID: nodeID, distance: distance}
		if replace >= 0 {
			classCounts[memberships[replace]]--
			class := classFor(nodeID)
			memberships[replace] = class
			classCounts[class]++
			candidates[replace] = candidate
		} else {
			candidates = append(candidates, candidate)
		}
	}
	// Frozen-prefix planning can see only the immutable prefix, not the future
	// ordinal immediately before this plan. Use its last existing node instead.
	predecessor := minInt(sourceNodeID-1, len(idx.nodes)-1)
	for i, known := range [2]int{oldCurrent, predecessor} {
		if i == 1 && known == oldCurrent {
			continue
		}
		admit(known)
		if known >= 0 && known < len(idx.nodes) {
			for _, neighbor := range idx.layerNeighborsLocked(known, 0) {
				admit(int(neighbor.nodeID))
			}
		}
	}
	return candidates
}

func (idx *columnGraphHNSWBuilder) appendFrozenPrefixVectorRowLocked(vector []float32) []float32 {
	if idx.constructionRowsFixed {
		start := len(idx.vectorRows)
		idx.vectorRows = idx.vectorRows[:start+len(vector)]
		return idx.vectorRows[start:]
	}
	start := len(idx.vectorRows)
	rebind := len(idx.vectorRows)+len(vector) > cap(idx.vectorRows)
	idx.vectorRows = append(idx.vectorRows, vector...)
	if rebind {
		for nodeID := range idx.nodes {
			offset := nodeID * idx.dimensions
			idx.nodes[nodeID].vector = idx.vectorRows[offset : offset+idx.dimensions]
		}
	}
	return idx.vectorRows[start : start+idx.dimensions]
}

func (idx *columnGraphHNSWBuilder) applyQualityPostfillLocked(trace *vectorIndexConstructionTraceV1, degreeLimit int) error {
	if degreeLimit <= 0 {
		return nil
	}
	for from := range idx.nodes {
		if len(idx.nodes[from].neighbors) == 0 {
			continue
		}
		neighbors := idx.nodes[from].neighbors[0]
		target := minInt(degreeLimit, len(idx.nodes)-1)
		if len(neighbors) > target {
			return fmt.Errorf("collections: least-redundant separation postfill degree from=%d degree=%d limit=%d", from, len(neighbors), degreeLimit)
		}
		pool := idx.qualityPostfillCandidates[from]
		for len(neighbors) < target {
			present := make(map[int]struct{}, len(neighbors))
			for _, neighbor := range neighbors {
				present[int(neighbor.nodeID)] = struct{}{}
			}
			best, bestMargin, bestDistance := -1, float32(math.Inf(-1)), float32(math.Inf(1))
			for to := range pool {
				if to < 0 || to >= len(idx.nodes) || to == from {
					continue
				}
				if _, exists := present[to]; exists {
					continue
				}
				distance, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[from], &idx.nodes[to])
				if err != nil {
					return err
				}
				margin := float32(math.Inf(1))
				for _, neighbor := range neighbors {
					separation, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[to], &idx.nodes[int(neighbor.nodeID)])
					if err != nil {
						return err
					}
					if separation < margin {
						margin = separation
					}
				}
				if margin > bestMargin || (margin == bestMargin && (distance < bestDistance || (distance == bestDistance && to < best))) {
					best, bestMargin, bestDistance = to, margin, distance
				}
			}
			if best < 0 {
				break
			}
			neighbors = append(neighbors, vectorIndexNeighbor{nodeID: uint32(best), distance: bestDistance})
			if trace != nil {
				trace.init()
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: best, Layer: 0}
				if _, exists := trace.origins[key]; exists {
					return fmt.Errorf("collections: least-redundant separation postfill duplicate edge from=%d to=%d", from, best)
				}
				trace.origins[key] = "quality_postfill"
				trace.record(from, best, 0, "quality_postfill", "quality_postfill_add")
				if trace.postfillEdges == ^uint64(0) {
					return fmt.Errorf("collections: least-redundant separation postfill overflow")
				}
				trace.postfillEdges++
			}
		}
		idx.nodes[from].neighbors[0] = neighbors
		if len(neighbors) != target {
			return fmt.Errorf("collections: least-redundant separation postfill insufficient observed candidates from=%d degree=%d target=%d pool=%d", from, len(neighbors), target, len(pool))
		}
	}
	return nil
}

func (idx *columnGraphHNSWBuilder) applyRobustPruneRefinementLocked(trace *vectorIndexConstructionTraceV1, degreeLimit int) error {
	// For normalized vectors this builder stores cosine distance, which is
	// proportional to squared Euclidean distance. DiskANN's alpha*d(a,b)
	// threshold therefore becomes alpha^2*cosineDistance(a,b), not alpha.
	const alphaSquared = float32(1.44)
	if degreeLimit <= 0 {
		return nil
	}
	type candidate struct {
		nodeID   int
		distance float32
	}
	for from := range idx.nodes {
		if len(idx.nodes[from].neighbors) == 0 {
			continue
		}
		target := minInt(degreeLimit, len(idx.nodes)-1)
		old := append([]vectorIndexNeighbor(nil), idx.nodes[from].neighbors[0]...)
		pool := make(map[int]struct{}, len(old)+len(idx.qualityPostfillCandidates[from]))
		for _, neighbor := range old {
			pool[int(neighbor.nodeID)] = struct{}{}
		}
		for to := range idx.qualityPostfillCandidates[from] {
			pool[to] = struct{}{}
		}
		candidates := make([]candidate, 0, len(pool))
		for to := range pool {
			if to < 0 || to >= len(idx.nodes) || to == from {
				continue
			}
			distance, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[from], &idx.nodes[to])
			if err != nil {
				return err
			}
			candidates = append(candidates, candidate{nodeID: to, distance: distance})
		}
		slices.SortFunc(candidates, func(left, right candidate) int {
			if left.distance < right.distance {
				return -1
			}
			if left.distance > right.distance {
				return 1
			}
			return left.nodeID - right.nodeID
		})
		selected := make([]candidate, 0, target)
		remaining := append([]candidate(nil), candidates...)
		for len(remaining) != 0 && len(selected) < target {
			chosen := remaining[0]
			selected = append(selected, chosen)
			next := remaining[:0]
			for _, other := range remaining[1:] {
				separation, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[chosen.nodeID], &idx.nodes[other.nodeID])
				if err != nil {
					return err
				}
				if vectorIndexRobustPruneOccludesV1(alphaSquared, separation, other.distance) {
					continue
				}
				next = append(next, other)
			}
			remaining = next
		}
		// robust remains the pre-residual RobustPrune result; present grows with
		// residual fill so the provenance of those additional edges stays distinct.
		robust := make(map[int]struct{}, len(selected))
		for _, item := range selected {
			robust[item.nodeID] = struct{}{}
		}
		present := make(map[int]struct{}, len(selected))
		for _, item := range selected {
			present[item.nodeID] = struct{}{}
		}
		for _, item := range candidates {
			if len(selected) == target {
				break
			}
			if _, exists := present[item.nodeID]; exists {
				continue
			}
			selected = append(selected, item)
			present[item.nodeID] = struct{}{}
		}
		if len(selected) != target {
			return fmt.Errorf("collections: robust prune refinement insufficient candidates from=%d selected=%d target=%d", from, len(selected), target)
		}
		oldSet := make(map[int]struct{}, len(old))
		for _, neighbor := range old {
			oldSet[int(neighbor.nodeID)] = struct{}{}
		}
		if trace != nil {
			trace.init()
			for _, neighbor := range old {
				if _, keep := present[int(neighbor.nodeID)]; keep {
					continue
				}
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: int(neighbor.nodeID), Layer: 0}
				origin, ok := trace.origins[key]
				if !ok {
					return fmt.Errorf("collections: robust prune refinement missing origin from=%d to=%d", from, int(neighbor.nodeID))
				}
				delete(trace.origins, key)
				trace.record(from, int(neighbor.nodeID), 0, origin, "robust_prune_drop")
			}
			for _, item := range selected {
				if _, existed := oldSet[item.nodeID]; existed {
					continue
				}
				origin, action := "robust_prune_refinement", "robust_prune_add"
				if _, selectedByRobust := robust[item.nodeID]; !selectedByRobust {
					origin, action = "robust_prune_residual_fill", "robust_prune_residual_fill_add"
				}
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: item.nodeID, Layer: 0}
				trace.origins[key] = origin
				trace.record(from, item.nodeID, 0, origin, action)
			}
		}
		neighbors := make([]vectorIndexNeighbor, len(selected))
		for i, item := range selected {
			neighbors[i] = vectorIndexNeighbor{nodeID: uint32(item.nodeID), distance: item.distance}
		}
		idx.nodes[from].neighbors[0] = neighbors
	}
	return nil
}

func (idx *columnGraphHNSWBuilder) bindFixedConstructionRowsLocked(rows []float32, rowCount int) error {
	if idx == nil || idx.dimensions <= 0 || rowCount <= 0 || len(idx.nodes) != 0 || idx.vectorRows != nil || rowCount > int(^uint(0)>>1)/idx.dimensions || len(rows) != rowCount*idx.dimensions {
		return errors.New("collections: invalid fixed vector construction rows")
	}
	idx.vectorRows = rows[:0:len(rows)]
	idx.constructionRowsFixed = true
	return nil
}

func (idx *columnGraphHNSWBuilder) canPlanFrozenPrefixBatchLocked(documentIDs [][]byte) bool {
	if (idx.mutationObserver == nil && !idx.parallelReciprocalLinks) || idx.encoding != VectorIndexEncodingFloat32 || idx.metric != VectorMetricCosine || len(documentIDs) < 2 || idx.m < len(documentIDs) || idx.constructionTrace != nil || idx.layer0ConstructionPolicy != nil || idx.qualityPostfillCandidates != nil {
		return false
	}
	for i, documentID := range documentIDs {
		if _, exists := idx.currentNode[string(documentID)]; exists {
			return false
		}
		for j := 0; j < i; j++ {
			if bytes.Equal(documentID, documentIDs[j]) {
				return false
			}
		}
	}
	return true
}

func (idx *columnGraphHNSWBuilder) captureQualityPostfillCandidatesLocked(from int, candidates []vectorIndexCandidate) {
	if len(candidates) == 0 || from < 0 {
		return
	}
	if idx.qualityPostfillCandidates == nil {
		idx.qualityPostfillCandidates = make(map[int]map[int]struct{})
	}
	seen := idx.qualityPostfillCandidates[from]
	if seen == nil {
		seen = make(map[int]struct{}, len(candidates))
		idx.qualityPostfillCandidates[from] = seen
	}
	for _, candidate := range candidates {
		to := candidate.nodeID
		if to < 0 || to >= len(idx.nodes) || to == from {
			continue
		}
		seen[to] = struct{}{}
		// Distance is symmetric for the cosine-only offline policies.  Retain
		// the reverse endpoint too: this exact pair was scored during HNSW
		// construction, even if only `from` selected it at that time.
		reverse := idx.qualityPostfillCandidates[to]
		if reverse == nil {
			reverse = make(map[int]struct{})
			idx.qualityPostfillCandidates[to] = reverse
		}
		reverse[from] = struct{}{}
	}
}

func (idx *columnGraphHNSWBuilder) commitFrozenPrefixBatchLocked(plans []vectorIndexFrozenPrefixInsert, scratch *vectorIndexFrozenPrefixCommitScratch) {
	links := scratch.links[:0]
	for i := range plans {
		plan := plans[i]
		nodeID := len(idx.nodes)
		idx.nodes = append(idx.nodes, idx.newVectorIndexNodePrepared(plan.documentID, plan.vector, plan.level, nil, 0))
		idx.markGraphChangedLocked()
		idx.markVectorNodeDirtyLocked(nodeID)
		idx.markVectorDocDirtyLocked(plan.documentID)
		idx.currentNode[string(plan.documentID)] = nodeID
		for layer := len(plan.neighbors) - 1; layer >= 0; layer-- {
			for _, neighborID := range plan.neighbors[layer] {
				idx.linkLayerLocked(nodeID, neighborID, layer, true)
				links = append(links, vectorIndexFrozenPrefixReciprocalLink{fromNodeID: neighborID, toNodeID: nodeID, layer: layer})
			}
		}
		if plan.level > idx.maxLevel {
			idx.entry = nodeID
			idx.maxLevel = plan.level
			idx.markVectorMetaDirtyLocked()
		}
	}

	slices.SortStableFunc(links, func(left, right vectorIndexFrozenPrefixReciprocalLink) int {
		if left.fromNodeID != right.fromNodeID {
			return left.fromNodeID - right.fromNodeID
		}
		return left.layer - right.layer
	})
	groups := scratch.groups[:0]
	for start := 0; start < len(links); {
		end := start + 1
		for end < len(links) && links[end].fromNodeID == links[start].fromNodeID && links[end].layer == links[start].layer {
			end++
		}
		groups = append(groups, vectorIndexFrozenPrefixReciprocalGroup{start: start, end: end})
		start = end
	}
	if cap(scratch.pruned) < len(groups) {
		scratch.pruned = make([]bool, len(groups))
	} else {
		scratch.pruned = scratch.pruned[:len(groups)]
		clear(scratch.pruned)
	}
	pruned := scratch.pruned
	workers := idx.constructionWorkerCount(vectorIndexReciprocalLinkWorkerCount(len(groups)))
	var decisionObservers []vectorIndexConstructionDecisionObserverV1
	if idx.decisionObserver != nil {
		decisionObservers = scratch.decisionObserverWorkers(workers)
	}
	if cap(scratch.dots) < workers {
		scratch.dots = make([]vectorIndexFrozenPrefixDiversityScratch, workers)
	} else {
		scratch.dots = scratch.dots[:workers]
	}
	for worker := range scratch.dots {
		scratch.dots[worker].indexedBatches = 0
	}
	var nextGroup atomic.Int64
	runGroup := func(worker int) {
		observer := (*vectorIndexConstructionDecisionObserverV1)(nil)
		if decisionObservers != nil {
			observer = &decisionObservers[worker]
		}
		for {
			group := int(nextGroup.Add(1) - 1)
			if group >= len(groups) {
				return
			}
			pruned[group] = idx.linkFrozenPrefixReciprocalGroupObservedLocked(links[groups[group].start:groups[group].end], &scratch.dots[worker], observer)
		}
	}
	var wg sync.WaitGroup
	for worker := 1; worker < workers; worker++ {
		worker := worker
		wg.Go(func() { runGroup(worker) })
	}
	runGroup(0)
	wg.Wait()
	for worker := range scratch.dots {
		idx.frozenPrefixIndexedDotBatches += scratch.dots[worker].indexedBatches
	}
	for group := range groups {
		idx.markVectorNodeDirtyLocked(links[groups[group].start].fromNodeID)
		if pruned[group] {
			idx.frozenPrefixReciprocalPrunes++
			idx.frozenPrefixReciprocalPruneEdges += uint64(groups[group].end - groups[group].start)
		}
	}
	scratch.links = links
	scratch.groups = groups
}

func (idx *columnGraphHNSWBuilder) compareConstructionCandidatesLocked(left, right vectorIndexCandidate, sourceNodeID int) int {
	if left.distance == right.distance {
		if left.nodeID >= 0 && right.nodeID >= 0 && left.nodeID < len(idx.nodes) && right.nodeID < len(idx.nodes) {
			if idx.nodes[left.nodeID].deleted != idx.nodes[right.nodeID].deleted {
				if idx.nodes[left.nodeID].deleted {
					return 1
				}
				return -1
			}
		}
		leftOffset := vectorIndexConstructionOrdinalOffset(left.nodeID, sourceNodeID)
		rightOffset := vectorIndexConstructionOrdinalOffset(right.nodeID, sourceNodeID)
		if leftOffset < rightOffset {
			return -1
		}
		if leftOffset > rightOffset {
			return 1
		}
	}
	return idx.compareVectorIndexCandidatesByDistanceLocked(left, right)
}

func (idx *columnGraphHNSWBuilder) compareVectorIndexCandidatesByDistanceLocked(left, right vectorIndexCandidate) int {
	if left.distance < right.distance {
		return -1
	}
	if left.distance > right.distance {
		return 1
	}
	if left.nodeID >= 0 && left.nodeID < len(idx.nodes) && right.nodeID >= 0 && right.nodeID < len(idx.nodes) {
		if cmp := bytes.Compare(idx.nodes[left.nodeID].documentID, idx.nodes[right.nodeID].documentID); cmp != 0 {
			return cmp
		}
	}
	if left.nodeID < right.nodeID {
		return -1
	}
	if left.nodeID > right.nodeID {
		return 1
	}
	return 0
}

func (idx *columnGraphHNSWBuilder) constructionCandidatesHaveIdenticalVectorsLocked(candidates []vectorIndexCandidate) bool {
	for start := 0; start < len(candidates); {
		end := start + 1
		for end < len(candidates) && candidates[end].distance == candidates[start].distance {
			end++
		}
		for i := start + 1; i < end; i++ {
			for j := start; j < i; j++ {
				if idx.constructionNodesHaveIdenticalVectorsLocked(candidates[i].nodeID, candidates[j].nodeID) {
					return true
				}
			}
		}
		start = end
	}
	return false
}

func (idx *columnGraphHNSWBuilder) constructionNodesHaveIdenticalVectorsLocked(left, right int) bool {
	if left < 0 || right < 0 || left >= len(idx.nodes) || right >= len(idx.nodes) || idx.dimensions <= 0 {
		return false
	}
	switch idx.metric {
	case VectorMetricCosine, VectorMetricL2, VectorMetricInnerProduct:
	default:
		return false
	}
	a, b := &idx.nodes[left], &idx.nodes[right]
	// A differing coordinate rejects identity cheaply; matching probes still
	// require full equality and validation. Both nodes use the same coordinates.
	first, second := left%idx.dimensions, right%idx.dimensions
	switch idx.encoding {
	case VectorIndexEncodingFloat32:
		if len(a.vector) != idx.dimensions || len(b.vector) != idx.dimensions || a.vector[first] != b.vector[first] || (second != first && a.vector[second] != b.vector[second]) || !slices.Equal(a.vector, b.vector) {
			return false
		}
		if validateFloat32Vector(a.vector) != nil {
			return false
		}
		return idx.metric != VectorMetricCosine || vectorNormSquared(a.vector) > 0
	case VectorIndexEncodingInt8:
		if len(a.quantized) != idx.dimensions || len(b.quantized) != idx.dimensions || a.quantScale <= 0 || a.quantScale != b.quantScale || math.IsInf(float64(a.quantScale), 0) || math.IsNaN(float64(a.quantScale)) || a.quantized[first] != b.quantized[first] || (second != first && a.quantized[second] != b.quantized[second]) || !slices.Equal(a.quantized, b.quantized) {
			return false
		}
		if idx.metric == VectorMetricCosine {
			for _, value := range a.quantized {
				if value != 0 {
					return true
				}
			}
			return false
		}
		return true
	default:
		return false
	}
}

func (idx *columnGraphHNSWBuilder) constructionWorkerCount(work int) int {
	if idx.constructionWorkers > 0 {
		return minInt(idx.constructionWorkers, work)
	}
	return work
}

func (idx *columnGraphHNSWBuilder) currentVectorMatchesLocked(documentID []byte, vector []float32) bool {
	nodeID, ok := idx.currentNode[string(documentID)]
	if !ok || nodeID < 0 || nodeID >= len(idx.nodes) {
		return false
	}
	node := &idx.nodes[nodeID]
	return !node.deleted && idx.vectorIndexNodeMatchesSourceVectorLocked(node, vector)
}

func (idx *columnGraphHNSWBuilder) distanceBetweenFloat32CosineCandidateAndNodeLocked(candidate *vectorIndexNode, existingNodeID int) float32 {
	if existingNodeID < 0 || existingNodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	existing := &idx.nodes[existingNodeID]
	if len(existing.vector) == len(candidate.vector) && existing.cachedInvNorm != 0 {
		return vectorDistanceBetweenFloat32NodesCosineUnchecked(candidate, existing)
	}
	distance, err := vectorDistanceBetweenStoredNodes(candidate, existing, idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func (idx *columnGraphHNSWBuilder) distanceBetweenNodesFastLocked(leftNodeID, rightNodeID int) (float32, bool) {
	if leftNodeID < 0 || leftNodeID >= len(idx.nodes) || rightNodeID < 0 || rightNodeID >= len(idx.nodes) {
		return 0, false
	}
	left := &idx.nodes[leftNodeID]
	right := &idx.nodes[rightNodeID]
	if idx.metric == VectorMetricCosine && canUseUncheckedFloat32NodeCosine(left, right, idx.dimensions) {
		return vectorDistanceBetweenFloat32NodesCosineUnchecked(left, right), true
	}
	distance, err := vectorDistanceBetweenStoredNodes(left, right, idx.metric)
	if err != nil {
		return 0, false
	}
	return distance, true
}

func (idx *columnGraphHNSWBuilder) distanceBetweenNodesLocked(leftNodeID, rightNodeID int) float32 {
	if leftNodeID < 0 || leftNodeID >= len(idx.nodes) || rightNodeID < 0 || rightNodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	distance, err := vectorDistanceBetweenStoredNodes(&idx.nodes[leftNodeID], &idx.nodes[rightNodeID], idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func (idx *columnGraphHNSWBuilder) distanceToNodeWithPreparedQueryLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, nodeID int) float32 {
	if nodeID < 0 || nodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	node := &idx.nodes[nodeID]
	if prepared != nil && idx.metric == VectorMetricCosine && len(node.vector) > 0 {
		return vectorDistanceToFloat32NodeCosineUnchecked(*prepared, node)
	}
	distance, err := vectorDistanceToStoredNodeWithQueryNorm(query, queryNormSquared, node, idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func (idx *columnGraphHNSWBuilder) firstLiveNodeAtLevelLocked(level int) int {
	for i := range idx.nodes {
		if !idx.nodes[i].deleted && idx.nodes[i].level >= level {
			return i
		}
	}
	return -1
}

func (idx *columnGraphHNSWBuilder) firstLiveNodeLocked() int {
	for i := range idx.nodes {
		if !idx.nodes[i].deleted {
			return i
		}
	}
	return -1
}

func (idx *columnGraphHNSWBuilder) firstVectorIndexCandidateInversionLocked(candidates []vectorIndexCandidate) int {
	for i := 1; i < len(candidates); i++ {
		if idx.compareVectorIndexCandidatesByDistanceLocked(candidates[i-1], candidates[i]) > 0 {
			return i
		}
	}
	return -1
}

func (idx *columnGraphHNSWBuilder) frozenPrefixIndexedDotRowsReadyLocked(candidateNodeID int, candidate *vectorIndexNode, selected []vectorIndexCandidate, scratch *vectorIndexFrozenPrefixDiversityScratch, context *vectorIndexConstructionDecisionContextV1) bool {
	if candidateNodeID < 0 || candidateNodeID >= len(idx.nodes) || scratch == nil || !vectorops.DotFloat32IndexedOptimizedEligible(len(selected), idx.dimensions) || len(candidate.vector) != idx.dimensions || len(idx.vectorRows) != len(idx.nodes)*idx.dimensions {
		return false
	}
	// The comparison margin below bounds FP32 product/accumulation error by
	// 8*(dims+2)*2^-24. Decline shapes where that bound or gradual-underflow
	// allowance is unavailable; scalar normalized differences remain valid.
	if idx.dimensions >= 1<<21 {
		return false
	}
	minNormProduct := float64(idx.dimensions) * 0x1p-126 / 0x1p-24
	if cap(scratch.rowIDs) < len(selected) {
		scratch.rowIDs = make([]uint32, len(selected))
	} else {
		scratch.rowIDs = scratch.rowIDs[:len(selected)]
	}
	if cap(scratch.dots) < len(selected) {
		scratch.dots = make([]float32, len(selected))
	} else {
		scratch.dots = scratch.dots[:len(selected)]
	}
	for i, existing := range selected {
		if existing.nodeID < 0 || uint64(existing.nodeID) > uint64(^uint32(0)) || existing.nodeID >= len(idx.nodes) || idx.nodes[existing.nodeID].cachedInvNorm == 0 || !safeFloat32DotProductForCosine(candidate.normSquared, idx.nodes[existing.nodeID].normSquared) {
			return false
		}
		if candidate.normSquared*idx.nodes[existing.nodeID].normSquared < minNormProduct*minNormProduct {
			return false
		}
		scratch.rowIDs[i] = uint32(existing.nodeID)
	}
	status := vectorops.DotFloat32IndexedPrevalidated(scratch.dots, idx.vectorRows, candidate.vector, scratch.rowIDs, idx.dimensions)
	if context != nil && status.Rows > 0 {
		context.recordIndexedExactFP32Call()
		for i := 0; i < status.Rows && i < len(selected); i++ {
			context.recordRowFrom(candidateNodeID, selected[i].nodeID, true)
		}
	}
	return !status.Invalid && status.Optimized && status.Rows == len(selected)
}

func (idx *columnGraphHNSWBuilder) getSearchScratch() *vectorIndexSearchScratch {
	if scratch, ok := idx.searchScratch.Get().(*vectorIndexSearchScratch); ok {
		return scratch
	}
	return &vectorIndexSearchScratch{}
}

func (idx *columnGraphHNSWBuilder) greedyNearestAtLayerCanonicalConstructionLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, layer int) int {
	if entryPoint < 0 {
		return entryPoint
	}
	best := vectorIndexCandidate{nodeID: entryPoint, distance: idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, entryPoint)}
	for changed := true; changed; {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best.nodeID, layer) {
			candidate := vectorIndexCandidate{nodeID: int(neighbor.nodeID), distance: idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, int(neighbor.nodeID))}
			if idx.compareVectorIndexCandidatesByDistanceLocked(candidate, best) < 0 {
				best = candidate
				changed = true
			}
		}
	}
	return best.nodeID
}

func (idx *columnGraphHNSWBuilder) greedyNearestAtLayerLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, layer int) int {
	if entryPoint < 0 {
		return entryPoint
	}
	best := entryPoint
	bestDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, best)
	changed := true
	for changed {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best, layer) {
			neighborID := int(neighbor.nodeID)
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID)
			if distance < bestDistance {
				best = neighborID
				bestDistance = distance
				changed = true
			}
		}
	}
	return best
}

func (idx *columnGraphHNSWBuilder) greedyNearestAtLayerObservedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, layer int, context *vectorIndexConstructionDecisionContextV1) int {
	if entryPoint < 0 {
		return entryPoint
	}
	best := entryPoint
	context.recordRow(best, false)
	bestDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, best)
	changed := true
	for changed {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best, layer) {
			neighborID := int(neighbor.nodeID)
			context.recordRow(neighborID, false)
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID)
			if distance < bestDistance {
				best = neighborID
				bestDistance = distance
				changed = true
			}
		}
	}
	return best
}

func (idx *columnGraphHNSWBuilder) initialNeighborCapacityForLayer(layer int) int {
	return minInt(idx.maxNeighborsForLayer(layer), maxVectorIndexEagerNeighborCap)
}

func (idx *columnGraphHNSWBuilder) insertTailVectorIndexCandidateByDistanceLocked(candidates []vectorIndexCandidate) {
	tail := len(candidates) - 1
	if tail <= 0 {
		return
	}
	candidate := candidates[tail]
	insert := tail - 1
	for insert >= 0 && idx.compareVectorIndexCandidatesByDistanceLocked(candidates[insert], candidate) > 0 {
		candidates[insert+1] = candidates[insert]
		insert--
	}
	candidates[insert+1] = candidate
}

func (idx *columnGraphHNSWBuilder) insertVectorBatchLocked(documentIDs [][]byte, vectors [][]float32) error {
	return idx.insertVectorBatchWithContextLocked(context.Background(), documentIDs, vectors)
}

func (idx *columnGraphHNSWBuilder) insertVectorBatchWithContextLocked(ctx context.Context, documentIDs [][]byte, vectors [][]float32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := idx.validateVectorBatchWithContext(ctx, documentIDs, vectors); err != nil {
		return err
	}
	if len(documentIDs) == 0 {
		return nil
	}
	idx.prepareSearchViewForMutationLocked()
	if len(documentIDs) < nativeVectorFrozenPrefixBatchMinimum {
		for row := range documentIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := idx.insertVectorLocked(documentIDs[row], vectors[row]); err != nil {
				return err
			}
		}
		return nil
	}
	// Own one search workspace per planning worker for this entire insertion
	// operation. A sync.Pool can retain more workspaces than the concurrent
	// worker count across successive batches, so it is not this cohort's owner.
	var planningScratch *[nativeVectorFrozenPrefixBatchWidth]vectorIndexSearchScratch
	if idx.parallelReciprocalLinks {
		planningScratch = new([nativeVectorFrozenPrefixBatchWidth]vectorIndexSearchScratch)
	}
	return idx.insertVectorFrozenPrefixBatchesWithPlanningScratchLocked(ctx, documentIDs, vectors, planningScratch)
}

func (idx *columnGraphHNSWBuilder) insertVectorFrozenPrefixBatchesWithPlanningScratchLocked(ctx context.Context, documentIDs [][]byte, vectors [][]float32, planningScratch *[nativeVectorFrozenPrefixBatchWidth]vectorIndexSearchScratch) error {
	var commitScratch vectorIndexFrozenPrefixCommitScratch
	if idx.decisionObserver != nil {
		defer func() {
			for worker := range commitScratch.decisionObservers {
				idx.decisionObserver.merge(&commitScratch.decisionObservers[worker])
			}
		}()
	}
	for start := 0; start < len(documentIDs); {
		if err := ctx.Err(); err != nil {
			return err
		}
		if idx.entry < 0 {
			if err := idx.insertVectorLocked(documentIDs[start], vectors[start]); err != nil {
				return err
			}
			start++
			continue
		}
		batchWidth := nativeVectorFrozenPrefixBatchWidth
		if idx.m == 8 {
			batchWidth = idx.m
		}
		end := minInt(start+batchWidth, len(documentIDs))
		if !idx.canPlanFrozenPrefixBatchLocked(documentIDs[start:end]) {
			for row := start; row < end; row++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := idx.insertVectorLocked(documentIDs[row], vectors[row]); err != nil {
					return err
				}
			}
			start = end
			continue
		}
		entry, maxLevel := idx.entry, idx.maxLevel
		plans := make([]vectorIndexFrozenPrefixInsert, end-start)
		errs := make([]error, len(plans))
		workers := idx.constructionWorkerCount(len(plans))
		var decisionObservers []vectorIndexConstructionDecisionObserverV1
		if idx.decisionObserver != nil {
			decisionObservers = commitScratch.decisionObserverWorkers(workers)
		}
		planRow := func(plan, worker int, observer *vectorIndexConstructionDecisionObserverV1) {
			var scratch *vectorIndexSearchScratch
			if planningScratch != nil {
				scratch = &planningScratch[worker]
			}
			plans[plan], errs[plan] = idx.planFrozenPrefixInsertWithScratchLocked(ctx, documentIDs[start+plan], vectors[start+plan], entry, maxLevel, len(idx.nodes)+plan, observer, scratch)
		}
		var wg sync.WaitGroup
		for worker := 1; worker < workers; worker++ {
			observer := (*vectorIndexConstructionDecisionObserverV1)(nil)
			if decisionObservers != nil {
				observer = &decisionObservers[worker]
			}
			wg.Go(func() {
				for plan := worker; plan < len(plans); plan += workers {
					planRow(plan, worker, observer)
				}
			})
		}
		observer := (*vectorIndexConstructionDecisionObserverV1)(nil)
		if decisionObservers != nil {
			observer = &decisionObservers[0]
		}
		for plan := 0; plan < len(plans); plan += workers {
			planRow(plan, 0, observer)
		}
		wg.Wait()
		for row, err := range errs {
			if err != nil {
				return fmt.Errorf("collections: vector batch row %d: %w", start+row, err)
			}
		}
		idx.prepareFrozenPrefixVectorRowsLocked(len(documentIDs) - start)
		idx.commitFrozenPrefixBatchLocked(plans, &commitScratch)
		idx.frozenPrefixBatches++
		start = end
	}
	return nil
}

func (idx *columnGraphHNSWBuilder) insertVectorLocked(documentID []byte, vector []float32) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	if len(vector) == 0 {
		return errors.New("collections: vector cannot be empty")
	}
	if err := validateFloat32Vector(vector); err != nil {
		return err
	}
	vectorNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		vectorNorm = vectorNormSquared(vector)
		if vectorNorm == 0 {
			return errors.New("collections: cosine vector cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(vector, vectorNorm)
		if err != nil {
			return err
		}
		prepared = &preparedQuery
	}
	if idx.dimensions == 0 {
		idx.dimensions = len(vector)
		idx.markVectorMetaDirtyLocked()
	} else if len(vector) != idx.dimensions {
		return fmt.Errorf("collections: vector field %q in document %q has dimension %d, want %d", idx.field, documentID, len(vector), idx.dimensions)
	}
	if idx.currentVectorMatchesLocked(documentID, vector) {
		return nil
	}
	var quantized []int8
	var quantScale float32
	if idx.encoding == VectorIndexEncodingInt8 {
		idx.insertQuantScratch, quantScale = quantizeVectorIndexInt8Into(idx.insertQuantScratch[:0], vector)
		quantized = idx.insertQuantScratch
	}
	if !vectorIndexNodeOrdinalsFitUint32(uint64(len(idx.nodes)), 1) {
		return errors.New("collections: vector index node ordinal exceeds uint32")
	}
	oldCurrent := -1
	if current, ok := idx.currentNode[string(documentID)]; ok {
		oldCurrent = current
	}
	idx.prepareSearchViewForMutationLocked()
	idx.tombstoneDocumentIDLocked(documentID)

	nodeID := len(idx.nodes)
	level := idx.levelForDocumentID(documentID)
	if idx.encoding == VectorIndexEncodingInt8 {
		quantized = append([]int8(nil), quantized...)
	}
	node := idx.newVectorIndexNodePrepared(documentID, vector, level, quantized, quantScale)
	idx.nodes = append(idx.nodes, node)
	idx.markGraphChangedLocked()
	idx.markVectorNodeDirtyLocked(nodeID)
	idx.markVectorDocDirtyLocked(documentID)
	idx.currentNode[string(documentID)] = nodeID
	if idx.entry < 0 {
		idx.entry = nodeID
		idx.maxLevel = level
		idx.markVectorMetaDirtyLocked()
		return nil
	}
	entryPoint := idx.entry
	canonicalConstruction := idx.layer0ConstructionPolicy != nil && idx.layer0ConstructionPolicy.preserveSearchSet
	for layer := idx.maxLevel; layer > level; layer-- {
		if canonicalConstruction {
			entryPoint = idx.greedyNearestAtLayerCanonicalConstructionLocked(vector, vectorNorm, prepared, entryPoint, layer)
		} else {
			entryPoint = idx.greedyNearestAtLayerLocked(vector, vectorNorm, prepared, entryPoint, layer)
		}
	}
	var descentCandidates []vectorIndexCandidate
	for layer := minInt(level, idx.maxLevel); layer >= 0; layer-- {
		var candidates []vectorIndexCandidate
		if canonicalConstruction {
			candidates = idx.searchLayerWithCanonicalCandidateSeedsScratchLocked(vector, vectorNorm, prepared, entryPoint, descentCandidates, idx.efConstruction, layer, &idx.insertScratch)
		} else {
			candidates = idx.searchLayerWithScratchLocked(vector, vectorNorm, prepared, entryPoint, idx.efConstruction, layer, &idx.insertScratch)
		}
		if layer == 0 {
			candidates = idx.admitRedundantConstructionNeighborhoodLocked(vector, vectorNorm, prepared, candidates, nodeID, oldCurrent, &idx.insertScratch, nil)
		}
		selectionLimit := idx.maxNeighborsForLayer(layer)
		if layer == 0 && idx.layer0ConstructionPolicy != nil {
			selectionLimit = idx.m * idx.layer0ConstructionPolicy.initialSelectionFactor
		}
		if idx.layer0ConstructionPolicy != nil && idx.layer0ConstructionPolicy.preserveSearchSet {
			// Neighbor selection rewrites its candidate slice in place. Preserve
			// the complete bounded SEARCH-LAYER result for the next layer first.
			descentCandidates = append(descentCandidates[:0], candidates...)
		}
		neighbors := idx.selectLayerNeighborsLocked(vector, vectorNorm, prepared, candidates, layer, selectionLimit, nodeID)
		idx.linkSelectedNeighborsLocked(nodeID, neighbors, layer)
		if (idx.layer0ConstructionPolicy == nil || !idx.layer0ConstructionPolicy.preserveSearchSet) && len(neighbors) > 0 {
			entryPoint = neighbors[0]
		}
	}
	if level > idx.maxLevel {
		idx.entry = nodeID
		idx.maxLevel = level
		idx.markVectorMetaDirtyLocked()
	}
	return nil
}

func (idx *columnGraphHNSWBuilder) layerNeighborsLocked(nodeID int, layer int) []vectorIndexNeighbor {
	if nodeID < 0 || nodeID >= len(idx.nodes) {
		return nil
	}
	node := idx.nodes[nodeID]
	if layer < 0 || layer >= len(node.neighbors) {
		return nil
	}
	return node.neighbors[layer]
}

func (idx *columnGraphHNSWBuilder) levelForDocumentID(documentID []byte) int {
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], xxhash.Sum64(documentID)^xxhash.Sum64String(idx.name))
	hash := xxhash.Sum64(seed[:])
	promotionBase := idx.m
	if promotionBase < 2 {
		promotionBase = 2
	}
	if hash == 0 {
		return 32
	}
	u := (float64(hash>>11) + 1) / (float64(uint64(1)<<53) + 1)
	level := int(-math.Log(u) / math.Log(float64(promotionBase)))
	if level > 32 {
		return 32
	}
	return level
}

func (idx *columnGraphHNSWBuilder) linkFrozenPrefixReciprocalGroupObservedLocked(links []vectorIndexFrozenPrefixReciprocalLink, dotScratch *vectorIndexFrozenPrefixDiversityScratch, observer *vectorIndexConstructionDecisionObserverV1) bool {
	if len(links) == 0 {
		return false
	}
	fromNodeID, layer := links[0].fromNodeID, links[0].layer
	var context *vectorIndexConstructionDecisionContextV1
	if observer != nil {
		context = &vectorIndexConstructionDecisionContextV1{observer: observer, phase: vectorIndexConstructionDecisionReciprocal, source: fromNodeID, layer: layer, dimensions: idx.dimensions}
		started := time.Now()
		defer func() { context.recordWall(time.Since(started)) }()
	}
	if fromNodeID < 0 || fromNodeID >= len(idx.nodes) || layer < 0 || layer > idx.nodes[fromNodeID].level {
		return false
	}
	neighbors := idx.nodes[fromNodeID].neighbors[layer]
	candidates := neighbors
	if dotScratch != nil {
		candidates = append(dotScratch.neighbors[:0], neighbors...)
	}
	for _, link := range links {
		duplicate := false
		for _, existing := range candidates {
			if existing.nodeID == uint32(link.toNodeID) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		if context != nil {
			context.recordRowFrom(fromNodeID, link.toNodeID, false)
		}
		distance, ok := normalizeVectorIndexEdgeDistance(idx.distanceBetweenNodesLocked(fromNodeID, link.toNodeID))
		if ok {
			candidates = append(candidates, vectorIndexNeighbor{nodeID: uint32(link.toNodeID), distance: distance})
		}
	}
	limit := idx.maxNeighborsForLayer(layer)
	candidateCount := len(candidates)
	pruned := candidateCount > limit
	if pruned {
		if context == nil {
			candidates = idx.pruneLayerNeighborsWithFrozenPrefixScratchLocked(fromNodeID, candidates, limit, dotScratch)
		} else {
			candidates = idx.pruneLayerNeighborsWithFrozenPrefixScratchObservedLocked(fromNodeID, candidates, limit, dotScratch, context)
		}
	}
	if dotScratch != nil {
		dotScratch.neighbors = candidates
		neighbors = append(neighbors[:0], candidates...)
	} else {
		neighbors = candidates
	}
	if context != nil {
		context.recordGroup(len(links), candidateCount, len(neighbors))
	}
	idx.nodes[fromNodeID].neighbors[layer] = neighbors
	return pruned
}

func (idx *columnGraphHNSWBuilder) linkLayerLocked(fromNodeID, toNodeID, layer int, markDirty bool) {
	if fromNodeID < 0 || fromNodeID >= len(idx.nodes) {
		return
	}
	if layer < 0 || layer > idx.nodes[fromNodeID].level {
		return
	}
	neighbors := idx.nodes[fromNodeID].neighbors[layer]
	// Preserve the pre-link slice for trace-only reciprocal-prune accounting.
	// append may reuse the node slice's backing array, so reading the node's
	// old slice after append can incorrectly treat the newly linked edge as an
	// existing edge and produce a contradictory lifecycle.
	var preexisting []vectorIndexNeighbor
	if idx.constructionTrace != nil {
		preexisting = append(preexisting, neighbors...)
	}
	for _, existing := range neighbors {
		if existing.nodeID == uint32(toNodeID) {
			return
		}
	}
	distance := idx.distanceBetweenNodesLocked(fromNodeID, toNodeID)
	var ok bool
	distance, ok = normalizeVectorIndexEdgeDistance(distance)
	if !ok {
		return
	}
	idx.capturePartitionLiveDomainNodeV2Locked(fromNodeID)
	neighbors = append(neighbors, vectorIndexNeighbor{nodeID: uint32(toNodeID), distance: distance})
	trace := idx.constructionTrace
	var origin string
	if trace != nil {
		trace.init()
		key := vectorIndexConstructionEdgeKeyV1{From: fromNodeID, To: toNodeID, Layer: layer}
		origin = trace.pending[key]
		if origin != "" {
			delete(trace.pending, key)
			trace.record(fromNodeID, toNodeID, layer, origin, "initial_add")
		} else {
			origin = "reciprocal_add"
			trace.record(fromNodeID, toNodeID, layer, origin, "reciprocal_add")
		}
		trace.origins[key] = origin
	}
	limit := idx.maxNeighborsForLayer(layer)
	if len(neighbors) > limit {
		neighbors = idx.pruneLayerNeighborsLocked(fromNodeID, neighbors, limit)
		if trace != nil {
			kept := make(map[int]struct{}, len(neighbors))
			for _, neighbor := range neighbors {
				kept[int(neighbor.nodeID)] = struct{}{}
			}
			// The just-pruned list is the authoritative reciprocal maintenance
			// outcome for every edge that was present before the prune.
			for _, neighbor := range preexisting {
				key := vectorIndexConstructionEdgeKeyV1{From: fromNodeID, To: int(neighbor.nodeID), Layer: layer}
				edgeOrigin := trace.origins[key]
				if _, ok := kept[int(neighbor.nodeID)]; ok {
					trace.record(fromNodeID, int(neighbor.nodeID), layer, edgeOrigin, "reciprocal_prune_keep")
					continue
				}
				trace.record(fromNodeID, int(neighbor.nodeID), layer, edgeOrigin, "reciprocal_prune_drop")
				delete(trace.origins, key)
			}
			// The newly added edge is not yet in the node slice above.
			key := vectorIndexConstructionEdgeKeyV1{From: fromNodeID, To: toNodeID, Layer: layer}
			if _, ok := kept[toNodeID]; ok {
				trace.record(fromNodeID, toNodeID, layer, trace.origins[key], "reciprocal_prune_keep")
			} else {
				trace.record(fromNodeID, toNodeID, layer, trace.origins[key], "reciprocal_prune_drop")
				delete(trace.origins, key)
			}
		}
	}
	idx.nodes[fromNodeID].neighbors[layer] = neighbors
	if markDirty {
		idx.markVectorNodeDirtyLocked(fromNodeID)
	}
}

func (idx *columnGraphHNSWBuilder) linkSelectedNeighborsLocked(nodeID int, neighbors []int, layer int) {
	workers := 1
	if idx.parallelReciprocalLinks &&
		idx.constructionTrace == nil &&
		idx.layer0ConstructionPolicy == nil &&
		idx.qualityPostfillCandidates == nil &&
		vectorIndexNeighborIDsDistinct(neighbors) {
		workers = idx.constructionWorkerCount(vectorIndexReciprocalLinkWorkerCount(len(neighbors)))
	}
	if workers == 1 {
		for _, neighborID := range neighbors {
			// selectLayerNeighborsLocked records the exact selection origin before
			// this directed initial link is made.
			idx.linkLayerLocked(nodeID, neighborID, layer, true)
			idx.linkLayerLocked(neighborID, nodeID, layer, true)
		}
		return
	}

	// Keep the new node's ordered outgoing edges serial. Each worker below owns
	// a distinct old node's adjacency slice and only reads immutable node data.
	for _, neighborID := range neighbors {
		idx.linkLayerLocked(nodeID, neighborID, layer, true)
	}
	var wg sync.WaitGroup
	for worker := 1; worker < workers; worker++ {
		wg.Go(func() {
			for neighbor := worker; neighbor < len(neighbors); neighbor += workers {
				idx.linkLayerLocked(neighbors[neighbor], nodeID, layer, false)
			}
		})
	}
	for neighbor := 0; neighbor < len(neighbors); neighbor += workers {
		idx.linkLayerLocked(neighbors[neighbor], nodeID, layer, false)
	}
	wg.Wait()
	for _, neighborID := range neighbors {
		idx.markVectorNodeDirtyLocked(neighborID)
	}
}

func (idx *columnGraphHNSWBuilder) maxLiveLevelLocked() int {
	maxLevel := -1
	for i := range idx.nodes {
		if !idx.nodes[i].deleted && idx.nodes[i].level > maxLevel {
			maxLevel = idx.nodes[i].level
		}
	}
	return maxLevel
}

func (idx *columnGraphHNSWBuilder) maxNeighborsForLayer(layer int) int {
	if layer == 0 {
		return maxInt(idx.m*2, idx.m)
	}
	return idx.m
}

func (idx *columnGraphHNSWBuilder) newVectorIndexNodePrepared(documentID []byte, vector []float32, level int, quantized []int8, quantScale float32) vectorIndexNode {
	node := vectorIndexNode{
		documentID: bytes.Clone(documentID),
		level:      level,
		neighbors:  make([][]vectorIndexNeighbor, level+1),
	}
	for layer := range node.neighbors {
		capacity := idx.initialNeighborCapacityForLayer(layer)
		node.neighbors[layer] = make([]vectorIndexNeighbor, 0, capacity)
	}
	switch idx.encoding {
	case VectorIndexEncodingInt8:
		node.quantized = quantized
		node.quantScale = quantScale
	default:
		if idx.vectorRows != nil && len(vector) == idx.dimensions {
			node.vector = idx.appendFrozenPrefixVectorRowLocked(vector)
		} else {
			node.vector = append([]float32(nil), vector...)
		}
	}
	node.cacheVectorNorms()
	return node
}

func (idx *columnGraphHNSWBuilder) planFrozenPrefixInsertWithScratchLocked(ctx context.Context, documentID []byte, vector []float32, entry, maxLevel, sourceNodeID int, observer *vectorIndexConstructionDecisionObserverV1, scratch *vectorIndexSearchScratch) (vectorIndexFrozenPrefixInsert, error) {
	started := time.Time{}
	if observer != nil {
		started = time.Now()
		defer func() {
			(&vectorIndexConstructionDecisionContextV1{observer: observer, phase: vectorIndexConstructionDecisionPlanning}).recordWall(time.Since(started))
		}()
	}
	plan := vectorIndexFrozenPrefixInsert{documentID: documentID, vector: vector, level: idx.levelForDocumentID(documentID)}
	norm := vectorNormSquared(vector)
	prepared, err := prepareFloat32CosineQuery(vector, norm)
	if err != nil {
		return plan, err
	}
	for layer := maxLevel; layer > plan.level; layer-- {
		if err := ctx.Err(); err != nil {
			return plan, err
		}
		if observer == nil {
			entry = idx.greedyNearestAtLayerLocked(vector, norm, &prepared, entry, layer)
		} else {
			context := &vectorIndexConstructionDecisionContextV1{observer: observer, phase: vectorIndexConstructionDecisionPlanning, source: sourceNodeID, layer: layer, dimensions: idx.dimensions}
			entry = idx.greedyNearestAtLayerObservedLocked(vector, norm, &prepared, entry, layer, context)
		}
	}
	if scratch == nil {
		scratch = idx.getSearchScratch()
		defer idx.putSearchScratch(scratch)
	}
	scratch.setContext(ctx)
	defer scratch.clearContext()
	plan.neighbors = make([][]int, minInt(plan.level, maxLevel)+1)
	for layer := len(plan.neighbors) - 1; layer >= 0; layer-- {
		if observer == nil {
			candidates := idx.searchLayerWithScratchLocked(vector, norm, &prepared, entry, idx.efConstruction, layer, scratch)
			if err := scratch.finalContextErr(); err != nil {
				return plan, err
			}
			if layer == 0 {
				candidates = idx.admitRedundantConstructionNeighborhoodLocked(vector, norm, &prepared, candidates, sourceNodeID, -1, scratch, nil)
				if err := scratch.finalContextErr(); err != nil {
					return plan, err
				}
			}
			plan.neighbors[layer] = idx.selectLayerNeighborsLocked(vector, norm, &prepared, candidates, layer, idx.maxNeighborsForLayer(layer), sourceNodeID)
		} else {
			context := &vectorIndexConstructionDecisionContextV1{observer: observer, phase: vectorIndexConstructionDecisionPlanning, source: sourceNodeID, layer: layer, dimensions: idx.dimensions}
			candidates := idx.searchLayerWithScratchObservedLocked(vector, norm, &prepared, entry, idx.efConstruction, layer, scratch, context)
			if err := scratch.finalContextErr(); err != nil {
				return plan, err
			}
			if layer == 0 {
				candidates = idx.admitRedundantConstructionNeighborhoodLocked(vector, norm, &prepared, candidates, sourceNodeID, -1, scratch, context)
				if err := scratch.finalContextErr(); err != nil {
					return plan, err
				}
			}
			plan.neighbors[layer] = idx.selectLayerNeighborsObservedLocked(vector, norm, &prepared, candidates, layer, idx.maxNeighborsForLayer(layer), sourceNodeID, context)
		}
		if len(plan.neighbors[layer]) > 0 {
			entry = plan.neighbors[layer][0]
		}
	}
	return plan, nil
}

func (idx *columnGraphHNSWBuilder) prepareFrozenPrefixVectorRowsLocked(additional int) {
	if idx.vectorRows != nil || idx.encoding != VectorIndexEncodingFloat32 || idx.metric != VectorMetricCosine || idx.dimensions <= 0 || additional <= 0 {
		return
	}
	rows := make([]float32, 0, (len(idx.nodes)+additional)*idx.dimensions)
	for nodeID := range idx.nodes {
		if len(idx.nodes[nodeID].vector) != idx.dimensions {
			return
		}
		rows = append(rows, idx.nodes[nodeID].vector...)
	}
	idx.vectorRows = rows
	idx.frozenPrefixHeapRowStores++
	for nodeID := range idx.nodes {
		start := nodeID * idx.dimensions
		idx.nodes[nodeID].vector = idx.vectorRows[start : start+idx.dimensions]
	}
}

func (idx *columnGraphHNSWBuilder) pruneLayerNeighborsLocked(sourceNodeID int, neighbors []vectorIndexNeighbor, limit int) []vectorIndexNeighbor {
	return idx.pruneLayerNeighborsWithFrozenPrefixScratchLocked(sourceNodeID, neighbors, limit, nil)
}

func (idx *columnGraphHNSWBuilder) pruneLayerNeighborsWithFrozenPrefixScratchLocked(sourceNodeID int, neighbors []vectorIndexNeighbor, limit int, dotScratch *vectorIndexFrozenPrefixDiversityScratch) []vectorIndexNeighbor {
	return idx.pruneLayerNeighborsWithFrozenPrefixScratchObservedLocked(sourceNodeID, neighbors, limit, dotScratch, nil)
}

func (idx *columnGraphHNSWBuilder) pruneLayerNeighborsWithFrozenPrefixScratchObservedLocked(sourceNodeID int, neighbors []vectorIndexNeighbor, limit int, dotScratch *vectorIndexFrozenPrefixDiversityScratch, context *vectorIndexConstructionDecisionContextV1) []vectorIndexNeighbor {
	if limit <= 0 || len(neighbors) == 0 {
		return nil
	}
	if len(neighbors) <= limit {
		return neighbors
	}
	var stack [128]vectorIndexCandidate
	scored := stack[:0]
	if len(neighbors) > len(stack) {
		scored = make([]vectorIndexCandidate, 0, len(neighbors))
	}
	for _, neighbor := range neighbors {
		neighborID := int(neighbor.nodeID)
		if neighborID < 0 || neighborID >= len(idx.nodes) {
			continue
		}
		distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
		if !ok {
			continue
		}
		scored = append(scored, vectorIndexCandidate{nodeID: neighborID, distance: distance})
	}
	scored, _, _, _, _ = idx.selectConstructionDiverseCandidatesLocked(scored, limit, false, true, false, dotScratch, context, sourceNodeID)
	out := neighbors[:0]
	for _, candidate := range scored {
		out = append(out, vectorIndexNeighbor{nodeID: uint32(candidate.nodeID), distance: candidate.distance})
	}
	return out
}

func (idx *columnGraphHNSWBuilder) putSearchScratch(scratch *vectorIndexSearchScratch) {
	if scratch == nil {
		return
	}
	idx.searchScratch.Put(scratch)
}

func (idx *columnGraphHNSWBuilder) scoreSearchNodeWithPreparedQueryLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, nodeID int, scratch *vectorIndexSearchScratch) (float32, bool) {
	if scratch != nil && scratch.scoreTracking {
		if scratch.scoreLimit > 0 && scratch.scoreCalls >= scratch.scoreLimit {
			scratch.scoreBudgetExceeded = true
			return 0, false
		}
		scratch.scoreCalls++
	}
	return idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, nodeID), true
}

func (idx *columnGraphHNSWBuilder) searchLayerWithCandidateSeedsScratchModeObservedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, seeds []vectorIndexCandidate, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch, currentOnly bool, context *vectorIndexConstructionDecisionContextV1) []vectorIndexCandidate {
	return idx.searchLayerWithCandidateSeedsScratchModeObservedOrderLocked(query, queryNormSquared, prepared, entryPoint, seeds, limit, explorationLimit, layer, scratch, currentOnly, context, nil)
}

func (idx *columnGraphHNSWBuilder) searchLayerWithCandidateSeedsScratchModeObservedOrderLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, seeds []vectorIndexCandidate, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch, currentOnly bool, context *vectorIndexConstructionDecisionContextV1, candidateLess func(vectorIndexCandidate, vectorIndexCandidate) bool) []vectorIndexCandidate {
	if limit <= 0 || explorationLimit <= 0 || (len(seeds) == 0 && (entryPoint < 0 || entryPoint >= len(idx.nodes))) {
		return nil
	}
	if scratch == nil {
		scratch = &vectorIndexSearchScratch{}
	}
	scratch.explorationLimit = explorationLimit
	visited, mark := scratch.nextVisitedEpoch(len(idx.nodes))
	queue := scratch.queue[:0]
	best := scratch.best[:0]
	liveBest := scratch.liveBest[:0]
	if len(seeds) == 0 {
		distance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, entryPoint, scratch)
		if !ok {
			return nil
		}
		seeds = []vectorIndexCandidate{{nodeID: entryPoint, distance: distance}}
	}
	scratch.explored = 0
	for _, seed := range seeds {
		if seed.nodeID < 0 || seed.nodeID >= len(idx.nodes) || idx.nodes[seed.nodeID].level < layer || visited[seed.nodeID] == mark || (!currentOnly && math.IsInf(float64(seed.distance), 1)) {
			continue
		}
		if currentOnly && scratch.explored >= explorationLimit {
			break
		}
		visited[seed.nodeID] = mark
		scratch.explored++
		// Pre-scored unusable seeds still consumed their layer-0 work allowance.
		if math.IsInf(float64(seed.distance), 1) {
			continue
		}
		if context != nil {
			context.recordRow(seed.nodeID, false)
		}
		if candidateLess == nil {
			queue.push(seed)
			best.pushBounded(seed, explorationLimit)
		} else {
			queue.pushBy(seed, candidateLess)
			best.pushBoundedBy(seed, explorationLimit, candidateLess)
		}
		if currentOnly && !idx.nodes[seed.nodeID].deleted {
			liveBest.pushBounded(seed, limit)
		}
	}
	if len(best) == 0 {
		return nil
	}
search:
	for len(queue) > 0 {
		var current vectorIndexCandidate
		if candidateLess == nil {
			current = queue.pop()
		} else {
			current = queue.popBy(candidateLess)
		}
		if len(best) >= explorationLimit && ((candidateLess == nil && vectorIndexCandidateWorse(current, best[0])) || (candidateLess != nil && candidateLess(best[0], current))) {
			break
		}
		if current.nodeID < 0 || current.nodeID >= len(idx.nodes) {
			continue
		}
		for _, neighbor := range idx.layerNeighborsLocked(current.nodeID, layer) {
			neighborID := int(neighbor.nodeID)
			if neighborID < 0 || neighborID >= len(idx.nodes) || visited[neighborID] == mark {
				continue
			}
			if currentOnly && scratch.explored >= explorationLimit {
				break search
			}
			visited[neighborID] = mark
			if context != nil {
				context.recordRow(neighborID, false)
			}
			distance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID, scratch)
			if !ok {
				break search
			}
			scratch.explored++
			if scratch.explored&63 == 0 && scratch.context != nil {
				scratch.contextErr = scratch.context.Err()
				if scratch.contextErr != nil {
					break search
				}
			}
			if math.IsInf(float64(distance), 1) {
				continue
			}
			candidate := vectorIndexCandidate{nodeID: neighborID, distance: distance}
			if len(best) >= explorationLimit && ((candidateLess == nil && !vectorIndexCandidateLess(candidate, best[0])) || (candidateLess != nil && !candidateLess(candidate, best[0]))) {
				continue
			}
			if candidateLess == nil {
				queue.push(candidate)
				best.pushBounded(candidate, explorationLimit)
			} else {
				queue.pushBy(candidate, candidateLess)
				best.pushBoundedBy(candidate, explorationLimit, candidateLess)
			}
			if currentOnly && !idx.nodes[neighborID].deleted {
				liveBest.pushBounded(candidate, limit)
			}
		}
	}
	scratch.queue = queue[:0]
	scratch.best = best[:0]
	scratch.liveBest = liveBest[:0]
	out := scratch.out[:0]
	if currentOnly {
		out = append(out, liveBest...)
	} else {
		out = append(out, best...)
	}
	scratch.out = out
	if candidateLess == nil {
		sortVectorIndexCandidates(out)
	} else {
		idx.sortVectorIndexCandidatesByDistanceLocked(out)
	}
	return out
}

func (idx *columnGraphHNSWBuilder) searchLayerWithCanonicalCandidateSeedsScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, seeds []vectorIndexCandidate, limit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchLayerWithCandidateSeedsScratchModeObservedOrderLocked(query, queryNormSquared, prepared, entryPoint, seeds, limit, limit, layer, scratch, false, nil, idx.vectorIndexCanonicalCandidateLessLocked)
}

func (idx *columnGraphHNSWBuilder) searchLayerWithScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchLayerWithScratchModeLocked(query, queryNormSquared, prepared, entryPoint, limit, limit, layer, scratch, false)
}

func (idx *columnGraphHNSWBuilder) searchLayerWithScratchModeLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch, currentOnly bool) []vectorIndexCandidate {
	return idx.searchLayerWithScratchModeObservedLocked(query, queryNormSquared, prepared, entryPoint, limit, explorationLimit, layer, scratch, currentOnly, nil)
}

func (idx *columnGraphHNSWBuilder) searchLayerWithScratchModeObservedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch, currentOnly bool, context *vectorIndexConstructionDecisionContextV1) []vectorIndexCandidate {
	return idx.searchLayerWithCandidateSeedsScratchModeObservedLocked(query, queryNormSquared, prepared, entryPoint, nil, limit, explorationLimit, layer, scratch, currentOnly, context)
}

func (idx *columnGraphHNSWBuilder) searchLayerWithScratchObservedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit int, layer int, scratch *vectorIndexSearchScratch, context *vectorIndexConstructionDecisionContextV1) []vectorIndexCandidate {
	return idx.searchLayerWithScratchModeObservedLocked(query, queryNormSquared, prepared, entryPoint, limit, limit, layer, scratch, false, context)
}

func (idx *columnGraphHNSWBuilder) selectConstructionDiverseCandidatesLocked(candidates []vectorIndexCandidate, limit int, includeOrigins, backfillEnabled, captureQualityPostfill bool, dotScratch *vectorIndexFrozenPrefixDiversityScratch, context *vectorIndexConstructionDecisionContextV1, sourceNodeID int) ([]vectorIndexCandidate, int, int, map[int]bool, []vectorIndexCandidate) {
	if limit <= 0 || len(candidates) == 0 {
		return nil, 0, 0, nil, nil
	}
	idx.sortVectorIndexCandidatesByDistanceLocked(candidates)
	redundant := sourceNodeID >= 0 && idx.constructionCandidatesHaveIdenticalVectorsLocked(candidates)
	compare := idx.compareVectorIndexCandidatesByDistanceLocked
	if redundant {
		// A single transitive tie order applies to this bounded redundant pool.
		// Serving keeps its stable-ID comparator.
		compare = func(left, right vectorIndexCandidate) int {
			return idx.compareConstructionCandidatesLocked(left, right, sourceNodeID)
		}
		slices.SortFunc(candidates, compare)
	}
	orderedHash := uint64(0x4461)
	if context != nil {
		orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(context.phase+1))
		orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(context.source+1))
		orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(context.layer+1))
		for _, candidate := range candidates {
			orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(candidate.nodeID+1))
			orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(math.Float32bits(candidate.distance)))
		}
	}
	recordDecision := func(selected []vectorIndexCandidate) {
		if context != nil {
			context.recordDecision(orderedHash, len(candidates), len(selected), selected)
		}
	}
	// Backfill-on preserves the historical degree-filling fast path.  The
	// backfill-off construction policies instead need to run the diversity
	// predicate even below their selection cap: a small candidate set can still
	// contain mutually redundant neighbors, and those rejected candidates are
	// the causal pool for the later offline refinements.
	if len(candidates) <= limit && backfillEnabled {
		recordDecision(candidates)
		if !includeOrigins {
			return candidates, len(candidates), 0, nil, nil
		}
		diversity := make(map[int]bool, len(candidates))
		for _, candidate := range candidates {
			diversity[candidate.nodeID] = true
		}
		return candidates, len(candidates), 0, diversity, nil
	}
	if idx.metric == VectorMetricInnerProduct && !redundant {
		recordDecision(candidates[:limit])
		if !includeOrigins {
			return candidates[:limit], limit, 0, nil, nil
		}
		diversity := make(map[int]bool, limit)
		for _, candidate := range candidates[:limit] {
			diversity[candidate.nodeID] = true
		}
		return candidates[:limit], limit, 0, diversity, nil
	}
	var selectedStack [128]vectorIndexCandidate
	selected := selectedStack[:0]
	if limit > len(selectedStack) {
		selected = make([]vectorIndexCandidate, 0, limit)
	}
	var rejectedStack [128]vectorIndexCandidate
	rejected := rejectedStack[:0]
	if len(candidates) > len(rejectedStack) {
		rejected = make([]vectorIndexCandidate, 0, len(candidates))
	}
	for _, candidate := range candidates {
		if len(selected) >= limit {
			orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(candidate.nodeID+1)<<1)
			rejected = append(rejected, candidate)
			continue
		}
		identical := false
		if redundant {
			for _, existing := range selected {
				if idx.constructionNodesHaveIdenticalVectorsLocked(candidate.nodeID, existing.nodeID) {
					identical = true
					break
				}
			}
		}
		if !identical && (idx.metric == VectorMetricInnerProduct || idx.vectorIndexCandidateIsDiverseWithFrozenPrefixScratchObservedLocked(candidate, selected, dotScratch, context)) {
			orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(candidate.nodeID+1)<<1|1)
			selected = append(selected, candidate)
		} else {
			orderedHash = vectorIndexConstructionDecisionHashV1(orderedHash, uint64(candidate.nodeID+1)<<1)
			rejected = append(rejected, candidate)
		}
	}
	out := candidates[:0]
	var diversity map[int]bool
	if includeOrigins {
		diversity = make(map[int]bool, len(selected))
		for _, candidate := range selected {
			diversity[candidate.nodeID] = true
		}
	}
	if !backfillEnabled {
		out = append(out, selected...)
		recordDecision(out)
		if captureQualityPostfill {
			return out, len(selected), 0, diversity, append([]vectorIndexCandidate(nil), rejected...)
		}
		return out, len(selected), 0, diversity, nil
	}
	backfill := minInt(limit-len(selected), len(rejected))
	if backfill == 0 {
		out = append(out, selected...)
		recordDecision(out)
		return out, len(selected), 0, diversity, nil
	}
	selectedPos := 0
	rejectedPos := 0
	for selectedPos < len(selected) && rejectedPos < backfill {
		if compare(selected[selectedPos], rejected[rejectedPos]) <= 0 {
			out = append(out, selected[selectedPos])
			selectedPos++
			continue
		}
		out = append(out, rejected[rejectedPos])
		rejectedPos++
	}
	out = append(out, selected[selectedPos:]...)
	out = append(out, rejected[rejectedPos:backfill]...)
	recordDecision(out)
	return out, len(selected), 0, diversity, nil
}

func (idx *columnGraphHNSWBuilder) selectLayerNeighborsLocked(vector []float32, vectorNormSquared float64, prepared *preparedFloat32CosineQuery, candidates []vectorIndexCandidate, layer, limit, excludeNodeID int) []int {
	return idx.selectLayerNeighborsObservedLocked(vector, vectorNormSquared, prepared, candidates, layer, limit, excludeNodeID, nil)
}

func (idx *columnGraphHNSWBuilder) selectLayerNeighborsObservedLocked(vector []float32, vectorNormSquared float64, prepared *preparedFloat32CosineQuery, candidates []vectorIndexCandidate, layer, limit, excludeNodeID int, context *vectorIndexConstructionDecisionContextV1) []int {
	if limit <= 0 {
		return nil
	}
	trace := idx.constructionTrace
	candidateCount := len(candidates)
	var sampledCandidates []int
	sampled := false
	if trace != nil {
		// Evidence counts candidate ordinals, not raw heap entries. The search
		// selection itself deliberately retains duplicate entries for historical
		// behavior, but sampled and unsampled accounting must share set semantics.
		distinct := make(map[int]struct{}, len(candidates))
		sampled = layer == 0 && excludeNodeID >= 0 && excludeNodeID < len(idx.nodes)
		if sampled {
			_, sampled = trace.sampleIDs[string(idx.nodes[excludeNodeID].documentID)]
			if sampled {
				sampledCandidates = make([]int, 0, len(candidates))
			}
		}
		for _, candidate := range candidates {
			if candidate.nodeID == excludeNodeID || candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) || idx.nodes[candidate.nodeID].level < layer || math.IsInf(float64(candidate.distance), 1) {
				continue
			}
			if _, seen := distinct[candidate.nodeID]; seen {
				continue
			}
			distinct[candidate.nodeID] = struct{}{}
			if sampled {
				sampledCandidates = append(sampledCandidates, candidate.nodeID)
			}
		}
		candidateCount = len(distinct)
	}
	scored := candidates[:0]
	for _, candidate := range candidates {
		if candidate.nodeID == excludeNodeID || candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		if idx.nodes[candidate.nodeID].level < layer {
			continue
		}
		if math.IsInf(float64(candidate.distance), 1) {
			continue
		}
		scored = append(scored, candidate)
	}
	var diversitySelected int
	var diversity map[int]bool
	backfill := true
	qualityPostfill := false
	robustPruneRefinement := false
	if layer == 0 && idx.layer0ConstructionPolicy != nil {
		backfill = idx.layer0ConstructionPolicy.backfill
		qualityPostfill = idx.layer0ConstructionPolicy.qualityPostfill
		robustPruneRefinement = idx.layer0ConstructionPolicy.robustPruneRefinement
	}
	// Keep every finite construction comparison, not only diversity rejects.
	// An initial directed selection is an equally valid observed pair for the
	// reverse endpoint; retaining the symmetric relation lets the explicit
	// post-construction pass fill early nodes without inventing an unobserved
	// nearest-neighbor fallback.
	capturePostfill := qualityPostfill || robustPruneRefinement
	var constructionCandidates []vectorIndexCandidate
	if capturePostfill {
		// This candidate-pool snapshot is needed only by the two offline final
		// refinements. Keep the ordinary and non-refinement policy paths free of
		// this allocation.
		constructionCandidates = append([]vectorIndexCandidate(nil), scored...)
	}
	scored, diversitySelected, _, diversity, _ = idx.selectConstructionDiverseCandidatesLocked(scored, limit, trace != nil, backfill, false, nil, context, excludeNodeID)
	if capturePostfill {
		idx.captureQualityPostfillCandidatesLocked(excludeNodeID, constructionCandidates)
	}
	if trace != nil {
		selection := vectorIndexConstructionSelectionV1{Node: excludeNodeID, Layer: layer, Candidates: candidateCount, Selected: len(scored), DiversitySelected: diversitySelected, BackfillSelected: len(scored) - diversitySelected}
		if sampled {
			selection.Sampled = true
			selection.CandidateNodes = sampledCandidates
		}
		trace.selections = append(trace.selections, selection)
		for _, candidate := range scored {
			origin := "nearest_backfill"
			if diversity[candidate.nodeID] {
				origin = "diversity_selected"
			}
			trace.selectEdge(excludeNodeID, candidate.nodeID, layer, origin)
		}
	}
	out := make([]int, len(scored))
	for i := range scored {
		out[i] = scored[i].nodeID
	}
	return out
}

func (idx *columnGraphHNSWBuilder) sortVectorIndexCandidatesByDistanceLocked(candidates []vectorIndexCandidate) {
	inversion := idx.firstVectorIndexCandidateInversionLocked(candidates)
	switch {
	case inversion == -1:
		return
	case inversion == len(candidates)-1:
		idx.insertTailVectorIndexCandidateByDistanceLocked(candidates)
	default:
		slices.SortFunc(candidates, idx.compareVectorIndexCandidatesByDistanceLocked)
	}
}

func (idx *columnGraphHNSWBuilder) tombstoneDocumentIDLocked(documentID []byte) {
	idx.capturePartitionLiveDomainCurrentNodeV2Locked(documentID)
	nodeID, ok := idx.currentNode[string(documentID)]
	if !ok {
		return
	}
	idx.prepareSearchViewForMutationLocked()
	if nodeID >= 0 && nodeID < len(idx.nodes) {
		idx.capturePartitionLiveDomainNodeV2Locked(nodeID)
		idx.nodes[nodeID].deleted = true
		idx.markVectorNodeDirtyLocked(nodeID)
	}
	delete(idx.currentNode, string(documentID))
	idx.markVectorDocDirtyLocked(documentID)
	idx.markGraphChangedLocked()
	if idx.entry == nodeID {
		idx.entry = idx.firstLiveNodeLocked()
		idx.maxLevel = idx.maxLiveLevelLocked()
		if idx.entry >= 0 {
			idx.entry = idx.firstLiveNodeAtLevelLocked(idx.maxLevel)
		}
		idx.markVectorMetaDirtyLocked()
	}
}

func (idx *columnGraphHNSWBuilder) validateVectorBatchWithContext(ctx context.Context, documentIDs [][]byte, vectors [][]float32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(documentIDs) != len(vectors) {
		return errors.New("collections: vector index batch ids/vectors length mismatch")
	}
	if !vectorIndexNodeOrdinalsFitUint32(uint64(len(idx.nodes)), uint64(len(documentIDs))) {
		return errors.New("collections: vector index node ordinal exceeds uint32")
	}
	dimensions := idx.dimensions
	for row := range documentIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(documentIDs[row]) == 0 {
			return errors.New("collections: document id cannot be empty")
		}
		if dimensions == 0 {
			dimensions = len(vectors[row])
		}
		if len(vectors[row]) != dimensions {
			return fmt.Errorf("collections: vector field %q in document %q has dimension %d, want %d", idx.field, documentIDs[row], len(vectors[row]), dimensions)
		}
		var norm float64
		for i, value := range vectors[row] {
			if i&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			f := float64(value)
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return fmt.Errorf("element %d is not finite", i)
			}
			if idx.metric == VectorMetricCosine {
				norm += f * f
			}
		}
		if idx.metric == VectorMetricCosine && norm == 0 {
			return errors.New("collections: cosine vector cannot have zero magnitude")
		}
	}
	return nil
}

func (idx *columnGraphHNSWBuilder) vectorIndexCandidateIsDiverseWithFrozenPrefixScratchObservedLocked(candidate vectorIndexCandidate, selected []vectorIndexCandidate, dotScratch *vectorIndexFrozenPrefixDiversityScratch, context *vectorIndexConstructionDecisionContextV1) bool {
	if idx.metric == VectorMetricCosine && candidate.nodeID >= 0 && candidate.nodeID < len(idx.nodes) {
		candidateNode := &idx.nodes[candidate.nodeID]
		if len(candidateNode.vector) > 0 && candidateNode.cachedInvNorm != 0 {
			// Squared normalized differences cannot be smaller than zero.
			if candidate.distance <= 0 {
				if context != nil {
					context.recordEarlyExit(0, len(selected), true)
				}
				return true
			}
			if idx.frozenPrefixIndexedDotRowsReadyLocked(candidate.nodeID, candidateNode, selected, dotScratch, context) {
				for i, existing := range selected {
					distance := float32(1 - float64(dotScratch.dots[i])*candidateNode.cachedInvNorm*idx.nodes[existing.nodeID].cachedInvNorm)
					// A separated comparison can keep the indexed FP32 dot. Close
					// decisions require the same stable distance as scalar construction.
					if math.Abs(float64(distance)-float64(candidate.distance)) <= float64(idx.dimensions+2)*0x1p-21 || math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 0) {
						if context != nil {
							context.recordRowFrom(candidate.nodeID, existing.nodeID, false)
						}
						distance = vectorDistanceBetweenFloat32NodesCosineUnchecked(candidateNode, &idx.nodes[existing.nodeID])
					}
					if distance < candidate.distance {
						if context != nil {
							context.recordEarlyExit(i+1, len(selected), false)
						}
						return false
					}
				}
				dotScratch.indexedBatches++
				if context != nil {
					context.recordEarlyExit(len(selected), len(selected), true)
				}
				return true
			}
			for position, existing := range selected {
				if context != nil {
					context.recordRowFrom(candidate.nodeID, existing.nodeID, false)
				}
				distance := idx.distanceBetweenFloat32CosineCandidateAndNodeLocked(candidateNode, existing.nodeID)
				if math.IsInf(float64(distance), 1) {
					continue
				}
				if distance < candidate.distance {
					if context != nil {
						context.recordEarlyExit(position+1, len(selected), false)
					}
					return false
				}
			}
			if context != nil {
				context.recordEarlyExit(len(selected), len(selected), true)
			}
			return true
		}
	}
	for position, existing := range selected {
		if context != nil {
			context.recordRowFrom(candidate.nodeID, existing.nodeID, false)
		}
		distance, ok := idx.distanceBetweenNodesFastLocked(candidate.nodeID, existing.nodeID)
		if !ok {
			distance = idx.distanceBetweenNodesLocked(candidate.nodeID, existing.nodeID)
		}
		if distance < candidate.distance {
			if context != nil {
				context.recordEarlyExit(position+1, len(selected), false)
			}
			return false
		}
	}
	if context != nil {
		context.recordEarlyExit(len(selected), len(selected), true)
	}
	return true
}

func (idx *columnGraphHNSWBuilder) vectorIndexCanonicalCandidateLessLocked(left, right vectorIndexCandidate) bool {
	return idx.compareVectorIndexCandidatesByDistanceLocked(left, right) < 0
}

func (idx *columnGraphHNSWBuilder) vectorIndexNodeMatchesSourceVectorLocked(node *vectorIndexNode, vector []float32) bool {
	if idx.encoding == VectorIndexEncodingInt8 {
		return node.matchesInt8SourceVector(vector)
	}
	return node.matchesVector(vector)
}

func (idx *columnGraphHNSWBuilder) searchCandidatesLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if idx.entry < 0 || len(idx.nodes) == 0 || limit <= 0 {
		return nil
	}
	entryPoint := idx.entry
	for layer := idx.maxLevel; layer > 0; layer-- {
		entryPoint = idx.greedyNearestAtLayerLocked(query, queryNormSquared, prepared, entryPoint, layer)
	}
	return idx.searchLayerWithScratchLocked(query, queryNormSquared, prepared, entryPoint, limit, 0, scratch)
}

func (idx *columnGraphHNSWBuilder) searchCurrentCandidatesLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchCurrentCandidatesWithLiveDocsLocked(query, queryNormSquared, prepared, limit, len(idx.currentNode), scratch)
}

func (idx *columnGraphHNSWBuilder) searchCurrentCandidatesWithLiveDocsLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit, liveDocs int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if idx.entry < 0 || len(idx.nodes) == 0 || limit <= 0 {
		return nil
	}
	stale := len(idx.nodes) - liveDocs
	if stale == 0 {
		entryPoint := idx.entry
		upperExplored := 0
		for layer := idx.maxLevel; layer > 0; layer-- {
			entryPoint = idx.greedyNearestAtLayerSearchBoundedLocked(query, queryNormSquared, prepared, entryPoint, layer, math.MaxInt, &upperExplored, scratch)
			if scratch != nil && scratch.scoreBudgetExceeded {
				return nil
			}
		}
		return idx.searchLayerWithScratchLocked(query, queryNormSquared, prepared, entryPoint, limit, 0, scratch)
	}
	explorationLimit := limit
	if stale > 0 && explorationLimit < len(idx.nodes) {
		// Tombstones remain waypoints, but historical churn must not turn a
		// bounded ANN query into an O(nodes) traversal. Permit at most one
		// layer-0 neighbor fanout of stale waypoints per live candidate.
		degree := maxInt(1, idx.maxNeighborsForLayer(0))
		maxExtra := math.MaxInt
		if degree > 1 && limit <= math.MaxInt/(degree-1) {
			maxExtra = limit * (degree - 1)
		}
		explorationLimit += minInt(stale, minInt(maxExtra, len(idx.nodes)-explorationLimit))
	}
	if scratch == nil {
		scratch = &vectorIndexSearchScratch{}
	}
	entryPoint := idx.entry
	upperExplored := 0
	// Keep the requested live-candidate budget plus one possible stale entry
	// point for layer 0. Upper layers share the remaining stale allowance.
	upperLimit := maxInt(0, explorationLimit-limit-1)
	for layer := idx.maxLevel; layer > 0 && upperExplored < upperLimit; layer-- {
		entryPoint = idx.greedyNearestAtLayerSearchBoundedLocked(query, queryNormSquared, prepared, entryPoint, layer, upperLimit, &upperExplored, scratch)
		if scratch != nil && scratch.scoreBudgetExceeded {
			return nil
		}
	}
	remaining := explorationLimit - upperExplored
	var seeds []vectorIndexCandidate
	if remaining >= 4 && idx.entry < len(idx.nodes) && !idx.nodes[idx.entry].deleted && entryPoint >= 0 && entryPoint < len(idx.nodes) && idx.nodes[entryPoint].deleted && entryPoint != idx.entry {
		// The closer tombstone route can starve a farther live entry. Admit its
		// existing one-hop neighborhood, retaining both routes in the same budget.
		seeds = scratch.out[:0]
		// ponytail: duplicate checks are quadratic in this one-hop degree; use an epoch only if profiling warrants it.
		admit := func(nodeID int) bool {
			if nodeID < 0 || nodeID >= len(idx.nodes) {
				return true
			}
			for _, seed := range seeds {
				if seed.nodeID == nodeID {
					return true
				}
			}
			if scratch.finalContextErr() != nil {
				return false
			}
			distance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, nodeID, scratch)
			if !ok {
				return false
			}
			seeds = append(seeds, vectorIndexCandidate{nodeID: nodeID, distance: distance})
			return true
		}
		if !admit(entryPoint) || !admit(idx.entry) {
			return nil
		}
		neighbors := idx.layerNeighborsLocked(idx.entry, 0)
		// The endpoints consume two slots; reserve one for normal expansion.
		for _, neighbor := range neighbors[:minInt(len(neighbors), remaining-3)] {
			if !admit(int(neighbor.nodeID)) {
				return nil
			}
		}
		scratch.out = seeds
	}
	result := idx.searchLayerWithCandidateSeedsScratchModeObservedLocked(query, queryNormSquared, prepared, entryPoint, seeds, limit, remaining, 0, scratch, true, nil)
	scratch.explored += upperExplored
	scratch.explorationLimit = explorationLimit
	return result
}

func (idx *columnGraphHNSWBuilder) planFrozenPrefixInsertLocked(ctx context.Context, documentID []byte, vector []float32, entry, maxLevel, sourceNodeID int, observer *vectorIndexConstructionDecisionObserverV1) (vectorIndexFrozenPrefixInsert, error) {
	return idx.planFrozenPrefixInsertWithScratchLocked(ctx, documentID, vector, entry, maxLevel, sourceNodeID, observer, nil)
}

func (idx *columnGraphHNSWBuilder) linkFrozenPrefixReciprocalGroupLocked(links []vectorIndexFrozenPrefixReciprocalLink, dotScratch *vectorIndexFrozenPrefixDiversityScratch) bool {
	return idx.linkFrozenPrefixReciprocalGroupObservedLocked(links, dotScratch, idx.decisionObserver)
}

func (idx *columnGraphHNSWBuilder) newVectorIndexNode(documentID []byte, vector []float32, level int) vectorIndexNode {
	var quantized []int8
	var quantScale float32
	if idx.encoding == VectorIndexEncodingInt8 {
		quantized, quantScale = quantizeVectorIndexInt8(vector)
	}
	return idx.newVectorIndexNodePrepared(documentID, vector, level, quantized, quantScale)
}

func (idx *columnGraphHNSWBuilder) greedyNearestAtLayerSearchBoundedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint, layer, scoreLimit int, scored *int, scratch *vectorIndexSearchScratch) int {
	if entryPoint < 0 || scored == nil || *scored >= scoreLimit {
		return entryPoint
	}
	best := entryPoint
	bestDistance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, best, scratch)
	if !ok {
		return best
	}
	(*scored)++
	for changed := true; changed; {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best, layer) {
			if *scored >= scoreLimit {
				return best
			}
			distance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, int(neighbor.nodeID), scratch)
			if !ok {
				return best
			}
			(*scored)++
			if distance < bestDistance {
				best = int(neighbor.nodeID)
				bestDistance = distance
				changed = true
			}
		}
	}
	return best
}

func (idx *columnGraphHNSWBuilder) searchLayerWithCandidateSeedsScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, seeds []vectorIndexCandidate, limit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchLayerWithCandidateSeedsScratchModeObservedLocked(query, queryNormSquared, prepared, entryPoint, seeds, limit, limit, layer, scratch, false, nil)
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesLocked(candidates []vectorIndexCandidate, limit int) []vectorIndexCandidate {
	selected, _ := idx.selectDiverseCandidatesWithAccountingLocked(candidates, limit)
	return selected
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesWithAccountingLocked(candidates []vectorIndexCandidate, limit int) ([]vectorIndexCandidate, int) {
	return idx.selectDiverseCandidatesWithFrozenPrefixScratchLocked(candidates, limit, nil)
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesWithFrozenPrefixScratchLocked(candidates []vectorIndexCandidate, limit int, dotScratch *vectorIndexFrozenPrefixDiversityScratch) ([]vectorIndexCandidate, int) {
	selected, diversitySelected, _, _, _ := idx.selectDiverseCandidatesWithDetailsAndFrozenPrefixScratchLocked(candidates, limit, false, true, false, dotScratch)
	return selected, diversitySelected
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesWithOriginsLocked(candidates []vectorIndexCandidate, limit int) ([]vectorIndexCandidate, int, map[int]bool) {
	selected, diversitySelected, _, origins, _ := idx.selectDiverseCandidatesWithDetailsLocked(candidates, limit, true, true, false)
	return selected, diversitySelected, origins
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesWithDetailsLocked(candidates []vectorIndexCandidate, limit int, includeOrigins, backfillEnabled, captureQualityPostfill bool) ([]vectorIndexCandidate, int, int, map[int]bool, []vectorIndexCandidate) {
	return idx.selectDiverseCandidatesWithDetailsAndFrozenPrefixScratchLocked(candidates, limit, includeOrigins, backfillEnabled, captureQualityPostfill, nil)
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesWithDetailsAndFrozenPrefixScratchLocked(candidates []vectorIndexCandidate, limit int, includeOrigins, backfillEnabled, captureQualityPostfill bool, dotScratch *vectorIndexFrozenPrefixDiversityScratch) ([]vectorIndexCandidate, int, int, map[int]bool, []vectorIndexCandidate) {
	return idx.selectDiverseCandidatesWithDetailsAndFrozenPrefixScratchObservedLocked(candidates, limit, includeOrigins, backfillEnabled, captureQualityPostfill, dotScratch, nil)
}

func (idx *columnGraphHNSWBuilder) selectDiverseCandidatesWithDetailsAndFrozenPrefixScratchObservedLocked(candidates []vectorIndexCandidate, limit int, includeOrigins, backfillEnabled, captureQualityPostfill bool, dotScratch *vectorIndexFrozenPrefixDiversityScratch, context *vectorIndexConstructionDecisionContextV1) ([]vectorIndexCandidate, int, int, map[int]bool, []vectorIndexCandidate) {
	return idx.selectConstructionDiverseCandidatesLocked(candidates, limit, includeOrigins, backfillEnabled, captureQualityPostfill, dotScratch, context, -1)
}

func (idx *columnGraphHNSWBuilder) vectorIndexCandidateIsDiverseLocked(candidate vectorIndexCandidate, selected []vectorIndexCandidate) bool {
	return idx.vectorIndexCandidateIsDiverseWithFrozenPrefixScratchLocked(candidate, selected, nil)
}

func (idx *columnGraphHNSWBuilder) vectorIndexCandidateIsDiverseWithFrozenPrefixScratchLocked(candidate vectorIndexCandidate, selected []vectorIndexCandidate, dotScratch *vectorIndexFrozenPrefixDiversityScratch) bool {
	return idx.vectorIndexCandidateIsDiverseWithFrozenPrefixScratchObservedLocked(candidate, selected, dotScratch, nil)
}
