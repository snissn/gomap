package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

const (
	vectorPartitionVamanaDegreeV1       = 64
	vectorPartitionVamanaSearchListV1   = 256
	vectorPartitionVamanaCandidateCapV1 = 750
	vectorPartitionVamanaFinalAlphaV1   = float32(1.2)
)

type vectorPartitionVamanaScratchV1 struct {
	visited    []uint32
	epoch      uint32
	queue      vectorIndexMinCandidateHeap
	best       vectorIndexMaxCandidateHeap
	expanded   []vectorIndexCandidate
	candidates []vectorIndexCandidate
	byNode     map[int]float32
	removed    []bool
}

type vectorPartitionVamanaBuildStatsV1 struct {
	PassSearches         [2]uint64
	OutgoingReplacements [2]uint64
	ReciprocalInsertions [2]uint64
	OverflowPrunes       [2]uint64
	ConnectivityRepairs  int
	MaxVisitedCandidates int
	MaxRobustPrunePool   int
}

// buildVectorPartitionVamanaV1 implements the two-pass Vamana construction
// from the DiskANN paper over one membership-bound partition, followed by the
// shared degree-preserving entry-reachability pass required by the serving
// contract. Rows are then reordered breadth-first from the centroid entry so
// the existing flat search pack can keep its entry-at-zero contract.
func buildVectorPartitionVamanaV1(ctx context.Context, rows []columnVectorGraphAssetRow, dimensions int) error {
	return buildVectorPartitionVamanaWithStatsV1(ctx, rows, dimensions, nil)
}

func buildVectorPartitionVamanaWithStatsV1(ctx context.Context, rows []columnVectorGraphAssetRow, dimensions int, stats *vectorPartitionVamanaBuildStatsV1) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if dimensions <= 0 || uint64(len(rows)) > maxColumnVectorGraphAdjacencyOrdinal {
		return errors.New("collections: invalid partition-local Vamana shape")
	}
	if len(rows) > math.MaxInt/dimensions {
		return errors.New("collections: partition-local Vamana vector matrix overflows int")
	}
	vectors := make([]float32, len(rows)*dimensions)
	centroid := make([]float64, dimensions)
	for ordinal := range rows {
		if ordinal&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if len(rows[ordinal].Vector) != dimensions {
			return fmt.Errorf("collections: partition-local Vamana row[%d] dimensions=%d want %d", ordinal, len(rows[ordinal].Vector), dimensions)
		}
		invNorm := rows[ordinal].InvNorm
		if invNorm <= 0 || math.IsNaN(float64(invNorm)) || math.IsInf(float64(invNorm), 0) {
			var err error
			invNorm, err = columnVectorGraphInvNorm(rows[ordinal].Vector)
			if err != nil {
				return fmt.Errorf("collections: partition-local Vamana row[%d] inverse norm: %w", ordinal, err)
			}
			rows[ordinal].InvNorm = invNorm
		}
		base := ordinal * dimensions
		for dim, value := range rows[ordinal].Vector {
			normalized := value * invNorm
			if math.IsNaN(float64(normalized)) || math.IsInf(float64(normalized), 0) {
				return fmt.Errorf("collections: partition-local Vamana row[%d] dimension[%d] is not finite", ordinal, dim)
			}
			vectors[base+dim] = normalized
			centroid[dim] += float64(normalized)
		}
	}
	if len(rows) == 1 {
		rows[0].Adjacency = []uint32{columnVectorGraphLayeredAdjacencyMagic, 0, 0}
		return nil
	}
	entry := vectorPartitionVamanaCentroidEntryV1(vectors, centroid, len(rows), dimensions)
	seed := vectorPartitionVamanaSeedV1(rows)
	adjacency := vectorPartitionVamanaInitialGraphV1(len(rows), seed)
	scratch := vectorPartitionVamanaScratchV1{visited: make([]uint32, len(rows))}
	permutation := make([]int, len(rows))
	for i := range permutation {
		permutation[i] = i
	}
	for pass, alpha := range []float32{1, vectorPartitionVamanaFinalAlphaV1} {
		vectorPartitionVamanaShuffleV1(permutation, seed+uint64(pass+1)*0x9e3779b97f4a7c15)
		for position, source := range permutation {
			if position&31 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			visited := scratch.greedySearch(entry, source, vectors, dimensions, adjacency, vectorPartitionVamanaSearchListV1)
			if len(visited) > 1+vectorPartitionVamanaSearchListV1*vectorPartitionVamanaDegreeV1 {
				return errors.New("collections: partition-local Vamana construction search exceeded bounded work")
			}
			if stats != nil {
				stats.PassSearches[pass]++
				stats.MaxVisitedCandidates = max(stats.MaxVisitedCandidates, len(visited))
			}
			scratch.candidates = vectorPartitionVamanaMergeCurrentNeighborsV1(scratch.candidates[:0], source, visited, adjacency[source], vectors, dimensions)
			var previous []uint32
			if stats != nil {
				previous = append(previous, adjacency[source]...)
			}
			next := scratch.robustPrune(source, scratch.candidates, vectors, dimensions, alpha, vectorPartitionVamanaDegreeV1, adjacency[source][:0], stats)
			if stats != nil && !slices.Equal(previous, next) {
				stats.OutgoingReplacements[pass]++
			}
			adjacency[source] = next
			for _, neighbor := range adjacency[source] {
				to := int(neighbor)
				if vectorPartitionVamanaContainsV1(adjacency[to], uint32(source)) {
					continue
				}
				adjacency[to] = append(adjacency[to], uint32(source))
				if stats != nil {
					stats.ReciprocalInsertions[pass]++
				}
				if len(adjacency[to]) > vectorPartitionVamanaDegreeV1 {
					scratch.candidates = scratch.candidates[:0]
					for _, candidate := range adjacency[to] {
						scratch.candidates = append(scratch.candidates, vectorIndexCandidate{nodeID: int(candidate), distance: vectorPartitionVamanaDistanceV1(vectors, dimensions, to, int(candidate))})
					}
					adjacency[to] = scratch.robustPrune(to, scratch.candidates, vectors, dimensions, alpha, vectorPartitionVamanaDegreeV1, adjacency[to][:0], stats)
					if stats != nil {
						stats.OverflowPrunes[pass]++
					}
				}
			}
		}
	}
	for ordinal := range rows {
		rows[ordinal].Adjacency = adjacency[ordinal]
	}
	repairs, err := repairVectorPartitionLocalLayer0ReachabilityV1(rows, entry)
	if err != nil {
		return err
	}
	if stats != nil {
		stats.ConnectivityRepairs = repairs
	}
	for ordinal := range rows {
		adjacency[ordinal] = rows[ordinal].Adjacency
	}
	return vectorPartitionVamanaLocalityOrderV1(ctx, rows, adjacency, entry)
}

func vectorPartitionVamanaMergeCurrentNeighborsV1(dst []vectorIndexCandidate, source int, visited []vectorIndexCandidate, current []uint32, vectors []float32, dimensions int) []vectorIndexCandidate {
	dst = append(dst, visited...)
	for _, neighbor := range current {
		dst = append(dst, vectorIndexCandidate{nodeID: int(neighbor), distance: vectorPartitionVamanaDistanceV1(vectors, dimensions, source, int(neighbor))})
	}
	return dst
}

func vectorPartitionVamanaCentroidEntryV1(vectors []float32, centroid []float64, rows, dimensions int) int {
	best, bestDistance := 0, math.Inf(1)
	for row := 0; row < rows; row++ {
		var distance float64
		base := row * dimensions
		for dim := 0; dim < dimensions; dim++ {
			delta := float64(vectors[base+dim]) - centroid[dim]/float64(rows)
			distance += delta * delta
		}
		if distance < bestDistance {
			best, bestDistance = row, distance
		}
	}
	return best
}

func vectorPartitionVamanaSeedV1(rows []columnVectorGraphAssetRow) uint64 {
	h := sha256.New()
	h.Write([]byte("treedb-partition-vamana-v1/"))
	var length [8]byte
	for _, row := range rows {
		binary.LittleEndian.PutUint64(length[:], uint64(len(row.ID)))
		h.Write(length[:])
		h.Write(row.ID)
	}
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}

func vectorPartitionVamanaInitialGraphV1(rows int, seed uint64) [][]uint32 {
	graph := make([][]uint32, rows)
	degree := min(vectorPartitionVamanaDegreeV1, rows-1)
	if degree <= 0 {
		return graph
	}
	stride := degree + 1
	storage := make([]uint32, rows*stride)
	for row := range graph {
		neighbors := storage[row*stride : row*stride : (row+1)*stride]
		add := func(candidate int) {
			if candidate != row && !vectorPartitionVamanaContainsV1(neighbors, uint32(candidate)) {
				neighbors = append(neighbors, uint32(candidate))
			}
		}
		add((row + 1) % rows)
		add((row + rows - 1) % rows)
		state := seed ^ uint64(row+1)*0x9e3779b97f4a7c15
		for len(neighbors) < degree {
			state = vectorPartitionVamanaSplitMix64V1(state)
			add(int(state % uint64(rows)))
		}
		sort.Slice(neighbors, func(i, j int) bool { return neighbors[i] < neighbors[j] })
		graph[row] = neighbors
	}
	return graph
}

func (s *vectorPartitionVamanaScratchV1) greedySearch(entry, target int, vectors []float32, dimensions int, adjacency [][]uint32, limit int) []vectorIndexCandidate {
	s.epoch++
	if s.epoch == 0 {
		clear(s.visited)
		s.epoch = 1
	}
	s.queue = s.queue[:0]
	s.best = s.best[:0]
	s.expanded = s.expanded[:0]
	visit := func(node int) {
		s.visited[node] = s.epoch
		distance := vectorPartitionVamanaDistanceV1(vectors, dimensions, target, node)
		candidate := vectorIndexCandidate{nodeID: node, distance: distance}
		if len(s.best) < limit || vectorIndexCandidateLess(candidate, s.best[0]) {
			s.queue.push(candidate)
			s.best.pushBounded(candidate, limit)
		}
	}
	visit(entry)
	for len(s.queue) != 0 {
		current := s.queue.pop()
		if len(s.best) >= limit && vectorIndexCandidateWorse(current, s.best[0]) {
			break
		}
		s.expanded = append(s.expanded, current)
		for _, raw := range adjacency[current.nodeID] {
			neighbor := int(raw)
			if s.visited[neighbor] != s.epoch {
				visit(neighbor)
			}
		}
	}
	out := s.expanded[:0]
	for _, candidate := range s.expanded {
		if candidate.nodeID != target {
			out = append(out, candidate)
		}
	}
	s.expanded = out
	return s.expanded
}

func (s *vectorPartitionVamanaScratchV1) robustPrune(source int, candidates []vectorIndexCandidate, vectors []float32, dimensions int, alpha float32, degree int, selected []uint32, stats *vectorPartitionVamanaBuildStatsV1) []uint32 {
	if s.byNode == nil {
		s.byNode = make(map[int]float32, min(len(candidates), vectorPartitionVamanaCandidateCapV1))
	} else {
		clear(s.byNode)
	}
	for _, candidate := range candidates {
		if candidate.nodeID == source {
			continue
		}
		if distance, ok := s.byNode[candidate.nodeID]; !ok || candidate.distance < distance {
			s.byNode[candidate.nodeID] = candidate.distance
		}
	}
	pool := candidates[:0]
	for node, distance := range s.byNode {
		pool = append(pool, vectorIndexCandidate{nodeID: node, distance: distance})
	}
	sort.Slice(pool, func(i, j int) bool { return vectorIndexCandidateLess(pool[i], pool[j]) })
	if len(pool) > vectorPartitionVamanaCandidateCapV1 {
		pool = pool[:vectorPartitionVamanaCandidateCapV1]
	}
	if stats != nil {
		stats.MaxRobustPrunePool = max(stats.MaxRobustPrunePool, len(pool))
	}
	selected = selected[:0]
	if cap(selected) < min(degree, len(pool)) {
		selected = make([]uint32, 0, min(degree, len(pool)))
	}
	if cap(s.removed) < len(pool) {
		s.removed = make([]bool, len(pool))
	} else {
		s.removed = s.removed[:len(pool)]
		clear(s.removed)
	}
	for i := range pool {
		if s.removed[i] {
			continue
		}
		selected = append(selected, uint32(pool[i].nodeID))
		if len(selected) == degree {
			break
		}
		for j := i + 1; j < len(pool); j++ {
			if !s.removed[j] && alpha*alpha*vectorPartitionVamanaDistanceV1(vectors, dimensions, pool[i].nodeID, pool[j].nodeID) <= pool[j].distance {
				s.removed[j] = true
			}
		}
	}
	return selected
}

func vectorPartitionVamanaLocalityOrderV1(ctx context.Context, rows []columnVectorGraphAssetRow, adjacency [][]uint32, entry int) error {
	order := make([]int, 0, len(rows))
	oldToNew := make([]int, len(rows))
	for i := range oldToNew {
		oldToNew[i] = -1
	}
	queue := []int{entry}
	oldToNew[entry] = 0
	for head := 0; head < len(queue); head++ {
		if head&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		row := queue[head]
		order = append(order, row)
		for _, raw := range adjacency[row] {
			neighbor := int(raw)
			if oldToNew[neighbor] == -1 {
				oldToNew[neighbor] = len(queue)
				queue = append(queue, neighbor)
			}
		}
	}
	if len(order) != len(rows) {
		return fmt.Errorf("collections: partition-local Vamana entry reaches %d of %d rows", len(order), len(rows))
	}
	ordered := make([]columnVectorGraphAssetRow, len(rows))
	totalNeighbors := 0
	for _, neighbors := range adjacency {
		totalNeighbors += len(neighbors)
	}
	remapped := make([]uint32, totalNeighbors)
	cursor := 0
	for newOrdinal, oldOrdinal := range order {
		ordered[newOrdinal] = rows[oldOrdinal]
		end := cursor + len(adjacency[oldOrdinal])
		neighbors := remapped[cursor:end:end]
		for i, raw := range adjacency[oldOrdinal] {
			neighbors[i] = uint32(oldToNew[int(raw)])
		}
		ordered[newOrdinal].Adjacency = neighbors
		cursor = end
	}
	copy(rows, ordered)
	return nil
}

func vectorPartitionVamanaDistanceV1(vectors []float32, dimensions, left, right int) float32 {
	l := vectors[left*dimensions : (left+1)*dimensions]
	r := vectors[right*dimensions : (right+1)*dimensions]
	return 2 * vectorops.CosineDistanceFloat32Normalized(l, r, 1, 1)
}

func vectorPartitionVamanaContainsV1(values []uint32, value uint32) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func vectorPartitionVamanaShuffleV1(values []int, seed uint64) {
	state := seed
	for i := len(values) - 1; i > 0; i-- {
		state = vectorPartitionVamanaSplitMix64V1(state)
		j := int(state % uint64(i+1))
		values[i], values[j] = values[j], values[i]
	}
}

func vectorPartitionVamanaSplitMix64V1(state uint64) uint64 {
	state += 0x9e3779b97f4a7c15
	state = (state ^ state>>30) * 0xbf58476d1ce4e5b9
	state = (state ^ state>>27) * 0x94d049bb133111eb
	return state ^ state>>31
}
