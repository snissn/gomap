package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeDocumentGraphSummary is part of the existing meta JSON record. N is
// one-past-last ordinal (zero for an empty graph), not a reusable ordinal.
// Only the checked v5 producer changes these totals with its touched records.
type nativeDocumentGraphSummary struct {
	Incarnation   uint64 `json:"incarnation"`
	Nodes         uint64 `json:"nodes"`
	Live          uint64 `json:"live"`
	Edges         uint64 `json:"edges"`
	VectorIDBytes uint64 `json:"vector_id_bytes"`
}

type nativeDocumentGraphNode struct {
	value vectorIndexNode
	dirty bool
}

// This is private operation scratch, never a published graph or a cache of a
// mutable graph. get borrows the SAME captured domain; admission belongs to
// its original transaction/read scope. Nodes are decoded only when visited.
// Errors propagate through every accessor rather than becoming a distance0.
type nativeDocumentGraph struct {
	config      *VectorIndex
	meta        vectorIndexPersistMeta
	get         func([]byte) ([]byte, error)
	admit       func(uint64) error
	nodes       map[int]*nativeDocumentGraphNode
	docs        map[string]int
	changedDocs map[string]bool
	scanNode    func(int) (vectorIndexPersistNode, error)
}

func nativeDocumentGraphCharge(admit func(uint64) error, n uint64) error {
	if admit == nil {
		return memtable.ErrCOWCapacity
	}
	return admit(memtable.COWAllocationCharge(n))
}

func newNativeDocumentGraph(def VectorIndexDefinition, incarnation, generation uint64, get func([]byte) ([]byte, error), admit func(uint64) error) (*nativeDocumentGraph, error) {
	// Configuration/map headers and the existing normalized-index constructor's
	// fixed backing are admitted before their allocation. Record-dependent
	// backing is admitted separately before each actual decode/growth.
	if err := nativeDocumentGraphCharge(admit, uint64(unsafe.Sizeof(VectorIndex{}))+uint64(unsafe.Sizeof(nativeDocumentGraph{}))+1024+uint64(len(def.Name)+len(def.Field))*16); err != nil {
		return nil, err
	}
	cfg, err := newVectorIndex(nil, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return nil, err
	}
	g := &nativeDocumentGraph{config: cfg, get: get, admit: admit, nodes: make(map[int]*nativeDocumentGraphNode), docs: make(map[string]int), changedDocs: make(map[string]bool)}
	raw, err := get([]byte(vectorIndexNativeKeyMeta))
	if errors.Is(err, tree.ErrKeyNotFound) {
		if generation != 0 {
			return nil, fmt.Errorf("collections: missing native document graph at generation %d", generation)
		}
		g.meta = cfg.persistMetaLocked()
		g.meta.SourceDocumentGenerationVersion = nativeDocumentVectorCoverageVersion
		g.meta.NativeDocument = &nativeDocumentGraphSummary{Incarnation: incarnation}
		return g, nil
	}
	if err != nil {
		return nil, err
	}
	if err = g.decode(raw, &g.meta); err != nil {
		return nil, err
	}
	m := &g.meta
	s := m.NativeDocument
	if s == nil || s.Incarnation != incarnation || m.SourceDocumentGenerationVersion != nativeDocumentVectorCoverageVersion || m.SourceDocumentGeneration != generation || m.Name != cfg.name || m.Field != cfg.field || m.Metric != cfg.metric || m.Encoding != cfg.encoding || m.Dimensions != cfg.dimensions || m.M != cfg.m || m.EfConstruction != cfg.efConstruction || m.EfSearch != cfg.efSearch || m.PartitionLive != nil || m.Preparation != nil || s.Live > s.Nodes || s.Nodes > uint64(int(^uint(0)>>1)) || s.Nodes > uint64(math.MaxUint32)+1 {
		return nil, fmt.Errorf("collections: invalid native document graph authority")
	}
	if (s.Live == 0 && (m.Entry != -1 || m.MaxLevel != -1)) || (s.Live != 0 && (m.Entry < 0 || uint64(m.Entry) >= s.Nodes || m.MaxLevel < 0 || m.MaxLevel > 32)) {
		return nil, fmt.Errorf("collections: invalid native document graph entry")
	}
	return g, nil
}

func (g *nativeDocumentGraph) decode(raw []byte, dst any) error {
	// The maintained JSON codec's source image bounds every string, numeric
	// element and decoder container; this quota is charged before unmarshalling.
	n, err := nativeTypedCheckedMul(uint64(len(raw)), 32)
	if err != nil {
		return err
	}
	if err = nativeTypedCheckedAdd(&n, 1024); err != nil {
		return err
	}
	if err = nativeDocumentGraphCharge(g.admit, n); err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

func (g *nativeDocumentGraph) readNode(id int) (*nativeDocumentGraphNode, error) {
	if n, ok := g.nodes[id]; ok {
		return n, nil
	}
	if id < 0 || uint64(id) >= g.meta.NativeDocument.Nodes {
		return nil, fmt.Errorf("collections: graph node ordinal %d outside native graph", id)
	}
	raw, err := g.get(vectorIndexNativeNodeKey(id))
	if err != nil {
		return nil, err
	}
	disk, err := decodeNativeDocumentGraphNode(raw, g.decode)
	if err != nil {
		return nil, err
	}
	if disk.DocumentID == "" || disk.Level < 0 || disk.Level > 32 || len(disk.Vector) != g.config.dimensions || len(disk.Quantized) != 0 || disk.QuantScale != 0 {
		return nil, fmt.Errorf("collections: invalid native graph node %d", id)
	}
	if err = validateFloat32Vector(disk.Vector); err != nil {
		return nil, err
	}
	if vectorNormSquared(disk.Vector) == 0 {
		return nil, fmt.Errorf("collections: zero cosine native graph node %d", id)
	}
	if err = nativeDocumentGraphCharge(g.admit, uint64(unsafe.Sizeof(nativeDocumentGraphNode{}))+uint64(disk.Level+1)*uint64(unsafe.Sizeof([]vectorIndexNeighbor{}))+256); err != nil {
		return nil, err
	}
	n := &nativeDocumentGraphNode{value: vectorIndexNode{documentID: []byte(disk.DocumentID), vector: disk.Vector, level: disk.Level, deleted: disk.Deleted, neighbors: make([][]vectorIndexNeighbor, disk.Level+1)}}
	n.value.cacheVectorNorms()
	for layer := 0; layer <= disk.Level; layer++ {
		raw, err = g.get(vectorIndexNativeEdgeKey(id, layer))
		if err != nil {
			return nil, err
		}
		var edge vectorIndexPersistEdges
		if err = g.decode(raw, &edge); err != nil {
			return nil, err
		}
		if edge.NodeID != id || edge.Layer != layer || len(edge.Neighbor) != len(edge.Distances) || len(edge.Neighbor) > g.config.maxNeighborsForLayer(layer) {
			return nil, fmt.Errorf("collections: invalid native graph adjacency %d/%d", id, layer)
		}
		if err = nativeDocumentGraphCharge(g.admit, uint64(len(edge.Neighbor))*uint64(unsafe.Sizeof(vectorIndexNeighbor{}))); err != nil {
			return nil, err
		}
		ns := make([]vectorIndexNeighbor, len(edge.Neighbor))
		for j, to := range edge.Neighbor {
			distance, ok := normalizeVectorIndexEdgeDistance(edge.Distances[j])
			if !ok || to < 0 || uint64(to) >= g.meta.NativeDocument.Nodes || to == id {
				return nil, fmt.Errorf("collections: invalid native graph edge %d/%d", id, layer)
			}
			for k := 0; k < j; k++ {
				if edge.Neighbor[k] == to {
					return nil, fmt.Errorf("collections: duplicate native graph edge")
				}
			}
			ns[j] = vectorIndexNeighbor{nodeID: uint32(to), distance: distance}
		}
		n.value.neighbors[layer] = ns
	}
	tomb, terr := g.get(vectorIndexNativeTombstoneKey(id))
	if disk.Deleted {
		var actual int
		if terr != nil {
			return nil, terr
		}
		if err = g.decode(tomb, &actual); err != nil {
			return nil, err
		}
		if actual != id {
			return nil, fmt.Errorf("collections: invalid native graph tombstone")
		}
	} else if terr == nil {
		return nil, fmt.Errorf("collections: unexpected native graph tombstone")
	} else if !errors.Is(terr, tree.ErrKeyNotFound) {
		return nil, terr
	}
	g.nodes[id] = n
	return n, nil
}

func (g *nativeDocumentGraph) current(id []byte) (int, error) {
	key := string(id)
	if n, ok := g.docs[key]; ok {
		return n, nil
	}
	raw, err := g.get(vectorIndexNativeDocKey(key))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return -1, nil
	}
	if err != nil {
		return -1, err
	}
	var n int
	if err = g.decode(raw, &n); err != nil {
		return -1, err
	}
	v, err := g.readNode(n)
	if err != nil {
		return -1, err
	}
	if v.value.deleted || !bytes.Equal(v.value.documentID, id) {
		return -1, fmt.Errorf("collections: invalid native graph document mapping")
	}
	if err = nativeDocumentGraphCharge(g.admit, uint64(len(id))+256); err != nil {
		return -1, err
	}
	g.docs[key] = n
	return n, nil
}

func (g *nativeDocumentGraph) distance(query []float32, id int) (float32, error) {
	n, err := g.readNode(id)
	if err != nil {
		return 0, err
	}
	return vectorDistanceToStoredNode(query, &n.value, g.config.metric)
}

// SEARCH-LAYER retains only visited ordinals and its actual bounded frontier.
// No allocation is indexed by graph high-water. Original candidate ordering
// and cosine evaluator are shared with the maintained HNSW implementation.
func (g *nativeDocumentGraph) searchLayer(query []float32, entry, ef, layer int) ([]vectorIndexCandidate, error) {
	return g.searchLayerMode(query, entry, ef, ef, layer, false)
}

// currentSearchExplorationLimit keeps the requested live-result limit separate
// from one bounded layer-0 fanout of stale routing waypoints per live candidate.
// The summary was checked against the actual graph by its original certificate.
func (g *nativeDocumentGraph) currentSearchExplorationLimit(limit int) int {
	explorationLimit := limit
	nodes := int(g.meta.NativeDocument.Nodes)
	stale := int(g.meta.NativeDocument.Nodes - g.meta.NativeDocument.Live)
	if stale > 0 && explorationLimit < nodes {
		degree := maxInt(1, g.config.maxNeighborsForLayer(0))
		maxExtra := math.MaxInt
		if degree > 1 && limit <= math.MaxInt/(degree-1) {
			maxExtra = limit * (degree - 1)
		}
		explorationLimit += minInt(stale, minInt(maxExtra, nodes-explorationLimit))
	}
	return explorationLimit
}

// currentSearchCandidates uses the maintained current-search budget across
// upper and layer-0 traversal. Construction keeps its original greedy route.
func (g *nativeDocumentGraph) currentSearchCandidates(query []float32, ef int) ([]vectorIndexCandidate, error) {
	entry := g.meta.Entry
	if g.meta.NativeDocument.Nodes == g.meta.NativeDocument.Live {
		for layer := g.meta.MaxLevel; layer > 0; layer-- {
			var err error
			entry, err = g.greedy(query, entry, layer)
			if err != nil {
				return nil, err
			}
		}
		return g.searchLayerMode(query, entry, ef, ef, 0, true)
	}
	explorationLimit := g.currentSearchExplorationLimit(ef)
	upperLimit := maxInt(0, explorationLimit-ef-1)
	upperExplored := 0
	for layer := g.meta.MaxLevel; layer > 0 && upperExplored < upperLimit; layer-- {
		var err error
		entry, err = g.greedyCurrentBounded(query, entry, layer, upperLimit, &upperExplored)
		if err != nil {
			return nil, err
		}
	}
	remaining := explorationLimit - upperExplored
	var seeds []vectorIndexCandidate
	if remaining >= 4 && entry != g.meta.Entry {
		original, err := g.readNode(g.meta.Entry)
		if err != nil {
			return nil, err
		}
		selected, err := g.readNode(entry)
		if err != nil {
			return nil, err
		}
		if !original.value.deleted && selected.value.deleted {
			// Retain the original live entry's bounded one-hop route when a
			// closer tombstone wins the upper descent. Reserve an expansion slot.
			neighbors := original.value.neighbors[0]
			count := minInt(len(neighbors), remaining-3)
			if err := nativeDocumentGraphCharge(g.admit, uint64(count+2)*uint64(unsafe.Sizeof(vectorIndexCandidate{}))); err != nil {
				return nil, err
			}
			seeds = make([]vectorIndexCandidate, 0, count+2)
			add := func(id int) error {
				for _, seed := range seeds {
					if seed.nodeID == id {
						return nil
					}
				}
				d, err := g.distance(query, id)
				if err != nil {
					return err
				}
				seeds = append(seeds, vectorIndexCandidate{nodeID: id, distance: d})
				return nil
			}
			if err := add(entry); err != nil {
				return nil, err
			}
			if err := add(g.meta.Entry); err != nil {
				return nil, err
			}
			for _, neighbor := range neighbors[:count] {
				if err := add(int(neighbor.nodeID)); err != nil {
					return nil, err
				}
			}
		}
	}
	return g.searchLayerSeedsMode(query, entry, seeds, ef, remaining, 0, true)
}

func (g *nativeDocumentGraph) greedyCurrentBounded(query []float32, entry, layer, limit int, scored *int) (int, error) {
	score := func(id int) (float32, error) {
		if err := nativeDocumentGraphCharge(g.admit, 128); err != nil {
			return 0, err
		}
		d, err := g.distance(query, id)
		if err != nil {
			return 0, err
		}
		(*scored)++
		return d, nil
	}
	bestDistance, err := score(entry)
	if err != nil {
		return 0, err
	}
	for changed := true; changed; {
		changed = false
		n, err := g.readNode(entry)
		if err != nil {
			return 0, err
		}
		if n.value.level < layer {
			return 0, fmt.Errorf("collections: native graph edge reaches absent layer")
		}
		for _, edge := range n.value.neighbors[layer] {
			if *scored >= limit {
				return entry, nil
			}
			id := int(edge.nodeID)
			distance, err := score(id)
			if err != nil {
				return 0, err
			}
			if g.nodes[id].value.level < layer {
				return 0, fmt.Errorf("collections: native graph neighbor level mismatch")
			}
			if distance < bestDistance {
				entry, bestDistance, changed = id, distance, true
			}
		}
	}
	return entry, nil
}

// Public current queries retain live results separately from routing candidates.
// Tombstones still participate in navigation and construction.
func (g *nativeDocumentGraph) searchLayerMode(query []float32, entry, ef, explorationLimit, layer int, currentOnly bool) ([]vectorIndexCandidate, error) {
	return g.searchLayerSeedsMode(query, entry, nil, ef, explorationLimit, layer, currentOnly)
}

func (g *nativeDocumentGraph) searchLayerSeedsMode(query []float32, entry int, seeds []vectorIndexCandidate, ef, explorationLimit, layer int, currentOnly bool) ([]vectorIndexCandidate, error) {
	if ef < 1 {
		ef = 1
	}
	if explorationLimit < ef {
		explorationLimit = ef
	}
	if err := nativeDocumentGraphCharge(g.admit, 256); err != nil {
		return nil, err
	}
	visited := make(map[int]struct{})
	var queue vectorIndexMinCandidateHeap
	var best vectorIndexMaxCandidateHeap
	var liveBest vectorIndexMaxCandidateHeap
	push := func(id int, distance float32) error {
		candidateBytes := 2 * uint64(unsafe.Sizeof(vectorIndexCandidate{}))
		if currentOnly {
			candidateBytes += uint64(unsafe.Sizeof(vectorIndexCandidate{}))
		}
		if err := nativeDocumentGraphCharge(g.admit, 256+candidateBytes); err != nil {
			return err
		}
		visited[id] = struct{}{}
		c := vectorIndexCandidate{nodeID: id, distance: distance}
		queue.push(c)
		best.pushBounded(c, explorationLimit)
		if currentOnly {
			n, err := g.readNode(id)
			if err != nil {
				return err
			}
			if !n.value.deleted {
				liveBest.pushBounded(c, ef)
			}
		}
		return nil
	}
	explored := 0
	if len(seeds) == 0 {
		distance, err := g.distance(query, entry)
		if err != nil {
			return nil, err
		}
		if err := push(entry, distance); err != nil {
			return nil, err
		}
		explored = 1
	} else {
		for _, seed := range seeds {
			if err := push(seed.nodeID, seed.distance); err != nil {
				return nil, err
			}
			explored++
		}
	}
	boundedStale := currentOnly && g.meta.NativeDocument.Nodes > g.meta.NativeDocument.Live
search:
	for len(queue) != 0 {
		c := queue.pop()
		if len(best) >= explorationLimit && vectorIndexCandidateLess(best[0], c) {
			break
		}
		n, err := g.readNode(c.nodeID)
		if err != nil {
			return nil, err
		}
		if layer > n.value.level {
			return nil, fmt.Errorf("collections: native graph edge reaches absent layer")
		}
		for _, edge := range n.value.neighbors[layer] {
			id := int(edge.nodeID)
			if _, ok := visited[id]; ok {
				continue
			}
			if boundedStale && explored >= explorationLimit {
				break search
			}
			// Mark even rejected candidates once; no repeated exponential traversal.
			if err := nativeDocumentGraphCharge(g.admit, 128); err != nil {
				return nil, err
			}
			visited[id] = struct{}{}
			explored++
			target, err := g.readNode(id)
			if err != nil {
				return nil, err
			}
			if target.value.level < layer {
				return nil, fmt.Errorf("collections: native graph neighbor level mismatch")
			}
			d, err := g.distance(query, id)
			if err != nil {
				return nil, err
			}
			if len(best) < explorationLimit || vectorIndexCandidateLess(vectorIndexCandidate{nodeID: id, distance: d}, best[0]) {
				if err = push(id, d); err != nil {
					return nil, err
				}
			}
		}
	}
	if currentOnly {
		best = liveBest
	}
	if err := nativeDocumentGraphCharge(g.admit, uint64(len(best))*uint64(unsafe.Sizeof(vectorIndexCandidate{}))); err != nil {
		return nil, err
	}
	out := append([]vectorIndexCandidate(nil), best...)
	sort.Slice(out, func(i, j int) bool { return vectorIndexCandidateLess(out[i], out[j]) })
	return out, nil
}

func (g *nativeDocumentGraph) greedy(query []float32, entry, layer int) (int, error) {
	for {
		candidates, err := g.searchLayer(query, entry, 1, layer)
		if err != nil {
			return 0, err
		}
		if len(candidates) == 0 || candidates[0].nodeID == entry {
			return entry, nil
		}
		entry = candidates[0].nodeID
	}
}

// selectNeighbors uses the maintained relative-neighborhood heuristic, with
// original candidate tie-breaking. All neighbor reads remain error-returning.
func (g *nativeDocumentGraph) selectNeighbors(candidates []vectorIndexCandidate, limit, exclude int) ([]vectorIndexNeighbor, error) {
	if err := nativeDocumentGraphCharge(g.admit, uint64(limit)*uint64(unsafe.Sizeof(vectorIndexNeighbor{}))); err != nil {
		return nil, err
	}
	out := make([]vectorIndexNeighbor, 0, limit)
	for _, c := range candidates {
		if c.nodeID == exclude {
			continue
		}
		n, err := g.readNode(c.nodeID)
		if err != nil {
			return nil, err
		}
		if n.value.deleted {
			continue
		}
		keep := true
		for _, selected := range out {
			s, err := g.readNode(int(selected.nodeID))
			if err != nil {
				return nil, err
			}
			d, err := vectorDistanceBetweenStoredNodes(&n.value, &s.value, g.config.metric)
			if err != nil {
				return nil, err
			}
			if d < c.distance {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, vectorIndexNeighbor{nodeID: uint32(c.nodeID), distance: c.distance})
			if len(out) == limit {
				break
			}
		}
	}
	// The maintained construction path fills unused degree from its original
	// candidate pool. Prefer live backfill, then retain deleted routing waypoints
	// rather than severing them during either insertion or reciprocal pruning.
	for _, deleted := range []bool{false, true} {
		for _, c := range candidates {
			if len(out) == limit {
				return out, nil
			}
			if c.nodeID == exclude {
				continue
			}
			n, err := g.readNode(c.nodeID)
			if err != nil {
				return nil, err
			}
			if n.value.deleted != deleted {
				continue
			}
			found := false
			for _, selected := range out {
				if int(selected.nodeID) == c.nodeID {
					found = true
					break
				}
			}
			if !found {
				out = append(out, vectorIndexNeighbor{nodeID: uint32(c.nodeID), distance: c.distance})
			}
		}
	}
	return out, nil
}

func (g *nativeDocumentGraph) replaceNeighbors(id, layer int, next []vectorIndexNeighbor) error {
	n, err := g.readNode(id)
	if err != nil {
		return err
	}
	if layer > n.value.level {
		return fmt.Errorf("collections: native graph link outside node layers")
	}
	old := uint64(len(n.value.neighbors[layer]))
	count := uint64(len(next))
	s := g.meta.NativeDocument
	if old > s.Edges || count > math.MaxUint64-(s.Edges-old) {
		return fmt.Errorf("collections: native graph edge count overflow")
	}
	s.Edges = s.Edges - old + count
	n.value.neighbors[layer] = next
	n.dirty = true
	return nil
}

func (g *nativeDocumentGraph) tombstone(id []byte) error {
	old, err := g.current(id)
	if err != nil {
		return err
	}
	if old < 0 {
		return nil
	}
	n, err := g.readNode(old)
	if err != nil {
		return err
	}
	if g.meta.NativeDocument.Live == 0 {
		return fmt.Errorf("collections: native graph live count underflow")
	}
	n.value.deleted = true
	n.dirty = true
	g.docs[string(id)] = -1
	g.changedDocs[string(id)] = true
	g.meta.NativeDocument.Live--
	if g.meta.Entry != old {
		return nil
	}
	// Entry deletion is exceptional streaming work. Do not retain a high-water
	// node census: election needs only the best ordinal and one decoded record.
	best, level := -1, -1
	for i := uint64(0); i < g.meta.NativeDocument.Nodes; i++ {
		if v, ok := g.nodes[int(i)]; ok {
			if !v.value.deleted && (v.value.level > level || (v.value.level == level && (best < 0 || int(i) < best))) {
				best, level = int(i), v.value.level
			}
			continue
		}
		var v vectorIndexPersistNode
		var err error
		if g.scanNode != nil {
			v, err = g.scanNode(int(i))
		} else {
			var raw []byte
			raw, err = g.get(vectorIndexNativeNodeKey(int(i)))
			if err == nil {
				err = g.decode(raw, &v)
			}
		}
		if err != nil {
			return err
		}
		if v.Level < 0 || v.Level > 32 || len(v.Vector) != g.config.dimensions {
			return fmt.Errorf("collections: invalid node in native graph entry election")
		}
		if !v.Deleted && (v.Level > level || (v.Level == level && (best < 0 || int(i) < best))) {
			best, level = int(i), v.Level
		}
	}
	g.meta.Entry, g.meta.MaxLevel = best, level
	return nil
}

func (g *nativeDocumentGraph) upsert(id []byte, vector []float32) error {
	if len(vector) != g.config.dimensions {
		return fmt.Errorf("collections: native graph vector dimension mismatch")
	}
	if err := validateFloat32Vector(vector); err != nil {
		return err
	}
	if vectorNormSquared(vector) == 0 {
		return fmt.Errorf("collections: zero cosine native graph vector")
	}
	old, err := g.current(id)
	if err != nil {
		return err
	}
	if old >= 0 {
		n, err := g.readNode(old)
		if err != nil {
			return err
		}
		same := len(n.value.vector) == len(vector)
		for i := 0; same && i < len(vector); i++ {
			same = n.value.vector[i] == vector[i]
		}
		if same {
			return nil
		}
	}
	if !vectorIndexNodeOrdinalsFitUint32(g.meta.NativeDocument.Nodes, 1) || g.meta.NativeDocument.Nodes >= uint64(int(^uint(0)>>1)) {
		return fmt.Errorf("collections: native graph ordinal overflow")
	}
	if err = g.tombstone(id); err != nil {
		return err
	}
	level := g.config.levelForDocumentID(id)
	nID := int(g.meta.NativeDocument.Nodes)
	charge := uint64(len(id)) + uint64(len(vector))*4 + uint64(unsafe.Sizeof(nativeDocumentGraphNode{})) + uint64(level+1)*uint64(unsafe.Sizeof([]vectorIndexNeighbor{})) + 768
	if err = nativeDocumentGraphCharge(g.admit, charge); err != nil {
		return err
	}
	n := &nativeDocumentGraphNode{value: vectorIndexNode{documentID: bytes.Clone(id), vector: append([]float32(nil), vector...), level: level, neighbors: make([][]vectorIndexNeighbor, level+1)}, dirty: true}
	n.value.cacheVectorNorms()
	g.nodes[nID] = n
	g.meta.NativeDocument.Nodes++
	g.meta.NativeDocument.Live++
	if uint64(len(id))+uint64(len(vector))*4 > math.MaxUint64-g.meta.NativeDocument.VectorIDBytes {
		return fmt.Errorf("collections: native graph vector byte overflow")
	}
	g.meta.NativeDocument.VectorIDBytes += uint64(len(id)) + uint64(len(vector))*4
	g.docs[string(id)] = nID
	g.changedDocs[string(id)] = true
	if g.meta.Entry < 0 {
		g.meta.Entry, g.meta.MaxLevel = nID, level
		return nil
	}
	entry := g.meta.Entry
	for layer := g.meta.MaxLevel; layer > level; layer-- {
		entry, err = g.greedy(vector, entry, layer)
		if err != nil {
			return err
		}
	}
	for layer := min(level, g.meta.MaxLevel); layer >= 0; layer-- {
		candidates, err := g.searchLayer(vector, entry, g.config.efConstruction, layer)
		if err != nil {
			return err
		}
		selected, err := g.selectNeighbors(candidates, g.config.maxNeighborsForLayer(layer), nID)
		if err != nil {
			return err
		}
		if err = g.replaceNeighbors(nID, layer, selected); err != nil {
			return err
		}
		for _, neighbor := range selected {
			from, err := g.readNode(int(neighbor.nodeID))
			if err != nil {
				return err
			}
			if err = nativeDocumentGraphCharge(g.admit, uint64(len(from.value.neighbors[layer])+1)*uint64(unsafe.Sizeof(vectorIndexCandidate{}))); err != nil {
				return err
			}
			cs := make([]vectorIndexCandidate, 0, len(from.value.neighbors[layer])+1)
			for _, e := range from.value.neighbors[layer] {
				cs = append(cs, vectorIndexCandidate{nodeID: int(e.nodeID), distance: e.distance})
			}
			cs = append(cs, vectorIndexCandidate{nodeID: nID, distance: neighbor.distance})
			sort.Slice(cs, func(i, j int) bool { return vectorIndexCandidateLess(cs[i], cs[j]) })
			ns, err := g.selectNeighbors(cs, g.config.maxNeighborsForLayer(layer), int(neighbor.nodeID))
			if err != nil {
				return err
			}
			if err = g.replaceNeighbors(int(neighbor.nodeID), layer, ns); err != nil {
				return err
			}
		}
		if len(selected) != 0 {
			entry = int(selected[0].nodeID)
		}
	}
	if level > g.meta.MaxLevel {
		g.meta.Entry, g.meta.MaxLevel = nID, level
	}
	return nil
}

func (g *nativeDocumentGraph) mutations(nextGeneration uint64) ([]memtable.COWMutation, error) {
	g.meta.SourceDocumentGeneration = nextGeneration
	count := 1 + len(g.changedDocs)
	for _, n := range g.nodes {
		if n.dirty {
			count += 1 + len(n.value.neighbors)
			if n.value.deleted {
				count++
			}
		}
	}
	if err := nativeDocumentGraphCharge(g.admit, uint64(count)*uint64(unsafe.Sizeof(memtable.COWMutation{}))); err != nil {
		return nil, err
	}
	out := make([]memtable.COWMutation, 0, count)
	add := func(key []byte, value any) error {
		// JSON float32 scalars fit32 printable bytes; node ID/definition strings
		// may expand sixfold. Charge the exact source-dependent upper bound first.
		bound := uint64(1024+len(key)) * 8
		switch v := value.(type) {
		case nativeDocumentPersistNode:
			bound += uint64(len(v.DocumentIDBytes))*6 + uint64(len(v.Vector))*32
		case vectorIndexPersistEdges:
			bound += uint64(len(v.Neighbor)) * 64
		case vectorIndexPersistMeta:
			bound += uint64(len(v.Name)+len(v.Field)) * 6
		}
		if err := nativeDocumentGraphCharge(g.admit, bound); err != nil {
			return err
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		out = append(out, memtable.COWMutation{Key: key, Value: append(raw, '\n'), Flags: node.FlagInline})
		return nil
	}
	if err := add([]byte(vectorIndexNativeKeyMeta), g.meta); err != nil {
		return nil, err
	}
	for id, n := range g.nodes {
		if !n.dirty {
			continue
		}
		v := n.value
		if err := add(vectorIndexNativeNodeKey(id), nativeDocumentPersistNode{vectorIndexPersistNode: vectorIndexPersistNode{Vector: v.vector, Level: v.level, Deleted: v.deleted}, DocumentIDBytes: v.documentID}); err != nil {
			return nil, err
		}
		for layer, neighbors := range v.neighbors {
			if err := nativeDocumentGraphCharge(g.admit, uint64(len(neighbors))*(uint64(unsafe.Sizeof(int(0)))+4)); err != nil {
				return nil, err
			}
			e := vectorIndexPersistEdges{NodeID: id, Layer: layer, Neighbor: make([]int, len(neighbors)), Distances: make([]float32, len(neighbors))}
			for i, n := range neighbors {
				e.Neighbor[i], e.Distances[i] = int(n.nodeID), n.distance
			}
			if err := add(vectorIndexNativeEdgeKey(id, layer), e); err != nil {
				return nil, err
			}
		}
		if v.deleted {
			if err := add(vectorIndexNativeTombstoneKey(id), id); err != nil {
				return nil, err
			}
		}
	}
	for id := range g.changedDocs {
		n := g.docs[id]
		key := vectorIndexNativeDocKey(id)
		if n < 0 {
			out = append(out, memtable.COWMutation{Key: key, Flags: node.FlagTombstone})
		} else if err := add(key, n); err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	return out, nil
}
