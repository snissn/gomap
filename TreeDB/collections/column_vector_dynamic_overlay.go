package collections

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const columnVectorDynamicGraphStrategyCosine = "column_graph_dynamic_overlay_cosine"

// ColumnVectorDynamicMutationKind identifies one logical overlay mutation.
type ColumnVectorDynamicMutationKind uint8

const (
	ColumnVectorDynamicMutationInsert ColumnVectorDynamicMutationKind = iota + 1
	ColumnVectorDynamicMutationUpdate
	ColumnVectorDynamicMutationDelete
)

// ColumnVectorDynamicMutation is one copy-on-write mutation applied to the
// current dynamic overlay generation.
type ColumnVectorDynamicMutation struct {
	Kind       ColumnVectorDynamicMutationKind
	DocumentID []byte
	Vector     []float32
}

// ColumnVectorDynamicPublishStats reports one overlay generation publication.
type ColumnVectorDynamicPublishStats struct {
	BaseGeneration    uint64
	OverlayGeneration uint64
	Inserted          int
	Updated           int
	Deleted           int
	OverlayRows       int
	OverlayLiveRows   int
	OverlayTombstones int
	PublishDuration   time.Duration
}

// ColumnVectorDynamicGraphSearchTrace reports one dynamic overlay search. The
// hot search path intentionally reports counters only; timing of writer publish,
// rebuild, and seal work is measured outside SearchCosine.
type ColumnVectorDynamicGraphSearchTrace struct {
	VectorIndexTrace
	BaseTrace         ColumnVectorGraphSearchTrace
	BaseGeneration    uint64
	OverlayGeneration uint64
	BaseReturned      int
	BaseTombstoned    int
	OverlayRows       int
	OverlayLiveRows   int
	OverlayTombstones int
	OverlayScanned    int
	OverlayCandidates int
	MergeCandidates   int
	EdgesVisited      int
}

// ColumnVectorDynamicGraphSearchScratch is caller-owned reusable dynamic graph
// search workspace. It is not safe for concurrent use. SearchCosine returns
// slices backed by this scratch, and reusing the scratch invalidates previously
// returned result slices. Result document IDs alias immutable base or overlay
// snapshot storage. Parallel searches are valid only when each goroutine owns
// its own scratch.
type ColumnVectorDynamicGraphSearchScratch struct {
	Base    ColumnVectorGraphSearchScratch
	overlay []VectorSearchResult
	results []VectorSearchResult
}

// ColumnVectorDynamicGraphSnapshot is one read-consistent dynamic graph view.
type ColumnVectorDynamicGraphSnapshot struct {
	base              *ColumnVectorGraph
	overlay           *ColumnVectorDynamicOverlaySnapshot
	baseGeneration    uint64
	overlayGeneration uint64
}

// Base returns the immutable base graph for this snapshot.
func (s ColumnVectorDynamicGraphSnapshot) Base() *ColumnVectorGraph {
	return s.base
}

// Overlay returns the immutable exact-scan overlay for this snapshot.
func (s ColumnVectorDynamicGraphSnapshot) Overlay() *ColumnVectorDynamicOverlaySnapshot {
	return s.overlay
}

// BaseGeneration returns the base graph generation observed by this snapshot.
func (s ColumnVectorDynamicGraphSnapshot) BaseGeneration() uint64 {
	return s.baseGeneration
}

// OverlayGeneration returns the overlay generation observed by this snapshot.
func (s ColumnVectorDynamicGraphSnapshot) OverlayGeneration() uint64 {
	return s.overlayGeneration
}

// ColumnVectorDynamicGraph combines one immutable base ColumnVectorGraph with a
// copy-on-write exact-scan overlay. Readers atomically load one snapshot and do
// not observe in-place mutation of either base or overlay state.
type ColumnVectorDynamicGraph struct {
	mu           sync.Mutex
	snapshot     atomic.Pointer[ColumnVectorDynamicGraphSnapshot]
	baseDocIndex map[string]int
}

// NewColumnVectorDynamicGraph returns a dynamic overlay wrapper around an
// immutable base graph. The base graph and its borrowed columns must remain
// immutable for the wrapper lifetime.
func NewColumnVectorDynamicGraph(base *ColumnVectorGraph) (*ColumnVectorDynamicGraph, error) {
	if base == nil {
		return nil, errors.New("collections: nil column vector graph base")
	}
	if base.Rows() == 0 {
		return nil, errors.New("collections: dynamic column vector graph base must have rows")
	}
	baseDocIndex := make(map[string]int, base.Rows())
	for ordinal := 0; ordinal < base.Rows(); ordinal++ {
		documentID := base.documentID(ordinal)
		if len(documentID) == 0 {
			return nil, fmt.Errorf("collections: empty base document ID at ordinal %d", ordinal)
		}
		key := string(documentID)
		if previous, ok := baseDocIndex[key]; ok {
			return nil, fmt.Errorf("collections: duplicate base document ID %q at ordinals %d and %d", documentID, previous, ordinal)
		}
		baseDocIndex[key] = ordinal
	}
	graph := &ColumnVectorDynamicGraph{baseDocIndex: baseDocIndex}
	graph.snapshot.Store(&ColumnVectorDynamicGraphSnapshot{
		base:              base,
		baseGeneration:    1,
		overlay:           newColumnVectorDynamicOverlaySnapshot(base.Dims()),
		overlayGeneration: 0,
	})
	return graph, nil
}

// Snapshot returns a value copy of the current immutable read snapshot. The
// zero value is returned when the graph is nil or not initialized.
func (g *ColumnVectorDynamicGraph) Snapshot() ColumnVectorDynamicGraphSnapshot {
	if g == nil {
		return ColumnVectorDynamicGraphSnapshot{}
	}
	snapshot := g.snapshot.Load()
	if snapshot == nil {
		return ColumnVectorDynamicGraphSnapshot{}
	}
	return *snapshot
}

// ApplyBatch publishes one copy-on-write overlay generation containing all
// requested mutations. Inserts append vectors into overlay state, updates
// tombstone the previous logical document and append the replacement vector, and
// deletes tombstone the current logical document.
func (g *ColumnVectorDynamicGraph) ApplyBatch(mutations []ColumnVectorDynamicMutation) (ColumnVectorDynamicPublishStats, error) {
	start := time.Now()
	var stats ColumnVectorDynamicPublishStats
	if g == nil {
		return stats, errors.New("collections: nil dynamic column vector graph")
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	current := g.snapshot.Load()
	if current == nil || current.base == nil || current.overlay == nil {
		return stats, errors.New("collections: dynamic column vector graph has no snapshot")
	}
	if len(mutations) == 0 {
		return stats, nil
	}

	nextOverlay := current.overlay.clone(current.overlayGeneration + 1)
	inserted, updated, deleted := 0, 0, 0
	for mutationIndex, mutation := range mutations {
		if len(mutation.DocumentID) == 0 {
			return stats, fmt.Errorf("collections: dynamic mutation %d has empty document ID", mutationIndex)
		}
		switch mutation.Kind {
		case ColumnVectorDynamicMutationInsert:
			if err := g.applyInsert(nextOverlay, mutation.DocumentID, mutation.Vector); err != nil {
				return stats, fmt.Errorf("collections: dynamic insert mutation %d: %w", mutationIndex, err)
			}
			inserted++
		case ColumnVectorDynamicMutationUpdate:
			if err := g.applyUpdate(nextOverlay, mutation.DocumentID, mutation.Vector); err != nil {
				return stats, fmt.Errorf("collections: dynamic update mutation %d: %w", mutationIndex, err)
			}
			updated++
		case ColumnVectorDynamicMutationDelete:
			if err := g.applyDelete(nextOverlay, mutation.DocumentID); err != nil {
				return stats, fmt.Errorf("collections: dynamic delete mutation %d: %w", mutationIndex, err)
			}
			deleted++
		default:
			return stats, fmt.Errorf("collections: dynamic mutation %d has unsupported kind %d", mutationIndex, mutation.Kind)
		}
	}
	nextOverlay.sortAndDedupeTombstones()
	nextSnapshot := &ColumnVectorDynamicGraphSnapshot{
		base:              current.base,
		baseGeneration:    current.baseGeneration,
		overlay:           nextOverlay,
		overlayGeneration: nextOverlay.generation,
	}
	g.snapshot.Store(nextSnapshot)
	stats.BaseGeneration = nextSnapshot.baseGeneration
	stats.OverlayGeneration = nextSnapshot.overlayGeneration
	stats.Inserted = inserted
	stats.Updated = updated
	stats.Deleted = deleted
	stats.OverlayRows = nextOverlay.Rows()
	stats.OverlayLiveRows = nextOverlay.LiveRows()
	stats.OverlayTombstones = nextOverlay.Tombstones()
	stats.PublishDuration = time.Since(start)
	return stats, nil
}

// SearchCosine searches the current immutable base snapshot and exact-scan
// overlay snapshot, filters tombstoned base documents, and merges top-k results.
// It does not fetch full documents.
func (g *ColumnVectorDynamicGraph) SearchCosine(query []float32, opts ColumnVectorGraphSearchOptions, scratch *ColumnVectorDynamicGraphSearchScratch) ([]VectorSearchResult, ColumnVectorDynamicGraphSearchTrace, error) {
	trace := ColumnVectorDynamicGraphSearchTrace{
		VectorIndexTrace: VectorIndexTrace{Strategy: columnVectorDynamicGraphStrategyCosine},
	}
	if g == nil {
		return nil, trace, errors.New("collections: nil dynamic column vector graph")
	}
	if scratch == nil {
		return nil, trace, errors.New("collections: nil dynamic column vector graph search scratch")
	}
	if opts.TopK <= 0 {
		return nil, trace, errors.New("collections: dynamic column vector graph TopK must be positive")
	}
	snapshot := g.snapshot.Load()
	if snapshot == nil || snapshot.base == nil || snapshot.overlay == nil {
		return nil, trace, errors.New("collections: dynamic column vector graph has no snapshot")
	}
	base := snapshot.base
	overlay := snapshot.overlay
	trace.BaseGeneration = snapshot.baseGeneration
	trace.OverlayGeneration = snapshot.overlayGeneration
	trace.OverlayRows = overlay.Rows()
	trace.OverlayLiveRows = overlay.LiveRows()
	trace.OverlayTombstones = overlay.Tombstones()
	if len(query) != base.Dims() {
		return nil, trace, fmt.Errorf("collections: dynamic column vector graph query has dimension %d, want %d", len(query), base.Dims())
	}
	queryNormSquared, badDim, badValue := columnVectorGraphNormSquared(query)
	if badDim >= 0 {
		return nil, trace, fmt.Errorf("collections: dynamic column vector graph query dim %d is not finite: %g", badDim, badValue)
	}
	queryInvNorm, err := validateColumnVectorGraphQueryInvNorm(queryNormSquared)
	if err != nil {
		return nil, trace, err
	}

	baseOpts := opts
	baseOpts.TopK = columnVectorDynamicBaseTopK(base.Rows(), opts.TopK, overlay.LiveRows(), overlay.Tombstones())
	if baseOpts.EfSearch < baseOpts.TopK {
		baseOpts.EfSearch = baseOpts.TopK
	}
	baseResults, baseTrace, err := base.SearchCosine(query, baseOpts, &scratch.Base)
	if err != nil {
		return nil, trace, err
	}
	trace.BaseTrace = baseTrace
	trace.EdgesVisited = baseTrace.EdgesVisited

	if cap(scratch.results) < opts.TopK {
		scratch.results = make([]VectorSearchResult, 0, opts.TopK)
	}
	results := scratch.results[:0]
	for _, result := range baseResults {
		if overlay.DocumentTombstoned(result.DocumentID) {
			trace.BaseTombstoned++
			continue
		}
		trace.BaseReturned++
		results = appendBoundedVectorSearchResult(results, result, opts.TopK)
	}

	overlayResults, overlayScanned := overlay.searchCosine(query, queryInvNorm, opts.TopK, scratch)
	trace.OverlayScanned = overlayScanned
	trace.OverlayCandidates = len(overlayResults)
	trace.CandidatesExamined = baseTrace.CandidatesExamined + overlayScanned
	for _, result := range overlayResults {
		results = appendBoundedVectorSearchResult(results, result, opts.TopK)
	}
	trace.MergeCandidates = trace.BaseReturned + trace.OverlayCandidates
	trace.CandidatesAfterTombstone = trace.BaseReturned + trace.OverlayScanned
	trace.CandidatesAfterFilter = trace.MergeCandidates
	trace.RerankCount = trace.MergeCandidates
	trace.ReturnedCount = len(results)
	scratch.results = results
	return results, trace, nil
}

func (g *ColumnVectorDynamicGraph) applyInsert(overlay *ColumnVectorDynamicOverlaySnapshot, documentID []byte, vector []float32) error {
	if overlay.HasLiveDocument(documentID) {
		return fmt.Errorf("document %q already exists in overlay", documentID)
	}
	if _, ok := g.baseDocIndex[string(documentID)]; ok && !overlay.documentTombstonedWriter(documentID) {
		return fmt.Errorf("document %q already exists in base", documentID)
	}
	return overlay.appendLiveDocument(documentID, vector)
}

func (g *ColumnVectorDynamicGraph) applyUpdate(overlay *ColumnVectorDynamicOverlaySnapshot, documentID []byte, vector []float32) error {
	overlayFound := overlay.tombstoneOverlayDocument(documentID)
	_, baseFound := g.baseDocIndex[string(documentID)]
	baseLive := baseFound && !overlay.documentTombstonedWriter(documentID)
	if !overlayFound && !baseLive {
		return fmt.Errorf("document %q does not exist", documentID)
	}
	if baseLive {
		overlay.addTombstone(documentID)
	}
	return overlay.appendLiveDocument(documentID, vector)
}

func (g *ColumnVectorDynamicGraph) applyDelete(overlay *ColumnVectorDynamicOverlaySnapshot, documentID []byte) error {
	overlayFound := overlay.tombstoneOverlayDocument(documentID)
	_, baseFound := g.baseDocIndex[string(documentID)]
	baseLive := baseFound && !overlay.documentTombstonedWriter(documentID)
	if !overlayFound && !baseLive {
		return fmt.Errorf("document %q does not exist", documentID)
	}
	if baseLive {
		overlay.addTombstone(documentID)
	}
	return nil
}

func columnVectorDynamicPublishStats(snapshot *ColumnVectorDynamicGraphSnapshot, inserted int, updated int, deleted int, duration time.Duration) ColumnVectorDynamicPublishStats {
	stats := ColumnVectorDynamicPublishStats{
		Inserted:          inserted,
		Updated:           updated,
		Deleted:           deleted,
		PublishDuration:   duration,
		BaseGeneration:    snapshot.baseGeneration,
		OverlayGeneration: snapshot.overlayGeneration,
	}
	if snapshot.overlay != nil {
		stats.OverlayRows = snapshot.overlay.Rows()
		stats.OverlayLiveRows = snapshot.overlay.LiveRows()
		stats.OverlayTombstones = snapshot.overlay.Tombstones()
	}
	return stats
}

func columnVectorDynamicBaseTopK(baseRows int, topK int, overlayLiveRows int, tombstones int) int {
	if topK <= 0 || baseRows <= 0 {
		return 0
	}
	if topK >= baseRows {
		return baseRows
	}
	baseTopK := topK
	if overlayLiveRows > baseRows-baseTopK {
		return baseRows
	}
	baseTopK += overlayLiveRows
	if tombstones > baseRows-baseTopK {
		return baseRows
	}
	return baseTopK + tombstones
}

// ColumnVectorDynamicOverlaySnapshot is an immutable exact-scan overlay
// generation. It deliberately starts with a flat vector column plus tombstone
// list: seal this into immutable mini-graphs or rebuild a new base graph once
// overlay scan or tombstone filtering costs dominate base graph traversal.
type ColumnVectorDynamicOverlaySnapshot struct {
	generation      uint64
	dims            int
	vectors         []float32
	invNorms        []float32
	idArena         []byte
	idOffsets       []uint32
	live            []bool
	liveRows        int
	liveDocIndex    map[string]int
	tombstoneDocIDs [][]byte
}

func newColumnVectorDynamicOverlaySnapshot(dims int) *ColumnVectorDynamicOverlaySnapshot {
	return &ColumnVectorDynamicOverlaySnapshot{
		dims:         dims,
		idOffsets:    []uint32{0},
		liveDocIndex: make(map[string]int),
	}
}

func (o *ColumnVectorDynamicOverlaySnapshot) clone(generation uint64) *ColumnVectorDynamicOverlaySnapshot {
	next := &ColumnVectorDynamicOverlaySnapshot{
		generation:      generation,
		dims:            o.dims,
		vectors:         append([]float32(nil), o.vectors...),
		invNorms:        append([]float32(nil), o.invNorms...),
		idArena:         append([]byte(nil), o.idArena...),
		idOffsets:       append([]uint32(nil), o.idOffsets...),
		live:            append([]bool(nil), o.live...),
		liveRows:        o.liveRows,
		tombstoneDocIDs: append([][]byte(nil), o.tombstoneDocIDs...),
		liveDocIndex:    make(map[string]int, len(o.liveDocIndex)),
	}
	for key, ordinal := range o.liveDocIndex {
		next.liveDocIndex[key] = ordinal
	}
	return next
}

// Rows returns all overlay vector rows, including tombstoned superseded rows.
func (o *ColumnVectorDynamicOverlaySnapshot) Rows() int {
	if o == nil {
		return 0
	}
	return len(o.live)
}

// LiveRows returns overlay rows visible to readers.
func (o *ColumnVectorDynamicOverlaySnapshot) LiveRows() int {
	if o == nil {
		return 0
	}
	return o.liveRows
}

// Tombstones returns the number of base document IDs hidden by this overlay.
func (o *ColumnVectorDynamicOverlaySnapshot) Tombstones() int {
	if o == nil {
		return 0
	}
	return len(o.tombstoneDocIDs)
}

// HasLiveDocument reports whether documentID currently resolves to a live
// overlay row. It is intended for writer-side validation.
func (o *ColumnVectorDynamicOverlaySnapshot) HasLiveDocument(documentID []byte) bool {
	if o == nil {
		return false
	}
	ordinal, ok := o.liveDocIndex[string(documentID)]
	return ok && ordinal >= 0 && ordinal < len(o.live) && o.live[ordinal]
}

// DocumentTombstoned reports whether a base document ID is hidden by this
// overlay. It is allocation-free and safe for hot search use.
func (o *ColumnVectorDynamicOverlaySnapshot) DocumentTombstoned(documentID []byte) bool {
	if o == nil || len(o.tombstoneDocIDs) == 0 {
		return false
	}
	_, found := slices.BinarySearchFunc(o.tombstoneDocIDs, documentID, bytes.Compare)
	return found
}

func (o *ColumnVectorDynamicOverlaySnapshot) documentTombstonedWriter(documentID []byte) bool {
	if o == nil {
		return false
	}
	for _, tombstone := range o.tombstoneDocIDs {
		if bytes.Equal(tombstone, documentID) {
			return true
		}
	}
	return false
}

func (o *ColumnVectorDynamicOverlaySnapshot) appendLiveDocument(documentID []byte, vector []float32) error {
	if len(vector) != o.dims {
		return fmt.Errorf("vector has dimension %d, want %d", len(vector), o.dims)
	}
	normSquared, badDim, badValue := columnVectorGraphNormSquared(vector)
	if badDim >= 0 {
		return fmt.Errorf("vector dim %d is not finite: %g", badDim, badValue)
	}
	invNorm, err := validateColumnVectorGraphQueryInvNorm(normSquared)
	if err != nil {
		return err
	}
	if uint64(len(o.idArena))+uint64(len(documentID)) > columnVectorGraphMaxUint32 {
		return errors.New("overlay document ID arena exceeds uint32 offsets")
	}
	ordinal := len(o.live)
	o.vectors = append(o.vectors, vector...)
	o.invNorms = append(o.invNorms, invNorm)
	o.idArena = append(o.idArena, documentID...)
	o.idOffsets = append(o.idOffsets, uint32(len(o.idArena)))
	o.live = append(o.live, true)
	o.liveRows++
	o.liveDocIndex[string(documentID)] = ordinal
	return nil
}

func (o *ColumnVectorDynamicOverlaySnapshot) tombstoneOverlayDocument(documentID []byte) bool {
	ordinal, ok := o.liveDocIndex[string(documentID)]
	if !ok || ordinal < 0 || ordinal >= len(o.live) || !o.live[ordinal] {
		return false
	}
	o.live[ordinal] = false
	o.liveRows--
	delete(o.liveDocIndex, string(documentID))
	return true
}

func (o *ColumnVectorDynamicOverlaySnapshot) addTombstone(documentID []byte) {
	o.tombstoneDocIDs = append(o.tombstoneDocIDs, bytes.Clone(documentID))
}

func (o *ColumnVectorDynamicOverlaySnapshot) sortAndDedupeTombstones() {
	if len(o.tombstoneDocIDs) <= 1 {
		return
	}
	slices.SortFunc(o.tombstoneDocIDs, bytes.Compare)
	out := o.tombstoneDocIDs[:1]
	for _, documentID := range o.tombstoneDocIDs[1:] {
		if !bytes.Equal(documentID, out[len(out)-1]) {
			out = append(out, documentID)
		}
	}
	o.tombstoneDocIDs = out
}

func (o *ColumnVectorDynamicOverlaySnapshot) searchCosine(query []float32, queryInvNorm float32, topK int, scratch *ColumnVectorDynamicGraphSearchScratch) ([]VectorSearchResult, int) {
	if topK <= 0 || o == nil || o.liveRows == 0 {
		if scratch != nil {
			scratch.overlay = scratch.overlay[:0]
			return scratch.overlay, 0
		}
		return nil, 0
	}
	if cap(scratch.overlay) < topK {
		scratch.overlay = make([]VectorSearchResult, 0, topK)
	}
	results := scratch.overlay[:0]
	scanned := 0
	for ordinal, live := range o.live {
		if !live {
			continue
		}
		scanned++
		vector := o.vectorAt(ordinal)
		dot := columnVectorGraphDotProductFloat64(query, vector)
		cosine := float32(dot) * queryInvNorm * o.invNorms[ordinal]
		distance := float32(1 - clampColumnVectorGraphCosine(float64(cosine)))
		if !columnVectorGraphFinite(distance) {
			continue
		}
		results = appendBoundedVectorSearchResult(results, VectorSearchResult{
			DocumentID: o.documentID(ordinal),
			Distance:   distance,
		}, topK)
	}
	scratch.overlay = results
	return results, scanned
}

func (o *ColumnVectorDynamicOverlaySnapshot) vectorAt(ordinal int) []float32 {
	start := ordinal * o.dims
	return o.vectors[start : start+o.dims : start+o.dims]
}

func (o *ColumnVectorDynamicOverlaySnapshot) documentID(ordinal int) []byte {
	start := o.idOffsets[ordinal]
	end := o.idOffsets[ordinal+1]
	return o.idArena[start:end:end]
}
