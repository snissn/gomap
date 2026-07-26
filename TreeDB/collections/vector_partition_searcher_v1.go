package collections

// M3's partition-local asset is deliberately a no-document search surface. It
// can be carried by a serving group without claiming canonical-row ownership.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"unsafe"
)

var ErrVectorPartitionSearchUnavailable = errors.New("collections: vector partition search unavailable")

// VectorPartitionCanonicalScoreContractV1 names the score bits published by
// vector-partition search. Both operands are normalized in FP32, their dot
// product is accumulated left-to-right in binary64, and the result is rounded
// once to FP32.
const VectorPartitionCanonicalScoreContractV1 = "fp32_normalized_cosine_binary64_accum_score_desc_stable_id_asc_best_duplicate_v1"

// CanonicalVectorPartitionCosineScoreV1 executes the public V1 score contract
// over caller-owned source vectors.
func CanonicalVectorPartitionCosineScoreV1(query, vector []float32) (float32, error) {
	if len(query) == 0 || len(query) != len(vector) {
		return 0, fmt.Errorf("%w: canonical score dimensions", ErrVectorPartitionSearchUnavailable)
	}
	scorer, err := NewCanonicalVectorPartitionCosineScorerV1(query)
	if err != nil {
		return 0, err
	}
	return scorer.ScoreV1(vector)
}

// CanonicalVectorPartitionCosineScorerV1 retains one normalized query so a
// full-source exact oracle can score many caller-owned vectors without
// allocating a normalized query and vector for every source row.
type CanonicalVectorPartitionCosineScorerV1 struct {
	normalizedQuery []float32
}

// NewCanonicalVectorPartitionCosineScorerV1 prepares one query for repeated
// execution of VectorPartitionCanonicalScoreContractV1.
func NewCanonicalVectorPartitionCosineScorerV1(query []float32) (*CanonicalVectorPartitionCosineScorerV1, error) {
	normalizedQuery, err := canonicalVectorPartitionNormalizeV1(query)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical query norm: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	return &CanonicalVectorPartitionCosineScorerV1{normalizedQuery: normalizedQuery}, nil
}

// ScoreV1 scores one source vector without allocating. Multiplication by the
// FP32 inverse norm is rounded to FP32 before the binary64 dot accumulation,
// preserving the public score bits produced by the one-shot helper.
func (s *CanonicalVectorPartitionCosineScorerV1) ScoreV1(vector []float32) (float32, error) {
	if s == nil || len(s.normalizedQuery) == 0 || len(s.normalizedQuery) != len(vector) {
		return 0, fmt.Errorf("%w: canonical score dimensions", ErrVectorPartitionSearchUnavailable)
	}
	invNorm, err := columnVectorGraphInvNorm(vector)
	if err != nil {
		return 0, fmt.Errorf("%w: canonical vector norm: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	var dot float64
	for i := range vector {
		normalized := vector[i] * invNorm
		dot += float64(s.normalizedQuery[i]) * float64(normalized)
	}
	if math.IsNaN(dot) || math.IsInf(dot, 0) {
		return 0, fmt.Errorf("%w: canonical score nonfinite", ErrVectorPartitionSearchUnavailable)
	}
	return float32(dot), nil
}

func canonicalVectorPartitionNormalizeV1(vector []float32) ([]float32, error) {
	invNorm, err := columnVectorGraphInvNorm(vector)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical score norm: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	normalized := make([]float32, len(vector))
	for i := range vector {
		normalized[i] = vector[i] * invNorm
	}
	return normalized, nil
}

func canonicalVectorPartitionNormalizedScoreV1(left, right []float32) (float32, error) {
	if len(left) == 0 || len(left) != len(right) {
		return 0, fmt.Errorf("%w: canonical normalized score dimensions", ErrVectorPartitionSearchUnavailable)
	}
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	if math.IsNaN(dot) || math.IsInf(dot, 0) {
		return 0, fmt.Errorf("%w: canonical score nonfinite", ErrVectorPartitionSearchUnavailable)
	}
	return float32(dot), nil
}

type VectorPartitionMembershipKindV1 string

const (
	VectorPartitionMembershipHomeV1    VectorPartitionMembershipKindV1 = "home"
	VectorPartitionMembershipOverlapV1 VectorPartitionMembershipKindV1 = "overlap"
)

// VectorPartitionSearchAssetV1 is an immutable, generation-bound local HNSW
// input. Vectors are retained as FP32-equivalent float32 values for exact
// authoritative scoring; Documents are intentionally absent.
type VectorPartitionSearchAssetV1 struct {
	// Source binds this input to the non-circular M1 source identity. It is
	// required by the persistent native-pack path.
	Source           VectorPartitionSourceIdentityV1
	ManifestChecksum string
	Generation       uint64
	PartitionID      uint32
	Dimensions       int
	IDs              []string
	Vectors          [][]float32
	Kinds            []VectorPartitionMembershipKindV1
	// Adjacency is optional HNSW traversal state. Search always validates it,
	// but exact rerank never trusts remote/canonical document state.
	Adjacency [][]uint32
}

type VectorPartitionSearchResultV1 struct {
	ID    string
	Score float32
}

// vectorPartitionSearchResultMaxHeapV1 retains the worst selected result at
// index zero so an exact scan can keep only top_k results. Draining the heap
// from worst to best into a reverse output cursor yields the canonical
// score-descending, stable-ID-ascending result order without one uninterruptible
// full-partition sort.
type vectorPartitionSearchResultMaxHeapV1 []VectorPartitionSearchResultV1

type vectorPartitionSearchRefResultV1 struct {
	ID    []byte
	Score float32
}

type vectorPartitionSearchRefResultMaxHeapV1 []vectorPartitionSearchRefResultV1

func vectorPartitionSearchRefResultBetterV1(left, right vectorPartitionSearchRefResultV1) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	return bytes.Compare(left.ID, right.ID) < 0
}

func vectorPartitionSearchRefResultWorseV1(left, right vectorPartitionSearchRefResultV1) bool {
	return vectorPartitionSearchRefResultBetterV1(right, left)
}

func (h *vectorPartitionSearchRefResultMaxHeapV1) pushBounded(limit int, candidate vectorPartitionSearchRefResultV1) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(len(*h) - 1)
		return
	}
	if !vectorPartitionSearchRefResultBetterV1(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(0)
}

func (h *vectorPartitionSearchRefResultMaxHeapV1) popWorst() vectorPartitionSearchRefResultV1 {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	if len(*h) > 0 {
		h.down(0)
	}
	return out
}

func (h *vectorPartitionSearchRefResultMaxHeapV1) up(child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !vectorPartitionSearchRefResultWorseV1((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorPartitionSearchRefResultMaxHeapV1) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(*h) {
			return
		}
		child := left
		right := left + 1
		if right < len(*h) && vectorPartitionSearchRefResultWorseV1((*h)[right], (*h)[left]) {
			child = right
		}
		if !vectorPartitionSearchRefResultWorseV1((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		parent = child
	}
}

func vectorPartitionSearchResultBetterV1(left, right VectorPartitionSearchResultV1) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	return left.ID < right.ID
}

func vectorPartitionSearchResultWorseV1(left, right VectorPartitionSearchResultV1) bool {
	return vectorPartitionSearchResultBetterV1(right, left)
}

func (h *vectorPartitionSearchResultMaxHeapV1) pushBounded(limit int, candidate VectorPartitionSearchResultV1) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(len(*h) - 1)
		return
	}
	if !vectorPartitionSearchResultBetterV1(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(0)
}

func (h *vectorPartitionSearchResultMaxHeapV1) popWorst() VectorPartitionSearchResultV1 {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	if len(*h) > 0 {
		h.down(0)
	}
	return out
}

func (h *vectorPartitionSearchResultMaxHeapV1) up(child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !vectorPartitionSearchResultWorseV1((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorPartitionSearchResultMaxHeapV1) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(*h) {
			return
		}
		child := left
		right := left + 1
		if right < len(*h) && vectorPartitionSearchResultWorseV1((*h)[right], (*h)[left]) {
			child = right
		}
		if !vectorPartitionSearchResultWorseV1((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		parent = child
	}
}

// VectorPartitionSearchMetricsV1 reports native traversal work for one local
// no-document search. Candidates and Edges use the same counters as TreeDB's
// prepared HNSW path.
type VectorPartitionSearchMetricsV1 struct {
	Candidates uint64
	Edges      uint64
	Route      string
}

// VectorPartitionSearchOptionsV1 is the bounded M3 no-document search
// contract consumed by routed serving layers. EfSearch is applied by the
// native HNSW pack. The exact in-memory fallback validates it but does not need
// a traversal frontier. MaxStableIDBytes lets a serving boundary reject an
// immutable asset before either route materializes result IDs; zero leaves the
// local searcher uncapped.
type VectorPartitionSearchOptionsV1 struct {
	TopK             int
	EfSearch         int
	MaxStableIDBytes int
}

const (
	VectorPartitionSearchRouteHNSWSearchPackV1 = "hnsw_search_pack_v1"
	VectorPartitionSearchRouteExactFP32ScanV1  = "exact_fp32_scan_v1"
)

type VectorPartitionSearchStatusV1 struct {
	Generation                          uint64
	PartitionID                         uint32
	HomeMemberships, OverlapMemberships int
	MaxStableIDBytes                    int
	PackBytes, MappedBytes, HeapBytes   uint64
	OpenNanos                           uint64
	SearchRoute                         string
	Candidates, Edges                   uint64
	ActivePins                          uint64
	Opened, Searches, Failures          uint64
	Retired                             bool
}

// SearchScratchBytesV1 returns a conservative upper bound for the transient
// candidate/search scratch allocated by one SearchWithOptionsV1 call. Serving
// layers use it before search so a small ef_search cannot conceal the
// row-count-sized visit bitmap required by the native HNSW pack.
func (s *VectorPartitionLocalSearcherV1) SearchScratchBytesV1(opts VectorPartitionSearchOptionsV1) (uint64, error) {
	_, scratchBytes, err := s.SearchPreflightV1(opts)
	return scratchBytes, err
}

// SearchPreflightV1 returns one coherent status and scratch-bound snapshot.
// Serving layers use it to validate a partition before any request traversal
// without separately reacquiring the searcher mutex for Status.
func (s *VectorPartitionLocalSearcherV1) SearchPreflightV1(opts VectorPartitionSearchOptionsV1) (VectorPartitionSearchStatusV1, uint64, error) {
	if s == nil || opts.TopK < 1 || opts.EfSearch < 0 || opts.MaxStableIDBytes < 0 {
		return VectorPartitionSearchStatusV1{}, 0, ErrVectorPartitionSearchUnavailable
	}
	s.mu.Lock()
	if s.retired {
		s.mu.Unlock()
		return VectorPartitionSearchStatusV1{}, 0, ErrVectorPartitionSearchUnavailable
	}
	status := s.statusLockedV1()
	prepared := s.prepared
	exactRows := len(s.asset.IDs)
	dimensions := s.asset.Dimensions
	maxStableIDBytes := s.maxStableIDBytes
	var header columnHNSWSearchPackHeader
	if prepared != nil {
		header = prepared.Header
	}
	s.mu.Unlock()

	scratchBytes, err := vectorPartitionSearchScratchBytesV1(opts, prepared, exactRows, dimensions, maxStableIDBytes, header)
	return status, scratchBytes, err
}

func vectorPartitionSearchScratchBytesV1(opts VectorPartitionSearchOptionsV1, prepared *columnHNSWSearchPackPreparedView, exactRows, dimensions, maxStableIDBytes int, header columnHNSWSearchPackHeader) (uint64, error) {
	if opts.MaxStableIDBytes > 0 && maxStableIDBytes > opts.MaxStableIDBytes {
		return 0, fmt.Errorf("%w: stable ID bytes=%d exceeds limit=%d", ErrVectorPartitionSearchUnavailable, maxStableIDBytes, opts.MaxStableIDBytes)
	}
	if prepared == nil {
		return vectorPartitionExactSearchScratchBytesV1(exactRows, dimensions, opts.TopK)
	}
	rowCount := header.Rows
	if rowCount < 0 || header.VectorStride < 0 || header.M < 0 {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	topK := opts.TopK
	if topK > rowCount {
		topK = rowCount
	}
	efSearch := opts.EfSearch
	if efSearch == 0 {
		efSearch = header.EfSearch
	}
	if efSearch < topK {
		efSearch = topK
	}
	if efSearch > rowCount {
		efSearch = rowCount
	}
	degree := header.M
	if degree < 1 {
		degree = 1
	}
	if degree <= math.MaxInt/2 {
		degree *= 2
	}
	if degree > rowCount {
		// A graph cannot expose more distinct neighbors than it has rows.
		// Capping here keeps the preflight bound identical to the scratch the
		// native path actually prepares, even for a tiny shard whose persisted
		// index definition has a much larger M.
		degree = rowCount
	}
	return vectorPartitionHNSWSearchScratchBytesV1(rowCount, header.Dimensions, header.VectorStride, degree, topK, efSearch)
}

func vectorPartitionExactSearchScratchBytesV1(rows, dimensions, topK int) (uint64, error) {
	if rows < 0 || dimensions <= 0 || topK < 1 {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	width := uint64(unsafe.Sizeof(VectorPartitionSearchResultV1{}))
	rowBytes, err := checkedVectorPartitionScratchProductV1(uint64(rows), width)
	if err != nil {
		return 0, err
	}
	selectedBytes, err := checkedVectorPartitionScratchProductV1(uint64(min(rows, topK)), width)
	if err != nil || math.MaxUint64-selectedBytes < selectedBytes {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	queryBytes, err := checkedVectorPartitionScratchProductV1(uint64(dimensions), uint64(unsafe.Sizeof(float32(0))))
	if err != nil {
		return 0, err
	}
	// Preserve the original row-sized conservative floor while covering the
	// bounded heap and final output slice, which coexist during heap draining.
	// The canonical FP32 query copy remains live through the complete scan.
	resultBytes := max(rowBytes, selectedBytes+selectedBytes)
	if math.MaxUint64-resultBytes < queryBytes {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	return resultBytes + queryBytes, nil
}

// VectorPartitionConservativeSearchScratchBytesV1 returns a transport-neutral
// upper bound for either M3 search route when only durable membership
// cardinality and request shape are available. Coordinators use it before
// weighting surplus candidate memory so a small high-dimensional partition
// cannot be starved by a larger peer.
func VectorPartitionConservativeSearchScratchBytesV1(rows, dimensions int, opts VectorPartitionSearchOptionsV1) (uint64, error) {
	if rows < 0 || dimensions <= 0 || opts.TopK < 1 || opts.EfSearch < opts.TopK || opts.MaxStableIDBytes < 0 {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	exactBytes, err := vectorPartitionExactSearchScratchBytesV1(rows, dimensions, opts.TopK)
	if err != nil {
		return 0, err
	}
	vectorStride, err := columnHNSWSearchPackVectorStrideForDimensions(dimensions)
	if err != nil {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	topK := min(opts.TopK, rows)
	efSearch := min(opts.EfSearch, rows)
	if efSearch < topK {
		efSearch = topK
	}
	// Search caps the doubled HNSW degree at row count. Using row count here
	// therefore covers every persisted M without requiring shard-local headers.
	hnswBytes, err := vectorPartitionHNSWSearchScratchBytesV1(rows, dimensions, vectorStride, rows, topK, efSearch)
	if err != nil {
		return 0, err
	}
	return max(exactBytes, hnswBytes), nil
}

func vectorPartitionHNSWSearchScratchBytesV1(rowCount, dimensions, vectorStride, degree, topK, efSearch int) (uint64, error) {
	if rowCount < 0 || dimensions <= 0 || vectorStride < 0 || degree < 0 || topK < 0 || efSearch < 0 {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	frontier := columnVectorGraphNativeSearchFrontierCapacity(rowCount, degree, topK, efSearch)

	var total uint64
	add := func(count int, width uintptr) error {
		if count < 0 {
			return ErrVectorPartitionSearchUnavailable
		}
		bytes, err := checkedVectorPartitionScratchProductV1(uint64(count), uint64(width))
		if err != nil || math.MaxUint64-total < bytes {
			return ErrVectorPartitionSearchUnavailable
		}
		total += bytes
		return nil
	}
	for _, item := range []struct {
		count int
		width uintptr
	}{
		{rowCount, unsafe.Sizeof(uint64(0))},
		{frontier, unsafe.Sizeof(columnVectorGraphSearchCandidate{})},
		{efSearch, unsafe.Sizeof(columnVectorGraphSearchCandidate{})},
		{topK, unsafe.Sizeof(columnVectorGraphNativeSearchResult{})},
		// Native results remain live while the public FP32/stable-ID buffer is
		// built and canonically reordered.
		{topK, unsafe.Sizeof(VectorPartitionSearchResultV1{})},
		{topK, unsafe.Sizeof([]byte(nil))},
		{topK, unsafe.Sizeof(int(0))},
		{topK, unsafe.Sizeof(int(0))},
		{topK, unsafe.Sizeof(DocumentRowRef{})},
		{topK, unsafe.Sizeof(bool(false))},
		{degree, unsafe.Sizeof(float64(0))},
		{degree, unsafe.Sizeof(uint32(0))},
		{degree, unsafe.Sizeof(float32(0))},
		{vectorStride, unsafe.Sizeof(float32(0))},
		// Canonical result scoring normalizes the request query into its own
		// FP32 buffer after native traversal; it does not reuse native scratch.
		{dimensions, unsafe.Sizeof(float32(0))},
	} {
		if err := add(item.count, item.width); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func checkedVectorPartitionScratchProductV1(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	return left * right, nil
}

type VectorPartitionLocalSearcherV1 struct {
	mu                                  sync.Mutex
	asset                               VectorPartitionSearchAssetV1
	normalizedVectors                   [][]float32
	digest                              string
	pins                                uint64
	retired                             bool
	opened, searches, failures          uint64
	partitionPin                        *VectorPartitionReaderPinV1
	closing                             bool
	persistentPinReleased               bool
	prepared                            *columnHNSWSearchPackPreparedView
	homeMemberships, overlapMemberships int
	packBytes, mappedBytes, heapBytes   uint64
	openNanos                           uint64
	searchRoute                         string
	maxStableIDBytes                    int
	candidates, edges                   uint64
}

// Close releases the M1 generation pin held by a persistent opener. It is
// harmless for an in-memory-only searcher.
func (s *VectorPartitionLocalSearcherV1) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.retired = true
	s.closing = true
	release := s.releasePersistentPinLocked()
	view := s.releasePreparedLocked()
	s.mu.Unlock()
	if view != nil {
		_ = view.Close()
	}
	if release != nil {
		release.Release()
	}
	return nil
}

func (s *VectorPartitionLocalSearcherV1) releasePreparedLocked() *columnHNSWSearchPackPreparedView {
	if s.closing && s.pins == 0 && s.prepared != nil {
		v := s.prepared
		s.prepared = nil
		return v
	}
	return nil
}

func (s *VectorPartitionLocalSearcherV1) releasePersistentPinLocked() *VectorPartitionReaderPinV1 {
	if s.closing && s.pins == 0 && !s.persistentPinReleased && s.partitionPin != nil {
		s.persistentPinReleased = true
		return s.partitionPin
	}
	return nil
}

func OpenVectorPartitionLocalSearcherV1(asset VectorPartitionSearchAssetV1) (*VectorPartitionLocalSearcherV1, error) {
	if err := validateVectorPartitionSearchAssetV1(asset); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	normalizedVectors := make([][]float32, len(asset.Vectors))
	for i, v := range asset.Vectors {
		normalized, err := canonicalVectorPartitionNormalizeV1(v)
		if err != nil {
			return nil, fmt.Errorf("%w: vector[%d] norm: %v", ErrVectorPartitionSearchUnavailable, i, err)
		}
		normalizedVectors[i] = normalized
	}
	h := sha256.New()
	h.Write([]byte(asset.ManifestChecksum))
	for i, id := range asset.IDs {
		h.Write([]byte(id))
		for _, x := range asset.Vectors[i] {
			h.Write([]byte(fmt.Sprintf("%08x", math.Float32bits(x))))
		}
	}
	return &VectorPartitionLocalSearcherV1{
		asset:             clonePartitionSearchAsset(asset),
		normalizedVectors: normalizedVectors,
		digest:            hex.EncodeToString(h.Sum(nil)),
		opened:            1,
		searchRoute:       VectorPartitionSearchRouteExactFP32ScanV1,
		maxStableIDBytes:  vectorPartitionMaxStableIDBytesV1(asset.IDs),
	}, nil
}

func vectorPartitionMaxStableIDBytesV1(ids []string) int {
	maxBytes := 0
	for _, id := range ids {
		if len(id) > maxBytes {
			maxBytes = len(id)
		}
	}
	return maxBytes
}

func vectorPartitionPreparedMaxStableIDBytesV1(view *columnHNSWSearchPackPreparedView) (int, error) {
	if view == nil || len(view.DocumentIDOffsets) != view.Header.Rows+1 {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	maxBytes := uint64(0)
	for i := 0; i < view.Header.Rows; i++ {
		start, end := view.DocumentIDOffsets[i], view.DocumentIDOffsets[i+1]
		if end < start || end > uint64(len(view.DocumentIDBytes)) {
			return 0, ErrVectorPartitionSearchUnavailable
		}
		if width := end - start; width > maxBytes {
			maxBytes = width
		}
	}
	if maxBytes > uint64(math.MaxInt) {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	return int(maxBytes), nil
}

func validateVectorPartitionSearchAssetV1(a VectorPartitionSearchAssetV1) error {
	if a.ManifestChecksum == "" || len(a.ManifestChecksum) != 64 || a.Generation == 0 || a.Dimensions < 1 || a.Dimensions > 4096 || len(a.IDs) == 0 || len(a.IDs) > 1_000_000 || len(a.IDs) != len(a.Vectors) || len(a.IDs) != len(a.Kinds) || len(a.Adjacency) != 0 && len(a.Adjacency) != len(a.IDs) {
		return errors.New("identity/count bounds")
	}
	seen := make(map[string]struct{}, len(a.IDs))
	for i, id := range a.IDs {
		if id == "" {
			return errors.New("empty stable ID")
		}
		if _, ok := seen[id]; ok {
			return errors.New("duplicate stable ID")
		}
		seen[id] = struct{}{}
		if len(a.Vectors[i]) != a.Dimensions {
			return errors.New("vector dimensions")
		}
		for _, x := range a.Vectors[i] {
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
				return errors.New("nonfinite vector")
			}
		}
		if a.Kinds[i] != VectorPartitionMembershipHomeV1 && a.Kinds[i] != VectorPartitionMembershipOverlapV1 {
			return errors.New("membership kind")
		}
		if len(a.Adjacency) > 0 {
			last := uint32(0)
			for j, n := range a.Adjacency[i] {
				if int(n) >= len(a.IDs) || (j > 0 && n <= last) {
					return errors.New("invalid HNSW adjacency")
				}
				last = n
			}
		}
	}
	if _, err := hex.DecodeString(a.ManifestChecksum); err != nil {
		return errors.New("manifest checksum")
	}
	return nil
}
func clonePartitionSearchAsset(a VectorPartitionSearchAssetV1) VectorPartitionSearchAssetV1 {
	out := a
	out.IDs = append([]string(nil), a.IDs...)
	out.Kinds = append([]VectorPartitionMembershipKindV1(nil), a.Kinds...)
	out.Vectors = make([][]float32, len(a.Vectors))
	out.Adjacency = make([][]uint32, len(a.Adjacency))
	for i := range a.Vectors {
		out.Vectors[i] = append([]float32(nil), a.Vectors[i]...)
	}
	for i := range a.Adjacency {
		out.Adjacency[i] = append([]uint32(nil), a.Adjacency[i]...)
	}
	return out
}
func (s *VectorPartitionLocalSearcherV1) Acquire() error {
	if s == nil {
		return ErrVectorPartitionSearchUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		s.failures++
		return ErrVectorPartitionSearchUnavailable
	}
	s.pins++
	return nil
}
func (s *VectorPartitionLocalSearcherV1) Release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.pins > 0 {
		s.pins--
	}
	release := s.releasePersistentPinLocked()
	view := s.releasePreparedLocked()
	s.mu.Unlock()
	if view != nil {
		_ = view.Close()
	}
	if release != nil {
		release.Release()
	}
}
func (s *VectorPartitionLocalSearcherV1) Retire() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.retired = true
	s.closing = true
	if s.pins != 0 {
		s.mu.Unlock()
		return fmt.Errorf("%w: generation still pinned", ErrVectorPartitionSearchUnavailable)
	}
	release := s.releasePersistentPinLocked()
	view := s.releasePreparedLocked()
	s.mu.Unlock()
	if view != nil {
		_ = view.Close()
	}
	if release != nil {
		release.Release()
	}
	return nil
}
func (s *VectorPartitionLocalSearcherV1) Search(query []float32, topK int) ([]VectorPartitionSearchResultV1, error) {
	results, _, err := s.SearchWithOptionsV1(context.Background(), query, VectorPartitionSearchOptionsV1{TopK: topK})
	return results, err
}

// SearchWithMetrics executes the same authoritative no-document path as
// Search and returns native candidate/edge accounting for benchmark and status
// attribution.
func (s *VectorPartitionLocalSearcherV1) SearchWithMetrics(query []float32, topK int) ([]VectorPartitionSearchResultV1, VectorPartitionSearchMetricsV1, error) {
	return s.SearchWithOptionsV1(context.Background(), query, VectorPartitionSearchOptionsV1{TopK: topK})
}

// SearchWithOptionsV1 executes M3's authoritative no-document path with an
// explicit HNSW candidate frontier. It observes cancellation before acquiring
// resources and before returning results. Serving layers that need to abandon
// an in-flight native traversal may call this method in a goroutine: Close
// defers the persistent generation-pin release until the search pin exits.
func (s *VectorPartitionLocalSearcherV1) SearchWithOptionsV1(ctx context.Context, query []float32, opts VectorPartitionSearchOptionsV1) ([]VectorPartitionSearchResultV1, VectorPartitionSearchMetricsV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	if err := s.Acquire(); err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	defer s.Release()
	if opts.TopK < 1 || opts.EfSearch < 0 || opts.MaxStableIDBytes < 0 || len(query) != s.asset.Dimensions {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: query bounds", ErrVectorPartitionSearchUnavailable)
	}
	if opts.MaxStableIDBytes > 0 && s.maxStableIDBytes > opts.MaxStableIDBytes {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: stable ID bytes=%d exceeds limit=%d", ErrVectorPartitionSearchUnavailable, s.maxStableIDBytes, opts.MaxStableIDBytes)
	}
	var queryNormSquared float64
	for _, x := range query {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: query nonfinite", ErrVectorPartitionSearchUnavailable)
		}
		queryNormSquared += float64(x) * float64(x)
	}
	if queryNormSquared == 0 {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: zero query", ErrVectorPartitionSearchUnavailable)
	}
	if s.prepared != nil {
		results, stats, err := s.prepared.searchCosineWithContext(ctx, query, columnVectorGraphNativeSearchOptions{TopK: opts.TopK, EfSearch: opts.EfSearch}, &columnVectorGraphNativeSearchScratch{})
		if err != nil {
			s.recordFailure()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, VectorPartitionSearchMetricsV1{}, err
			}
			return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: native HNSW: %v", ErrVectorPartitionSearchUnavailable, err)
		}
		if err := ctx.Err(); err != nil {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, err
		}
		out, err := canonicalizeVectorPartitionNativeResultsV1(ctx, s.prepared, query, results)
		if err != nil {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, err
		}
		metrics := VectorPartitionSearchMetricsV1{Candidates: stats.Candidates, Edges: stats.Edges, Route: VectorPartitionSearchRouteHNSWSearchPackV1}
		s.mu.Lock()
		s.searches++
		s.candidates += metrics.Candidates
		s.edges += metrics.Edges
		s.mu.Unlock()
		return out, metrics, nil
	}
	normalizedQuery, err := canonicalVectorPartitionNormalizeV1(query)
	if err != nil {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	limit := min(opts.TopK, len(s.asset.IDs))
	top := make(vectorPartitionSearchResultMaxHeapV1, 0, limit)
	var edges uint64
	for i := range s.asset.Vectors {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				s.recordFailure()
				return nil, VectorPartitionSearchMetricsV1{}, err
			}
		}
		score, err := canonicalVectorPartitionNormalizedScoreV1(normalizedQuery, s.normalizedVectors[i])
		if err != nil {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, err
		}
		top.pushBounded(limit, VectorPartitionSearchResultV1{
			ID:    s.asset.IDs[i],
			Score: score,
		})
		if len(s.asset.Adjacency) > i {
			edges += uint64(len(s.asset.Adjacency[i]))
		}
	}
	out := make([]VectorPartitionSearchResultV1, len(top))
	for completed, target := 0, len(out)-1; target >= 0; completed, target = completed+1, target-1 {
		if completed&255 == 0 {
			if err := ctx.Err(); err != nil {
				s.recordFailure()
				return nil, VectorPartitionSearchMetricsV1{}, err
			}
		}
		out[target] = top.popWorst()
	}
	if err := ctx.Err(); err != nil {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	s.mu.Lock()
	s.searches++
	s.candidates += uint64(len(s.asset.IDs))
	s.edges += edges
	s.mu.Unlock()
	return out, VectorPartitionSearchMetricsV1{Candidates: uint64(len(s.asset.IDs)), Edges: edges, Route: VectorPartitionSearchRouteExactFP32ScanV1}, nil
}

// SearchExactWithOptionsV1 scans the already generation-pinned prepared pack.
// It is diagnostic attribution only; it never reopens mutable source state.
func (s *VectorPartitionLocalSearcherV1) SearchExactWithOptionsV1(ctx context.Context, query []float32, opts VectorPartitionSearchOptionsV1) ([]VectorPartitionSearchResultV1, VectorPartitionSearchMetricsV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	if err := s.Acquire(); err != nil {
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	defer s.Release()
	if s.prepared == nil || opts.TopK < 1 || opts.EfSearch < 0 || opts.MaxStableIDBytes < 0 || len(query) != s.asset.Dimensions {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: query bounds", ErrVectorPartitionSearchUnavailable)
	}
	if opts.MaxStableIDBytes > 0 && s.maxStableIDBytes > opts.MaxStableIDBytes {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: stable ID bytes=%d exceeds limit=%d", ErrVectorPartitionSearchUnavailable, s.maxStableIDBytes, opts.MaxStableIDBytes)
	}
	normalizedQuery, err := canonicalVectorPartitionNormalizeV1(query)
	if err != nil {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	rows, dims, stride := int(s.prepared.Header.Rows), s.asset.Dimensions, int(s.prepared.Header.VectorStride)
	limit := min(opts.TopK, rows)
	top := make(vectorPartitionSearchRefResultMaxHeapV1, 0, limit)
	for row := 0; row < rows; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				s.recordFailure()
				return nil, VectorPartitionSearchMetricsV1{}, err
			}
		}
		score, err := canonicalVectorPartitionNormalizedScoreV1(normalizedQuery, s.prepared.NormalizedVectors[row*stride:row*stride+dims])
		if err != nil {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, err
		}
		start, end := s.prepared.DocumentIDOffsets[row], s.prepared.DocumentIDOffsets[row+1]
		top.pushBounded(limit, vectorPartitionSearchRefResultV1{ID: s.prepared.DocumentIDBytes[start:end], Score: score})
	}
	out := make([]VectorPartitionSearchResultV1, len(top))
	for target := len(out) - 1; target >= 0; target-- {
		if target&255 == 0 {
			if err := ctx.Err(); err != nil {
				s.recordFailure()
				return nil, VectorPartitionSearchMetricsV1{}, err
			}
		}
		candidate := top.popWorst()
		out[target] = VectorPartitionSearchResultV1{ID: string(candidate.ID), Score: candidate.Score}
	}
	if err := ctx.Err(); err != nil {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	s.mu.Lock()
	s.searches++
	s.candidates += uint64(rows)
	s.mu.Unlock()
	return out, VectorPartitionSearchMetricsV1{Candidates: uint64(rows), Route: VectorPartitionSearchRouteExactFP32ScanV1}, nil
}

func canonicalizeVectorPartitionNativeResultsV1(ctx context.Context, prepared *columnHNSWSearchPackPreparedView, query []float32, results []columnVectorGraphNativeSearchResult) ([]VectorPartitionSearchResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Native HNSW traversal may use platform-optimized FP32 kernels. Recompute
	// the returned candidate scores with the deterministic public contract, then
	// rebuild and drain the bounded heap before M5 publishes owned results.
	if prepared == nil || len(query) != prepared.Header.Dimensions {
		return nil, ErrVectorPartitionSearchUnavailable
	}
	normalizedQuery, err := canonicalVectorPartitionNormalizeV1(query)
	if err != nil {
		return nil, err
	}
	top := make(vectorPartitionSearchResultMaxHeapV1, 0, len(results))
	for i, result := range results {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if result.Ordinal < 0 || result.Ordinal >= prepared.Header.Rows {
			return nil, ErrVectorPartitionSearchUnavailable
		}
		base := result.Ordinal * prepared.Header.VectorStride
		idStart, idEnd := prepared.DocumentIDOffsets[result.Ordinal], prepared.DocumentIDOffsets[result.Ordinal+1]
		if !bytes.Equal(result.ID, prepared.DocumentIDBytes[idStart:idEnd]) {
			return nil, ErrVectorPartitionSearchUnavailable
		}
		score, err := canonicalVectorPartitionNormalizedScoreV1(normalizedQuery, prepared.NormalizedVectors[base:base+prepared.Header.Dimensions])
		if err != nil {
			return nil, err
		}
		top.pushBounded(len(results), VectorPartitionSearchResultV1{ID: string(result.ID), Score: score})
	}
	out := []VectorPartitionSearchResultV1(top)
	for completed, target := 0, len(out)-1; target >= 0; completed, target = completed+1, target-1 {
		if completed&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		out[target] = top.popWorst()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *VectorPartitionLocalSearcherV1) recordFailure() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.failures++
	s.mu.Unlock()
}

func (s *VectorPartitionLocalSearcherV1) Status() VectorPartitionSearchStatusV1 {
	if s == nil {
		return VectorPartitionSearchStatusV1{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusLockedV1()
}

func (s *VectorPartitionLocalSearcherV1) statusLockedV1() VectorPartitionSearchStatusV1 {
	st := VectorPartitionSearchStatusV1{Generation: s.asset.Generation, PartitionID: s.asset.PartitionID, ActivePins: s.pins, Opened: s.opened, Searches: s.searches, Failures: s.failures, Retired: s.retired, HomeMemberships: s.homeMemberships, OverlapMemberships: s.overlapMemberships, MaxStableIDBytes: s.maxStableIDBytes, PackBytes: s.packBytes, MappedBytes: s.mappedBytes, HeapBytes: s.heapBytes, OpenNanos: s.openNanos, SearchRoute: s.searchRoute, Candidates: s.candidates, Edges: s.edges}
	if s.prepared != nil {
		return st
	}
	for i, k := range s.asset.Kinds {
		if k == VectorPartitionMembershipHomeV1 {
			st.HomeMemberships++
		} else {
			st.OverlapMemberships++
		}
		st.HeapBytes += uint64(4*len(s.asset.Vectors[i]) + 4*len(s.normalizedVectors[i]) + len(s.asset.IDs[i]))
	}
	st.PackBytes = st.HeapBytes
	return st
}
