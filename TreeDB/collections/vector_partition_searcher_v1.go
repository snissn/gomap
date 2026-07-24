package collections

// M3's partition-local asset is deliberately a no-document search surface. It
// can be carried by a serving group without claiming canonical-row ownership.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"unsafe"
)

var ErrVectorPartitionSearchUnavailable = errors.New("collections: vector partition search unavailable")

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
// a traversal frontier.
type VectorPartitionSearchOptionsV1 struct {
	TopK     int
	EfSearch int
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
	if s == nil || opts.TopK < 1 || opts.EfSearch < 0 {
		return 0, ErrVectorPartitionSearchUnavailable
	}
	s.mu.Lock()
	if s.retired {
		s.mu.Unlock()
		return 0, ErrVectorPartitionSearchUnavailable
	}
	prepared := s.prepared
	exactRows := len(s.asset.IDs)
	var header columnHNSWSearchPackHeader
	if prepared != nil {
		header = prepared.Header
	}
	s.mu.Unlock()

	if prepared == nil {
		return checkedVectorPartitionScratchProductV1(uint64(exactRows), uint64(unsafe.Sizeof(VectorPartitionSearchResultV1{})))
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
		{topK, unsafe.Sizeof([]byte(nil))},
		{topK, unsafe.Sizeof(int(0))},
		{topK, unsafe.Sizeof(int(0))},
		{topK, unsafe.Sizeof(DocumentRowRef{})},
		{topK, unsafe.Sizeof(bool(false))},
		{degree, unsafe.Sizeof(float64(0))},
		{degree, unsafe.Sizeof(uint32(0))},
		{degree, unsafe.Sizeof(float32(0))},
		{header.VectorStride, unsafe.Sizeof(float32(0))},
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
	norms                               []float32
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
	norms := make([]float32, len(asset.Vectors))
	for i, v := range asset.Vectors {
		var sum float64
		for _, x := range v {
			sum += float64(x) * float64(x)
		}
		norms[i] = float32(math.Sqrt(sum))
		if norms[i] == 0 {
			return nil, fmt.Errorf("%w: zero vector", ErrVectorPartitionSearchUnavailable)
		}
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
		asset:            clonePartitionSearchAsset(asset),
		norms:            norms,
		digest:           hex.EncodeToString(h.Sum(nil)),
		opened:           1,
		searchRoute:      VectorPartitionSearchRouteExactFP32ScanV1,
		maxStableIDBytes: vectorPartitionMaxStableIDBytesV1(asset.IDs),
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
	if opts.TopK < 1 || opts.EfSearch < 0 || len(query) != s.asset.Dimensions {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: query bounds", ErrVectorPartitionSearchUnavailable)
	}
	var qn float64
	for _, x := range query {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: query nonfinite", ErrVectorPartitionSearchUnavailable)
		}
		qn += float64(x) * float64(x)
	}
	if qn == 0 {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: zero query", ErrVectorPartitionSearchUnavailable)
	}
	if s.prepared != nil {
		results, stats, err := s.prepared.searchCosine(query, columnVectorGraphNativeSearchOptions{TopK: opts.TopK, EfSearch: opts.EfSearch}, &columnVectorGraphNativeSearchScratch{})
		if err != nil {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, fmt.Errorf("%w: native HNSW: %v", ErrVectorPartitionSearchUnavailable, err)
		}
		if err := ctx.Err(); err != nil {
			s.recordFailure()
			return nil, VectorPartitionSearchMetricsV1{}, err
		}
		out := make([]VectorPartitionSearchResultV1, len(results))
		for i, r := range results {
			out[i] = VectorPartitionSearchResultV1{ID: string(r.ID), Score: float32(r.Score)}
		}
		metrics := VectorPartitionSearchMetricsV1{Candidates: stats.Candidates, Edges: stats.Edges, Route: VectorPartitionSearchRouteHNSWSearchPackV1}
		s.mu.Lock()
		s.searches++
		s.candidates += metrics.Candidates
		s.edges += metrics.Edges
		s.mu.Unlock()
		return out, metrics, nil
	}
	out := make([]VectorPartitionSearchResultV1, len(s.asset.IDs))
	var edges uint64
	for i, v := range s.asset.Vectors {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				s.recordFailure()
				return nil, VectorPartitionSearchMetricsV1{}, err
			}
		}
		var d float64
		for j, x := range v {
			d += float64(x) * float64(query[j])
		}
		out[i] = VectorPartitionSearchResultV1{ID: s.asset.IDs[i], Score: float32(d / (math.Sqrt(qn) * float64(s.norms[i])))}
		if len(s.asset.Adjacency) > i {
			edges += uint64(len(s.asset.Adjacency[i]))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	if err := ctx.Err(); err != nil {
		s.recordFailure()
		return nil, VectorPartitionSearchMetricsV1{}, err
	}
	if opts.TopK < len(out) {
		out = out[:opts.TopK]
	}
	s.mu.Lock()
	s.searches++
	s.candidates += uint64(len(s.asset.IDs))
	s.edges += edges
	s.mu.Unlock()
	return out, VectorPartitionSearchMetricsV1{Candidates: uint64(len(s.asset.IDs)), Edges: edges, Route: VectorPartitionSearchRouteExactFP32ScanV1}, nil
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
		st.HeapBytes += uint64(4*len(s.asset.Vectors[i]) + len(s.asset.IDs[i]))
	}
	st.PackBytes = st.HeapBytes
	return st
}
