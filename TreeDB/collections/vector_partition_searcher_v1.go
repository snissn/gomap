package collections

// M3's partition-local asset is deliberately a no-document search surface. It
// can be carried by a serving group without claiming canonical-row ownership.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
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
type VectorPartitionSearchStatusV1 struct {
	Generation                          uint64
	PartitionID                         uint32
	HomeMemberships, OverlapMemberships int
	PackBytes, HeapBytes                uint64
	ActivePins                          uint64
	Opened, Searches, Failures          uint64
	Retired                             bool
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
	packBytes, heapBytes                uint64
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
	return &VectorPartitionLocalSearcherV1{asset: clonePartitionSearchAsset(asset), norms: norms, digest: hex.EncodeToString(h.Sum(nil)), opened: 1}, nil
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
	if err := s.Acquire(); err != nil {
		return nil, err
	}
	defer s.Release()
	if topK < 1 || len(query) != s.asset.Dimensions {
		return nil, fmt.Errorf("%w: query bounds", ErrVectorPartitionSearchUnavailable)
	}
	var qn float64
	for _, x := range query {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, fmt.Errorf("%w: query nonfinite", ErrVectorPartitionSearchUnavailable)
		}
		qn += float64(x) * float64(x)
	}
	if qn == 0 {
		return nil, fmt.Errorf("%w: zero query", ErrVectorPartitionSearchUnavailable)
	}
	if s.prepared != nil {
		results, _, err := s.prepared.searchCosine(query, columnVectorGraphNativeSearchOptions{TopK: topK}, &columnVectorGraphNativeSearchScratch{})
		if err != nil {
			return nil, fmt.Errorf("%w: native HNSW: %v", ErrVectorPartitionSearchUnavailable, err)
		}
		out := make([]VectorPartitionSearchResultV1, len(results))
		for i, r := range results {
			out[i] = VectorPartitionSearchResultV1{ID: string(r.ID), Score: float32(r.Score)}
		}
		s.mu.Lock()
		s.searches++
		s.mu.Unlock()
		return out, nil
	}
	out := make([]VectorPartitionSearchResultV1, len(s.asset.IDs))
	for i, v := range s.asset.Vectors {
		var d float64
		for j, x := range v {
			d += float64(x) * float64(query[j])
		}
		out[i] = VectorPartitionSearchResultV1{ID: s.asset.IDs[i], Score: float32(d / (math.Sqrt(qn) * float64(s.norms[i])))}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	if topK < len(out) {
		out = out[:topK]
	}
	s.mu.Lock()
	s.searches++
	s.mu.Unlock()
	return out, nil
}
func (s *VectorPartitionLocalSearcherV1) Status() VectorPartitionSearchStatusV1 {
	if s == nil {
		return VectorPartitionSearchStatusV1{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := VectorPartitionSearchStatusV1{Generation: s.asset.Generation, PartitionID: s.asset.PartitionID, ActivePins: s.pins, Opened: s.opened, Searches: s.searches, Failures: s.failures, Retired: s.retired, HomeMemberships: s.homeMemberships, OverlapMemberships: s.overlapMemberships, PackBytes: s.packBytes, HeapBytes: s.heapBytes}
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
