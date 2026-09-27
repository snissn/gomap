package collections

import (
	"context"
	"fmt"
)

// VectorPartitionPhysicalResourcesV1 describes one idle retained prepared pack.
// MappedExtentBytes includes mapping alignment, not resident pages. HeapCopyBytes
// is fallback payload. MetadataBytesBound and StableIDBytesBound are conservative
// retained accounting estimates, not heap measurements. LogicalHandles are
// manager leases, not OS file descriptors. Chunks count section extents opened
// and validated in full; they do not count query touches (the root is separate).
type VectorPartitionPhysicalResourcesV1 struct {
	Generation         uint64 `json:"generation"`
	PartitionID        uint32 `json:"partition_id"`
	PackBytes          uint64 `json:"pack_bytes"`
	MappedExtentBytes  uint64 `json:"mapped_extent_bytes"`
	HeapCopyBytes      uint64 `json:"heap_copy_bytes"`
	MetadataBytesBound uint64 `json:"metadata_bytes_bound"`
	StableIDBytesBound uint64 `json:"stable_id_bytes_bound"`
	LogicalHandles     uint64 `json:"logical_handles"`
	RequiredChunks     uint64 `json:"required_chunks"`
	OpenedChunks       uint64 `json:"opened_chunks"`
	ValidatedChunks    uint64 `json:"validated_chunks"`
}

// VectorPartitionPhysicalReleaseV1 is observed from the captured private pack
// manager, including after the searcher detaches its prepared view on Close.
// It proves owned lease release, not Go metadata reclamation or a process RSS.
type VectorPartitionPhysicalReleaseV1 struct {
	LogicalHandles    int64  `json:"logical_handles"`
	MappedExtentBytes int64  `json:"mapped_extent_bytes"`
	HeapCopyBytes     int64  `json:"heap_copy_bytes"`
	Acquires          uint64 `json:"acquires"`
	Releases          uint64 `json:"releases"`
	Errors            uint64 `json:"errors"`
}

// InspectPhysicalResourcesV1 is an offline diagnostic, never called by Status
// or serving preflight. The caller must keep this searcher idle and own its
// lifetime. The returned observer retains only the manager (no prepared slices)
// and can be called after Close to check actual release counters.
func (s *VectorPartitionLocalSearcherV1) InspectPhysicalResourcesV1(ctx context.Context) (VectorPartitionPhysicalResourcesV1, func() VectorPartitionPhysicalReleaseV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionPhysicalResourcesV1{}, nil, err
	}
	if s == nil {
		return VectorPartitionPhysicalResourcesV1{}, nil, ErrVectorPartitionSearchUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.prepared
	if s.retired || s.pins != 0 || v == nil || v.manager == nil {
		return VectorPartitionPhysicalResourcesV1{}, nil, fmt.Errorf("%w: physical inspection requires idle live prepared pack", ErrVectorPartitionSearchUnavailable)
	}
	manager := v.manager
	observe := func() VectorPartitionPhysicalReleaseV1 {
		st := manager.Stats()
		return VectorPartitionPhysicalReleaseV1{st.ActiveHandles, st.ActiveMappedBytes, st.ActiveHeapCopyBytes, st.TotalAcquires, st.TotalReleases, st.Errors}
	}
	live := observe()
	if live.Errors != 0 || live.LogicalHandles != v.activeHandles || live.LogicalHandles < 1 || live.MappedExtentBytes < 0 || live.HeapCopyBytes < 0 || uint64(live.MappedExtentBytes) != v.mappedBytes || uint64(live.HeapCopyBytes) != v.heapCopyBytes {
		return VectorPartitionPhysicalResourcesV1{}, nil, fmt.Errorf("%w: physical manager/prepared accounting mismatch", ErrVectorPartitionSearchUnavailable)
	}
	return VectorPartitionPhysicalResourcesV1{
		Generation: s.asset.Generation, PartitionID: s.asset.PartitionID, PackBytes: s.packBytes,
		MappedExtentBytes: v.mappedBytes, HeapCopyBytes: v.heapCopyBytes,
		MetadataBytesBound: v.retainedChunkMetadataBytes(), StableIDBytesBound: s.stableIDOrdinalBytes,
		LogicalHandles: uint64(v.activeHandles), RequiredChunks: s.sectionChunks, OpenedChunks: s.sectionChunks, ValidatedChunks: s.sectionChunks,
	}, observe, nil
}
