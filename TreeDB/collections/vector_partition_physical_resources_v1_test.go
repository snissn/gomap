package collections

import (
	"context"
	"errors"
	"testing"
)

// Called by the real persistent multichunk opener and the forced heap fallback
// multichunk fixture, after each fixture's search/parity assertions.
func assertVectorPartitionPhysicalResourcesV1(t *testing.T, s *VectorPartitionLocalSearcherV1) {
	t.Helper()
	v := s.prepared
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := s.InspectPhysicalResourcesV1(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled inspection: %v", err)
	}
	live, release, err := s.InspectPhysicalResourcesV1(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if live.MappedExtentBytes != v.mappedBytes || live.HeapCopyBytes != v.heapCopyBytes || live.MetadataBytesBound != v.retainedChunkMetadataBytes() || live.LogicalHandles != uint64(len(v.handles)+1) || live.RequiredChunks != s.sectionChunks || live.OpenedChunks != live.RequiredChunks || live.ValidatedChunks != live.RequiredChunks {
		t.Fatalf("physical counters %+v", live)
	}
	before := release()
	if before.LogicalHandles != int64(live.LogicalHandles) || before.MappedExtentBytes != int64(live.MappedExtentBytes) || before.HeapCopyBytes != int64(live.HeapCopyBytes) {
		t.Fatalf("manager/live mismatch %+v %+v", before, live)
	}
	if _, bound, err := s.SearchPreflightV1(VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 64}); err != nil || bound == 0 {
		t.Fatalf("scratch bound=%d err=%v", bound, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	after := release()
	if after.Errors != 0 || after.LogicalHandles != 0 || after.MappedExtentBytes != 0 || after.HeapCopyBytes != 0 || after.Acquires != after.Releases {
		t.Fatalf("owned manager release %+v", after)
	}
	if _, _, err := s.InspectPhysicalResourcesV1(t.Context()); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("closed inspection: %v", err)
	}
	if err := s.Close(); err != nil || release() != after {
		t.Fatal("double close changed release counters")
	}
}
