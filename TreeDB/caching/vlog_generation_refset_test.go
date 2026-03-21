package caching

import (
	"context"
	"testing"
)

type referencedSegmentsBackendStub struct {
	*MockBackend
	ids   map[uint32]struct{}
	calls int
}

func (b *referencedSegmentsBackendStub) ReferencedValueLogSegments(ctx context.Context) (map[uint32]struct{}, error) {
	b.calls++
	out := make(map[uint32]struct{}, len(b.ids))
	for id := range b.ids {
		out[id] = struct{}{}
	}
	return out, nil
}

func TestCurrentReferencedValueLogIDs_UsesBackendReferencedSetWhenAvailable(t *testing.T) {
	backend := &referencedSegmentsBackendStub{
		MockBackend: NewMockBackend(),
		ids: map[uint32]struct{}{
			11: {},
			42: {},
		},
	}
	db := &DB{backend: backend}
	ids, err := db.currentReferencedValueLogIDs()
	if err != nil {
		t.Fatalf("currentReferencedValueLogIDs: %v", err)
	}
	if backend.calls != 1 {
		t.Fatalf("backend calls=%d want 1", backend.calls)
	}
	if len(ids) != 2 {
		t.Fatalf("len(ids)=%d want 2", len(ids))
	}
	if _, ok := ids[11]; !ok {
		t.Fatalf("missing id 11")
	}
	if _, ok := ids[42]; !ok {
		t.Fatalf("missing id 42")
	}
}
