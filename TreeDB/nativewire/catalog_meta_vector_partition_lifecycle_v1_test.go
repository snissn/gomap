package nativewire

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type catalogMetaLinearizableAppliedIndexProviderTestV1 struct {
	index uint64
	err   error
	calls int
}

func (p *catalogMetaLinearizableAppliedIndexProviderTestV1) LinearizableCatalogMetaAppliedIndexV1(context.Context) (uint64, error) {
	p.calls++
	return p.index, p.err
}

func TestLinearizableCatalogVectorPartitionLifecycleAuthorityFailsClosedBeforeLocalValidationV1(t *testing.T) {
	authority := raftplacement.NewCatalogMetaAuthorityV1()
	fence := &catalogMetaLinearizableAppliedIndexProviderTestV1{index: 7}
	adapter, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(authority, fence)
	if err != nil {
		t.Fatalf("NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1: %v", err)
	}
	_, err = adapter.ValidateVectorPartitionGenerationSearchV1(
		t.Context(),
		raftplacement.CollectionRefV1{Database: "app", Catalog: "default", Collection: "users"},
		"embedding", 1, "definition", 1, 2, 3, 4,
	)
	if !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
		t.Fatalf("stale local catalog err=%v want ErrCatalogMetaUnavailable", err)
	}
	if fence.calls != 1 {
		t.Fatalf("linearizable fence calls=%d want 1", fence.calls)
	}
}

func TestLinearizableCatalogVectorPartitionLifecycleAuthorityPropagatesFenceFailureV1(t *testing.T) {
	fence := &catalogMetaLinearizableAppliedIndexProviderTestV1{err: raftcluster.ErrNotLeader}
	adapter, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(raftplacement.NewCatalogMetaAuthorityV1(), fence)
	if err != nil {
		t.Fatalf("NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1: %v", err)
	}
	_, err = adapter.ValidateVectorPartitionGenerationSearchV1(
		t.Context(),
		raftplacement.CollectionRefV1{Database: "app", Catalog: "default", Collection: "users"},
		"embedding", 1, "definition", 1, 2, 3, 4,
	)
	if !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) || !errors.Is(err, raftcluster.ErrNotLeader) {
		t.Fatalf("fence failure err=%v want assets unavailable + not leader", err)
	}
}
