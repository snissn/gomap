package documentservice

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

type documentVectorPartitionBackendV1 struct{ calls int }

func (b *documentVectorPartitionBackendV1) SearchVectorPartitionV1(_ context.Context, r vectorpartition.SearchRequestV1) (vectorpartition.SearchResponseV1, error) {
	b.calls++
	return vectorpartition.SearchResponseV1{Generation: r.Generation}, nil
}
func (*documentVectorPartitionBackendV1) RegisterVectorPartitionV1(context.Context, vectorpartition.GenerationRegistrationV1) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) GenerationStatusV1(context.Context, vectorpartition.GenerationIDV1) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) PrepareVectorPartitionV1(context.Context, vectorpartition.GenerationIDV1) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) ActivateVectorPartitionV1(context.Context, vectorpartition.GenerationIDV1) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) InvalidateVectorPartitionV1(context.Context, vectorpartition.GenerationIDV1, string) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) RetireVectorPartitionV1(context.Context, vectorpartition.GenerationIDV1) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) RequestVectorPartitionRebuildV1(context.Context, vectorpartition.GenerationIDV1) (vectorpartition.GenerationStatusV1, error) {
	return vectorpartition.GenerationStatusV1{}, nil
}
func (*documentVectorPartitionBackendV1) VectorPartitionCleanupEligibilityV1(context.Context, vectorpartition.GenerationIDV1) (vectorpartition.CleanupEligibilityV1, error) {
	return vectorpartition.CleanupEligibilityV1{}, nil
}

func TestVectorPartitionServiceRegistrationDoesNotTransferBackendOwnershipV1(t *testing.T) {
	backend := &documentVectorPartitionBackendV1{}
	service, err := vectorpartition.NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	doc := New(nil)
	if err := doc.RegisterVectorPartitionServiceV1(service); err != nil {
		t.Fatal(err)
	}
	if _, err := doc.VectorPartitionServiceV1(); err == nil {
		t.Fatal("production service escaped before enabled operations registration")
	}
	config := vectorpartition.ConservativeOperationsConfigV1()
	config.Enabled = true
	operations, err := vectorpartition.NewOperationsV1(service, config, func(context.Context) (vectorpartition.OperationsHealthV1, error) {
		return vectorpartition.OperationsHealthV1{Ready: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.RegisterVectorPartitionOperationsV1(operations); err != nil {
		t.Fatal(err)
	}
	if _, err := doc.VectorPartitionServiceV1(); err != nil {
		t.Fatal(err)
	}
	if _, err := doc.VectorPartitionOperationsV1(); err != nil {
		t.Fatal(err)
	}
	if err := doc.Close(); err != nil {
		t.Fatal(err)
	}
	id := vectorpartition.GenerationIDV1{Index: "embedding", Generation: 1}
	if _, err := service.Search(context.Background(), vectorpartition.SearchRequestV1{Version: 1, Generation: id, Query: []float32{1}, Metric: vectorpartition.MetricCosineV1, TopK: 1, Probes: 1, EfSearch: 1, Consistency: vectorpartition.ConsistencyGenerationSnapshotV1, Limits: vectorpartition.SearchLimitsV1{RequestBytes: 4, CandidateBytes: 1, ResponseBytes: 1, MergeEntries: 1}}); err != nil {
		t.Fatal(err)
	}
	if backend.calls != 1 {
		t.Fatalf("backend calls = %d", backend.calls)
	}
}
