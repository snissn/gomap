package nativewire

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestVectorPartitionPublicBackendMapsCoordinatorErrorsV1(t *testing.T) {
	cases := []struct {
		code VectorPartitionCoordinatorErrorCodeV1
		want public.ErrorCodeV1
	}{
		{VectorPartitionCoordinatorErrorInvalidRequestV1, public.ErrorInvalidRequestV1},
		{VectorPartitionCoordinatorErrorGenerationMismatchV1, public.ErrorGenerationMismatchV1},
		{VectorPartitionCoordinatorErrorCanceledV1, public.ErrorCanceledV1},
		{VectorPartitionCoordinatorErrorDeadlineV1, public.ErrorDeadlineExceededV1},
		{VectorPartitionCoordinatorErrorUnavailableV1, public.ErrorUnavailableV1},
	}
	for _, tc := range cases {
		err := publicBackendErrorV1(&VectorPartitionCoordinatorErrorV1{Code: tc.code})
		var got *public.ErrorV1
		if !errors.As(err, &got) || got.Code != tc.want {
			t.Fatalf("code=%q got=%v", tc.code, err)
		}
	}
	if got := publicBackendErrorV1(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("canceled = %v", got)
	}
}

func TestVectorPartitionPublicBackendSearchesProductionTopologyV1(t *testing.T) {
	topology, base, reads := newVectorPartitionProductionTopologyTwoGroupTestV1(t)
	defer topology.Close()
	backend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{
		Topology: topology, RequestBase: base,
		Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: base.IndexName}, Generation: 7},
	}}
	request := public.SearchRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: base.IndexName, Generation: 7}, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: base.TopK, Probes: base.PartitionProbes, EfSearch: base.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: base.RequestBytesLimit, CandidateBytes: base.CandidateBytesLimit, ResponseBytes: base.ResponseBytesLimit, MergeEntries: base.MergeEntriesLimit}}
	response, err := backend.SearchVectorPartitionV1(context.Background(), request)
	if err != nil || len(response.Neighbors) == 0 {
		t.Fatalf("search = %#v, %v", response, err)
	}
	for group, read := range reads {
		if read.callCount() == 0 {
			t.Fatalf("group %q did not produce read evidence", group)
		}
	}
}
