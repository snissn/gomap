package nativewire

import (
	"bytes"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestSplitInsertVisibilityWireOwnedAndBoundedV1(t *testing.T) {
	limits := iwire.DefaultLimits()
	request := public.SearchRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 1}, Query: []float32{0, 1},
		Metric: public.MetricCosineV1, TopK: 1, Probes: 1, EfSearch: 1, Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 1 << 20, ResponseBytes: 1 << 20, MergeEntries: 1}, Deadline: time.Unix(0, 1234)}
	plain, err := appendVectorPartitionSearchRequestSectionV1(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	plainRaw := bytes.Clone(vectorPartitionTestSectionV1(t, plain, iwire.SectionVectorSearchRequest, limits))
	request.VisibilityToken = []byte("owned-durable-watermark")
	encoded, err := appendVectorPartitionSearchRequestSectionV1(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	raw := vectorPartitionTestSectionV1(t, encoded, iwire.SectionVectorSearchRequest, limits)
	if !bytes.HasPrefix(raw, plainRaw) {
		t.Fatal("token changed existing nil-token payload fields")
	}
	decoded, err := decodeVectorPartitionSearchRequestV1(raw, limits)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(decoded.VisibilityToken, request.VisibilityToken) {
		t.Fatal("search token aliases reusable frame")
	}
	request.VisibilityToken = make([]byte, (128<<10)+1)
	if _, err := appendVectorPartitionSearchRequestSectionV1(nil, request, limits); err == nil {
		t.Fatal("oversized search token encoded")
	}
	if _, err := decodeVectorPartitionSearchRequestV1(append(bytes.Clone(plainRaw), 0), limits); err == nil {
		t.Fatal("empty search token extension accepted")
	}

	response := public.InsertResponseV1{Generation: request.Generation, VisibilityGeneration: request.Generation, VisibleID: "doc", OwnerGroup: "group-b",
		CommitTerm: 1, CommitIndex: 2, AppliedIndex: 2, ProductionConsensus: true, LiveRevision: 3,
		Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}
	plain, err = appendVectorPartitionInsertResponseSectionV1(nil, response)
	if err != nil {
		t.Fatal(err)
	}
	plainRaw = bytes.Clone(vectorPartitionTestSectionV1(t, plain, iwire.SectionVectorInsertResponse, limits))
	response.VisibilityToken = []byte("owned-durable-watermark")
	encoded, err = appendVectorPartitionInsertResponseSectionV1(nil, response)
	if err != nil {
		t.Fatal(err)
	}
	raw = vectorPartitionTestSectionV1(t, encoded, iwire.SectionVectorInsertResponse, limits)
	if !bytes.HasPrefix(raw, plainRaw) {
		t.Fatal("token changed existing nil-token response fields")
	}
	got, err := decodeVectorPartitionInsertResponseV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(got.VisibilityToken, response.VisibilityToken) {
		t.Fatal("insert token aliases reusable frame")
	}
	response.VisibilityToken = make([]byte, (128<<10)+1)
	if _, err := appendVectorPartitionInsertResponseSectionV1(nil, response); err == nil {
		t.Fatal("oversized insert token encoded")
	}
	if _, err := decodeVectorPartitionInsertResponseV1(append(bytes.Clone(plainRaw), 0)); err == nil {
		t.Fatal("empty insert token extension accepted")
	}
}
