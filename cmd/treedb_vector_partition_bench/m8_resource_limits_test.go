package main

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestM8RowCountersTrackTruePerRequestMaximaV1(t *testing.T) {
	var row m8ProductionRowV1
	m8AccumulateProductionRowCountersV1(&row, nativewire.VectorPartitionCoordinatorCountersV1{
		Requests: 2, RPCs: 3, Retries: 1, Redirects: 1,
		RequestBytes: 100, CandidateBytes: 200, ResponseBytes: 300, MergeEntries: 20,
		MaxShardRequestBytes: 70, MaxShardCandidateBytes: 150, MaxShardResponseBytes: 220,
	})
	m8AccumulateProductionRowCountersV1(&row, nativewire.VectorPartitionCoordinatorCountersV1{
		Requests: 4, RPCs: 4, RequestBytes: 90, CandidateBytes: 400, ResponseBytes: 250, MergeEntries: 40,
		MaxShardRequestBytes: 60, MaxShardCandidateBytes: 300, MaxShardResponseBytes: 180,
	})
	if row.RequestBytes != 190 || row.CandidateBytes != 600 || row.ResponseBytes != 550 || row.RPCs != 7 {
		t.Fatalf("aggregate counters=%+v", row)
	}
	if row.MaxRequests != 4 || row.MaxRPCs != 4 || row.MaxRetries != 1 || row.MaxRedirects != 1 ||
		row.MaxRequestBytes != 100 || row.MaxCandidateBytes != 400 || row.MaxResponseBytes != 300 || row.MaxMergeEntries != 40 ||
		row.MaxShardRequestBytes != 70 || row.MaxShardCandidateBytes != 300 || row.MaxShardResponseBytes != 220 {
		t.Fatalf("per-request maxima=%+v", row)
	}
}

func TestM8ObservedResourceMaximaUseRecordedMaximaNotAveragesV1(t *testing.T) {
	rows := []m8ProductionRowV1{{
		Samples: 2, RPCs: 10, RequestBytes: 1000, CandidateBytes: 2000, ResponseBytes: 3000,
		MaxRequests: 3, MaxRPCs: 6, MaxRetries: 2, MaxRedirects: 1,
		MaxRequestBytes: 900, MaxCandidateBytes: 1800, MaxResponseBytes: 2700, MaxMergeEntries: 25,
		MaxShardRequestBytes: 800, MaxShardCandidateBytes: 1700, MaxShardResponseBytes: 2600,
	}}
	got := m8ObservedResourceMaximaV1(rows)
	if got.Requests != 3 || got.RPCs != 6 || got.Retries != 2 || got.Redirects != 1 ||
		got.RequestBytes != 900 || got.CandidateBytes != 1800 || got.ResponseBytes != 2700 || got.MergeEntries != 25 ||
		got.ShardRequestBytes != 800 || got.ShardCandidateBytes != 1700 || got.ShardResponseBytes != 2600 {
		t.Fatalf("observed maxima=%+v", got)
	}
}
