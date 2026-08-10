package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLocalHNSWAttributionHistoricalBaselineV1(t *testing.T) {
	dir := t.TempDir()
	digest := func(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
	write := func(name string, raw []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cell := func(probes int, recall float64) vectorPartitionSystemBenchCellV1 {
		return vectorPartitionSystemBenchCellV1{Status: "valid", Budget: map[string]int{"probes": probes}, Concurrency: 1, Metrics: vectorPartitionSystemBenchMetricsV1{Queries: 1000, CompletedQueries: 1000, ResultCount: 10000, RecallAt10: recall, QPS: 1, P50Nanos: 1, P95Nanos: 2, P99Nanos: 3}, Counters: map[string]uint64{"candidates": 1, "edges": 1}}
	}
	report := vectorPartitionSystemBenchResultV1{SchemaVersion: 1, ResultKind: "vector_partition_system_bench_v1", Topology: "single_daemon_four_group", TopologyIdentitySHA256: localHNSWAttributionHistoricalTopologySHA256V1[0], DatasetChecksum: strings.Repeat("a", 64), TruthArtifactSHA256: strings.Repeat("b", 64), TopK: 10, EfSearch: 128, Cells: []vectorPartitionSystemBenchCellV1{cell(2, .9247), cell(16, .9265)}}
	encode := func(name string, r vectorPartitionSystemBenchResultV1) (string, string) {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		return write(name, raw), digest(raw)
	}
	marshal := func(value vectorPartitionSystemBenchResultV1) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	cfg := localHNSWAttributionInputConfigV1{Fixture: fixtureManifest{Checksum: report.DatasetChecksum}, TruthArtifactSHA256: report.TruthArtifactSHA256}
	for i := range cfg.HistoricalSearchReports {
		report.TopologyIdentitySHA256 = localHNSWAttributionHistoricalTopologySHA256V1[i]
		cfg.HistoricalSearchReports[i], cfg.HistoricalReportSHA256[i] = encode("valid"+strconv.Itoa(i)+".json", report)
	}
	report.TopologyIdentitySHA256 = localHNSWAttributionHistoricalTopologySHA256V1[0]
	if evidence, err := localHNSWAttributionHistoricalBaselineV1(cfg); err != nil || evidence[0].Probe2.Probes != 2 || evidence[0].Probe16.Probes != 16 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	for _, tc := range []struct {
		name string
		raw  func() []byte
	}{
		{"identity", func() []byte {
			x := cloneLocalHNSWAttributionHistoryReportV1(t, report)
			x.TopK = 9
			return marshal(x)
		}},
		{"truth", func() []byte {
			x := cloneLocalHNSWAttributionHistoryReportV1(t, report)
			x.TruthArtifactSHA256 = strings.Repeat("c", 64)
			return marshal(x)
		}},
		{"missing cell", func() []byte {
			x := cloneLocalHNSWAttributionHistoryReportV1(t, report)
			x.Cells = x.Cells[:1]
			return marshal(x)
		}},
		{"duplicate cell", func() []byte {
			x := cloneLocalHNSWAttributionHistoryReportV1(t, report)
			x.Cells = append(x.Cells, x.Cells[0])
			return marshal(x)
		}},
		{"recall", func() []byte {
			x := cloneLocalHNSWAttributionHistoryReportV1(t, report)
			x.Cells[0].Metrics.RecallAt10 = 1
			return marshal(x)
		}},
		{"incomplete metrics", func() []byte {
			x := cloneLocalHNSWAttributionHistoryReportV1(t, report)
			x.Cells[0].Metrics.CompletedQueries = 999
			return marshal(x)
		}},
		{"unknown JSON", func() []byte {
			raw := marshal(cloneLocalHNSWAttributionHistoryReportV1(t, report))
			return append(raw[:len(raw)-1], []byte(`,"unknown":true}`)...)
		}},
		{"trailing JSON", func() []byte {
			raw := marshal(cloneLocalHNSWAttributionHistoryReportV1(t, report))
			return append(raw, []byte(` {}`)...)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.raw()
			bad := cfg
			bad.HistoricalSearchReports[0], bad.HistoricalReportSHA256[0] = write(tc.name+".json", raw), digest(raw)
			if _, err := localHNSWAttributionHistoricalBaselineV1(bad); err == nil {
				t.Fatal("invalid report accepted")
			}
		})
	}
}

func cloneLocalHNSWAttributionHistoryReportV1(t *testing.T, report vectorPartitionSystemBenchResultV1) vectorPartitionSystemBenchResultV1 {
	t.Helper()
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var out vectorPartitionSystemBenchResultV1
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
