package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

var localHNSWAttributionHistoricalTopologySHA256V1 = [3]string{
	"ab794fe123d72ef8c84f6fdd46d513b029f08a57f3665bcd88b4e2a88f502b29",
	"6f170c01c0f69a7ecfee4b1554ac7c57d87c0adf0ac0ff19dd1b98d53c990545",
	"c1745e47b20b52d9981a97da1cfa2c0d321fcc347b2ce8233cd06ede04484f51",
}

type localHNSWAttributionHistoricalCellV1 struct {
	Probes     int                                 `json:"probes"`
	Metrics    vectorPartitionSystemBenchMetricsV1 `json:"metrics"`
	Candidates uint64                              `json:"candidates"`
	Edges      uint64                              `json:"edges"`
}

type localHNSWAttributionHistoricalReportV1 struct {
	Path                   string                               `json:"path"`
	SHA256                 string                               `json:"sha256"`
	TopologyIdentitySHA256 string                               `json:"topology_identity_sha256"`
	Probe2                 localHNSWAttributionHistoricalCellV1 `json:"probe_2"`
	Probe16                localHNSWAttributionHistoricalCellV1 `json:"probe_16"`
}

func localHNSWAttributionHistoricalBaselineV1(cfg localHNSWAttributionInputConfigV1) ([3]localHNSWAttributionHistoricalReportV1, error) {
	var evidence [3]localHNSWAttributionHistoricalReportV1
	if !localHNSWAttributionSHA256V1(cfg.Fixture.Checksum) || !localHNSWAttributionSHA256V1(cfg.TruthArtifactSHA256) {
		return evidence, errors.New("invalid local HNSW historical identity")
	}
	seen := map[string]bool{}
	for i, path := range cfg.HistoricalSearchReports {
		if path == "" || seen[path] {
			return evidence, errors.New("invalid local HNSW historical report paths")
		}
		seen[path] = true
		if !localHNSWAttributionSHA256V1(cfg.HistoricalReportSHA256[i]) {
			return evidence, errors.New("invalid local HNSW historical report identity")
		}
		if err := localHNSWAttributionMatchFileSHA256V1(path, m8ProfileArtifactMaxBytesV1, cfg.HistoricalReportSHA256[i]); err != nil {
			return evidence, err
		}
		report, err := loadLocalHNSWAttributionHistoricalReportV1(path)
		if err != nil {
			return evidence, err
		}
		probe2, probe16, err := validateLocalHNSWAttributionHistoricalReportV1(report, localHNSWAttributionHistoricalTopologySHA256V1[i], cfg.Fixture.Checksum, cfg.TruthArtifactSHA256)
		if err != nil {
			return evidence, err
		}
		evidence[i] = localHNSWAttributionHistoricalReportV1{Path: path, SHA256: cfg.HistoricalReportSHA256[i], TopologyIdentitySHA256: report.TopologyIdentitySHA256, Probe2: probe2, Probe16: probe16}
	}
	return evidence, nil
}

func loadLocalHNSWAttributionHistoricalReportV1(path string) (vectorPartitionSystemBenchResultV1, error) {
	f, err := os.Open(path)
	if err != nil {
		return vectorPartitionSystemBenchResultV1{}, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, m8ProfileArtifactMaxBytesV1+1))
	d.DisallowUnknownFields()
	var report vectorPartitionSystemBenchResultV1
	if err := d.Decode(&report); err != nil {
		return report, err
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return report, errors.New("local HNSW historical report trailing JSON")
	}
	return report, nil
}

func validateLocalHNSWAttributionHistoricalReportV1(report vectorPartitionSystemBenchResultV1, topologySHA256, fixtureChecksum, truthSHA256 string) (localHNSWAttributionHistoricalCellV1, localHNSWAttributionHistoricalCellV1, error) {
	var probe2, probe16 localHNSWAttributionHistoricalCellV1
	if report.SchemaVersion != 1 || report.ResultKind != "vector_partition_system_bench_v1" || report.Topology != "single_daemon_four_group" || report.TopologyIdentitySHA256 != topologySHA256 || report.DatasetChecksum != fixtureChecksum || report.TruthArtifactSHA256 != truthSHA256 || report.TopK != 10 || report.EfSearch != 128 {
		return probe2, probe16, fmt.Errorf("invalid local HNSW historical report identity: schema=%d kind=%q topology=%q topology_sha256=%q dataset=%q want_dataset=%q truth=%q want_truth=%q top_k=%d ef_search=%d", report.SchemaVersion, report.ResultKind, report.Topology, report.TopologyIdentitySHA256, report.DatasetChecksum, fixtureChecksum, report.TruthArtifactSHA256, truthSHA256, report.TopK, report.EfSearch)
	}
	for _, cell := range report.Cells {
		if cell.Concurrency != 1 || cell.Budget["probes"] == 0 {
			continue
		}
		var destination *localHNSWAttributionHistoricalCellV1
		var recall float64
		switch cell.Budget["probes"] {
		case 2:
			destination, recall = &probe2, .9247
		case 16:
			destination, recall = &probe16, .9265
		default:
			continue
		}
		if destination.Probes != 0 || cell.Status != "valid" || cell.Error != "" || cell.Metrics.Queries != 1000 || cell.Metrics.CompletedQueries != 1000 || cell.Metrics.ResultCount != 10000 || cell.Metrics.Errors != 0 || cell.Metrics.Timeouts != 0 || math.IsNaN(cell.Metrics.RecallAt10) || math.Abs(cell.Metrics.RecallAt10-recall) > 1e-12 || math.IsNaN(cell.Metrics.QPS) || math.IsInf(cell.Metrics.QPS, 0) || cell.Metrics.QPS <= 0 || cell.Metrics.P50Nanos == 0 || cell.Metrics.P50Nanos > cell.Metrics.P95Nanos || cell.Metrics.P95Nanos > cell.Metrics.P99Nanos || cell.Counters["candidates"] == 0 || cell.Counters["edges"] == 0 {
			return probe2, probe16, fmt.Errorf("invalid local HNSW historical cell: probes=%d status=%q queries=%d completed=%d recall=%g qps=%g", cell.Budget["probes"], cell.Status, cell.Metrics.Queries, cell.Metrics.CompletedQueries, cell.Metrics.RecallAt10, cell.Metrics.QPS)
		}
		*destination = localHNSWAttributionHistoricalCellV1{Probes: cell.Budget["probes"], Metrics: cell.Metrics, Candidates: cell.Counters["candidates"], Edges: cell.Counters["edges"]}
	}
	if probe2.Probes != 2 || probe16.Probes != 16 {
		return probe2, probe16, errors.New("incomplete local HNSW historical cells")
	}
	return probe2, probe16, nil
}
