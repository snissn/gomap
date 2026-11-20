package main

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

const (
	legacyShapeBenchmark = "BenchmarkCollectionShapeInsertBatch"
	nativeShapeBenchmark = "BenchmarkCollectionShapeInsertBatchNativeCOW"
	legacyBackendOwner   = "legacy_backend"
	nativeCOWOwner       = "shared_native_cow_btree"
	companionProfile     = "command_wal_relaxed"
	companionStorage     = "data_outer=true,index_outer=true"
)

type benchmarkSample struct {
	Name        string             `json:"name"`
	Iterations  int                `json:"iterations"`
	NsPerOp     float64            `json:"ns_per_op"`
	BytesPerOp  *float64           `json:"bytes_per_op"`
	AllocsPerOp *float64           `json:"allocs_per_op"`
	Metrics     map[string]float64 `json:"metrics"`
}

func validateNativeCOWConfig(cfg config) error {
	switch {
	case cfg.SkipTimedMatrix:
		return errors.New("-native-cow-companion requires the timed matrix")
	case cfg.TreeEngine != companionProfile || cfg.Profile != companionProfile:
		return errors.New("-native-cow-companion requires command_wal_relaxed for -tree-engine and -profile")
	case cfg.StorageCells != "index-vlog":
		return errors.New("-native-cow-companion requires the canonical index-vlog storage cell")
	case cfg.Benchtime != "" && cfg.Benchtime != strconv.Itoa(cfg.Docs)+"x":
		return errors.New("-native-cow-companion requires a fixed -benchtime of <docs>x")
	}
	formats := canonicalFormatList(splitCSV(cfg.Formats))
	if len(formats) == 0 {
		return errors.New("-native-cow-companion requires at least one format")
	}
	for _, format := range formats {
		if !supportsNativeCOWShape(format, cfg.Indexes) {
			return fmt.Errorf("-native-cow-companion supports BSON/JSON with zero secondary indexes and BSON with two; got %s with %d", format, cfg.Indexes)
		}
	}
	return nil
}

func supportsNativeCOWShape(format string, indexes int) bool {
	return indexes == 0 && (format == "json" || format == "bson") || indexes == 2 && format == "bson"
}

func addNativeCOWTimedRow(canon *canonicalRun, report collectionReport, bench reportBenchmark, path string, cfg config) error {
	if err := validateCompanionBenchmark(report, bench, cfg, true); err != nil {
		return err
	}
	total := int64(bench.MeanMetrics["disk_total_bytes"])
	format := canonicalFormat(report.DocumentFormat)
	row := resultRow{
		ConfigName:      canonicalConfigName("treedb_native_cow", format, "collection", cfg.Indexes),
		Engine:          report.BenchmarkEngine,
		Format:          format,
		Shape:           "collection",
		IndexCount:      cfg.Indexes,
		DocumentCount:   cfg.Docs,
		BenchmarkName:   bench.Name,
		Phase:           phasePostInsert,
		MaintenanceMode: "none",
		TotalBytes:      int64Ptr(total),
		BytesPerDoc:     floatPtr(bytesPerDoc(total, cfg.Docs)),
		DocsPerSec:      floatPtr(bench.OpsPerSec),
		NsPerDoc:        floatPtr(bench.MeanNSPerOp),
		BatchSize:       cfg.BatchSize,
		BenchmarkTimed:  true,
		MeasurementKind: "go_benchmark",
		MeasurementNote: "InsertBatch through native COW publication. Checked public checkpoint and primary-key census follow the timer.",
		SourceArtifact:  path,
		Owner:           nativeCOWOwner,
		BytesPerOp:      bench.MeanBytesPerOp,
		AllocsPerOp:     bench.MeanAllocsPerOp,
		Samples:         bench.Runs,
	}
	canon.Results = append(canon.Results, row)
	return nil
}

// Check each raw sample because averaging metrics can hide missing or failed
// route evidence when the report contains more than one sample.
func validateCompanionBenchmark(report collectionReport, bench reportBenchmark, cfg config, native bool) error {
	format := canonicalFormat(report.DocumentFormat)
	if report.Status != "ok" || report.BenchmarkEngine != companionProfile || report.StoragePolicy != companionStorage ||
		!slices.Contains(canonicalFormatList(splitCSV(cfg.Formats)), format) ||
		!supportsNativeCOWShape(format, cfg.Indexes) || report.CollectionBatchSize != cfg.BatchSize || report.Count != cfg.Count {
		return fmt.Errorf("%s: benchmark report has incompatible invocation metadata", bench.Name)
	}
	want := legacyShapeBenchmark
	if native {
		want = nativeShapeBenchmark
	}
	want += fmt.Sprintf("/indexes_%d", cfg.Indexes)
	if bench.Name != want || len(bench.Runs) != cfg.Count {
		return fmt.Errorf("%s: expected %s with %d samples, got %d samples", bench.Name, want, cfg.Count, len(bench.Runs))
	}
	if !positiveFinite(bench.MeanNSPerOp) || !positiveFinite(bench.OpsPerSec) ||
		!finiteAllocation(bench.MeanBytesPerOp) || !finiteAllocation(bench.MeanAllocsPerOp) ||
		!positiveFinite(bench.MeanMetrics["disk_total_bytes"]) {
		return fmt.Errorf("%s: missing or invalid timing, allocation or size summary", bench.Name)
	}

	for i, sample := range bench.Runs {
		if err := validateCompanionSample(report, sample, cfg, want, native); err != nil {
			return fmt.Errorf("%s sample %d: %w", bench.Name, i+1, err)
		}
	}
	return nil
}

func validateCompanionSample(report collectionReport, sample benchmarkSample, cfg config, name string, native bool) error {
	if sample.Name != name || sample.Iterations != cfg.Docs || !positiveFinite(sample.NsPerOp) ||
		!finiteAllocation(sample.BytesPerOp) || !finiteAllocation(sample.AllocsPerOp) {
		return errors.New("missing or invalid benchmark name, iteration count, timing or allocations")
	}
	metrics := sample.Metrics
	for _, metric := range []struct {
		name string
		want int
	}{
		{"indexes/doc", cfg.Indexes}, {"stored_docs", cfg.Docs}, {"verified_shape_docs", cfg.Docs}, {"target_docs/batch", cfg.BatchSize},
	} {
		if got, ok := metrics[metric.name]; !ok || got != float64(metric.want) {
			return fmt.Errorf("%s missing or mismatched, got %g, want %d", metric.name, got, metric.want)
		}
	}
	if !positiveFinite(metrics["checkpoint_ns/op"]) || !positiveFinite(metrics["disk_total_bytes"]) {
		return errors.New("missing checked post-timer checkpoint or disk usage")
	}
	if native {
		return validateNativeCOWProof(metrics, cfg)
	}

	const route = "ordered_root_route_command_wal_publish_"
	ev := productionEvidenceFromTreeDBTimedReport(report, metrics, cfg)
	if metrics["legacy_backend_owner"] != 1 || metrics["native_cow_owner"] != 0 ||
		!positiveFinite(metrics[route+"candidate_ops"]) || !positiveFinite(metrics[route+"used_ops"]) ||
		!hasRowProductionEvidence(resultRow{IndexCount: cfg.Indexes, ProductionEvidence: ev}) {
		return errors.New("missing legacy backend owner or command_wal_publish producer proof")
	}
	return nil
}

func validateNativeCOWProof(metrics map[string]float64, cfg config) error {
	batches := 1 + (cfg.Docs-1)/cfg.BatchSize
	for _, metric := range []struct {
		name string
		want int
	}{
		{"native_cow_owner", 1}, {"native_acknowledged_docs", cfg.Docs}, {"native_insert_batches", batches},
		{"cow_insert_publication_delta", batches}, {"native_checkpoint_calls_during_insert", 0},
		{"native_checkpoint_calls", 1}, {"native_checkpoint_errors", 0},
	} {
		if got, ok := metrics[metric.name]; !ok || got != float64(metric.want) {
			return fmt.Errorf("native COW proof %s missing or invalid, got %g, want %d", metric.name, got, metric.want)
		}
	}
	if metrics["legacy_backend_owner"] != 0 || metrics["ordered_root_route_command_wal_publish_used_ops"] != 0 {
		return errors.New("native COW sample contains legacy producer evidence")
	}
	for _, key := range []string{
		"cow_publications_before_insert", "cow_publications_after_insert",
		"native_accepted_lsn_before_insert", "native_accepted_lsn_after_insert", "native_checkpoint_applied_lsn",
	} {
		v, ok := metrics[key]
		if !ok || v < 0 || v >= 1<<53 || math.Trunc(v) != v {
			return fmt.Errorf("native COW proof %s is missing or invalid", key)
		}
	}
	if metrics["cow_publications_after_insert"]-metrics["cow_publications_before_insert"] != float64(batches) ||
		metrics["native_accepted_lsn_after_insert"] <= metrics["native_accepted_lsn_before_insert"] ||
		metrics["native_checkpoint_applied_lsn"] < metrics["native_accepted_lsn_after_insert"] {
		return errors.New("native COW publication delta or checkpoint LSN coverage is inconsistent")
	}
	return nil
}

func positiveFinite(v float64) bool {
	return v > 0 && !math.IsInf(v, 0)
}

func finiteAllocation(v *float64) bool {
	return v != nil && *v >= 0 && !math.IsInf(*v, 0)
}

func validateCompanionPairs(canon *canonicalRun) []guardrailCheck {
	var checks []guardrailCheck
	for _, format := range canon.Config.Formats {
		for _, owner := range []struct{ owner, configTag string }{
			{legacyBackendOwner, "treedb"},
			{nativeCOWOwner, "treedb_native_cow"},
		} {
			name := canonicalConfigName(owner.configTag, format, "collection", canon.Config.Indexes)
			count := 0
			for _, row := range canon.Results {
				if row.ConfigName == name && row.Owner == owner.owner && row.Phase == phasePostInsert {
					count++
				}
			}
			if count != 1 {
				checks = append(checks, guardrailCheck{
					Severity: "error",
					Code:     "native_cow_companion.pair_required",
					Message:  fmt.Sprintf("%s requires exactly one qualified row for %s, found %d", format, owner.owner, count),
				})
			}
		}
	}
	return checks
}

func writeNativeCOWCompanion(sb *strings.Builder, canon *canonicalRun) {
	sb.WriteString("## Matched owner samples\n\n")
	sb.WriteString("The timer covers `InsertBatch` publication and the legacy final flush when buffered. The checked checkpoint and primary-key census follow the timer. One benchmark operation is one document.\n\n")
	sb.WriteString("| Config | Owner | Sample | ns/doc | B/op | allocs/op | Checkpoint ns | COW insert publications |\n")
	sb.WriteString("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range canon.Results {
		if row.Phase != phasePostInsert || row.Owner == "" {
			continue
		}
		for i, sample := range row.Samples {
			fmt.Fprintf(sb, "| `%s` | `%s` | %d | %.1f | %s | %s | %.0f | %s |\n",
				row.ConfigName, row.Owner, i+1, sample.NsPerOp, formatFloatPtr(sample.BytesPerOp, 0),
				formatFloatPtr(sample.AllocsPerOp, 0), sample.Metrics["checkpoint_ns/op"],
				formatFloatPtr(floatMetricPtr(sample.Metrics, "cow_insert_publication_delta"), 0))
		}
	}
	sb.WriteString("\n")
}
