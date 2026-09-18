package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestM8MeasurementDiagnosticProjectionBindsWithoutCopyingQueries(t *testing.T) {
	quality := &m8QualityAttributionV1{}
	// Deliberately large offline population: projection must not copy it into
	// the small measured-row transcript. It stays in the authoritative report.
	for i := 0; i < 1000; i++ {
		quality.Queries = append(quality.Queries, m8QualityQueryV1{QuerySHA256: strings.Repeat("a", 64)})
	}
	report := m8ProductionReportV1{Config: m8ProductionConfigEvidenceV1{QualityDiagnostics: true}, Rows: []m8ProductionRowV1{{Samples: 1000, Attribution: m8ProductionAttributionV1{Quality: quality}}}}
	raw, err := json.Marshal(report.Rows)
	if err != nil {
		t.Fatal(err)
	}
	rows, hashes, err := m8MeasurementRowsWithAttributionBindingsV1(report)
	if err != nil || len(hashes) != 1 || !m8SHA256V1(hashes[0]) {
		t.Fatal(hashes, err)
	}
	projected, err := json.Marshal(rows)
	if err != nil || len(raw) <= 64<<10 || len(projected) >= 16<<10 {
		t.Fatal("projection not bounded", len(raw), len(projected), err)
	}
	if report.Rows[0].Attribution.Quality != quality || len(quality.Queries) != 1000 || !reflect.DeepEqual(rows[0].Attribution, m8ProductionAttributionV1{}) {
		t.Fatal("mutated source or copied diagnostics")
	}
	_, again, err := m8MeasurementRowsWithAttributionBindingsV1(report)
	if err != nil || !reflect.DeepEqual(hashes, again) {
		t.Fatal("nondeterministic attribution binding")
	}
	quality.Queries[999].QuerySHA256 = strings.Repeat("b", 64)
	_, changed, err := m8MeasurementRowsWithAttributionBindingsV1(report)
	if err != nil || reflect.DeepEqual(hashes, changed) {
		t.Fatal("ignored tail of query population")
	}
	report.Config.QualityDiagnostics = false
	legacy, hashes, err := m8MeasurementRowsWithAttributionBindingsV1(report)
	if err != nil || hashes != nil || !reflect.DeepEqual(legacy, report.Rows) {
		t.Fatal("ordinary transcript changed")
	}
}
