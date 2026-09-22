package jsonbenchcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalJSONBenchAllowsRequestedAndValidRowsToDiffer(t *testing.T) {
	manifest := validManifest(t)
	manifest.Pins.Dataset.RequestedRows = 1_000_006
	manifest.Pins.Dataset.ValidRows = 1_000_000
	for index, resultPath := range manifest.TreeDB.ResultPaths {
		rows := validTreeDBRows()
		for _, row := range rows {
			row["requested_rows"] = int64(1_000_006)
			row["dataset_size"] = int64(1_000_000)
			row["attempts_seconds"] = []float64{0.001 + float64(index)*0.0001}
		}
		writeJSONFile(t, resultPath, map[string]any{
			"schema_version": TreeDBResultSchemaVersion,
			"rows":           rows,
		})

		clickHouse := validClickHouseResult(1_000_000)
		clickHouse["requested_rows"] = int64(1_000_006)
		for _, timings := range clickHouse["result"].([][]float64) {
			timings[0] += float64(index) * 0.0001
		}
		writeJSONFile(t, manifest.ClickHouse.ResultPaths[index], clickHouse)
	}

	if err := Validate(manifest, manifest.ArtifactRoot); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCanonicalJSONBenchRejectsRowCountMismatches(t *testing.T) {
	tests := []struct {
		name string
		edit func(Manifest)
		want string
	}{
		{
			name: "treedb requested",
			edit: func(manifest Manifest) {
				rewriteFirstTreeDBRowCounts(t, manifest, 999_999, 1_000_000)
			},
			want: "requested_rows 999999 does not match pinned requested_rows 1000000",
		},
		{
			name: "treedb valid",
			edit: func(manifest Manifest) {
				rewriteFirstTreeDBRowCounts(t, manifest, 1_000_000, 999_999)
			},
			want: "dataset_size 999999 does not match pinned valid_rows 1000000",
		},
		{
			name: "clickhouse requested",
			edit: func(manifest Manifest) {
				result := validClickHouseResult(1_000_000)
				result["requested_rows"] = int64(999_999)
				writeJSONFile(t, manifest.ClickHouse.ResultPaths[0], result)
			},
			want: "requested_rows 999999 does not match pinned requested_rows 1000000",
		},
		{
			name: "clickhouse valid",
			edit: func(manifest Manifest) {
				result := validClickHouseResult(1_000_000)
				result["dataset_size"] = int64(999_999)
				writeJSONFile(t, manifest.ClickHouse.ResultPaths[0], result)
			},
			want: "dataset_size 999999 does not match pinned valid_rows 1000000",
		},
		{
			name: "clickhouse loaded",
			edit: func(manifest Manifest) {
				result := validClickHouseResult(1_000_000)
				result["num_loaded_documents"] = int64(999_999)
				writeJSONFile(t, manifest.ClickHouse.ResultPaths[0], result)
			},
			want: "num_loaded_documents 999999 does not match pinned valid_rows 1000000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := validManifest(t)
			test.edit(manifest)
			assertErrorContains(t, Validate(manifest, manifest.ArtifactRoot), test.want)
		})
	}
}

func TestCanonicalJSONBenchNoSkipDatasetUsesEqualRequestedAndValidRows(t *testing.T) {
	manifest := validManifest(t)
	if manifest.Pins.Dataset.RequestedRows != manifest.Pins.Dataset.ValidRows {
		t.Fatalf("no-skip fixture requested_rows=%d valid_rows=%d",
			manifest.Pins.Dataset.RequestedRows, manifest.Pins.Dataset.ValidRows)
	}
	if err := Validate(manifest, manifest.ArtifactRoot); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCanonicalJSONBenchStrictlyRejectsMisspelledRowCountPin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	data := strings.ReplaceAll(
		`{"schema_version":"`+SchemaVersion+`","pins":{"dataset":{"requested_rows":1,"valid_rows":1}}}`,
		`"valid_rows"`,
		`"vaild_rows"`,
	)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path); err == nil || !strings.Contains(err.Error(), `unknown field "vaild_rows"`) {
		t.Fatalf("LoadManifest() error = %v, want unknown row-count field", err)
	}
}

func rewriteFirstTreeDBRowCounts(t *testing.T, manifest Manifest, requested, valid int64) {
	t.Helper()
	rows := validTreeDBRows()
	rows[0]["requested_rows"] = requested
	rows[0]["dataset_size"] = valid
	writeJSONFile(t, manifest.TreeDB.ResultPaths[0], map[string]any{
		"schema_version": TreeDBResultSchemaVersion,
		"rows":           rows,
	})
}
