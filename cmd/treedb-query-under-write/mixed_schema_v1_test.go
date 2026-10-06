package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestMixedSchemaAndDocumentationDriftV1(t *testing.T) {
	raw, err := os.ReadFile("mixed-report-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err = json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]any)
	report := properties["Report"].(map[string]any)
	encoded, err := json.Marshal(mixedReport{windowReport: windowReport{Version: 1, Kind: "fixed_cluster_mixed_window_v1"}})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range report["required"].([]any) {
		if _, ok := fields[name.(string)]; !ok {
			t.Fatalf("schema required missing report field%s", name)
		}
	}

	reportProperties := report["properties"].(map[string]any)
	originals := reportProperties["Originals"].(map[string]any)
	if originals["minimum"].(float64) != 6 || originals["maximum"].(float64) != 63 {
		t.Fatal("original count schema drift")
	}
	if reportProperties["Prefixes"].(map[string]any)["maxItems"].(float64) != 64 {
		t.Fatal("prefix count schema drift")
	}
	prefixProperties := reportProperties["ReadPrefixes"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	// Read the integer limit exactly; decoding JSON numbers as float64 loses bit63 precision.
	var exact map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&exact); err != nil {
		t.Fatal(err)
	}
	exactReport := exact["properties"].(map[string]any)["Report"].(map[string]any)["properties"].(map[string]any)
	exactPrefixes := exactReport["ReadPrefixes"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	if exactPrefixes["CompatibleMask"].(map[string]any)["maximum"].(json.Number).String() != "18446744073709551615" {
		t.Fatal("uint64 schema bound drift")
	}

	for _, name := range []string{"Lower", "Upper", "Matched"} {
		if prefixProperties[name].(map[string]any)["maximum"].(float64) != 63 {
			t.Fatal("prefix ordinal schema drift")
		}
	}
	if _, present := fields["Originals"]; present {
		t.Fatal("legacy report added optional original count")
	}

	audit := schema["$defs"].(map[string]any)["auditPlan"].(map[string]any)
	if int(audit["x-maxEncodedBytes"].(float64)) != nativewire.ColocatedAuditPlanMaxBytesV1 {
		t.Fatal("audit encoded bound drift")
	}
	writes := audit["properties"].(map[string]any)["Writes"].(map[string]any)
	if writes["minItems"].(float64) != 0 || writes["maxItems"].(float64) != 63 {
		t.Fatal("bounded outcome schema drift")
	}
	if len(audit["anyOf"].([]any)) != 2 {
		t.Fatal("outcome/population-only schema drift")
	}
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"-mode mixed-window", "-mixed-interval", "-colocated-audit-plan", "ReadPrefixes", "HighestNewCommitIndex", "RequiredAppliedIndex", "524288", "capability-absence", "not an added recall=1 gate"} {
		if !strings.Contains(string(readme), text) {
			t.Fatalf("missing contract documentation%q", text)
		}
	}
}

func TestMixedChangingProfileSchemaV2(t *testing.T) {
	raw, err := os.ReadFile("mixed-report-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	report := schema["properties"].(map[string]any)["Report"].(map[string]any)["properties"].(map[string]any)
	if report["Profile"].(map[string]any)["const"] != mixedProfileChangingTop10 {
		t.Fatal("profile schema drift")
	}
	var rejected bytes.Buffer
	err = runArgs(context.Background(), []string{"-mode", "mixed-window", "-mixed-profile", "invalid"}, &rejected)
	if err == nil {
		t.Fatal("invalid profile accepted")
	}
	var failure struct {
		Event  string
		Report map[string]json.RawMessage
	}
	if err := json.Unmarshal(rejected.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Event != "result" || string(failure.Report["Verdict"]) != `"FAILED"` || len(failure.Report["Error"]) == 0 {
		t.Fatal("configuration failure evidence missing")
	}
	if _, present := failure.Report["Profile"]; present {
		t.Fatal("rejected profile violates the report schema")
	}
	prefix := report["ReadPrefixes"].(map[string]any)["items"].(map[string]any)
	encoded, err := json.Marshal(mixedReadPrefix{CompatibleMask: 3, RecallAt10: .9})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range prefix["required"].([]any) {
		if _, ok := fields[name.(string)]; !ok {
			t.Fatalf("missing prefix field%s", name)
		}
	}
	defs := schema["$defs"].(map[string]any)
	audit := defs["auditPlan"].(map[string]any)
	if audit["properties"].(map[string]any)["Population"].(map[string]any)["$ref"] != "#/$defs/populationExpectation" {
		t.Fatal("optional population attachment drift")
	}
	for _, field := range audit["required"].([]any) {
		if field == "Population" {
			t.Fatal("population became required for known-ID audit")
		}
	}
	limits := defs["populationExpectation"].(map[string]any)["properties"].(map[string]any)["Limits"].(map[string]any)["properties"].(map[string]any)
	if defs["populationExpectation"].(map[string]any)["properties"].(map[string]any)["Rows"].(map[string]any)["minimum"].(float64) != 0 {
		t.Fatal("empty population schema drift")
	}
	for _, name := range []string{"MaxRows", "MaxIDBytes", "MaxSourceRecordBytes", "MaxTotalBytes", "MaxInspected"} {
		if limits[name] == nil {
			t.Fatalf("missing population limit%s", name)
		}
	}
	proof := defs["populationProof"].(map[string]any)["properties"].(map[string]any)
	if proof["Encoding"].(map[string]any)["const"] != "id-le32-fp32-le-v1" {
		t.Fatal("population encoding drift")
	}
	for _, name := range []string{"SourceRecordBytes", "AssetBytes", "TotalBytes", "HashedBytes", "Inspected"} {
		if proof[name] == nil {
			t.Fatalf("missing population accounting%s", name)
		}
	}
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"-mixed-profile changing-top10", "CompatibleMask", "minimum", "OracleBasis", "BenchmarkMixedPrefixValidationGuardV1", "MaxSourceRecordBytes"} {
		if !strings.Contains(string(readme), text) {
			t.Fatalf("missing changing profile documentation%q", text)
		}
	}
}
