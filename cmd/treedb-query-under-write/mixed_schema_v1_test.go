package main

import (
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
	audit := schema["$defs"].(map[string]any)["auditPlan"].(map[string]any)
	if int(audit["x-maxEncodedBytes"].(float64)) != nativewire.ColocatedAuditPlanMaxBytesV1 {
		t.Fatal("audit encoded bound drift")
	}
	writes := audit["properties"].(map[string]any)["Writes"].(map[string]any)
	if writes["minItems"].(float64) != 6 || writes["maxItems"].(float64) != 6 {
		t.Fatal("six outcome schema drift")
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
