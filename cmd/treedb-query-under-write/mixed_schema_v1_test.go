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
