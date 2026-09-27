package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestM8ServingResourceReceiptFailClosedV1(t *testing.T) {
	row := m8CompleteMeasurementGoldenV1(t)
	row.Probes, row.EfSearch, row.VariantID = 1, 96, "variant"
	for i := 90; i < len(row.Accounting.Attempts); i++ {
		row.Accounting.Attempts[i] = row.Accounting.Attempts[0]
	}
	if err := m8SummarizeAttemptsV1(&row, 10); err != nil {
		t.Fatal(err)
	}
	parent := m8ProductionReportV1{ExecutionID: "parent", Rows: []m8ProductionRowV1{row}, Config: m8ProductionConfigEvidenceV1{Probes: []int{1}, EfSearch: []int{96}, Concurrency: []int{1}, Overlap: []float64{0}, TopK: 10, MeasuredRepetitions: 1, MeasurementAccounting: m8CompleteAttemptsV1, RecallTarget: .9}}
	outcome := m8ProductionRowOutcomeIdentityV1(row)
	for _, attempt := range row.Accounting.Attempts {
		outcome.TopKIDs = append(outcome.TopKIDs, []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
		outcome.TopKScoreBits = append(outcome.TopKScoreBits, make([]uint32, 10))
		outcome.TotalNanos = append(outcome.TotalNanos, attempt.CoordinatorNanos)
	}
	transcript := m8ProductionMeasurementTranscriptV1{Outcomes: []m8ProductionRowOutcomesV1{outcome}}
	header := m8ServingResourceHeaderV1{Kind: "header", Contract: m8ServingResourceContractV1, ParentExecutionID: parent.ExecutionID, Config: parent.Config, Cells: 1, HeadSHA: strings.Repeat("a", 40), ExecutableSHA256: strings.Repeat("b", 64)}
	cell := m8ServingResourceCellV1{Kind: "cell", Row: row, Outcomes: outcome, Resources: m8ServingResourcesV1{Before: m8ProcessResourceSnapshotV1{CPUAvailable: true, SnapshotNanos: 1}, After: m8ProcessResourceSnapshotV1{CPUAvailable: true, SnapshotNanos: 1}, WorkerWallNanos: row.ElapsedNanos}}
	footer := m8ServingResourceFooterV1{Kind: "complete", Cells: 1}
	capture, err := startM8ProfileCaptureV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	paths, err := capture.Stop()
	if err != nil {
		t.Fatal(err)
	}
	footer.Profiles, err = m8ProfileArtifactsV1(paths)
	if err != nil {
		t.Fatal(err)
	}
	header.ProfileDirectory = capture.dir
	encode := func(values ...any) []byte {
		t.Helper()
		var out bytes.Buffer
		for _, value := range values {
			if err := json.NewEncoder(&out).Encode(value); err != nil {
				t.Fatal(err)
			}
		}
		return out.Bytes()
	}
	raw := encode(header, cell, footer)
	got, err := m8ReadServingResourceCellsV1(bytes.NewReader(raw), header, parent, transcript)
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], cell) {
		t.Fatalf("valid receipt: cells=%d err=%v", len(got), err)
	}
	for name, raw := range map[string][]byte{
		"missing footer":   encode(header, cell),
		"missing cell":     encode(header, footer),
		"duplicate cell":   encode(header, cell, cell, footer),
		"duplicate footer": encode(header, cell, footer, footer),
		"trailing bytes":   append(encode(header, cell, footer), '!'),
		"wrong source":     bytes.Replace(raw, []byte(header.HeadSHA), []byte(strings.Repeat("d", 40)), 1),
		"wrong executable": bytes.Replace(raw, []byte(header.ExecutableSHA256), []byte(strings.Repeat("e", 64)), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(raw), header, parent, transcript); err == nil {
				t.Fatal("accepted incomplete or unbound resource receipt")
			}
		})
	}
	for name, mutate := range map[string]func(*m8ServingResourceCellV1){
		"coordinate":      func(c *m8ServingResourceCellV1) { c.Row.Probes++ },
		"population":      func(c *m8ServingResourceCellV1) { c.Row.Samples-- },
		"result id":       func(c *m8ServingResourceCellV1) { c.Outcomes.TopKIDs[0][0] = "other" },
		"score bits":      func(c *m8ServingResourceCellV1) { c.Outcomes.TopKScoreBits[0][0]++ },
		"failure":         func(c *m8ServingResourceCellV1) { c.Error = "retained failure" },
		"unavailable cpu": func(c *m8ServingResourceCellV1) { c.Resources.After.CPUAvailable = false },
		"wrong wall":      func(c *m8ServingResourceCellV1) { c.Resources.WorkerWallNanos++ },
	} {
		t.Run(name, func(t *testing.T) {
			var changed m8ServingResourceCellV1
			if err := json.Unmarshal(encode(cell), &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(encode(header, changed, footer)), header, parent, transcript); err == nil {
				t.Fatal("accepted altered cell")
			}
		})
	}
	for name, mutate := range map[string]func(*m8ServingResourceHeaderV1){
		"fixture":    func(h *m8ServingResourceHeaderV1) { h.Dataset.Vectors++ },
		"cell count": func(h *m8ServingResourceHeaderV1) { h.Cells++ },
		"execution":  func(h *m8ServingResourceHeaderV1) { h.ParentExecutionID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := header
			mutate(&changed)
			if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(encode(changed, cell, footer)), changed, parent, transcript); err == nil {
				t.Fatal("accepted header inconsistent with parent")
			}
		})
	}
	for _, status := range []string{"fail", "pass"} {
		t.Run("later repetition "+status, func(t *testing.T) {
			changed := parent
			changed.Config.MeasuredRepetitions = 2
			later := row
			later.Repetition, later.Status, later.RecallAtK = 1, status, .5
			changed.Rows = []m8ProductionRowV1{row, later}
			if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(raw), header, changed, transcript); err == nil {
				t.Fatal("accepted failed or low-quality later parent repetition")
			}
		})
	}
	t.Run("profile content changed", func(t *testing.T) {
		changed := footer
		changed.Profiles = append([]m8ProductionProfileArtifactV1(nil), footer.Profiles...)
		changed.Profiles[0].SHA256 = strings.Repeat("f", 64)
		if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(encode(header, cell, changed)), header, parent, transcript); err == nil {
			t.Fatal("accepted changed retained profile")
		}
	})
	t.Run("profile directory changed", func(t *testing.T) {
		changed := header
		changed.ProfileDirectory = t.TempDir()
		if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(encode(changed, cell, footer)), changed, parent, transcript); err == nil {
			t.Fatal("accepted profiles outside frozen directory")
		}
	})
	t.Run("oversized whitespace hides trailing data", func(t *testing.T) {
		input := io.MultiReader(bytes.NewReader(raw), io.LimitReader(m8ResourceTestSpacesV1{}, m8CompleteMeasurementMaxBytesV1), strings.NewReader("!"))
		if _, err := m8ReadServingResourceCellsV1(input, header, parent, transcript); err == nil {
			t.Fatal("accepted artificial EOF at receipt size limit")
		}
	})
	t.Run("profile missing", func(t *testing.T) {
		if err := os.Remove(paths[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := m8ReadServingResourceCellsV1(bytes.NewReader(raw), header, parent, transcript); err == nil {
			t.Fatal("accepted missing retained profile")
		}
	})
}

type m8ResourceTestSpacesV1 struct{}

func (m8ResourceTestSpacesV1) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	return len(p), nil
}

func TestM8ServingResourceAdmissionV1(t *testing.T) {
	for _, args := range [][]string{nil, {"-out", "relative"}, {"-out", "/tmp/no-receipt", "-profiles", "/tmp/no-profiles"}} {
		if err := runM8ServingResourcesV1(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted incomplete provenance: %v", args)
		}
	}
}
