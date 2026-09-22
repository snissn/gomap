package dgraphdurability

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestFixedMixedOperationCounts(t *testing.T) {
	want := []int{50_000, 100_000, 250_000, 500_000}
	if !reflect.DeepEqual(fixedMixedOperationCounts, want) {
		t.Fatalf("fixedMixedOperationCounts=%v want %v", fixedMixedOperationCounts, want)
	}
}

func TestConcurrentDurableMatrix(t *testing.T) {
	if !reflect.DeepEqual(concurrentDurableConcurrencies, []int{1, 2, 4, 8, 16}) {
		t.Fatalf("concurrencies=%v", concurrentDurableConcurrencies)
	}
	if !reflect.DeepEqual(concurrentDurableBatchSizes, []int{1, 16}) {
		t.Fatalf("batch sizes=%v", concurrentDurableBatchSizes)
	}
	if !reflect.DeepEqual(concurrentDurableValueSizes, []int{128, 4096}) {
		t.Fatalf("value sizes=%v", concurrentDurableValueSizes)
	}
}

func TestCounterSchemaIncludesAttributionBoundaries(t *testing.T) {
	want := map[string]bool{
		"iterator_snapshot_rotations/lookup":       false,
		"iterator_sources/lookup":                  false,
		"command_wal_syncs/write_commit":           false,
		"command_wal_group_acks/write_commit":      false,
		"command_wal_group_syncs/write_commit":     false,
		"command_wal_group_fallbacks/write_commit": false,
		"value_log_syncs/write_commit":             false,
		"checkpoints/write_commit":                 false,
	}
	for _, metric := range storeCounterMetrics {
		if _, ok := want[metric.name]; ok {
			want[metric.name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing attribution metric %q", name)
		}
	}
}

func TestRunnerRecordsWorkspaceIsolationForEveryCommand(t *testing.T) {
	script, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := bytes.Count(script, []byte("GOWORK=off")), 6; got != want {
		t.Fatalf("run.sh GOWORK=off occurrences=%d want %d (three recorded and three executed commands)", got, want)
	}
}
