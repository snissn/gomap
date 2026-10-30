//go:build ignore

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFailedOutcomesRetainedWithoutOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempt.json")
	cause := errors.New("readback failed after ACK")
	observed := []outcome{{ID: "user0001", Sequence: 1, Acknowledged: true, ReadbackOK: false, Error: cause.Error()}}
	id := identity{1, "source", "fixture", "command_wal_durable", "native", "json"}
	if err := persistOutcomes(path, id, "update_operations", observed, cause); !errors.Is(err, cause) {
		t.Fatalf("wanted original failure, got %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved snapshot
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Stage != "update_operations" || len(saved.Operations) != 1 || !saved.Operations[0].Acknowledged || saved.Operations[0].ReadbackOK || saved.Operations[0].Error != cause.Error() {
		t.Fatalf("lost failure/ACK: %+v", saved)
	}
	if err := persistOutcomes(path, id, "load_operations", nil, nil); err == nil {
		t.Fatal("reused attempt path accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(data) {
		t.Fatal("attempt was overwritten")
	}
}
