package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestM3FootprintDiagnosticsSurviveForcedGateFailureAndCleanup(t *testing.T) {
	retained := t.TempDir()
	t.Setenv(m3DiagnosticsDirEnv, retained)
	dir := filepath.Join(t.TempDir(), "db")
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	const reserveBytes int64 = 4199424
	// A deterministic physical-retention control, not an assertion about either
	// historical CI file set. Account every file regardless of its interpretation.
	reserve := filepath.Join(dir, "retained-reserve.control")
	file, err := os.Create(reserve)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(reserveBytes); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	measured, err := m3DirectoryBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := m3CaptureClosedFootprint(config{headSHA: "candidate", command: []string{"test-control"}}, database, dir, "forced_retention", .2, 2, measured)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range snapshot.Files {
		if file.Path == "retained-reserve.control" && file.Bytes == reserveBytes {
			found = true
		}
	}
	if !found {
		t.Fatalf("reserve omitted: %+v", snapshot.Files)
	}
	report := m3PartitionIndexReport{Rows: []m3PartitionIndexRow{{Ratio: 0, FinalDerivedPhysicalBytes: 48568}, {Ratio: .2, FinalDerivedPhysicalBytes: 52444 + reserveBytes, FinalFootprintDiagnostic: snapshot}}}
	report.ReplicationGate = m3ReplicationGate(report.Rows)
	if !strings.HasPrefix(report.ReplicationGate, "failed: 87.544638x exceeds 1.35x") {
		t.Fatal(report.ReplicationGate)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := m3RetainDiagnostic("report", raw); err != nil {
		t.Fatal(err)
	}
	if err := validateM3PartitionIndexReport(report); err == nil {
		t.Fatal("accepted forced invalid report")
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(retained)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("retained files=%d want=2", len(entries))
	}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(retained, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(raw) || !strings.Contains(string(raw), "retained-reserve.control") {
			t.Fatalf("lost diagnostic: %s", raw)
		}
	}
	if err := m3RetainDiagnostic("report", []byte(`{"attempt":2}`)); err != nil {
		t.Fatal(err)
	}
	entries, _ = os.ReadDir(retained)
	if len(entries) != 3 {
		t.Fatalf("attempt overwrite: %d", len(entries))
	}
}

func TestM3FootprintDiagnosticWriteFailureIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(m3DiagnosticsDirEnv, path)
	if err := m3RetainDiagnostic("report", []byte(`{}`)); err == nil {
		t.Fatal("silently ignored retention failure")
	}
}

func TestM3FootprintCaptureErrorKeepsObservationAndFailsStage(t *testing.T) {
	dir := t.TempDir()
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	measured, err := m3DirectoryBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocked, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(m3DiagnosticsDirEnv, blocked)
	snapshot, err := m3CaptureClosedFootprint(config{}, database, dir, "final", .2, 2, measured)
	if err == nil || snapshot.Error == "" || len(snapshot.Files) == 0 || snapshot.TotalBytes != measured {
		t.Fatalf("lost partial observation: %+v; %v", snapshot, err)
	}
	if err := m3FootprintDiagnosticErrors(m3PartitionIndexReport{Rows: []m3PartitionIndexRow{{Ratio: .2, FinalFootprintDiagnostic: snapshot}}}); err == nil {
		t.Fatal("capture failure silently accepted")
	}
	t.Setenv(m3DiagnosticsDirEnv, filepath.Join(dir, "evidence"))
	snapshot, err = m3CaptureClosedFootprint(config{}, database, dir, "final", .2, 2, measured)
	if err == nil || snapshot.Error == "" {
		t.Fatal("accepted evidence destination inside measured DB")
	}
	if _, err := os.Stat(filepath.Join(dir, "evidence")); !os.IsNotExist(err) {
		t.Fatalf("created diagnostic files in measured DB: %v", err)
	}
}
