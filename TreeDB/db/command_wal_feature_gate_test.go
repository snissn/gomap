package db

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALFeatureGateRequiresCleanLegacyWALBeforeActivation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	w, err := commitlog.NewWriter(filepath.Join(walDir, "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendBatch([]commitlog.Record{{Op: commitlog.OpSetInline, Key: []byte("k"), Value: []byte("v"), Seq: 1}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ValidateCommandWALActivationClean(dir); err == nil || !strings.Contains(err.Error(), "clean legacy WAL") {
		t.Fatalf("ValidateCommandWALActivationClean error=%v, want clean legacy WAL failure", err)
	}
}

func TestCommandWALRequiredFeatureFailsClosedUntilExecutionEnabled(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("Open error=%v, want ErrCommandWALUnsupported", err)
	}
}

func TestLoadFormatConfigRejectsUnknownRequiredFeature(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{"future_feature"}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	_, _, err := LoadFormatConfig(dir)
	if !errors.Is(err, ErrUnsupportedRequiredFeature) {
		t.Fatalf("LoadFormatConfig error=%v, want ErrUnsupportedRequiredFeature", err)
	}
}

func TestLoadFormatConfigRejectsRequiredFeatureInVersion2File(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "format.json"), []byte(`{"version":2,"required_features":["command_wal_v1"]}`), 0o600); err != nil {
		t.Fatalf("write format.json: %v", err)
	}
	_, _, err := LoadFormatConfig(dir)
	if err == nil || !strings.Contains(err.Error(), "required_features require format version 3") {
		t.Fatalf("LoadFormatConfig error=%v, want required_features version failure", err)
	}
}
