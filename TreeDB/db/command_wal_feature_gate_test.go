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

func TestCommandWALFeatureGateRequiresCleanLegacyCollectionWALBeforeActivation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "collection-l0-000001.log"), []byte("dirty collection wal"), 0o600); err != nil {
		t.Fatalf("write collection wal: %v", err)
	}
	err := ValidateCommandWALActivationClean(dir)
	if !errors.Is(err, ErrRecoveryRequired) || !strings.Contains(err.Error(), "clean legacy collection WAL") {
		t.Fatalf("ValidateCommandWALActivationClean error=%v, want clean legacy collection WAL recovery failure", err)
	}
}

func TestCommandWALFeatureGateRequiresCleanLegacyValueWALBeforeActivation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "value-l0-000001.log"), []byte("dirty value wal"), 0o600); err != nil {
		t.Fatalf("write value wal: %v", err)
	}
	if err := ValidateCommandWALActivationClean(dir); err == nil || !strings.Contains(err.Error(), "value-l0-000001.log") {
		t.Fatalf("ValidateCommandWALActivationClean error=%v, want dirty value WAL failure", err)
	}
}

func TestSaveFormatConfigCommandWALRequiresCleanActivation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l0-000001.log"), []byte("dirty commit wal"), 0o600); err != nil {
		t.Fatalf("write commit wal: %v", err)
	}
	err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}})
	if err == nil || !strings.Contains(err.Error(), "clean legacy WAL") {
		t.Fatalf("SaveFormatConfig error=%v, want clean legacy WAL failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "format.json")); !os.IsNotExist(statErr) {
		t.Fatalf("format.json stat error=%v, want not exist after rejected activation", statErr)
	}
}

func TestSaveOpenFormatConfigPreservesActiveCommandWALFeature(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("k"), Value: []byte("v")},
	})
	if err := saveOpenFormatConfig(Options{Dir: dir, CommandWAL: true}); err != nil {
		t.Fatalf("saveOpenFormatConfig: %v", err)
	}
}

func TestCommandWALOptionPersistsFeatureBeforeJournalActivation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true})
	if err != nil {
		t.Fatalf("Open CommandWAL: %v", err)
	}
	requiresCommandWAL, err := CommandWALRequiredFeatureEnabled(dir)
	if err != nil {
		t.Fatalf("CommandWALRequiredFeatureEnabled: %v", err)
	}
	if !requiresCommandWAL {
		t.Fatalf("command_wal_v1 required feature was not persisted before journal activation")
	}
	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open persisted CommandWAL: %v", err)
	}
	if !reopened.commandWAL {
		t.Fatalf("reopened commandWAL=false, want true from persisted feature")
	}
	assertDBValue(t, reopened, "k", "v")
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}
}

func TestCommandWALOptionRejectsDirtyLegacyWALActivation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l0-000001.log"), []byte("dirty commit wal"), 0o600); err != nil {
		t.Fatalf("write commit wal: %v", err)
	}
	_, err := Open(Options{Dir: dir, CommandWAL: true})
	if err == nil || !strings.Contains(err.Error(), "clean legacy WAL") {
		t.Fatalf("Open CommandWAL dirty legacy WAL error=%v, want clean legacy WAL failure", err)
	}
	if _, statErr := os.Stat(formatConfigPath(dir)); !os.IsNotExist(statErr) {
		t.Fatalf("format.json stat error=%v, want not exist after rejected activation", statErr)
	}
}

func TestCommandWALRequiredFeatureEnablesBackendCommandWAL(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !db.commandWAL {
		t.Fatalf("commandWAL=false, want true")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Open(Options{Dir: dir, IgnoreFormatConfig: true})
	if err != nil {
		t.Fatalf("Open IgnoreFormatConfig: %v", err)
	}
	if !db.commandWAL {
		t.Fatalf("IgnoreFormatConfig commandWAL=false, want true from required-feature gate")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close IgnoreFormatConfig: %v", err)
	}
}

func TestCommandWALRequiredFeatureFailsClosedForOfflineMaintenance(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("VacuumIndexOffline error=%v, want ErrCommandWALUnsupported", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir, IgnoreFormatConfig: true}); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("VacuumIndexOffline IgnoreFormatConfig error=%v, want ErrCommandWALUnsupported", err)
	}
	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("ValueLogRewriteOffline error=%v, want ErrCommandWALUnsupported", err)
	}
	if _, err := ValueLogRewriteOffline(Options{Dir: dir, IgnoreFormatConfig: true}); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("ValueLogRewriteOffline IgnoreFormatConfig error=%v, want ErrCommandWALUnsupported", err)
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

func TestSaveFormatConfigNormalizesRequiredFeaturesToVersion3(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{Version: formatConfigVersion, RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	cfg, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatalf("expected format config")
	}
	if cfg.Version != formatConfigRequiredFeaturesVersion {
		t.Fatalf("version=%d, want %d", cfg.Version, formatConfigRequiredFeaturesVersion)
	}
}

func TestValidateFormatRequiredFeatureGateAllowsIgnoredOrdinaryFormatErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
	}{
		{name: "malformed", json: `{"version":`},
		{name: "future version without required features", json: `{"version":999}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "format.json"), []byte(tc.json), 0o600); err != nil {
				t.Fatalf("write format.json: %v", err)
			}
			if err := ValidateFormatRequiredFeatureGate(dir); err != nil {
				t.Fatalf("ValidateFormatRequiredFeatureGate: %v", err)
			}
			db, err := Open(Options{Dir: dir, IgnoreFormatConfig: true})
			if err != nil {
				t.Fatalf("Open IgnoreFormatConfig: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func TestValidateFormatRequiredFeatureGateRejectsMalformedRequiredFeatureFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "format.json"), []byte(`{"version":3,"required_features":["command_wal_v1"`), 0o600); err != nil {
		t.Fatalf("write format.json: %v", err)
	}
	err := ValidateFormatRequiredFeatureGate(dir)
	if err == nil || !strings.Contains(err.Error(), "required-feature gate") {
		t.Fatalf("ValidateFormatRequiredFeatureGate error=%v, want required-feature decode failure", err)
	}
	_, err = Open(Options{Dir: dir, IgnoreFormatConfig: true})
	if err == nil || !strings.Contains(err.Error(), "required-feature gate") {
		t.Fatalf("Open IgnoreFormatConfig error=%v, want required-feature decode failure", err)
	}
}

func TestValidateFormatRequiredFeatureGateRejectsRequiredFeatureWithIgnore(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "format.json"), []byte(`{"version":3,"required_features":["future_feature"]}`), 0o600); err != nil {
		t.Fatalf("write format.json: %v", err)
	}
	err := ValidateFormatRequiredFeatureGate(dir)
	if !errors.Is(err, ErrUnsupportedRequiredFeature) {
		t.Fatalf("ValidateFormatRequiredFeatureGate error=%v, want ErrUnsupportedRequiredFeature", err)
	}
	_, err = Open(Options{Dir: dir, IgnoreFormatConfig: true})
	if !errors.Is(err, ErrUnsupportedRequiredFeature) {
		t.Fatalf("Open IgnoreFormatConfig error=%v, want ErrUnsupportedRequiredFeature", err)
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
	err = ValidateFormatRequiredFeatureGate(dir)
	if err == nil || !strings.Contains(err.Error(), "required_features require format version 3") {
		t.Fatalf("ValidateFormatRequiredFeatureGate error=%v, want required_features version failure", err)
	}
}
