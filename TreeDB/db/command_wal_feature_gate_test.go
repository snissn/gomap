package db

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALRejectedErrorDistinctFromUnsupported(t *testing.T) {
	if ErrCommandWALRejected == ErrCommandWALUnsupported {
		t.Fatal("ErrCommandWALRejected must be a distinct public sentinel from ErrCommandWALUnsupported")
	}
	if errors.Is(ErrCommandWALRejected, ErrCommandWALUnsupported) {
		t.Fatal("ErrCommandWALRejected must not match ErrCommandWALUnsupported")
	}
}

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
	if err := ValidateCommandWALActivationClean(dir); !errors.Is(err, ErrCommandWALDirtyActivation) {
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
	if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, ErrCommandWALDirtyActivation) {
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
	if !errors.Is(err, ErrCommandWALDirtyActivation) {
		t.Fatalf("SaveFormatConfig error=%v, want clean legacy WAL failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "format.json")); !os.IsNotExist(statErr) {
		t.Fatalf("format.json stat error=%v, want not exist after rejected activation", statErr)
	}
}

func TestSaveFormatConfigCommandWALResaveExistingFeatureAllowsCommandSegments(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig initial: %v", err)
	}
	writeCommandWALSegmentFrames(t, dir, 1, 1)
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig resave existing command WAL feature: %v", err)
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

func TestSaveOpenFormatConfigRefreshesActiveCommandWALKnobs(t *testing.T) {
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
	if err := saveOpenFormatConfig(Options{
		Dir:                   dir,
		CommandWAL:            true,
		LeafPrefixCompression: true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionOff,
		},
	}); err != nil {
		t.Fatalf("saveOpenFormatConfig: %v", err)
	}
	cfg, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok || !cfg.RequiresCommandWALV1() {
		t.Fatalf("format config command_wal_v1 missing: ok=%v cfg=%+v", ok, cfg)
	}
	if !cfg.LeafPrefixCompression {
		t.Fatalf("LeafPrefixCompression=false, want refreshed true")
	}
	if cfg.ValueLogCompression != "off" {
		t.Fatalf("ValueLogCompression=%q, want off", cfg.ValueLogCompression)
	}
}

func TestCommandWALRejectsWALOffDurability(t *testing.T) {
	dir := t.TempDir()
	_, err := Open(Options{Dir: dir, CommandWAL: true, Durability: DurabilityWALOffRelaxed})
	if !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("Open CommandWAL WAL-off error=%v, want ErrCommandWALUnsupported", err)
	}

	persistedDir := t.TempDir()
	if err := os.MkdirAll(persistedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll persistedDir: %v", err)
	}
	if err := SaveFormatConfig(persistedDir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig persistedDir: %v", err)
	}
	_, err = Open(Options{Dir: persistedDir, Durability: DurabilityWALOffRelaxed})
	if !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("Open persisted CommandWAL WAL-off error=%v, want ErrCommandWALUnsupported", err)
	}
}

func TestCommandWALOptionPersistsFeatureBeforeJournalActivation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true})
	if err != nil {
		t.Fatalf("Open CommandWAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})
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
		dbClosed = true
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true

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
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l0-000001.log"), []byte("dirty commit wal"), 0o600); err != nil {
		t.Fatalf("write commit wal: %v", err)
	}
	_, err := Open(Options{Dir: dir, CommandWAL: true})
	if !errors.Is(err, ErrCommandWALDirtyActivation) {
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

func TestCommandWALRequiredFeatureRejectsOfflineValueLogRewrite(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
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
		name        string
		json        string
		wantOpenErr string
	}{
		{name: "malformed", json: `{"version":`},
		{
			name:        "future version without required features",
			json:        `{"version":999}`,
			wantOpenErr: "requires durability_profile",
		},
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
			if tc.wantOpenErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantOpenErr) {
					t.Fatalf("Open IgnoreFormatConfig error=%v, want substring %q", err, tc.wantOpenErr)
				}
				return
			}
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
