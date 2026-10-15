package db

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFormatConfig_SaveLoadApply_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	cfg := formatConfigFromOptions(Options{
		IndexOuterLeavesInValueLog: true,

		LeafPrefixCompression:     true,
		IndexColumnarLeaves:       true,
		IndexPackedValuePtr:       true,
		IndexInternalBaseDelta:    true, // forced off when outer leaves in vlog is enabled
		IndexAdaptiveLeafEncoding: true,

		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionDict,
			BlockCodec:  ValueLogBlockZSTD,
			AutoPolicy:  ValueLogAutoSize,
		},
	})
	cfg.Version = 0 // SaveFormatConfig should default this.

	if err := SaveFormatConfig(dir, cfg); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}

	loaded, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatalf("expected format config to exist")
	}
	if loaded.Version != formatConfigVersion {
		t.Fatalf("loaded version=%d, want %d", loaded.Version, formatConfigVersion)
	}

	applied := Options{
		IndexOuterLeavesInValueLog: false,
		LeafPrefixCompression:      false,
		IndexColumnarLeaves:        false,
		IndexPackedValuePtr:        false,
		IndexInternalBaseDelta:     true,
		IndexAdaptiveLeafEncoding:  false,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionOff,
			BlockCodec:  ValueLogBlockSnappy,
			AutoPolicy:  ValueLogAutoBalanced,
		},
	}
	loaded.ApplyToOptions(&applied)

	if !applied.IndexOuterLeavesInValueLog {
		t.Fatalf("IndexOuterLeavesInValueLog=false, want true")
	}
	if !applied.LeafPrefixCompression {
		t.Fatalf("LeafPrefixCompression=false, want true")
	}
	if !applied.IndexColumnarLeaves {
		t.Fatalf("IndexColumnarLeaves=false, want true")
	}
	if !applied.IndexPackedValuePtr {
		t.Fatalf("IndexPackedValuePtr=false, want true")
	}
	if applied.IndexInternalBaseDelta {
		t.Fatalf("IndexInternalBaseDelta=true, want false (forced off for leaf pages in vlog)")
	}
	if !applied.IndexAdaptiveLeafEncoding {
		t.Fatalf("IndexAdaptiveLeafEncoding=false, want true")
	}
	if got, want := applied.ValueLog.Compression, ValueLogCompressionDict; got != want {
		t.Fatalf("ValueLog.Compression=%v, want %v", got, want)
	}
	if got, want := applied.ValueLog.BlockCodec, ValueLogBlockZSTD; got != want {
		t.Fatalf("ValueLog.BlockCodec=%v, want %v", got, want)
	}
	if got, want := applied.ValueLog.AutoPolicy, ValueLogAutoSize; got != want {
		t.Fatalf("ValueLog.AutoPolicy=%v, want %v", got, want)
	}
}

func TestLoadFormatConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for missing format.json")
	}
}

func TestLoadFormatConfig_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, formatConfigFileName), nil, 0o600); err != nil {
		t.Fatalf("write empty format.json: %v", err)
	}
	_, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for empty format.json")
	}
}

func TestLoadFormatConfig_UnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, formatConfigFileName), []byte(`{"version":999}`), 0o600); err != nil {
		t.Fatalf("write format.json: %v", err)
	}
	_, ok, err := LoadFormatConfig(dir)
	if err == nil {
		t.Fatalf("expected LoadFormatConfig error")
	}
	if ok {
		t.Fatalf("expected ok=false for unsupported format.json version")
	}
}

func TestFormatConfig_PersistsAndGatesCanonicalDurabilityProfile(t *testing.T) {
	dir := t.TempDir()
	cfg := formatConfigFromOptions(Options{ResolvedProfile: ProfileNoWALFast})
	if err := SaveFormatConfig(dir, cfg); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	loaded, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatal("format config missing")
	}
	if loaded.Version != formatConfigDurabilityProfileVersion || loaded.DurabilityProfile != ProfileNoWALFast {
		t.Fatalf("loaded profile manifest=(version=%d profile=%q)", loaded.Version, loaded.DurabilityProfile)
	}
	if err := ValidateDurabilityProfileGate(dir, ProfileNoWALFast); err != nil {
		t.Fatalf("matching profile gate: %v", err)
	}
	if err := ValidateDurabilityProfileGate(dir, ProfileCommandWALDurable); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("mismatched profile gate error=%v, want ErrLegacyFormatRebuildRequired", err)
	}
	if err := ValidateDurabilityProfileGate(dir, ""); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("missing selected profile gate error=%v, want ErrLegacyFormatRebuildRequired", err)
	}
}

func TestLoadPersistedDurabilityProfile_ReadsOnlyContractGate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		want      DurabilityProfile
		wantFound bool
		wantErr   string
	}{
		{name: "missing"},
		{name: "legacy", body: `{"version":2}`, wantFound: false},
		{name: "production", body: `{"version":4,"durability_profile":"no_wal_fast","future_field":{"nested":true}}`, want: ProfileNoWALFast, wantFound: true},
		{name: "benchmark", body: `{"version":4,"durability_profile":"bench_unsafe"}`, want: ProfileBenchUnsafe, wantFound: true},
		{name: "unrelated_malformed_legacy", body: `{"version":2,"future_field":`, wantFound: false},
		{name: "malformed_profile_gate", body: `{"version":4,"durability_profile":`, wantErr: "durability-profile gate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.body != "" {
				if err := os.WriteFile(filepath.Join(dir, formatConfigFileName), []byte(tc.body), 0o600); err != nil {
					t.Fatalf("write format.json: %v", err)
				}
			}

			got, found, err := LoadPersistedDurabilityProfile(dir)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadPersistedDurabilityProfile error=%v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadPersistedDurabilityProfile: %v", err)
			}
			if got != tc.want || found != tc.wantFound {
				t.Fatalf("LoadPersistedDurabilityProfile=(%q,%t), want (%q,%t)", got, found, tc.want, tc.wantFound)
			}
		})
	}
}

func TestSaveFormatConfig_CannotClearOrReplacePersistedDurabilityProfile(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, formatConfigFromOptions(Options{ResolvedProfile: ProfileCommandWALRelaxed})); err != nil {
		t.Fatalf("SaveFormatConfig initial: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{LeafPrefixCompression: true}); err != nil {
		t.Fatalf("SaveFormatConfig preserving profile: %v", err)
	}
	loaded, ok, err := LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok || loaded.DurabilityProfile != ProfileCommandWALRelaxed || loaded.Version != formatConfigDurabilityProfileVersion {
		t.Fatalf("profile binding lost after resave: ok=%t cfg=%+v", ok, loaded)
	}
	if err := SaveFormatConfig(dir, FormatConfig{DurabilityProfile: ProfileNoWALFast}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("profile replacement error=%v, want ErrLegacyFormatRebuildRequired", err)
	}
}

func TestLoadFormatConfig_RejectsInvalidDurabilityProfileManifest(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing", body: `{"version":4}`, want: "requires durability_profile"},
		{name: "unknown", body: `{"version":4,"durability_profile":"fast"}`, want: "unsupported durability_profile"},
		{name: "old_version", body: `{"version":3,"durability_profile":"no_wal_fast"}`, want: "requires format version 4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, formatConfigFileName), []byte(tc.body), 0o600); err != nil {
				t.Fatalf("write format.json: %v", err)
			}
			if _, _, err := LoadFormatConfig(dir); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadFormatConfig error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateDurabilityProfileGate_RejectsLegacyManifestForPublicProfile(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	if err := ValidateDurabilityProfileGate(dir, ProfileCommandWALDurable); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("legacy profile gate error=%v, want ErrLegacyFormatRebuildRequired", err)
	}
}

func TestOpen_AppliesPersistedIndexFormatConfig(t *testing.T) {
	dir := t.TempDir()

	writer, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen without explicit format flags: %v", err)
	}
	defer reopen.Close()

	if !reopen.indexOuterLeavesInValueLog {
		t.Fatalf("indexOuterLeavesInValueLog=false, want true from persisted format config")
	}
	if reopen.leafGenerationManifest == nil {
		t.Fatalf("leafGenerationManifest=nil, want manifest loaded from persisted format config")
	}
}
