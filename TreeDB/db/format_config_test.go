package db

import (
	"os"
	"path/filepath"
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
			BlockCodec:  ValueLogBlockLZ4,
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
	if got, want := applied.ValueLog.BlockCodec, ValueLogBlockLZ4; got != want {
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
