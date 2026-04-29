package treedb

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestApplyEnvIndexRuntimeOverrides_BoolKnobs(t *testing.T) {
	opts := Options{
		VerifyOnRead:               false,
		PreferAppendAlloc:          true,
		DisablePiggybackCompaction: false,

		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		IndexInternalBaseDelta:     false,
		IndexOuterLeavesInValueLog: true,
		IndexAdaptiveLeafEncoding:  false,
	}

	t.Setenv(envVerifyOnRead, "true")
	t.Setenv(envPreferAppendAlloc, "false")
	t.Setenv(envDisablePiggybackCompaction, "") // empty => true

	applyEnvIndexRuntimeOverrides(&opts)

	if !opts.VerifyOnRead {
		t.Fatalf("VerifyOnRead=false, want true")
	}
	if opts.PreferAppendAlloc {
		t.Fatalf("PreferAppendAlloc=true, want false")
	}
	if !opts.DisablePiggybackCompaction {
		t.Fatalf("DisablePiggybackCompaction=false, want true")
	}

	// Runtime overrides must not mutate persisted index-format knobs.
	if !opts.LeafPrefixCompression {
		t.Fatalf("LeafPrefixCompression=false, want true")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("IndexColumnarLeaves=false, want true")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("IndexPackedValuePtr=false, want true")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("IndexInternalBaseDelta=true, want false")
	}
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("IndexOuterLeavesInValueLog=false, want true")
	}
	if opts.IndexAdaptiveLeafEncoding {
		t.Fatalf("IndexAdaptiveLeafEncoding=true, want false")
	}
}

func TestApplyEnvIndexFormatOverrides_BoolKnobs(t *testing.T) {
	opts := Options{
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		IndexInternalBaseDelta:     false,
		IndexOuterLeavesInValueLog: true,
		IndexAdaptiveLeafEncoding:  false,
	}

	t.Setenv(envLeafPrefixCompression, "false")
	t.Setenv(envIndexColumnarLeaves, "false")
	t.Setenv(envIndexPackedValuePtr, "false")
	t.Setenv(envIndexInternalBaseDelta, "true")
	t.Setenv(envIndexOuterLeavesInValueLog, "false")
	t.Setenv(envIndexAdaptiveLeafEncoding, "true")

	applyEnvIndexFormatOverrides(&opts)

	if opts.LeafPrefixCompression {
		t.Fatalf("LeafPrefixCompression=true, want false")
	}
	if opts.IndexColumnarLeaves {
		t.Fatalf("IndexColumnarLeaves=true, want false")
	}
	if opts.IndexPackedValuePtr {
		t.Fatalf("IndexPackedValuePtr=true, want false")
	}
	if !opts.IndexInternalBaseDelta {
		t.Fatalf("IndexInternalBaseDelta=false, want true")
	}
	if opts.IndexOuterLeavesInValueLog {
		t.Fatalf("IndexOuterLeavesInValueLog=true, want false")
	}
	if !opts.IndexAdaptiveLeafEncoding {
		t.Fatalf("IndexAdaptiveLeafEncoding=false, want true")
	}
}

func TestOpen_EnvIndexFormatConflictWithPersistedFormat(t *testing.T) {
	dir := t.TempDir()
	maindbDir := filepath.Join(dir, "maindb")
	if err := os.MkdirAll(maindbDir, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	cfg := db.FormatConfig{
		Version: 2,

		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		IndexInternalBaseDelta:     false,
		IndexAdaptiveLeafEncoding:  false,
	}
	if err := db.SaveFormatConfig(maindbDir, cfg); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}

	t.Setenv(envIndexOuterLeavesInValueLog, "false")

	_, err := Open(Options{Dir: dir, ReadOnly: true})
	if err == nil {
		t.Fatalf("expected Open error")
	}
	if !strings.Contains(err.Error(), envIndexOuterLeavesInValueLog) {
		t.Fatalf("unexpected error %q; want mention of %s", err.Error(), envIndexOuterLeavesInValueLog)
	}
	if !strings.Contains(err.Error(), "format.json") {
		t.Fatalf("unexpected error %q; want mention of format.json", err.Error())
	}
}
