package kvstoreadapter

import (
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParseProfileUsesStandardNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw      string
		fallback treedb.Profile
		want     treedb.Profile
	}{
		{raw: "", fallback: treedb.ProfileWALOnFast, want: treedb.ProfileWALOnFast},
		{raw: "fast", fallback: treedb.ProfileDurable, want: treedb.ProfileFast},
		{raw: "walonfast", fallback: treedb.ProfileDurable, want: treedb.ProfileWALOnFast},
		{raw: "durable", fallback: treedb.ProfileFast, want: treedb.ProfileDurable},
		{raw: "command-wal-durable", fallback: treedb.ProfileFast, want: treedb.ProfileCommandWALDurable},
		{raw: "legacy_wal_relaxed_fast", fallback: treedb.ProfileFast, want: treedb.ProfileLegacyWALRelaxedFast},
		{raw: "no_wal_fast", fallback: treedb.ProfileDurable, want: treedb.ProfileNoWALFast},
		{raw: "bench", fallback: treedb.ProfileFast, want: treedb.ProfileBench},
		{raw: "unknown", fallback: treedb.ProfileFast, want: treedb.ProfileFast},
	}
	for _, tc := range cases {
		if got := ParseProfile(tc.raw, tc.fallback); got != tc.want {
			t.Fatalf("ParseProfile(%q, %q) = %q, want %q", tc.raw, tc.fallback, got, tc.want)
		}
	}
}

func TestResolveOptionsAppliesDefaultsAndEnvOverrides(t *testing.T) {
	t.Setenv(EnvOpenProfile, "fast")
	t.Setenv(EnvKeepRecent, "7")
	t.Setenv(EnvMemtableMode, "skiplist")

	opts, path, err := ResolveOptions(OpenConfig{
		ParentDir:                   t.TempDir(),
		Name:                        "application",
		DefaultProfile:              treedb.ProfileDurable,
		DefaultKeepRecent:           1,
		DefaultAdaptiveMemtableBase: "hash_sorted",
	})
	if err != nil {
		t.Fatalf("ResolveOptions error: %v", err)
	}
	if path != filepath.Join(opts.Dir) {
		t.Fatalf("path %q != opts.Dir %q", path, opts.Dir)
	}
	if opts.Durability != treedb.DurabilityWALOffRelaxed {
		t.Fatalf("Durability = %v, want fast WAL-off relaxed", opts.Durability)
	}
	if opts.KeepRecent != 7 {
		t.Fatalf("KeepRecent = %d, want 7", opts.KeepRecent)
	}
	if opts.MemtableMode != "skiplist" {
		t.Fatalf("MemtableMode = %q, want skiplist", opts.MemtableMode)
	}
}

func TestResolveOptionsAppliesAdaptiveMemtableFallback(t *testing.T) {
	t.Parallel()

	opts, _, err := ResolveOptions(OpenConfig{
		ParentDir:                   t.TempDir(),
		Name:                        "application",
		DefaultProfile:              treedb.ProfileWALOnFast,
		DefaultKeepRecent:           1,
		DefaultAdaptiveMemtableBase: "hash_sorted",
	})
	if err != nil {
		t.Fatalf("ResolveOptions error: %v", err)
	}
	if opts.KeepRecent != 1 {
		t.Fatalf("KeepRecent = %d, want 1", opts.KeepRecent)
	}
	if opts.MemtableMode != "adaptive:hash_sorted" {
		t.Fatalf("MemtableMode = %q, want adaptive:hash_sorted", opts.MemtableMode)
	}
}

func TestResolveOptionsNormalizesDefaultMemtableMode(t *testing.T) {
	t.Parallel()

	opts, _, err := ResolveOptions(OpenConfig{
		ParentDir:           t.TempDir(),
		Name:                "application",
		DefaultProfile:      treedb.ProfileWALOnFast,
		DefaultMemtableMode: "SkipList",
	})
	if err != nil {
		t.Fatalf("ResolveOptions error: %v", err)
	}
	if opts.MemtableMode != "skiplist" {
		t.Fatalf("MemtableMode = %q, want skiplist", opts.MemtableMode)
	}
}

func TestResolveOptionsRejectsInvalidKeepRecent(t *testing.T) {
	t.Setenv(EnvKeepRecent, "not-a-number")
	if _, _, err := ResolveOptions(OpenConfig{
		ParentDir:      t.TempDir(),
		Name:           "application",
		DefaultProfile: treedb.ProfileWALOnFast,
	}); err == nil {
		t.Fatal("expected invalid keep recent error")
	}
}

func TestResolveOptionsRejectsInvalidPathInputs(t *testing.T) {
	t.Parallel()

	cases := []OpenConfig{
		{
			ParentDir:      "",
			Name:           "application",
			DefaultProfile: treedb.ProfileWALOnFast,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           "",
			DefaultProfile: treedb.ProfileWALOnFast,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           "bad/name",
			DefaultProfile: treedb.ProfileWALOnFast,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           `bad\name`,
			DefaultProfile: treedb.ProfileWALOnFast,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           "application",
			DBFileSuffix:   "../bad",
			DefaultProfile: treedb.ProfileWALOnFast,
		},
	}
	for _, cfg := range cases {
		if _, _, err := ResolveOptions(cfg); err == nil {
			t.Fatalf("expected ResolveOptions error for cfg=%+v", cfg)
		}
	}
}

func TestResolveOptionsIsSideEffectFreeOnError(t *testing.T) {
	t.Setenv(EnvKeepRecent, "not-a-number")

	parentDir := t.TempDir()
	dbPath := filepath.Join(parentDir, "application.db")
	if _, _, err := ResolveOptions(OpenConfig{
		ParentDir:      parentDir,
		Name:           "application",
		DefaultProfile: treedb.ProfileWALOnFast,
	}); err == nil {
		t.Fatal("expected invalid keep recent error")
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("ResolveOptions created %q on error, stat err=%v", dbPath, err)
	}
}

func TestOpenReturnsDBAndNamedAdapter(t *testing.T) {
	t.Parallel()

	parentDir := t.TempDir()
	opened, err := Open(OpenConfig{
		ParentDir:         parentDir,
		Name:              "application",
		AdapterName:       "TreeDB Test",
		DefaultProfile:    treedb.ProfileFast,
		DefaultKeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	t.Cleanup(func() {
		_ = opened.DB.Close()
	})
	if opened.DB == nil || opened.KV == nil {
		t.Fatal("expected DB and KV adapter")
	}
	if opened.KV.Name() != "TreeDB Test" {
		t.Fatalf("adapter name = %q, want %q", opened.KV.Name(), "TreeDB Test")
	}
	if _, err := os.Stat(opened.Path); err != nil {
		t.Fatalf("stat opened path: %v", err)
	}
}
