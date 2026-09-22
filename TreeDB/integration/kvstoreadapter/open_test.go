package kvstoreadapter

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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
		{raw: "", fallback: "", want: treedb.ProfileCommandWALRelaxed},
		{raw: "", fallback: treedb.ProfileWALOnFast, want: treedb.ProfileWALOnFast},
		{raw: "fast", fallback: treedb.ProfileDurable, want: treedb.ProfileDurable},
		{raw: "walonfast", fallback: treedb.ProfileDurable, want: treedb.ProfileDurable},
		{raw: "durable", fallback: treedb.ProfileFast, want: treedb.ProfileFast},
		{raw: "command_wal_durable", fallback: treedb.ProfileFast, want: treedb.ProfileCommandWALDurable},
		{raw: "legacy_wal_relaxed_fast", fallback: treedb.ProfileFast, want: treedb.ProfileFast},
		{raw: "no_wal_fast", fallback: treedb.ProfileDurable, want: treedb.ProfileNoWALFast},
		{raw: "bench_unsafe", fallback: treedb.ProfileFast, want: treedb.ProfileBenchUnsafe},
		{raw: "bench", fallback: treedb.ProfileFast, want: treedb.ProfileFast},
		{raw: "unknown", fallback: treedb.ProfileFast, want: treedb.ProfileFast},
	}
	for _, tc := range cases {
		if got := ParseProfile(tc.raw, tc.fallback); got != tc.want {
			t.Fatalf("ParseProfile(%q, %q) = %q, want %q", tc.raw, tc.fallback, got, tc.want)
		}
	}
}

func TestParsePublicProfileUsesPublicNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw      string
		fallback treedb.Profile
		want     treedb.Profile
	}{
		{raw: "", fallback: "", want: treedb.ProfileCommandWALRelaxed},
		{raw: "", fallback: treedb.ProfileCommandWALDurable, want: treedb.ProfileCommandWALDurable},
		{raw: "command_wal_durable", fallback: treedb.ProfileCommandWALRelaxed, want: treedb.ProfileCommandWALDurable},
		{raw: "command_wal_relaxed", fallback: treedb.ProfileCommandWALDurable, want: treedb.ProfileCommandWALRelaxed},
		{raw: "no_wal_fast", fallback: treedb.ProfileCommandWALRelaxed, want: treedb.ProfileNoWALFast},
	}
	for _, tc := range cases {
		got, err := ParsePublicProfile(tc.raw, tc.fallback)
		if err != nil {
			t.Fatalf("ParsePublicProfile(%q, %q): %v", tc.raw, tc.fallback, err)
		}
		if got != tc.want {
			t.Fatalf("ParsePublicProfile(%q, %q) = %q, want %q", tc.raw, tc.fallback, got, tc.want)
		}
	}
}

func TestParsePublicProfileRejectsDeprecatedAndUnknownNames(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"fast", "walonfast", "durable", "legacy_wal_relaxed_fast", "bench", "bench_unsafe", "command-wal-durable", "unknown"} {
		t.Run(raw, func(t *testing.T) {
			_, err := ParsePublicProfile(raw, treedb.ProfileCommandWALRelaxed)
			if err == nil {
				t.Fatal("ParsePublicProfile succeeded, want error")
			}
		})
	}
	if _, err := ParsePublicProfile("", treedb.ProfileFast); err == nil {
		t.Fatal("ParsePublicProfile accepted deprecated fallback, want error")
	} else if !strings.Contains(err.Error(), "fallback profile") {
		t.Fatalf("ParsePublicProfile deprecated fallback err=%v, want fallback profile context", err)
	}
}

func TestResolveOptionsAppliesDefaultsAndEnvOverrides(t *testing.T) {
	t.Setenv(EnvOpenProfile, "command_wal_relaxed")
	t.Setenv(EnvKeepRecent, "7")
	t.Setenv(EnvMemtableMode, "skiplist")

	opts, path, err := ResolveOptions(OpenConfig{
		ParentDir:                   t.TempDir(),
		Name:                        "application",
		DefaultProfile:              treedb.ProfileCommandWALDurable,
		DefaultKeepRecent:           1,
		DefaultAdaptiveMemtableBase: "hash_sorted",
	})
	if err != nil {
		t.Fatalf("ResolveOptions error: %v", err)
	}
	if path != filepath.Join(opts.Dir) {
		t.Fatalf("path %q != opts.Dir %q", path, opts.Dir)
	}
	if !opts.CommandWAL {
		t.Fatal("CommandWAL=false, want command WAL profile")
	}
	if opts.Durability != treedb.DurabilityWALOnRelaxed {
		t.Fatalf("Durability = %v, want command WAL relaxed durability", opts.Durability)
	}
	if opts.KeepRecent != 7 {
		t.Fatalf("KeepRecent = %d, want 7", opts.KeepRecent)
	}
	if opts.MemtableMode != "skiplist" {
		t.Fatalf("MemtableMode = %q, want skiplist", opts.MemtableMode)
	}
}

func TestResolveOptionsDefaultsToCommandWALRelaxedProfile(t *testing.T) {
	t.Parallel()

	opts, _, err := ResolveOptions(OpenConfig{
		ParentDir: t.TempDir(),
		Name:      "application",
	})
	if err != nil {
		t.Fatalf("ResolveOptions error: %v", err)
	}
	if !opts.CommandWAL {
		t.Fatal("CommandWAL=false, want default command WAL profile")
	}
	if opts.Durability != treedb.DurabilityWALOnRelaxed {
		t.Fatalf("Durability = %v, want command WAL relaxed durability", opts.Durability)
	}
}

func TestResolveOptionsAppliesAdaptiveMemtableFallback(t *testing.T) {
	t.Parallel()

	opts, _, err := ResolveOptions(OpenConfig{
		ParentDir:                   t.TempDir(),
		Name:                        "application",
		DefaultProfile:              treedb.ProfileCommandWALRelaxed,
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
		DefaultProfile:      treedb.ProfileCommandWALRelaxed,
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
		DefaultProfile: treedb.ProfileCommandWALRelaxed,
	}); err == nil {
		t.Fatal("expected invalid keep recent error")
	}
}

func TestResolveOptionsRejectsDeprecatedProfileEnv(t *testing.T) {
	t.Setenv(EnvOpenProfile, "fast")
	if _, _, err := ResolveOptions(OpenConfig{
		ParentDir: t.TempDir(),
		Name:      "application",
	}); err == nil {
		t.Fatal("expected deprecated profile env error")
	}
}

func TestResolveOptionsRejectsInvalidPathInputs(t *testing.T) {
	t.Parallel()

	cases := []OpenConfig{
		{
			ParentDir:      "",
			Name:           "application",
			DefaultProfile: treedb.ProfileCommandWALRelaxed,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           "",
			DefaultProfile: treedb.ProfileCommandWALRelaxed,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           "bad/name",
			DefaultProfile: treedb.ProfileCommandWALRelaxed,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           `bad\name`,
			DefaultProfile: treedb.ProfileCommandWALRelaxed,
		},
		{
			ParentDir:      t.TempDir(),
			Name:           "application",
			DBFileSuffix:   "../bad",
			DefaultProfile: treedb.ProfileCommandWALRelaxed,
		},
	}
	for _, cfg := range cases {
		if _, _, err := ResolveOptions(cfg); err == nil {
			t.Fatalf("expected ResolveOptions error for cfg=%+v", cfg)
		}
	}
}

func TestResolveOptionsRejectsUnsupportedAdapterFeatures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		cfg     OpenConfig
		wantMsg string
	}{
		{
			name: "encryption",
			cfg: OpenConfig{
				ParentDir:         t.TempDir(),
				Name:              "application",
				DefaultProfile:    treedb.ProfileCommandWALRelaxed,
				RequireEncryption: true,
			},
			wantMsg: "encryption",
		},
		{
			name: "in memory",
			cfg: OpenConfig{
				ParentDir:       t.TempDir(),
				Name:            "application",
				DefaultProfile:  treedb.ProfileCommandWALRelaxed,
				RequireInMemory: true,
			},
			wantMsg: "in-memory",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ResolveOptions(tc.cfg); !errors.Is(err, ErrUnsupportedAdapterFeature) {
				t.Fatalf("ResolveOptions error=%v, want ErrUnsupportedAdapterFeature", err)
			} else if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("ResolveOptions error=%v, want %q context", err, tc.wantMsg)
			}
		})
	}
}

func TestResolveOptionsIsSideEffectFreeOnError(t *testing.T) {
	t.Setenv(EnvKeepRecent, "not-a-number")

	parentDir := t.TempDir()
	dbPath := filepath.Join(parentDir, "application.db")
	if _, _, err := ResolveOptions(OpenConfig{
		ParentDir:      parentDir,
		Name:           "application",
		DefaultProfile: treedb.ProfileCommandWALRelaxed,
	}); err == nil {
		t.Fatal("expected invalid keep recent error")
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("ResolveOptions created %q on error, stat err=%v", dbPath, err)
	}
}

func TestOpenRejectsUnsupportedAdapterFeaturesBeforeCreatingDB(t *testing.T) {
	t.Parallel()

	parentDir := t.TempDir()
	dbPath := filepath.Join(parentDir, "application.db")
	_, err := Open(OpenConfig{
		ParentDir:         parentDir,
		Name:              "application",
		DefaultProfile:    treedb.ProfileCommandWALRelaxed,
		RequireEncryption: true,
	})
	if !errors.Is(err, ErrUnsupportedAdapterFeature) {
		t.Fatalf("Open error=%v, want ErrUnsupportedAdapterFeature", err)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("Open created %q on unsupported feature error, stat err=%v", dbPath, statErr)
	}
}

func TestOpenReturnsDBAndNamedAdapter(t *testing.T) {
	t.Parallel()

	parentDir := t.TempDir()
	opened, err := Open(OpenConfig{
		ParentDir:         parentDir,
		Name:              "application",
		AdapterName:       "TreeDB Test",
		DefaultProfile:    treedb.ProfileCommandWALRelaxed,
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
