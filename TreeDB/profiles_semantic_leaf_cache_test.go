package treedb

import (
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestOptionsForSemanticDefaultLeafPageReadCacheEntries(t *testing.T) {
	t.Setenv(db.LeafPageReadCacheEntriesEnvKey, "")

	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsForSemantic(profile, t.TempDir())
			if opts.LeafPageReadCacheEntries != defaultSemanticLeafPageReadCacheEntries {
				t.Fatalf("OptionsForSemantic(%s).LeafPageReadCacheEntries=%d, want %d", profile, opts.LeafPageReadCacheEntries, defaultSemanticLeafPageReadCacheEntries)
			}
			if opts.LeafPageReadCacheEntries != 32768 {
				t.Fatalf("OptionsForSemantic(%s).LeafPageReadCacheEntries=%d, want 32768", profile, opts.LeafPageReadCacheEntries)
			}

			baseOpts := OptionsFor(profile, t.TempDir())
			if baseOpts.LeafPageReadCacheEntries != 0 {
				t.Fatalf("OptionsFor(%s).LeafPageReadCacheEntries=%d, want 0", profile, baseOpts.LeafPageReadCacheEntries)
			}
		})
	}

	// Whitespace-only env var behaves like unset and still applies the semantic default.
	t.Setenv(db.LeafPageReadCacheEntriesEnvKey, "   ")
	opts := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
	if opts.LeafPageReadCacheEntries != defaultSemanticLeafPageReadCacheEntries {
		t.Fatalf("whitespace env LeafPageReadCacheEntries=%d, want %d", opts.LeafPageReadCacheEntries, defaultSemanticLeafPageReadCacheEntries)
	}

	// Opening a small semantic DB wires the 32768-slot cache lazily without preallocating 128 MiB.
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(OptionsForSemantic): %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	stats := database.Stats()
	if got := stats["treedb.process.read_path.outer_leaf.cache.capacity"]; got != "32768" {
		t.Fatalf("outer_leaf.cache.capacity=%q, want 32768", got)
	}
	if rawBacking, ok := stats["treedb.process.read_path.outer_leaf.cache.backing_bytes"]; ok {
		backing, err := strconv.ParseUint(rawBacking, 10, 64)
		if err != nil {
			t.Fatalf("parse backing_bytes %q: %v", rawBacking, err)
		}
		if backing > 1<<20 {
			t.Fatalf("small DB preallocated outer_leaf.cache.backing_bytes=%d (>1 MiB)", backing)
		}
	}
}

func TestOptionsForSemanticLeafPageReadCacheEnvOverride(t *testing.T) {
	t.Run("positive_env", func(t *testing.T) {
		t.Setenv(db.LeafPageReadCacheEntriesEnvKey, "256")
		opts := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
		if opts.LeafPageReadCacheEntries != 0 {
			t.Fatalf("OptionsForSemantic with env set got LeafPageReadCacheEntries=%d, want 0 to preserve env precedence", opts.LeafPageReadCacheEntries)
		}
		resolved, err := db.ResolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries)
		if err != nil {
			t.Fatalf("ResolveLeafPageReadCacheEntries: %v", err)
		}
		if resolved != 256 {
			t.Fatalf("resolved entries=%d, want 256", resolved)
		}
		database, err := Open(opts)
		if err != nil {
			t.Fatalf("Open with env override: %v", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}()
		if got := database.Stats()["treedb.process.read_path.outer_leaf.cache.capacity"]; got != "256" {
			t.Fatalf("outer_leaf.cache.capacity=%q, want 256", got)
		}
	})

	t.Run("zero_env_disables", func(t *testing.T) {
		t.Setenv(db.LeafPageReadCacheEntriesEnvKey, "0")
		opts := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
		if opts.LeafPageReadCacheEntries != 0 {
			t.Fatalf("OptionsForSemantic with env=0 got LeafPageReadCacheEntries=%d, want 0", opts.LeafPageReadCacheEntries)
		}
		resolved, err := db.ResolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries)
		if err != nil {
			t.Fatalf("ResolveLeafPageReadCacheEntries: %v", err)
		}
		if resolved != 0 {
			t.Fatalf("resolved entries=%d, want 0", resolved)
		}
		database, err := Open(opts)
		if err != nil {
			t.Fatalf("Open with env=0: %v", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}()
		if got := database.Stats()["treedb.process.read_path.outer_leaf.cache.capacity"]; got != "0" {
			t.Fatalf("outer_leaf.cache.capacity=%q, want 0", got)
		}
	})
}

func TestOptionsForSemanticLeafPageReadCacheCallerOverrides(t *testing.T) {
	t.Run("negative_disables", func(t *testing.T) {
		t.Setenv(db.LeafPageReadCacheEntriesEnvKey, "")
		opts := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
		opts.LeafPageReadCacheEntries = -1
		resolved, err := db.ResolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries)
		if err != nil {
			t.Fatalf("ResolveLeafPageReadCacheEntries(-1): %v", err)
		}
		if resolved != 0 {
			t.Fatalf("resolved entries=%d, want 0", resolved)
		}
		database, err := Open(opts)
		if err != nil {
			t.Fatalf("Open(-1): %v", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}()
		if got := database.Stats()["treedb.process.read_path.outer_leaf.cache.capacity"]; got != "0" {
			t.Fatalf("outer_leaf.cache.capacity=%q, want 0", got)
		}
	})

	t.Run("explicit_positive_override_wins_over_default_and_env", func(t *testing.T) {
		t.Setenv(db.LeafPageReadCacheEntriesEnvKey, "256")
		opts := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
		opts.LeafPageReadCacheEntries = 4096
		resolved, err := db.ResolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries)
		if err != nil {
			t.Fatalf("ResolveLeafPageReadCacheEntries(4096): %v", err)
		}
		if resolved != 4096 {
			t.Fatalf("resolved entries=%d, want 4096", resolved)
		}
		database, err := Open(opts)
		if err != nil {
			t.Fatalf("Open(4096): %v", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}()
		if got := database.Stats()["treedb.process.read_path.outer_leaf.cache.capacity"]; got != "4096" {
			t.Fatalf("outer_leaf.cache.capacity=%q, want 4096", got)
		}
	})
}

func TestOptionsForSemanticGroupedFrameCacheDefaults(t *testing.T) {
	for _, profile := range []Profile{
		ProfileCommandWALDurable,
		ProfileCommandWALRelaxed,
		ProfileNoWALFast,
	} {
		opts := OptionsForSemantic(profile, t.TempDir())
		if opts.ValueLog.GroupedFrameCacheEntries != defaultSemanticGroupedFrameCacheEntries {
			t.Fatalf("OptionsForSemantic(%s).ValueLog.GroupedFrameCacheEntries=%d want %d", profile, opts.ValueLog.GroupedFrameCacheEntries, defaultSemanticGroupedFrameCacheEntries)
		}
		if opts.ValueLog.GroupedFrameCacheMaxBytes != defaultSemanticGroupedFrameCacheMaxBytes {
			t.Fatalf("OptionsForSemantic(%s).ValueLog.GroupedFrameCacheMaxBytes=%d want %d", profile, opts.ValueLog.GroupedFrameCacheMaxBytes, defaultSemanticGroupedFrameCacheMaxBytes)
		}
	}

	// Verify caller disable (-1) and explicit positive overrides open cleanly.
	optsOff := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
	optsOff.ValueLog.GroupedFrameCacheEntries = -1
	dbOff, err := Open(optsOff)
	if err != nil {
		t.Fatalf("Open(GroupedFrameCacheEntries=-1): %v", err)
	}
	if err := dbOff.Close(); err != nil {
		t.Fatalf("Close(off): %v", err)
	}

	optsCustom := OptionsForSemantic(ProfileCommandWALRelaxed, t.TempDir())
	optsCustom.ValueLog.GroupedFrameCacheEntries = 512
	optsCustom.ValueLog.GroupedFrameCacheMaxBytes = 16 << 20
	dbCustom, err := Open(optsCustom)
	if err != nil {
		t.Fatalf("Open(custom grouped frame cache): %v", err)
	}
	if err := dbCustom.Close(); err != nil {
		t.Fatalf("Close(custom): %v", err)
	}
}
