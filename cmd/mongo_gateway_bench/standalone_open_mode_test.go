package main

import (
	"context"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

func TestParseConfigTreeDBOpenModeValidation(t *testing.T) {
	t.Run("default legacy", func(t *testing.T) {
		cfg, err := parseConfig([]string{"-target", "treedb"})
		if err != nil {
			t.Fatalf("parseConfig default: %v", err)
		}
		if cfg.TreeDBOpenMode != treeDBOpenModeLegacy {
			t.Fatalf("TreeDBOpenMode=%q, want %q", cfg.TreeDBOpenMode, treeDBOpenModeLegacy)
		}
	})

	t.Run("standalone with COW production profiles", func(t *testing.T) {
		for _, prof := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
			cfg, err := parseConfig([]string{
				"-target", "treedb",
				"-treedb-open-mode", "standalone",
				"-treedb-profile", string(prof),
				"-treedb-document-format", "bson",
			})
			if err != nil {
				t.Fatalf("parseConfig(standalone, %s): %v", prof, err)
			}
			if cfg.TreeDBOpenMode != treeDBOpenModeStandalone {
				t.Fatalf("TreeDBOpenMode=%q, want standalone", cfg.TreeDBOpenMode)
			}
		}
	})

	t.Run("reject invalid open mode", func(t *testing.T) {
		if _, err := parseConfig([]string{"-target", "treedb", "-treedb-open-mode", "bogus"}); err == nil {
			t.Fatal("expected error for unknown -treedb-open-mode")
		}
	})

	t.Run("reject standalone with bench_unsafe", func(t *testing.T) {
		_, err := parseConfig([]string{"-target", "treedb", "-treedb-open-mode", "standalone", "-treedb-profile", "bench_unsafe"})
		if err == nil || !strings.Contains(err.Error(), "COW production") {
			t.Fatalf("expected bench_unsafe rejection for standalone, got %v", err)
		}
	})

	t.Run("reject standalone with route-mode production", func(t *testing.T) {
		_, err := parseConfig([]string{
			"-target", "treedb",
			"-treedb-open-mode", "standalone",
			"-treedb-profile", "command_wal_relaxed",
			"-route-mode", "production",
		})
		if err == nil || !strings.Contains(err.Error(), "route-mode production") {
			t.Fatalf("expected route-mode production rejection for standalone, got %v", err)
		}
	})
	t.Run("reject standalone with explicit maintenance full", func(t *testing.T) {
		_, err := parseConfig([]string{
			"-target", "treedb",
			"-treedb-open-mode", "standalone",
			"-treedb-profile", "no_wal_fast",
			"-treedb-maintenance", "full",
		})
		if err == nil || !strings.Contains(err.Error(), "treedb-maintenance full") {
			t.Fatalf("expected maintenance full rejection for standalone, got %v", err)
		}
	})
}

func TestRunBenchmarkStandaloneOpenModeSmoke(t *testing.T) {
	cases := []struct {
		name        string
		clientMode  string
		profile     string
		maintenance string
	}{
		{name: "direct_relaxed_checkpoint", clientMode: clientModeDirect, profile: string(treedb.ProfileCommandWALRelaxed), maintenance: treeDBMaintenanceCheckpoint},
		{name: "driver_relaxed_checkpoint", clientMode: clientModeDriverCommandRaw, profile: string(treedb.ProfileCommandWALRelaxed), maintenance: treeDBMaintenanceCheckpoint},
		{name: "direct_nowal_checkpoint", clientMode: clientModeDirect, profile: string(treedb.ProfileNoWALFast), maintenance: treeDBMaintenanceCheckpoint},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig([]string{
				"-target", "treedb",
				"-treedb-open-mode", "standalone",
				"-treedb-profile", tc.profile,
				"-treedb-document-format", "bson",
				"-client-mode", tc.clientMode,
				"-documents", "64",
				"-batch-size", "32",
				"-insert-producers", "2",
				"-secondary-indexes", "2",
				"-range-index",
				"-reads", "16",
				"-range-reads", "8",
				"-updates", "8",
				"-treedb-maintenance", tc.maintenance,
				"-treedb-read-state", "settled",
				"-timeout", "60s",
			})
			if err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			target, err := openTarget(ctx, cfg)
			if err != nil {
				t.Fatalf("openTarget(standalone, %s): %v", tc.name, err)
			}
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer closeCancel()
				if err := closeBenchTarget(closeCtx, target); err != nil {
					t.Errorf("closeBenchTarget: %v", err)
				}
			}()
			if target.owner == nil {
				t.Fatal("expected non-nil target.owner in standalone mode")
			}

			result, err := runBenchmark(ctx, cfg, target, nil)
			if err != nil {
				t.Fatalf("runBenchmark(standalone, %s): %v", tc.name, err)
			}
			if result.TreeDBOpenMode != treeDBOpenModeStandalone {
				t.Fatalf("result.TreeDBOpenMode=%q, want standalone", result.TreeDBOpenMode)
			}
			if result.TreeDBDocumentFormat != string(collections.DocumentFormatBSON) {
				t.Fatalf("result.TreeDBDocumentFormat=%q, want bson", result.TreeDBDocumentFormat)
			}
			if got := result.TreeDBStatsAfterLoad["treedb.cache.memtable_mode"]; got != "cow_btree" {
				t.Fatalf("TreeDBStatsAfterLoad[treedb.cache.memtable_mode]=%q, want cow_btree (stats=%v)", got, result.TreeDBStatsAfterLoad)
			}
			if len(result.Phases) < 4 {
				t.Fatalf("expected load+read+range+update phases, got %d", len(result.Phases))
			}
		})
	}
}
