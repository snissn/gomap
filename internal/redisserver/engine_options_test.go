package redisserver

import (
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestOpenEngine_TreeDB_DefaultsToPublicCommandWALProfile(t *testing.T) {
	db, err := OpenEngine(Config{
		Dir:              t.TempDir(),
		Engine:           "treedb",
		TreeDBWriteLanes: 4,
	})
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	defer db.Close()

	statser, ok := db.(interface{ Stats() map[string]string })
	if !ok {
		t.Fatalf("TreeDB adapter does not expose Stats")
	}
	stats := statser.Stats()
	if got := stats["treedb.command_wal.enabled"]; got != "true" {
		t.Fatalf("command_wal.enabled=%q, want true", got)
	}
	if got := stats["treedb.cache.journal_lanes.configured"]; got != "4" {
		t.Fatalf("journal_lanes.configured=%q, want 4", got)
	}
}

func TestOpenEngine_TreeDB_RejectsDeprecatedProfiles(t *testing.T) {
	for _, raw := range []string{
		"fast",
		"wal_on_fast",
		"durable",
		"legacy_wal_durable",
		"legacy_wal_relaxed_fast",
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := OpenEngine(Config{
				Dir:           t.TempDir(),
				Engine:        "treedb",
				TreeDBProfile: raw,
			})
			if err == nil {
				t.Fatal("OpenEngine succeeded, want profile error")
			}
			if !strings.Contains(err.Error(), treedb.ProfileFlagHelp) {
				t.Fatalf("error=%v, want allowed profile guidance", err)
			}
		})
	}
}

func TestOpenEngine_TreeDB_AcceptsCanonicalNoWALFastProfile(t *testing.T) {
	db, err := OpenEngine(Config{
		Dir:           t.TempDir(),
		Engine:        "treedb",
		TreeDBProfile: string(treedb.ProfileNoWALFast),
	})
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	defer db.Close()

	statser, ok := db.(interface{ Stats() map[string]string })
	if !ok {
		t.Fatalf("TreeDB adapter does not expose Stats")
	}
	stats := statser.Stats()
	if got := stats["treedb.profile.resolved"]; got != string(treedb.ProfileNoWALFast) {
		t.Fatalf("profile.resolved=%q, want %q", got, treedb.ProfileNoWALFast)
	}
	if got := stats["treedb.command_wal.enabled"]; got != "false" {
		t.Fatalf("command_wal.enabled=%q, want false", got)
	}
}
