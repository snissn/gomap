package treedb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestPublicExternalCommandWALHasOneByteAccountingOwner(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir: dir, Durability: DurabilityWALOnRelaxed, CommandWAL: true,
		CommandWALSegmentTargetBytes: 256, DisableSideStores: true,
		MaxWALBytes: -1, BackgroundCheckpointInterval: -1,
		BackgroundCheckpointIdleDuration: -1,
	}
	check := func(database *DB) {
		t.Helper()
		for _, key := range []string{"treedb.cache.wal_closed_bytes_estimate", "treedb.cache.wal_current_bytes_estimate", "treedb.cache.wal_bytes_estimate"} {
			if got := database.Stats()[key]; got != "0" {
				t.Fatalf("external command WAL also counted as cached redo: %s=%s", key, got)
			}
		}
		paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(dir), "commit-l*.log"))
		if err != nil || len(paths) == 0 {
			t.Fatalf("command WAL files=%v err=%v", paths, err)
		}
		var logical, physical int64
		for _, path := range paths {
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			size, sizeErr := commitlog.LogicalFileSize(file)
			info, statErr := file.Stat()
			_ = file.Close()
			if sizeErr != nil || statErr != nil {
				t.Fatalf("WAL sizes: logical=%v physical=%v", sizeErr, statErr)
			}
			logical += size
			physical += info.Size()
		}
		if got := database.backend.CommandWALBytes(); got != logical {
			t.Fatalf("logical WAL pressure=%d, want %d (physical footprint=%d)", got, logical, physical)
		}
		if database.Stats()["treedb.command_wal.writer.container"] == "fixed" {
			if physical <= logical {
				t.Fatalf("fixture lacks initialized reserve: physical=%d logical=%d", physical, logical)
			}
		} else if physical != logical {
			t.Fatalf("stream WAL physical=%d, want logical size %d", physical, logical)
		}
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	check(database) // A newly created WAL has no logical debt in either format.
	for i := 0; i < 6; i++ {
		if err := database.Set([]byte(fmt.Sprintf("accounting-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	check(database) // Active and closed segments retain the same single owner.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	check(database) // Recovery must not import backend files into cached debt.
	for i := 0; i < 6; i++ {
		if got, err := database.Get([]byte(fmt.Sprintf("accounting-%d", i))); err != nil || string(got) != "value" {
			t.Fatalf("reopened value %d=%q err=%v", i, got, err)
		}
	}
}
