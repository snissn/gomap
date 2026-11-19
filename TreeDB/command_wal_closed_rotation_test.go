package treedb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPublicCOWAutomaticCheckpointSyncsExactRotatedWALOnce(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stream    bool
		wantSyncs int
	}{
		{name: "ext4-stream", stream: true, wantSyncs: 1},
		{name: "fixed", wantSyncs: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPublicCOWAutomaticCheckpointSyncsExactRotatedWAL(t, tc.stream, tc.wantSyncs)
		})
	}
}

func testPublicCOWAutomaticCheckpointSyncsExactRotatedWAL(t *testing.T, stream bool, wantSyncs int) {
	t.Helper()
	dir := t.TempDir()
	if stream {
		file, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		ext4 := commitlog.IsExt4WALFilesystem(file)
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if !ext4 {
			t.Skip("requires ext4 for the production stream selector")
		}
	} else if !commitlog.FixedWALCreationSupported() {
		t.Skip("fixed WAL creation unsupported")
	}
	opts := commandWALDurabilityProofOptions(dir)
	ApplyProfile(&opts, ProfileCommandWALRelaxed)
	opts.MemtableMode = "cow_btree"
	opts.FreshRelaxedStandaloneWALStream = stream
	opts.BackgroundCheckpointInterval = time.Hour
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.CommandWALSegmentTargetBytes = 1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	stats := database.Stats()
	container := stats["treedb.command_wal.writer.container"]
	if stream && container != "stream" {
		t.Fatalf("production stream selector did not activate, container=%q", container)
	}
	if !stream && container != "fixed" {
		t.Skipf("test filesystem does not select the production fixed container, got %q", container)
	}
	key := []byte("held")
	if err := database.Set(key, []byte("one")); err != nil {
		t.Fatal(err)
	}
	const writerPathStat = "treedb.command_wal.writer.path"
	older := database.Stats()[writerPathStat]
	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing held reader")
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := database.Set(key, []byte("two")); err != nil {
		t.Fatal(err)
	}
	walDir := backenddb.WALDirPath(database.backend.Dir())
	target := database.Stats()[writerPathStat]
	if filepath.Dir(older) != walDir || filepath.Dir(target) != walDir || target == older {
		t.Fatalf("missing observed relaxed append rotation: older=%q target=%q", older, target)
	}
	oldInfo, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat active cutover segment: %v", err)
	}

	var observed struct {
		sync.Mutex
		files       map[string]int
		directories int
	}
	observed.files = make(map[string]int)
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		observed.Lock()
		defer observed.Unlock()
		switch event.Point {
		case durabilitycut.AfterNewFileDirectorySync:
			if event.Path == walDir {
				observed.directories++
			}
		case durabilitycut.AfterDependencyFileSync:
			if event.Path == target {
				if event.StablePath == nil {
					return fmt.Errorf("rotation target sync missing its exact open handle")
				}
				info, err := event.StablePath.Stat()
				if err != nil {
					return err
				}
				if !os.SameFile(oldInfo, info) {
					return fmt.Errorf("target sync used a different object: %s", event.Path)
				}
			}
			if filepath.Dir(event.Path) == walDir {
				observed.files[event.Path]++
			}
		}
		return nil
	})
	defer func() {
		if restore != nil {
			restore()
		}
	}()
	database.triggerAutoCheckpointForTest()
	if err := waitForPublicCommandWALAutoCheckpointCount(database, 1); err != nil {
		t.Fatal(err)
	}
	restore()
	restore = nil
	successor := database.Stats()[writerPathStat]
	if filepath.Dir(successor) != walDir || successor == target {
		t.Fatalf("checkpoint left target=%q active, successor=%q", target, successor)
	}
	observed.Lock()
	olderSyncs, targetSyncs, successorSyncs := observed.files[older], observed.files[target], observed.files[successor]
	directorySyncs := observed.directories
	observed.Unlock()
	if targetSyncs != wantSyncs || olderSyncs == 0 || successorSyncs == 0 || directorySyncs < 2 {
		t.Fatalf("physical fences: older=%d exact rotated=%d (want %d) successor=%d namespace=%d",
			olderSyncs, targetSyncs, wantSyncs, successorSyncs, directorySyncs)
	}
	if got := database.Stats()["treedb.command_wal.durable_wal_lsn"]; got != "2" {
		t.Fatalf("wal durable frontier=%q, want 2", got)
	}
	if got, err := held.Get(key); err != nil || string(got) != "one" {
		t.Fatalf("held read=%q, err=%v", got, err)
	}
	requireRawKVValue(t, database, key, []byte("two"))
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	database = reopened
	requireRawKVValue(t, database, key, []byte("two"))
}
