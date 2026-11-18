package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALCleanupSingleValidationRejectsChangedReserve(t *testing.T) {
	for _, name := range []string{"inactive", "active", "legacy"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
			if name != "active" {
				writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
			}
			if name == "legacy" {
				target := filepath.Join(WALDirPath(dir), "wal-000001.log")
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				path = target
			}
			decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			closeCommandWALCleanupDecisions(decisions)
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.WriteAt([]byte{1}, 8191); err != nil {
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
			decisions, err = scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
			defer closeCommandWALCleanupDecisions(decisions)
			if !errors.Is(err, commitlog.ErrCorrupt) {
				t.Fatalf("changed reserve accepted: %v", err)
			}
			if _, err = os.Stat(path); err != nil {
				t.Fatalf("corrupt file removed: %v", err)
			}
		})
	}
}

func TestCommandWALCleanupCorruptReserveMissingLineageIsHardError(t *testing.T) {
	for _, operation := range []string{"explicit", "checkpoint", "prefix", "close"} {
		for _, advance := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advance=%t", operation, advance), func(t *testing.T) {
				dir := t.TempDir()
				d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				if stats := d.commandJournal.WriterBufferStats(); stats.Container != "fixed" {
					t.Skipf("requires production fixed container; selected %s (%s)", stats.Container, stats.ContainerReason)
				}
				if err = d.SetSync([]byte("k"), []byte("v")); err != nil {
					t.Fatal(err)
				}
				if operation != "checkpoint" || advance {
					if err = d.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				if advance {
					if err = d.SetSync([]byte("k"), []byte("next")); err != nil {
						t.Fatal(err)
					}
					if operation != "checkpoint" {
						if err = d.Checkpoint(); err != nil {
							t.Fatal(err)
						}
					}
				}
				proof, err := d.captureDurableWALCleanupProofV1()
				if err != nil {
					t.Fatal(err)
				}
				if advance && operation != "checkpoint" && proof.cleanupThrough == 0 {
					t.Fatal("expected nonzero cleanup frontier")
				}
				segments, err := listWALSegments(dir)
				if err != nil {
					t.Fatal(err)
				}
				var path string
				for _, s := range segments {
					if s.size > 0 && !s.valueLog {
						path = s.path
						break
					}
				}
				if path == "" {
					t.Fatal("missing framed segment")
				}
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				info, err := f.Stat()
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.WriteAt([]byte{1}, info.Size()-1); err != nil {
					t.Fatal(err)
				}
				if err = f.Close(); err != nil {
					t.Fatal(err)
				}
				beforeScans := d.commandWALCleanupScans.Load()
				switch operation {
				case "explicit":
					err = d.CleanupCommandWALCoveredSegments(false)
				case "checkpoint":
					err = d.Checkpoint()
				case "prefix":
					_, err = d.CleanupCommandWALCoveredPrefix()
				case "close":
					err = d.Close()
				}
				if operation == "checkpoint" && advance {
					afterProof, e := d.captureDurableWALCleanupProofV1()
					if e != nil || afterProof.cleanupThrough == 0 {
						t.Fatalf("checkpoint did not establish nonzero frontier: %+v %v", afterProof, e)
					}
				}
				if d.commandWALCleanupScans.Load() <= beforeScans {
					t.Fatal("operation did not invoke cleanup scan")
				}
				if !errors.Is(err, commitlog.ErrCorrupt) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
					t.Fatalf("corruption normalized away: %v", err)
				}
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("corrupt path removed: %v", statErr)
				}
			})
		}
	}
}
