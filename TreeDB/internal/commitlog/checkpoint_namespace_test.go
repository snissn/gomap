package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCheckpointSuccessorDirectoryErrorAndFallback(t *testing.T) {
	for _, preexisting := range []bool{false, true} {
		name := "fresh"
		if preexisting {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			j, err := OpenCommandJournal(dir, CommandJournalOptions{PreferExt4Stream: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = j.Close() })
			if !IsExt4WALFilesystem(j.writer.f) {
				t.Skip("requires the ext4 stream selector")
			}
			if _, err := j.AppendCommand(fixedWALTestBarrier()); err != nil {
				t.Fatal(err)
			}
			if preexisting {
				path := filepath.Join(dir, CommandSegmentName(j.lane, j.segmentSeq+1))
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			failure := errors.New("directory barrier rejected")
			attempts := 0
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point != durabilitycut.BeforeNewFileDirectorySync || e.Path != dir {
					return nil
				}
				attempts++
				if preexisting {
					return nil
				}
				if e.StablePath != j.stableParent {
					t.Error("directory barrier failed to use the retained parent")
				}
				return failure
			})
			defer restore()
			finish, err := j.RotateAtAutomaticCOWCut()
			if err != nil {
				t.Fatal(err)
			}
			if preexisting {
				if finish != nil || attempts != 1 {
					t.Fatalf("pre-existing successor: handoff=%t, inline syncs=%d", finish != nil, attempts)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if finish == nil || attempts != 0 {
				t.Fatalf("fresh successor: handoff=%t, inline syncs=%d", finish != nil, attempts)
			}
			next := j.NextLSN()
			lsn, appendErr := j.AppendCommand(fixedWALTestBarrier())
			if !errors.Is(appendErr, failure) || lsn != 0 || j.NextLSN() != next {
				t.Fatalf("durable caller: lsn=%d, next=%d, err=%v", lsn, j.NextLSN(), appendErr)
			}
			if !errors.Is(finish(), failure) || !errors.Is(j.Sync(), failure) || !errors.Is(j.Close(), failure) || attempts != 1 {
				t.Fatalf("terminal result: original error=%v, physical attempts=%d", j.fileSync.err(), attempts)
			}
		})
	}
}
