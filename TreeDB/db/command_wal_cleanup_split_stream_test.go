package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func prepareSplitStreamCleanupTest(t *testing.T, covered, durable uint64) (*DB, durableWALCleanupProofV1, []string) {
	t.Helper()
	db := &DB{dir: t.TempDir(), commandWAL: true, durability: DurabilityDurable}
	var sealed []string
	for lsn := uint64(1); lsn <= durable; lsn++ {
		writeCommandWALFrame(t, db.dir, lsn, lsn)
		sealed = append(sealed, filepath.Join(WALDirPath(db.dir), commitlog.CommandSegmentName(0, lsn)))
	}
	installCommandWALCleanupTwoRootRuntimeForTest(db, 1, covered)
	db.commandWALDurableLSN.Store(durable)
	installCommandWALCleanupJournalOwnerForTest(t, db, durable)
	proof, err := db.captureDurableWALCleanupProofV1()
	if err != nil {
		t.Fatal(err)
	}
	return db, proof, sealed
}

// The actual split scanner can validate a small stream fixture on one
// volume. The Linux placement test exercises automatic activation.
func TestCommandWALCleanupSplitStreamPrefix(t *testing.T) {
	t.Run("ordinary append and retained replay", func(t *testing.T) {
		db, proof, sealed := prepareSplitStreamCleanupTest(t, 2, 3)
		var visited []string
		scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, func(path string) {
			visited = append(visited, path)
			isSealed := len(visited) <= len(sealed)
			if db.commandJournal.AppendLockAvailableForTesting() != isSealed {
				t.Errorf("unexpected append lock availability for %q", path)
				return
			}
			if len(visited) == 1 {
				if _, err := db.commandJournal.AppendRawKVPointCommandTrustedWithRotateSync(3, commitlog.RawKVOpSet, []byte("successor"), []byte("value"), true); err != nil {
					t.Fatal(err)
				}
				if err := db.commandJournal.Flush(); err != nil {
					t.Fatal(err)
				}
			}
		})
		attempted, scanErr := db.scanSplitCommandWALCleanupPrefix(proof, scan, os.Open, 1)
		decisions, err := scan.finish(scanErr)
		defer closeCommandWALCleanupDecisions(decisions)
		if !attempted || err != nil || !slices.Equal(visited, append(slices.Clone(sealed), proof.journal.ActivePath)) {
			t.Fatalf("cleanup attempted=%t visited=%q err=%v", attempted, visited, err)
		}
		if len(decisions) < 4 {
			t.Fatalf("only %d decisions for nonempty WAL segments", len(decisions))
		}
		for i, path := range sealed {
			d := decisions[i]
			covered := i < 2
			if d.Path != path || d.Frames != 1 || d.Active || d.Covered != covered ||
				(d.file != nil) != covered || (d.validatedExtent != nil) != covered {
				t.Errorf("unexpected validated stream decision: %+v", d)
			}
		}
		if d := decisions[3]; d.Path != proof.journal.ActivePath || !d.Active || d.Covered || d.Frames != 1 || d.MaxLSN != 4 {
			t.Errorf("capture-active successor did not retain its appended frame: %+v", d)
		}
	})
	t.Run("rotation closes staged readers and restarts", func(t *testing.T) {
		db, proof, sealed := prepareSplitStreamCleanupTest(t, 2, 3)
		var visited []string
		var opened []*os.File
		scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, func(path string) {
			visited = append(visited, path)
			if len(visited) != 1 {
				return
			}
			if !db.commandJournal.AppendLockAvailableForTesting() {
				t.Fatal("rotation must start during detached validation")
			}
			if err := db.commandJournal.RotateActiveSegment(true); err != nil {
				t.Fatal(err)
			}
		})
		attempted, scanErr := db.scanSplitCommandWALCleanupPrefix(proof, scan, func(path string) (*os.File, error) {
			file, err := os.Open(path)
			if err == nil {
				opened = append(opened, file)
			}
			return file, err
		}, 1)
		checkClosedSplitStreamReaders(t, opened, len(sealed))
		if attempted || scanErr != nil || len(scan.decisions) != 0 || !slices.Equal(visited, sealed[:1]) {
			t.Fatalf("yielded cleanup attempted=%t decisions=%+v visited=%q err=%v", attempted, scan.decisions, visited, scanErr)
		}
		active, _ := db.commandWALActiveSegmentSnapshot()
		decisions, err := scan.finish(db.commandJournal.WithCleanupScan(proof.journal, func() error {
			return scan.scanLockedDirectory(db.dir)
		}))
		if closeErr := closeCommandWALCleanupDecisions(decisions); closeErr != nil {
			t.Fatal(closeErr)
		}
		wantScans := append([]string{sealed[0]}, sealed...)
		wantScans = append(wantScans, proof.journal.ActivePath, active)
		if err != nil || active == proof.journal.ActivePath || !slices.Equal(visited, wantScans) {
			t.Fatalf("locked retry visited=%q want=%q err=%v", visited, wantScans, err)
		}
	})
	t.Run("rotation preserves unrelated validation errors", func(t *testing.T) {
		db, proof, _ := prepareSplitStreamCleanupTest(t, 2, 2)
		readErr := errors.New("independent WAL read failure")
		var scan *commandWALCleanupScan
		scan = newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, func(path string) {
			if !db.commandJournal.AppendLockAvailableForTesting() {
				t.Fatal("fault must occur during detached validation")
			}
			scan.hardErr = fmt.Errorf("scan %s: %w", path, errors.Join(os.ErrClosed, readErr, commitlog.ErrCorrupt))
			if err := db.commandJournal.RotateActiveSegment(true); err != nil {
				t.Fatal(err)
			}
		})
		attempted, scanErr := db.scanSplitCommandWALCleanupPrefix(proof, scan, os.Open, 1)
		if !attempted || !errors.Is(scanErr, readErr) || !errors.Is(scanErr, commitlog.ErrCorrupt) {
			t.Fatalf("yield hid unrelated failure: attempted=%t error=%v", attempted, scanErr)
		}
	})
	t.Run("hard corruption before suffix", func(t *testing.T) {
		db, proof, sealed := prepareSplitStreamCleanupTest(t, 2, 3)
		last := sealed[len(sealed)-1]
		contents, err := os.ReadFile(last)
		if err != nil {
			t.Fatal(err)
		}
		contents[len(contents)-1] ^= 1
		if err := os.WriteFile(last, contents, 0600); err != nil {
			t.Fatal(err)
		}
		var visited []string
		var opened []*os.File
		scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, func(path string) {
			visited = append(visited, path)
		})
		attempted, scanErr := db.scanSplitCommandWALCleanupPrefix(proof, scan, func(path string) (*os.File, error) {
			file, err := os.Open(path)
			if err == nil {
				opened = append(opened, file)
			}
			return file, err
		}, 1)
		_, err = scan.finish(scanErr)
		if !attempted || !errors.Is(err, commitlog.ErrCorrupt) || !slices.Equal(visited, sealed) {
			t.Fatalf("corrupt cleanup attempted=%t visited=%q err=%v", attempted, visited, err)
		}
		checkClosedSplitStreamReaders(t, opened, len(sealed))
	})
	t.Run("rotation already visible at staging", func(t *testing.T) {
		db, proof, _ := prepareSplitStreamCleanupTest(t, 2, 2)
		if err := db.commandJournal.RotateActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, func(path string) {
			t.Fatalf("unexpected scan before locked fallback of %q", path)
		})
		if attempted, err := db.scanSplitCommandWALCleanupPrefix(proof, scan, os.Open, 1); attempted || err != nil {
			t.Fatalf("rotation before staging: attempted=%t err=%v", attempted, err)
		}
	})
}

func checkClosedSplitStreamReaders(t *testing.T, opened []*os.File, want int) {
	t.Helper()
	if len(opened) != want {
		t.Fatalf("cleanup opened %d handles, want %d", len(opened), want)
	}
	for _, file := range opened {
		if _, err := file.Seek(0, io.SeekCurrent); !errors.Is(err, os.ErrClosed) {
			t.Errorf("cleanup leaked %q: %v", file.Name(), err)
		}
	}
}
