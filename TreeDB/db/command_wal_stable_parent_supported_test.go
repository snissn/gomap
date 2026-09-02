//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestPublicCommandWALRotationRefreshesParentForLaterStableCapture(t *testing.T) {
	root := t.TempDir()
	database, err := Open(Options{Dir: root, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	walDir := WALDirPath(root)
	originalDir := filepath.Join(root, "wal-original")
	if err := os.Rename(walDir, originalDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}

	// This is the public checkpoint-relevant ordinary rotation route.
	if err := database.RotateCommandWALActiveSegment(false); err != nil {
		t.Fatal(err)
	}
	movedReplacementDir := filepath.Join(root, "wal-replacement-moved")
	if err := os.Rename(walDir, movedReplacementDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rotation, err := database.commandJournal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	rotation.Release()

	secondSuccessor := commitlog.CommandSegmentName(0, 2)
	if _, err := os.Stat(filepath.Join(movedReplacementDir, secondSuccessor)); err != nil {
		t.Fatalf("stable successor missing from public rotation's exact parent: %v", err)
	}
	for _, wrong := range []string{
		filepath.Join(originalDir, secondSuccessor),
		filepath.Join(walDir, secondSuccessor),
	} {
		if _, err := os.Stat(wrong); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stable successor escaped to %q: %v", wrong, err)
		}
	}
}
