package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// Metadata discovery must not read reserve bytes; the exact-handle cleanup
// reader still validates every recognized non-value file, even outside the
// command lane. No classification survives to the next cleanup invocation.
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
			// Discovery preserves physical metadata only. The payload must still fail
			// the complete cleanup scan after an earlier successful invocation.
			if _, err = listSegmentsInDirWithLogicalSizes(WALDirPath(dir), false); err != nil {
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

// This isolated scan is also the syscall-count fixture: two fixed containers
// must each undergo one complete reserve validation, not listing plus reader.
func TestCommandWALCleanupSingleValidationLogicalAccounting(t *testing.T) {
	dir := t.TempDir()
	first := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
	second := writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeCommandWALCleanupDecisions(decisions)
	for _, path := range []string{first, second} {
		d := emptyFixedCleanupDecision(t, decisions, path)
		if d.Size != 8192 || d.LogicalSize != 0 || !d.Covered {
			t.Fatalf("physical/logical accounting: %+v", d)
		}
		if d.Active != (path == second) {
			t.Fatalf("active classification: %+v", d)
		}
	}
}
