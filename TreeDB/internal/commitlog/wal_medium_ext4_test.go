//go:build linux

package commitlog

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALMountIsExactExt4(t *testing.T) {
	for _, tc := range []struct {
		id, typeName string
		want         bool
	}{{"1", "ext4", true}, {"1", "ext2", false}, {"1", "ext3", false}, {"2", "overlay", false}, {"3", "ext4", false}} {
		line := "1 0 1:0 / /some\\040path rw shared:1 - " + tc.typeName + " /dev/sda rw\n"
		if got := walMountIsExt4(bufio.NewScanner(strings.NewReader(line)), tc.id); got != tc.want {
			t.Errorf("mount ID=%s type=%s: got %t want %t", tc.id, tc.typeName, got, tc.want)
		}
	}
}

func TestWALMediumExt4KeepsOccupiedContainersAndHeldSync(t *testing.T) {
	dir := t.TempDir()
	mount, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := IsExt4WALFilesystem(mount)
	if err := mount.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires a positively identified ext4 filesystem")
	}
	for _, tc := range []struct {
		name     string
		oldFixed bool
		stable   bool
	}{
		{"fixed-to-stream/ordinary", true, false},
		{"fixed-to-stream/stable", true, true},
		{"stream-to-fixed/ordinary", false, false},
		{"stream-to-fixed/stable", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wal := t.TempDir()
			path := filepath.Join(wal, CommandSegmentName(0, 1))
			var seed *Writer
			var err error
			if tc.oldFixed {
				seed, err = NewFixedWriterWithCapacity(path, 8192)
			} else {
				seed, err = NewWriter(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			seedFrame := fixedWALTestBarrier()
			seedFrame.LSN = 1
			if err := seed.AppendCommandV2(seedFrame); err != nil {
				_ = seed.Close()
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			old, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			opts := CommandJournalOptions{CaptureStableResources: tc.stable, PreferExt4Stream: tc.oldFixed, SegmentTargetBytes: 1024}
			journal, err := OpenCommandJournal(wal, opts)
			if err != nil {
				t.Fatal(err)
			}
			if (journal.writer.fixed != nil) != tc.oldFixed || journal.writer.containerSelection.reason != "existing_format" {
				t.Fatalf("occupied selection %+v", journal.writer.containerSelection)
			}
			t.Cleanup(func() { _ = journal.Close() })
			held, err := journal.CaptureSync(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			if tc.stable {
				rotation, err := journal.RotateActiveSegmentWithStableResources(false)
				if err != nil {
					t.Fatal(err)
				}
				rotation.Release()
			} else if err := journal.RotateActiveSegment(false); err != nil {
				t.Fatal(err)
			}
			if (journal.writer.fixed != nil) == tc.oldFixed || held.active.dataOnlySync != tc.oldFixed {
				t.Fatalf("new selection %+v old data-only=%t", journal.writer.containerSelection, held.active.dataOnlySync)
			}
			if err := held.active.token.WithPinnedFile(func(f *os.File) error {
				info, err := f.Stat()
				if err == nil && !os.SameFile(old, info) {
					t.Errorf("held sync lost the old inode")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := held.Sync(); err != nil {
				t.Fatal(err)
			}
			held.Release()
			if lsn, err := journal.AppendCommand(fixedWALTestBarrier()); err != nil || lsn != 2 {
				t.Fatalf("second LSN=%d err=%v", lsn, err)
			}
			if err := journal.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}

			opts.PreferExt4Stream = !opts.PreferExt4Stream
			journal, err = OpenCommandJournal(wal, opts)
			if err != nil {
				t.Fatal(err)
			}
			if (journal.writer.fixed != nil) == tc.oldFixed || journal.writer.containerSelection.reason != "existing_format" || journal.NextLSN() != 3 {
				t.Errorf("reopen selection %+v next LSN=%d", journal.writer.containerSelection, journal.NextLSN())
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
