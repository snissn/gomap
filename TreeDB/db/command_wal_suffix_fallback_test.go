package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALV2RepairPreservesReplayFromOlderRoot(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	seed := func(lsn uint64) {
		t.Helper()
		frame := mustCommandWALV2Frame(t, lsn, commitlog.CommandDurabilityDurable, nil)
		env, err := commitlog.DecodeCommandFrameV2(frame)
		if err != nil {
			t.Fatal(err)
		}
		if err := applyRawKVCommandWALFrame(database, env, nil, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	seed(1)
	olderPage := database.metaPageID
	seed(2)
	newerPage := database.metaPageID
	if newerPage == olderPage {
		t.Fatal("fixture did not publish two distinct recovery slots")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	enableCommandWALFormat(t, dir)
	walDir := WALDirPath(dir)
	a := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	b := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	disposable := filepath.Join(walDir, commitlog.CommandSegmentName(2, 1))
	writeCommandWALV2Segment(t, a, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	writeCommandWALV2Segment(t, b, mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityDurable, nil))
	keepB := mustReadFile(t, b)
	originalB, err := os.Stat(b)
	if err != nil {
		t.Fatal(err)
	}
	appendCommandWALV2Segment(t, a, mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil))
	appendCommandWALV2Segment(t, b, mustCommandWALV2Frame(t, 4, commitlog.CommandDurabilityRelaxed, nil))
	writeCommandWALV2Segment(t, disposable, mustCommandWALV2Frame(t, 5, commitlog.CommandDurabilityRelaxed, nil))
	if err := os.Truncate(a, int64(len(mustReadFile(t, a))-1)); err != nil {
		t.Fatal(err)
	}

	repaired := openCommandWALDB(t, dir)
	defer func() { _ = repaired.Close() }()
	if got := repaired.State().AppliedCommandLSN; got != 2 || repaired.metaPageID != newerPage {
		t.Fatalf("repair selected the wrong root: LSN=%d page=%d", got, repaired.metaPageID)
	}
	assertDBValue(t, repaired, "k-1", "v")
	assertDBValue(t, repaired, "k-2", "v")
	for lsn := 3; lsn <= 5; lsn++ {
		if got, err := repaired.Has([]byte(fmt.Sprintf("k-%d", lsn))); err != nil || got {
			t.Fatalf("discarded LSN %d still visible: found=%v err=%v", lsn, got, err)
		}
	}
	if err := repaired.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disposable); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("suffix-only file survived repair: %v", err)
	}
	corruptIndexPageByte(t, dir, newerPage)
	fallback := openCommandWALDB(t, dir)
	defer func() { _ = fallback.Close() }()
	if got := fallback.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("older root failed to replay through LSN 2: recovered through %d", got)
	}
	assertDBValue(t, fallback, "k-1", "v")
	assertDBValue(t, fallback, "k-2", "v")
	if err := fallback.Close(); err != nil {
		t.Fatal(err)
	}
	if afterB, err := os.Stat(b); err != nil || !os.SameFile(originalB, afterB) || !bytes.Equal(mustReadFile(t, b), keepB) {
		t.Fatalf("repair did not preserve the original lane-1 prefix and identity: %v", err)
	}
}
