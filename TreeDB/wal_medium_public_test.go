package treedb

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestPublicCommandWALMemoryMediumSelectionAndSyncFailure(t *testing.T) {
	dir, err := os.MkdirTemp("/dev/shm", "treedb-public-medium-*")
	if err != nil {
		t.Skipf("memory filesystem unavailable: %v", err)
	}
	defer os.RemoveAll(dir)
	opts := commandWALDurabilityProofOptions(dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	stats := database.Stats()
	fsType, probeErr := strconv.ParseUint(stats["treedb.command_wal.writer.filesystem_type"], 10, 64)
	if probeErr != nil || (fsType != 0x01021994 && fsType != 0x858458f6) {
		t.Skipf("actual opened journal is not positively recognized memory filesystem: type=%s reason=%s", stats["treedb.command_wal.writer.filesystem_type"], stats["treedb.command_wal.writer.container_reason"])
	}
	if stats["treedb.command_wal.writer.container"] != "stream" || stats["treedb.command_wal.writer.container_reason"] != "memory_filesystem" || stats["treedb.command_wal.writer.memory_filesystem"] != "true" {
		t.Fatalf("actual creation=%v/%v/%v", stats["treedb.command_wal.writer.container"], stats["treedb.command_wal.writer.container_reason"], stats["treedb.command_wal.writer.filesystem_type"])
	}
	value := bytes.Repeat([]byte("pointer"), 12000)
	if err = database.SetSync([]byte("accepted"), value); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if got, e := database.Get([]byte("accepted")); e != nil || !bytes.Equal(got, value) {
		t.Fatalf("reopen bytes=%d err=%v", len(got), e)
	}
	if database.Stats()["treedb.command_wal.writer.container"] != "stream" {
		t.Fatal("reopen converted stream")
	}
	injected := errors.New("stream physical sync rejected")
	after := 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && filepath.IsAbs(e.Path) && len(e.Path) >= len(dir) && e.Path[:len(dir)] == dir {
			if e.Point == durabilitycut.BeforeDependencyFileSync {
				return injected
			}
			if e.Point == durabilitycut.AfterDependencyFileSync {
				after++
			}
		}
		return nil
	})
	err = database.SetSync([]byte("rejected"), []byte("must-not-ack"))
	restore()
	if !errors.Is(err, injected) || after != 0 {
		t.Fatalf("sync failure acknowledged: err=%v after=%d", err, after)
	}
	if err = database.SetSync([]byte("after-failure"), nil); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("unpoisoned error=%v", err)
	}
}
