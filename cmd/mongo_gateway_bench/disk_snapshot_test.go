package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCollectDiskSnapshotMissingRootErrorsAndConcurrentDeleteSafe(t *testing.T) {
	t.Run("normal_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.db"), []byte("12345"), 0o600); err != nil {
			t.Fatal(err)
		}
		vlogDir := filepath.Join(dir, "maindb", "leaf_vlog")
		if err := os.MkdirAll(vlogDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(vlogDir, "value-l255-000001.log"), []byte("abcdef"), 0o600); err != nil {
			t.Fatal(err)
		}

		snap, err := collectDiskSnapshot(dir)
		if err != nil {
			t.Fatalf("collectDiskSnapshot(%q): %v", dir, err)
		}
		if snap.TotalBytes != 11 {
			t.Fatalf("TotalBytes=%d want 11", snap.TotalBytes)
		}
		if got := snap.Paths["index.db"]; got != 5 {
			t.Fatalf("Paths[index.db]=%d want 5", got)
		}
		if got := snap.Paths["maindb/leaf_vlog"]; got != 6 {
			t.Fatalf("Paths[maindb/leaf_vlog]=%d want 6", got)
		}
		if got := snap.Paths["maindb/leaf_vlog/value-l255-000001.log"]; got != 6 {
			t.Fatalf("Paths[maindb/leaf_vlog/value-l255-000001.log]=%d want 6", got)
		}
	})

	t.Run("missing_root_errors", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "does-not-exist")
		_, err := collectDiskSnapshot(missing)
		if err == nil {
			t.Fatalf("expected error for missing root %q", missing)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("collectDiskSnapshot(%q) err=%v want fs.ErrNotExist", missing, err)
		}
	})

	t.Run("concurrent_delete_safe", func(t *testing.T) {
		dir := t.TempDir()
		stableContent := []byte("stable-index-bytes")
		if err := os.WriteFile(filepath.Join(dir, "index.db"), stableContent, 0o600); err != nil {
			t.Fatal(err)
		}
		vlogDir := filepath.Join(dir, "maindb", "leaf_vlog")
		if err := os.MkdirAll(vlogDir, 0o700); err != nil {
			t.Fatal(err)
		}

		// Pre-populate several segment files so WalkDir always has plenty of entries to visit.
		const numSlots = 32
		payload := []byte("segment-payload-0123456789")
		for i := 0; i < numSlots; i++ {
			p := filepath.Join(vlogDir, fmt.Sprintf("value-l255-%06d.log", i))
			if err := os.WriteFile(p, payload, 0o600); err != nil {
				t.Fatal(err)
			}
		}

		var stop atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: continuously delete and recreate leaf_vlog files to race with WalkDir/entry.Info.
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				slot := i % numSlots
				p := filepath.Join(vlogDir, fmt.Sprintf("value-l255-%06d.log", slot))
				_ = os.Remove(p)
				_ = os.WriteFile(p, payload, 0o600)
			}
		}()

		// Goroutine 2: continuously create and remove a transient subdirectory to race with WalkDir ReadDir/Lstat.
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				sub := filepath.Join(dir, "maindb", fmt.Sprintf("ephemeral-%02d", i%8))
				_ = os.MkdirAll(sub, 0o700)
				_ = os.WriteFile(filepath.Join(sub, "seg.log"), payload, 0o600)
				_ = os.RemoveAll(sub)
			}
		}()

		const walks = 250
		for i := 0; i < walks; i++ {
			snap, err := collectDiskSnapshot(dir)
			if err != nil {
				stop.Store(true)
				wg.Wait()
				t.Fatalf("collectDiskSnapshot walk %d failed under concurrent deletes: %v", i, err)
			}
			if snap.TotalBytes < int64(len(stableContent)) {
				stop.Store(true)
				wg.Wait()
				t.Fatalf("collectDiskSnapshot walk %d TotalBytes=%d < stable %d", i, snap.TotalBytes, len(stableContent))
			}
			if snap.Paths["index.db"] != int64(len(stableContent)) {
				stop.Store(true)
				wg.Wait()
				t.Fatalf("collectDiskSnapshot walk %d Paths[index.db]=%d want %d", i, snap.Paths["index.db"], len(stableContent))
			}
		}

		stop.Store(true)
		wg.Wait()
	})
}
