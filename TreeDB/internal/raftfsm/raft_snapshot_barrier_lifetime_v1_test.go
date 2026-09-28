package raftfsm

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func holdRaftSnapshotStorageBarrierV1(t *testing.T, root string) func() {
	t.Helper()
	entered, unblock := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- collections.WithVectorPartitionStorageBarrierV1(root, func() error {
			close(entered)
			<-unblock
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("storage barrier holder did not start")
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			close(unblock)
			if err := <-done; err != nil {
				t.Errorf("release storage barrier: %v", err)
			}
		})
	}
	t.Cleanup(release)
	return release
}

func waitRaftSnapshotOperationAdmissionV1(t *testing.T, fsm *FSM) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !fsm.snapshotOperationActive.Load() {
		if time.Now().After(deadline) {
			t.Fatal("snapshot operation did not reach admission")
		}
		time.Sleep(time.Millisecond)
	}
}

func requireRaftSnapshotStagingEmptyV1(t *testing.T, fsm *FSM) {
	t.Helper()
	entries, err := os.ReadDir(raftSnapshotStagingDirV1(fsm.cluster.Layout.SnapshotDir))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("snapshot staging not empty after cancellation: %v", entries)
	}
}

func TestRaftSnapshotCaptureBarrierWaitHonorsLifetimeV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	fsm := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer fsm.Close()
	applySnapshotSourceEntries(t, fsm, []byte(`{"_id":"u-large","name":"before"}`))
	fsm.snapshotCaptureLimits.Lifetime = 250 * time.Millisecond
	release := holdRaftSnapshotStorageBarrierV1(t, database.Dir())
	done := make(chan error, 1)
	go func() {
		snapshot, err := fsm.CaptureRaftSnapshotV1()
		if err == nil {
			err = snapshot.Release()
		}
		done <- err
	}()
	waitRaftSnapshotOperationAdmissionV1(t, fsm)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("barrier wait returned %v, want lifetime deadline", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("capture remained blocked after lifetime expiry")
	}
	if fsm.snapshotOperationActive.Load() {
		t.Fatal("expired capture retained admission")
	}
	requireRaftSnapshotStagingEmptyV1(t, fsm)
	release()
	fsm.snapshotCaptureLimits.Lifetime = 0
	snapshot, err := fsm.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal("capture after released barrier", err)
	}
	if err := snapshot.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRaftSnapshotInstallBarrierWaitHonorsLifetimeV1(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer sourceDB.Close()
	source := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer source.Close()
	doc := []byte(`{"_id":"u-large","name":"installed"}`)
	applySnapshotSourceEntries(t, source, doc)
	snapshot, err := source.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	archive := readRaftSnapshotArchiveForTest(t, snapshot)

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, true)
	defer targetDB.Close()
	target := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer target.Close()
	stale := []byte(`{"_id":"u-large","name":"original"}`)
	applySnapshotSourceEntries(t, target, stale)
	target.snapshotCaptureLimits.Lifetime = 250 * time.Millisecond
	release := holdRaftSnapshotStorageBarrierV1(t, targetDB.Dir())
	done := make(chan error, 1)
	go func() { done <- target.InstallRaftSnapshotV1(bytes.NewReader(archive)) }()
	waitRaftSnapshotOperationAdmissionV1(t, target)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("barrier wait returned %v, want lifetime deadline", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("install remained blocked after lifetime expiry")
	}
	if target.snapshotOperationActive.Load() {
		t.Fatal("expired install retained admission")
	}
	requireRaftSnapshotStagingEmptyV1(t, target)
	assertSnapshotDocument(t, target, "u-large", stale)
	release()
	target.snapshotCaptureLimits.Lifetime = 0
	if err := target.InstallRaftSnapshotV1(bytes.NewReader(archive)); err != nil {
		t.Fatal("install after released barrier", err)
	}
	assertSnapshotDocument(t, target, "u-large", doc)
}
