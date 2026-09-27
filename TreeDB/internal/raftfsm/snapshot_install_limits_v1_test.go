package raftfsm

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestRaftSnapshotInstallLimitsRefuseBeforeEntryWriteV1(t *testing.T) {
	for _, test := range []struct {
		name   string
		limits SnapshotCaptureLimitsV1
		size   int64
	}{
		{"bytes", SnapshotCaptureLimitsV1{MaxBytes: 1}, 2},
		{"staging", SnapshotCaptureLimitsV1{MaxStagingBytes: 1}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			var archive bytes.Buffer
			writer := tar.NewWriter(&archive)
			if err := writer.WriteHeader(&tar.Header{Name: "db/payload", Size: test.size, Mode: 0600}); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write([]byte("xx")); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			main, side, apply := t.TempDir(), t.TempDir(), t.TempDir()
			limits, err := test.limits.normalized()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := extractRaftSnapshotArchiveWithLimitsV1(context.Background(), &archive, main, side, apply, limits); err == nil {
				t.Fatal("limit accepted")
			}
			if _, err := os.Stat(filepath.Join(main, "payload")); !os.IsNotExist(err) {
				t.Fatal("overlimit entry written", err)
			}
		})
	}
}

func TestRaftSnapshotInstallCleanupFailureRetainsAdmissionV1(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	dir := t.TempDir()
	db := openRaftSnapshotFSMTestDB(t, dir, true)
	defer db.Close()
	fsm := openRaftSnapshotFSMForTest(t, db, dir, true)
	defer fsm.Close()
	denied := errors.New("install scratch cleanup denied")
	var targets []string
	raftSnapshotBeforeCleanupForTest = func(path string) error { targets = append(targets, path); return denied }
	defer func() { raftSnapshotBeforeCleanupForTest = nil }()
	if err := fsm.InstallRaftSnapshotV1(strings.NewReader("malformed archive")); !errors.Is(err, denied) {
		t.Fatal("cleanup failure lost", err)
	}
	if !fsm.snapshotOperationActive.Load() || len(targets) != 3 {
		t.Fatalf("unowned scratch: active=%v targets=%v", fsm.snapshotOperationActive.Load(), targets)
	}
	if _, err := fsm.CaptureRaftSnapshotV1(); !errors.Is(err, denied) {
		t.Fatal("admission forgot cleanup", err)
	}
	raftSnapshotBeforeCleanupForTest = nil
	if err := fsm.Close(); err != nil {
		t.Fatal(err)
	}
	if fsm.snapshotOperationActive.Load() {
		t.Fatal("Close failed cleanup retry")
	}
	for _, target := range targets {
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatal("scratch remains", target, err)
		}
	}
}

func TestRaftSnapshotScratchCleanupSyncDebtV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		return
	}
	parent := t.TempDir()
	path := filepath.Join(parent, "scratch")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := raftSnapshotScratchDirsV1{main: path}
	// A parent moved away after unlink must keep the cleanup target, even
	// though RemoveAll is now an idempotent no-op. Restore it for retry.
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	moved := parent + "-moved"
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(moved, parent)
	if err := scratch.close(); err == nil || scratch.main == "" {
		t.Fatal("lost namespace sync debt", err)
	}
	if err := os.Rename(moved, parent); err != nil {
		t.Fatal(err)
	}
	if err := scratch.close(); err != nil || scratch.main != "" {
		t.Fatal("cleanup retry", err)
	}
}

// Close must retain the install owner until the actual blocking Read returns;
// cancellation alone is not permission to unlink its scratch directories.
func TestRaftSnapshotInstallCloseWaitsForBlockedReaderV1(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	fsm := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer fsm.Close()
	reader := &snapshotInstallBlockingReaderV1{entered: make(chan struct{}), resume: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(reader.resume) }) }
	defer unblock()
	installed, closed := make(chan error, 1), make(chan error, 1)
	go func() { installed <- fsm.InstallRaftSnapshotV1(reader) }()
	select {
	case <-reader.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("install reader not entered")
	}
	if _, err := fsm.CaptureRaftSnapshotV1(); err == nil {
		t.Fatal("concurrent export admitted")
	}
	go func() { closed <- fsm.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		fsm.snapshotMu.Lock()
		detached := fsm.snapshotNamespace == nil
		fsm.snapshotMu.Unlock()
		if detached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not revoke namespace")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatal("Close passed blocked install", err)
	default:
	}
	if !fsm.snapshotOperationActive.Load() {
		t.Fatal("active install lost admission")
	}
	unblock()
	select {
	case err := <-installed:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("install cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("install did not return")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}
	if fsm.snapshotOperationActive.Load() {
		t.Fatal("completed cleanup retained admission")
	}
}

type snapshotInstallBlockingReaderV1 struct {
	entered, resume chan struct{}
	once            sync.Once
}

func (r *snapshotInstallBlockingReaderV1) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.resume
	return 0, io.EOF
}

func TestRaftSnapshotInstallLimitsCountImplicitDirectoriesV1(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "db/a/b/payload", Size: 1, Mode: 0600}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	main, side, apply := t.TempDir(), t.TempDir(), t.TempDir()
	limits, err := (SnapshotCaptureLimitsV1{MaxFiles: 2}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := extractRaftSnapshotArchiveWithLimitsV1(context.Background(), &archive, main, side, apply, limits); err == nil || !strings.Contains(err.Error(), "path limit") {
		t.Fatal("implicit directories bypassed limit", err)
	}
	entries, err := os.ReadDir(main)
	if err != nil || len(entries) != 0 {
		t.Fatal("overlimit paths created", entries, err)
	}
}

func TestRaftSnapshotInstalledVerificationCancelsNonemptyDigestV1(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	fsm := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer fsm.Close()
	applySnapshotSourceEntries(t, fsm, []byte(`{"_id":"row","value":"snapshot"}`))
	snapshot, err := fsm.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if err := fsm.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &snapshotInstallCancelContextV1{Context: ctx, cancel: cancel}
	if err := fsm.VerifyInstalledSnapshotManifestWithContextV1(probe, snapshot.Manifest); !errors.Is(err, context.Canceled) {
		t.Fatal("digest cancellation", err)
	}
	if probe.calls < 5 {
		t.Fatal("did not reach nonempty digest", probe.calls)
	}
	if err := fsm.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatal("verification mutated state", err)
	}
}

type snapshotInstallCancelContextV1 struct {
	context.Context
	cancel context.CancelFunc
	calls  int
}

func (c *snapshotInstallCancelContextV1) Err() error {
	c.calls++
	if c.calls == 5 {
		c.cancel()
	}
	return c.Context.Err()
}
