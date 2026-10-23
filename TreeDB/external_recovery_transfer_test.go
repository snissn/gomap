package treedb

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func externalTransferOwner(t *testing.T, dir, identity string) *ExternalRecovery {
	t.Helper()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	o, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	return o
}

func externalTransferFixture(t *testing.T) ([]byte, ExternalRecoveryCheckpoint, []byte) {
	t.Helper()
	source := externalTransferOwner(t, t.TempDir(), "group-7/source-node-1")
	want := bytes.Repeat([]byte("certified-peer-pointer"), 4096)
	externalTestSet(t, source, "key", want)
	cert, err := source.Checkpoint(context.Background(), []byte("group=7;index=42;term=3;raft=1"))
	if err != nil {
		t.Fatal(err)
	}
	externalTestSet(t, source, "key", []byte("uncertified future"))
	externalTestSet(t, source, "suffix", []byte("uncertified future"))
	if err := source.DB().Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := source.ExportCheckpoint(context.Background(), &archive, cert.Generation, cert.Digest); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes(), cert, want
}

func TestExternalRecoveryTransferExactRootAndReceiverAuthority(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	archive, source, want := externalTransferFixture(t)
	dir := t.TempDir()
	target := externalTransferOwner(t, dir, "group-7/receiver-node-2")
	externalTestSet(t, target, "key", []byte("receiver-before-install"))
	previous, err := target.Checkpoint(context.Background(), []byte("receiver-before-install"))
	if err != nil {
		t.Fatal(err)
	}
	oldDB := target.DB()
	called := false
	receiverMetadata := []byte("group=7;index=42;term=3;raft=2")
	installed, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(archive), func(received ExternalRecoveryCheckpoint) ([]byte, error) {
		called = true
		if received.Digest != source.Digest || received.Identity != source.Identity || !bytes.Equal(received.Metadata, source.Metadata) {
			return nil, errors.New("snapshot semantic authority mismatch")
		}
		return receiverMetadata, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || installed.Identity != target.config.Identity || installed.PreviousDigest != previous.Digest || installed.DataDirectory == previous.DataDirectory || !bytes.Equal(installed.Metadata, receiverMetadata) || installed.Root != source.Root {
		t.Fatalf("invalid receiver certificate: %+v", installed)
	}
	if target.DB() == oldDB {
		t.Fatal("install did not replace DB pointer")
	}
	got, err := target.DB().Get([]byte("key"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("installed value bytes=%d err=%v", len(got), err)
	}
	if found, err := target.DB().Has([]byte("suffix")); err != nil || found {
		t.Fatalf("uncertified transferred suffix found=%v err=%v", found, err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target = externalTransferOwner(t, dir, "group-7/receiver-node-2")
	got, err = target.DB().Get([]byte("key"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reopened installed value bytes=%d err=%v", len(got), err)
	}
	for i := 0; i < 3; i++ {
		if _, err := target.Checkpoint(context.Background(), receiverMetadata); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(dir, "checkpoints"))
	if err != nil || len(entries) != 2 {
		t.Fatalf("retained certificate generations=%d err=%v", len(entries), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "working")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired receiver namespace remains: %v", err)
	}
}

func TestExternalRecoveryTransferRefusalPreservesOldOwner(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	archive, _, _ := externalTransferFixture(t)
	target := externalTransferOwner(t, t.TempDir(), "receiver")
	externalTestSet(t, target, "key", []byte("old"))
	old, err := target.Checkpoint(context.Background(), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	oldDB := target.DB()
	refused := errors.New("unaccepted group/index/term")
	if _, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(archive), func(ExternalRecoveryCheckpoint) ([]byte, error) { return nil, refused }); !errors.Is(err, refused) {
		t.Fatal(err)
	}
	if _, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(archive[:len(archive)/2]), func(ExternalRecoveryCheckpoint) ([]byte, error) {
		t.Error("authorization called before complete archive validation")
		return nil, nil
	}); err == nil {
		t.Fatal("truncated snapshot accepted")
	}
	if _, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(archive), nil); !errors.Is(err, ErrExternalRecovery) {
		t.Fatal(err)
	}
	current, _ := target.RecoveredCheckpoint()
	if current.Digest != old.Digest || target.DB() != oldDB {
		t.Fatal("preparation refusal changed receiver authority")
	}
	if err := target.Apply(func() error { return target.DB().Set([]byte("still-usable"), []byte("yes")) }); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := target.ExportCheckpoint(context.Background(), &output, old.Generation, "wrong-digest"); !errors.Is(err, ErrExternalRecovery) || output.Len() != 0 {
		t.Fatalf("stale export err=%v bytes=%d", err, output.Len())
	}
}

func TestExternalRecoveryTransferRejectsTraversalBeforeWriting(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	_, source, _ := externalTransferFixture(t)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	raw, _ := json.Marshal(source)
	if err := writer.WriteHeader(&tar.Header{Name: "source.json", Size: int64(len(raw)), Mode: 0600}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteHeader(&tar.Header{Name: "db/../../escape", Size: 1, Mode: 0600}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	target := externalTransferOwner(t, t.TempDir(), "receiver")
	if _, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(archive.Bytes()), func(ExternalRecoveryCheckpoint) ([]byte, error) { return nil, nil }); !errors.Is(err, ErrExternalRecovery) {
		t.Fatal(err)
	}
}

func TestExternalRecoveryTransferCrashCuts(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	archive, _, want := externalTransferFixture(t)
	archivePath := filepath.Join(t.TempDir(), "snapshot.tar")
	if err := os.WriteFile(archivePath, archive, 0600); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"install-data-stable", "before-current", "current-stable"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			target := externalTransferOwner(t, dir, "receiver")
			externalTestSet(t, target, "key", []byte("old"))
			if _, err := target.Checkpoint(context.Background(), []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestExternalRecoveryTransferCrashHelper$")
			cmd.Env = append(os.Environ(), "TREEDB_TRANSFER_DIR="+dir, "TREEDB_TRANSFER_ARCHIVE="+archivePath, "TREEDB_TRANSFER_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("helper err=%v output=%s", err, out)
			}
			target = externalTransferOwner(t, dir, "receiver")
			expected := []byte("old")
			if stage == "current-stable" {
				expected = want
			}
			got, err := target.DB().Get([]byte("key"))
			if err != nil || !bytes.Equal(got, expected) {
				t.Fatalf("crash recovered bytes=%d want=%d err=%v", len(got), len(expected), err)
			}
		})
	}
}

func TestExternalRecoveryTransferCrashHelper(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	dir := os.Getenv("TREEDB_TRANSFER_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	target := externalTransferOwner(t, dir, "receiver")
	target.testCut = func(stage string) error {
		if stage == os.Getenv("TREEDB_TRANSFER_STAGE") {
			os.Exit(73)
		}
		return nil
	}
	archive, err := os.Open(os.Getenv("TREEDB_TRANSFER_ARCHIVE"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if _, err := target.InstallCheckpoint(context.Background(), archive, func(ExternalRecoveryCheckpoint) ([]byte, error) { return []byte("receiver-progress"), nil }); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash cut not reached")
}

type externalBlockedTransferWriter struct {
	bytes.Buffer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *externalBlockedTransferWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.Buffer.Write(p)
}
func TestExternalRecoveryTransferExportAllowsSuccessorApply(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	source := externalTransferOwner(t, t.TempDir(), "source")
	externalTestSet(t, source, "key", []byte("certified"))
	cert, err := source.Checkpoint(context.Background(), []byte("cut"))
	if err != nil {
		t.Fatal(err)
	}
	writer := &externalBlockedTransferWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(writer.release) })
	exported := make(chan error, 1)
	go func() {
		exported <- source.ExportCheckpoint(context.Background(), writer, cert.Generation, cert.Digest)
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not reach copy")
	}
	checkpointed := make(chan error, 1)
	go func() {
		_, err := source.Checkpoint(context.Background(), []byte("successor-cut"))
		checkpointed <- err
	}()
	// Wait until the writer is queued behind the export. RWMutex writer
	// preference must not propagate from this transfer lock into Apply's lock.
	deadline := time.Now().Add(5 * time.Second)
	for source.transferMu.TryRLock() {
		source.transferMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("checkpoint did not queue behind export")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-checkpointed:
		t.Fatalf("checkpoint completed before export release: %v", err)
	default:
	}
	applied := make(chan error, 1)
	go func() {
		applied <- source.Apply(func() error {
			if err := source.DB().Set([]byte("key"), []byte("future")); err != nil {
				return err
			}
			return source.DB().Checkpoint()
		})
	}()
	select {
	case err := <-applied:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("export blocked successor Apply")
	}
	release.Do(func() { close(writer.release) })
	if err := <-exported; err != nil {
		t.Fatal(err)
	}
	if err := <-checkpointed; err != nil {
		t.Fatal(err)
	}
	target := externalTransferOwner(t, t.TempDir(), "receiver")
	if _, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(writer.Bytes()), func(ExternalRecoveryCheckpoint) ([]byte, error) { return []byte("receiver-cut"), nil }); err != nil {
		t.Fatal(err)
	}
	got, err := target.DB().Get([]byte("key"))
	if err != nil || string(got) != "certified" {
		t.Fatalf("transferred %q err=%v", got, err)
	}
}

func TestExternalRecoveryTransferPublicationFailureRequiresReopen(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	archive, _, want := externalTransferFixture(t)
	dir := t.TempDir()
	target := externalTransferOwner(t, dir, "receiver")
	externalTestSet(t, target, "key", []byte("old"))
	if _, err := target.Checkpoint(context.Background(), []byte("old")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("publication return interrupted")
	target.testCut = func(stage string) error {
		if stage == "current-stable" {
			return injected
		}
		return nil
	}
	if _, err := target.InstallCheckpoint(context.Background(), bytes.NewReader(archive), func(ExternalRecoveryCheckpoint) ([]byte, error) { return []byte("accepted-intent"), nil }); err == nil {
		t.Fatal("publication failure was hidden")
	}
	if err := target.Apply(func() error { return nil }); err == nil {
		t.Fatal("ambiguous install admitted mutation")
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target = externalTransferOwner(t, dir, "receiver")
	cert, ok := target.RecoveredCheckpoint()
	if !ok || !bytes.Equal(cert.Metadata, []byte("accepted-intent")) {
		t.Fatalf("accepted intent not recoverable: %+v", cert)
	}
	got, err := target.DB().Get([]byte("key"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("recovered bytes=%d err=%v", len(got), err)
	}
}
