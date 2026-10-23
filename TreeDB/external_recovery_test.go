package treedb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func externalTestOpen(t *testing.T, dir string) *ExternalRecovery {
	t.Helper()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	o, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: "test-group/epoch-1"})
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

func externalTestSet(t *testing.T, o *ExternalRecovery, key string, value []byte) {
	t.Helper()
	if err := o.Apply(func() error { return o.DB().Set([]byte(key), value) }); err != nil {
		t.Fatal(err)
	}
}

// A later ordinary checkpoint/clean Close must not make uncertified application
// progress part of the recovery base. Large pointer values exercise exact file
// identity rebinding and the full persistent value/outer-leaf closure.
func TestExternalRecoveryRestoresExactCertifiedImage(t *testing.T) {
	dir := t.TempDir()
	o := externalTestOpen(t, dir)
	want := bytes.Repeat([]byte("certified"), 2048)
	externalTestSet(t, o, "key", want)
	cert, err := o.Checkpoint(context.Background(), []byte("raft=41;oracle=90;pending=0"))
	if err != nil {
		t.Fatal(err)
	}
	if cert.Root.CommitSeq == 0 || cert.Generation != 1 || cert.Digest == "" {
		t.Fatal(cert)
	}
	externalTestSet(t, o, "key", []byte("future"))
	externalTestSet(t, o, "uncertified-only", []byte("future"))
	if err := o.DB().Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = externalTestOpen(t, dir)
	got, err := o.DB().Get([]byte("key"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("value=%q err=%v", got, err)
	}
	if found, err := o.DB().Has([]byte("uncertified-only")); err != nil || found {
		t.Fatalf("uncertified found=%v err=%v", found, err)
	}
	recovered, ok := o.RecoveredCheckpoint()
	if !ok || recovered.Digest != cert.Digest || !bytes.Equal(recovered.Metadata, cert.Metadata) {
		t.Fatal(recovered)
	}
	if _, err := Open(OptionsFor(ProfileNoWALFast, dir)); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("ordinary open=%v", err)
	}
}

func TestExternalRecoveryGenesisDiscardsUncertifiedClose(t *testing.T) {
	dir := t.TempDir()
	o := externalTestOpen(t, dir)
	externalTestSet(t, o, "suffix", []byte("uncheckpointed"))
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = externalTestOpen(t, dir)
	if found, err := o.DB().Has([]byte("suffix")); err != nil || found {
		t.Fatalf("suffix found=%v err=%v", found, err)
	}
	if _, ok := o.RecoveredCheckpoint(); ok {
		t.Fatal("uncertified progress adopted")
	}
}

func TestExternalRecoveryRejectsCorruptionIdentityAndConcurrentOwner(t *testing.T) {
	dir := t.TempDir()
	o := externalTestOpen(t, dir)
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	if other, err := OpenExternalRecovery(opts, o.config); err == nil {
		other.Close()
		t.Fatal("second owner admitted")
	}
	externalTestSet(t, o, "key", []byte("base"))
	cert, err := o.Checkpoint(context.Background(), []byte("index=1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: "wrong"}); err == nil {
		other.Close()
		t.Fatal("wrong identity admitted")
	}
	f, err := os.OpenFile(filepath.Join(o.imageDir(cert.Generation), "index.db"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, e := f.WriteAt([]byte{0x77}, 0)
	if err := errors.Join(e, f.Close()); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenExternalRecovery(opts, o.config); err == nil {
		other.Close()
		t.Fatal("corrupted image admitted")
	}
}

func TestExternalRecoveryFailedCheckpointPreservesAuthority(t *testing.T) {
	dir := t.TempDir()
	o := externalTestOpen(t, dir)
	externalTestSet(t, o, "key", []byte("base"))
	first, err := o.Checkpoint(context.Background(), []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	externalTestSet(t, o, "key", []byte("next"))
	injected := errors.New("before certificate publication")
	o.testCut = func(stage string) error {
		if stage == "before-current" {
			return injected
		}
		return nil
	}
	if _, err := o.Checkpoint(context.Background(), []byte("next")); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = externalTestOpen(t, dir)
	cert, _ := o.RecoveredCheckpoint()
	if cert.Digest != first.Digest {
		t.Fatal("failed checkpoint changed authority")
	}
	got, err := o.DB().Get([]byte("key"))
	if err != nil || string(got) != "base" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

// These are abrupt subprocess exits, not power-cut simulation: kernel dirty
// state may survive, but recovery must still honor only the certified image.
func TestExternalRecoveryCrashCuts(t *testing.T) {
	for _, stage := range []string{"image-stable", "before-current", "current-stable"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			o := externalTestOpen(t, dir)
			externalTestSet(t, o, "key", []byte("base"))
			if _, err := o.Checkpoint(context.Background(), []byte("base")); err != nil {
				t.Fatal(err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestExternalRecoveryCrashHelper$")
			cmd.Env = append(os.Environ(), "TREEDB_EXTERNAL_CRASH_DIR="+dir, "TREEDB_EXTERNAL_CRASH_STAGE="+stage)
			if out, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("helper did not exit: %s", out)
			} else if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 73 {
				t.Fatalf("helper err=%v output=%s", err, out)
			}
			o = externalTestOpen(t, dir)
			want := "base"
			if stage == "current-stable" {
				want = "next"
			}
			got, err := o.DB().Get([]byte("key"))
			if err != nil || string(got) != want {
				t.Fatalf("got=%q want=%s err=%v", got, want, err)
			}
		})
	}
}

func TestExternalRecoveryCrashHelper(t *testing.T) {
	dir := os.Getenv("TREEDB_EXTERNAL_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	o := externalTestOpen(t, dir)
	externalTestSet(t, o, "key", []byte("next"))
	o.testCut = func(stage string) error {
		if stage == os.Getenv("TREEDB_EXTERNAL_CRASH_STAGE") {
			os.Exit(73)
		}
		return nil
	}
	if _, err := o.Checkpoint(context.Background(), []byte("next")); err != nil {
		t.Fatal(err)
	}
	t.Fatal("requested cut not reached")
}

func TestExternalRecoveryCheckpointLimitsAndRetention(t *testing.T) {
	o := externalTestOpen(t, t.TempDir())
	for i := 0; i < 3; i++ {
		externalTestSet(t, o, "key", []byte{byte(i)})
		if _, err := o.Checkpoint(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(o.dir, "checkpoints"))
	if err != nil || len(entries) != 2 {
		t.Fatalf("retained generations=%d err=%v", len(entries), err)
	}
	prior, _ := o.RecoveredCheckpoint()
	o.config.MaxCheckpointBytes = 1
	if _, err := o.Checkpoint(context.Background(), nil); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("limit err=%v", err)
	}
	current, _ := o.RecoveredCheckpoint()
	if prior.Digest != current.Digest {
		t.Fatal("limit changed recovery authority")
	}
}

func TestExternalRecoveryCheckpointWaitsForCompleteApply(t *testing.T) {
	o := externalTestOpen(t, t.TempDir())
	entered, release := make(chan struct{}), make(chan struct{})
	applyDone := make(chan error, 1)
	go func() {
		applyDone <- o.Apply(func() error {
			close(entered)
			<-release
			return o.DB().Set([]byte("complete"), []byte("value"))
		})
	}()
	<-entered
	checkpointDone := make(chan error, 1)
	go func() {
		_, err := o.Checkpoint(context.Background(), []byte("complete apply"))
		checkpointDone <- err
	}()
	select {
	case err := <-checkpointDone:
		close(release)
		t.Fatalf("checkpoint passed outstanding apply: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-applyDone; err != nil {
		t.Fatal(err)
	}
	if err := <-checkpointDone; err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = externalTestOpen(t, o.dir)
	got, err := o.DB().Get([]byte("complete"))
	if err != nil || string(got) != "value" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestExternalRecoveryCertificateDoesNotAliasCaller(t *testing.T) {
	o := externalTestOpen(t, t.TempDir())
	externalTestSet(t, o, "key", []byte("base"))
	cert, err := o.Checkpoint(context.Background(), []byte("metadata"))
	if err != nil {
		t.Fatal(err)
	}
	cert.Metadata[0] = 'x'
	cert.Files[0].Path = "caller mutation"
	recovered, _ := o.RecoveredCheckpoint()
	if string(recovered.Metadata) != "metadata" || recovered.Files[0].Path == "caller mutation" {
		t.Fatal("returned certificate aliases recovery owner")
	}
}

func TestExternalRecoveryRejectsUnrecoverableIdentity(t *testing.T) {
	for _, identity := range []string{strings.Repeat("<", 2000), string([]byte{0xff})} {
		opts := OptionsFor(ProfileNoWALFast, t.TempDir())
		opts.MemtableMode = "cow_ordered"
		if o, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: identity}); err == nil {
			o.Close()
			t.Fatal("unrecoverable encoded identity admitted")
		}
	}
}
