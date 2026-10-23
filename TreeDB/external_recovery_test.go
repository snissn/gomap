package treedb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestExternalRecoveryTwoGenerationClosureAcrossRewriteAndReopen(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	for _, outer := range []bool{false, true} {
		t.Run(fmt.Sprint("outer=", outer), func(t *testing.T) {
			dir := t.TempDir()
			opts := OptionsFor(ProfileNoWALFast, dir)
			opts.MemtableMode = "cow_ordered"
			opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
			opts.IndexOuterLeavesInValueLog = outer
			config := ExternalRecoveryConfig{Identity: "two-generation"}
			o, err := OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = o.Close() }()
			firstValue := bytes.Repeat([]byte("first"), 16000)
			secondValue := bytes.Repeat([]byte("second"), 16000)
			externalTestSet(t, o, "key", firstValue)
			first, err := o.Checkpoint(context.Background(), []byte("first"))
			if err != nil {
				t.Fatal(err)
			}
			firstRoot, err := o.readRetainedRoot(first)
			if err != nil {
				t.Fatal(err)
			}
			externalTestSet(t, o, "key", secondValue)
			second, err := o.Checkpoint(context.Background(), []byte("second"))
			if err != nil {
				t.Fatal(err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			o, err = OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			roots, err := o.db.backend.CaptureRecoverableRootSet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			foundFirst, foundSecond := false, false
			for _, root := range roots.Roots() {
				foundFirst = foundFirst || root.CommitSeq == first.Root.CommitSeq
				foundSecond = foundSecond || root.CommitSeq == second.Root.CommitSeq
			}
			roots.Release()
			if !foundFirst || !foundSecond {
				t.Fatal("Open did not reacquire both certified roots")
			}
			for i := 0; i < 12; i++ {
				externalTestSet(t, o, "key", []byte(fmt.Sprint("suffix", i)))
				if err := o.db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := o.db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{BatchSize: 4, MaxCopiedBytes: 1 << 20}); err != nil {
				t.Fatal(err)
			}
			if _, err := o.db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			o, err = OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			got, err := o.DB().Get([]byte("key"))
			if err != nil || !bytes.Equal(got, secondValue) {
				t.Fatalf("current lost after rewrite: bytes=%d err=%v", len(got), err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			// Direct physical restore proves previous selectability, independently
			// of the external owner's fail-closed CURRENT metadata policy.
			working := filepath.Join(dir, "working")
			if err := backenddb.RestoreRetainedPhysicalRootsV1(working, firstRoot, nil); err != nil {
				t.Fatal(err)
			}
			physicalOpts := OptionsFor(ProfileNoWALFast, working)
			physicalOpts.DisableSideStores, physicalOpts.DisableBackgroundPrune = true, true
			database, err := backenddb.Open(physicalOpts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			got, err = database.Get([]byte("key"))
			if err != nil || !bytes.Equal(got, firstValue) {
				t.Fatalf("previous root lost: bytes=%d err=%v", len(got), err)
			}
		})
	}
}

func TestExternalRecoveryInitializationOwnership(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	for i := 0; i < 12; i++ {
		dir := t.TempDir()
		opts := OptionsFor(ProfileNoWALFast, dir)
		opts.MemtableMode, opts.DisableSideStores = "cow_ordered", true
		start := make(chan struct{})
		type result struct {
			close func() error
			err   error
		}
		ordinary, external := make(chan result, 1), make(chan result, 1)
		go func() {
			<-start
			o, err := Open(opts)
			r := result{err: err}
			if o != nil {
				r.close = o.Close
			}
			ordinary <- r
		}()
		go func() {
			<-start
			o, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: "owner"})
			r := result{err: err}
			if o != nil {
				r.close = o.Close
			}
			external <- r
		}()
		close(start)
		a, b := <-ordinary, <-external
		if a.close != nil {
			defer a.close()
		}
		if b.close != nil {
			defer b.close()
		}
		if a.err == nil && b.err == nil {
			t.Fatal("ordinary and external initialization both acquired ownership")
		}
		if a.err != nil && b.err != nil {
			t.Fatalf("no owner succeeded: ordinary=%v external=%v", a.err, b.err)
		}
		if a.close != nil {
			if err := a.close(); err != nil {
				t.Fatal(err)
			}
		}
		if b.close != nil {
			if err := b.close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Failure after lock acquisition must release it, permitting another policy.
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "adaptive:cow_ordered"
	if o, err := Open(opts); err == nil {
		_ = o.Close()
		t.Fatal("invalid configuration accepted")
	}
	_ = externalTestOpen(t, dir)
}

func TestExternalRecoveryRetainsRootWithoutCopyingImage(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	dir := t.TempDir()
	o := externalTestOpen(t, dir)
	want := bytes.Repeat([]byte("retained-pointer"), 16384)
	for i := 0; i < 24; i++ {
		externalTestSet(t, o, fmt.Sprintf("key-%03d", i), want)
	}
	cert, err := o.Checkpoint(context.Background(), []byte("exact-root"))
	if err != nil {
		t.Fatal(err)
	}
	var checkpointBytes int64
	for _, file := range cert.Files {
		checkpointBytes += file.Bytes
	}
	if checkpointBytes > 64<<10 {
		t.Fatalf("checkpoint copied database: %d bytes", checkpointBytes)
	}
	before, err := os.Stat(filepath.Join(dir, "working", "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 24; i++ {
		externalTestSet(t, o, "key-000", []byte(fmt.Sprintf("suffix-%d", i)))
		if err := o.DB().Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.DB().ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = externalTestOpen(t, dir)
	after, err := os.Stat(filepath.Join(dir, "working", "index.db"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("recovery replaced working inode: %v", err)
	}
	got, err := o.DB().Get([]byte("key-000"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("retained root lost: bytes=%d err=%v", len(got), err)
	}
}

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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
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

func TestExternalRecoveryMissingCurrentPreservesCertifiedState(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	for _, invalid := range []string{"later-generation", "initial-has-predecessor", "initial-corrupt"} {
		t.Run(invalid, func(t *testing.T) {
			dir := t.TempDir()
			o := externalTestOpen(t, dir)
			externalTestSet(t, o, "key", []byte("retained"))
			cert, err := o.Checkpoint(context.Background(), []byte("first"))
			if err != nil {
				t.Fatal(err)
			}
			if invalid == "later-generation" {
				cert, err = o.Checkpoint(context.Background(), []byte("second"))
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dir, "CURRENT")); err != nil {
				t.Fatal(err)
			}
			certificate := filepath.Join(filepath.Dir(o.imageDir(cert.Generation)), "certificate.json")
			if invalid != "later-generation" {
				if invalid == "initial-has-predecessor" {
					cert.PreviousGeneration, cert.PreviousDigest = 9, "prior"
					cert.Digest, err = externalDigest(cert)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					cert.Digest = "corrupt"
				}
				raw, err := json.Marshal(cert)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(certificate, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			retained, err := os.ReadFile(certificate)
			if err != nil {
				t.Fatal(err)
			}
			canary := filepath.Join(dir, "working", "preserve-on-refusal")
			if err := os.WriteFile(canary, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			opts := OptionsFor(ProfileNoWALFast, dir)
			opts.MemtableMode = "cow_ordered"
			if other, err := OpenExternalRecovery(opts, o.config); err == nil || other != nil {
				if other != nil {
					_ = other.Close()
				}
				t.Fatalf("missing CURRENT accepted: owner=%v err=%v", other, err)
			}
			if got, err := os.ReadFile(certificate); err != nil || !bytes.Equal(got, retained) {
				t.Fatalf("refusal changed retained certificate: %v", err)
			}
			if got, err := os.ReadFile(canary); err != nil || string(got) != "preserve" {
				t.Fatalf("refusal cleaned working state: %v", err)
			}
		})
	}
}

func TestExternalRecoveryInterruptedFirstPublicationRecreatesGenesis(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	dir := t.TempDir()
	o := externalTestOpen(t, dir)
	externalTestSet(t, o, "suffix", []byte("uncertified"))
	injected := errors.New("before first CURRENT")
	o.testCut = func(stage string) error {
		if stage == "before-current" {
			return injected
		}
		return nil
	}
	if _, err := o.Checkpoint(context.Background(), []byte("unpublished")); !errors.Is(err, injected) {
		t.Fatalf("checkpoint=%v, want injected cut", err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = externalTestOpen(t, dir)
	if _, ok := o.RecoveredCheckpoint(); ok {
		t.Fatal("adopted unpublished first certificate")
	}
	if found, err := o.DB().Has([]byte("suffix")); err != nil || found {
		t.Fatalf("uncertified suffix found=%v err=%v", found, err)
	}
}

func TestExternalRecoveryRejectsCorruptionIdentityAndConcurrentOwner(t *testing.T) {
	requireExternalRecoveryPlatform(t)
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
	f, err := os.OpenFile(filepath.Join(o.imageDir(cert.Generation), "root.json"), os.O_WRONLY, 0)
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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
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
	requireExternalRecoveryPlatform(t)
	for _, identity := range []string{strings.Repeat("<", 2000), string([]byte{0xff})} {
		opts := OptionsFor(ProfileNoWALFast, t.TempDir())
		opts.MemtableMode = "cow_ordered"
		if o, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: identity}); err == nil {
			o.Close()
			t.Fatal("unrecoverable encoded identity admitted")
		}
	}
}

func requireExternalRecoveryPlatform(tb testing.TB) {
	tb.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		tb.Skip("external recovery requires exact stable relative namespace primitives")
	}
}

func TestExternalRecoveryUnsupportedPlatformRefusesBeforeMutation(t *testing.T) {
	if rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("platform supports external recovery")
	}
	parent := t.TempDir()
	dir := filepath.Join(parent, "must-remain-absent")
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	owner, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: "unsupported"})
	if owner != nil || !errors.Is(err, ErrExternalRecovery) {
		if owner != nil {
			_ = owner.Close()
		}
		t.Fatalf("owner=%v err=%v", owner != nil, err)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported open mutated namespace: %v", err)
	}
}
