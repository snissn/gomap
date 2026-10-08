package raftfsm

import (
	"archive/tar"
	"bytes"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/page"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCapturedPrimaryRaftSnapshotRebindsBothFilesAndReplaysTail(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	sourceDir := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := backenddb.SaveFormatConfig(sourceDir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureDependencyDirectoryV2}}); err != nil {
		t.Fatal(err)
	}
	opts := backenddb.Options{Dir: sourceDir, IndexPrimaryDirectory: true, CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}}
	database, err := backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, sourceDir, true)
	defer source.Close()
	doc := []byte(`{"_id":"u-large","payload":"` + string(bytes.Repeat([]byte("captured"), 1024)) + `"}`)
	applySnapshotSourceEntries(t, source, doc)
	state, ok := database.StateToken()
	if !ok || state.RootPageID < page.PrimaryBankNamespace {
		t.Fatalf("source primary root %+v", state)
	}
	captured, err := source.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Release()
	tail := deterministicInsertBatchEntry(t, "users", "primary:deferred-tail", nativewire.DocumentFormatJSON, [][]byte{[]byte("tail")}, [][]byte{[]byte(`{"_id":"tail","value":1}`)})
	if _, err = source.ApplyCommittedEntryV1(committedCommand(2, 3, tail)); err != nil {
		t.Fatal(err)
	}
	// Materialization must use the retained pre-tail bank/component/dependency
	// closure, rather than rereading a mutable current publication.
	ready, err := captured.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Release()
	archive := readRaftSnapshotArchiveForTest(t, ready)
	reader := tar.NewReader(bytes.NewReader(archive))
	var companion int
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == raftSnapshotDBPrefixV1+"/index.db.primary" {
			companion++
			if header.Size < 3*page.PageSize || header.Size%page.PageSize != 0 {
				t.Fatalf("primary archive extent %d", header.Size)
			}
		}
	}
	if companion != 1 {
		t.Fatalf("primary companion entries %d", companion)
	}
	targetDir := t.TempDir()
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, true)
	defer targetDB.Close()
	target := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer target.Close()
	installRaftSnapshotForTest(t, target, ready)
	assertSnapshotDocument(t, target, "u-large", doc)
	if err = target.VerifyInstalledSnapshotManifestV1(ready.Manifest); err != nil {
		t.Fatal(err)
	}
	result, err := target.ApplyCommittedEntryV1(committedCommand(2, 3, tail))
	if err != nil || result.Status != raftentry.ApplyStatusApplied {
		t.Fatalf("restored tail %+v %v", result, err)
	}
	assertSnapshotDocument(t, target, "tail", []byte(`{"_id":"tail","value":1}`))
	if err = target.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: targetDir, ReadOnly: true, CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true, ValueLog: opts.ValueLog})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, ok = reopened.StateToken()
	if !ok || state.RootPageID < page.PrimaryBankNamespace {
		t.Fatalf("restored primary root %+v", state)
	}
}
