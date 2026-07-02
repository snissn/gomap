package raftfsm

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/template"
)

func TestRaftSnapshotV1ScratchDirsUseDestinationParents(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name        string
		mainDir     string
		applyDir    string
		applyParent string
	}{
		{
			name:        "external apply dir",
			mainDir:     filepath.Join(root, "external", "data", "treedb"),
			applyDir:    filepath.Join(root, "external", "raft", "apply"),
			applyParent: filepath.Join(root, "external", "raft"),
		},
		{
			name:        "apply dir under main dir",
			mainDir:     filepath.Join(root, "internal", "treedb"),
			applyDir:    filepath.Join(root, "internal", "treedb", "raftcluster", "nodes", "node-a", "groups", "default", "apply"),
			applyParent: filepath.Join(root, "internal"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scratch, err := createRaftSnapshotScratchDirsV1(tt.mainDir, tt.applyDir)
			if err != nil {
				t.Fatalf("createRaftSnapshotScratchDirsV1: %v", err)
			}
			defer func() {
				_ = os.RemoveAll(scratch.main)
				_ = os.RemoveAll(scratch.side)
				_ = os.RemoveAll(scratch.apply)
			}()

			if got, want := filepath.Dir(scratch.main), filepath.Dir(tt.mainDir); got != want {
				t.Fatalf("main scratch parent=%q want %q", got, want)
			}
			if got, want := filepath.Dir(scratch.side), filepath.Dir(tt.mainDir); got != want {
				t.Fatalf("side scratch parent=%q want %q", got, want)
			}
			if got := filepath.Dir(scratch.apply); got != tt.applyParent {
				t.Fatalf("apply scratch parent=%q want %q", got, tt.applyParent)
			}
		})
	}
}

func TestRaftSnapshotV1ExtractRejectsOversizedArchiveHeader(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	headerSize := int64(raftcluster.RaftSnapshotArchiveHeaderMaxBytes + 1)
	if err := tw.WriteHeader(&tar.Header{
		Name: raftcluster.RaftSnapshotArchiveManifestPathV1,
		Mode: 0o600,
		Size: headerSize,
	}); err != nil {
		t.Fatalf("WriteHeader oversized manifest: %v", err)
	}
	if _, err := tw.Write(bytes.Repeat([]byte("x"), int(headerSize))); err != nil {
		t.Fatalf("Write oversized manifest: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}

	_, err := extractRaftSnapshotArchiveV1(bytes.NewReader(buf.Bytes()), t.TempDir(), t.TempDir(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "snapshot archive header") {
		t.Fatalf("extractRaftSnapshotArchiveV1 err=%v want oversized header rejection", err)
	}
}

func TestRaftSnapshotV1ExtractRejectsDuplicateArchiveHeader(t *testing.T) {
	headerBytes := validRaftSnapshotArchiveHeaderBytesForTest(t)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i := 0; i < 2; i++ {
		if err := tw.WriteHeader(&tar.Header{
			Name: raftcluster.RaftSnapshotArchiveManifestPathV1,
			Mode: 0o600,
			Size: int64(len(headerBytes)),
		}); err != nil {
			t.Fatalf("WriteHeader manifest %d: %v", i, err)
		}
		if _, err := tw.Write(headerBytes); err != nil {
			t.Fatalf("Write manifest %d: %v", i, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}

	_, err := extractRaftSnapshotArchiveV1(bytes.NewReader(buf.Bytes()), t.TempDir(), t.TempDir(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "duplicate snapshot archive header") {
		t.Fatalf("extractRaftSnapshotArchiveV1 err=%v, want duplicate header rejection", err)
	}
}

func TestRaftSnapshotV1CopyFileContentDoesNotOverrunHeaderSize(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: "db/index.db",
		Mode: 0o600,
		Size: 4,
	}); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if err := copyRaftSnapshotFileContentV1(tw, strings.NewReader("abcdef"), 4); err != nil {
		t.Fatalf("copyRaftSnapshotFileContentV1: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}

	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	header, err := tr.Next()
	if err != nil {
		t.Fatalf("Next archive entry: %v", err)
	}
	if header.Size != 4 {
		t.Fatalf("archive entry size=%d want 4", header.Size)
	}
	raw, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("ReadAll archive entry: %v", err)
	}
	if got, want := string(raw), "abcd"; got != want {
		t.Fatalf("archive content=%q want %q", got, want)
	}
}

func TestRaftSnapshotV1InstallEmptyTargetPreservesDigestAndValueLogPointers(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	largeDoc := []byte(`{"_id":"u-large","name":"Ada","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(t, sourceFSM, largeDoc)

	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	assertSnapshotArchiveHasNonEmptyApplyMetadata(t, readRaftSnapshotArchiveForTest(t, snapshot))
	assertSnapshotValueLogHasFile(t, raftcluster.ValueLogDir(sourceDir))
	sourceDigest, err := sourceFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("source LogicalDigestV1: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	targetDigest, err := targetFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("target LogicalDigestV1: %v", err)
	}
	if targetDigest != sourceDigest {
		t.Fatalf("digest mismatch after snapshot install: target=%s source=%s", targetDigest.Hex(), sourceDigest.Hex())
	}
	assertSnapshotValueLogHasFile(t, raftcluster.ValueLogDir(targetDir))
	assertSnapshotDocument(t, targetFSM, "u-large", largeDoc)
}

func TestRaftSnapshotV1ExportStagesArchiveWithoutPayloadAndReleaseCleans(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	largeDoc := []byte(`{"_id":"u-large","name":"staged","payload":"` + stringsRepeatForSnapshotTest("x", 1<<20) + `"}`)
	applySnapshotSourceEntries(t, sourceFSM, largeDoc)

	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	if len(snapshot.Payload) != 0 {
		t.Fatalf("production snapshot payload len=%d, want file-backed archive", len(snapshot.Payload))
	}
	if snapshot.ArchivePath == "" {
		t.Fatal("production snapshot missing staged archive path")
	}
	info, err := os.Stat(snapshot.ArchivePath)
	if err != nil {
		t.Fatalf("Stat staged archive: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("staged archive is empty")
	}
	if err := snapshot.Release(); err != nil {
		t.Fatalf("Release staged archive: %v", err)
	}
	if err := snapshot.Release(); err != nil {
		t.Fatalf("Release staged archive again: %v", err)
	}
	if _, err := os.Stat(snapshot.ArchivePath); !os.IsNotExist(err) {
		t.Fatalf("staged archive survived Release: stat err=%v", err)
	}
}

func TestRaftSnapshotV1OpenCleansAbandonedStagedArchives(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()

	resolved, err := raftcluster.Validate(validFSMClusterConfig(sourceDir))
	if err != nil {
		t.Fatalf("Validate cluster config: %v", err)
	}
	stagingDir := raftSnapshotStagingDirV1(resolved.Layout.SnapshotDir)
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		t.Fatalf("Mkdir staging dir: %v", err)
	}
	orphanPath := filepath.Join(stagingDir, "treedb-snapshot-orphan.tar")
	if err := os.WriteFile(orphanPath, []byte("abandoned snapshot archive"), 0o600); err != nil {
		t.Fatalf("Write orphan staged archive: %v", err)
	}
	unrelatedPath := filepath.Join(stagingDir, "operator-note.txt")
	if err := os.WriteFile(unrelatedPath, []byte("not a TreeDB snapshot archive"), 0o600); err != nil {
		t.Fatalf("Write unrelated staged file: %v", err)
	}
	wrongSuffixPath := filepath.Join(stagingDir, "treedb-snapshot-manual.tmp")
	if err := os.WriteFile(wrongSuffixPath, []byte("not a temp snapshot archive"), 0o600); err != nil {
		t.Fatalf("Write non-archive staged file: %v", err)
	}
	nestedDir := filepath.Join(stagingDir, "nested")
	if err := os.MkdirAll(nestedDir, 0o700); err != nil {
		t.Fatalf("Mkdir nested staging dir: %v", err)
	}

	fsm := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, false)
	defer func() { _ = fsm.Close() }()

	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("Open did not remove abandoned staged archive: stat err=%v", err)
	}
	if _, err := os.Stat(unrelatedPath); err != nil {
		t.Fatalf("Open should leave unrelated staged files alone: %v", err)
	}
	if _, err := os.Stat(wrongSuffixPath); err != nil {
		t.Fatalf("Open should leave non-archive staged files alone: %v", err)
	}
	if _, err := os.Stat(nestedDir); err != nil {
		t.Fatalf("Open should leave staged subdirectories alone: %v", err)
	}
}

func TestRaftSnapshotV1OpenClassifiesStagedCleanupFailure(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()

	resolved, err := raftcluster.Validate(validFSMClusterConfig(sourceDir))
	if err != nil {
		t.Fatalf("Validate cluster config: %v", err)
	}
	if err := os.MkdirAll(resolved.Layout.SnapshotDir, 0o700); err != nil {
		t.Fatalf("Mkdir snapshot dir: %v", err)
	}
	stagingPath := raftSnapshotStagingDirV1(resolved.Layout.SnapshotDir)
	if err := os.WriteFile(stagingPath, []byte("not a staging directory"), 0o600); err != nil {
		t.Fatalf("Write staging path file: %v", err)
	}

	fsm, openErr := Open(Options{
		DB:      sourceDB,
		Cluster: validFSMClusterConfig(sourceDir),
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if openErr == nil {
		_ = fsm.Close()
		t.Fatal("Open with invalid staged snapshot path succeeded, want unsafe durability error")
	}
	if code, ok := ErrorCodeOf(openErr); !ok || code != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("ErrorCodeOf(Open staged cleanup failure)=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorUnsafeDurabilityModeV1, openErr)
	}
}

func TestRaftSnapshotV1InstallReplacesStaleReplica(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"source"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	staleCreate := deterministicCreateCollectionEntry(t, "stale", "fsm:snapshot:stale:create")
	result, err := targetFSM.ApplyCommittedEntryV1(committedCommand(1, 1, staleCreate))
	if err != nil {
		t.Fatalf("target stale ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
	if _, err := collections.NewCollectionManager(targetFSM.db).OpenCollection("stale"); err == nil {
		t.Fatalf("stale collection survived snapshot restore")
	}
}

func TestRaftSnapshotV1InstallReplacesStaleFormatConfig(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"source-format"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	sourceFormatPath := filepath.Join(raftcluster.MainDBDir(sourceDir), raftSnapshotFormatConfigFileV1)
	sourceFormat, err := os.ReadFile(sourceFormatPath)
	if err != nil {
		t.Fatalf("ReadFile source format config: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	targetFormatPath := filepath.Join(raftcluster.MainDBDir(targetDir), raftSnapshotFormatConfigFileV1)
	if err := os.WriteFile(targetFormatPath, []byte(`{"version":999}`), 0o600); err != nil {
		t.Fatalf("WriteFile stale target format config: %v", err)
	}
	staleTransientPath := filepath.Join(targetDir, "index.db.bak")
	if err := os.WriteFile(staleTransientPath, []byte("stale-index-backup"), 0o600); err != nil {
		t.Fatalf("WriteFile stale transient file: %v", err)
	}

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	targetFormat, err := os.ReadFile(targetFormatPath)
	if err != nil {
		t.Fatalf("ReadFile target format config: %v", err)
	}
	if !bytes.Equal(targetFormat, sourceFormat) {
		t.Fatalf("target format config=%s want source %s", targetFormat, sourceFormat)
	}
	if _, err := os.Stat(staleTransientPath); !os.IsNotExist(err) {
		t.Fatalf("stale transient file survived snapshot restore: err=%v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
}

func TestRaftSnapshotV1InstallRejectsCorruptArchiveBeforeReplacingLiveState(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"source-corrupt"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	corruptPayload := rewriteRaftSnapshotArchiveHeaderForTest(t, readRaftSnapshotArchiveForTest(t, snapshot), func(header *raftcluster.RaftSnapshotArchiveHeaderV1) {
		header.Manifest.LogicalDigestV1 = strings.Repeat("f", 64)
	})

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	defer func() { _ = targetDB.Close() }()
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	targetDoc := []byte(`{"_id":"u-large","name":"target-before-corrupt"}`)
	applySnapshotSourceEntries(t, targetFSM, targetDoc)

	err = targetFSM.InstallRaftSnapshotV1(bytes.NewReader(corruptPayload))
	if err == nil {
		t.Fatal("InstallRaftSnapshotV1 accepted corrupt archive")
	}
	if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("InstallRaftSnapshotV1 err=%v code=%s, want rejected conflict", err, code)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", targetDoc)
}

func TestRaftSnapshotV1TailReplayMatchesSourceDigest(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	initialDoc := []byte(`{"_id":"u1","name":"before"}`)
	applySnapshotSourceEntries(t, sourceFSM, initialDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	tailDoc := []byte(`{"_id":"u1","name":"after"}`)
	tail := committedCommand(3, snapshot.Manifest.LastIncludedIndex+1, deterministicReplaceBatchEntry(t, "users", "fsm:snapshot:tail:replace", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{tailDoc}))
	result, err := sourceFSM.ApplyCommittedEntryV1(tail)
	if err != nil {
		t.Fatalf("source tail ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	sourceDigest, err := sourceFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("source LogicalDigestV1 after tail: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	installRaftSnapshotForTest(t, targetFSM, snapshot)
	result, err = targetFSM.ApplyCommittedEntryV1(tail)
	if err != nil {
		t.Fatalf("target tail ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	targetDigest, err := targetFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("target LogicalDigestV1 after tail: %v", err)
	}
	if targetDigest != sourceDigest {
		t.Fatalf("digest mismatch after tail replay: target=%s source=%s", targetDigest.Hex(), sourceDigest.Hex())
	}
	assertSnapshotDocument(t, targetFSM, "u1", tailDoc)
}

func TestRaftSnapshotV1InstallReplacesSideStores(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	sourceDir := filepath.Join(sourceRoot, "maindb")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()
	applySnapshotSourceEntries(t, sourceFSM, []byte(`{"_id":"u-large","name":"source"}`))
	sourceDict := filepath.Join(sourceRoot, "dictdb")
	if err := os.MkdirAll(sourceDict, 0o700); err != nil {
		t.Fatalf("MkdirAll source dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDict, "sentinel"), []byte("source-side-store"), 0o600); err != nil {
		t.Fatalf("WriteFile source sentinel: %v", err)
	}
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetRoot := filepath.Join(root, "target")
	targetDir := filepath.Join(targetRoot, "maindb")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	staleTemplate := filepath.Join(targetRoot, "templatedb")
	if err := os.MkdirAll(staleTemplate, 0o700); err != nil {
		t.Fatalf("MkdirAll target templatedb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleTemplate, "stale"), []byte("stale-side-store"), 0o600); err != nil {
		t.Fatalf("WriteFile target stale side store: %v", err)
	}

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	got, err := os.ReadFile(filepath.Join(targetRoot, "dictdb", "sentinel"))
	if err != nil {
		t.Fatalf("ReadFile restored side-store sentinel: %v", err)
	}
	if string(got) != "source-side-store" {
		t.Fatalf("restored side-store sentinel=%q", got)
	}
	if _, err := os.Stat(staleTemplate); !os.IsNotExist(err) {
		t.Fatalf("stale target side store exists after restore: err=%v", err)
	}
}

func TestRaftSnapshotV1InstallReopensRestoredMainDBForRootLayout(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	sourceDir := filepath.Join(sourceRoot, "maindb")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTestWithClusterDir(t, sourceDB, sourceRoot, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"root-layout","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetRoot := filepath.Join(root, "target")
	targetDir := filepath.Join(targetRoot, "maindb")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTestWithClusterDir(t, targetDB, targetRoot, true)
	defer func() { _ = targetFSM.Close() }()

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
	if _, err := os.Stat(filepath.Join(targetRoot, "index.db")); !os.IsNotExist(err) {
		t.Fatalf("restore reopened TreeDB root as backend DB: index.db stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "index.db")); err != nil {
		t.Fatalf("restored main DB index missing: %v", err)
	}
}

func TestRaftSnapshotV1InstallPreservesFlatLayoutRaftMetadata(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"flat-layout","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	raftSentinel := filepath.Join(targetDir, "raftcluster", "nodes", "node-a", "groups", "default", "log", "sentinel")
	if err := os.MkdirAll(filepath.Dir(raftSentinel), 0o700); err != nil {
		t.Fatalf("MkdirAll raft sentinel: %v", err)
	}
	if err := os.WriteFile(raftSentinel, []byte("local-raft-log"), 0o600); err != nil {
		t.Fatalf("WriteFile raft sentinel: %v", err)
	}

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
	got, err := os.ReadFile(raftSentinel)
	if err != nil {
		t.Fatalf("ReadFile raft sentinel after restore: %v", err)
	}
	if string(got) != "local-raft-log" {
		t.Fatalf("raft sentinel=%q want local-raft-log", got)
	}
}

func TestRaftSnapshotV1InstallDisabledSideStoresDoesNotTouchParentSideStores(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source", "maindb")
	sourceDB := openRaftSnapshotFSMTestDBWithOptions(t, sourceDir, false, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTestWithOptions(t, sourceDB, sourceDir, false, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"disabled-side-stores"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetParent := filepath.Join(root, "target")
	targetDir := filepath.Join(targetParent, "maindb")
	targetDB := openRaftSnapshotFSMTestDBWithOptions(t, targetDir, false, true)
	targetFSM := openRaftSnapshotFSMForTestWithOptions(t, targetDB, targetDir, false, true)
	defer func() { _ = targetFSM.Close() }()
	parentSideStoreSentinel := filepath.Join(targetParent, "dictdb", "sentinel")
	if err := os.MkdirAll(filepath.Dir(parentSideStoreSentinel), 0o700); err != nil {
		t.Fatalf("MkdirAll parent side-store sentinel: %v", err)
	}
	if err := os.WriteFile(parentSideStoreSentinel, []byte("parent-side-store"), 0o600); err != nil {
		t.Fatalf("WriteFile parent side-store sentinel: %v", err)
	}

	installRaftSnapshotForTest(t, targetFSM, snapshot)
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
	got, err := os.ReadFile(parentSideStoreSentinel)
	if err != nil {
		t.Fatalf("ReadFile parent side-store sentinel after restore: %v", err)
	}
	if string(got) != "parent-side-store" {
		t.Fatalf("parent side-store sentinel=%q want parent-side-store", got)
	}
}

func TestRaftSnapshotV1RestoreWiresRestoredSideStoreLookups(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	sideRoot := snapshotSideStoreRootV1(mainDir)
	ctx := context.Background()

	dictBackend, err := backenddb.Open(backenddb.Options{
		Dir:                    filepath.Join(sideRoot, "dictdb"),
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open dictdb: %v", err)
	}
	dictStore := dictdb.New(dictBackend)
	dictBytes := []byte("restored-dict")
	dictID, err := dictStore.PutDictBytes(ctx, dictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := dictBackend.Close(); err != nil {
		t.Fatalf("Close dictdb: %v", err)
	}

	templateBackend, err := backenddb.Open(backenddb.Options{
		Dir:                    filepath.Join(sideRoot, "templatedb"),
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open templatedb: %v", err)
	}
	templateStore := templatedb.New(raftSnapshotTemplateBackendKVV1{db: templateBackend}, templatedb.Config{})
	templateBytes := []byte("restored-template")
	templateID, err := templateStore.PutTemplateDef(ctx, templateBytes, nil)
	if err != nil {
		t.Fatalf("PutTemplateDef: %v", err)
	}
	if err := templateBackend.Close(); err != nil {
		t.Fatalf("Close templatedb: %v", err)
	}

	opts := backenddb.Options{
		ReadOnly: true,
		ValueLog: backenddb.ValueLogOptions{
			TemplateMode: template.TemplateOff,
		},
	}
	cleanup, err := wireRaftSnapshotSideStoreLookupsV1(sideRoot, &opts)
	if err != nil {
		t.Fatalf("wireRaftSnapshotSideStoreLookupsV1: %v", err)
	}
	defer func() { _ = cleanup() }()

	if opts.ValueLog.DictLookup == nil {
		t.Fatal("DictLookup was not wired from restored side store")
	}
	gotDict, err := opts.ValueLog.DictLookup(dictID)
	if err != nil {
		t.Fatalf("DictLookup(%d): %v", dictID, err)
	}
	if !bytes.Equal(gotDict, dictBytes) {
		t.Fatalf("DictLookup(%d)=%q want %q", dictID, gotDict, dictBytes)
	}
	if opts.ValueLog.TemplateLookup == nil {
		t.Fatal("TemplateLookup was not wired from restored side store")
	}
	gotTemplate, err := opts.ValueLog.TemplateLookup(templateID)
	if err != nil {
		t.Fatalf("TemplateLookup(%d): %v", templateID, err)
	}
	if !bytes.Equal(gotTemplate, templateBytes) {
		t.Fatalf("TemplateLookup(%d)=%q want %q", templateID, gotTemplate, templateBytes)
	}
}

func TestRaftSnapshotV1RestoreScrubsMainPlacementOptionsForSideStores(t *testing.T) {
	opts := backenddb.Options{
		IndexOuterLeavesInValueLog: true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
			DomainInlineThresholds: []backenddb.ValueLogDomainThreshold{
				{Prefix: []byte("main/"), InlineThreshold: 0},
			},
			Compression: backenddb.ValueLogCompressionAuto,
			DictLookup: func(uint64) ([]byte, error) {
				return nil, nil
			},
			TemplateMode: template.TemplatePrepass,
			TemplateLookup: func(uint64) ([]byte, error) {
				return nil, nil
			},
			TemplateDecodeOptions: template.DecodeOptions{MaxDecodedBytes: 32, MaxGaps: 4, DefCacheSize: 2},
		},
	}

	scrubRaftSnapshotSideStoreOptionsV1(&opts)

	if opts.IndexOuterLeavesInValueLog {
		t.Fatal("IndexOuterLeavesInValueLog was not cleared")
	}
	if opts.ValueLog.ForcePointers {
		t.Fatal("ForcePointers was not cleared")
	}
	if opts.ValueLog.PointerThreshold != 0 {
		t.Fatalf("PointerThreshold=%d want 0", opts.ValueLog.PointerThreshold)
	}
	if opts.ValueLog.DomainInlineThresholds != nil {
		t.Fatalf("DomainInlineThresholds=%v want nil", opts.ValueLog.DomainInlineThresholds)
	}
	if opts.ValueLog.Compression != backenddb.ValueLogCompressionOff {
		t.Fatalf("Compression=%v want off", opts.ValueLog.Compression)
	}
	if opts.ValueLog.DictLookup != nil {
		t.Fatal("DictLookup was not cleared")
	}
	if opts.ValueLog.DictTrain.TrainBytes != -1 {
		t.Fatalf("DictTrain.TrainBytes=%d want -1", opts.ValueLog.DictTrain.TrainBytes)
	}
	if opts.ValueLog.TemplateMode != template.TemplateOff {
		t.Fatalf("TemplateMode=%v want off", opts.ValueLog.TemplateMode)
	}
	if opts.ValueLog.TemplateLookup != nil {
		t.Fatal("TemplateLookup was not cleared")
	}
	if opts.ValueLog.TemplateDecodeOptions != (template.DecodeOptions{}) {
		t.Fatalf("TemplateDecodeOptions=%+v want zero", opts.ValueLog.TemplateDecodeOptions)
	}
}

func TestRaftSnapshotV1ExportRejectsTopLevelSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "outside")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile symlink target: %v", err)
	}
	link := filepath.Join(root, "value_vlog")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported on this platform: %v", err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := appendRaftSnapshotStoragePathV1(tw, "value_vlog", link)
	_ = tw.Close()
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("appendRaftSnapshotStoragePathV1 symlink error=%v, want symlink rejection", err)
	}
}

func TestRaftSnapshotV1ExportRejectsDirectoryRootSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "outside")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("MkdirAll symlink target: %v", err)
	}
	link := filepath.Join(root, "maindb")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported on this platform: %v", err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := appendRaftSnapshotDirV1(tw, "db", link)
	_ = tw.Close()
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("appendRaftSnapshotDirV1 symlink root error=%v, want symlink rejection", err)
	}
}

func BenchmarkRaftSnapshotV1ExportInstall(b *testing.B) {
	root := b.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(b, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(b, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()
	largeDoc := []byte(`{"_id":"u-large","name":"Ada","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(b, sourceFSM, largeDoc)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		targetDir := filepath.Join(root, "target", strconv.Itoa(i))
		targetDB := openRaftSnapshotFSMTestDB(b, targetDir, true)
		targetFSM := openRaftSnapshotFSMForTest(b, targetDB, targetDir, true)
		b.StartTimer()
		snapshot, err := sourceFSM.ExportRaftSnapshotV1()
		if err != nil {
			b.Fatalf("ExportRaftSnapshotV1: %v", err)
		}
		installRaftSnapshotForTest(b, targetFSM, snapshot)
		b.StopTimer()
		_ = targetFSM.Close()
	}
}

func openRaftSnapshotFSMTestDB(t testing.TB, dir string, forceValuePointers bool) *backenddb.DB {
	t.Helper()
	return openRaftSnapshotFSMTestDBWithOptions(t, dir, forceValuePointers, false)
}

func openRaftSnapshotFSMTestDBWithOptions(t testing.TB, dir string, forceValuePointers, disableSideStores bool) *backenddb.DB {
	t.Helper()
	opts := backenddb.Options{
		Dir:                          dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
		DisableSideStores:            disableSideStores,
	}
	if forceValuePointers {
		opts.ValueLog = backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		}
	}
	db, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	return db
}

func openRaftSnapshotFSMForTest(t testing.TB, db *backenddb.DB, dbDir string, forceValuePointers bool) *FSM {
	t.Helper()
	return openRaftSnapshotFSMForTestWithClusterDir(t, db, dbDir, forceValuePointers)
}

func openRaftSnapshotFSMForTestWithClusterDir(t testing.TB, db *backenddb.DB, clusterDir string, forceValuePointers bool) *FSM {
	t.Helper()
	return openRaftSnapshotFSMForTestWithOptions(t, db, clusterDir, forceValuePointers, false)
}

func openRaftSnapshotFSMForTestWithOptions(t testing.TB, db *backenddb.DB, clusterDir string, forceValuePointers, disableSideStores bool) *FSM {
	t.Helper()
	restoreOpts := backenddb.Options{
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
		DisableSideStores:            disableSideStores,
	}
	if forceValuePointers {
		restoreOpts.ValueLog = backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		}
	}
	cluster := validFSMClusterConfig(clusterDir)
	cluster.DisableSideStores = disableSideStores
	fsm, err := Open(Options{
		DB:                       db,
		Cluster:                  cluster,
		SnapshotRestoreDBOptions: restoreOpts,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync:          true,
			AllowInitialIndexGap: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	return fsm
}

func applySnapshotSourceEntries(tb testing.TB, fsm *FSM, doc []byte) {
	tb.Helper()
	create := deterministicCreateCollectionEntry(tb, "users", "fsm:snapshot:create:users")
	insert := deterministicInsertBatchEntry(tb, "users", "fsm:snapshot:insert:users", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1"), []byte("u-large")}, [][]byte{[]byte(`{"_id":"u1","name":"small"}`), doc})
	results, err := fsm.ApplyCommittedEntriesV1([]CommittedEntryV1{
		committedCommand(2, 1, create),
		committedCommand(2, 2, insert),
	})
	if err != nil {
		tb.Fatalf("ApplyCommittedEntriesV1: %v results=%+v", err, results)
	}
	if len(results) != 2 {
		tb.Fatalf("results=%d, want 2", len(results))
	}
	assertSnapshotApplyResult(tb, results[0], raftentry.ApplyStatusApplied, 1)
	assertSnapshotApplyResult(tb, results[1], raftentry.ApplyStatusApplied, 2)
}

func installRaftSnapshotForTest(tb testing.TB, fsm *FSM, snapshot raftcluster.RaftSnapshotV1) {
	tb.Helper()
	reader := openRaftSnapshotArchiveForTest(tb, snapshot)
	defer func() { _ = reader.Close() }()
	if err := fsm.InstallRaftSnapshotV1(reader); err != nil {
		tb.Fatalf("InstallRaftSnapshotV1: %v", err)
	}
}

func readRaftSnapshotArchiveForTest(tb testing.TB, snapshot raftcluster.RaftSnapshotV1) []byte {
	tb.Helper()
	reader := openRaftSnapshotArchiveForTest(tb, snapshot)
	defer func() { _ = reader.Close() }()
	payload, err := io.ReadAll(reader)
	if err != nil {
		tb.Fatalf("ReadAll snapshot archive: %v", err)
	}
	return payload
}

func openRaftSnapshotArchiveForTest(tb testing.TB, snapshot raftcluster.RaftSnapshotV1) io.ReadCloser {
	tb.Helper()
	reader, err := snapshot.OpenArchive()
	if err != nil {
		tb.Fatalf("OpenArchive: %v", err)
	}
	return reader
}

func rewriteRaftSnapshotArchiveHeaderForTest(t testing.TB, payload []byte, mut func(*raftcluster.RaftSnapshotArchiveHeaderV1)) []byte {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(payload))
	var out bytes.Buffer
	tw := tar.NewWriter(&out)
	found := false
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next archive entry: %v", err)
		}
		raw, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("ReadAll archive entry %q: %v", header.Name, err)
		}
		next := *header
		if header.Name == raftcluster.RaftSnapshotArchiveManifestPathV1 {
			decoded, err := raftcluster.DecodeRaftSnapshotArchiveHeaderV1(raw, raftcluster.SnapshotScopeIdentityV1{})
			if err != nil {
				t.Fatalf("DecodeRaftSnapshotArchiveHeaderV1: %v", err)
			}
			mut(&decoded)
			raw, err = raftcluster.EncodeRaftSnapshotArchiveHeaderV1(decoded)
			if err != nil {
				t.Fatalf("EncodeRaftSnapshotArchiveHeaderV1: %v", err)
			}
			next.Size = int64(len(raw))
			found = true
		}
		if err := tw.WriteHeader(&next); err != nil {
			t.Fatalf("WriteHeader archive entry %q: %v", next.Name, err)
		}
		if _, err := tw.Write(raw); err != nil {
			t.Fatalf("Write archive entry %q: %v", next.Name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close rewritten archive: %v", err)
	}
	if !found {
		t.Fatalf("archive missing %s", raftcluster.RaftSnapshotArchiveManifestPathV1)
	}
	return out.Bytes()
}

func validRaftSnapshotArchiveHeaderBytesForTest(t testing.TB) []byte {
	t.Helper()
	manifest := raftcluster.SnapshotManifestV1{
		Format:            raftcluster.SnapshotManifestFormatV1,
		Version:           raftcluster.SnapshotManifestVersion1,
		GroupID:           "group-a",
		NodeID:            "node-a",
		LastIncludedTerm:  1,
		LastIncludedIndex: 1,
		AppliedCommandLSN: 1,
		LogicalDigestV1:   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		Scope: raftcluster.SnapshotScopeIdentityV1{
			ScopeRule:     string(raftentry.ScopeRuleSingleGroupV1),
			DatabaseScope: raftentry.DatabaseScopeDefaultV1,
			CatalogScope:  raftentry.CatalogScopeDefaultV1,
		},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}
	headerBytes, err := raftcluster.EncodeRaftSnapshotArchiveHeaderV1(raftcluster.NewRaftSnapshotArchiveHeaderV1(manifest))
	if err != nil {
		t.Fatalf("EncodeRaftSnapshotArchiveHeaderV1: %v", err)
	}
	return headerBytes
}

func assertSnapshotArchiveHasNonEmptyApplyMetadata(t testing.TB, payload []byte) {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(payload))
	want := map[string]bool{
		raftSnapshotApplyPrefixV1 + "/apply-progress-v1.log": false,
		raftSnapshotApplyPrefixV1 + "/apply-results-v1.log":  false,
	}
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next archive entry: %v", err)
		}
		if _, ok := want[header.Name]; !ok {
			continue
		}
		if header.Size == 0 {
			t.Fatalf("snapshot archive apply metadata %q is zero-length", header.Name)
		}
		want[header.Name] = true
	}
	for name, found := range want {
		if !found {
			t.Fatalf("snapshot archive missing apply metadata %q", name)
		}
	}
}

func assertSnapshotApplyResult(tb testing.TB, result raftentry.ApplyResultV1, wantStatus raftentry.ApplyStatusV1, wantAffected int64) {
	tb.Helper()
	if result.Status != wantStatus || result.AffectedCount != wantAffected {
		tb.Fatalf("apply result=%+v want status=%s affected=%d", result, wantStatus, wantAffected)
	}
}

func assertSnapshotDocument(t *testing.T, fsm *FSM, id string, want []byte) {
	t.Helper()
	coll, err := collections.NewCollectionManager(fsm.db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	got, err := coll.Get([]byte(id))
	if err != nil {
		t.Fatalf("Get %s: %v", id, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("document %s=%s want %s", id, got, want)
	}
}

func assertSnapshotValueLogHasFile(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir value log %s: %v", dir, err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err == nil && info.Mode().IsRegular() && info.Size() > 0 {
			return
		}
	}
	t.Fatalf("value log %s has no non-empty segment files", dir)
}

func stringsRepeatForSnapshotTest(value string, count int) string {
	var b strings.Builder
	for i := 0; i < count; i++ {
		b.WriteString(value)
	}
	return b.String()
}
