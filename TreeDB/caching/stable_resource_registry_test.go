package caching

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestOpenSharesBackendValueLogIdentityPinRegistry(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	database, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	registry := backend.ValueLogIdentityPinRegistry()
	if registry == nil {
		t.Fatal("backend has no value-log identity pin registry")
	}
	if got := database.ValueLogIdentityPinRegistry(); got != registry {
		t.Fatalf("cached DB registry = %p, backend registry = %p", got, registry)
	}
	if got := database.valueLogReader.StableResourcePinRegistry(); got != registry {
		t.Fatalf("cached value-log reader registry = %p, backend registry = %p", got, registry)
	}
}

func TestCleanupOrphanedRetainedValueLogRejectsReboundPathAfterEvict(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit replacing an open segment path")
	}
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	database, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	fileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(database.valueLogDir, valueLogName(0, 2))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogReader.RegisterSegment(path, fileID); err != nil {
		t.Fatal(err)
	}
	expected, ok := database.valueLogReader.StableSegmentIdentity(fileID)
	if !ok {
		t.Fatal("manager did not capture segment identity")
	}
	if err := database.valueLogReader.EvictSegment(fileID); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	const replacement = "replacement survives retained cleanup"
	if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}
	replacementFile, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	replacementIdentity, err := rootpublication.StableIdentityFromFile(replacementFile)
	closeErr := replacementFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("capture replacement identity: identity=%v close=%v", err, closeErr)
	}
	if database.cleanupOrphanedRetainedValueLogExpected(path, expected) {
		t.Fatal("cleanup removed a rebound pathname")
	}
	registered, ok := database.valueLogReader.StableSegmentIdentity(fileID)
	if !ok {
		t.Fatal("cleanup did not register the rebound segment")
	}
	if !rootpublication.SamePhysicalIdentity(registered, replacementIdentity) {
		t.Fatal("cleanup registered an unexpected replacement identity")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != replacement {
		t.Fatalf("replacement changed: got=%q err=%v", got, err)
	}
}

func TestCleanupOrphanedRetainedValueLogPreservesStablePinnedSegment(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	database, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(database.valueLogDir, valueLogName(0, 1))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("stable retained value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogReader.RegisterSegment(path, fileID); err != nil {
		t.Fatal(err)
	}
	database.markValueLogRetain(path)
	token, err := database.valueLogReader.StableResourceToken(fileID, valuelog.StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "0", Generation: 1,
		DiagnosticPath: "value_vlog/" + filepath.Base(path), Digest: [32]byte{1},
		Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if database.cleanupOrphanedRetainedValueLog(path) {
		t.Fatal("cleanup reported removal while stable token was live")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stable-pinned retained file disappeared: %v", err)
	}
	database.valueLogMu.Lock()
	_, retained := database.valueLogRetain[path]
	database.valueLogMu.Unlock()
	if !retained {
		t.Fatal("stable-pinned retained file was forgotten")
	}
	token.Release()
	if !database.cleanupOrphanedRetainedValueLog(path) {
		t.Fatal("cleanup did not remove retained file after stable token release")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("retained file still exists after release: %v", err)
	}
}
