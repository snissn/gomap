package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestCollectionQueryReadyGenerationKeyIsQueryIndependentAndInvalidatesPhysicalIdentity(t *testing.T) {
	identity := queryReadyCollectionTestIdentity()
	want, ok := collectionQueryReadyGenerationOpenKey(identity)
	if !ok {
		t.Fatal("expected valid key")
	}
	if got, ok := collectionQueryReadyGenerationOpenKey(identity); !ok || got != want {
		t.Fatalf("key=%+v ok=%v want %+v", got, ok, want)
	}
	mutations := []func(*ColumnStoreCacheIdentity){
		func(id *ColumnStoreCacheIdentity) { id.SchemaHash++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestGeneration++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestChecksum++ },
		func(id *ColumnStoreCacheIdentity) { id.CatalogCommitSeq++ },
		func(id *ColumnStoreCacheIdentity) { id.RecoveryAuthoritativeAppliedCommandLSN++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestRoot++ },
	}
	for i, mutate := range mutations {
		changed := identity
		mutate(&changed)
		got, ok := collectionQueryReadyGenerationOpenKey(changed)
		if !ok {
			t.Fatalf("mutation %d unexpectedly invalid", i)
		}
		if got == want {
			t.Fatalf("mutation %d did not invalidate key", i)
		}
	}
}

func TestCollectionQueryReadyGenerationCacheRejectsStaleAndReplacesByColumnStoreIdentity(t *testing.T) {
	collection := &Collection{}
	identity := queryReadyCollectionTestIdentity()
	files := queryReadyCollectionMissingFiles(t, identity)
	if _, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, files); err == nil {
		t.Fatal("expected absent rebuildable file")
	} else {
		var openErr *typedcolumn.QueryReadyGenerationOpenError
		if !errors.As(err, &openErr) || openErr.State != typedcolumn.QueryReadyOpenAbsentRebuildable {
			t.Fatalf("err=%v", err)
		}
	}
	first := collection.collectionQueryReadyGenerationCacheSnapshot()
	if !first.Present || first.CacheBuilds != 1 || first.Open.State != typedcolumn.QueryReadyOpenAbsentRebuildable {
		t.Fatalf("first=%+v", first)
	}

	staleFiles := files
	staleFiles.Key.ManifestHash[0] ^= 1
	if _, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, staleFiles); err == nil {
		t.Fatal("expected stale key")
	} else {
		var openErr *typedcolumn.QueryReadyGenerationOpenError
		if !errors.As(err, &openErr) || openErr.State != typedcolumn.QueryReadyOpenUnsupportedOrStale {
			t.Fatalf("err=%v", err)
		}
	}
	if after := collection.collectionQueryReadyGenerationCacheSnapshot(); after.CacheBuilds != first.CacheBuilds || after.Invalidations != first.Invalidations {
		t.Fatalf("stale request mutated cache before=%+v after=%+v", first, after)
	}

	nextIdentity := identity
	nextIdentity.ManifestGeneration++
	nextIdentity.ManifestChecksum++
	nextFiles := queryReadyCollectionMissingFiles(t, nextIdentity)
	if _, err := collection.openCollectionQueryReadyGenerationForIdentity(nextIdentity, nextFiles); err == nil {
		t.Fatal("expected next generation absent file")
	}
	after := collection.collectionQueryReadyGenerationCacheSnapshot()
	if after.Identity != nextIdentity || after.CacheBuilds != 2 || after.Invalidations != 1 {
		t.Fatalf("after=%+v", after)
	}
	if err := collection.closeCollectionQueryReadyGenerationCache(); err != nil {
		t.Fatal(err)
	}
	if closed := collection.collectionQueryReadyGenerationCacheSnapshot(); closed.Present || closed.Invalidations != 2 {
		t.Fatalf("closed=%+v", closed)
	}
}

func TestCollectionQueryReadyGenerationLeasePinsOldMappingAcrossInvalidation(t *testing.T) {
	collection := &Collection{}
	identity := queryReadyCollectionTestIdentity()
	oldFiles := queryReadyCollectionValidFiles(t, identity, 8101)
	oldLease, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, oldFiles)
	if err != nil {
		t.Fatalf("open old: %v", err)
	}
	oldPrepared := oldLease.Prepared()
	if oldPrepared == nil || oldPrepared.Closed() {
		t.Fatal("old prepared unexpectedly closed")
	}

	next := identity
	next.ManifestGeneration++
	next.ManifestChecksum++
	newFiles := queryReadyCollectionValidFiles(t, next, 8102)
	newLease, err := collection.openCollectionQueryReadyGenerationForIdentity(next, newFiles)
	if err != nil {
		t.Fatalf("open new: %v", err)
	}
	if oldPrepared.Closed() {
		t.Fatal("identity invalidation closed an actively leased old mapping")
	}
	part, ok := oldPrepared.Part(0)
	if !ok || len(part.View.Image.Bytes) == 0 || part.Generation != identity.ManifestGeneration {
		t.Fatalf("old leased view invalid after replacement: %+v ok=%v", part, ok)
	}
	if err := newLease.Close(); err != nil {
		t.Fatalf("close new lease: %v", err)
	}
	if err := oldLease.Close(); err != nil {
		t.Fatalf("release old lease: %v", err)
	}
	if !oldPrepared.Closed() {
		t.Fatal("stale mapping remained open after final lease release")
	}
	if err := collection.closeCollectionQueryReadyGenerationCache(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionQueryReadyGenerationCloseDefersUnmapUntilActiveLeaseRelease(t *testing.T) {
	collection := &Collection{}
	identity := queryReadyCollectionTestIdentity()
	files := queryReadyCollectionValidFiles(t, identity, 8201)
	lease, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, files)
	if err != nil {
		t.Fatal(err)
	}
	prepared := lease.Prepared()
	if err := collection.closeCollectionQueryReadyGenerationCache(); err != nil {
		t.Fatal(err)
	}
	if prepared.Closed() {
		t.Fatal("collection close unmapped an active reader lease")
	}
	if snapshot := collection.collectionQueryReadyGenerationCacheSnapshot(); snapshot.Present {
		t.Fatalf("detached cache still current: %+v", snapshot)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if !prepared.Closed() {
		t.Fatal("final detached lease release did not close mapping")
	}
}

func queryReadyCollectionTestIdentity() ColumnStoreCacheIdentity {
	return ColumnStoreCacheIdentity{
		Collection: "events", SchemaHash: 41, CatalogSystemRoot: 101, CatalogCommitSeq: 202,
		ManifestGeneration: 7, ManifestChecksum: 303,
		RecoveryAuthoritativeGeneration: 7, RecoveryAuthoritativeChecksum: 303,
		RecoveryAuthoritativeAppliedCommandLSN: 404,
		ManifestRoot:                           505, ManifestRootName: "events-column-manifest",
	}
}

func queryReadyCollectionMissingFiles(t *testing.T, identity ColumnStoreCacheIdentity) typedcolumn.QueryReadyGenerationOpenFiles {
	t.Helper()
	key, ok := collectionQueryReadyGenerationOpenKey(identity)
	if !ok {
		t.Fatal("invalid identity")
	}
	return typedcolumn.QueryReadyGenerationOpenFiles{
		Key: key,
		Base: typedcolumn.QueryReadyGenerationFile{
			Path: filepath.Join(t.TempDir(), "missing.qrbg"), Identity: key.Identity, Kind: typedcolumn.QueryReadyGenerationBase,
		},
		SnapshotGeneration: key.Identity.Generation,
	}
}

func queryReadyCollectionValidFiles(t *testing.T, identity ColumnStoreCacheIdentity, partID uint64) typedcolumn.QueryReadyGenerationOpenFiles {
	t.Helper()
	if !typedcolumn.QueryReadyGenerationFileOpenSupported() {
		t.Skip("query-ready generation file open requires read-only mmap support")
	}
	key, ok := collectionQueryReadyGenerationOpenKey(identity)
	if !ok {
		t.Fatal("invalid identity")
	}
	field := TypedStorageField{Name: "value", Path: "value", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64}
	raw := columnAssetTypedColumnPartDirectViewTestImage(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: int64(partID)}})
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("parse part: %v", err)
	}
	built, err := typedcolumn.BuildQueryReadyBaseGeneration(key.Identity, []typedcolumn.QueryReadyBasePartInput{{SourceGeneration: key.Identity.Generation, Image: image}})
	if err != nil {
		t.Fatalf("build QRBG: %v", err)
	}
	path := filepath.Join(t.TempDir(), "base.qrbg")
	if err := os.WriteFile(path, built.Bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return typedcolumn.QueryReadyGenerationOpenFiles{
		Key:                key,
		Base:               typedcolumn.QueryReadyGenerationFile{Path: path, Identity: key.Identity, Kind: typedcolumn.QueryReadyGenerationBase},
		SnapshotGeneration: key.Identity.Generation,
	}
}
