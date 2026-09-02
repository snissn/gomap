package collections

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestQueryReadyExecutionColdReopenParity(t *testing.T) {
	identity := queryReadyCollectionTestIdentity()
	opened := queryReadyCollectionValidFiles(t, identity, 8401)
	files := QueryReadyColumnGenerationFiles{Base: QueryReadyColumnGenerationFile{
		Path: opened.Base.Path, Offset: opened.Base.Offset, Length: opened.Base.Length,
		Generation: opened.Base.Identity.Generation, Kind: QueryReadyColumnGenerationBase,
	}}
	request := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQuerySumSecondOfDaySquare, ValueColumn: "value"}
	collection := &Collection{}
	run := func() ColumnPhysicalQueryResult {
		runner, err := collection.prepareQueryReadyColumnPhysicalQueryForIdentity(identity, files, request)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if err := runner.Close(); err != nil {
			t.Fatalf("close runner: %v", err)
		}
		if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceQueryReadyBaseDelta || result.Diagnostics.QueryReadyEncodedExecutions != 1 || result.Diagnostics.DocumentMaterializations != 0 || result.Diagnostics.QueryReadyLegacyFallbacks != 0 {
			t.Fatalf("diagnostics=%+v", result.Diagnostics)
		}
		return result
	}
	first := run()
	if err := collection.closeCollectionQueryReadyGenerationCache(); err != nil {
		t.Fatal(err)
	}
	second := run()
	if !slices.Equal(first.Groups, second.Groups) {
		t.Fatalf("cold reopen groups first=%+v second=%+v", first.Groups, second.Groups)
	}
	if err := collection.closeCollectionQueryReadyGenerationCache(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryReadyColumnOpenFilesPreservesDefaultGenerationAndPartBoundsOnPartialOverride(t *testing.T) {
	identity := queryReadyCollectionTestIdentity()
	defaults := typedcolumn.DefaultQueryReadyDeltaBoundPolicy()
	tests := []struct {
		name     string
		maxRows  int64
		maxBytes int64
	}{
		{name: "rows", maxRows: 123},
		{name: "bytes", maxBytes: 456},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opened, err := queryReadyColumnOpenFiles(identity, QueryReadyColumnGenerationFiles{
				Base: QueryReadyColumnGenerationFile{
					Path:       "base.qrbg",
					Generation: identity.ManifestGeneration,
					Kind:       QueryReadyColumnGenerationBase,
				},
				MaxRows:  test.maxRows,
				MaxBytes: test.maxBytes,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := opened.Bound.MaxVisibleGenerations; got != defaults.MaxVisibleGenerations {
				t.Fatalf("MaxVisibleGenerations=%d want default %d", got, defaults.MaxVisibleGenerations)
			}
			if got := opened.Bound.MaxAccumulatedDeltaParts; got != defaults.MaxAccumulatedDeltaParts {
				t.Fatalf("MaxAccumulatedDeltaParts=%d want default %d", got, defaults.MaxAccumulatedDeltaParts)
			}
			if opened.Bound.MaxRows != test.maxRows || opened.Bound.MaxBytes != test.maxBytes {
				t.Fatalf("partial override bound=%+v want rows=%d bytes=%d", opened.Bound, test.maxRows, test.maxBytes)
			}
		})
	}
}

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
		func(id *ColumnStoreCacheIdentity) { id.Collection += "-changed" },
		func(id *ColumnStoreCacheIdentity) { id.SchemaHash++ },
		func(id *ColumnStoreCacheIdentity) { id.CatalogSystemRoot++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestGeneration++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestChecksum++ },
		func(id *ColumnStoreCacheIdentity) { id.CatalogCommitSeq++ },
		func(id *ColumnStoreCacheIdentity) { id.RecoveryAuthoritativeGeneration++ },
		func(id *ColumnStoreCacheIdentity) { id.RecoveryAuthoritativeChecksum++ },
		func(id *ColumnStoreCacheIdentity) { id.RecoveryAuthoritativeAppliedCommandLSN++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestRoot++ },
		func(id *ColumnStoreCacheIdentity) { id.ManifestRootName += "-changed" },
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

func TestCollectionQueryReadyGenerationCacheInvalidatesWhenFileSelectionChanges(t *testing.T) {
	collection := &Collection{}
	identity := queryReadyCollectionTestIdentity()
	firstFiles := queryReadyCollectionValidFiles(t, identity, 8051)
	first, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, firstFiles)
	if err != nil {
		t.Fatal(err)
	}
	firstPart, ok := first.Prepared().Part(0)
	if !ok {
		t.Fatal("first prepared generation has no part")
	}
	firstImage := slices.Clone(firstPart.View.Image.Bytes)
	if len(firstImage) == 0 {
		t.Fatal("first prepared generation has an empty part image")
	}
	if firstPart.View.Image.PartID != 42 {
		t.Fatalf("first part=%+v", firstPart)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondFiles := queryReadyCollectionValidFiles(t, identity, 8052)
	second, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, secondFiles)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	secondPart, ok := second.Prepared().Part(0)
	if !ok {
		t.Fatal("second prepared generation has no part")
	}
	if slices.Equal(secondPart.View.Image.Bytes, firstImage) {
		t.Fatal("file selection replacement reused the first mapped generation")
	}
	if secondPart.View.Image.PartID != 42 {
		t.Fatalf("second part=%+v", secondPart)
	}
	if snapshot := collection.collectionQueryReadyGenerationCacheSnapshot(); snapshot.CacheBuilds != 2 || snapshot.Invalidations != 1 || snapshot.CacheHits != 0 {
		t.Fatalf("replacement snapshot=%+v", snapshot)
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

func TestCollectionQueryReadyGenerationCacheKeepsManagerRegistrationAcrossVectorCleanup(t *testing.T) {
	for _, cleanup := range []struct {
		name string
		run  func(*Collection) error
	}{
		{name: "unregister", run: func(c *Collection) error {
			index, err := newVectorIndex(c, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2})
			if err != nil {
				return err
			}
			if err := c.RegisterVectorIndex(index); err != nil {
				return err
			}
			c.UnregisterVectorIndex(index.name)
			return nil
		}},
		{name: "persist-clean", run: func(c *Collection) error { return c.persistDirtyNativeVectorIndexes() }},
	} {
		t.Run(cleanup.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close() })
			manager := NewCollectionManager(d)
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "events"}); err != nil {
				t.Fatal(err)
			}
			collection, err := manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			identity := queryReadyCollectionTestIdentity()
			files := queryReadyCollectionValidFiles(t, identity, 8301)
			lease, err := collection.openCollectionQueryReadyGenerationForIdentity(identity, files)
			if err != nil {
				t.Fatal(err)
			}
			prepared := lease.Prepared()
			if got := collectionManagerHandleCount(manager); got != 1 {
				t.Fatalf("registered handles=%d want 1", got)
			}
			if err := cleanup.run(collection); err != nil {
				t.Fatal(err)
			}
			if got := collectionManagerHandleCount(manager); got != 1 {
				t.Fatalf("vector cleanup dropped query-ready owner handle count=%d want 1", got)
			}
			if err := collection.closeCollectionQueryReadyGenerationCache(); err != nil {
				t.Fatal(err)
			}
			if prepared.Closed() {
				t.Fatal("cache cleanup unmapped active lease")
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
			if !prepared.Closed() {
				t.Fatal("final lease release did not close mapping")
			}
		})
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
