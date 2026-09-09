package rootpublication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestStableResourceEntrySize64Bit(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) == 8 && unsafe.Sizeof(stableResourceEntry{}) != 232 {
		t.Fatalf("stableResourceEntry size=%d want232", unsafe.Sizeof(stableResourceEntry{}))
	}
}

func writeStableResourceFixture(t testing.TB, dir, name, contents string) *os.File {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func stableTokenFixture(t testing.TB, dir, name string, generation, frontier uint64, reachability ReachabilityField, digest string, options ...func(*StableResourceSpec)) *StableResourceToken {
	t.Helper()
	file := writeStableResourceFixture(t, dir, name, "original-resource-bytes")
	spec := StableResourceSpec{
		Kind:           ResourceValueLog,
		LogicalLane:    "main",
		ResourceID:     name,
		Generation:     generation,
		DiagnosticPath: filepath.Join("maindb", "value_vlog", name),
		File:           file,
		Frontier:       DurableFrontier{Bytes: frontier},
		Digest:         sha256.Sum256([]byte(digest)),
		Reachability:   reachability,
	}
	for _, option := range options {
		option(&spec)
	}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatalf("NewStableResourceToken: %v", err)
	}
	return token
}

func distinctPhysicalTokenFixture(t testing.TB, file *os.File, id uint64) *StableResourceToken {
	t.Helper()
	var objectID [16]byte
	binary.LittleEndian.PutUint64(objectID[:], id)
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: fmt.Sprintf("%d", id), Generation: 1,
		DiagnosticPath: "columns/scale.bin", File: file, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityColumnManifest, ContentSynced: true,
		StableIdentityOverride: StableIdentity{Platform: "scale-test", ObjectID: objectID},
	})
	if err != nil {
		t.Fatalf("new distinct physical token %d: %v", id, err)
	}
	return token
}

func immutableGenerationTokenFixture(t testing.TB, file *os.File, generation uint64, digest [32]byte) *StableResourceToken {
	t.Helper()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceOuterLeafPack, LogicalLane: "packs", ResourceID: fmt.Sprint(generation),
		Generation: generation, DiagnosticPath: "maindb/outer_leaf/generation.pack", File: file,
		Frontier: DurableFrontier{Bytes: 1}, Digest: digest,
		Reachability: ReachabilityOuterLeafPackedPointer,
	})
	if err != nil {
		t.Fatalf("new immutable generation token %d: %v", generation, err)
	}
	return token
}

func TestStableResourceBuilderDistinctPhysicalLookupWorkIsBounded(t *testing.T) {
	const entries = 4096
	file := writeStableResourceFixture(t, t.TempDir(), "scale.bin", "x")
	builder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= entries; id++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
	}
	work := builder.ClosureWorkSnapshot()
	indexedEntries := uint64(entries - stableResourceEntryLinearLookupLimit)
	if work.PhysicalEntryLookupProbes != indexedEntries || work.PhysicalEntryLookupComparisons != 0 || work.PhysicalEntryLookupAdmissions != indexedEntries {
		builder.Abandon()
		t.Fatalf("distinct physical lookup work=%+v want %d probes, zero comparisons, and %d admissions", work, indexedEntries, indexedEntries)
	}
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if got := resources.Len(); got != entries {
		t.Fatalf("frozen entries=%d want %d", got, entries)
	}
}

func TestStableResourceSetDependencyManifestEncodingReusesRetainedEntries(t *testing.T) {
	const entries = 4096
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "manifest-cache.bin", "x")
	makeToken := func(id uint64) *StableResourceToken {
		return distinctPhysicalTokenFixture(t, file, id)
	}
	builder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= entries; id++ {
		if err := builder.Add(makeToken(id)); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	inherited, _, err := CloneStableResourceSetForLogicalObligationsWithWork(source, StableLogicalObligationRequirements{})
	if err != nil {
		t.Fatal(err)
	}

	first, firstWork, err := source.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	if firstWork.EntriesVisited != entries || firstWork.EntriesEncoded != entries || firstWork.BytesEncoded == 0 {
		t.Fatalf("first manifest work=%+v", firstWork)
	}
	second, secondWork, err := source.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	if secondWork.EntriesVisited != entries || secondWork.EntriesEncoded != 0 || secondWork.BytesEncoded != 0 {
		t.Fatalf("cached manifest work=%+v", secondWork)
	}
	if !bytes.Equal(dependencyManifestPayloadForTest(t, first), dependencyManifestPayloadForTest(t, second)) || first.digest != second.digest {
		t.Fatal("cached manifest changed canonical V1 encoding")
	}
	// Reassembly must stay within the existing payload-sized budget, not
	// copy every wide normalized entry again. Allow payload size-class rounding
	// and bounded sorting/traversal overhead without timing the operation.
	const builds = 4
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range builds {
		manifest, work, err := source.DependencyManifestV1()
		if err != nil || work.EntriesEncoded != 0 || manifest.digest != first.digest {
			t.Fatalf("cached reassembly: work=%+v err=%v", work, err)
		}
	}
	runtime.ReadMemStats(&after)
	bytesPerBuild := (after.TotalAlloc - before.TotalAlloc) / builds
	byteLimit := uint64(2*first.byteLength + 32*entries + 64<<10)
	t.Logf("cached manifest assembly: entries=%d payload=%d bytes/build=%d limit=%d", entries, first.byteLength, bytesPerBuild, byteLimit)
	if bytesPerBuild > byteLimit {
		t.Fatalf("cached manifest assembly allocated %d bytes/build, limit %d", bytesPerBuild, byteLimit)
	}

	childBuilder := NewStableResourceSetBuilder()
	if err := childBuilder.Add(makeToken(entries + 1)); err != nil {
		inherited.Release()
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		inherited.Release()
		t.Fatal(err)
	}
	candidateBuilder := NewStableResourceSetBuilder()
	if err := candidateBuilder.Merge(inherited); err != nil {
		child.Release()
		t.Fatal(err)
	}
	if err := candidateBuilder.Merge(child); err != nil {
		candidateBuilder.Abandon()
		t.Fatal(err)
	}
	candidate, err := candidateBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	_, candidateWork, err := candidate.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	if candidateWork.EntriesVisited != entries+1 || candidateWork.EntriesEncoded != 1 || candidateWork.BytesEncoded == 0 {
		t.Fatalf("one-entry append manifest work=%+v", candidateWork)
	}
}

func TestStableResourceSetDependencyManifestSurvivesCoalescingAndRelease(t *testing.T) {
	dir := t.TempDir()
	first := stableTokenFixture(t, dir, "first.vlog", 1, 8, ReachabilityValueLogPointer, "same-header")
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(first); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	manifest, _, err := source.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	wantEntries, wantPayload, wantDigest := manifest.Entries(), dependencyManifestPayloadForTest(t, manifest), manifest.digest
	inherited, err := CloneStableResourceSetExcludingKinds(source)
	if err != nil {
		t.Fatal(err)
	}
	advanced := stableTokenFixture(t, dir, "advanced.vlog", 1, 16, ReachabilityValueLogPointer, "same-header", func(spec *StableResourceSpec) {
		spec.StableIdentityOverride = first.Identity()
		spec.ResourceID = "first.vlog"
	})
	next := NewStableResourceSetBuilder()
	if err := next.Merge(inherited); err != nil {
		t.Fatal(err)
	}
	if err := next.Add(advanced); err != nil {
		next.Abandon()
		t.Fatal(err)
	}
	candidate, err := next.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	updated, work, err := candidate.DependencyManifestV1()
	if err != nil || work.EntriesEncoded != 1 || updated.Entries()[0].Frontier.Bytes != 16 {
		t.Fatalf("advanced manifest: work=%+v err=%v", work, err)
	}
	source.Release()
	candidate.Release()
	if ResourceOwnerState(first.owner.Load()) != ResourceOwnerReleased || ResourceOwnerState(advanced.owner.Load()) != ResourceOwnerReleased {
		t.Fatal("manifest retained physical pins after resource owners released")
	}
	if !reflect.DeepEqual(manifest.Entries(), wantEntries) || !bytes.Equal(dependencyManifestPayloadForTest(t, manifest), wantPayload) || manifest.digest != wantDigest {
		t.Fatal("coalescing/release changed the old immutable manifest")
	}
	uncached, err := NewDependencyManifestV1(manifest.Entries())
	if err != nil || uncached.digest != wantDigest || !bytes.Equal(dependencyManifestPayloadForTest(t, uncached), wantPayload) {
		t.Fatalf("cached/uncached canonical encoding differs after release: %v", err)
	}
}

func TestStableResourceBuilderMergeDistinctPhysicalLookupWorkIsBounded(t *testing.T) {
	const entries = 2048
	file := writeStableResourceFixture(t, t.TempDir(), "scale.bin", "x")
	parent := NewStableResourceSetBuilder()
	childBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= entries; id++ {
		if err := parent.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil {
			t.Fatal(err)
		}
		if err := childBuilder.Add(distinctPhysicalTokenFixture(t, file, id+entries)); err != nil {
			t.Fatal(err)
		}
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer child.Release()
	if err := parent.Merge(child); err != nil {
		t.Fatal(err)
	}
	work := parent.ClosureWorkSnapshot()
	indexedEntries := uint64(entries*2 - stableResourceEntryLinearLookupLimit)
	if work.PhysicalEntryLookupProbes != indexedEntries || work.PhysicalEntryLookupComparisons != 0 || work.PhysicalEntryLookupAdmissions != indexedEntries {
		parent.Abandon()
		t.Fatalf("distinct physical merge work=%+v", work)
	}
	resources, err := parent.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if got := resources.Len(); got != entries*2 {
		t.Fatalf("frozen entries=%d want %d", got, entries*2)
	}
}

func TestStableResourceBuilderRepresentativeReplacementUpdatesLogicalLookup(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "shared.bin", "x")
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "shared.bin", DiagnosticPath: "columns/shared.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	newToken := func(lane, resourceID, path string, file *os.File, ns *StableNamespaceToken) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceColumnAsset, LogicalLane: lane, ResourceID: resourceID, Generation: 1,
			DiagnosticPath: path, File: file, Frontier: DurableFrontier{Bytes: 1},
			Digest: sha256.Sum256([]byte("shared")), Reachability: ReachabilityColumnManifest,
			Namespace: ns, ContentSynced: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	builder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, resource, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Add(newToken("old", "old", "columns/old.bin", resource, nil)); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(newToken("new", "new", "columns/shared.bin", resource, namespace)); err != nil {
		t.Fatal(err)
	}
	other := writeStableResourceFixture(t, dir, "other.bin", "x")
	if err := builder.Add(newToken("old", "old", "columns/other.bin", other, nil)); err != nil {
		t.Fatalf("stale logical representative lookup rejected a distinct replacement: %v", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if got := set.Len(); got != stableResourceEntryLinearLookupLimit+2 {
		t.Fatalf("entries=%d want %d after representative replacement", got, stableResourceEntryLinearLookupLimit+2)
	}
}

func TestStableResourceBuilderClosureWorkSnapshotConcurrentWithAdd(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "scale.bin", "x")
	builder := NewStableResourceSetBuilder()
	tokens := make([]*StableResourceToken, 64)
	for index := range tokens {
		tokens[index] = distinctPhysicalTokenFixture(t, file, uint64(index+1))
	}
	done := make(chan error, 1)
	go func() {
		for _, token := range tokens {
			if err := builder.Add(token); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			builder.Abandon()
			return
		default:
			_ = builder.State()
			_ = builder.ClosureWorkSnapshot()
		}
	}
}

func testStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.vlog")
	file := writeStableResourceFixture(t, dir, "000001.vlog", "old-identity")
	var synced atomic.Bool
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "1", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/000001.vlog", File: file,
		Frontier: DurableFrontier{Bytes: uint64(len("old-identity"))}, Reachability: ReachabilityValueLogPointer,
		SyncThrough: func(pinned *os.File, _ DurableFrontier) error {
			got := make([]byte, len("old-identity"))
			if _, err := pinned.ReadAt(got, 0); err != nil {
				return err
			}
			if string(got) != "old-identity" {
				return errors.New("sync callback observed path replacement")
			}
			synced.Store(true)
			return pinned.Sync()
		},
	})
	if err != nil {
		t.Fatalf("register token: %v", err)
	}
	t.Cleanup(token.Release)
	if err := os.Rename(path, filepath.Join(dir, "rotated.vlog")); err != nil {
		t.Fatalf("rename original: %v", err)
	}
	if err := os.WriteFile(path, []byte("new-identity"), 0o600); err != nil {
		t.Fatalf("recreate path: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough: %v", err)
	}
	if !synced.Load() {
		t.Fatal("pinned sync callback was not invoked")
	}
}

func testCloneStableResourceSetUsesExactHandlesAndIndependentPins(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "asset.bin", "original-resource-bytes")
	identity, err := StableIdentityFromFile(resource)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	observed := true
	defer func() {
		if observed {
			_ = registry.Unobserve(identity)
		}
	}()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "asset.bin", DiagnosticPath: "columns/asset.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "asset", Generation: 1,
		DiagnosticPath: "columns/asset.bin", File: resource,
		Frontier: DurableFrontier{Bytes: uint64(len("original-resource-bytes"))},
		Digest:   sha256.Sum256([]byte("asset")), Reachability: ReachabilityColumnManifest,
		Namespace: namespace, ContentSynced: true, PinRegistry: registry,
		OnRelease: func() { _ = registry.Unobserve(identity) },
	})
	if err != nil {
		t.Fatal(err)
	}
	observed = false
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	clone, work, err := CloneStableResourceSetForLogicalObligationsWithWork(source, StableLogicalObligationRequirements{})
	if err != nil {
		source.Release()
		t.Fatal(err)
	}
	if work.PhysicalHandleCopies != 0 || work.PhysicalHandleShares != 0 || work.PhysicalRootShares != 1 {
		clone.Release()
		source.Release()
		t.Fatalf("clone handle work=%+v, want one immutable-root share and no per-handle work", work)
	}
	if err := source.Tokens()[0].WithPinnedFile(func(sourceFile *os.File) error {
		return clone.Tokens()[0].WithPinnedFile(func(cloneFile *os.File) error {
			if sourceFile != cloneFile {
				return errors.New("clone did not share the exact retained handle")
			}
			return nil
		})
	}); err != nil {
		clone.Release()
		source.Release()
		t.Fatal(err)
	}
	if got := clone.Descriptors(); len(got) != 1 || got[0].Identity() != source.Descriptors()[0].Identity() {
		clone.Release()
		source.Release()
		t.Fatalf("clone descriptors = %+v, want exact source identity", got)
	}
	source.Release()
	if got := registry.Stats().ActivePins; got != 1 {
		clone.Release()
		t.Fatalf("active pins after source release = %d, want 1", got)
	}
	path := filepath.Join(dir, "asset.bin")
	if err := os.Rename(path, filepath.Join(dir, "old-asset.bin")); err != nil {
		clone.Release()
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement-resource"), 0o600); err != nil {
		clone.Release()
		t.Fatal(err)
	}
	var got string
	if err := clone.Tokens()[0].WithPinnedFile(func(file *os.File) error {
		buf := make([]byte, len("original-resource-bytes"))
		if _, err := file.ReadAt(buf, 0); err != nil {
			return err
		}
		got = string(buf)
		return nil
	}); err != nil {
		clone.Release()
		t.Fatal(err)
	}
	if got != "original-resource-bytes" {
		clone.Release()
		t.Fatalf("cloned handle read %q, want original bytes", got)
	}
	clone.Release()
	if got := registry.Stats().ActivePins; got != 0 {
		t.Fatalf("active pins after clone release = %d, want 0", got)
	}
}

func TestCloneStableResourceSetSharesCustomSyncHandle(t *testing.T) {
	var syncs atomic.Uint64
	token := stableTokenFixture(t, t.TempDir(), "custom-sync.bin", 1, 8, ReachabilityValueLogPointer, "custom-sync", func(spec *StableResourceSpec) {
		spec.ContentSynced = true
		spec.SyncThrough = func(*os.File, DurableFrontier) error {
			syncs.Add(1)
			return nil
		}
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	clone, work, err := CloneStableResourceSetForLogicalObligationsWithWork(source, StableLogicalObligationRequirements{})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	if work.PhysicalHandleCopies != 0 || work.PhysicalHandleShares != 0 || work.PhysicalRootShares != 1 {
		t.Fatalf("custom-sync clone work=%+v, want one immutable-root share and no per-handle work", work)
	}
	source.Release()
	if err := clone.Tokens()[0].syncThrough(DurableFrontier{Bytes: 9}); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != 1 {
		t.Fatalf("custom sync calls=%d, want 1 after source release", syncs.Load())
	}
}

func testCloneStableResourceSetFiltersExactLogicalObligationClosure(t *testing.T) {
	dir := t.TempDir()
	makeObligation := func(partID, fileID uint64) StableLogicalObligation {
		obligation := StableLogicalObligation{
			Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
			Generation: 1, PartID: partID, FileID: fileID, Offset: 0, Length: 8,
			Checksum: uint32(partID), Reachability: ReachabilityColumnManifest,
		}
		obligation.Digest = sha256.Sum256([]byte(fmt.Sprintf("obligation-%d", partID)))
		return obligation
	}
	keep, stale := makeObligation(1, 1), makeObligation(2, 2)
	builder := NewStableResourceSetBuilder()
	for i, obligation := range []StableLogicalObligation{keep, stale} {
		token := stableTokenFixture(t, dir, fmt.Sprintf("asset-%d.bin", i+1), uint64(i+1), 8, ReachabilityColumnManifest, fmt.Sprintf("asset-%d", i+1), func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.LogicalLane = "columns"
			spec.ResourceID = fmt.Sprint(i + 1)
			spec.LogicalObligations = []StableLogicalObligation{obligation}
			spec.ContentSynced = true
		})
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	requirements := StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{keep},
	}
	clone, err := CloneStableResourceSetForLogicalObligations(source, requirements)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	if err := ValidateStableResourceSetLogicalObligations(clone, requirements); err != nil {
		t.Fatal(err)
	}
	descriptors := clone.Descriptors()
	if len(descriptors) != 1 || len(descriptors[0].LogicalObligations()) != 1 || descriptors[0].LogicalObligations()[0] != keep {
		t.Fatalf("filtered descriptors=%+v want only retained obligation", descriptors)
	}
	if err := ValidateStableResourceSetLogicalObligations(clone, StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{stale},
	}); !errors.Is(err, ErrResourceConflict) && !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("mismatched exact closure err=%v", err)
	}
	empty, err := CloneStableResourceSetForLogicalObligations(source, StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Release()
	if empty.Len() != 0 {
		t.Fatalf("empty scoped closure retained %d resources", empty.Len())
	}
}

func testCloneStableResourceSetReportsPhysicalAndLogicalWorkSeparately(t *testing.T) {
	dir := t.TempDir()
	makeObligation := func(partID uint64) StableLogicalObligation {
		obligation := StableLogicalObligation{
			Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
			Generation: 1, PartID: partID, FileID: 1, Offset: int64(partID * 8), Length: 8,
			Checksum: uint32(partID), Reachability: ReachabilityColumnManifest,
		}
		obligation.Digest = sha256.Sum256([]byte(fmt.Sprintf("work-obligation-%d", partID)))
		return obligation
	}
	obligations := []StableLogicalObligation{makeObligation(1), makeObligation(2), makeObligation(3)}
	token := stableTokenFixture(t, dir, "asset.bin", 1, 8, ReachabilityColumnManifest, "asset", func(spec *StableResourceSpec) {
		spec.Kind = ResourceColumnAsset
		spec.LogicalLane = "columns"
		spec.ResourceID = "1"
		spec.LogicalObligations = obligations
		spec.ContentSynced = true
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()

	clone, work, err := CloneStableResourceSetForLogicalObligationsWithWork(source, StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  obligations[:2],
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	if work.CloneOperations != 1 || work.FreezeOperations != 1 || work.SourceEntriesInspected != 1 || work.SourceObligationsInspected != 3 {
		t.Fatalf("clone operation/source work=%+v", work)
	}
	if work.RetainedEntries != 1 || work.RetainedObligations != 2 || work.DroppedEntries != 0 || work.DroppedObligations != 1 {
		t.Fatalf("retained/dropped work=%+v", work)
	}
	if work.CopiedEntries != 1 || work.CopiedObligations != 2 || work.PhysicalHandleCopies != 1 || work.LogicalObligationNormalizations != 0 {
		t.Fatalf("copy/normalization work=%+v", work)
	}
}

func testAppendOnlyResourceClosureCloneWorkIsBoundedByMutation(t *testing.T) {
	const batches = 32
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "asset.bin", string(make([]byte, 4096)))
	makeObligation := func(partID uint64) StableLogicalObligation {
		obligation := StableLogicalObligation{
			Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
			Generation: partID, PartID: partID, FileID: 1, Offset: int64(partID * 8), Length: 8,
			Checksum: uint32(partID), Reachability: ReachabilityColumnManifest,
		}
		obligation.Digest = sha256.Sum256([]byte(fmt.Sprintf("append-only-obligation-%d", partID)))
		return obligation
	}
	makeToken := func(obligation StableLogicalObligation) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", Generation: 1,
			DiagnosticPath: "columns/asset.bin", File: file, Frontier: DurableFrontier{Bytes: uint64(obligation.Offset + obligation.Length)},
			Digest: sha256.Sum256([]byte("append-only-segment")), Reachability: ReachabilityColumnManifest,
			LogicalObligations: []StableLogicalObligation{obligation}, ContentSynced: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	all := make([]StableLogicalObligation, 0, batches)
	var cumulative StableResourceClosureWork
	var visible *StableResourceSet
	for batch := 1; batch <= batches; batch++ {
		added := makeObligation(uint64(batch))
		all = append(all, added)
		mutation := StableLogicalObligationMutation{
			ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
			Added:        []StableLogicalObligation{added},
		}
		inherited, cloneWork, err := CloneStableResourceSetApplyingLogicalObligationMutation(visible, mutation)
		if err != nil {
			t.Fatal(err)
		}
		producerBuilder := NewStableResourceSetBuilder()
		if err := producerBuilder.Add(makeToken(added)); err != nil {
			t.Fatal(err)
		}
		producer, err := producerBuilder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidateBuilder := NewStableResourceSetBuilder()
		if inherited != nil {
			if err := candidateBuilder.Merge(inherited); err != nil {
				producer.Release()
				t.Fatal(err)
			}
		}
		mergeWork, err := candidateBuilder.MergeAppendOnlyLogicalObligations(producer, mutation)
		if err != nil {
			t.Fatal(err)
		}
		next, err := candidateBuilder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		cumulative.RequirementObligationsInspected += cloneWork.RequirementObligationsInspected + mergeWork.RequirementObligationsInspected
		cumulative.SourceObligationsInspected += cloneWork.SourceObligationsInspected + mergeWork.SourceObligationsInspected
		cumulative.CopiedObligations += cloneWork.CopiedObligations + mergeWork.CopiedObligations
		cumulative.LogicalObligationNormalizations += cloneWork.LogicalObligationNormalizations + mergeWork.LogicalObligationNormalizations
		cumulative.RetainedIndexNodeVisits += cloneWork.RetainedIndexNodeVisits + mergeWork.RetainedIndexNodeVisits
		cumulative.RetainedIndexNodeCopies += cloneWork.RetainedIndexNodeCopies + mergeWork.RetainedIndexNodeCopies
		cumulative.LogicalIndexNodesAdmitted += cloneWork.LogicalIndexNodesAdmitted + mergeWork.LogicalIndexNodesAdmitted
		visible.Release()
		visible = next
	}
	defer visible.Release()
	descriptors := visible.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), all) {
		t.Fatalf("final append-only closure descriptors=%+v want %d exact obligations", descriptors, len(all))
	}
	// One new obligation per batch should not make candidate clone work follow
	// the triangular retained-history total (batches*(batches+1)/2).
	if cumulative.RequirementObligationsInspected > batches*2 || cumulative.SourceObligationsInspected > batches*2 || cumulative.CopiedObligations > batches*2 || cumulative.LogicalObligationNormalizations > batches*2 {
		t.Fatalf("append-only cumulative clone work=%+v want O(%d) mutation work", cumulative, batches)
	}
	if cumulative.RetainedIndexNodeVisits == 0 || cumulative.RetainedIndexNodeVisits > batches*16 || cumulative.RetainedIndexNodeCopies != cumulative.RetainedIndexNodeVisits || cumulative.LogicalIndexNodesAdmitted != batches-1 {
		t.Fatalf("append-only persistent-index work=%+v want explicit O(new obligations * index depth)", cumulative)
	}
}

func testLogicalObligationRemovalMutationUsesExactFilter(t *testing.T) {
	dir := t.TempDir()
	makeObligation := func(partID uint64) StableLogicalObligation {
		obligation := StableLogicalObligation{
			Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
			Generation: 1, PartID: partID, FileID: partID, Offset: 0, Length: 8,
			Checksum: uint32(partID), Reachability: ReachabilityColumnManifest,
		}
		obligation.Digest = sha256.Sum256([]byte(fmt.Sprintf("remove-obligation-%d", partID)))
		return obligation
	}
	keep, remove := makeObligation(1), makeObligation(2)
	builder := NewStableResourceSetBuilder()
	for i, obligation := range []StableLogicalObligation{keep, remove} {
		token := stableTokenFixture(t, dir, fmt.Sprintf("remove-%d.bin", i), uint64(i+1), 8, ReachabilityColumnManifest, fmt.Sprintf("remove-%d", i), func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.LogicalLane = "columns"
			spec.ResourceID = fmt.Sprint(i + 1)
			spec.LogicalObligations = []StableLogicalObligation{obligation}
			spec.ContentSynced = true
		})
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	filtered, work, err := CloneStableResourceSetApplyingLogicalObligationMutation(source, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Removed:      []StableLogicalObligation{remove},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer filtered.Release()
	if work.AppendOnlyFastPath != 0 || work.RemovedObligations != 1 || work.SourceObligationsInspected == 0 {
		t.Fatalf("destructive mutation work=%+v want exact full filter", work)
	}
	descriptors := filtered.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []StableLogicalObligation{keep}) {
		t.Fatalf("filtered descriptors=%+v want only retained obligation", descriptors)
	}
	if got := source.Descriptors(); len(got) != 2 {
		t.Fatalf("source closure changed by destructive clone: %+v", got)
	}
}

func testStableResourceSetRejectsDataStableNamespaceUnstable(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "000001.vlog", "original-resource-bytes")
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "maindb/value_vlog/000001.vlog",
	})
	if err != nil {
		t.Fatalf("namespace token: %v", err)
	}
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "000001.vlog", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/000001.vlog", File: resource, Frontier: DurableFrontier{Bytes: 8},
		Digest: sha256.Sum256([]byte("header")), Reachability: ReachabilityValueLogPointer, Namespace: namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := builder.Freeze(); !errors.Is(err, ErrNamespaceUnstable) {
		t.Fatalf("Freeze error = %v, want ErrNamespaceUnstable", err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatalf("Stabilize: %v", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatalf("Freeze after namespace sync: %v", err)
	}
	set.Release()
}

func TestStableResourceSetRetainsRotatedIdentitiesAndGreatestFrontier(t *testing.T) {
	dir := t.TempDir()
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	first := stableTokenFixture(t, dir, "000001.vlog", 1, 8, ReachabilityValueLogPointer, "header-one")
	firstAdvanced := stableTokenFixture(t, dir, "000001-copy.vlog", 1, 16, ReachabilityValueLogPointer, "header-one", func(spec *StableResourceSpec) {
		spec.StableIdentityOverride = first.Identity()
		spec.ResourceID = "000001.vlog"
	})
	second := stableTokenFixture(t, dir, "000002.vlog", 2, 8, ReachabilityValueLogPointer, "header-two")
	third := stableTokenFixture(t, dir, "000003.vlog", 3, 8, ReachabilityValueLogPointer, "header-three")
	for _, token := range []*StableResourceToken{first, firstAdvanced, second, third} {
		if err := builder.Add(token); err != nil {
			t.Fatalf("Add generation %d: %v", token.Generation(), err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	defer set.Release()
	if got := set.Len(); got != 3 {
		t.Fatalf("set.Len()=%d want 3 rotated identities", got)
	}
	if got := set.FrontierFor(first.Identity(), 1).Bytes; got != 16 {
		t.Fatalf("coalesced frontier=%d want 16", got)
	}
	if count, bytes := set.PhysicalSummary(); count != 3 || bytes != 32 {
		t.Fatalf("physical summary=(%d,%d) want (3,32)", count, bytes)
	}
}

func TestStableResourceSetUnionsExactRIDMembershipAndExposesCoalescedDescriptor(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "000001.vlog", "original-resource-bytes")
	digest := sha256.Sum256([]byte("segment-header"))
	newToken := func(lane, resourceID string, frontier DurableFrontier) *StableResourceToken {
		t.Helper()
		frontier.Bytes += uint64(len(lane))
		frontier.MaxLSN += uint64(len(resourceID))
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceCommandWALExternalRID, LogicalLane: lane, ResourceID: resourceID, Generation: 1,
			DiagnosticPath: "maindb/value_vlog/000001.vlog", File: file, Frontier: frontier,
			Digest: digest, Reachability: ReachabilityCommandWALExternalRIDFence,
		})
		if err != nil {
			t.Fatalf("NewStableResourceToken: %v", err)
		}
		return token
	}

	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			first := newToken("first", "segment-a", NewRIDFrontier([]uint64{9, 2, 9, 20}))
			second := newToken("second-lane", "segment-bb", NewRIDFrontier([]uint64{4, 9, 20, 30}))
			tokens := []*StableResourceToken{first, second}
			if reverse {
				tokens[0], tokens[1] = tokens[1], tokens[0]
			}
			builder := NewStableResourceSetBuilder(ReachabilityCommandWALExternalRIDFence)
			for _, token := range tokens {
				if err := builder.Add(token); err != nil {
					t.Fatalf("Add: %v", err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatalf("Freeze: %v", err)
			}
			defer set.Release()
			if got := set.Len(); got != 1 {
				t.Fatalf("set.Len()=%d want one physical segment", got)
			}

			descriptors := set.Descriptors()
			if len(descriptors) != 1 {
				t.Fatalf("descriptor count=%d want 1", len(descriptors))
			}
			descriptor := descriptors[0]
			if descriptor.LogicalLane() != "first" || descriptor.ResourceID() != "segment-a" || descriptor.DiagnosticPath() != "maindb/value_vlog/000001.vlog" {
				t.Fatalf("descriptor logical representative=(%q,%q,%q), want deterministic first/segment-a/path", descriptor.LogicalLane(), descriptor.ResourceID(), descriptor.DiagnosticPath())
			}
			wantRIDs := []uint64{2, 4, 9, 20, 30}
			if got := descriptor.RIDs(); !slices.Equal(got, wantRIDs) {
				t.Fatalf("descriptor RIDs=%v want %v", got, wantRIDs)
			}
			frontier := descriptor.Frontier()
			if frontier.Bytes != uint64(len("second-lane")) || frontier.MaxLSN != uint64(len("segment-bb")) {
				t.Fatalf("coalesced scalar frontier=%+v", frontier)
			}
			wantRIDFrontier := NewRIDFrontier(wantRIDs)
			if frontier.RIDCount != wantRIDFrontier.RIDCount || frontier.RIDMin != wantRIDFrontier.RIDMin ||
				frontier.RIDMax != wantRIDFrontier.RIDMax || frontier.MaxRID != wantRIDFrontier.MaxRID ||
				frontier.RIDSetDigest != wantRIDFrontier.RIDSetDigest {
				t.Fatalf("coalesced RID summary=%+v want %+v", frontier, wantRIDFrontier)
			}
			if descriptor.Kind() != ResourceCommandWALExternalRID || descriptor.Identity() != first.Identity() ||
				descriptor.Generation() != 1 || descriptor.Digest() != digest {
				t.Fatalf("descriptor identity metadata changed: kind=%q identity=%+v generation=%d digest=%x",
					descriptor.Kind(), descriptor.Identity(), descriptor.Generation(), descriptor.Digest())
			}
			if got := descriptor.ReachabilityFields(); !slices.Equal(got, []ReachabilityField{ReachabilityCommandWALExternalRIDFence}) {
				t.Fatalf("descriptor reachability=%v", got)
			}

			returned := descriptor.RIDs()
			returned[0] = 999
			fields := descriptor.ReachabilityFields()
			fields[0] = ReachabilityValueLogPointer
			if got := set.Descriptors()[0].RIDs(); !slices.Equal(got, wantRIDs) {
				t.Fatalf("descriptor returned-slice mutation changed set RIDs: %v", got)
			}
			if got := set.Descriptors()[0].ReachabilityFields(); !slices.Equal(got, []ReachabilityField{ReachabilityCommandWALExternalRIDFence}) {
				t.Fatalf("descriptor returned-slice mutation changed fields: %v", got)
			}
			fromLookup := set.FrontierFor(first.Identity(), 1).RIDs()
			fromLookup[0] = 777
			if got := set.FrontierFor(first.Identity(), 1).RIDs(); !slices.Equal(got, wantRIDs) {
				t.Fatalf("FrontierFor returned-slice mutation changed set RIDs: %v", got)
			}
		})
	}
}

func TestStableResourceSetRejectsConflictingLogicalObligationAtomically(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "logical.asset", "0123456789abcdef")
	physicalDigest := sha256.Sum256([]byte("physical-segment"))
	logical := StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "query_ready_base_v1", Namespace: "events",
		Generation: 7, PartID: 11, FileID: 1, Offset: 0, Length: 4, Checksum: 101,
		Reachability: ReachabilityQueryReadyBase, Digest: sha256.Sum256([]byte("logical-one")),
	}
	newToken := func(frontier uint64, obligation StableLogicalObligation) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceQueryReadyAsset, LogicalLane: "events", ResourceID: "1", Generation: 1,
			DiagnosticPath: "logical.asset", File: file, Frontier: DurableFrontier{Bytes: frontier},
			Digest: physicalDigest, Reachability: ReachabilityQueryReadyBase,
			LogicalObligations: []StableLogicalObligation{obligation},
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	builder := NewStableResourceSetBuilder(ReachabilityQueryReadyBase)
	if err := builder.Add(newToken(4, logical)); err != nil {
		t.Fatal(err)
	}
	conflicting := logical
	conflicting.Checksum++
	conflicting.Digest = sha256.Sum256([]byte("logical-two"))
	if err := builder.Add(newToken(8, conflicting)); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("conflicting logical obligation error=%v want ErrResourceConflict", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	descriptor := set.Descriptors()[0]
	if descriptor.Frontier().Bytes != 4 {
		t.Fatalf("rejected logical obligation advanced frontier=%d want 4", descriptor.Frontier().Bytes)
	}
	obligations := descriptor.LogicalObligations()
	if len(obligations) != 1 || obligations[0] != logical {
		t.Fatalf("logical obligations after rejection=%+v want original", obligations)
	}
	obligations[0].PartID = 99
	if got := descriptor.LogicalObligations()[0].PartID; got != logical.PartID {
		t.Fatalf("descriptor logical obligations are mutable through returned slice: part=%d", got)
	}
}

func TestStableResourceSetLargeLogicalObligationIndexRejectsConflictAtomically(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "large-logical.asset", "0123456789abcdef")
	physicalDigest := sha256.Sum256([]byte("large-physical-segment"))
	obligations := make([]StableLogicalObligation, stableLogicalObligationLinearLimit+1)
	for i := range obligations {
		obligations[i] = StableLogicalObligation{
			Class: "column-asset-ref-v1", Kind: "query_ready_base_v1", Namespace: "events",
			Generation: 7, PartID: uint64(i + 1), FileID: 1, Offset: int64(i), Length: 1,
			Checksum: uint32(100 + i), Reachability: ReachabilityQueryReadyBase,
			Digest: sha256.Sum256([]byte(fmt.Sprintf("large-logical-%d", i))),
		}
	}
	newToken := func(logical []StableLogicalObligation) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceQueryReadyAsset, LogicalLane: "events", ResourceID: "1", Generation: 1,
			DiagnosticPath: "large-logical.asset", File: file, Frontier: DurableFrontier{Bytes: 16},
			Digest: physicalDigest, Reachability: ReachabilityQueryReadyBase, LogicalObligations: logical,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	builder := NewStableResourceSetBuilder(ReachabilityQueryReadyBase)
	if err := builder.Add(newToken(obligations)); err != nil {
		t.Fatal(err)
	}
	conflicting := obligations[stableLogicalObligationLinearLimit/2]
	conflicting.Checksum++
	conflicting.Digest = sha256.Sum256([]byte("large-logical-conflict"))
	if err := builder.Add(newToken([]StableLogicalObligation{conflicting})); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("large conflicting logical obligation error=%v want ErrResourceConflict", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	descriptor := set.Descriptors()[0]
	if got := len(descriptor.LogicalObligations()); got != len(obligations) {
		t.Fatalf("logical obligations after large rejection=%d want %d", got, len(obligations))
	}
	if descriptor.Frontier().Bytes != 16 {
		t.Fatalf("large rejected logical obligation advanced frontier=%d want 16", descriptor.Frontier().Bytes)
	}
}

func stableLogicalObligationFixture(partID uint64, checksum uint32, label string) StableLogicalObligation {
	return StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "query_ready_base_v1", Namespace: "events",
		Generation: 7, PartID: partID, FileID: 1, Offset: int64(partID), Length: 1,
		Checksum: checksum, Reachability: ReachabilityQueryReadyBase,
		Digest: sha256.Sum256([]byte(label)),
	}
}

func TestStableResourceSetRejectedLogicalObligationBatchDoesNotLeakEarlierAddition(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "atomic-logical.asset", "0123456789abcdef")
	physicalDigest := sha256.Sum256([]byte("atomic-logical-physical"))
	newToken := func(obligations []StableLogicalObligation) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceQueryReadyAsset, LogicalLane: "events", ResourceID: "1", Generation: 1,
			DiagnosticPath: "atomic-logical.asset", File: file, Frontier: DurableFrontier{Bytes: 16},
			Digest: physicalDigest, Reachability: ReachabilityQueryReadyBase, LogicalObligations: obligations,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	original := stableLogicalObligationFixture(2, 200, "atomic-logical-original")
	builder := NewStableResourceSetBuilder(ReachabilityQueryReadyBase)
	if err := builder.Add(newToken([]StableLogicalObligation{original})); err != nil {
		t.Fatal(err)
	}
	conflicting := original
	conflicting.Checksum++
	conflicting.Digest = sha256.Sum256([]byte("atomic-logical-conflict"))
	if err := builder.Add(newToken([]StableLogicalObligation{
		stableLogicalObligationFixture(1, 100, "atomic-logical-new-before-conflict"),
		conflicting,
	})); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("conflicting logical obligation batch error=%v want ErrResourceConflict", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if got := set.Descriptors()[0].LogicalObligations(); len(got) != 1 || got[0] != original {
		t.Fatalf("rejected logical obligation batch mutated builder obligations: %+v", got)
	}
}

func TestStableResourceSetRejectedLargeLogicalObligationBatchDoesNotLeakEarlierAddition(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "atomic-large-logical.asset", "0123456789abcdef")
	physicalDigest := sha256.Sum256([]byte("atomic-large-logical-physical"))
	newToken := func(obligations []StableLogicalObligation) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceQueryReadyAsset, LogicalLane: "events", ResourceID: "1", Generation: 1,
			DiagnosticPath: "atomic-large-logical.asset", File: file, Frontier: DurableFrontier{Bytes: 16},
			Digest: physicalDigest, Reachability: ReachabilityQueryReadyBase, LogicalObligations: obligations,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	original := make([]StableLogicalObligation, stableLogicalObligationLinearLimit+1)
	for i := range original {
		original[i] = stableLogicalObligationFixture(uint64(i+2), uint32(200+i), fmt.Sprintf("atomic-large-logical-%d", i))
	}
	builder := NewStableResourceSetBuilder(ReachabilityQueryReadyBase)
	if err := builder.Add(newToken(original)); err != nil {
		t.Fatal(err)
	}
	conflicting := original[len(original)-1]
	conflicting.Checksum++
	conflicting.Digest = sha256.Sum256([]byte("atomic-large-logical-conflict"))
	if err := builder.Add(newToken([]StableLogicalObligation{
		stableLogicalObligationFixture(1, 100, "atomic-large-logical-new-before-conflict"),
		conflicting,
	})); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("large conflicting logical obligation batch error=%v want ErrResourceConflict", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if got := set.Descriptors()[0].LogicalObligations(); len(got) != len(original) {
		t.Fatalf("rejected large logical obligation batch mutated builder count=%d want %d", len(got), len(original))
	}
}

func TestStableResourceTokenCapturesSingleLogicalObligationByValue(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "single-logical.asset", "0123456789abcdef")
	original := stableLogicalObligationFixture(1, 100, "single-logical-original")
	input := []StableLogicalObligation{original}
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceQueryReadyAsset, LogicalLane: "events", ResourceID: "1", Generation: 1,
		DiagnosticPath: "single-logical.asset", File: file, Frontier: DurableFrontier{Bytes: 16},
		Digest: sha256.Sum256([]byte("single-logical-physical")), Reachability: ReachabilityQueryReadyBase,
		LogicalObligations: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	input[0] = stableLogicalObligationFixture(9, 900, "single-logical-mutated")
	if got := token.LogicalObligations(); len(got) != 1 || got[0] != original {
		t.Fatalf("token aliases caller logical obligation input: %+v", got)
	}
}

func TestStableResourceTokenCapturesExactRIDMembershipByValue(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "capture.vlog", "original-resource-bytes")
	rids := []uint64{9, 2, 4}
	frontier := NewRIDFrontier(rids)
	rids[0] = 900 // Mutation after frontier construction, before registration.
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceCommandWALExternalRID, LogicalLane: "main", ResourceID: "capture", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/capture.vlog", File: file, Frontier: frontier,
		Digest: sha256.Sum256([]byte("segment-header")), Reachability: ReachabilityCommandWALExternalRIDFence,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	rids[1] = 200 // Mutation after registration.
	returned := token.Frontier().RIDs()
	returned[0] = 100
	if got, want := token.Frontier().RIDs(), []uint64{2, 4, 9}; !slices.Equal(got, want) {
		t.Fatalf("captured RIDs=%v want %v", got, want)
	}
}

func TestStableResourceTokenRejectsRIDSummaryWithoutExactMembership(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "summary-only.vlog", "original-resource-bytes")
	exact := NewRIDFrontier([]uint64{2, 4, 9})
	summaryOnly := DurableFrontier{
		MaxRID: exact.MaxRID, RIDSetDigest: exact.RIDSetDigest, RIDCount: exact.RIDCount,
		RIDMin: exact.RIDMin, RIDMax: exact.RIDMax,
	}
	_, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceCommandWALExternalRID, LogicalLane: "main", ResourceID: "summary-only", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/summary-only.vlog", File: file, Frontier: summaryOnly,
		Digest: sha256.Sum256([]byte("segment-header")), Reachability: ReachabilityCommandWALExternalRIDFence,
	})
	if !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("NewStableResourceToken error=%v want ErrUnresolvedResource", err)
	}
}

func TestStableResourceSetSyncUsesCoalescedGreatestFrontier(t *testing.T) {
	dir := t.TempDir()
	var synced atomic.Uint64
	first := stableTokenFixture(t, dir, "frontier-first.vlog", 1, 8, ReachabilityValueLogPointer, "same-header", func(spec *StableResourceSpec) {
		spec.ResourceID = "frontier.vlog"
		spec.SyncThrough = func(_ *os.File, frontier DurableFrontier) error {
			synced.Store(frontier.Bytes)
			return nil
		}
	})
	advanced := stableTokenFixture(t, dir, "frontier-advanced.vlog", 1, 16, ReachabilityValueLogPointer, "same-header", func(spec *StableResourceSpec) {
		spec.ResourceID = first.ResourceID()
		spec.StableIdentityOverride = first.Identity()
	})
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(first); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(advanced); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := synced.Load(); got != 16 {
		t.Fatalf("sync frontier=%d want coalesced greatest frontier 16", got)
	}
}

func TestStableResourceSetLogicalClonePreservesContentCertificate(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, certifiedBytes := range []uint64{0, 8, 16} {
			t.Run(fmt.Sprintf("scoped=%t/certified=%d", scoped, certifiedBytes), func(t *testing.T) {
				dir := t.TempDir()
				file := writeStableResourceFixture(t, dir, "asset.bin", "0123456789abcdef")
				obligation := StableLogicalObligation{
					Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
					Generation: 1, PartID: 1, FileID: 1, Length: 8, Checksum: 1,
					Digest: sha256.Sum256([]byte("obligation")), Reachability: ReachabilityColumnManifest,
				}
				want := errors.New("producer sync failed")
				var flushes, syncs atomic.Uint64
				newToken := func(frontier uint64, certified bool) *StableResourceToken {
					t.Helper()
					required := NewRIDFrontier([]uint64{frontier})
					required.Bytes, required.MaxLSN = frontier, frontier
					token, err := NewStableResourceToken(StableResourceSpec{
						Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", Generation: 1,
						DiagnosticPath: "asset.bin", File: file, Frontier: required,
						Digest: sha256.Sum256([]byte("header")), Reachability: ReachabilityColumnManifest,
						LogicalObligations: []StableLogicalObligation{obligation}, ContentSynced: certified,
						FlushThrough: func(_ *os.File, frontier DurableFrontier) error {
							if frontier.Bytes != 16 {
								t.Errorf("flush frontier=%d want16", frontier.Bytes)
							}
							flushes.Add(1)
							return nil
						},
						SyncThrough: func(_ *os.File, frontier DurableFrontier) error {
							if frontier.Bytes != 16 {
								t.Errorf("sync frontier=%d want16", frontier.Bytes)
							}
							if syncs.Add(1) == 1 {
								return want
							}
							return nil
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					return token
				}
				builder := NewStableResourceSetBuilder()
				defer builder.Abandon()
				if certifiedBytes != 0 {
					if err := file.Sync(); err != nil {
						t.Fatal(err)
					}
					if err := builder.Add(newToken(certifiedBytes, true)); err != nil {
						t.Fatal(err)
					}
				}
				if certifiedBytes != 16 {
					if err := builder.Add(newToken(16, false)); err != nil {
						t.Fatal(err)
					}
				}
				source, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				defer source.Release()
				requirements := StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityQueryReadyBase}}
				if scoped {
					requirements = StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: []StableLogicalObligation{obligation}}
				}
				clone, err := CloneStableResourceSetForLogicalObligations(source, requirements)
				if err != nil {
					t.Fatal(err)
				}
				defer clone.Release()
				clone.mu.Lock()
				entries := clone.entrySnapshotLocked()
				clonedToken := activeEntryToken(entries[0])
				certificate := cloneDurableFrontier(clonedToken.syncedFrontier)
				hasCertificate := clonedToken.hasSyncedFrontier
				clone.mu.Unlock()
				if hasCertificate != (certifiedBytes != 0) || certificate.Bytes != certifiedBytes || certificate.MaxLSN != certifiedBytes {
					t.Fatalf("clone changed certificate: present=%t frontier=%+v", hasCertificate, certificate)
				}
				if certifiedBytes != 0 && !slices.Equal(certificate.RIDs(), []uint64{certifiedBytes}) {
					t.Fatalf("clone changed exact RID certificate: %v", certificate.RIDs())
				}
				stats := clone.Stats(time.Now())
				wantPhysical := uint64(0)
				if certifiedBytes != 0 {
					wantPhysical = 1
				}
				if len(stats) != 1 || stats[0].PhysicalFileSyncs != wantPhysical {
					t.Fatalf("clone manufactured physical sync metrics: %+v", stats)
				}
				source.Release()
				if err := clone.FlushThrough(); err != nil {
					t.Fatal(err)
				}
				if got := flushes.Load(); got != 1 {
					t.Errorf("producer flush calls=%d want1", got)
				}
				err = clone.SyncThrough()
				if certifiedBytes == 16 {
					if err != nil || syncs.Load() != 0 {
						t.Fatalf("covered clone sync=%v calls=%d", err, syncs.Load())
					}
					return
				}
				if !errors.Is(err, want) {
					t.Fatalf("uncertified frontier sync error=%v want producer failure", err)
				}
				if err := clone.SyncThrough(); err != nil {
					t.Fatal(err)
				}
				if got := syncs.Load(); got != 2 {
					t.Fatalf("producer sync attempts=%d want2", got)
				}
			})
		}
	}
}

func TestStableResourceSetMergeFrozenUnsyncedChildPreservesSyncRequirement(t *testing.T) {
	dir := t.TempDir()
	var childSyncs, parentSyncs atomic.Uint64
	childToken := stableTokenFixture(t, dir, "child.vlog", 1, 16, ReachabilityValueLogPointer, "child", func(spec *StableResourceSpec) {
		spec.SyncThrough = func(_ *os.File, frontier DurableFrontier) error {
			if frontier.Bytes != 16 {
				t.Fatalf("child sync frontier=%d want16", frontier.Bytes)
			}
			childSyncs.Add(1)
			return nil
		}
	})
	childBuilder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	defer childBuilder.Abandon()
	if err := childBuilder.Add(childToken); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer child.Release()
	parent := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	defer parent.Abandon()
	if err := parent.Add(stableTokenFixture(t, dir, "parent.vlog", 1, 8, ReachabilityValueLogPointer, "parent", func(spec *StableResourceSpec) {
		spec.SyncThrough = func(_ *os.File, _ DurableFrontier) error {
			parentSyncs.Add(1)
			return nil
		}
	})); err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(child); err != nil {
		t.Fatal(err)
	}
	set, err := parent.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := parentSyncs.Load(); got != 1 {
		t.Errorf("unrelated parent sync calls=%d want1", got)
	}
	if got := childSyncs.Load(); got != 1 {
		t.Errorf("unsynced child sync calls=%d want1 after frozen-child materialization", got)
	}
}

func TestStableResourceSetAlreadySyncedTokenDoesNotCoverAdvancedCoalescedFrontier(t *testing.T) {
	for _, syncedFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("synced-first=%t", syncedFirst), func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.OpenFile(filepath.Join(dir, "shared.vlog"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write([]byte("12345678")); err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			var syncCalls atomic.Uint64
			var syncedBytes atomic.Uint64
			newToken := func(frontier uint64, alreadySynced bool) *StableResourceToken {
				token, err := NewStableResourceToken(StableResourceSpec{
					Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "shared", Generation: 1,
					DiagnosticPath: "shared.vlog", File: file, Frontier: DurableFrontier{Bytes: frontier},
					Digest: sha256.Sum256([]byte("same-header")), Reachability: ReachabilityValueLogPointer,
					ContentSynced: alreadySynced,
					SyncThrough: func(_ *os.File, requested DurableFrontier) error {
						syncCalls.Add(1)
						syncedBytes.Store(requested.Bytes)
						return nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return token
			}
			synced := newToken(8, true)
			if _, err := file.Write([]byte("abcdefgh")); err != nil {
				t.Fatal(err)
			}
			advanced := newToken(16, false)
			builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
			ordered := []*StableResourceToken{synced, advanced}
			if !syncedFirst {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			for _, token := range ordered {
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			if err := set.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			if got := syncCalls.Load(); got != 1 {
				t.Fatalf("physical sync calls=%d want 1", got)
			}
			if got := syncedBytes.Load(); got != 16 {
				t.Fatalf("synced bytes=%d want 16", got)
			}
		})
	}
}

func TestStableResourceSetStrongerProducerCertificateCoversCoalescedFrontier(t *testing.T) {
	for _, certifiedFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("certified-first=%t", certifiedFirst), func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.OpenFile(filepath.Join(dir, "shared.vlog"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write([]byte("12345678")); err != nil {
				t.Fatal(err)
			}
			var syncCalls atomic.Uint64
			obligation := StableLogicalObligation{
				Class: "column-asset-ref-v1", Kind: "part", Namespace: "main", Generation: 1,
				PartID: 1, FileID: 1, Length: 16, Checksum: 1,
				Digest: sha256.Sum256([]byte("obligation")), Reachability: ReachabilityValueLogPointer,
			}
			newToken := func(frontier uint64, certified bool) *StableResourceToken {
				t.Helper()
				token, err := NewStableResourceToken(StableResourceSpec{
					Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "shared", Generation: 1,
					DiagnosticPath: "shared.vlog", File: file, Frontier: DurableFrontier{Bytes: frontier},
					Digest: sha256.Sum256([]byte("same-header")), Reachability: ReachabilityValueLogPointer,
					ContentSynced: certified, LogicalObligations: []StableLogicalObligation{obligation},
					SyncThrough: func(_ *os.File, _ DurableFrontier) error {
						syncCalls.Add(1)
						return nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return token
			}
			old := newToken(8, false)
			if _, err := file.Write([]byte("abcdefgh")); err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			certified := newToken(16, true)
			ordered := []*StableResourceToken{old, certified}
			if certifiedFirst {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
			for _, token := range ordered {
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			if err := set.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			if got := syncCalls.Load(); got != 0 {
				t.Fatalf("physical sync calls=%d want0", got)
			}
			stats := set.Stats(time.Now())
			if len(stats) != 1 || stats[0].PhysicalFileSyncs != 1 || stats[0].PhysicalFileSyncDuration != 0 {
				t.Fatalf("entry-certified stats=%+v want one retained barrier with no invented duration", stats)
			}
			for _, scoped := range []bool{false, true} {
				requirements := StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityQueryReadyBase}}
				if scoped {
					requirements = StableLogicalObligationRequirements{
						ScopedFields: []ReachabilityField{ReachabilityValueLogPointer},
						Obligations:  []StableLogicalObligation{obligation},
					}
				}
				clone, err := CloneStableResourceSetForLogicalObligations(set, requirements)
				if err != nil {
					t.Fatal(err)
				}
				if err := clone.SyncThrough(); err != nil {
					clone.Release()
					t.Fatal(err)
				}
				clone.Release()
			}
			if got := syncCalls.Load(); got != 0 {
				t.Fatalf("physical sync calls after logical clones=%d want0", got)
			}
			parent := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
			defer parent.Abandon()
			if err := parent.Add(newToken(8, false)); err != nil {
				t.Fatal(err)
			}
			if err := parent.Merge(set); err != nil {
				t.Fatal(err)
			}
			merged, err := parent.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer merged.Release()
			if err := merged.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			if got := syncCalls.Load(); got != 0 {
				t.Fatalf("physical sync calls after generic materialization=%d want0", got)
			}
		})
	}
}

func TestStableResourceSetDoesNotUnionIncomparableProducerCertificates(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "shared.vlog", "0123456789abcdef")
	defer file.Close()
	left, right := NewRIDFrontier([]uint64{1}), NewRIDFrontier([]uint64{2})
	left.Bytes, left.MaxLSN = 16, 1
	right.Bytes, right.MaxLSN = 8, 2
	var syncCalls atomic.Uint64
	newToken := func(frontier DurableFrontier) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "shared", Generation: 1,
			DiagnosticPath: "shared.vlog", File: file, Frontier: frontier,
			Digest: sha256.Sum256([]byte("same-header")), Reachability: ReachabilityValueLogPointer,
			ContentSynced: true,
			SyncThrough: func(_ *os.File, got DurableFrontier) error {
				syncCalls.Add(1)
				if got.Bytes != 16 || got.MaxLSN != 2 || !slices.Equal(got.RIDs(), []uint64{1, 2}) {
					t.Fatalf("sync frontier=%+v rids=%v want bytes16 lsn2 rids[1 2]", got, got.RIDs())
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	for _, token := range []*StableResourceToken{newToken(left), newToken(right)} {
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if syncCalls.Load() != 1 {
		t.Fatalf("physical sync calls=%d want1", syncCalls.Load())
	}
}

func TestStableResourceSetEntryCertificateDoesNotCoverLargerCloneDestination(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "shared.vlog", "0123456789abcdefghij")
	defer file.Close()
	var syncCalls atomic.Uint64
	newToken := func(frontier uint64, certified bool) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "shared", Generation: 1,
			DiagnosticPath: "shared.vlog", File: file, Frontier: DurableFrontier{Bytes: frontier},
			Digest: sha256.Sum256([]byte("same-header")), Reachability: ReachabilityValueLogPointer,
			ContentSynced: certified,
			SyncThrough: func(_ *os.File, got DurableFrontier) error {
				syncCalls.Add(1)
				if got.Bytes != 20 {
					t.Fatalf("sync frontier=%d want20", got.Bytes)
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	sourceBuilder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := sourceBuilder.Add(newToken(16, true)); err != nil {
		t.Fatal(err)
	}
	source, err := sourceBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	destination := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	defer destination.Abandon()
	if err := destination.Add(newToken(20, false)); err != nil {
		t.Fatal(err)
	}
	if err := destination.Merge(source); err != nil {
		t.Fatal(err)
	}
	set, err := destination.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if syncCalls.Load() != 1 {
		t.Fatalf("physical sync calls=%d want1", syncCalls.Load())
	}
}

func TestStableResourceTokenSyncedRIDCoverage(t *testing.T) {
	stable := NewRIDFrontier([]uint64{2, 8, 20})
	stable.Bytes, stable.MaxLSN = 8, 5
	for _, tc := range []struct {
		name       string
		rids       []uint64
		bytes, lsn uint64
		wantSync   bool
	}{
		{"equal", []uint64{2, 8, 20}, 8, 5, false},
		{"subset", []uint64{8, 20}, 8, 5, false},
		{"empty", nil, 8, 5, false},
		{"same summary bounds different member", []uint64{2, 9, 20}, 8, 5, true},
		{"higher RID", []uint64{21}, 8, 5, true},
		{"advanced bytes", []uint64{2, 8, 20}, 9, 5, true},
		{"advanced LSN", []uint64{2, 8, 20}, 8, 6, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			token := stableTokenFixture(t, t.TempDir(), "synced-rids.vlog", 1, 8, ReachabilityValueLogPointer, "same-header", func(spec *StableResourceSpec) {
				spec.Frontier, spec.ContentSynced = stable, true
				spec.SyncThrough = func(_ *os.File, frontier DurableFrontier) error {
					calls++
					if frontier.Bytes != tc.bytes || frontier.MaxLSN != tc.lsn || !slices.Equal(frontier.RIDs(), tc.rids) {
						t.Fatal("sync lost requested frontier")
					}
					return nil
				}
			})
			defer token.Release()
			required := NewRIDFrontier(tc.rids)
			required.Bytes, required.MaxLSN = tc.bytes, tc.lsn
			if err := token.syncThrough(required); err != nil {
				t.Fatal(err)
			}
			if (calls == 1) != tc.wantSync || calls > 1 {
				t.Fatalf("sync calls=%d want sync=%t", calls, tc.wantSync)
			}
		})
	}
}

func TestStableResourceSetRejectsFrontierIdentityDigestAndGenerationConflicts(t *testing.T) {
	dir := t.TempDir()
	t.Run("frontier beyond file", func(t *testing.T) {
		file := writeStableResourceFixture(t, dir, "short.vlog", "short")
		_, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "short", Generation: 1,
			DiagnosticPath: "short.vlog", File: file, Frontier: DurableFrontier{Bytes: 99},
			Reachability: ReachabilityValueLogPointer,
		})
		if !errors.Is(err, ErrFrontierBeyondResource) {
			t.Fatalf("error=%v want ErrFrontierBeyondResource", err)
		}
	})
	t.Run("digest", func(t *testing.T) {
		first := stableTokenFixture(t, dir, "digest-a", 1, 1, ReachabilityColumnManifest, "a", func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
		})
		second := stableTokenFixture(t, dir, "digest-b", 1, 1, ReachabilityColumnManifest, "b", func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.ResourceID = first.ResourceID()
			spec.StableIdentityOverride = first.Identity()
		})
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(first); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(second); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("error=%v want ErrResourceConflict", err)
		}
		if _, err := second.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrResourceOwnership) {
			t.Fatalf("rejected token remains builder-owned: ReadAt error=%v want ErrResourceOwnership", err)
		}
		builder.Abandon()
	})
	t.Run("logical generation identity", func(t *testing.T) {
		first := stableTokenFixture(t, dir, "generation-a", 7, 1, ReachabilityValueLogPointer, "same")
		second := stableTokenFixture(t, dir, "generation-b", 7, 1, ReachabilityValueLogPointer, "same", func(spec *StableResourceSpec) {
			spec.ResourceID = first.ResourceID()
		})
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(first); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(second); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("error=%v want ErrResourceConflict", err)
		}
		builder.Abandon()
	})
}

func TestStableResourceNestedUnionMustResolveEveryRequiredChild(t *testing.T) {
	dir := t.TempDir()
	child := NewStableResourceSetBuilder(ReachabilityColumnManifest, ReachabilityVectorGraphPack)
	if err := child.Add(stableTokenFixture(t, dir, "column.asset", 1, 4, ReachabilityColumnManifest, "column")); err != nil {
		t.Fatal(err)
	}
	if _, err := child.Freeze(); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("child Freeze error=%v want ErrUnresolvedResource", err)
	}
	if err := child.Add(stableTokenFixture(t, dir, "vector.asset", 1, 4, ReachabilityVectorGraphPack, "vector", func(spec *StableResourceSpec) {
		spec.Kind = ResourceVectorGraphPack
	})); err != nil {
		t.Fatal(err)
	}
	childSet, err := child.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	parent := NewStableResourceSetBuilder(ReachabilityIndexFile, ReachabilityColumnManifest, ReachabilityVectorGraphPack)
	if err := parent.Add(stableTokenFixture(t, dir, "index.db", 1, 4, ReachabilityIndexFile, "index", func(spec *StableResourceSpec) {
		spec.Kind = ResourceIndex
	})); err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(childSet); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	set, err := parent.Freeze()
	if err != nil {
		t.Fatalf("parent Freeze: %v", err)
	}
	set.Release()
}

func TestCompositeRegistrarExposesIDsOnlyAfterCompleteTransitiveFreeze(t *testing.T) {
	type childSpec struct {
		field ReachabilityField
		kind  ResourceKind
		id    string
	}
	children := []childSpec{
		{ReachabilityDictionaryGeneration, ResourceDictionary, "dictionary-7"},
		{ReachabilityTemplateGeneration, ResourceTemplate, "template-9"},
		{ReachabilityVectorGraphPack, ResourceVectorGraphPack, "vector-11"},
		{ReachabilityColumnManifest, ResourceColumnAsset, "column-13"},
	}
	required := make([]ReachabilityField, len(children))
	for i := range children {
		required[i] = children[i].field
	}
	buildChild := func(t *testing.T, spec childSpec, suffix string) *StableResourceSet {
		t.Helper()
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, spec.id+suffix, 1, 4, spec.field, spec.id, func(tokenSpec *StableResourceSpec) {
			tokenSpec.Kind = spec.kind
			tokenSpec.ResourceID = spec.id
		})
		builder := NewStableResourceSetBuilder(spec.field)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return set
	}

	for omitted := range children {
		for _, reverse := range []bool{false, true} {
			name := fmt.Sprintf("omit-%s-reverse-%t", children[omitted].field, reverse)
			t.Run(name, func(t *testing.T) {
				registrar := NewStableCompositeRegistrar(required...)
				order := append([]childSpec(nil), children...)
				if reverse {
					for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
						order[i], order[j] = order[j], order[i]
					}
				}
				for _, child := range order {
					if child.field == children[omitted].field {
						continue
					}
					if err := registrar.RegisterChild(child.field, child.id, buildChild(t, child, name)); err != nil {
						t.Fatal(err)
					}
				}
				set, ids, err := registrar.Freeze()
				if !errors.Is(err, ErrUnresolvedResource) || set != nil || ids != nil {
					t.Fatalf("incomplete Freeze set=%v ids=%v err=%v", set, ids, err)
				}
				registrar.Abandon()
			})
		}
	}

	var wantIDs []string
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete-reverse-%t", reverse), func(t *testing.T) {
			registrar := NewStableCompositeRegistrar(required...)
			order := append([]childSpec(nil), children...)
			if reverse {
				for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
					order[i], order[j] = order[j], order[i]
				}
			}
			for _, child := range order {
				if err := registrar.RegisterChild(child.field, child.id, buildChild(t, child, fmt.Sprint(reverse))); err != nil {
					t.Fatal(err)
				}
			}
			set, ids, err := registrar.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			got := make([]string, len(ids))
			for i := range ids {
				got[i] = string(ids[i].Field()) + "=" + ids[i].Value()
			}
			if wantIDs == nil {
				wantIDs = got
			} else if fmt.Sprint(got) != fmt.Sprint(wantIDs) {
				t.Fatalf("registered IDs order=%v want deterministic %v", got, wantIDs)
			}
		})
	}
}

func TestCompositeRegistrarRejectsIDUnrelatedToCoveredChildResource(t *testing.T) {
	dir := t.TempDir()
	childBuilder := NewStableResourceSetBuilder(ReachabilityDictionaryGeneration)
	if err := childBuilder.Add(stableTokenFixture(
		t, dir, "dictionary-7", 7, 4, ReachabilityDictionaryGeneration, "dictionary-7",
		func(spec *StableResourceSpec) { spec.Kind = ResourceDictionary },
	)); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer child.Release()

	registrar := NewStableCompositeRegistrar(ReachabilityDictionaryGeneration)
	defer registrar.Abandon()
	if err := registrar.RegisterChild(ReachabilityDictionaryGeneration, "dictionary-999", child); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("RegisterChild error=%v want ErrResourceConflict", err)
	}
}

func TestMutableIndexAliasesCoalesceByPhysicalIdentity(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "index.db", "0123456789abcdef")
	digest := sha256.Sum256([]byte("index-header"))
	fields := []ReachabilityField{
		ReachabilityIndexFile,
		ReachabilityMetaPage,
		ReachabilityUserRoot,
		ReachabilitySystemRoot,
		ReachabilityFreelist,
	}
	builder := NewStableResourceSetBuilder(fields...)
	var identity StableIdentity
	for i, field := range fields {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceIndex, LogicalLane: fmt.Sprintf("root-lane-%d", i),
			ResourceID: fmt.Sprintf("root-page-%d", i), Generation: 9,
			DiagnosticPath: "maindb/index.db", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest, Reachability: field,
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			identity = token.Identity()
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add %q: %v", field, err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 1 {
		t.Fatalf("same-index DB aliases retained %d physical pins, want 1", set.Len())
	}
	if got := set.FrontierFor(identity, 9).Bytes; got != uint64(len(fields)) {
		t.Fatalf("same-index DB frontier=%d want %d", got, len(fields))
	}
	wantFields := append([]ReachabilityField(nil), fields...)
	slices.Sort(wantFields)
	if got := set.Descriptors()[0].ReachabilityFields(); !slices.Equal(got, wantFields) {
		t.Fatalf("same-index DB descriptor fields=%v want %v", got, wantFields)
	}
}

func TestMutableCollectionAndTextRootAliasesCoalesceByPhysicalIdentity(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "index.db", "0123456789abcdef")
	digest := sha256.Sum256([]byte("index-header"))
	fields := []ReachabilityField{
		ReachabilityCollectionSystemRoot,
		ReachabilityCollectionPrimaryRoot,
		ReachabilityCollectionTemplateRoot,
		ReachabilityCollectionIndexStateRoot,
		ReachabilityCollectionColumnRoot,
		ReachabilityCollectionSecondaryRoot,
		ReachabilityCollectionVectorRoot,
		ReachabilityCollectionTextDictionary,
		ReachabilityCollectionTextPosting,
		ReachabilityCollectionTextPosition,
	}
	builder := NewStableResourceSetBuilder(fields...)
	for i, field := range fields {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceIndex, LogicalLane: fmt.Sprintf("collection-%d", i),
			ResourceID: fmt.Sprintf("catalog-root-%d", i), Generation: 11,
			DiagnosticPath: "maindb/index.db", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest, Reachability: field,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add %q: %v", field, err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 1 {
		t.Fatalf("same-index collection/text aliases retained %d physical pins, want 1", set.Len())
	}
}

func TestImmutableAliasesCoalesceByIdentityAndDigest(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "generation.pack", "immutable-pack")
	digest := sha256.Sum256([]byte("immutable-pack"))
	builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
	for i := range 2 {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: fmt.Sprintf("pack-lane-%d", i),
			ResourceID: fmt.Sprintf("pack-alias-%d", i), Generation: 7,
			DiagnosticPath: "maindb/outer_leaf/generation.pack", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest,
			Reachability: ReachabilityOuterLeafPackedPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add immutable alias: %v", err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 1 {
		t.Fatalf("immutable aliases retained %d physical pins, want 1", set.Len())
	}
}

func TestImmutableAliasesPreserveDistinctGenerationPins(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "generation.pack", "immutable-pack")
	digest := sha256.Sum256([]byte("immutable-pack"))
	builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil {
			t.Fatal(err)
		}
	}
	tokens := make([]*StableResourceToken, 0, 2)
	for _, generation := range []uint64{7, 8, 7} {
		i := len(tokens)
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: fmt.Sprintf("pack-lane-%d", i),
			ResourceID: fmt.Sprintf("pack-alias-%d", i), Generation: generation,
			DiagnosticPath: "maindb/outer_leaf/generation.pack", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest,
			Reachability: ReachabilityOuterLeafPackedPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add immutable generation: %v", err)
		}
		if generation != 7 || len(tokens) == 0 {
			tokens = append(tokens, token)
		}
	}
	if work := builder.ClosureWorkSnapshot(); work.PhysicalEntryLookupComparisons != 1 || work.PhysicalEntryLookupAdmissions != 2 {
		builder.Abandon()
		t.Fatalf("indexed immutable collision work=%+v want one same-generation comparison and two admissions", work)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != stableResourceEntryLinearLookupLimit+len(tokens) {
		t.Fatalf("immutable generations retained %d physical pins, want %d", set.Len(), stableResourceEntryLinearLookupLimit+len(tokens))
	}
	guard := set.DeletionGuard()
	for _, token := range tokens {
		if err := guard.Check(token.Identity(), token.Generation()); !errors.Is(err, ErrResourcePinned) {
			t.Fatalf("Check generation %d=%v want ErrResourcePinned", token.Generation(), err)
		}
		want := token.Frontier()
		if token.Generation() == 7 {
			want.Bytes = 3 // the indexed same-generation alias coalesces at its larger frontier.
		}
		if got := set.FrontierFor(token.Identity(), token.Generation()); got.Bytes != want.Bytes {
			t.Fatalf("FrontierFor generation %d=%+v want %+v", token.Generation(), got, want)
		}
	}
}

func TestImmutableGenerationLookupWorkIsBounded(t *testing.T) {
	const entries = 4096
	file := writeStableResourceFixture(t, t.TempDir(), "generation.pack", "immutable-pack")
	digest := sha256.Sum256([]byte("immutable-pack"))
	builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
	defer builder.Abandon()
	for generation := uint64(1); generation <= entries; generation++ {
		if err := builder.Add(immutableGenerationTokenFixture(t, file, generation, digest)); err != nil {
			t.Fatalf("Add generation %d: %v", generation, err)
		}
	}
	indexedEntries := uint64(entries - stableResourceEntryLinearLookupLimit)
	work := builder.ClosureWorkSnapshot()
	if work.PhysicalEntryLookupProbes != indexedEntries || work.PhysicalEntryLookupComparisons != 0 || work.PhysicalEntryLookupAdmissions != indexedEntries {
		t.Fatalf("immutable-generation lookup work=%+v want %d probes, zero comparisons, and %d admissions", work, indexedEntries, indexedEntries)
	}
	if err := builder.Add(immutableGenerationTokenFixture(t, file, entries+1, sha256.Sum256([]byte("conflicting-pack")))); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("cross-generation digest conflict=%v want ErrResourceConflict", err)
	}
	mutable, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceIndex, LogicalLane: "main", ResourceID: "index", Generation: 1,
		DiagnosticPath: "maindb/index.db", File: file, Frontier: DurableFrontier{Bytes: 1},
		Digest: digest, Reachability: ReachabilityIndexFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(mutable); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("cross-stability identity conflict=%v want ErrResourceConflict", err)
	}
}

func TestImmutableIdentityRejectsConflictingDigest(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "generation.pack", "immutable-pack")
	builder := NewStableResourceSetBuilder()
	for i, digest := range [][32]byte{sha256.Sum256([]byte("first")), sha256.Sum256([]byte("second"))} {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: fmt.Sprintf("pack-%d", i), ResourceID: fmt.Sprint(i),
			Generation: uint64(i + 1), DiagnosticPath: "maindb/outer_leaf/generation.pack", File: file,
			Frontier: DurableFrontier{Bytes: 1}, Digest: digest, Reachability: ReachabilityOuterLeafPackedPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = builder.Add(token)
		if i == 1 {
			if !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("conflicting immutable digest error=%v want ErrResourceConflict", err)
			}
			builder.Abandon()
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("conflicting immutable digest unexpectedly registered")
}

func TestLogicalResourceRejectsDifferentPhysicalIdentity(t *testing.T) {
	dir := t.TempDir()
	digest := sha256.Sum256([]byte("index-header"))
	builder := NewStableResourceSetBuilder()
	for _, name := range []string{"index-a.db", "index-b.db"} {
		file := writeStableResourceFixture(t, dir, name, "index-page")
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceIndex, LogicalLane: "main", ResourceID: "index", Generation: 7,
			DiagnosticPath: "maindb/index.db", File: file, Frontier: DurableFrontier{Bytes: 1},
			Digest: digest, Reachability: ReachabilityIndexFile,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = builder.Add(token)
		if name == "index-b.db" {
			if !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("logical resource identity replacement error=%v want ErrResourceConflict", err)
			}
			builder.Abandon()
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("logical resource identity replacement unexpectedly registered")
}

func TestBuilderMergeReleasesDroppedDuplicateChildPin(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "shared.vlog", "shared-value-log")
	digest := sha256.Sum256([]byte("vlog-header"))
	var parentReleased, childReleased atomic.Uint64
	makeToken := func(field ReachabilityField, lane, id string, frontier uint64, released *atomic.Uint64) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: lane, ResourceID: id, Generation: 3,
			DiagnosticPath: "maindb/value_vlog/000003.vlog", File: file,
			Frontier: DurableFrontier{Bytes: frontier}, Digest: digest, Reachability: field,
			OnRelease: func() { released.Add(1) },
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	parent := NewStableResourceSetBuilder()
	if err := parent.Add(makeToken(ReachabilityValueLogPointer, "main", "3", 4, &parentReleased)); err != nil {
		t.Fatal(err)
	}
	childBuilder := NewStableResourceSetBuilder()
	if err := childBuilder.Add(makeToken(ReachabilityCommandWALExternalRIDFence, "external", "alias-3", 8, &childReleased)); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(child); err != nil {
		t.Fatal(err)
	}
	if got := childReleased.Load(); got != 1 {
		t.Fatalf("coalesced child releases=%d want 1 immediately after ownership transfer", got)
	}
	if got := parentReleased.Load(); got != 0 {
		t.Fatalf("representative parent releases=%d before final set release", got)
	}
	set, err := parent.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 1 {
		t.Fatalf("merged physical pins=%d want 1", set.Len())
	}
	set.Release()
	if got := parentReleased.Load(); got != 1 {
		t.Fatalf("representative parent releases=%d want 1", got)
	}
}

func TestBuilderMergeDuplicateChildPinStressDoesNotGrowDescriptors(t *testing.T) {
	beforeEntries, fdCheck := os.ReadDir("/dev/fd")
	checkFDs := fdCheck == nil
	const iterations = 256
	var releases atomic.Uint64
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.vlog")
	for i := range iterations {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("shared-value-log")); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte("vlog-header"))
		makeToken := func(field ReachabilityField, id string) *StableResourceToken {
			token, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceValueLog, LogicalLane: id, ResourceID: id, Generation: uint64(i + 1),
				DiagnosticPath: "maindb/value_vlog/shared.vlog", File: file,
				Frontier: DurableFrontier{Bytes: 1}, Digest: digest, Reachability: field,
				OnRelease: func() { releases.Add(1) },
			})
			if err != nil {
				t.Fatal(err)
			}
			return token
		}
		parent := NewStableResourceSetBuilder()
		if err := parent.Add(makeToken(ReachabilityValueLogPointer, "parent")); err != nil {
			t.Fatal(err)
		}
		childBuilder := NewStableResourceSetBuilder()
		if err := childBuilder.Add(makeToken(ReachabilityCommandWALExternalRIDFence, "child")); err != nil {
			t.Fatal(err)
		}
		child, err := childBuilder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		if err := parent.Merge(child); err != nil {
			t.Fatal(err)
		}
		set, err := parent.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		set.Release()
		_ = file.Close()
	}
	if got, want := releases.Load(), uint64(iterations*2); got != want {
		t.Fatalf("released duplicate/representative pins=%d want %d", got, want)
	}
	if checkFDs {
		afterEntries, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Fatal(err)
		}
		if got, wantMax := len(afterEntries), len(beforeEntries)+2; got > wantMax {
			t.Fatalf("descriptor count grew from %d to %d after %d duplicate merges", len(beforeEntries), got, iterations)
		}
	}
}

func TestBuilderMergeConflictRetainsBothOwnersTransactionally(t *testing.T) {
	dir := t.TempDir()
	first := writeStableResourceFixture(t, dir, "first.vlog", "first-resource")
	second := writeStableResourceFixture(t, dir, "second.vlog", "second-resource")
	var releases atomic.Uint64
	makeToken := func(file *os.File) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "7", Generation: 7,
			DiagnosticPath: "maindb/value_vlog/000007.vlog", File: file,
			Frontier: DurableFrontier{Bytes: 1}, Digest: sha256.Sum256([]byte("header")),
			Reachability: ReachabilityValueLogPointer, OnRelease: func() { releases.Add(1) },
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	parent := NewStableResourceSetBuilder()
	if err := parent.Add(makeToken(first)); err != nil {
		t.Fatal(err)
	}
	fillers := writeStableResourceFixture(t, dir, "fillers.bin", "x")
	for id := uint64(1); id <= 7; id++ {
		if err := parent.Add(distinctPhysicalTokenFixture(t, fillers, id)); err != nil {
			t.Fatal(err)
		}
	}
	childBuilder := NewStableResourceSetBuilder()
	for id := uint64(8); id <= 15; id++ {
		if err := childBuilder.Add(distinctPhysicalTokenFixture(t, fillers, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := childBuilder.Add(makeToken(second)); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(child); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Merge conflict=%v want ErrResourceConflict", err)
	}
	if got := releases.Load(); got != 0 {
		t.Fatalf("failed merge released %d pins before owner cleanup", got)
	}
	if child.Owner() != ResourceOwnerBuilder {
		t.Fatalf("failed merge child owner=%v want builder", child.Owner())
	}
	if err := parent.Add(distinctPhysicalTokenFixture(t, fillers, 100)); err != nil {
		t.Fatalf("Add after failed indexed merge: %v", err)
	}
	parent.Abandon()
	child.Release()
	if got := releases.Load(); got != 2 {
		t.Fatalf("explicit cleanup releases=%d want 2", got)
	}
}

func TestPinnedResourceBlocksExplicitDeletionUntilRelease(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "pinned.vlog", 1, 4, ReachabilityValueLogPointer, "pinned")
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	guard := set.DeletionGuard()
	if err := guard.Check(token.Identity(), token.Generation()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Check while pinned=%v want ErrResourcePinned", err)
	}
	physicalIdentity := token.Identity()
	physicalIdentity.Generation = 0
	if err := guard.Check(physicalIdentity, token.Generation()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Check with handle-derived physical identity=%v want ErrResourcePinned", err)
	}
	if got, want := set.FrontierFor(physicalIdentity, token.Generation()), token.Frontier(); got.Bytes != want.Bytes {
		t.Fatalf("FrontierFor with handle-derived physical identity=%+v want %+v", got, want)
	}
	if err := guard.Check(physicalIdentity, token.Generation()+1); err != nil {
		t.Fatalf("Check for unpinned logical generation: %v", err)
	}
	set.Release()
	if err := guard.Check(token.Identity(), token.Generation()); err != nil {
		t.Fatalf("Check after release: %v", err)
	}
}

func TestUnionDeletionGuardRetainsEveryCoalescedSourcePin(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "shared.pack", "immutable-pack")
	digest := sha256.Sum256([]byte("immutable-pack"))
	newSet := func(lane string) (*StableResourceSet, *StableResourceToken) {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: lane, ResourceID: "shared", Generation: 7,
			DiagnosticPath: "maindb/outer_leaf/shared.pack", File: file,
			Frontier: DurableFrontier{Bytes: 14}, Digest: digest,
			Reachability: ReachabilityOuterLeafPackedPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return set, token
	}
	first, firstToken := newSet("first")
	second, _ := newSet("second")
	view, err := UnionStableResourceSets(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.Len(); got != 1 {
		t.Fatalf("union obligations=%d want 1", got)
	}
	guard := view.DeletionGuard()
	first.Release()
	if err := guard.Check(firstToken.Identity(), firstToken.Generation()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Check after representative release=%v want ErrResourcePinned from coalesced alias", err)
	}
	second.Release()
	if err := guard.Check(firstToken.Identity(), firstToken.Generation()); err != nil {
		t.Fatalf("Check after all source releases=%v", err)
	}
}

func duplicatePhysicalStableResourceSets(tb testing.TB, sourceCount int) []*StableResourceSet {
	tb.Helper()
	dir := tb.TempDir()
	file := writeStableResourceFixture(tb, dir, "shared.vlog", "shared-benchmark-resource")
	digest := sha256.Sum256([]byte("shared-benchmark-header"))
	sets := make([]*StableResourceSet, sourceCount)
	for i := range sets {
		builder := NewStableResourceSetBuilder()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: fmt.Sprintf("lane-%d", i), ResourceID: fmt.Sprintf("alias-%d", i), Generation: 1,
			DiagnosticPath: "maindb/value_vlog/shared.vlog", File: file,
			Frontier: DurableFrontier{Bytes: 4}, Digest: digest,
			Reachability: ReachabilityValueLogPointer,
		})
		if err != nil {
			tb.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			tb.Fatal(err)
		}
		sets[i], err = builder.Freeze()
		if err != nil {
			tb.Fatal(err)
		}
	}
	tb.Cleanup(func() {
		for _, set := range sets {
			set.Release()
		}
	})
	return sets
}

func TestUnionStableResourceSetsDuplicatePinsHasBoundedAllocationGrowth(t *testing.T) {
	const sourceCount = 1024
	sets := duplicatePhysicalStableResourceSets(t, sourceCount)

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	view, err := UnionStableResourceSets(sets...)
	if err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(view)
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if allocated > 2<<20 {
		t.Fatalf("union of %d duplicate pins allocated %d bytes; want <= %d", sourceCount, allocated, 2<<20)
	}
}

type unsupportedNamespaceAdapter struct{}

func (unsupportedNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (unsupportedNamespaceAdapter) Sync(*os.File) error {
	return ErrNamespacePersistenceUnsupported
}

func (unsupportedNamespaceAdapter) ValidateLink(*os.File, *os.File, string) error { return nil }
func (unsupportedNamespaceAdapter) ValidateIdentity(*os.File, StableIdentity, string) error {
	return nil
}

type countingNamespaceAdapter struct {
	syncs atomic.Uint64
}

func (*countingNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (adapter *countingNamespaceAdapter) Sync(*os.File) error {
	adapter.syncs.Add(1)
	time.Sleep(time.Millisecond)
	return nil
}

func (*countingNamespaceAdapter) ValidateLink(*os.File, *os.File, string) error { return nil }
func (*countingNamespaceAdapter) ValidateIdentity(*os.File, StableIdentity, string) error {
	return nil
}

type exactParentNamespaceAdapter struct {
	syncedIdentity StableIdentity
	links          atomic.Uint64
}

func (*exactParentNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (adapter *exactParentNamespaceAdapter) ValidateLink(parent, resource *os.File, name string) error {
	adapter.links.Add(1)
	return validateStableChildLink(parent, resource, name)
}

func (adapter *exactParentNamespaceAdapter) ValidateIdentity(parent *os.File, identity StableIdentity, name string) error {
	return validateStableChildIdentity(parent, identity, name)
}

func (adapter *exactParentNamespaceAdapter) Sync(parent *os.File) error {
	identity, err := stableIdentityFromFile(parent)
	if err == nil {
		adapter.syncedIdentity = identity
	}
	return err
}

func testStableNamespacePinsExactLinkedParentAcrossRenameRecreate(t *testing.T) {
	root := t.TempDir()
	originalDir := filepath.Join(root, "segments")
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	resource := writeStableResourceFixture(t, originalDir, "000001.vlog", "resource")
	parent, err := os.Open(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &exactParentNamespaceAdapter{}
	namespace, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "segments",
	}, adapter)
	_ = parent.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	if got := adapter.links.Load(); got != 1 {
		t.Fatalf("link validations=%d want 1", got)
	}

	movedDir := filepath.Join(root, "segments-moved")
	if err := os.Rename(originalDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementParent, err := os.Open(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementParent.Close()
	replacementIdentity, err := stableIdentityFromFile(replacementParent)
	if err != nil {
		t.Fatal(err)
	}
	if replacementIdentity == namespace.ParentIdentity() {
		t.Fatal("replacement directory unexpectedly reused captured stable identity")
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	wantIdentity := namespace.ParentIdentity()
	wantIdentity.Generation = 0
	if adapter.syncedIdentity != wantIdentity {
		t.Fatalf("synced identity=%+v want captured parent %+v", adapter.syncedIdentity, wantIdentity)
	}
}

func testStableNamespaceRejectsResourceOutsideExactParent(t *testing.T) {
	root := t.TempDir()
	leftDir, rightDir := filepath.Join(root, "left"), filepath.Join(root, "right")
	if err := os.Mkdir(leftDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(rightDir, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(leftDir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, rightDir, "foreign.vlog", "resource")
	_, err = newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "foreign.vlog", DiagnosticPath: "left",
	}, &exactParentNamespaceAdapter{})
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("newStableNamespaceToken error=%v want ErrResourceConflict", err)
	}
}

func testStableResourceNamespaceRequiresExactLinkedChild(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	linked := writeStableResourceFixture(t, dir, "linked.vlog", "linked-resource")
	other := writeStableResourceFixture(t, dir, "other.vlog", "other-resource")

	unbound, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "linked.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unbound.Release()
	if token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "linked", Generation: 1,
		DiagnosticPath: "segments/linked.vlog", File: linked, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityValueLogPointer, Namespace: unbound,
	}); !errors.Is(err, ErrUnresolvedResource) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("unbound namespace registration=%v want ErrUnresolvedResource", err)
	}

	bound, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: linked, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "linked.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	if token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "other", Generation: 1,
		DiagnosticPath: "segments/other.vlog", File: other, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityValueLogPointer, Namespace: bound,
	}); !errors.Is(err, ErrResourceConflict) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("wrong linked child registration=%v want ErrResourceConflict", err)
	}
}

func testStableResourceNamespaceAcceptsExactLinkedChild(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "linked.vlog", "linked-resource")
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "linked.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "linked", Generation: 1,
		DiagnosticPath: "segments/linked.vlog", File: resource, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityValueLogPointer, Namespace: namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
}

func TestStableResourceDefaultFlushDoesNotFsync(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	var syncs atomic.Uint64
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "pipe", Generation: 1,
		DiagnosticPath: "pipe.vlog", File: writer, Reachability: ReachabilityValueLogPointer,
		StableIdentityOverride: StableIdentity{Platform: "test", ObjectID: [16]byte{1}},
		SyncThrough:            func(*os.File, DurableFrontier) error { syncs.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if err := token.FlushThrough(); err != nil {
		t.Fatalf("default FlushThrough attempted file sync: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("content sync calls=%d want exactly 1", got)
	}
}

func TestStableNamespaceStabilizeIsSingleFlight(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	adapter := &countingNamespaceAdapter{}
	namespace, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "single-flight.vlog", DiagnosticPath: "single-flight.vlog",
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := namespace.Stabilize(); err != nil {
				t.Errorf("Stabilize: %v", err)
			}
		}()
	}
	workers.Wait()
	if got := adapter.syncs.Load(); got != 1 {
		t.Fatalf("namespace sync calls=%d want 1", got)
	}
}

func TestStableNamespaceCreationProofSyncsOnceAcrossRepeatedBind(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child := writeStableResourceFixture(t, dir, "proof.vlog", "proof")
	defer child.Close()
	adapter := &countingNamespaceAdapter{}
	proof, err := newStableNamespaceCreationProof(parent, child, "proof.vlog", adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	if got := adapter.syncs.Load(); got != 1 {
		t.Fatalf("creation proof syncs=%d want 1", got)
	}
	for generation := uint64(1); generation <= 4; generation++ {
		token, err := proof.Bind(parent, generation, "proof.vlog", "display-only")
		if err != nil {
			t.Fatal(err)
		}
		if got := token.syncs.Load(); got != 1 {
			t.Fatalf("bound token generation=%d namespace syncs=%d want 1", generation, got)
		}
		if got := token.syncNanos.Load(); got == 0 {
			t.Fatalf("bound token generation=%d lost namespace sync duration", generation)
		}
		if err := token.Stabilize(); err != nil {
			t.Fatal(err)
		}
		token.Release()
	}
	if got := adapter.syncs.Load(); got != 1 {
		t.Fatalf("repeated binds resynced namespace: syncs=%d", got)
	}
	if token, err := proof.Bind(parent, 0, "proof.vlog", "display-only"); !errors.Is(err, ErrUnresolvedResource) || token != nil {
		if token != nil {
			token.Release()
		}
		t.Fatalf("Bind with zero generation token=%v err=%v want ErrUnresolvedResource", token, err)
	}
	proof.Release()
	if token, err := proof.Bind(parent, 5, "proof.vlog", "display-only"); !errors.Is(err, ErrResourceOwnership) || token != nil {
		if token != nil {
			token.Release()
		}
		t.Fatalf("Bind after release token=%v err=%v want ErrResourceOwnership", token, err)
	}
}

func TestStableNamespaceRegistrationAndReleaseAreSerialized(t *testing.T) {
	for iteration := range 100 {
		dir := t.TempDir()
		resource := writeStableResourceFixture(t, dir, "registered.vlog", "resource")
		parent, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		adapter := &countingNamespaceAdapter{}
		namespace, err := newStableNamespaceToken(StableNamespaceSpec{
			Parent: parent, LinkedResource: resource, ParentGeneration: uint64(iteration + 1), Operation: NamespaceCreate,
			NewName: "registered.vlog", DiagnosticPath: "registered.vlog",
		}, adapter)
		_ = parent.Close()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		result := make(chan struct {
			token *StableResourceToken
			err   error
		}, 1)
		go func() {
			<-start
			token, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "registered", Generation: 1,
				DiagnosticPath: "registered.vlog", File: resource, Frontier: DurableFrontier{Bytes: 1},
				Reachability: ReachabilityValueLogPointer, Namespace: namespace,
			})
			result <- struct {
				token *StableResourceToken
				err   error
			}{token: token, err: err}
		}()
		close(start)
		namespace.Release()
		registration := <-result
		if registration.err == nil {
			if err := namespace.Stabilize(); err != nil {
				t.Fatalf("iteration %d successful registration retained closed namespace: %v", iteration, err)
			}
			registration.token.Release()
		} else if !errors.Is(registration.err, ErrResourceOwnership) {
			t.Fatalf("iteration %d registration error=%v", iteration, registration.err)
		}
	}
}

func TestUnsupportedNamespaceFailsBeforeCandidateVisibility(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "new.vlog", "resource")
	ns, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "new.vlog", DiagnosticPath: "new.vlog",
	}, unsupportedNamespaceAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.Stabilize(); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Stabilize=%v want unsupported", err)
	}
	token := stableTokenFixture(t, dir, "new.vlog", 1, 4, ReachabilityValueLogPointer, "new", func(spec *StableResourceSpec) { spec.Namespace = ns })
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Freeze(); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Freeze=%v want unsupported", err)
	}
	if builder.State() != ResourceOwnerBuilder {
		t.Fatalf("builder owner=%v want builder after rejection", builder.State())
	}
	builder.Abandon()
}

func TestResourceOwnershipSuccessRetryStopAndPoison(t *testing.T) {
	newCandidate := func(t *testing.T, seq uint64, released *atomic.Uint64) *PreparedRootCandidate {
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, "owned.vlog", seq, 4, ReachabilityValueLogPointer, "owned", func(spec *StableResourceSpec) {
			spec.OnRelease = func() { released.Add(1) }
		})
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{
			Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set,
		})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}

	t.Run("success releases exactly once", func(t *testing.T) {
		var released atomic.Uint64
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		candidate := newCandidate(t, 1, &released)
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if got := released.Load(); got != 1 {
			t.Fatalf("release count=%d want 1", got)
		}
		if err := coordinator.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("retry retains", func(t *testing.T) {
		var released atomic.Uint64
		called := make(chan struct{}, 1)
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			called <- struct{}{}
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("retry")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), newCandidate(t, 1, &released)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.WaitThrough(ctx, 1); err == nil {
			t.Fatal("WaitThrough unexpectedly succeeded")
		}
		<-called
		if got := released.Load(); got != 0 {
			t.Fatalf("release count after retry=%d want 0", got)
		}
		if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
			t.Fatalf("Stop=%v want ErrPublicationStopped", err)
		}
		handoff, err := coordinator.TakeRecoveryHandoff()
		if err != nil {
			t.Fatal(err)
		}
		if handoff.Len() != 1 {
			t.Fatalf("handoff len=%d want 1", handoff.Len())
		}
		handoff.Release()
		if got := released.Load(); got != 1 {
			t.Fatalf("release count after handoff=%d want 1", got)
		}
	})

	t.Run("ambiguous poison hands off", func(t *testing.T) {
		var released atomic.Uint64
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("unknown meta")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), newCandidate(t, 1, &released)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.WaitThrough(ctx, 1); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("WaitThrough=%v want recovery required", err)
		}
		if got := released.Load(); got != 0 {
			t.Fatalf("release count before handoff=%d want 0", got)
		}
		handoff, err := coordinator.TakeRecoveryHandoff()
		if err != nil {
			t.Fatal(err)
		}
		handoff.Release()
		if got := released.Load(); got != 1 {
			t.Fatalf("release count after handoff=%d want 1", got)
		}
		_ = coordinator.Stop(context.Background())
	})
}

func TestRecoveryHandoffRejectsLiveAndPublishingCoordinator(t *testing.T) {
	var released atomic.Uint64
	started := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		close(started)
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "live.vlog", 1, 4, ReachabilityValueLogPointer, "live", func(spec *StableResourceSpec) {
		spec.OnRelease = func() { released.Add(1) }
	})
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1), ResourceSet: set})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- coordinator.WaitThrough(context.Background(), 1) }()
	<-started
	if _, err := coordinator.TakeRecoveryHandoff(); !errors.Is(err, ErrRecoveryHandoffUnavailable) {
		t.Fatalf("live TakeRecoveryHandoff=%v want unavailable", err)
	}
	if got := released.Load(); got != 0 {
		t.Fatalf("live handoff released pins=%d", got)
	}
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("Stop=%v", err)
	}
	if err := <-waitDone; !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("WaitThrough=%v", err)
	}
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	handoff.Release()
	if got := released.Load(); got != 1 {
		t.Fatalf("release count=%d want 1", got)
	}
}

func TestStableResourceSetConcurrentReadsAndRelease(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "race.vlog", 1, 4, ReachabilityValueLogPointer, "race")
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	identity := token.Identity()
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range 16 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				_ = set.Len()
				_ = set.Tokens()
				_ = set.FrontierFor(identity, 1)
				_ = set.DeletionGuard()
				_ = set.Stats(time.Now())
				_, _ = UnionStableResourceSets(set)
			}
		}()
	}
	close(start)
	set.Release()
	readers.Wait()
}

func TestCoordinatorResourcePinHighWaterSurvivesRiseAndRelease(t *testing.T) {
	releasePublish := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		<-releasePublish
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, fmt.Sprintf("hwm-%d.vlog", seq), seq, 4, ReachabilityValueLogPointer, fmt.Sprint(seq))
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	stats := coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 3 || stats[0].PinHighWater != 3 {
		t.Fatalf("peak resource stats=%+v", stats)
	}
	close(releasePublish)
	if err := coordinator.WaitThrough(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	stats = coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 0 || stats[0].PinHighWater != 3 {
		t.Fatalf("released resource stats=%+v", stats)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorResourceActivePinsTrackPublishedPrefix(t *testing.T) {
	started := make(chan uint64, 2)
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		seq := candidate.Frontier().CommitSeq()
		started <- seq
		if seq == 1 {
			<-releaseFirst
		} else {
			<-releaseSecond
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	makeCandidate := func(seq uint64) *PreparedRootCandidate {
		t.Helper()
		token := stableTokenFixture(t, t.TempDir(), fmt.Sprintf("prefix-%d.vlog", seq), seq, 4, ReachabilityValueLogPointer, fmt.Sprint(seq))
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}

	if err := coordinator.Enqueue(context.Background(), makeCandidate(1)); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- coordinator.WaitThrough(context.Background(), 1) }()
	if seq := <-started; seq != 1 {
		t.Fatalf("first published sequence=%d want 1", seq)
	}
	if err := coordinator.Enqueue(context.Background(), makeCandidate(2)); err != nil {
		t.Fatal(err)
	}
	stats := coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 2 || stats[0].PinHighWater != 2 {
		t.Fatalf("in-flight prefix resource stats=%+v", stats)
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	stats = coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 1 || stats[0].PinHighWater != 2 {
		t.Fatalf("remaining suffix resource stats=%+v", stats)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- coordinator.WaitThrough(context.Background(), 2) }()
	if seq := <-started; seq != 2 {
		t.Fatalf("second published sequence=%d want 2", seq)
	}
	close(releaseSecond)
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	stats = coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 0 || stats[0].PinHighWater != 2 {
		t.Fatalf("released resource stats=%+v", stats)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorPinMetricsCountDuplicateDescriptorsForCoalescedIdentity(t *testing.T) {
	releasePublish := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		<-releasePublish
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for seq := uint64(1); seq <= 2; seq++ {
		token := stableTokenFixture(t, dir, fmt.Sprintf("duplicate-%d.vlog", seq), 1, 4, ReachabilityValueLogPointer, "same", func(spec *StableResourceSpec) {
			spec.ResourceID = "same-segment"
			spec.StableIdentityOverride = StableIdentity{Platform: "test", ObjectID: [16]byte{9}, Generation: 1}
		})
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	stats := coordinator.Stats()
	if stats.ResourceCoalesces != 1 || len(stats.Resources) != 1 ||
		stats.Resources[0].PendingCount != 1 || stats.Resources[0].ActivePins != 2 || stats.Resources[0].PinHighWater != 2 {
		t.Fatalf("coalesced duplicate descriptor stats=%+v", stats)
	}
	close(releasePublish)
	if err := coordinator.WaitThrough(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	stats = coordinator.Stats()
	if len(stats.Resources) != 1 || stats.Resources[0].ActivePins != 0 || stats.Resources[0].PinHighWater != 2 {
		t.Fatalf("released duplicate descriptor stats=%+v", stats.Resources)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConflictingCandidateRejectedBeforeVisibleFrontier(t *testing.T) {
	dir := t.TempDir()
	makeCandidate := func(seq uint64, digest string) *PreparedRootCandidate {
		token := stableTokenFixture(t, dir, "candidate-"+digest, 1, 4, ReachabilityColumnManifest, digest, func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.LogicalLane = "columns"
			spec.ResourceID = "manifest-generation-1"
			spec.StableIdentityOverride = StableIdentity{Platform: "test", ObjectID: [16]byte{7}, Generation: 1}
		})
		builder := NewStableResourceSetBuilder(ReachabilityColumnManifest)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}
	coordinator, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	first := makeCandidate(1, "a")
	if err := coordinator.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := makeCandidate(2, "b")
	if err := coordinator.Enqueue(context.Background(), second); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("second Enqueue=%v want ErrResourceConflict", err)
	}
	stats := coordinator.Stats()
	if stats.VisibleCommitSeq != 1 || stats.ResourceConflicts != 1 || stats.RejectedCandidates != 1 {
		t.Fatalf("stats after rejection=%+v", stats)
	}
	second.AbandonResources()
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("Stop=%v", err)
	}
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	handoff.Release()
}

func testStableResourceMetricsSeparateFileAndNamespaceOperations(t *testing.T) {
	dir := t.TempDir()
	resource := writeStableResourceFixture(t, dir, "metrics.vlog", "original-resource-bytes")
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "metrics.vlog", DiagnosticPath: "metrics.vlog",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	var flushes, syncs atomic.Uint64
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "metrics", Generation: 1,
		DiagnosticPath: "metrics.vlog", File: resource, Frontier: DurableFrontier{Bytes: 4},
		Digest: sha256.Sum256([]byte("metrics")), Reachability: ReachabilityValueLogPointer, Namespace: namespace,
		FlushThrough: func(*os.File, DurableFrontier) error { flushes.Add(1); return nil },
		SyncThrough:  func(*os.File, DurableFrontier) error { syncs.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := token.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].Flushes != 1 || stats[0].Syncs != 1 || stats[0].PhysicalFileSyncs != 1 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("resource stats=%+v", stats)
	}
	if flushes.Load() != 1 || syncs.Load() != 1 {
		t.Fatalf("callbacks flush=%d sync=%d", flushes.Load(), syncs.Load())
	}
	set.Release()
}

func TestStableResourceMetricsDistinguishCoveredSyncAttemptFromPhysicalFileSync(t *testing.T) {
	resource := writeStableResourceFixture(t, t.TempDir(), "covered.vlog", "covered-resource-bytes")
	var callbacks atomic.Uint64
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "covered", Generation: 1,
		DiagnosticPath: "covered.vlog", File: resource, Frontier: DurableFrontier{Bytes: 4},
		Digest: sha256.Sum256([]byte("covered")), Reachability: ReachabilityValueLogPointer,
		ContentSynced: true,
		SyncThrough: func(*os.File, DurableFrontier) error {
			callbacks.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		set.Release()
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].Syncs != 1 || stats[0].PhysicalFileSyncs != 1 || callbacks.Load() != 0 {
		set.Release()
		t.Fatalf("covered frontier stats=%+v callbacks=%d", stats, callbacks.Load())
	}
	set.Release()
}

func BenchmarkStableResourceTokenConstruction(b *testing.B) {
	dir := b.TempDir()
	file := writeStableResourceFixture(b, dir, "bench.vlog", "benchmark-resource")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "bench", Generation: uint64(i + 1),
			DiagnosticPath: "bench.vlog", File: file, Frontier: DurableFrontier{Bytes: 4}, Reachability: ReachabilityValueLogPointer,
		})
		if err != nil {
			b.Fatal(err)
		}
		token.Release()
	}
}

func BenchmarkStableResourceSetCoalesce(b *testing.B) {
	dir := b.TempDir()
	sets := make([]*StableResourceSet, 8)
	for i := range sets {
		builder := NewStableResourceSetBuilder()
		token := stableTokenFixture(b, dir, "coalesce-"+string(rune('a'+i)), uint64(i+1), 4, ReachabilityValueLogPointer, "bench")
		if err := builder.Add(token); err != nil {
			b.Fatal(err)
		}
		var err error
		sets[i], err = builder.Freeze()
		if err != nil {
			b.Fatal(err)
		}
		defer sets[i].Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnionStableResourceSets(sets...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStableResourceSetCoalesceDuplicatePhysical(b *testing.B) {
	dir := b.TempDir()
	file := writeStableResourceFixture(b, dir, "coalesce-shared.vlog", "shared-benchmark-resource")
	sets := make([]*StableResourceSet, 8)
	for i := range sets {
		builder := NewStableResourceSetBuilder()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: fmt.Sprintf("lane-%d", i), ResourceID: fmt.Sprintf("alias-%d", i), Generation: 1,
			DiagnosticPath: "maindb/value_vlog/coalesce-shared.vlog", File: file,
			Frontier: DurableFrontier{Bytes: 4}, Digest: sha256.Sum256([]byte("shared-benchmark-header")),
			Reachability: ReachabilityValueLogPointer,
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			b.Fatal(err)
		}
		sets[i], err = builder.Freeze()
		if err != nil {
			b.Fatal(err)
		}
		defer sets[i].Release()
	}
	view, err := UnionStableResourceSets(sets...)
	if err != nil {
		b.Fatal(err)
	}
	if got := stableSetPhysicalCount(sets); got != len(sets) {
		b.Fatalf("source-owned descriptor pins=%d want %d", got, len(sets))
	}
	if got := view.Len(); got != 1 {
		b.Fatalf("coalesced durability obligations=%d want 1", got)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := UnionStableResourceSets(sets...)
		if err != nil {
			b.Fatal(err)
		}
		if got := view.Len(); got != 1 {
			b.Fatalf("coalesced durability obligations=%d want 1", got)
		}
	}
	b.StopTimer()
	if got := stableSetPhysicalCount(sets); got != len(sets) {
		b.Fatalf("source-owned descriptor pins after union=%d want %d", got, len(sets))
	}
	b.ReportMetric(float64(len(sets)), "source-owned-descriptor-pins/op")
	b.ReportMetric(1, "coalesced-durability-obligations/op")
	b.ReportMetric(float64(len(sets)-1), "durability-obligation-coalesces/op")
}

func BenchmarkStableResourceSetUnionDuplicatePhysicalScale(b *testing.B) {
	for _, sourceCount := range []int{8, 64, 256, 1024} {
		b.Run(fmt.Sprintf("sources=%d", sourceCount), func(b *testing.B) {
			sets := duplicatePhysicalStableResourceSets(b, sourceCount)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				view, err := UnionStableResourceSets(sets...)
				if err != nil {
					b.Fatal(err)
				}
				if got := view.Len(); got != 1 {
					b.Fatalf("coalesced durability obligations=%d want 1", got)
				}
			}
		})
	}
}

func TestStableResourceTokenPinnedReadRemainsUsable(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "read.vlog", 1, 4, ReachabilityValueLogPointer, "read")
	defer token.Release()
	buf := make([]byte, 4)
	if _, err := token.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
}

func TestStableResourcePublicationDebtCoverage(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "debt.vlog", "12345678901234567890123456789012")
	frontier := NewRIDFrontier([]uint64{2, 8})
	frontier.Bytes, frontier.MaxLSN = 16, 5
	baseSpec := StableResourceSpec{Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "debt", Generation: 1,
		DiagnosticPath: "debt.vlog", File: file, Frontier: frontier,
		Digest: sha256.Sum256([]byte("header")), Reachability: ReachabilityValueLogPointer, ContentSynced: true}
	makeSet := func(specs ...StableResourceSpec) *StableResourceSet {
		builder := NewStableResourceSetBuilder()
		defer builder.Abandon()
		for _, spec := range specs {
			token, err := NewStableResourceToken(spec)
			if err != nil {
				t.Fatal(err)
			}
			if err := builder.Add(token); err != nil {
				token.Release()
				t.Fatal(err)
			}
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(set.Release)
		return set
	}
	published := makeSet(baseSpec)
	tests := []struct {
		name   string
		change func(*StableResourceSpec)
		extra  bool
		want   uint64
	}{
		{name: "covered"},
		{name: "covered RID subset", change: func(s *StableResourceSpec) {
			s.Frontier = NewRIDFrontier([]uint64{8})
			s.Frontier.Bytes, s.Frontier.MaxLSN = 12, 4
		}},
		{name: "advanced bytes", change: func(s *StableResourceSpec) { s.Frontier.Bytes = 20 }, want: 20},
		{name: "advanced LSN", change: func(s *StableResourceSpec) { s.Frontier.MaxLSN++ }, want: 16},
		{name: "same maximum different RID", change: func(s *StableResourceSpec) {
			s.Frontier = NewRIDFrontier([]uint64{3, 8})
			s.Frontier.Bytes, s.Frontier.MaxLSN = 16, 5
		}, want: 16},
		{name: "different generation", change: func(s *StableResourceSpec) { s.Generation++ }, want: 16},
		{name: "different digest", change: func(s *StableResourceSpec) { s.Digest[0]++ }, want: 16},
		{name: "logical alias", change: func(s *StableResourceSpec) { s.ResourceID = "alias" }, want: 16},
		{name: "different identity same path", change: func(s *StableResourceSpec) {
			s.StableIdentityOverride = published.Tokens()[0].Identity()
			s.StableIdentityOverride.ObjectID[0] ^= 1
		}, want: 16},
		{name: "cross kind", change: func(s *StableResourceSpec) {
			s.Kind, s.Reachability = ResourceOuterLeafLog, ReachabilityOuterLeafRawPointer
		}, want: 16},
		{name: "advanced coalesced entry", change: func(s *StableResourceSpec) { s.Frontier.Bytes = 20 }, extra: true, want: 20},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := baseSpec
			if tc.change != nil {
				tc.change(&spec)
			}
			specs := []StableResourceSpec{spec}
			if tc.extra {
				specs = append([]StableResourceSpec{baseSpec}, specs...)
			}
			candidate := makeSet(specs...)
			before := candidate.Descriptors()
			got, err := candidate.BytesNotCoveredBy(published)
			if err != nil || got != tc.want {
				t.Fatalf("debt=%d err=%v want=%d", got, err, tc.want)
			}
			if !reflect.DeepEqual(before, candidate.Descriptors()) {
				t.Fatal("accounting mutated the full closure")
			}
			if got, err := candidate.BytesNotCoveredBy(nil); err != nil || got != before[0].Frontier().Bytes {
				t.Fatalf("unpublished pre-synced debt=%d err=%v", got, err)
			}
		})
	}
	// Alias and logical coverage is directional, independently of byte coverage.
	prior := &stableResourceEntry{token: &StableResourceToken{kind: ResourceColumnAsset, generation: 1, stability: ResourceMutableAppend}, frontier: DurableFrontier{Bytes: 16}, reachability: map[ReachabilityField]struct{}{ReachabilityColumnManifest: {}}}
	candidate := *prior
	candidate.token = &StableResourceToken{kind: ResourceColumnAsset, generation: 1, stability: ResourceMutableAppend}
	candidate.token.namespace = &StableNamespaceToken{operation: NamespaceCreate, newName: "new", hasLinkedResource: true}
	if stableResourceEntryCoversPublication(prior, &candidate) {
		t.Fatal("new namespace credited by namespace-free baseline")
	}
	prior.token.namespace = &StableNamespaceToken{operation: NamespaceCreate, newName: "new", hasLinkedResource: true}
	if !stableResourceEntryCoversPublication(prior, &candidate) {
		t.Fatal("equal namespace not covered")
	}
	for _, change := range []func(*StableNamespaceToken){
		func(n *StableNamespaceToken) { n.operation = NamespaceRename },
		func(n *StableNamespaceToken) { n.oldName = "old" },
		func(n *StableNamespaceToken) { n.newName = "other" },
		func(n *StableNamespaceToken) { n.parentIdentity.Generation++ },
		func(n *StableNamespaceToken) { n.linkedResourceIdentity.ObjectID[0]++ },
	} {
		candidate.token.namespace = &StableNamespaceToken{operation: NamespaceCreate, newName: "new", hasLinkedResource: true}
		change(candidate.token.namespace)
		if stableResourceEntryCoversPublication(prior, &candidate) {
			t.Fatal("different namespace credited")
		}
	}
	candidate.token.namespace = nil
	candidate.reachability = map[ReachabilityField]struct{}{ReachabilityTypedColumnValue: {}}
	if stableResourceEntryCoversPublication(prior, &candidate) {
		t.Fatal("new reachability credited")
	}
	candidate.reachability = prior.reachability
	obligation := StableLogicalObligation{Class: "asset", Kind: "rows", Namespace: "new", Generation: 1, PartID: 1, FileID: 1, Length: 1, Reachability: ReachabilityColumnManifest}
	candidate.logicalObligations = newStableLogicalObligationView([]StableLogicalObligation{obligation})
	if stableResourceEntryCoversPublication(prior, &candidate) {
		t.Fatal("new logical obligation credited")
	}
	prior.logicalObligations = newStableLogicalObligationView([]StableLogicalObligation{obligation})
	if !stableResourceEntryCoversPublication(prior, &candidate) {
		t.Fatal("equal logical obligation not covered")
	}
	obligation.Checksum++
	candidate.logicalObligations = newStableLogicalObligationView([]StableLogicalObligation{obligation})
	if stableResourceEntryCoversPublication(prior, &candidate) {
		t.Fatal("different logical checksum credited")
	}
}

func TestStableResourcePublicationDebtRetainsIndexedOwnership(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "debt-index.bin", "x")
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	for id := uint64(1); id <= 128; id++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil {
			t.Fatal(err)
		}
	}
	published, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer published.Release()
	candidate, err := CloneStableResourceSetExcludingKinds(published)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	allocs := testing.AllocsPerRun(10, func() {
		got, err := candidate.BytesNotCoveredBy(published)
		if err != nil || got != 0 {
			t.Fatalf("debt=%d err=%v", got, err)
		}
	})
	t.Logf("128 retained entries: %.0f allocations/accounting call", allocs)
	// Only a wrapper and small per-kind maps are needed; no per-entry copies.
	if allocs > 32 {
		t.Fatalf("accounting allocations=%g exceed fixed-container allowance 32", allocs)
	}
	// Exercise exact sparse membership too: empty frontiers do not detect
	// per-entry RID copies or repeated digest reconstruction during accounting.
	rids := make([]uint64, 512)
	for i := range rids {
		rids[i] = uint64(i+1) * 2
	}
	makeRIDSet := func(t *testing.T, members []uint64) *StableResourceSet {
		t.Helper()
		frontier := NewRIDFrontier(members)
		frontier.Bytes = 1
		builder := NewStableResourceSetBuilder()
		defer builder.Abandon()
		for id := uint64(1); id <= 128; id++ {
			var objectID [16]byte
			binary.LittleEndian.PutUint64(objectID[:], id)
			token, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceValueLog, LogicalLane: "main", ResourceID: fmt.Sprint(id), Generation: 1,
				DiagnosticPath: "debt-index.bin", File: file, Frontier: frontier,
				Reachability: ReachabilityValueLogPointer, ContentSynced: true,
				StableIdentityOverride: StableIdentity{Platform: "scale-test", ObjectID: objectID},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := builder.Add(token); err != nil {
				token.Release()
				t.Fatal(err)
			}
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(set.Release)
		return set
	}
	ridPublished := makeRIDSet(t, rids)
	different := slices.Clone(rids)
	different[1]++ // Same count/min/max, different exact membership.
	for _, tc := range []struct {
		name    string
		members []uint64
		want    uint64
	}{
		{"equal", rids, 0},
		{"subset", rids[256:], 0},
		{"different", different, 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ridCandidate := makeRIDSet(t, tc.members)
			allocs := testing.AllocsPerRun(10, func() {
				got, err := ridCandidate.BytesNotCoveredBy(ridPublished)
				if err != nil || got != tc.want {
					t.Fatalf("debt=%d err=%v want=%d", got, err, tc.want)
				}
			})
			t.Logf("128 entries with %d/%d required/durable RIDs: %.0f allocations/accounting call", len(tc.members), len(rids), allocs)
			if allocs > 32 {
				t.Fatalf("RID accounting allocations=%g exceed fixed-container allowance 32", allocs)
			}
		})
	}
	ridConcurrent, err := CloneStableResourceSetExcludingKinds(ridPublished)
	if err != nil {
		t.Fatal(err)
	}
	defer ridConcurrent.Release()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(reverse bool) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				left, right := ridConcurrent, ridPublished
				if reverse {
					left, right = right, left
				}
				if got, err := left.BytesNotCoveredBy(right); err != nil || got != 0 {
					t.Errorf("concurrent debt=%d err=%v", got, err)
					return
				}
			}
		}(i == 1)
	}
	wg.Wait()
	published.Release()
	if _, err := candidate.BytesNotCoveredBy(published); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released baseline error=%v", err)
	}
	if got, err := candidate.BytesNotCoveredBy(nil); err != nil || got != 128 {
		t.Fatalf("independent owner debt=%d err=%v", got, err)
	}
	candidate.Release()
	if _, err := candidate.BytesNotCoveredBy(nil); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released candidate error=%v", err)
	}
}

// Original one-input reconciliation, retained as an exact representation oracle.
func originalStableResourceUnionForTest(mode stableResourceViewMode, sets ...*StableResourceSet) (*StableResourceSet, error) {
	view := &StableResourceSet{}
	view.owner.Store(uint32(ResourceOwnerView))
	lookup := stableResourceEntryLookup{}
	var evidenceCandidates map[ResourceKind][]stableLogicalMembershipEvidence
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		if mode != stableResourceViewValidatedCount {
			evidenceCandidates = appendStableLogicalMembershipEvidenceCandidates(evidenceCandidates, set)
		}
		var mergeErr error
		set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			var err error
			if lookup.logical == nil && len(view.entries) < stableResourceEntryLinearLookupLimit {
				err = mergeViewEntryLinear(&view.entries, *entry, mode, nil)
			} else {
				if lookup.logical == nil {
					lookup = newStableResourceEntryLookup(view.entries)
				}
				err = mergeViewEntry(&view.entries, &lookup, *entry, mode, nil)
			}
			if err != nil {
				mergeErr = err
				return false
			}
			return true
		})
		set.mu.Unlock()
		if mergeErr != nil {
			return nil, mergeErr
		}
	}
	if mode == stableResourceViewValidatedCount {
		return view, nil
	}
	sortStableResourceEntries(view.entries)
	if len(evidenceCandidates) != 0 {
		view.logicalMembershipEvidence = stableUnionLogicalMembershipEvidence(view.entries, evidenceCandidates)
	}
	view.pinHighWater = stableResourcePinCounts(view.entries)
	return view, nil
}

func assertSingleStableResourceUnionParity(t *testing.T, source *StableResourceSet) (*StableResourceSet, *StableResourceSet) {
	t.Helper()
	// Shared caches are warmed once before comparing encoding work on both views.
	if source != nil {
		if _, _, err := source.DependencyManifestV1(); err != nil {
			t.Fatal(err)
		}
	}
	got, err := UnionStableResourceSets(nil, source, nil)
	want, wantErr := originalStableResourceUnionForTest(stableResourceViewPinned, source)
	if err != nil || wantErr != nil {
		t.Fatalf("union errors: %v / %v", err, wantErr)
	}
	assertStableResourceViewParity(t, got, want)
	count, err := validatedStableResourceUnionCount(nil, source, nil)
	countView, countErr := originalStableResourceUnionForTest(stableResourceViewValidatedCount, source)
	if err != nil || countErr != nil || count != len(countView.entries) {
		t.Fatalf("count=%d/%d errors=%v/%v", count, len(countView.entries), err, countErr)
	}
	return got, want
}

func assertStableResourceViewParity(t *testing.T, got, want *StableResourceSet) {
	t.Helper()
	if got.Owner() != ResourceOwnerView || got.kindViews != nil || !reflect.DeepEqual(got.entries, want.entries) || !reflect.DeepEqual(got.pinHighWater, want.pinHighWater) || !reflect.DeepEqual(got.logicalMembershipEvidence, want.logicalMembershipEvidence) {
		t.Fatal("single union changed flat entry, pin or evidence representation")
	}
	for i := range got.entries {
		if got.entries[i].token != want.entries[i].token || got.entries[i].dependencyManifestV1 != want.entries[i].dependencyManifestV1 {
			t.Fatal("representative or cached encoding changed")
		}
	}
	now := time.Unix(123, 0)
	if !reflect.DeepEqual(got.Descriptors(), want.Descriptors()) || !reflect.DeepEqual(got.Stats(now), want.Stats(now)) {
		t.Fatal("descriptor/stat parity")
	}
	gm, gw, ge := got.DependencyManifestV1()
	wm, ww, we := want.DependencyManifestV1()
	if ge != nil || we != nil || gw != ww || gm.digest != wm.digest || !bytes.Equal(dependencyManifestPayloadForTest(t, gm), dependencyManifestPayloadForTest(t, wm)) {
		t.Fatalf("manifest parity: %v/%v work=%+v/%+v", ge, we, gw, ww)
	}
	for _, entry := range got.entries {
		if ge, we := got.DeletionGuard().Check(entry.token.identity, entry.token.generation), want.DeletionGuard().Check(entry.token.identity, entry.token.generation); !errors.Is(ge, we) {
			t.Fatalf("guard parity: %v/%v", ge, we)
		}
	}
	gc, ge := CloneStableResourceSetExcludingKinds(got)
	wc, we := CloneStableResourceSetExcludingKinds(want)
	if !errors.Is(ge, we) {
		t.Fatalf("clone parity: %v/%v", ge, we)
	}
	if gc != nil {
		gc.Release()
	}
	if wc != nil {
		wc.Release()
	}
}

func TestSingleStableResourceUnionSnapshotParity(t *testing.T) {
	for _, action := range []string{"release", "merge", "transfer"} {
		t.Run(action, func(t *testing.T) {
			file := writeStableResourceFixture(t, t.TempDir(), "single.pack", "x")
			builder := NewStableResourceSetBuilder()
			for id := uint64(1); id <= 32; id++ {
				if err := builder.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil {
					t.Fatal(err)
				}
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer source.Release()
			if _, _, err := source.DependencyManifestV1(); err != nil {
				t.Fatal(err)
			}
			got, want := assertSingleStableResourceUnionParity(t, source)
			assertSingleStableResourceUnionParity(t, got) // canonical flat union chaining
			before := got.Descriptors()
			got.Release()
			got.Release()
			switch action {
			case "release":
				source.Release()
			case "merge":
				next := NewStableResourceSetBuilder()
				if err := next.Merge(source); err != nil {
					t.Fatal(err)
				}
				if err := next.Add(distinctPhysicalTokenFixture(t, file, 33)); err != nil {
					t.Fatal(err)
				}
				merged, err := next.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				defer merged.Release()
				assertSingleStableResourceUnionParity(t, merged)
			case "transfer":
				if err := source.transfer(ResourceOwnerBuilder, ResourceOwnerRecovery); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(before, got.Descriptors()) {
				t.Fatal("source mutation changed captured metadata")
			}
			assertStableResourceViewParity(t, got, want)
			assertSingleStableResourceUnionParity(t, source)
		})
	}
	sets := duplicatePhysicalStableResourceSets(t, 3)
	flat, err := UnionStableResourceSets(sets...)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := flat.DependencyManifestV1(); err != nil {
		t.Fatal(err)
	}
	assertSingleStableResourceUnionParity(t, flat) // all coalesced physical pins
	// Noncanonical internal flat owners retain original reconciliation.
	fallback := &StableResourceSet{entries: append(cloneStableResourceEntries(flat.entries), cloneStableResourceEntry(flat.entries[0]))}
	fallback.owner.Store(uint32(ResourceOwnerBuilder))
	got, err := UnionStableResourceSets(fallback)
	want, wantErr := originalStableResourceUnionForTest(stableResourceViewPinned, fallback)
	if err != nil || wantErr != nil || got.Len() != 1 || !reflect.DeepEqual(got.entries, want.entries) {
		t.Fatalf("flat fallback: %v/%v", err, wantErr)
	}
	assertSingleStableResourceUnionParity(t, nil)
}
