package collections

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/authorityinventory"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func testStableColumnConstructionPinBlocksCrossManagerGC(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	// Stabilize the seed publication before taking the registry baseline. The
	// construction-pin assertion is about the candidate append below, not pins
	// transferred asynchronously while the seed root becomes durable.
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	candidatePayload := []byte("construction-authority-candidate")
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 117, candidatePayload)

	otherManager := NewCollectionManager(d)
	otherCol, err := otherManager.OpenCollection(col.Meta().Name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := *otherCol.Meta().Options.ColumnStore
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	opened := make(chan struct{})
	resume := make(chan struct{})
	restoreHook := setColumnAssetStableConstructionTestHook(func(fileID uint32) {
		if fileID != candidate.FileID {
			return
		}
		close(opened)
		<-resume
	})
	defer restoreHook()
	type appendResult struct {
		resources *rootpublication.StableResourceSet
		err       error
	}
	done := make(chan appendResult, 1)
	go func() {
		session := newColumnPhysicalAssetAppendSessionWithStableResources(d.ColumnAssetRootDir(), cfg, registry)
		_, err := session.appendKinds(candidate.FileID, []columnPhysicalAssetAppendItem{{
			payload: []byte("later-unpublished-bytes"), kind: ColumnAssetKindTCS1PartImage, generation: 9, partID: 9,
		}})
		if err != nil {
			_ = session.abort()
			done <- appendResult{err: err}
			return
		}
		_, resources, err := session.closeWithStableResources()
		done <- appendResult{resources: resources, err: err}
	}()
	<-opened
	if got := registry.ActivePins(); got != baselinePins+1 {
		t.Fatalf("construction pins=%d want baseline+1=%d", got, baselinePins+1)
	}
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		close(resume)
		t.Fatal(err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		close(resume)
		t.Fatalf("construction-race GC stats=%+v want eligible retained", stats)
	}
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		close(resume)
		t.Fatal(err)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		close(resume)
		t.Fatalf("construction-race candidate removed: %v", err)
	}
	close(resume)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.resources == nil {
		t.Fatal("construction-race append returned no stable resources")
	}
	result.resources.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("pins after final release=%d want baseline=%d", got, baselinePins)
	}
}

func testStableColumnConstructionRejectsUnlinkBeforeObserve(t *testing.T) {
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "construction-pre-observe-unlink",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	unlinked := false
	restoreHook := setColumnAssetStableBeforeObserveTestHook(func(parent, _ *os.File, name string) {
		if err := rootpublication.RemoveStableChildFile(parent, name); err != nil {
			t.Fatal(err)
		}
		unlinked = true
	})
	defer restoreHook()
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
	refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
		payload: []byte("must-not-write-unlinked-inode"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		_ = session.abort()
		t.Fatalf("pre-observe unlink append error=%v want ErrResourceConflict", err)
	}
	if !unlinked || len(refs) != 0 {
		t.Fatalf("pre-observe unlink=%t refs=%+v want unlinked/no refs", unlinked, refs)
	}
	if err := session.abort(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("pre-observe unlink pins=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("pre-observe unlink identities=%d want 0", got)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(root, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(columnAssetM12ASegmentFileID))
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-observe unlinked segment visible: %v", err)
	}
}

func testColumnAssetGCRejectsParentDirectoryRebindFromPlan(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	originalPayload := []byte("planned-parent-original")
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 118, originalPayload)
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), candidate.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(candidate.FileID))
	rotatedDir := namespace.SegmentDir + ".planned-original"
	replacementPayload := []byte("same-size-replacement!!")
	if len(replacementPayload) != len(originalPayload) {
		t.Fatalf("test payload sizes replacement=%d original=%d", len(replacementPayload), len(originalPayload))
	}
	restoreHook := setColumnAssetStableDeleteAfterPlanTestHook(func() {
		if err := os.Rename(namespace.SegmentDir, rotatedDir); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(namespace.SegmentDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(segmentPath, replacementPayload, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	defer restoreHook()
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetGCPlanStale) {
		t.Fatalf("parent-rebind GC error=%v want ErrColumnAssetGCPlanStale", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("parent-rebind GC stats=%+v want eligible untouched", stats)
	}
	gotReplacement, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotReplacement) != string(replacementPayload) {
		t.Fatalf("replacement=%q want %q", gotReplacement, replacementPayload)
	}
	gotOriginal, err := os.ReadFile(filepath.Join(rotatedDir, columnAssetSegmentFileName(candidate.FileID)))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotOriginal) != string(originalPayload) {
		t.Fatalf("rotated original=%q want %q", gotOriginal, originalPayload)
	}
}

func testColumnAssetGCRejectsChildRebindFromPlan(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	originalPayload := []byte("planned-child-original")
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 120, originalPayload)
	segmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPath := segmentPath + ".planned-original"
	replacementPayload := []byte(strings.Repeat("R", len(originalPayload)))
	restoreHook := setColumnAssetStableDeleteAfterPlanTestHook(func() {
		if err := os.Rename(segmentPath, rotatedPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(segmentPath, replacementPayload, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	defer restoreHook()
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetGCPlanStale) {
		t.Fatalf("child-rebind GC error=%v want ErrColumnAssetGCPlanStale", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("child-rebind GC stats=%+v want eligible untouched", stats)
	}
	gotReplacement, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotReplacement) != string(replacementPayload) {
		t.Fatalf("replacement=%q want %q", gotReplacement, replacementPayload)
	}
	gotOriginal, err := os.ReadFile(rotatedPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotOriginal) != string(originalPayload) {
		t.Fatalf("rotated original=%q want %q", gotOriginal, originalPayload)
	}
}

func testColumnAssetGCRejectsCompletedCrossManagerPublicationAfterPlan(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	managerA := NewCollectionManager(d)
	managerB := NewCollectionManager(d)
	colA := openColumnStoreCollectionM10B(t, d, managerA)
	colB := openColumnStoreCollectionM10B(t, d, managerB)
	configureStableGCDirectViewPublication(t, colA, colB)
	if _, err := colA.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"did:seed"}`)); err != nil {
		t.Fatal(err)
	}

	candidatePayload := []byte("same-inode-candidate-before-publication")
	directFileID, err := directViewTypedColumnSegmentFileID(2)
	if err != nil {
		t.Fatal(err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), colA, directFileID, candidatePayload)
	segmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("published-after-gc-plan")
	document := []byte(`{"time_us":2,"kind":"repost","did":"did:published"}`)
	restoreHook := setColumnAssetStableDeleteAfterPlanTestHook(func() {
		if _, err := colB.Insert(key, document); err != nil {
			t.Fatal(err)
		}
	})
	defer restoreHook()

	stats, err := colA.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetGCPlanStale) {
		t.Fatalf("completed cross-manager publication GC error=%v want ErrColumnAssetGCPlanStale", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("completed cross-manager publication GC stats=%+v want eligible untouched", stats)
	}
	info, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatalf("published segment removed: %v", err)
	}
	if info.Size() <= int64(len(candidatePayload)) {
		t.Fatalf("published segment size=%d want greater than candidate frontier=%d", info.Size(), len(candidatePayload))
	}
	got, err := colB.Get(key)
	if err != nil {
		t.Fatalf("Get published document: %v", err)
	}
	if !bytes.Equal(got, document) {
		t.Fatalf("published document=%s want %s", got, document)
	}
	freshPlan, err := colA.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("fresh reachability plan: %v", err)
	}
	var publishedRef ColumnAssetRef
	for _, entry := range freshPlan.Entries {
		if entry.Ref.FileID != directFileID || entry.Status != ColumnAssetReachabilityProtected {
			continue
		}
		for _, source := range entry.Sources {
			if source == ColumnAssetReachabilitySourceActiveManifest {
				publishedRef = entry.Ref
				break
			}
		}
		if publishedRef.FileID != 0 {
			break
		}
	}
	if publishedRef.FileID == 0 {
		t.Fatalf("fresh plan has no protected active-manifest ref for direct file_id=%d: %+v", directFileID, freshPlan.Entries)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), publishedRef); err != nil {
		t.Fatalf("read checksum-valid published direct-view ref %+v: %v", publishedRef, err)
	} else if len(raw) == 0 {
		t.Fatalf("published direct-view ref %+v read empty payload", publishedRef)
	}
}

func testColumnAssetGCRejectsAppendedCandidateFrontierAfterPlan(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"did:seed"}`)); err != nil {
		t.Fatal(err)
	}
	candidatePayload := []byte("candidate-frontier-before-append")
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 121, candidatePayload)
	segmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	appended := []byte("-appended-after-plan")
	restoreHook := setColumnAssetStableDeleteAfterPlanTestHook(func() {
		file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(appended); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	})
	defer restoreHook()

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetGCPlanStale) {
		t.Fatalf("appended-frontier GC error=%v want ErrColumnAssetGCPlanStale", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("appended-frontier GC stats=%+v want eligible untouched", stats)
	}
	got, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("appended candidate removed: %v", err)
	}
	want := append(append([]byte(nil), candidatePayload...), appended...)
	if !bytes.Equal(got, want) {
		t.Fatalf("appended candidate=%q want %q", got, want)
	}
}

func testColumnAssetGCRejectsCommitAdvanceWithUnchangedCandidateFrontier(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	managerA := NewCollectionManager(d)
	managerB := NewCollectionManager(d)
	colA := openColumnStoreCollectionM10B(t, d, managerA)
	colB := openColumnStoreCollectionM10B(t, d, managerB)
	if _, err := colA.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"did:seed"}`)); err != nil {
		t.Fatal(err)
	}
	candidatePayload := []byte("unchanged-frontier-candidate")
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), colA, 122, candidatePayload)
	segmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("published-to-other-segment")
	document := []byte(`{"time_us":3,"kind":"like","did":"did:other"}`)
	// This hook runs after GC captured and pinned its RecoverableRootSet but
	// before the stable deleter's final revalidation. Advance the publication
	// basis deterministically so the stale capability must fail closed.
	restoreHook := setColumnAssetStableDeleteAfterPlanTestHook(func() {
		if _, err := colB.Insert(key, document); err != nil {
			t.Fatal(err)
		}
	})
	defer restoreHook()

	stats, err := colA.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetGCPlanStale) {
		t.Fatalf("commit-advance GC error=%v want ErrColumnAssetGCPlanStale", err)
	}
	if !errors.Is(err, backenddb.ErrRecoverableRootSetStale) {
		t.Fatalf("commit-advance GC error=%v want ErrRecoverableRootSetStale", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("commit-advance GC stats=%+v want eligible untouched", stats)
	}
	gotCandidate, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("unchanged-frontier candidate removed: %v", err)
	}
	if !bytes.Equal(gotCandidate, candidatePayload) {
		t.Fatalf("unchanged-frontier candidate=%q want %q", gotCandidate, candidatePayload)
	}
	gotDocument, err := colB.Get(key)
	if err != nil {
		t.Fatalf("Get cross-manager publication: %v", err)
	}
	if !bytes.Equal(gotDocument, document) {
		t.Fatalf("cross-manager publication=%s want %s", gotDocument, document)
	}
}

func testColumnPublishStableAbandonPreservesSameSizeReboundSegment(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	injected := errors.New("injected ordinary publication failure after asset preparation")
	var segmentPath, rotatedPath string
	var replacement []byte
	restoreHook := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(prepared ColumnPublishPreparedAssets) error {
		if !prepared.stableResourcesRequired || prepared.stableResources == nil {
			t.Fatalf("ordinary publication stable authority required=%t resources=%v", prepared.stableResourcesRequired, prepared.stableResources)
		}
		if len(prepared.Assets) == 0 {
			t.Fatal("ordinary publication prepared no assets")
		}
		var err error
		segmentPath, err = columnAssetSegmentPath(d.ColumnAssetRootDir(), prepared.Assets[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(segmentPath)
		if err != nil {
			t.Fatal(err)
		}
		rotatedPath = segmentPath + ".ordinary-stable-abandon-original"
		if err := os.Rename(segmentPath, rotatedPath); err != nil {
			t.Fatal(err)
		}
		replacement = bytes.Repeat([]byte{'R'}, int(info.Size()))
		if err := os.WriteFile(segmentPath, replacement, 0o600); err != nil {
			t.Fatal(err)
		}
		return injected
	})
	defer restoreHook()

	if _, err := col.Insert([]byte("ordinary-rebound"), []byte(`{"time_us":4,"kind":"like","did":"did:rebound"}`)); !errors.Is(err, injected) {
		t.Fatalf("ordinary Insert error=%v want injected failure", err)
	}
	got, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("ordinary same-size replacement mutated: bytes=%d want %d", len(got), len(replacement))
	}
	if info, err := os.Stat(rotatedPath); err != nil || info.Size() == 0 {
		t.Fatalf("ordinary retained original stat=%v info=%v", err, info)
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("ordinary abandoned publication pins=%d want baseline=%d", got, baselinePins)
	}
}

func configureStableGCDirectViewPublication(t *testing.T, collections ...*Collection) {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	for i := range cfg.Columns {
		if cfg.Columns[i].Name == "time_us" {
			cfg.Columns[i].Owner = TypedStorageOwnerColumnPart
			cfg.Columns[i].FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
		}
	}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !columnStoreConfigNeedsDirectViewTypedColumnAlignment(*normalized) {
		t.Fatal("configured collection does not require direct-view typed-column alignment")
	}
	for _, col := range collections {
		if col == nil || col.catalog == nil {
			t.Fatal("missing collection catalog")
		}
		copied := normalized.copy()
		col.catalog.meta.Options.ColumnStore = &copied
	}
}

func testStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "captured_parent",
		},
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	var fileSyncs atomic.Uint64
	originalFileSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
		if file == nil {
			t.Fatal("nil column segment sync handle")
		}
		fileSyncs.Add(1)
		return nil
	}
	defer func() { syncColumnAssetSegmentFileForPublish = originalFileSync }()
	var movedAnchor, replacementAnchor string
	originalOpenParent := openStableColumnAssetParent
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		movedAnchor, replacementAnchor = path+"-moved", path
		if err := os.Rename(path, movedAnchor); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			_ = parent.Close()
			return nil, err
		}
		return parent, nil
	}
	defer func() { openStableColumnAssetParent = originalOpenParent }()
	ref, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("stable-column-payload"), ColumnAssetKindTCS1TypedColumnPart, 7, 11)
	if err != nil {
		t.Fatal(err)
	}
	relSegmentDir, err := filepath.Rel(replacementAnchor, namespace.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	movedSegmentDir := filepath.Join(movedAnchor, relSegmentDir)
	defer token.Release()
	segmentPath, err := columnAssetSegmentPath(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(movedSegmentDir, filepath.Base(segmentPath))); err != nil {
		t.Fatalf("captured-parent column segment: %v", err)
	}
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path unexpectedly received column segment: %v", err)
	}
	if got := fileSyncs.Load(); got != 1 {
		t.Fatalf("column creation file syncs=%d want exactly 1", got)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTypedColumnMultipart)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := fileSyncs.Load(); got != 1 {
		t.Fatalf("already-synced column token added content sync: %d", got)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != 1 || stats[0].Flushes != 1 || stats[0].Syncs != 1 {
		t.Fatalf("column stable operation counts=%+v", stats)
	}
}

func testStableColumnAssetExistingUnknownNamespaceStabilizesThroughCapturedParent(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "existing_captured_parent",
		},
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatal(err)
	}
	refForPath := ColumnAssetRef{Namespace: cfg.AssetManager.Namespace, FileID: columnAssetM12ASegmentFileID}
	segmentPath, err := columnAssetSegmentPath(dir, refForPath)
	if err != nil {
		t.Fatal(err)
	}
	const existing = "existing-segment-prefix"
	if err := os.WriteFile(segmentPath, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	clearColumnAssetSegmentDirSyncKnown(segmentPath)

	var capturedAnchor, movedAnchor string
	originalOpenParent := openStableColumnAssetParent
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		capturedAnchor, movedAnchor = path, path+"-moved"
		if err := os.Rename(path, movedAnchor); err != nil {
			_ = parent.Close()
			return nil, err
		}
		// Deliberately leave path absent. A path-based directory-sync fallback
		// would fail; stable capture must use the retained parent handle.
		return parent, nil
	}
	defer func() { openStableColumnAssetParent = originalOpenParent }()

	ref, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("stable-append"), ColumnAssetKindTCS1TypedColumnPart, 7, 11,
	)
	if err != nil {
		t.Fatal(err)
	}
	relSegmentDir, err := filepath.Rel(capturedAnchor, namespace.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	movedSegmentDir := filepath.Join(movedAnchor, relSegmentDir)
	defer token.Release()
	if token.Namespace() == nil {
		t.Fatal("existing segment with unknown directory stability missing namespace token")
	}
	if ref.Offset != int64(len(existing)) {
		t.Fatalf("append offset=%d want %d", ref.Offset, len(existing))
	}
	if _, err := os.Stat(namespace.SegmentDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original segment path unexpectedly exists: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(movedSegmentDir, filepath.Base(segmentPath)))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != existing+"stable-append" {
		t.Fatalf("captured-parent segment contents=%q", got)
	}
	if columnAssetSegmentDirSyncKnown(segmentPath) {
		t.Fatal("handle-relative namespace sync certified the replaced pathname cache")
	}

	openStableColumnAssetParent = originalOpenParent
	if err := os.Mkdir(capturedAnchor, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segmentPath, []byte("replacement-prefix"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, replacementToken, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("replacement-append"), ColumnAssetKindTCS1TypedColumnPart, 8, 12,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementToken.Release()
	if replacementToken.Namespace() == nil {
		t.Fatal("replacement-path existing segment inherited stale namespace stability")
	}
	if columnAssetSegmentDirSyncKnown(segmentPath) {
		t.Fatal("stable replacement capture populated pathname-only directory cache")
	}

	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTypedColumnMultipart)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("stable operation counts=%+v want one namespace sync", stats)
	}
}

func testStableColumnAssetCreatedFailureRetainsOrphanAndRemainsRetryable(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "rollback",
		},
	}
	injected := errors.New("injected file sync failure")
	originalSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(*os.File) error { return injected }
	defer func() { syncColumnAssetSegmentFileForPublish = originalSync }()

	if _, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("first"), ColumnAssetKindTCS1TypedColumnPart, 7, 11); !errors.Is(err, injected) {
		t.Fatalf("first write error=%v want injected failure", err)
	} else if token != nil {
		t.Fatal("failed stable write returned a token")
	}
	segmentPath, err := columnAssetSegmentPath(dir, ColumnAssetRef{Namespace: cfg.AssetManager.Namespace, FileID: columnAssetM12ASegmentFileID})
	if err != nil {
		t.Fatal(err)
	}
	failedInfo, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatalf("failed creation orphan stat: %v", err)
	}
	if failedInfo.Size() != int64(len("first")) {
		t.Fatalf("failed creation orphan size=%d want %d", failedInfo.Size(), len("first"))
	}
	if columnAssetSegmentDirSyncKnown(segmentPath) {
		t.Fatal("failed stable creation marked pathname directory-sync cache known")
	}

	syncColumnAssetSegmentFileForPublish = originalSync
	retryPayload := []byte("retry")
	retryRef, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, retryPayload, ColumnAssetKindTCS1TypedColumnPart, 7, 11)
	if err != nil {
		t.Fatalf("retry stable write: %v", err)
	}
	token.Release()
	if retryRef.Offset != failedInfo.Size() {
		t.Fatalf("retry offset=%d want after orphan prefix %d", retryRef.Offset, failedInfo.Size())
	}
	got, err := readColumnPhysicalAssetFromManager(dir, retryRef)
	if err != nil {
		t.Fatalf("read retry ref: %v", err)
	}
	if string(got) != string(retryPayload) {
		t.Fatalf("retry payload=%q want %q", got, retryPayload)
	}
	before, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	syncColumnAssetSegmentFileForPublish = func(*os.File) error { return injected }
	if _, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("unreachable"), ColumnAssetKindTCS1TypedColumnPart, 7, 12); !errors.Is(err, injected) {
		t.Fatalf("append error=%v want injected failure", err)
	} else if token != nil {
		t.Fatal("failed stable append returned a token")
	}
	after, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("failed stable append size=%d want rollback to %d", after.Size(), before.Size())
	}
}

func testStableColumnAssetCaptureFailureInvalidatesPathSyncCache(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "capture_failure_cache",
		},
	}
	primeRef, err := writeColumnAssetToManager(
		dir, cfg, []byte("ordinary-prime"), ColumnAssetKindTCS1TypedColumnPart, 1, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	assetPath, err := columnAssetSegmentPath(dir, primeRef)
	if err != nil {
		t.Fatal(err)
	}
	if !columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("ordinary write did not prime segment directory-sync cache")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	var capturedAnchor, movedAnchor, movedSegmentDir string
	replacementPrefix := []byte("replacement-prefix")
	var retainedParent *os.File
	originalOpenParent := openStableColumnAssetParent
	openParentInstalled := true
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		retainedParent = parent
		capturedAnchor, movedAnchor = path, path+"-moved"
		if err := os.Rename(path, movedAnchor); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.WriteFile(assetPath, replacementPrefix, 0o600); err != nil {
			_ = parent.Close()
			return nil, err
		}
		relSegmentDir, err := filepath.Rel(capturedAnchor, namespace.SegmentDir)
		if err != nil {
			_ = parent.Close()
			return nil, err
		}
		movedSegmentDir = filepath.Join(movedAnchor, relSegmentDir)
		return parent, nil
	}
	t.Cleanup(func() {
		if openParentInstalled {
			openStableColumnAssetParent = originalOpenParent
		}
	})

	injectedCapture := errors.New("injected stable column asset capture failure")
	var retainedFile *os.File
	originalResourceToken := stableColumnAssetResourceTokenForPublish
	resourceHookInstalled := true
	stableColumnAssetResourceTokenForPublish = func(file *os.File, _ ColumnAssetRef, _ *rootpublication.StableNamespaceToken) (*rootpublication.StableResourceToken, error) {
		retainedFile = file
		return nil, injectedCapture
	}
	t.Cleanup(func() {
		if resourceHookInstalled {
			stableColumnAssetResourceTokenForPublish = originalResourceToken
		}
	})

	failedRef, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("failed-stable-append"), ColumnAssetKindTCS1TypedColumnPart, 2, 2,
	)
	openStableColumnAssetParent = originalOpenParent
	openParentInstalled = false
	stableColumnAssetResourceTokenForPublish = originalResourceToken
	resourceHookInstalled = false
	if !errors.Is(err, injectedCapture) {
		t.Fatalf("stable capture error=%v, want injected capture failure", err)
	}
	if failedRef != (ColumnAssetRef{}) || token != nil {
		t.Fatalf("failed stable capture leaked success: ref=%+v token=%v", failedRef, token)
	}
	if retainedParent == nil || retainedFile == nil {
		t.Fatalf("stable capture did not retain exact handles: parent=%p file=%p", retainedParent, retainedFile)
	}
	if _, err := retainedParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed stable capture leaked parent handle: %v", err)
	}
	if _, err := retainedFile.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed stable capture leaked segment handle: %v", err)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Errorf("failed stable capture retained stale pathname directory-sync cache")
	}

	injectedDirSync := errors.New("injected replacement column asset directory sync")
	originalDirSync := syncColumnAssetSegmentDirForPublish
	dirSyncHookInstalled := true
	dirSyncCalls := 0
	syncColumnAssetSegmentDirForPublish = func(path string) error {
		dirSyncCalls++
		if path != namespace.SegmentDir {
			t.Errorf("directory sync path=%q, want replacement %q", path, namespace.SegmentDir)
		}
		return injectedDirSync
	}
	t.Cleanup(func() {
		if dirSyncHookInstalled {
			syncColumnAssetSegmentDirForPublish = originalDirSync
		}
	})

	retryRef, err := writeColumnAssetToManager(
		dir, cfg, []byte("ordinary-retry"), ColumnAssetKindTCS1TypedColumnPart, 3, 3,
	)
	if !errors.Is(err, injectedDirSync) {
		t.Errorf("ordinary replacement retry error=%v, want required directory sync failure", err)
	}
	if retryRef != (ColumnAssetRef{}) {
		t.Errorf("failed ordinary replacement retry leaked success ref=%+v", retryRef)
	}
	if dirSyncCalls != 1 {
		t.Errorf("replacement directory sync calls=%d, want 1", dirSyncCalls)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Errorf("failed replacement directory sync marked pathname cache stable")
	}

	syncColumnAssetSegmentDirForPublish = originalDirSync
	dirSyncHookInstalled = false
	finalRef, err := writeColumnAssetToManager(
		dir, cfg, []byte("ordinary-retry-success"), ColumnAssetKindTCS1TypedColumnPart, 4, 4,
	)
	if err != nil {
		t.Fatalf("ordinary replacement retry after restored sync: %v", err)
	}
	if finalRef == (ColumnAssetRef{}) || !columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatalf("successful replacement retry did not publish synced path: ref=%+v known=%v", finalRef, columnAssetSegmentDirSyncKnown(assetPath))
	}
	if _, err := os.Stat(filepath.Join(movedSegmentDir, filepath.Base(assetPath))); err != nil {
		t.Fatalf("failed stable append did not remain bound to moved parent: %v", err)
	}
}

func testStableColumnAssetCaptureFailureResourcePlateau(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "capture_failure_plateau",
		},
	}
	primeRef, err := writeColumnAssetToManager(
		dir, cfg, []byte("ordinary-prime"), ColumnAssetKindTCS1TypedColumnPart, 1, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	assetPath, err := columnAssetSegmentPath(dir, primeRef)
	if err != nil {
		t.Fatal(err)
	}

	injectedCapture := errors.New("injected repeated stable column asset capture failure")
	originalOpenParent := openStableColumnAssetParent
	openParentInstalled := true
	var retainedParents []*os.File
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err == nil {
			retainedParents = append(retainedParents, parent)
		}
		return parent, err
	}
	t.Cleanup(func() {
		if openParentInstalled {
			openStableColumnAssetParent = originalOpenParent
		}
	})
	originalResourceToken := stableColumnAssetResourceTokenForPublish
	resourceHookInstalled := true
	var retainedFiles []*os.File
	stableColumnAssetResourceTokenForPublish = func(file *os.File, _ ColumnAssetRef, _ *rootpublication.StableNamespaceToken) (*rootpublication.StableResourceToken, error) {
		retainedFiles = append(retainedFiles, file)
		return nil, injectedCapture
	}
	t.Cleanup(func() {
		if resourceHookInstalled {
			stableColumnAssetResourceTokenForPublish = originalResourceToken
		}
	})

	const attempts = 64
	for attempt := 0; attempt < attempts; attempt++ {
		if !columnAssetSegmentDirSyncKnown(assetPath) {
			t.Fatalf("attempt %d began without ordinary synced-path cache", attempt)
		}
		generation := uint64(attempt*2 + 2)
		failedRef, token, err := writeColumnAssetToManagerWithStableResource(
			dir, cfg, []byte("failed-stable-append"), ColumnAssetKindTCS1TypedColumnPart, generation, generation,
		)
		if !errors.Is(err, injectedCapture) || failedRef != (ColumnAssetRef{}) || token != nil {
			t.Fatalf("attempt %d stable result ref=%+v token=%v err=%v", attempt, failedRef, token, err)
		}
		if columnAssetSegmentDirSyncKnown(assetPath) {
			t.Fatalf("attempt %d retained stale pathname cache", attempt)
		}
		if len(retainedParents) != attempt+1 || len(retainedFiles) != attempt+1 {
			t.Fatalf("attempt %d retained handle counts parent=%d file=%d", attempt, len(retainedParents), len(retainedFiles))
		}
		if _, err := retainedParents[attempt].Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("attempt %d leaked parent: %v", attempt, err)
		}
		if _, err := retainedFiles[attempt].Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("attempt %d leaked file: %v", attempt, err)
		}
		ordinaryRef, err := writeColumnAssetToManager(
			dir, cfg, []byte("ordinary-reprime"), ColumnAssetKindTCS1TypedColumnPart, generation+1, generation+1,
		)
		if err != nil || ordinaryRef == (ColumnAssetRef{}) || !columnAssetSegmentDirSyncKnown(assetPath) {
			t.Fatalf("attempt %d ordinary reprime ref=%+v known=%v err=%v", attempt, ordinaryRef, columnAssetSegmentDirSyncKnown(assetPath), err)
		}
	}
	openStableColumnAssetParent = originalOpenParent
	openParentInstalled = false
	stableColumnAssetResourceTokenForPublish = originalResourceToken
	resourceHookInstalled = false
}

func TestStableResourceInventoryClassifiesEveryColumnAssetKind(t *testing.T) {
	type expectedPolicy struct {
		field          rootpublication.ReachabilityField
		classification string
	}
	want := map[ColumnAssetKind]expectedPolicy{
		ColumnAssetKindTCS1PartImage:              {rootpublication.ReachabilityColumnManifest, "authoritative"},
		ColumnAssetKindTCS1TypedColumnPart:        {rootpublication.ReachabilityTypedColumnMultipart, "authoritative"},
		ColumnAssetKindTCS1AggregateMetadata:      {rootpublication.ReachabilityTypedColumnValue, "authoritative"},
		ColumnAssetKindTCS1DictionaryCodes:        {rootpublication.ReachabilityTypedColumnCode, "authoritative"},
		ColumnAssetKindTCS1Int64Values:            {rootpublication.ReachabilityTypedColumnValue, "authoritative"},
		ColumnAssetKindTCS1HNSWSearchPack:         {rootpublication.ReachabilityHNSWSearchPack, "authoritative"},
		ColumnAssetKindQueryReadyBase:             {rootpublication.ReachabilityQueryReadyBase, "rebuildable-non-authoritative"},
		ColumnAssetKindQueryReadyDelta:            {rootpublication.ReachabilityQueryReadyDelta, "rebuildable-non-authoritative"},
		ColumnAssetKindQueryReadyConsolidatedBase: {rootpublication.ReachabilityQueryReadyConsolidatedBase, "rebuildable-non-authoritative"},
	}
	declared := declaredColumnAssetKinds(t)
	generated := make(map[string]authorityinventory.Row)
	for _, row := range authorityinventory.Rows {
		const prefix = "collections.ColumnAssetKind."
		if strings.HasPrefix(row.Field, prefix) {
			generated[strings.TrimPrefix(row.Field, prefix)] = row
		}
	}
	if len(declared) != len(want) || len(generated) != len(declared) {
		t.Fatalf("column asset closure source=%d reviewed=%d generated=%d", len(declared), len(want), len(generated))
	}
	for name, kind := range declared {
		expected, ok := want[kind]
		if !ok {
			t.Errorf("source constant %s=%q has no reviewed stable-resource policy", name, kind)
			continue
		}
		gotKind, gotField, gotClassification, err := stableColumnAssetResourceClassification(kind)
		if err != nil {
			t.Errorf("classify %q: %v", kind, err)
			continue
		}
		if gotField != expected.field {
			t.Errorf("kind %q field=%q want %q", kind, gotField, expected.field)
		}
		if gotKind == "" {
			t.Errorf("kind %q has empty resource kind", kind)
		}
		if gotClassification != expected.classification {
			t.Errorf("kind %q classification=%q want literal %q", kind, gotClassification, expected.classification)
		}
		policy, ok := rootpublication.StableResourcePolicyFor(gotField)
		if !ok || policy.Kind != gotKind || policy.Classification != gotClassification || policy.Producer != rootpublication.StableProducerColumnAsset {
			t.Errorf("kind %q collection policy=(%q,%q,%q) canonical=%+v", kind, gotKind, gotField, gotClassification, policy)
		}
		row, ok := generated[name]
		if !ok {
			t.Errorf("source constant %s missing generated authority row", name)
			continue
		}
		wantState := authorityinventory.ActivationActive
		if gotClassification == "rebuildable-non-authoritative" {
			wantState = authorityinventory.ActivationNonAuthoritative
		}
		if row.ActivationState != wantState {
			t.Errorf("generated row %s state=%q want %q for canonical classification %q", row.Field, row.ActivationState, wantState, gotClassification)
		}
	}
	if _, _, _, err := stableColumnAssetResourceClassification(ColumnAssetKind("future-authoritative-kind")); err == nil {
		t.Fatal("unknown column asset kind did not fail inventory coverage")
	}
}

func TestStableColumnPreparedValidationRejectsMissingAuthoritativeResources(t *testing.T) {
	for _, kind := range []ColumnAssetKind{
		ColumnAssetKindTCS1PartImage,
		ColumnAssetKindTCS1TypedColumnPart,
		ColumnAssetKindTCS1AggregateMetadata,
		ColumnAssetKindTCS1DictionaryCodes,
		ColumnAssetKindTCS1Int64Values,
		ColumnAssetKindTCS1HNSWSearchPack,
	} {
		t.Run(string(kind), func(t *testing.T) {
			authoritative := ColumnPreparedAsset{Ref: ColumnAssetRef{
				Kind: kind, Namespace: "missing-authority", Generation: 1,
				PartID: 1, FileID: 1, Offset: 0, Length: 8, Checksum: 1,
			}}
			if err := validateStableColumnResourcesMatchPrepared([]ColumnPreparedAsset{authoritative}, nil); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("authoritative prepared ref without stable resources error=%v want ErrUnresolvedResource", err)
			}
		})
	}
	authoritative := ColumnPreparedAsset{Ref: ColumnAssetRef{
		Kind: ColumnAssetKindTCS1PartImage, Namespace: "missing-authority", Generation: 1,
		PartID: 1, FileID: 1, Offset: 0, Length: 8, Checksum: 1,
	}}
	rebuildable := authoritative
	rebuildable.Ref.Kind = ColumnAssetKindQueryReadyBase
	if err := validateStableColumnResourcesMatchPrepared([]ColumnPreparedAsset{rebuildable}, nil); err != nil {
		t.Fatalf("rebuildable prepared ref requires no authoritative resource: %v", err)
	}
}

func TestStableColumnPreparedValidationRejectsEachMissingProductionObligation(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column production authority requires exact relative namespace support")
	}
	kinds := []ColumnAssetKind{
		ColumnAssetKindTCS1PartImage,
		ColumnAssetKindTCS1TypedColumnPart,
		ColumnAssetKindTCS1AggregateMetadata,
		ColumnAssetKindTCS1Int64Values,
		ColumnAssetKindTCS1DictionaryCodes,
		ColumnAssetKindTCS1HNSWSearchPack,
	}
	const authoritativeProductionChildCount = 6
	if len(kinds) != authoritativeProductionChildCount {
		t.Fatalf("production authoritative child inventory=%d want %d", len(kinds), authoritativeProductionChildCount)
	}
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	cfg.AssetManager = &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "production-omission-matrix",
	}
	registry := rootpublication.NewIdentityPinRegistry()
	sources := make([]*rootpublication.StableResourceSet, 0, len(kinds))
	expected := make([]ColumnPreparedAsset, 0, len(kinds))
	for i, kind := range kinds {
		session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
		refs, err := session.appendKinds(uint32(i+1), []columnPhysicalAssetAppendItem{{
			payload: []byte("production-child-" + string(kind)), kind: kind, generation: 1, partID: uint64(i + 1),
		}})
		if err != nil {
			_ = session.abort()
			t.Fatal(err)
		}
		_, resources, err := session.closeWithStableResources()
		if err != nil {
			t.Fatal(err)
		}
		if resources == nil || len(refs) != 1 {
			t.Fatalf("kind %s refs=%d resources=%v", kind, len(refs), resources)
		}
		sources = append(sources, resources)
		expected = append(expected, ColumnPreparedAsset{Ref: refs[0]})
	}
	full, err := rootpublication.UnionStableResourceSets(sources...)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateStableColumnResourcesMatchPrepared(expected, full); err != nil {
		t.Fatalf("complete production closure: %v", err)
	}
	logicalObligationCount := 0
	for _, descriptor := range full.Descriptors() {
		logicalObligationCount += len(descriptor.LogicalObligations())
	}
	if logicalObligationCount != authoritativeProductionChildCount {
		t.Fatalf("complete production closure logical children=%d want %d", logicalObligationCount, authoritativeProductionChildCount)
	}
	for omitted := range sources {
		partialSources := append([]*rootpublication.StableResourceSet(nil), sources[:omitted]...)
		partialSources = append(partialSources, sources[omitted+1:]...)
		partial, err := rootpublication.UnionStableResourceSets(partialSources...)
		if err != nil {
			t.Fatal(err)
		}
		wantField := stableColumnLogicalObligation(expected[omitted].Ref, mustStableColumnReachability(t, expected[omitted].Ref.Kind)).Reachability
		err = validateStableColumnResourcesMatchPrepared(expected, partial)
		if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !strings.Contains(err.Error(), string(wantField)) {
			t.Fatalf("omitted production child %s error=%v want typed missing field %s", kinds[omitted], err, wantField)
		}
	}
	for _, resources := range sources {
		resources.Release()
	}
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatalf("production omission release pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities())
	}
}

func mustStableColumnReachability(t *testing.T, kind ColumnAssetKind) rootpublication.ReachabilityField {
	t.Helper()
	_, field, _, err := stableColumnAssetResourceClassification(kind)
	if err != nil {
		t.Fatal(err)
	}
	return field
}

func TestStableLegacyVectorResourceTokenIsExcluded(t *testing.T) {
	_, err := NewStableLegacyVectorResourceToken(rootpublication.StableResourceSpec{
		Reachability: rootpublication.ReachabilityLegacyVectorSnapshot,
	})
	if !errors.Is(err, rootpublication.ErrResourceExcluded) {
		t.Fatalf("legacy vector token error=%v, want ErrResourceExcluded", err)
	}
}

func declaredColumnAssetKinds(t *testing.T) map[string]ColumnAssetKind {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "column_publish_plan.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := make(map[string]ColumnAssetKind)
	for _, declaration := range parsed.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, rawSpec := range general.Specs {
			spec := rawSpec.(*ast.ValueSpec)
			for i, name := range spec.Names {
				if !strings.HasPrefix(name.Name, "ColumnAssetKind") {
					continue
				}
				typeName, typed := spec.Type.(*ast.Ident)
				if !typed || typeName.Name != "ColumnAssetKind" || len(spec.Values) != len(spec.Names) {
					t.Fatalf("%s must use an explicit ColumnAssetKind string declaration for inventory closure", name.Name)
				}
				literal, ok := spec.Values[i].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					t.Fatalf("%s must use an explicit string literal", name.Name)
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				declared[name.Name] = ColumnAssetKind(value)
			}
		}
	}
	return declared
}

func testStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "creation-first", true: "creation-last"}[reverse], func(t *testing.T) {
			dir := t.TempDir()
			cfg := ColumnStoreConfig{
				Enabled: true,
				AssetManager: &ColumnAssetManagerConfig{
					Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "coalesce_resource",
				},
			}
			firstRef, first, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("first-payload"), ColumnAssetKindTCS1TypedColumnPart, 7, 11)
			if err != nil {
				t.Fatal(err)
			}
			secondRef, second, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("second-payload"), ColumnAssetKindTCS1TypedColumnPart, 7, 12)
			if err != nil {
				first.Release()
				t.Fatal(err)
			}
			if first.Namespace() == nil || second.Namespace() == nil {
				first.Release()
				second.Release()
				t.Fatalf("stable captures must retain exact namespace obligations: first=%v second=%v", first.Namespace() != nil, second.Namespace() != nil)
			}
			ordered := []*rootpublication.StableResourceToken{first, second}
			if reverse {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTypedColumnMultipart)
			for _, token := range ordered {
				if err := builder.Add(token); err != nil {
					builder.Abandon()
					t.Fatal(err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			if set.Len() != 1 {
				t.Fatalf("coalesced len=%d want 1", set.Len())
			}
			if got, want := set.FrontierFor(first.Identity(), uint64(firstRef.FileID)).Bytes, uint64(secondRef.Offset+secondRef.Length); got != want {
				t.Fatalf("coalesced frontier=%d want %d", got, want)
			}
			if firstRef.Generation != secondRef.Generation || firstRef.FileID != secondRef.FileID ||
				firstRef.PartID == secondRef.PartID || firstRef.Offset == secondRef.Offset ||
				firstRef.Length == secondRef.Length || firstRef.Checksum == secondRef.Checksum {
				t.Fatalf("test requires sibling logical refs in one physical segment: first=%+v second=%+v", firstRef, secondRef)
			}
			descriptors := set.Descriptors()
			if len(descriptors) != 1 {
				t.Fatalf("descriptors=%d want one coalesced physical descriptor", len(descriptors))
			}
			obligations := descriptors[0].LogicalObligations()
			if len(obligations) != 2 {
				t.Fatalf("logical obligations=%+v want both sibling refs", obligations)
			}
			byPart := make(map[uint64]rootpublication.StableLogicalObligation, len(obligations))
			for _, obligation := range obligations {
				byPart[obligation.PartID] = obligation
			}
			for _, ref := range []ColumnAssetRef{firstRef, secondRef} {
				obligation, ok := byPart[ref.PartID]
				if !ok {
					t.Fatalf("logical obligations=%+v missing part %d", obligations, ref.PartID)
				}
				if obligation.Class != "column-asset-ref-v1" || obligation.Kind != string(ref.Kind) ||
					obligation.Namespace != ref.Namespace || obligation.Generation != ref.Generation ||
					obligation.FileID != uint64(ref.FileID) || obligation.Offset != ref.Offset ||
					obligation.Length != ref.Length || obligation.Checksum != ref.Checksum ||
					obligation.Reachability != rootpublication.ReachabilityTypedColumnMultipart || obligation.Digest == [32]byte{} {
					t.Fatalf("logical obligation=%+v does not preserve ref=%+v", obligation, ref)
				}
			}
			stats := set.Stats(time.Now())
			if len(stats) != 1 || stats[0].ActivePins != 1 || stats[0].NamespaceSyncs != 1 || stats[0].LogicalObligationCount != 2 {
				t.Fatalf("coalesced stats=%+v", stats)
			}
		})
	}
}

func testStableColumnAssetTokenBindsExactSegmentAndRange(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable_resource",
		},
	}
	ref, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("stable-column-payload"), ColumnAssetKindTCS1TypedColumnPart, 7, 11)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if token.Kind() != rootpublication.ResourceTypedColumnAsset || token.Reachability() != rootpublication.ReachabilityTypedColumnMultipart {
		t.Fatalf("token kind=%q field=%q", token.Kind(), token.Reachability())
	}
	if token.Frontier().Bytes != uint64(ref.Offset+ref.Length) {
		t.Fatalf("frontier=%d want %d", token.Frontier().Bytes, ref.Offset+ref.Length)
	}
	if token.Digest() == [32]byte{} {
		t.Fatal("column asset token missing immutable ref digest")
	}
	if token.Namespace() == nil {
		t.Fatal("new column segment token missing stable namespace operation")
	}

	segmentPath, err := columnAssetSegmentPath(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPath := filepath.Join(filepath.Dir(segmentPath), "rotated-original.seg")
	if err := os.Rename(segmentPath, rotatedPath); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement-path-bytes")
	if err := os.WriteFile(segmentPath, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, ref.Length)
	if _, err := token.ReadAt(got, ref.Offset); err != nil {
		t.Fatal(err)
	}
	if string(got) != "stable-column-payload" {
		t.Fatalf("pinned token read %q after path replacement", got)
	}
}

func testStableColumnAppendSessionReturnsCoalescedPinnedAuthority(t *testing.T) {
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable-session",
		},
	}
	registry := rootpublication.NewIdentityPinRegistry()
	physicalResourceSyncs := 0
	originalResourceSync := syncStableColumnAssetResourceForPublish
	syncStableColumnAssetResourceForPublish = func(*os.File, rootpublication.DurableFrontier) error {
		physicalResourceSyncs++
		return nil
	}
	t.Cleanup(func() { syncStableColumnAssetResourceForPublish = originalResourceSync })
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
	refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{
		{payload: []byte("row-image"), kind: ColumnAssetKindTCS1PartImage, generation: 7, partID: 1},
		{payload: []byte("typed-column-part"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 7, partID: 2},
		{payload: []byte("dictionary-codes"), kind: ColumnAssetKindTCS1DictionaryCodes, generation: 7, partID: 3},
		{payload: []byte("hnsw-search-pack"), kind: ColumnAssetKindTCS1HNSWSearchPack, generation: 7, partID: 4},
		{payload: []byte("int64-values"), kind: ColumnAssetKindTCS1Int64Values, generation: 7, partID: 5},
	})
	if err != nil {
		_ = session.abort()
		t.Fatal(err)
	}
	closeStats, resources, err := session.closeWithStableResources()
	if err != nil {
		t.Fatal(err)
	}
	if resources == nil {
		t.Fatal("stable append session returned nil resources")
	}
	defer resources.Release()
	if closeStats.FileSyncCount != 1 || closeStats.SyncEpochCount != 1 {
		t.Fatalf("stable append close stats=%+v want one content sync epoch", closeStats)
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != 3 {
		t.Fatalf("stable descriptors=%d want manifest, typed-column, and vector resource kinds", len(descriptors))
	}
	byKind := make(map[rootpublication.ResourceKind]rootpublication.StableResourceDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		byKind[descriptor.Kind()] = descriptor
	}
	typed := byKind[rootpublication.ResourceTypedColumnAsset]
	if got, want := typed.Frontier().Bytes, uint64(refs[4].Offset+refs[4].Length); got != want {
		t.Fatalf("typed-column coalesced frontier=%d want %d", got, want)
	}
	vector := byKind[rootpublication.ResourceVectorGraphPack]
	if got, want := vector.Frontier().Bytes, uint64(refs[3].Offset+refs[3].Length); got != want {
		t.Fatalf("vector frontier=%d want %d", got, want)
	}
	manifest := byKind[rootpublication.ResourceColumnAsset]
	if got, want := manifest.Frontier().Bytes, uint64(refs[0].Offset+refs[0].Length); got != want {
		t.Fatalf("manifest asset frontier=%d want %d", got, want)
	}
	wantFields := map[rootpublication.ReachabilityField]bool{
		rootpublication.ReachabilityColumnManifest:       false,
		rootpublication.ReachabilityTypedColumnMultipart: false,
		rootpublication.ReachabilityTypedColumnCode:      false,
		rootpublication.ReachabilityTypedColumnValue:     false,
		rootpublication.ReachabilityHNSWSearchPack:       false,
	}
	for _, descriptor := range descriptors {
		for _, field := range descriptor.ReachabilityFields() {
			if _, ok := wantFields[field]; ok {
				wantFields[field] = true
			}
		}
	}
	for field, found := range wantFields {
		if !found {
			t.Errorf("stable append authority missing reachability field %q", field)
		}
	}
	var obligations []rootpublication.StableLogicalObligation
	for _, descriptor := range descriptors {
		obligations = append(obligations, descriptor.LogicalObligations()...)
	}
	if len(obligations) != len(refs) {
		t.Fatalf("logical obligations=%d want %d across resource kinds", len(obligations), len(refs))
	}
	for _, ref := range refs {
		found := false
		for _, obligation := range obligations {
			if obligation.Kind == string(ref.Kind) && obligation.Offset == ref.Offset && obligation.Length == ref.Length && obligation.Checksum == ref.Checksum {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("logical obligations=%+v missing ref=%+v", obligations, ref)
		}
	}
	if got := registry.ActivePins(); got != 3 {
		t.Fatalf("active kind-scoped pins=%d want 3", got)
	}
	resourceSyncCounts := func() (uint64, uint64) {
		t.Helper()
		var contentSyncs, namespaceSyncs uint64
		for _, stats := range resources.Stats(time.Now()) {
			contentSyncs += stats.Syncs
			namespaceSyncs += stats.NamespaceSyncs
		}
		return contentSyncs, namespaceSyncs
	}
	if contentSyncs, namespaceSyncs := resourceSyncCounts(); contentSyncs != 0 || namespaceSyncs != 1 {
		t.Fatalf("captured sync counts content=%d namespace=%d want 0,1", contentSyncs, namespaceSyncs)
	}
	if err := resources.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if contentSyncs, namespaceSyncs := resourceSyncCounts(); contentSyncs != uint64(len(descriptors)) || namespaceSyncs != 1 {
		t.Fatalf("post-SyncThrough attempt counts content=%d namespace=%d want %d,1", contentSyncs, namespaceSyncs, len(descriptors))
	}
	if physicalResourceSyncs != 0 {
		t.Fatalf("post-SyncThrough physical content syncs=%d want 0", physicalResourceSyncs)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(root, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentDir := namespace.SegmentDir
	if _, err := deleteColumnAssetSegmentWithStableLease(segmentDir, refs[0].FileID, registry); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("delete pinned segment error=%v want ErrResourcePinned", err)
	}
	resources.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active physical pins after release=%d want 0", got)
	}
	deleted, err := deleteColumnAssetSegmentWithStableLease(segmentDir, refs[0].FileID, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("stable delete reported no deletion")
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active registry identities after delete=%d want 0", got)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != 0 {
		t.Fatalf("stable namespace proofs after delete=%d want 0", got)
	}
}

func testStableColumnAppendSessionNamespaceSyncProofTracksExactIdentity(t *testing.T) {
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable-link-proof",
		},
	}
	registry := rootpublication.NewIdentityPinRegistry()
	appendAndClose := func(generation uint64) (ColumnAssetRef, uint64) {
		t.Helper()
		session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
		refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
			payload: []byte("payload"), kind: ColumnAssetKindTCS1PartImage, generation: generation, partID: generation,
		}})
		if err != nil {
			_ = session.abort()
			t.Fatal(err)
		}
		_, resources, err := session.closeWithStableResources()
		if err != nil {
			t.Fatal(err)
		}
		var namespaceSyncs uint64
		for _, stats := range resources.Stats(time.Now()) {
			namespaceSyncs += stats.NamespaceSyncs
		}
		resources.Release()
		return refs[0], namespaceSyncs
	}

	firstRef, firstSyncs := appendAndClose(1)
	if firstSyncs != 1 {
		t.Fatalf("first exact binding namespace syncs=%d want 1", firstSyncs)
	}
	_, secondSyncs := appendAndClose(2)
	if secondSyncs != 0 {
		t.Fatalf("known exact binding namespace syncs=%d want 0", secondSyncs)
	}
	segmentPath, err := columnAssetSegmentPath(root, firstRef)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(segmentPath, segmentPath+".rebound-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segmentPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	thirdRef, reboundSyncs := appendAndClose(3)
	if reboundSyncs != 1 {
		t.Fatalf("rebound exact binding namespace syncs=%d want 1", reboundSyncs)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != 1 {
		t.Fatalf("stable namespace proofs after rebound=%d want one current binding", got)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(root, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := deleteColumnAssetSegmentWithStableLease(namespace.SegmentDir, thirdRef.FileID, registry); err != nil || !deleted {
		t.Fatalf("delete rebound current binding deleted=%t err=%v", deleted, err)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != 0 {
		t.Fatalf("stable namespace proofs after exact delete=%d want 0", got)
	}
}

func testStableColumnAppendSessionFailureReleasesPinsAndNamespaceProof(t *testing.T) {
	for _, test := range []struct {
		name        string
		closeParent bool
	}{
		{name: "token-build"},
		{name: "parent-close", closeParent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "column-assets")
			cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
				Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable-failure-" + test.name,
			}}
			registry := rootpublication.NewIdentityPinRegistry()
			var parent *os.File
			originalOpenParent := openStableColumnAssetParent
			openStableColumnAssetParent = func(path string) (*os.File, error) {
				opened, err := originalOpenParent(path)
				parent = opened
				return opened, err
			}
			t.Cleanup(func() { openStableColumnAssetParent = originalOpenParent })
			injected := errors.New("injected stable capture failure")
			originalToken := stableColumnAssetResourceTokenWithRegistryForPublish
			stableColumnAssetResourceTokenWithRegistryForPublish = func(file *os.File, ref ColumnAssetRef, namespace *rootpublication.StableNamespaceToken, registry *rootpublication.IdentityPinRegistry) (*rootpublication.StableResourceToken, error) {
				if !test.closeParent {
					return nil, injected
				}
				token, err := originalToken(file, ref, namespace, registry)
				if err != nil {
					return nil, err
				}
				if err := parent.Close(); err != nil {
					token.Release()
					return nil, err
				}
				return token, nil
			}
			t.Cleanup(func() { stableColumnAssetResourceTokenWithRegistryForPublish = originalToken })

			session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
			if _, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
				payload: []byte("payload"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
			}}); err != nil {
				t.Fatal(err)
			}
			_, resources, err := session.closeWithStableResources()
			if err == nil {
				if resources != nil {
					resources.Release()
				}
				t.Fatal("injected stable close failure returned nil error")
			}
			if resources != nil {
				resources.Release()
				t.Fatal("failed stable close returned resources")
			}
			if got := registry.ActivePins(); got != 0 {
				t.Fatalf("active pins after failure=%d want 0", got)
			}
			if got := registry.ActiveIdentities(); got != 0 {
				t.Fatalf("active identities after failure=%d want 0", got)
			}
			if got := registry.ActiveStableNamespaceLinks(); got != 0 {
				t.Fatalf("stable namespace proofs after failure=%d want 0", got)
			}
		})
	}
}

func testColumnCommandWALPublishRetainsStableAssetAuthorityInDurableSlots(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	if _, err := col.Insert([]byte("stable-publish"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got <= baselinePins {
		t.Fatalf("active stable asset pins after command-WAL publish=%d want greater than baseline %d", got, baselinePins)
	}
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column asset manager config after publish: %+v", cfg)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(namespace.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	segments := 0
	var identities []rootpublication.StableIdentity
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), columnAssetSegmentFilePrefix) ||
			!strings.HasSuffix(entry.Name(), columnAssetSegmentFileSuffix) {
			continue
		}
		segments++
		segment, err := os.Open(filepath.Join(namespace.SegmentDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(segment)
		closeErr := segment.Close()
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		identities = append(identities, identity)
		if got := registry.PinCount(identity); got == 0 {
			t.Fatalf("column segment %q pins after command-WAL publish=%d want retained durable-slot authority", entry.Name(), got)
		}
		if got := registry.ObserverCount(identity); got == 0 {
			t.Fatalf("column segment %q observers after command-WAL publish=%d want retained durable-slot authority", entry.Name(), got)
		}
	}
	if segments == 0 {
		t.Fatal("command-WAL publish produced no column segment identities to verify")
	}
	if got := registry.ActiveStableNamespaceLinks(); got == 0 {
		t.Fatal("successful command-WAL publish retained no exact namespace sync proof")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, identity := range identities {
		if got := registry.PinCount(identity); got != 0 {
			t.Fatalf("column segment pins after DB close=%d want 0", got)
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("column segment observers after DB close=%d want 0", got)
		}
	}
}

func testColumnAssetStableDeletePreservesReboundEntry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "rebound-delete",
		},
	}
	ref, err := writeColumnAssetToManager(root, cfg, []byte("original"), ColumnAssetKindTCS1PartImage, 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPath := segmentPath + ".rotated"
	restoreHook := setColumnAssetStableDeleteBeforeUnlinkTestHook(func() {
		if err := os.Rename(segmentPath, rotatedPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(segmentPath, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(restoreHook)
	registry := rootpublication.NewIdentityPinRegistry()
	deleted, err := deleteColumnAssetSegmentWithStableLease(filepath.Dir(segmentPath), ref.FileID, registry)
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("rebound delete error=%v want ErrResourceConflict", err)
	}
	if deleted {
		t.Fatal("rebound delete reported deletion")
	}
	replacement, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(replacement) != "replacement" {
		t.Fatalf("rebound entry=%q want replacement", replacement)
	}
	original, err := os.ReadFile(rotatedPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(original) != "original" {
		t.Fatalf("rotated original=%q want original", original)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active registry identities after aborted delete=%d want 0", got)
	}
}

func benchmarkStableCentralColumnAppendSessionAuthority(b *testing.B) {
	root := filepath.Join(b.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable-central-append-benchmark",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	items := []columnPhysicalAssetAppendItem{
		{payload: []byte("column-manifest"), kind: ColumnAssetKindTCS1PartImage},
		{payload: []byte("typed-multipart"), kind: ColumnAssetKindTCS1TypedColumnPart},
		{payload: []byte("typed-value"), kind: ColumnAssetKindTCS1Int64Values},
		{payload: []byte("typed-code"), kind: ColumnAssetKindTCS1DictionaryCodes},
		{payload: []byte("hnsw-search-pack"), kind: ColumnAssetKindTCS1HNSWSearchPack},
	}
	var payloadBytes int64
	for _, item := range items {
		payloadBytes += int64(len(item.payload))
	}

	appendAndClose := func(generation uint64) (columnPhysicalAssetSegmentCloseStats, *rootpublication.StableResourceSet, uint64) {
		b.Helper()
		session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
		iterationItems := append([]columnPhysicalAssetAppendItem(nil), items...)
		for i := range iterationItems {
			iterationItems[i].generation = generation
			iterationItems[i].partID = uint64(i + 1)
		}
		if _, err := session.appendKinds(columnAssetM12ASegmentFileID, iterationItems); err != nil {
			_ = session.abort()
			b.Fatal(err)
		}
		closeStats, resources, err := session.closeWithStableResources()
		if err != nil {
			b.Fatal(err)
		}
		if resources == nil {
			b.Fatal("stable central append returned no authority")
		}
		var namespaceSyncs uint64
		for _, stats := range resources.Stats(time.Now()) {
			namespaceSyncs += stats.NamespaceSyncs
		}
		return closeStats, resources, namespaceSyncs
	}

	const descriptorsPerIteration = 3 // column, typed-column, and HNSW resource kinds
	primeStats, primeResources, primeNamespaceSyncs := appendAndClose(1)
	if primeStats.FileSyncCount != 1 || primeNamespaceSyncs != 1 {
		primeResources.Release()
		b.Fatalf("prime close stats=%+v namespace syncs=%d want one content sync and one shared namespace sync", primeStats, primeNamespaceSyncs)
	}
	primeResources.Release()

	b.ReportAllocs()
	b.SetBytes(payloadBytes)
	b.ResetTimer()
	var contentSyncs uint64
	var namespaceSyncs uint64
	var pinHighWater uint64
	var descriptors uint64
	var logicalObligations uint64
	for i := 0; i < b.N; i++ {
		closeStats, resources, iterationNamespaceSyncs := appendAndClose(uint64(i + 2))
		contentSyncs += uint64(closeStats.FileSyncCount)
		namespaceSyncs += iterationNamespaceSyncs
		descriptors += uint64(len(resources.Descriptors()))
		for _, descriptor := range resources.Descriptors() {
			logicalObligations += uint64(len(descriptor.LogicalObligations()))
		}
		for _, stats := range resources.Stats(time.Now()) {
			if stats.PinHighWater > pinHighWater {
				pinHighWater = stats.PinHighWater
			}
		}
		resources.Release()
	}
	b.StopTimer()

	if contentSyncs != uint64(b.N) {
		b.Fatalf("content syncs=%d want %d", contentSyncs, b.N)
	}
	if namespaceSyncs != 0 {
		b.Fatalf("namespace syncs after exact proof priming=%d want 0", namespaceSyncs)
	}
	if pinHighWater != 1 {
		b.Fatalf("pin high-water=%d want 1", pinHighWater)
	}
	if descriptors != uint64(b.N*descriptorsPerIteration) || logicalObligations != uint64(b.N*len(items)) {
		b.Fatalf("descriptors=%d logical obligations=%d want %d/%d", descriptors, logicalObligations, b.N*descriptorsPerIteration, b.N*len(items))
	}
	if got := registry.ActivePins(); got != 0 {
		b.Fatalf("active pins after benchmark=%d want 0", got)
	}
	b.ReportMetric(float64(contentSyncs)/float64(b.N), "content_syncs/op")
	b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "namespace_syncs/op")
	b.ReportMetric(float64(pinHighWater), "pin_high_water")
	b.ReportMetric(float64(descriptors)/float64(b.N), "descriptors/op")
	b.ReportMetric(float64(logicalObligations)/float64(b.N), "logical_obligations/op")
}
