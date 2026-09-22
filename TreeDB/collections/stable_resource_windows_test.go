//go:build windows

package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestColumnAssetRewriteUsesCreateOnlyStablePathOnWindows(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	before := columnAssetSegmentNamesM15C(t, d, col)
	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if stats.SegmentsRewritten != 1 || stats.RefsRemapped == 0 || len(stats.SupersededRefs) != stats.RefsRemapped {
		t.Fatalf("ColumnAssetRewrite stats=%+v want one rewritten segment and matched remap", stats)
	}
	after := columnAssetSegmentNamesM15C(t, d, col)
	if len(after) != len(before)+1 {
		t.Fatalf("create-only rewrite segments after=%v before=%v want one fresh segment", after, before)
	}
	if stats.RemapSegmentFileID == 0 || stats.RemapSegmentFileID == candidate.FileID {
		t.Fatalf("create-only rewrite remap file_id=%d source=%d", stats.RemapSegmentFileID, candidate.FileID)
	}
	sourcePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("create-only rewrite removed source %q: %v", sourcePath, err)
	}
	if registry.ActivePins() <= baselinePins || registry.ActiveIdentities() <= baselineIdentities {
		t.Fatalf("create-only rewrite retained pins=%d identities=%d want above baseline pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities(), baselinePins, baselineIdentities)
	}
}

func TestOrdinaryColumnWriteUsesCreateOnlyStablePathOnWindows(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Fatal("ordinary stable column create authority unexpectedly disabled on Windows")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineLinks := registry.ActiveStableNamespaceLinks()
	doc := []byte(`{"time_us":1,"kind":"like","did":"did:windows"}`)
	if _, err := col.Insert([]byte("windows-stable"), doc); err != nil {
		t.Fatalf("ordinary stable column Insert: %v", err)
	}
	got, err := col.Get([]byte("windows-stable"))
	if err != nil {
		t.Fatalf("ordinary stable column Get: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("ordinary stable column Get=%s want %s", got, doc)
	}
	if got := registry.ActivePins(); got <= baselinePins {
		t.Fatalf("ordinary stable publication active pins=%d want > baseline %d while durable roots retain authority", got, baselinePins)
	}
	if got := registry.ActiveStableNamespaceLinks(); got <= baselineLinks {
		t.Fatalf("ordinary stable publication namespace proofs=%d want > baseline %d", got, baselineLinks)
	}
	if got := registry.CachedStableDirectoryLinks(); got == 0 {
		t.Fatal("ordinary stable publication retained no directory ancestry authority")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close ordinary stable column DB: %v", err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("ordinary stable publication active pins after close=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("ordinary stable publication active identities after close=%d want 0", got)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != 0 {
		t.Fatalf("ordinary stable publication namespace proofs after close=%d want 0", got)
	}
	if got := registry.CachedStableDirectoryLinks(); got != 0 {
		t.Fatalf("ordinary stable publication directory ancestry proofs after close=%d want 0", got)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCollection, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("open ordinary stable collection after reopen: %v", err)
	}
	got, err = reopenedCollection.Get([]byte("windows-stable"))
	if err != nil {
		t.Fatalf("ordinary stable column Get after reopen: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("ordinary stable column Get after reopen=%s want %s", got, doc)
	}
}

func TestColumnAssetGCDestructiveFailsClosedOnWindowsBeforeUnlink(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	payload := []byte("windows-gc-candidate")
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 119, payload)
	segmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed: true, CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("destructive GC error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("destructive unsupported GC stats=%+v want eligible untouched", stats)
	}
	got, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unsupported GC candidate=%q want %q", got, payload)
	}
}

func TestStableColumnAssetCaptureUsesCreateOnlyWindowsEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable_parent_chain",
		},
	}
	payload := []byte("windows-stable-column")
	ref, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, payload, ColumnAssetKindTCS1TypedColumnPart, 7, 11,
	)
	if err != nil {
		t.Fatalf("stable column capture: %v", err)
	}
	if token == nil {
		t.Fatal("stable column capture returned nil authority")
	}
	defer token.Release()
	segmentPath, err := columnAssetSegmentPath(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	segment, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	end := ref.Offset + ref.Length
	if ref.Offset < 0 || end < ref.Offset || end > int64(len(segment)) {
		t.Fatalf("stable column ref=%+v segment bytes=%d", ref, len(segment))
	}
	if got := segment[ref.Offset:end]; !bytes.Equal(got, payload) {
		t.Fatalf("stable column bytes=%q want %q", got, payload)
	}
}

func TestStableColumnAssetCaptureStabilizesExistingParentChainOnWindows(t *testing.T) {
	dir := t.TempDir()
	cfg := stableColumnAppendTestConfig("existing_parent_chain")
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatal(err)
	}
	ref, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("existing-chain"), ColumnAssetKindTCS1TypedColumnPart, 7, 11,
	)
	if err != nil {
		t.Fatalf("stable existing-chain capture: %v", err)
	}
	if token == nil {
		t.Fatal("stable existing-chain capture returned nil authority")
	}
	token.Release()
	if ref.FileID == 0 {
		t.Fatalf("stable existing-chain ref=%+v", ref)
	}
}

func TestStableColumnAppendSessionUsesCreateOnlyWindowsEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable_session_parent",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	session := newColumnPhysicalAssetAppendSessionWithStableResources(dir, cfg, registry)
	refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
		payload: []byte("windows-stable-session"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	if err != nil {
		_ = session.abort()
		t.Fatalf("stable session append: %v", err)
	}
	if len(refs) != 1 {
		_ = session.abort()
		t.Fatalf("stable session refs=%+v want one", refs)
	}
	closeStats, resources, err := session.closeWithStableResources()
	if err != nil {
		t.Fatalf("close stable session: %v", err)
	}
	if resources == nil {
		t.Fatal("stable session returned nil authority")
	}
	if closeStats.FileSyncCount != 1 || closeStats.SyncEpochCount != 1 {
		resources.Release()
		t.Fatalf("stable session close stats=%+v want one sync epoch", closeStats)
	}
	resources.Release()
	segmentPath, err := columnAssetSegmentPath(dir, refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("stable session segment %q: %v", segmentPath, err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("released stable session active identities=%d want 0", got)
	}
}

func TestStableFreshColumnAssetSegmentUsesCreateOnlyWindowsEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "windows_fresh_segment",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(dir, cfg, registry)
	if err != nil {
		t.Fatalf("stable fresh segment: %v", err)
	}
	refs, err := appender.appendKinds([]columnPhysicalAssetAppendItem{{
		payload: []byte("windows-stable-fresh"), kind: ColumnAssetKindTCS1TypedColumnPart,
		generation: 1, partID: 1,
	}})
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append stable fresh segment: %v", err)
	}
	if len(refs) != 1 {
		_ = appender.abort()
		t.Fatalf("stable fresh segment refs=%+v want one", refs)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("close stable fresh segment: %v", err)
	}
	resources := appender.stableResources
	appender.stableResources = nil
	if resources == nil {
		t.Fatal("stable fresh segment returned nil authority")
	}
	var namespaceSyncs uint64
	for _, stats := range resources.Stats(time.Now()) {
		namespaceSyncs += stats.NamespaceSyncs
	}
	if namespaceSyncs != 1 {
		resources.Release()
		t.Fatalf("stable fresh segment namespace syncs=%d want 1", namespaceSyncs)
	}
	resources.Release()
	segmentPath, err := columnAssetSegmentPath(dir, refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("stable fresh segment %q: %v", segmentPath, err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("released stable fresh segment active identities=%d want 0", got)
	}
}

func TestColumnVectorGraphStableAuthorityUsesCreateOnlyWindowsEvidence(t *testing.T) {
	registry := rootpublication.NewIdentityPinRegistry()
	authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
	if err != nil {
		t.Fatalf("create-only vector stable authority: %v", err)
	}
	authority.abandon()
}
