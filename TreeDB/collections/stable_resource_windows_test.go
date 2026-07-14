//go:build windows

package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStandaloneVectorRebuildFailsClosedBeforeAssetVisibilityOnWindows(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionUncheckedV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	before := columnAssetSegmentNamesM15C(t, d, col)
	if _, err := col.RebuildVectorIndex(def.Name); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("RebuildVectorIndex error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if after := columnAssetSegmentNamesM15C(t, d, col); !slices.Equal(after, before) {
		t.Fatalf("unsupported vector rebuild exposed segments after=%v before=%v", after, before)
	}
	if registry.ActivePins() != baselinePins || registry.ActiveIdentities() != baselineIdentities {
		t.Fatalf("unsupported vector rebuild changed registry pins=%d identities=%d want baseline pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities(), baselinePins, baselineIdentities)
	}
}

func TestColumnAssetRewriteFailsClosedBeforeCopiedVisibilityOnWindows(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("seed"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	before := columnAssetSegmentNamesM15C(t, d, col)
	if _, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{CandidateRefs: []ColumnAssetRef{candidate}}); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("ColumnAssetRewrite error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if after := columnAssetSegmentNamesM15C(t, d, col); !slices.Equal(after, before) {
		t.Fatalf("unsupported rewrite exposed segments after=%v before=%v", after, before)
	}
	if registry.ActivePins() != baselinePins || registry.ActiveIdentities() != baselineIdentities {
		t.Fatalf("unsupported rewrite changed registry pins=%d identities=%d want baseline pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities(), baselinePins, baselineIdentities)
	}
}

func TestOrdinaryColumnWriteUsesLegacyPreCutoverPathOnWindows(t *testing.T) {
	if ordinaryColumnStableAuthorityEnabled() {
		t.Fatal("ordinary stable column authority unexpectedly enabled on Windows")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	doc := []byte(`{"time_us":1,"kind":"like","did":"did:windows"}`)
	if _, err := col.Insert([]byte("windows-legacy"), doc); err != nil {
		t.Fatalf("ordinary legacy column Insert: %v", err)
	}
	got, err := col.Get([]byte("windows-legacy"))
	if err != nil {
		t.Fatalf("ordinary legacy column Get: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("ordinary legacy column Get=%s want %s", got, doc)
	}
	registry := d.StableResourceIdentityPinRegistry()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("ordinary legacy publication active stable pins=%d want 0", got)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != 0 {
		t.Fatalf("ordinary legacy publication stable namespace proofs=%d want 0", got)
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

func TestStableColumnAssetCaptureFailsClosedWithoutRelativeParentOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_parent",
		},
	}
	ref, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("must-not-become-visible"), ColumnAssetKindQueryReadyBase, 7, 11,
	)
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("stable column capture error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if token != nil || ref != (ColumnAssetRef{}) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("unsupported capture returned ref=%+v token=%v", ref, token)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(columnAssetM12ASegmentFileID))
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported capture exposed segment %q: %v", segmentPath, err)
	}
}

func TestStableColumnAppendSessionFailsClosedBeforeSegmentVisibility(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_session_parent",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	session := newColumnPhysicalAssetAppendSessionWithStableResources(dir, cfg, registry)
	refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
		payload: []byte("must-not-become-visible"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		_ = session.abort()
		t.Fatalf("stable session append error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if len(refs) != 0 {
		t.Fatalf("unsupported stable session returned refs=%+v", refs)
	}
	if err := session.abort(); err != nil {
		t.Fatal(err)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(columnAssetM12ASegmentFileID))
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported stable session exposed segment %q: %v", segmentPath, err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("unsupported stable session active identities=%d want 0", got)
	}
}
