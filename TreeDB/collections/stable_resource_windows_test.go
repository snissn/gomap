//go:build windows

package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func prepareUnprovenColumnAssetNamespaceOnWindows(t *testing.T, rootDir, namespaceName string) columnAssetManagerNamespace {
	t.Helper()
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, namespaceName)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("prepare unproven column asset namespace: %v", err)
	}
	return namespace
}

func TestStableColumnAssetCreateRejectsFreshUnstableParentsOnWindows(t *testing.T) {
	dir := t.TempDir()
	cfg := stableColumnAppendTestConfig("fresh_unstable_parent")
	_, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("windows-stable-column"), ColumnAssetKindTCS1TypedColumnPart, 7, 11,
	)
	if token != nil {
		token.Release()
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("stable first write error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	namespace, namespaceErr := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if namespaceErr != nil {
		t.Fatal(namespaceErr)
	}
	if _, statErr := os.Stat(namespace.ManagerRootDir); !os.IsNotExist(statErr) {
		t.Fatalf("stable first write manager root stat=%v want not-exist", statErr)
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

func TestOrdinaryColumnWriteUsesLegacyPreActivationPathAndReopensOnWindows(t *testing.T) {
	if ordinaryColumnStableAuthorityEnabled() {
		t.Fatal("ordinary stable column authority unexpectedly enabled on Windows")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	registry := d.StableResourceIdentityPinRegistry()
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
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("ordinary legacy publication active stable pins=%d want 0", got)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != 0 {
		t.Fatalf("ordinary legacy publication stable namespace proofs=%d want 0", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close ordinary legacy column DB: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCollection, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("open ordinary legacy collection after reopen: %v", err)
	}
	got, err = reopenedCollection.Get([]byte("windows-legacy"))
	if err != nil {
		t.Fatalf("ordinary legacy column Get after reopen: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("ordinary legacy column Get after reopen=%s want %s", got, doc)
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

func TestStableColumnAssetCaptureRejectsUnprovenExistingParentsOnWindows(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_parent",
		},
	}
	namespace := prepareUnprovenColumnAssetNamespaceOnWindows(t, dir, cfg.AssetManager.Namespace)
	payload := []byte("windows-stable-column")
	_, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, payload, ColumnAssetKindTCS1TypedColumnPart, 7, 11,
	)
	if token != nil {
		token.Release()
		t.Fatal("unsupported stable column capture returned authority")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("stable column capture error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	entries, err := os.ReadDir(namespace.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported stable column capture exposed entries=%v", entries)
	}
}

func TestStableColumnAppendSessionRejectsUnprovenExistingParentsOnWindows(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_session_parent",
	}}
	namespace := prepareUnprovenColumnAssetNamespaceOnWindows(t, dir, cfg.AssetManager.Namespace)
	registry := rootpublication.NewIdentityPinRegistry()
	session := newColumnPhysicalAssetAppendSessionWithStableResources(dir, cfg, registry)
	_, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
		payload: []byte("windows-stable-session"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	_ = session.abort()
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("stable session append error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	entries, err := os.ReadDir(namespace.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported stable session exposed entries=%v", entries)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("unsupported stable session active identities=%d want 0", got)
	}
}

func TestStableFreshColumnAssetSegmentFailsClosedBeforeVisibilityOnWindows(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_fresh_segment",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(dir, cfg, registry)
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		if appender != nil {
			_ = appender.abort()
		}
		t.Fatalf("stable fresh segment error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if appender != nil {
		_ = appender.abort()
		t.Fatalf("unsupported stable fresh segment returned appender=%v", appender)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported stable fresh segment exposed entries=%v", entries)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("unsupported stable fresh segment active identities=%d want 0", got)
	}
}
