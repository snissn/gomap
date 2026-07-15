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

func TestStableColumnAssetCaptureUsesCreateOnlyWindowsEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_parent",
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

func TestStableColumnAppendSessionUsesCreateOnlyWindowsEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_session_parent",
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

func TestStableFreshColumnAssetSegmentFailsClosedBeforeVisibility(t *testing.T) {
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
