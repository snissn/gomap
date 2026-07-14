package collections

import (
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

	"github.com/snissn/gomap/TreeDB/internal/authorityinventory"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func testStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "captured_parent",
		},
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
	var movedDir, replacementDir string
	originalOpenParent := openStableColumnAssetParent
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		movedDir, replacementDir = path+"-moved", path
		if err := os.Rename(path, movedDir); err != nil {
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
	defer token.Release()
	segmentPath, err := columnAssetSegmentPath(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(movedDir, filepath.Base(segmentPath))); err != nil {
		t.Fatalf("captured-parent column segment: %v", err)
	}
	if _, err := os.Stat(filepath.Join(replacementDir, filepath.Base(segmentPath))); !errors.Is(err, os.ErrNotExist) {
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

	movedDir := namespace.SegmentDir + "-moved"
	originalOpenParent := openStableColumnAssetParent
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		if err := os.Rename(path, movedDir); err != nil {
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
	got, err := os.ReadFile(filepath.Join(movedDir, filepath.Base(segmentPath)))
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
	if err := os.Mkdir(namespace.SegmentDir, 0o700); err != nil {
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
	movedDir := namespace.SegmentDir + "-moved"
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
		if err := os.Rename(path, movedDir); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(path, filepath.Base(assetPath)), replacementPrefix, 0o600); err != nil {
			_ = parent.Close()
			return nil, err
		}
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
	if _, err := os.Stat(filepath.Join(movedDir, filepath.Base(assetPath))); err != nil {
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
		ColumnAssetKindTCS1PartImage:              {rootpublication.ReachabilityTypedColumnMultipart, "authoritative"},
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
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
	refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{
		{payload: []byte("row-image"), kind: ColumnAssetKindTCS1PartImage, generation: 7, partID: 1},
		{payload: []byte("dictionary-codes"), kind: ColumnAssetKindTCS1DictionaryCodes, generation: 7, partID: 2},
		{payload: []byte("hnsw-search-pack"), kind: ColumnAssetKindTCS1HNSWSearchPack, generation: 7, partID: 3},
		{payload: []byte("int64-values"), kind: ColumnAssetKindTCS1Int64Values, generation: 7, partID: 4},
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
	if len(descriptors) != 2 {
		t.Fatalf("stable descriptors=%d want typed-column and vector resource kinds", len(descriptors))
	}
	byKind := make(map[rootpublication.ResourceKind]rootpublication.StableResourceDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		byKind[descriptor.Kind()] = descriptor
	}
	typed := byKind[rootpublication.ResourceTypedColumnAsset]
	if got, want := typed.Frontier().Bytes, uint64(refs[3].Offset+refs[3].Length); got != want {
		t.Fatalf("typed-column coalesced frontier=%d want %d", got, want)
	}
	vector := byKind[rootpublication.ResourceVectorGraphPack]
	if got, want := vector.Frontier().Bytes, uint64(refs[2].Offset+refs[2].Length); got != want {
		t.Fatalf("vector frontier=%d want %d", got, want)
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
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("active kind-scoped pins=%d want 2", got)
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
	originalHook := columnAssetStableDeleteBeforeUnlink
	columnAssetStableDeleteBeforeUnlink = func() {
		if err := os.Rename(segmentPath, rotatedPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(segmentPath, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { columnAssetStableDeleteBeforeUnlink = originalHook })
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
