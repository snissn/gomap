package collections

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/authorityinventory"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func requireStableColumnNamespace(t testing.TB) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("stable relative directory-handle operations are unsupported on windows")
	}
}

func TestStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t *testing.T) {
	requireStableColumnNamespace(t)
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
	ref, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("stable-column-payload"), ColumnAssetKindQueryReadyBase, 7, 11)
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
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityQueryReadyBase)
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

func TestStableColumnAssetExistingUnknownNamespaceStabilizesThroughCapturedParent(t *testing.T) {
	requireStableColumnNamespace(t)
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
		dir, cfg, []byte("stable-append"), ColumnAssetKindQueryReadyBase, 7, 11,
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

	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityQueryReadyBase)
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

func TestStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t *testing.T) {
	requireStableColumnNamespace(t)
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "creation-first", true: "creation-last"}[reverse], func(t *testing.T) {
			dir := t.TempDir()
			cfg := ColumnStoreConfig{
				Enabled: true,
				AssetManager: &ColumnAssetManagerConfig{
					Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "coalesce_resource",
				},
			}
			firstRef, first, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("first-payload"), ColumnAssetKindQueryReadyBase, 7, 11)
			if err != nil {
				t.Fatal(err)
			}
			secondRef, second, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("second-payload"), ColumnAssetKindQueryReadyBase, 7, 12)
			if err != nil {
				first.Release()
				t.Fatal(err)
			}
			if first.Namespace() == nil || second.Namespace() != nil {
				first.Release()
				second.Release()
				t.Fatalf("namespace obligations first=%v second=%v", first.Namespace() != nil, second.Namespace() != nil)
			}
			ordered := []*rootpublication.StableResourceToken{first, second}
			if reverse {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityQueryReadyBase)
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
					obligation.Reachability != rootpublication.ReachabilityQueryReadyBase || obligation.Digest == [32]byte{} {
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

func TestStableColumnAssetTokenBindsExactSegmentAndRange(t *testing.T) {
	requireStableColumnNamespace(t)
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable_resource",
		},
	}
	ref, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("stable-column-payload"), ColumnAssetKindQueryReadyBase, 7, 11)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if token.Kind() != rootpublication.ResourceQueryReadyAsset || token.Reachability() != rootpublication.ReachabilityQueryReadyBase {
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
