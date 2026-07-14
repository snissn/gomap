package authorityinventory

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestInventoryValid(t *testing.T) {
	if err := Validate(Rows); err != nil {
		t.Fatal(err)
	}
}

func TestAuthoritativeStructFamiliesAreExhaustive(t *testing.T) {
	tests := []struct {
		file, typeName, prefix string
	}{
		{"page/page.go", "ValuePtr", "page.ValuePtr."},
		{"page/child_ref.go", "LogRecordRef", "page.LogRecordRef."},
		{"collections/column_store.go", "ColumnStoreConfig", "collections.ColumnStoreConfig."},
		{"collections/column_publish_plan.go", "ColumnAssetRef", "collections.ColumnAssetRef."},
		{"db/leaf_generation_manifest.go", "leafGenerationManifest", "db.leafGenerationManifest."},
		{"db/leaf_generation_manifest.go", "leafGenerationRecord", "db.leafGenerationRecord."},
		{"page/meta.go", "MetaPageBody", "page.MetaPageBody."},
		{"internal/commitlog/command_frame.go", "ExternalRef", "commitlog.ExternalRef."},
		{"internal/commitlog/command_frame.go", "CommandExtension", "commitlog.CommandExtension."},
		{"internal/commitlog/command_frame.go", "CommandEnvelope", "commitlog.CommandEnvelope."},
		{"internal/commitlog/command_frame_v2.go", "ExternalRefFenceV1", "commitlog.ExternalRefFenceV1."},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			want := structFieldNames(t, treeDBRoot(t, tt.file), tt.typeName)
			got := inventorySuffixes(tt.prefix)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("inventory fields = %v, source fields = %v", got, want)
			}
		})
	}
}

func TestAuthorityEnumsAreExhaustive(t *testing.T) {
	tests := []struct {
		file, typeName, inventoryPrefix, constantPrefix string
	}{
		{"collections/column_publish_plan.go", "ColumnAssetKind", "collections.ColumnAssetKind.", "ColumnAssetKind"},
		{"internal/commitlog/command_frame.go", "ExternalRefClass", "commitlog.ExternalRefClass.", "ExternalRef"},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			// Enumerate the public constant family by its mandatory name prefix,
			// rather than carrying type state between const specs. The latter can
			// miss a separately-declared converted or aliased enum constant.
			want := constantNamesWithPrefix(t, treeDBRoot(t, tt.file), tt.constantPrefix)
			got := inventorySuffixes(tt.inventoryPrefix)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("inventory constants = %v, source constants = %v", got, want)
			}
		})
	}
}

func TestTextRootFamiliesAreExhaustive(t *testing.T) {
	v1 := inventorySuffixes("collections.TextV1Root.")
	v2 := inventorySuffixes("collections.TextV2Root.")
	path := treeDBRoot(t, "collections/text_index.go")
	wantV1 := returnedFunctionCalls(t, path, "collectionTextRootNames")
	wantV2 := returnedFunctionCalls(t, path, "collectionTextV2RootNames")
	if len(wantV1) != 3 || len(wantV2) != 7 {
		t.Fatalf("text root source families v1=%d v2=%d, want 3 and 7", len(wantV1), len(wantV2))
	}
	if !reflect.DeepEqual(v1, wantV1) {
		t.Fatalf("v1 roots = %v, want %v", v1, wantV1)
	}
	if !reflect.DeepEqual(v2, wantV2) {
		t.Fatalf("v2 roots = %v, want %v", v2, wantV2)
	}
	for _, name := range append(wantV1, wantV2...) {
		if !declaresFunction(t, path, name) {
			t.Fatalf("inventory names missing source function %s", name)
		}
	}
}

func TestKnownNonAuthoritySurfacesFailClosed(t *testing.T) {
	want := map[string]ActivationState{
		"commitlog.ExternalRefClass.ExternalRefValueLog":                        ActivationQuarantined,
		"commitlog.ExternalRefClass.ExternalRefLeafLog":                         ActivationQuarantined,
		"commitlog.ExternalRefClass.ExternalRefPayloadFile":                     ActivationQuarantined,
		"commitlog.ExternalRef.Path":                                            ActivationNonAuthoritative,
		"collections.ColumnAssetKind.ColumnAssetKindQueryReadyBase":             ActivationNonAuthoritative,
		"collections.ColumnAssetKind.ColumnAssetKindQueryReadyDelta":            ActivationNonAuthoritative,
		"collections.ColumnAssetKind.ColumnAssetKindQueryReadyConsolidatedBase": ActivationNonAuthoritative,
		"collections.LegacyVectorSidecar":                                       ActivationNonAuthoritative,
		"page.MetaPageBody.ActiveSlabID":                                        ActivationQuarantined,
		"page.MetaPageBody.ActiveSlabTail":                                      ActivationQuarantined,
	}
	for _, row := range Rows {
		state, ok := want[row.Field]
		if !ok {
			continue
		}
		if row.ActivationState != state {
			t.Errorf("%s state=%s, want %s", row.Field, row.ActivationState, state)
		}
		delete(want, row.Field)
	}
	if len(want) != 0 {
		t.Fatalf("missing fail-closed inventory rows: %v", want)
	}
}

func TestAuthorityFamilyActivationBoundaries(t *testing.T) {
	for _, prefix := range []string{
		"page.ValuePtr.", "page.LogRecordRef.", "db.leafGenerationManifest.",
		"db.leafGenerationRecord.", "collections.ColumnAssetRef.",
		"commitlog.CommandExtension.", "commitlog.ExternalRefFenceV1.",
	} {
		for _, row := range Rows {
			if strings.HasPrefix(row.Field, prefix) && row.ActivationState != ActivationActive {
				t.Errorf("%s state=%s, want active", row.Field, row.ActivationState)
			}
		}
	}

	wantAdjacent := map[string]string{
		"page.MetaPageBody.CommitSeq":        "#3679",
		"page.MetaPageBody.UserRootPageID":   "#3679",
		"page.MetaPageBody.SystemRootPageID": "#3679",
		"page.MetaPageBody.FreelistHeadID":   "#3678",
		"page.MetaPageBody.TotalPages":       "#3678",
		"page.MetaPageBody.LastCommitHeight": "#3679",
		"page.MetaPageBody.MaxEntryRevision": "#3679",
	}
	for _, row := range Rows {
		if strings.HasPrefix(row.Field, "collections.ColumnStoreConfig.") {
			wantIssue := "#3679"
			if row.Field == "collections.ColumnStoreConfig.RecoveryAuthoritativeAppliedCommandLSN" {
				if row.ActivationState != ActivationActive {
					t.Errorf("%s state=%s, want active", row.Field, row.ActivationState)
				}
				continue
			}
			if row.ActivationState != ActivationAdjacent || row.AdjacentIssue != wantIssue {
				t.Errorf("%s state/owner=(%s,%s), want (adjacent,%s)", row.Field, row.ActivationState, row.AdjacentIssue, wantIssue)
			}
		}
		issue, ok := wantAdjacent[row.Field]
		if !ok {
			continue
		}
		if row.ActivationState != ActivationAdjacent || row.AdjacentIssue != issue {
			t.Errorf("%s state/owner=(%s,%s), want (adjacent,%s)", row.Field, row.ActivationState, row.AdjacentIssue, issue)
		}
		delete(wantAdjacent, row.Field)
	}
	for _, field := range []string{"page.MetaPageBody.AppliedCommandLSN", "commitlog.CommandEnvelope.Version", "commitlog.CommandEnvelope.DurabilityClass", "commitlog.CommandEnvelope.LSN", "commitlog.CommandEnvelope.Kind", "commitlog.CommandEnvelope.Scope", "commitlog.CommandEnvelope.FeatureFlags", "commitlog.CommandEnvelope.CatalogEpoch", "commitlog.CommandEnvelope.SchemaEpoch", "commitlog.CommandEnvelope.BaseAppliedLSN", "commitlog.CommandEnvelope.PayloadFormat", "commitlog.CommandEnvelope.Payload", "commitlog.CommandEnvelope.Preconditions", "commitlog.CommandEnvelope.ResultAssertions"} {
		found := false
		for _, row := range Rows {
			if row.Field == field {
				found = true
				if row.ActivationState != ActivationActive {
					t.Errorf("%s state=%s, want active", row.Field, row.ActivationState)
				}
				break
			}
		}
		if !found {
			t.Errorf("missing active authority row %s", field)
		}
	}
	if len(wantAdjacent) != 0 {
		t.Fatalf("missing adjacent authority rows: %v", wantAdjacent)
	}
}

func TestGeneratedMarkdownIsCurrent(t *testing.T) {
	want := RenderMarkdown(Rows)
	path := treeDBRoot(t, "docs/spec/authority-inventory.md")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s is stale; run go generate ./TreeDB/internal/authorityinventory", path)
	}
}

func inventorySuffixes(prefix string) []string {
	var out []string
	for _, row := range Rows {
		if strings.HasPrefix(row.Field, prefix) {
			out = append(out, strings.TrimPrefix(row.Field, prefix))
		}
	}
	slices.Sort(out)
	return out
}

func treeDBRoot(t *testing.T, rel string) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(current), "..", "..", rel)
}

func parseFile(t *testing.T, path string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func structFieldNames(t *testing.T, path, typeName string) []string {
	t.Helper()
	f := parseFile(t, path)
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec := spec.(*ast.TypeSpec)
			if typeSpec.Name.Name != typeName {
				continue
			}
			st, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("%s is not a struct", typeName)
			}
			var out []string
			for _, field := range st.Fields.List {
				if len(field.Names) == 0 {
					t.Fatalf("%s contains anonymous field %T; authority inventory requires an explicit reviewed field name", typeName, field.Type)
				}
				for _, name := range field.Names {
					out = append(out, name.Name)
				}
			}
			slices.Sort(out)
			return out
		}
	}
	t.Fatalf("type %s not found in %s", typeName, path)
	return nil
}

func constantNamesWithPrefix(t *testing.T, path, prefix string) []string {
	t.Helper()
	f := parseFile(t, path)
	var out []string
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for _, name := range value.Names {
				if strings.HasPrefix(name.Name, prefix) {
					out = append(out, name.Name)
				}
			}
		}
	}
	slices.Sort(out)
	return out
}

func declaresFunction(t *testing.T, path, name string) bool {
	t.Helper()
	for _, decl := range parseFile(t, path).Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return true
		}
	}
	return false
}

func returnedFunctionCalls(t *testing.T, path, name string) []string {
	t.Helper()
	for _, decl := range parseFile(t, path).Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name {
			continue
		}
		var out []string
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok {
				out = append(out, id.Name)
			}
			return true
		})
		slices.Sort(out)
		return out
	}
	t.Fatalf("function %s not found in %s", name, path)
	return nil
}
