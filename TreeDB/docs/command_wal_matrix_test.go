package docs_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type commandWALSupportMatrix struct {
	Version  int                      `json:"version"`
	Owner    string                   `json:"owner"`
	Tracker  string                   `json:"tracker"`
	Statuses []string                 `json:"statuses"`
	Entries  []commandWALSupportEntry `json:"entries"`
}

type commandWALSupportEntry struct {
	Surface     string   `json:"surface"`
	EntryPoint  string   `json:"entry_point"`
	Command     string   `json:"command"`
	Status      string   `json:"status"`
	PublicError string   `json:"public_error,omitempty"`
	FirstPR     string   `json:"first_pr"`
	Tests       []string `json:"tests"`
}

func TestCommandWALSupportMatrixIsWellFormed(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	if matrix.Version != 1 {
		t.Fatalf("matrix version=%d, want 1", matrix.Version)
	}
	if matrix.Owner == "" || matrix.Tracker == "" {
		t.Fatalf("matrix missing owner/tracker: %+v", matrix)
	}
	expectedStatuses := []string{"WAL-supported", "WAL-rejected", "WAL-off-only", "read-only", "future"}
	if !equalStringSlices(matrix.Statuses, expectedStatuses) {
		t.Fatalf("matrix statuses=%v, want fixed v1 order %v", matrix.Statuses, expectedStatuses)
	}
	allowedStatus := stringSet(expectedStatuses)
	testSymbols := collectTreeDBTestSymbols(t)
	seen := make(map[string]struct{}, len(matrix.Entries))
	for _, entry := range matrix.Entries {
		key := entry.Surface + "\x00" + entry.EntryPoint
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate matrix entry %q/%q", entry.Surface, entry.EntryPoint)
		}
		seen[key] = struct{}{}
		if entry.Surface == "" || entry.EntryPoint == "" || entry.Command == "" || entry.Status == "" || entry.FirstPR == "" {
			t.Fatalf("incomplete matrix entry: %+v", entry)
		}
		if _, ok := allowedStatus[entry.Status]; !ok {
			t.Fatalf("%s/%s has unknown status %q", entry.Surface, entry.EntryPoint, entry.Status)
		}
		if len(entry.Tests) == 0 && entry.Status != "future" {
			t.Fatalf("%s/%s status %s needs test evidence", entry.Surface, entry.EntryPoint, entry.Status)
		}
		if entry.Status != "future" {
			for _, testName := range entry.Tests {
				if _, ok := testSymbols[testName]; !ok {
					t.Fatalf("%s/%s references missing test evidence %q", entry.Surface, entry.EntryPoint, testName)
				}
			}
		}
	}
}

func TestCommandWALSupportMatrixCoversCollectionMutators(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, entryPoint := range []string{
		"CollectionManager.CreateCollection",
		"CollectionManager.FlushAll",
		"Collection.CreateIndex",
		"Collection.DropIndex",
		"Collection.DropIndexes",
		"Collection.DropAllIndexes",
		"Collection.Flush",
		"Collection.CompactRootOverlays",
		"Collection.Insert",
		"Collection.InsertBatch",
		"Collection.InsertBatchWithTemplateV1Encoder",
		"Collection.InsertBatchValidatedBSON",
		"Collection.Delete",
		"Collection.DeleteDocument",
		"Collection.DeleteBatch",
		"Collection.Update",
		"Collection.Replace",
		"Collection.UpdateBatch",
		"Collection.UpdateBatchIfNoSecondaryUniqueIndexes",
		"Collection.UpdateBatchIfNoSecondaryUniqueIndexChanges",
		"Collection.UpdateBSONSet",
		"Collection.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges",
	} {
		requireMatrixEntry(t, matrix, "collections", entryPoint)
	}
}

func TestCommandWALSupportMatrixCoversMongoMutationHandlers(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, command := range mongoGatewayMutationMatrixEntryPoints(t) {
		requireMatrixEntry(t, matrix, "mongo_gateway", command)
	}
}

func TestCommandWALSupportMatrixCoversMongoReadOnlyCommands(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, command := range []string{"explain", "serverStatus", "top", "dbStats", "collStats"} {
		entry := requireMatrixEntry(t, matrix, "mongo_gateway", command)
		if entry.Status != "read-only" || entry.Command != "none" {
			t.Fatalf("%s entry=%+v, want read-only/none", command, entry)
		}
	}
}

func TestCommandWALSupportMatrixCoversNativeWireMutationCommands(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	registry := iwire.MustV1Registry()
	var commands []string
	for _, schema := range registry.Schemas() {
		if schema.Kind != iwire.CommandKindMutation {
			continue
		}
		commands = append(commands, nativeWireCommandName(t, schema.ID))
	}
	sort.Strings(commands)
	for _, command := range commands {
		requireMatrixEntry(t, matrix, "nativewire", command)
	}
}

func TestCommandWALSupportMatrixCoversNativeWireReadOnlyCommands(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, command := range []string{
		"CommandListCollections",
		"CommandListIndexes",
		"CommandOpenCollection",
		"CommandCloseCollection",
		"CommandGetMany",
		"CommandIndexLookup",
		"CommandIndexRange",
		"CommandOpenScan",
		"CommandCursorNext",
		"CommandCursorClose",
		"CommandExplain",
		"CommandStats",
		"CommandVectorStatus",
		"CommandVectorSearchStrict",
		"CommandVectorSearchFast",
		"CommandVectorPinSearchSnapshot",
		"CommandVectorSearchPinned",
		"CommandVectorClosePinnedSnapshot",
	} {
		entry := requireMatrixEntry(t, matrix, "nativewire", command)
		if entry.Status != "read-only" || entry.Command != "none" {
			t.Fatalf("%s entry=%+v, want read-only/none", command, entry)
		}
	}
}

func TestCommandWALSupportMatrixDocumentsRejectedCommandsWithPublicError(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, entry := range matrix.Entries {
		if entry.Status != "WAL-rejected" {
			continue
		}
		want := "ErrCommandWALRejected"
		switch entry.Surface {
		case "mongo_gateway":
			want = "MongoCommandError(BadValue)"
		case "nativewire":
			want = "WireError(ErrUnsupportedFeature)"
		}
		if entry.PublicError != want {
			t.Fatalf("%s/%s WAL-rejected public_error=%q, want %s", entry.Surface, entry.EntryPoint, entry.PublicError, want)
		}
	}
	assertFileContains(t, filepath.Join(repoRootForDocsTest(t), "TreeDB", "errors.go"), "ErrCommandWALRejected")
}

func TestCommandWALSupportMatrixRejectsUnsupportedNativeWireLocalOnlyMutations(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	entry := requireMatrixEntry(t, matrix, "nativewire", "CommandDropCollection")
	if entry.Status != "WAL-rejected" {
		t.Fatalf("CommandDropCollection status=%q, want WAL-rejected", entry.Status)
	}
	if entry.Command != "future catalog collection drop" {
		t.Fatalf("CommandDropCollection command=%q, want future catalog collection drop", entry.Command)
	}
}

func TestCommandWALSupportMatrixRejectsQueryWideUpdate(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	entry := requireMatrixEntry(t, matrix, "collections", "query-wide update/delete")
	if entry.Status != "WAL-rejected" {
		t.Fatalf("query-wide update/delete status=%q, want WAL-rejected", entry.Status)
	}
	if entry.Command != "none" {
		t.Fatalf("query-wide update/delete command=%q, want none", entry.Command)
	}
}

func TestCommandWALNoActiveCollectionWALImplementationDrift(t *testing.T) {
	_, repoRoot := repoRoots(t)
	allowed := map[string]bool{
		filepath.Join(repoRoot, "TreeDB", "internal", "collectionwal"): true,
		filepath.Join(repoRoot, "TreeDB", "docs"):                      true,
	}
	err := filepath.WalkDir(filepath.Join(repoRoot, "TreeDB"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if allowed[path] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if collectionWALSegmentNamePattern.MatchString(text) {
			t.Fatalf("%s references collection WAL segment names outside deprecated implementation", path)
		}
		if imports, selectors := collectionWALImportAndSelectors(t, path); imports {
			if violations := collectionWALSelectorViolations(selectors); len(violations) > 0 {
				t.Fatalf("%s imports internal/collectionwal with disallowed or missing guard selectors: %v", path, violations)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk TreeDB: %v", err)
	}
}

var collectionWALSegmentNamePattern = regexp.MustCompile(`collection-l[0-9]+(?:\.(?:log|ref))?`)

func loadCommandWALSupportMatrix(t *testing.T) commandWALSupportMatrix {
	t.Helper()
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "command-wal-support-matrix.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read support matrix: %v", err)
	}
	var matrix commandWALSupportMatrix
	if err := json.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("decode support matrix: %v", err)
	}
	return matrix
}

func requireMatrixEntry(t *testing.T, matrix commandWALSupportMatrix, surface, entryPoint string) commandWALSupportEntry {
	t.Helper()
	for _, entry := range matrix.Entries {
		if entry.Surface == surface && entry.EntryPoint == entryPoint {
			return entry
		}
	}
	t.Fatalf("matrix missing %s/%s", surface, entryPoint)
	return commandWALSupportEntry{}
}

func mongoGatewayMutationMatrixEntryPoints(t *testing.T) []string {
	t.Helper()
	_, repoRoot := repoRoots(t)
	path := filepath.Join(repoRoot, "TreeDB", "mongo_gateway", "server_core.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse mongo gateway dispatch: %v", err)
	}
	nonMutating := map[string]struct{}{
		"aggregate":        {},
		"buildInfo":        {},
		"connectionStatus": {},
		"count":            {},
		"distinct":         {},
		"endSessions":      {},
		"explain":          {},
		"find":             {},
		"getMore":          {},
		"getParameter":     {},
		"hello":            {},
		"hostInfo":         {},
		"isMaster":         {},
		"ismaster":         {},
		"killCursors":      {},
		"listCollections":  {},
		"listIndexes":      {},
		"ping":             {},
		"saslContinue":     {},
		"saslStart":        {},
	}
	commands := make(map[string]struct{})
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || fn.Name.Name != "dispatchCommandResponse" || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			if ident, ok := sw.Tag.(*ast.Ident); !ok || ident.Name != "name" {
				return true
			}
			for _, stmt := range sw.Body.List {
				cc, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range cc.List {
					lit, ok := expr.(*ast.BasicLit)
					if !ok {
						continue
					}
					command := strings.Trim(lit.Value, `"`)
					if _, skip := nonMutating[command]; skip {
						continue
					}
					switch command {
					case "createIndexes":
						commands["createIndexes (auto-create collection)"] = struct{}{}
						commands["createIndexes (existing collection)"] = struct{}{}
					default:
						commands[command] = struct{}{}
					}
				}
			}
			return false
		})
	}
	if len(commands) == 0 {
		t.Fatalf("no mongo gateway mutation commands derived from dispatchCommandResponse")
	}
	out := make([]string, 0, len(commands))
	for command := range commands {
		out = append(out, command)
	}
	sort.Strings(out)
	return out
}

func nativeWireCommandName(t *testing.T, id iwire.CommandID) string {
	t.Helper()
	switch id {
	case iwire.CommandCreateCollection:
		return "CommandCreateCollection"
	case iwire.CommandCreateIndex:
		return "CommandCreateIndex"
	case iwire.CommandDropIndex:
		return "CommandDropIndex"
	case iwire.CommandDropCollection:
		return "CommandDropCollection"
	case iwire.CommandInsertBatch:
		return "CommandInsertBatch"
	case iwire.CommandReplaceBatch:
		return "CommandReplaceBatch"
	case iwire.CommandUpdateBSONSet:
		return "CommandUpdateBSONSet"
	case iwire.CommandDeleteBatch:
		return "CommandDeleteBatch"
	case iwire.CommandFlushCollection:
		return "CommandFlushCollection"
	case iwire.CommandFlushAll:
		return "CommandFlushAll"
	case iwire.CommandCheckpoint:
		return "CommandCheckpoint"
	default:
		t.Fatalf("native-wire mutation command %d needs a matrix name mapping", id)
		return ""
	}
}

func collectionWALImportAndSelectors(t *testing.T, path string) (bool, map[string]struct{}) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	collectionWALNames := make(map[string]struct{})
	for _, spec := range file.Imports {
		if spec.Path != nil && spec.Path.Value == `"github.com/snissn/gomap/TreeDB/internal/collectionwal"` {
			name := "collectionwal"
			if spec.Name != nil {
				name = spec.Name.Name
				if name == "." || name == "_" {
					t.Fatalf("%s imports internal/collectionwal with unsupported %q import; use a named import so guard selectors remain auditable", path, name)
				}
			}
			collectionWALNames[name] = struct{}{}
		}
	}
	if len(collectionWALNames) == 0 {
		return false, nil
	}
	selectors := make(map[string]struct{})
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if ok {
			if _, imported := collectionWALNames[ident.Name]; imported {
				selectors[sel.Sel.Name] = struct{}{}
			}
		}
		return true
	})
	return true, selectors
}

func collectionWALSelectorViolations(selectors map[string]struct{}) []string {
	allowed := map[string]struct{}{
		"ErrCollectionWALRecoveryRequired":  {},
		"RequireCleanForOfflineMaintenance": {},
		"RequireCleanForReadOnlyOpen":       {},
	}
	hasAllowed := false
	var violations []string
	for name := range selectors {
		if _, ok := allowed[name]; ok {
			hasAllowed = true
		} else {
			violations = append(violations, name)
		}
	}
	if !hasAllowed {
		violations = append(violations, "<missing approved legacy-clean guard selector>")
	}
	sort.Strings(violations)
	return violations
}

func repoRootForDocsTest(t *testing.T) string {
	t.Helper()
	_, repoRoot := repoRoots(t)
	return repoRoot
}

func assertFileContains(t *testing.T, path, substr string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(raw), substr) {
		t.Fatalf("%s missing %q", path, substr)
	}
}

func collectTreeDBTestSymbols(t *testing.T) map[string]struct{} {
	t.Helper()
	_, repoRoot := repoRoots(t)
	symbols := make(map[string]struct{})
	err := filepath.WalkDir(filepath.Join(repoRoot, "TreeDB"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			symbols[fn.Name.Name] = struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("collect TreeDB test symbols: %v", err)
	}
	return symbols
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
