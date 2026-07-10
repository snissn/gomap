package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestCollectionPublicationPathsConvergeOnCoherentSnapshotPublication(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(currentFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	fset := token.NewFileSet()
	calls := make(map[string]map[string]struct{})
	var publicationMethods []string
	atomicMutations := make(map[string]map[string]struct{})
	publishSnapshotViewCallers := make(map[string]struct{})
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			fnName := fn.Name.Name
			if isDBMethod(fn) && (strings.HasPrefix(fnName, "PublishSystemRoot") || strings.HasPrefix(fnName, "PublishOrderedRoot")) {
				publicationMethods = append(publicationMethods, fnName)
			}
			if calls[fnName] == nil {
				calls[fnName] = make(map[string]struct{})
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if mutation, ok := sourceGuardAtomicMutation(call); ok {
					if atomicMutations[mutation] == nil {
						atomicMutations[mutation] = make(map[string]struct{})
					}
					atomicMutations[mutation][fnName] = struct{}{}
				}
				switch target := call.Fun.(type) {
				case *ast.Ident:
					calls[fnName][target.Name] = struct{}{}
					if target.Name == "publishSnapshotView" {
						publishSnapshotViewCallers[fnName] = struct{}{}
					}
				case *ast.SelectorExpr:
					calls[fnName][target.Sel.Name] = struct{}{}
					if target.Sel.Name == "publishSnapshotView" {
						publishSnapshotViewCallers[fnName] = struct{}{}
					}
				}
				return true
			})
		}
	}

	sort.Strings(publicationMethods)
	if len(publicationMethods) == 0 {
		t.Fatal("no production collection publication methods found")
	}
	for _, method := range publicationMethods {
		if !sourceCallGraphReaches(calls, method, "publishSnapshotView", make(map[string]bool)) {
			t.Errorf("production publication method %s does not converge on publishSnapshotView", method)
		}
	}

	wantAtomicCallers := map[string]string{
		"idx.Store":                  "closeAllIndexes,openReadOnly,openReadOnlyNoLock,openWithLock,vacuumIndexOnline",
		"snapshotViewRO.Store":       "clearSnapshotView,publishSnapshotView",
		"state.Store":                "RefreshValueLogSet,finalizeCommitLockedWithOptions,openReadOnly,openReadOnlyNoLock,publishLeafGenerationState,publishValueLogSetNoRefresh,vacuumIndexOnline",
		"state.Swap":                 "openWithLock",
		"systemRootPublishEpoch.Add": "publishSnapshotView",
	}
	for mutation := range atomicMutations {
		if _, ok := wantAtomicCallers[mutation]; !ok {
			t.Errorf("unexpected direct coherent-state mutation %s by %s", mutation, strings.Join(sortedSourceGuardKeys(atomicMutations[mutation]), ","))
		}
	}
	for mutation, want := range wantAtomicCallers {
		got := strings.Join(sortedSourceGuardKeys(atomicMutations[mutation]), ",")
		if got != want {
			t.Errorf("%s callers=%s want %s", mutation, got, want)
		}
	}
	for _, mutation := range []string{"idx.Store", "idx.Swap", "state.Store", "state.Swap"} {
		for caller := range atomicMutations[mutation] {
			if caller == "closeAllIndexes" {
				continue
			}
			if !sourceCallGraphReaches(calls, caller, "publishSnapshotView", make(map[string]bool)) {
				t.Errorf("direct %s caller %s does not converge on publishSnapshotView", mutation, caller)
			}
		}
	}
	if got, want := strings.Join(sortedSourceGuardKeys(publishSnapshotViewCallers), ","), "RefreshValueLogSet,ensureCommandWALRecoverySnapshotView,finalizeCommitLockedWithOptions,openReadOnly,openReadOnlyNoLock,openWithLock,publishLeafGenerationState,publishValueLogSetNoRefresh,vacuumIndexOnline"; got != want {
		t.Errorf("publishSnapshotView callers=%s want %s", got, want)
	}
}

func TestPublicationSourceGuardFindsRenamedReceiverBypasses(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", `package db
func (renamed *DB) bypass(gen *indexGen, state *DBState) {
	renamed.idx.Store(gen)
	renamed.state.Swap(state)
}`, 0)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	got := make(map[string]struct{})
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if mutation, ok := sourceGuardAtomicMutation(call); ok {
			got[mutation] = struct{}{}
		}
		return true
	})
	if want := map[string]struct{}{"idx.Store": {}, "state.Swap": {}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mutations=%v want %v", got, want)
	}
}

func sourceGuardAtomicMutation(call *ast.CallExpr) (string, bool) {
	if call == nil {
		return "", false
	}
	target, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	base, ok := target.X.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	switch base.Sel.Name {
	case "idx", "state", "snapshotViewRO":
		if target.Sel.Name != "Store" && target.Sel.Name != "Swap" {
			return "", false
		}
	case "systemRootPublishEpoch":
		if target.Sel.Name != "Add" {
			return "", false
		}
	default:
		return "", false
	}
	return base.Sel.Name + "." + target.Sel.Name, true
}

func sortedSourceGuardKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func isDBMethod(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	ident, ok := typ.(*ast.Ident)
	return ok && ident.Name == "DB"
}

func sourceCallGraphReaches(calls map[string]map[string]struct{}, current, target string, seen map[string]bool) bool {
	if current == target {
		return true
	}
	if seen[current] {
		return false
	}
	seen[current] = true
	for next := range calls[current] {
		if sourceCallGraphReaches(calls, next, target, seen) {
			return true
		}
	}
	return false
}
