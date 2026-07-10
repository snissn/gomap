package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
	snapshotStores := make(map[string]struct{})
	epochAdds := make(map[string]struct{})
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
				switch target := call.Fun.(type) {
				case *ast.Ident:
					calls[fnName][target.Name] = struct{}{}
				case *ast.SelectorExpr:
					calls[fnName][target.Sel.Name] = struct{}{}
					if base, ok := target.X.(*ast.SelectorExpr); ok {
						if root, ok := base.X.(*ast.Ident); ok && root.Name == "db" && base.Sel.Name == "snapshotViewRO" && target.Sel.Name == "Store" {
							snapshotStores[fnName] = struct{}{}
						}
						if root, ok := base.X.(*ast.Ident); ok && root.Name == "db" && base.Sel.Name == "systemRootPublishEpoch" && target.Sel.Name == "Add" {
							epochAdds[fnName] = struct{}{}
						}
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
	storeCallers := sortedSourceGuardKeys(snapshotStores)
	if got, want := strings.Join(storeCallers, ","), "clearSnapshotView,publishSnapshotView"; got != want {
		t.Fatalf("snapshotViewRO.Store callers=%s want %s", got, want)
	}
	epochCallers := sortedSourceGuardKeys(epochAdds)
	if got, want := strings.Join(epochCallers, ","), "publishSnapshotView"; got != want {
		t.Fatalf("systemRootPublishEpoch.Add callers=%s want %s", got, want)
	}
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
