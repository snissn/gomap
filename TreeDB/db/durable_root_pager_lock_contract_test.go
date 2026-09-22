package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDurablePagerCandidateWriteLockContract(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate contract test source")
	}
	treeDBDir := filepath.Dir(filepath.Dir(filename))

	sink := parseGoFunctionForContract(t, filepath.Join(filepath.Dir(filename), "durable_root_runtime.go"), "WriteCandidatePageV1")
	if !contractCallsSelector(sink, "freelist", "WriteCandidatePageToPagerV1") {
		t.Fatal("durable candidate sink no longer uses the opaque pager bridge")
	}

	bridge := parseGoFunctionForContract(t, filepath.Join(treeDBDir, "freelist", "generation_v1_format.go"), "WriteCandidatePageToPagerV1")
	if !contractCallsSelector(bridge, "dst", "Write") {
		t.Fatal("opaque candidate bridge no longer delegates to Pager.Write")
	}

	write := parseGoFunctionForContract(t, filepath.Join(treeDBDir, "pager", "pager.go"), "Write")
	lockPos := contractSelectorCallPosition(write, "p.mu", "Lock", false)
	deferredUnlockPos := contractSelectorCallPosition(write, "p.mu", "Unlock", true)
	copyPos := contractBuiltinCallPosition(write, "copy")
	if !lockPos.IsValid() || !deferredUnlockPos.IsValid() || !copyPos.IsValid() {
		t.Fatalf("Pager.Write lock contract incomplete: lock=%v deferred_unlock=%v copy=%v", lockPos.IsValid(), deferredUnlockPos.IsValid(), copyPos.IsValid())
	}
	if !(lockPos < deferredUnlockPos && deferredUnlockPos < copyPos) {
		t.Fatalf("Pager.Write lock contract reordered: lock=%d deferred_unlock=%d copy=%d", lockPos, deferredUnlockPos, copyPos)
	}
	if immediate := contractSelectorCallPosition(write, "p.mu", "Unlock", false); immediate.IsValid() {
		t.Fatalf("Pager.Write releases pager mutex before returning at source position %d", immediate)
	}
}

func parseGoFunctionForContract(t *testing.T, path, name string) *ast.FuncDecl {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}
	t.Fatalf("function %s not found in %s", name, path)
	return nil
}

func contractCallsSelector(function *ast.FuncDecl, receiver, method string) bool {
	found := false
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, identOK := selector.X.(*ast.Ident)
		if identOK && ident.Name == receiver && selector.Sel.Name == method {
			found = true
		}
		return !found
	})
	return found
}

func contractSelectorCallPosition(function *ast.FuncDecl, receiver, method string, deferred bool) token.Pos {
	position := token.NoPos
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall || !contractSelectorMatches(call.Fun, receiver, method) {
			return true
		}
		_, isDeferred := parentDeferCall(function.Body, call)
		if isDeferred == deferred && !position.IsValid() {
			position = call.Pos()
		}
		return true
	})
	return position
}

func parentDeferCall(root ast.Node, target *ast.CallExpr) (*ast.DeferStmt, bool) {
	var found *ast.DeferStmt
	ast.Inspect(root, func(node ast.Node) bool {
		deferStatement, ok := node.(*ast.DeferStmt)
		if ok && deferStatement.Call == target {
			found = deferStatement
			return false
		}
		return found == nil
	})
	return found, found != nil
}

func contractSelectorMatches(expression ast.Expr, receiver, method string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != method {
		return false
	}
	return contractExpressionName(selector.X) == receiver
}

func contractExpressionName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		prefix := contractExpressionName(value.X)
		if prefix == "" {
			return value.Sel.Name
		}
		return prefix + "." + value.Sel.Name
	default:
		return ""
	}
}

func contractBuiltinCallPosition(function *ast.FuncDecl, name string) token.Pos {
	position := token.NoPos
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if ok && ident.Name == name && !position.IsValid() {
			position = call.Pos()
		}
		return true
	})
	return position
}
