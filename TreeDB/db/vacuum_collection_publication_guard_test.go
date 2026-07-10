package db

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

const publicationGuardPackagePath = "github.com/snissn/gomap/TreeDB/db"

type publicationGuardSource struct {
	fset  *token.FileSet
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
}

type publicationGuardMutation struct {
	slot   string
	method string
	caller *types.Func
	pos    token.Pos
}

type publicationGuardEscape struct {
	slot   string
	caller *types.Func
	pos    token.Pos
	detail string
}

type publicationGuardAnalysis struct {
	source              *publicationGuardSource
	calls               map[*types.Func]map[*types.Func]struct{}
	mutations           []publicationGuardMutation
	escapes             []publicationGuardEscape
	epochMutations      map[*types.Func]struct{}
	publicationMethods  []*types.Func
	publishSnapshotView *types.Func
}

func TestCollectionPublicationPathsConvergeOnCoherentSnapshotPublication(t *testing.T) {
	source := loadProductionPublicationGuardSource(t)
	analysis := analyzePublicationGuard(t, source)

	wantAtomicCallers := map[string][]string{
		"idx.Store": {
			publicationGuardDBMethodID("closeAllIndexes"),
			publicationGuardPackageFuncID("openReadOnly"),
			publicationGuardPackageFuncID("openReadOnlyNoLock"),
			publicationGuardPackageFuncID("openWithLock"),
			publicationGuardDBMethodID("vacuumIndexOnline"),
		},
		"snapshotViewRO.Store": {
			publicationGuardDBMethodID("clearSnapshotView"),
			publicationGuardDBMethodID("publishSnapshotView"),
		},
		"state.Store": {
			publicationGuardDBMethodID("RefreshValueLogSet"),
			publicationGuardDBMethodID("finalizeCommitLockedWithOptions"),
			publicationGuardPackageFuncID("openReadOnly"),
			publicationGuardPackageFuncID("openReadOnlyNoLock"),
			publicationGuardDBMethodID("publishLeafGenerationState"),
			publicationGuardDBMethodID("publishValueLogSetNoRefresh"),
			publicationGuardDBMethodID("vacuumIndexOnline"),
		},
		"state.CompareAndSwap": {publicationGuardDBMethodID("ensureCommandWALRecoverySnapshotView")},
		"state.Swap":           {publicationGuardPackageFuncID("openWithLock")},
	}

	gotAtomicCallers := make(map[string]map[string]struct{})
	for _, mutation := range analysis.mutations {
		key := mutation.slot + "." + mutation.method
		if gotAtomicCallers[key] == nil {
			gotAtomicCallers[key] = make(map[string]struct{})
		}
		gotAtomicCallers[key][publicationGuardFuncID(mutation.caller)] = struct{}{}
	}
	for key, callers := range gotAtomicCallers {
		if _, ok := wantAtomicCallers[key]; !ok {
			t.Errorf("unexpected coherent atomic mutation %s by %s", key, strings.Join(sortedPublicationGuardKeys(callers), ","))
		}
	}
	for key, want := range wantAtomicCallers {
		sort.Strings(want)
		got := sortedPublicationGuardKeys(gotAtomicCallers[key])
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s callers=%s want %s", key, strings.Join(got, ","), strings.Join(want, ","))
		}
	}
	for _, escape := range analysis.escapes {
		t.Errorf("coherent publication slot %s escapes typed analysis in %s: %s", escape.slot, publicationGuardFuncID(escape.caller), escape.detail)
	}

	if analysis.publishSnapshotView == nil {
		t.Fatal("typed guard did not find (*DB).publishSnapshotView")
	}
	sort.Slice(analysis.publicationMethods, func(i, j int) bool {
		return publicationGuardFuncID(analysis.publicationMethods[i]) < publicationGuardFuncID(analysis.publicationMethods[j])
	})
	if len(analysis.publicationMethods) == 0 {
		t.Fatal("no production collection publication methods found")
	}
	for _, method := range analysis.publicationMethods {
		if !publicationGuardCallGraphReaches(analysis.calls, method, analysis.publishSnapshotView, make(map[*types.Func]bool)) {
			t.Errorf("production publication method %s does not converge on %s", publicationGuardFuncID(method), publicationGuardFuncID(analysis.publishSnapshotView))
		}
	}

	for _, mutation := range analysis.mutations {
		if mutation.slot != "idx" && mutation.slot != "state" {
			continue
		}
		if publicationGuardFuncID(mutation.caller) == publicationGuardDBMethodID("closeAllIndexes") {
			continue
		}
		if !publicationGuardCallGraphReaches(analysis.calls, mutation.caller, analysis.publishSnapshotView, make(map[*types.Func]bool)) {
			t.Errorf("direct %s.%s caller %s does not converge on %s", mutation.slot, mutation.method, publicationGuardFuncID(mutation.caller), publicationGuardFuncID(analysis.publishSnapshotView))
		}
	}

	wantPublishCallers := []string{
		publicationGuardDBMethodID("RefreshValueLogSet"),
		publicationGuardDBMethodID("ensureCommandWALRecoverySnapshotView"),
		publicationGuardDBMethodID("finalizeCommitLockedWithOptions"),
		publicationGuardPackageFuncID("openReadOnly"),
		publicationGuardPackageFuncID("openReadOnlyNoLock"),
		publicationGuardPackageFuncID("openWithLock"),
		publicationGuardDBMethodID("publishLeafGenerationState"),
		publicationGuardDBMethodID("publishValueLogSetNoRefresh"),
		publicationGuardDBMethodID("vacuumIndexOnline"),
	}
	sort.Strings(wantPublishCallers)
	var gotPublishCallers []string
	for caller, callees := range analysis.calls {
		if _, ok := callees[analysis.publishSnapshotView]; ok {
			gotPublishCallers = append(gotPublishCallers, publicationGuardFuncID(caller))
		}
	}
	sort.Strings(gotPublishCallers)
	if strings.Join(gotPublishCallers, ",") != strings.Join(wantPublishCallers, ",") {
		t.Errorf("publishSnapshotView callers=%s want %s", strings.Join(gotPublishCallers, ","), strings.Join(wantPublishCallers, ","))
	}

	gotEpochCallers := make(map[string]struct{})
	for caller := range analysis.epochMutations {
		gotEpochCallers[publicationGuardFuncID(caller)] = struct{}{}
	}
	wantEpochCaller := publicationGuardDBMethodID("publishSnapshotView")
	if got := strings.Join(sortedPublicationGuardKeys(gotEpochCallers), ","); got != wantEpochCaller {
		t.Errorf("systemRootPublishEpoch.Add callers=%s want %s", got, wantEpochCaller)
	}
}

func TestPublicationSourceGuardRejectsTypedBypassFixtures(t *testing.T) {
	fixtures := map[string]struct {
		body      string
		wantKind  string
		collision bool
	}{
		"pointer alias": {
			body: `func (db *DB) bypass(gen *indexGen) {
	slot := &db.idx
	slot.Store(gen)
}`,
			wantKind: "idx.Store",
		},
		"typed helper": {
			body: `func mutate(slot *atomic.Pointer[DBState], state *DBState) { slot.Store(state) }
func (db *DB) bypass(state *DBState) { mutate(&db.state, state) }`,
			wantKind: "state.Store",
		},
		"method value alias": {
			body: `func (db *DB) bypass(state *DBState) {
	store := db.state.Store
	store(state)
}`,
			wantKind: "state.Store",
		},
		"compare and swap": {
			body: `func (db *DB) bypass(old, next *snapshotView) {
	db.snapshotViewRO.CompareAndSwap(old, next)
}`,
			wantKind: "snapshotViewRO.CompareAndSwap",
		},
		"swap": {
			body: `func (db *DB) bypass(next *DBState) {
	db.state.Swap(next)
}`,
			wantKind: "state.Swap",
		},
		"direct assignment": {
			body: `func (db *DB) bypass(next atomic.Pointer[DBState]) {
	db.state = next
}`,
			wantKind: "state.Assign",
		},
		"interface helper": {
			body: `type stateMutator interface { Store(*DBState) }
func mutate(m stateMutator, state *DBState) { m.Store(state) }
func (db *DB) bypass(state *DBState) { mutate(&db.state, state) }`,
			wantKind: "state escape",
		},
		"generic helper": {
			body: `func mutate[T any](slot *atomic.Pointer[T], value *T) { slot.Store(value) }
func (db *DB) bypass(state *DBState) { mutate(&db.state, state) }`,
			wantKind: "state escape",
		},
		"receiver collision": {
			body: `type collision struct{}
func (*collision) publishSnapshotView() {}
func (db *DB) PublishOrderedRootBypass(state *DBState) {
	db.state.Store(state)
	(&collision{}).publishSnapshotView()
}`,
			wantKind:  "state.Store",
			collision: true,
		},
	}

	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			source := loadPublicationGuardFixture(t, fixture.body)
			analysis := analyzePublicationGuard(t, source)
			var findings []string
			for _, mutation := range analysis.mutations {
				findings = append(findings, mutation.slot+"."+mutation.method)
			}
			for _, escape := range analysis.escapes {
				findings = append(findings, escape.slot+" escape")
			}
			sort.Strings(findings)
			if !containsPublicationGuardFinding(findings, fixture.wantKind) {
				t.Fatalf("typed guard findings=%v, want %q", findings, fixture.wantKind)
			}
			if fixture.collision {
				if len(analysis.publicationMethods) != 1 || analysis.publishSnapshotView == nil {
					t.Fatalf("collision fixture publication methods=%d publishSnapshotView=%v", len(analysis.publicationMethods), analysis.publishSnapshotView)
				}
				if publicationGuardCallGraphReaches(analysis.calls, analysis.publicationMethods[0], analysis.publishSnapshotView, make(map[*types.Func]bool)) {
					t.Fatal("same-name method on another receiver satisfied DB publication convergence")
				}
			}
		})
	}
}

func analyzePublicationGuard(t *testing.T, source *publicationGuardSource) *publicationGuardAnalysis {
	t.Helper()
	analysis := &publicationGuardAnalysis{
		source:         source,
		calls:          make(map[*types.Func]map[*types.Func]struct{}),
		epochMutations: make(map[*types.Func]struct{}),
	}
	targetTypes := map[string]types.Type{
		"idx":            publicationGuardNamedType(t, source.pkg, "indexGen"),
		"state":          publicationGuardNamedType(t, source.pkg, "DBState"),
		"snapshotViewRO": publicationGuardNamedType(t, source.pkg, "snapshotView"),
	}
	dbType := publicationGuardNamedType(t, source.pkg, "DB")
	epochField := publicationGuardStructField(t, dbType, "systemRootPublishEpoch")

	for _, file := range source.files {
		for _, decl := range file.Decls {
			fnDecl, ok := decl.(*ast.FuncDecl)
			if !ok || fnDecl.Body == nil {
				continue
			}
			caller, _ := source.info.Defs[fnDecl.Name].(*types.Func)
			if caller == nil {
				t.Fatalf("missing typed function object for %s", fnDecl.Name.Name)
			}
			if publicationGuardIsDBMethod(caller, dbType) {
				if caller.Name() == "publishSnapshotView" {
					analysis.publishSnapshotView = caller
				}
				if strings.HasPrefix(caller.Name(), "PublishSystemRoot") || strings.HasPrefix(caller.Name(), "PublishOrderedRoot") {
					analysis.publicationMethods = append(analysis.publicationMethods, caller)
				}
			}

			ast.Inspect(fnDecl.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.SelectorExpr:
					if slot, method, ok := publicationGuardAtomicMutation(n, source.info, targetTypes); ok {
						analysis.mutations = append(analysis.mutations, publicationGuardMutation{slot: slot, method: method, caller: caller, pos: n.Pos()})
					}
					if publicationGuardEpochAdd(n, source.info, epochField) {
						analysis.epochMutations[caller] = struct{}{}
					}
				case *ast.CallExpr:
					if callee := publicationGuardCalledFunc(n.Fun, source.info); callee != nil {
						if analysis.calls[caller] == nil {
							analysis.calls[caller] = make(map[*types.Func]struct{})
						}
						analysis.calls[caller][callee] = struct{}{}
					}
					analysis.escapes = append(analysis.escapes, publicationGuardCallEscapes(n, caller, source.info, targetTypes)...)
				case *ast.AssignStmt:
					analysis.mutations = append(analysis.mutations, publicationGuardAssignmentMutations(n, caller, source.info, targetTypes)...)
					analysis.escapes = append(analysis.escapes, publicationGuardAssignmentEscapes(n, caller, source.info, targetTypes)...)
				case *ast.ReturnStmt:
					analysis.escapes = append(analysis.escapes, publicationGuardReturnEscapes(n, caller, source.info, targetTypes)...)
				case *ast.SendStmt:
					if slot, ok := publicationGuardAtomicPointerSlot(source.info.TypeOf(n.Value), targetTypes); ok {
						analysis.escapes = append(analysis.escapes, publicationGuardEscape{slot: slot, caller: caller, pos: n.Pos(), detail: "sent through channel"})
					}
				}
				return true
			})
		}
	}
	return analysis
}

func publicationGuardAtomicMutation(sel *ast.SelectorExpr, info *types.Info, targets map[string]types.Type) (string, string, bool) {
	selection := info.Selections[sel]
	if selection == nil {
		return "", "", false
	}
	method := selection.Obj().Name()
	if method != "Store" && method != "Swap" && method != "CompareAndSwap" {
		return "", "", false
	}
	slot, ok := publicationGuardAtomicValueSlot(info.TypeOf(sel.X), targets)
	return slot, method, ok
}

func publicationGuardAtomicValueSlot(typ types.Type, targets map[string]types.Type) (string, bool) {
	typ = types.Unalias(typ)
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(ptr.Elem())
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "sync/atomic" || named.Obj().Name() != "Pointer" {
		return "", false
	}
	args := named.TypeArgs()
	if args == nil || args.Len() != 1 {
		return "", false
	}
	for slot, target := range targets {
		if types.Identical(types.Unalias(args.At(0)), types.Unalias(target)) {
			return slot, true
		}
	}
	return "", false
}

func publicationGuardAtomicPointerSlot(typ types.Type, targets map[string]types.Type) (string, bool) {
	typ = types.Unalias(typ)
	ptr, ok := typ.(*types.Pointer)
	if !ok {
		return "", false
	}
	return publicationGuardAtomicValueSlot(ptr.Elem(), targets)
}

func publicationGuardCallEscapes(call *ast.CallExpr, caller *types.Func, info *types.Info, targets map[string]types.Type) []publicationGuardEscape {
	var escapes []publicationGuardEscape
	if tv, ok := info.Types[call.Fun]; ok && tv.IsType() {
		if len(call.Args) == 1 {
			if slot, ok := publicationGuardAtomicPointerSlot(info.TypeOf(call.Args[0]), targets); ok && !types.Identical(info.TypeOf(call.Args[0]), info.TypeOf(call)) {
				escapes = append(escapes, publicationGuardEscape{slot: slot, caller: caller, pos: call.Pos(), detail: "converted to " + types.TypeString(info.TypeOf(call), publicationGuardQualifier)})
			}
		}
		return escapes
	}
	sig, _ := types.Unalias(info.TypeOf(call.Fun)).Underlying().(*types.Signature)
	if sig == nil {
		return escapes
	}
	callee := publicationGuardCalledFunc(call.Fun, info)
	for i, arg := range call.Args {
		slot, ok := publicationGuardAtomicPointerSlot(info.TypeOf(arg), targets)
		if !ok {
			continue
		}
		param := publicationGuardCallParam(sig, i)
		genericCallee := callee != nil && callee.Type().(*types.Signature).TypeParams() != nil && callee.Type().(*types.Signature).TypeParams().Len() > 0
		if param == nil || !types.Identical(info.TypeOf(arg), param) || genericCallee {
			detail := "passed to non-identical parameter"
			if genericCallee {
				detail = "passed through generic helper"
			}
			escapes = append(escapes, publicationGuardEscape{slot: slot, caller: caller, pos: arg.Pos(), detail: detail})
		}
	}
	return escapes
}

func publicationGuardCallParam(sig *types.Signature, arg int) types.Type {
	params := sig.Params()
	if params == nil || params.Len() == 0 {
		return nil
	}
	if !sig.Variadic() {
		if arg >= params.Len() {
			return nil
		}
		return params.At(arg).Type()
	}
	if arg < params.Len()-1 {
		return params.At(arg).Type()
	}
	slice, _ := types.Unalias(params.At(params.Len() - 1).Type()).(*types.Slice)
	if slice == nil {
		return nil
	}
	return slice.Elem()
}

func publicationGuardAssignmentMutations(assign *ast.AssignStmt, caller *types.Func, info *types.Info, targets map[string]types.Type) []publicationGuardMutation {
	var mutations []publicationGuardMutation
	for _, lhs := range assign.Lhs {
		if _, ok := lhs.(*ast.Ident); ok && assign.Tok == token.DEFINE {
			continue
		}
		if slot, ok := publicationGuardAtomicValueSlot(info.TypeOf(lhs), targets); ok {
			mutations = append(mutations, publicationGuardMutation{slot: slot, method: "Assign", caller: caller, pos: lhs.Pos()})
		}
	}
	return mutations
}

func publicationGuardAssignmentEscapes(assign *ast.AssignStmt, caller *types.Func, info *types.Info, targets map[string]types.Type) []publicationGuardEscape {
	var escapes []publicationGuardEscape
	for i, rhs := range assign.Rhs {
		slot, ok := publicationGuardAtomicPointerSlot(info.TypeOf(rhs), targets)
		if !ok {
			continue
		}
		if len(assign.Lhs) != len(assign.Rhs) || i >= len(assign.Lhs) {
			escapes = append(escapes, publicationGuardEscape{slot: slot, caller: caller, pos: rhs.Pos(), detail: "assigned through multi-valued expression"})
			continue
		}
		if !types.Identical(info.TypeOf(rhs), info.TypeOf(assign.Lhs[i])) {
			escapes = append(escapes, publicationGuardEscape{slot: slot, caller: caller, pos: rhs.Pos(), detail: "assigned to " + types.TypeString(info.TypeOf(assign.Lhs[i]), publicationGuardQualifier)})
		}
	}
	return escapes
}

func publicationGuardReturnEscapes(ret *ast.ReturnStmt, caller *types.Func, info *types.Info, targets map[string]types.Type) []publicationGuardEscape {
	sig, _ := caller.Type().(*types.Signature)
	if sig == nil || sig.Results() == nil {
		return nil
	}
	var escapes []publicationGuardEscape
	for i, result := range ret.Results {
		slot, ok := publicationGuardAtomicPointerSlot(info.TypeOf(result), targets)
		if !ok {
			continue
		}
		if i >= sig.Results().Len() || !types.Identical(info.TypeOf(result), sig.Results().At(i).Type()) {
			escapes = append(escapes, publicationGuardEscape{slot: slot, caller: caller, pos: result.Pos(), detail: "returned through non-identical type"})
		}
	}
	return escapes
}

func publicationGuardEpochAdd(sel *ast.SelectorExpr, info *types.Info, epochField *types.Var) bool {
	selection := info.Selections[sel]
	if selection == nil || selection.Obj().Name() != "Add" {
		return false
	}
	receiver, ok := sel.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	fieldSelection := info.Selections[receiver]
	return fieldSelection != nil && fieldSelection.Obj() == epochField
}

func publicationGuardCalledFunc(expr ast.Expr, info *types.Info) *types.Func {
	switch expr := expr.(type) {
	case *ast.ParenExpr:
		return publicationGuardCalledFunc(expr.X, info)
	case *ast.IndexExpr:
		return publicationGuardCalledFunc(expr.X, info)
	case *ast.IndexListExpr:
		return publicationGuardCalledFunc(expr.X, info)
	case *ast.Ident:
		fn, _ := info.Uses[expr].(*types.Func)
		return fn
	case *ast.SelectorExpr:
		if selection := info.Selections[expr]; selection != nil {
			fn, _ := selection.Obj().(*types.Func)
			return fn
		}
		fn, _ := info.Uses[expr.Sel].(*types.Func)
		return fn
	default:
		return nil
	}
}

func publicationGuardCallGraphReaches(calls map[*types.Func]map[*types.Func]struct{}, current, target *types.Func, seen map[*types.Func]bool) bool {
	if current == target {
		return true
	}
	if current == nil || seen[current] {
		return false
	}
	seen[current] = true
	for next := range calls[current] {
		if publicationGuardCallGraphReaches(calls, next, target, seen) {
			return true
		}
	}
	return false
}

func loadProductionPublicationGuardSource(t *testing.T) *publicationGuardSource {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(currentFile)
	cmd := exec.Command("go", "list", "-deps", "-export", "-json", ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list production package: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatalf("go list production package: %v", err)
	}
	type listedPackage struct {
		ImportPath string
		Dir        string
		GoFiles    []string
		CgoFiles   []string
		Export     string
	}
	exports := make(map[string]string)
	var production listedPackage
	decoder := json.NewDecoder(strings.NewReader(string(out)))
	for {
		var listed listedPackage
		if err := decoder.Decode(&listed); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode go list output: %v", err)
		}
		if listed.Export != "" {
			exports[listed.ImportPath] = listed.Export
		}
		if listed.ImportPath == publicationGuardPackagePath {
			production = listed
		}
	}
	if production.Dir == "" || len(production.GoFiles) == 0 {
		t.Fatalf("go list did not return production files for %s", publicationGuardPackagePath)
	}

	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(production.GoFiles)+len(production.CgoFiles))
	for _, name := range append(production.GoFiles, production.CgoFiles...) {
		file, err := parser.ParseFile(fset, filepath.Join(production.Dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse production file %s: %v", name, err)
		}
		files = append(files, file)
	}
	lookup := func(path string) (io.ReadCloser, error) {
		exportPath := exports[path]
		if exportPath == "" {
			return nil, fmt.Errorf("publication guard: no export data for %s", path)
		}
		return os.Open(exportPath)
	}
	info := publicationGuardTypesInfo()
	config := types.Config{Importer: importer.ForCompiler(fset, "gc", lookup)}
	pkg, err := config.Check(publicationGuardPackagePath, fset, files, info)
	if err != nil {
		t.Fatalf("type-check production package: %v", err)
	}
	return &publicationGuardSource{fset: fset, pkg: pkg, files: files, info: info}
}

func loadPublicationGuardFixture(t *testing.T, body string) *publicationGuardSource {
	t.Helper()
	const prelude = `package guardfixture
import "sync/atomic"
type indexGen struct{}
type DBState struct{}
type snapshotView struct{}
type DB struct {
	idx atomic.Pointer[indexGen]
	state atomic.Pointer[DBState]
	snapshotViewRO atomic.Pointer[snapshotView]
	systemRootPublishEpoch atomic.Uint64
}
func (db *DB) publishSnapshotView() {}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", prelude+body, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	info := publicationGuardTypesInfo()
	config := types.Config{Importer: importer.Default()}
	pkg, err := config.Check("example.test/guardfixture", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatalf("type-check fixture: %v", err)
	}
	return &publicationGuardSource{fset: fset, pkg: pkg, files: []*ast.File{file}, info: info}
}

func publicationGuardTypesInfo() *types.Info {
	return &types.Info{
		Types:      make(map[ast.Expr]types.TypeAndValue),
		Defs:       make(map[*ast.Ident]types.Object),
		Uses:       make(map[*ast.Ident]types.Object),
		Selections: make(map[*ast.SelectorExpr]*types.Selection),
	}
}

func publicationGuardNamedType(t *testing.T, pkg *types.Package, name string) types.Type {
	t.Helper()
	obj := pkg.Scope().Lookup(name)
	if obj == nil {
		t.Fatalf("typed guard package %s has no %s", pkg.Path(), name)
	}
	return obj.Type()
}

func publicationGuardStructField(t *testing.T, typ types.Type, name string) *types.Var {
	t.Helper()
	underlying, _ := types.Unalias(typ).Underlying().(*types.Struct)
	if underlying == nil {
		t.Fatalf("typed guard type %s is not a struct", typ)
	}
	for i := 0; i < underlying.NumFields(); i++ {
		if underlying.Field(i).Name() == name {
			return underlying.Field(i)
		}
	}
	t.Fatalf("typed guard struct %s has no field %s", typ, name)
	return nil
}

func publicationGuardIsDBMethod(fn *types.Func, dbType types.Type) bool {
	if fn == nil {
		return false
	}
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil {
		return false
	}
	recv := types.Unalias(sig.Recv().Type())
	if ptr, ok := recv.(*types.Pointer); ok {
		recv = types.Unalias(ptr.Elem())
	}
	return types.Identical(recv, types.Unalias(dbType))
}

func publicationGuardFuncID(fn *types.Func) string {
	if fn == nil || fn.Pkg() == nil {
		return "<nil>"
	}
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil {
		return fn.Pkg().Path() + "." + fn.Name()
	}
	recv := types.Unalias(sig.Recv().Type())
	pointer := false
	if ptr, ok := recv.(*types.Pointer); ok {
		pointer = true
		recv = types.Unalias(ptr.Elem())
	}
	named, _ := recv.(*types.Named)
	if named == nil || named.Obj() == nil {
		return fn.Pkg().Path() + ".(" + types.TypeString(sig.Recv().Type(), publicationGuardQualifier) + ")." + fn.Name()
	}
	receiver := named.Obj().Name()
	if pointer {
		receiver = "*" + receiver
	}
	return fn.Pkg().Path() + ".(" + receiver + ")." + fn.Name()
}

func publicationGuardPackageFuncID(name string) string {
	return publicationGuardPackagePath + "." + name
}

func publicationGuardDBMethodID(name string) string {
	return publicationGuardPackagePath + ".(*DB)." + name
}

func publicationGuardQualifier(pkg *types.Package) string {
	if pkg == nil {
		return ""
	}
	return pkg.Path()
}

func sortedPublicationGuardKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func containsPublicationGuardFinding(findings []string, want string) bool {
	for _, finding := range findings {
		if finding == want {
			return true
		}
	}
	return false
}
