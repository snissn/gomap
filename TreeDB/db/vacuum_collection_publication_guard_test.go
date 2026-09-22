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

type publicationGuardStateWrite struct {
	caller *types.Func
	pos    token.Pos
	detail string
}

type publicationGuardAnalysis struct {
	source              *publicationGuardSource
	calls               map[*types.Func]map[*types.Func]struct{}
	mutations           []publicationGuardMutation
	escapes             []publicationGuardEscape
	stateWrites         []publicationGuardStateWrite
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
			publicationGuardDBMethodID("vacuumIndexOnlineRebuildV1"),
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
			publicationGuardDBMethodID("publishCompactStorageValueLogSet"),
			publicationGuardDBMethodID("publishLeafGenerationState"),
			publicationGuardDBMethodID("publishValueLogSetNoRefresh"),
			publicationGuardDBMethodID("vacuumIndexOnlineRebuildV1"),
			publicationGuardMethodID("rootPublicationVisibleInstallV1", "activate"),
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
	for _, write := range analysis.stateWrites {
		t.Errorf("published DBState mutated in %s: %s", publicationGuardFuncID(write.caller), write.detail)
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
		publicationGuardDBMethodID("publishCompactStorageValueLogSet"),
		publicationGuardDBMethodID("publishLeafGenerationState"),
		publicationGuardDBMethodID("publishValueLogSetNoRefresh"),
		publicationGuardDBMethodID("vacuumIndexOnlineRebuildV1"),
		publicationGuardMethodID("rootPublicationVisibleInstallV1", "activate"),
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

func TestPublicationSourceGuardRejectsPublishedStateMutationFixtures(t *testing.T) {
	fixtures := map[string]string{
		"direct load": `func (db *DB) bypass(next uint64) {
	db.state.Load().SystemRootPageID = next
}`,
		"renamed receiver": `func (database *DB) bypass(next uint64) {
	state := database.state.Load()
	state.SystemRootPageID = next
}`,
		"pointer alias": `func (db *DB) bypass(next uint64) {
	state := db.state.Load()
	alias := state
	alias.SystemRootPageID = next
}`,
		"atomic slot alias": `func (db *DB) bypass(next uint64) {
	slot := &db.state
	state := slot.Load()
	state.SystemRootPageID = next
}`,
		"helper mediated": `func mutatePublishedState(state *DBState, next uint64) {
	state.SystemRootPageID = next
}
func (db *DB) bypass(next uint64) {
	mutatePublishedState(db.state.Load(), next)
}`,
		"helper returned": `func loadPublishedState(db *DB) *DBState {
	return db.state.Load()
}
func (db *DB) bypass(next uint64) {
	state := loadPublishedState(db)
	state.SystemRootPageID = next
}`,
		"pointer replacement": `func (db *DB) bypass(next uint64) {
	state := db.state.Load()
	*state = DBState{SystemRootPageID: next}
}`,
		"explicit dereference field": `func (db *DB) bypass(next uint64) {
	state := db.state.Load()
	(*state).SystemRootPageID = next
}`,
		"address dereference alias": `func (db *DB) bypass(next uint64) {
	state := db.state.Load()
	alias := &*state
	alias.SystemRootPageID = next
}`,
		"slice alias": `func (db *DB) bypass(next uint64) {
	states := []*DBState{db.state.Load()}
	states[0].SystemRootPageID = next
}`,
		"struct field alias": `type stateHolder struct { state *DBState }
func (db *DB) bypass(next uint64) {
	holder := stateHolder{state: db.state.Load()}
	holder.state.SystemRootPageID = next
}`,
		"closure returned": `func (db *DB) bypass(next uint64) {
	state := db.state.Load()
	load := func() *DBState { return state }
	load().SystemRootPageID = next
}`,
		"state method exposure": `func (db *DB) State() *DBState {
	return db.state.Load()
}
func (db *DB) bypass(next uint64) {
	state := db.State()
	state.SystemRootPageID = next
}`,
		"snapshot view alias": `func (db *DB) bypass(next uint64) {
	view := db.snapshotViewRO.Load()
	state := view.state
	state.SystemRootPageID = next
}`,
		"snapshot method exposure": `func (db *DB) AcquireSnapshot() *Snapshot {
	return &Snapshot{state: db.state.Load()}
}
func (snapshot *Snapshot) State() *DBState {
	return snapshot.state
}
func (db *DB) bypass(next uint64) {
	snapshot := db.AcquireSnapshot()
	state := snapshot.State()
	state.SystemRootPageID = next
}`,
	}

	for name, body := range fixtures {
		t.Run(name, func(t *testing.T) {
			analysis := analyzePublicationGuard(t, loadPublicationGuardFixture(t, body))
			if len(analysis.stateWrites) == 0 {
				t.Fatal("typed guard accepted published DBState mutation")
			}
		})
	}
}

func TestPublicationSourceGuardAllowsCopiedStateMutation(t *testing.T) {
	source := loadPublicationGuardFixture(t, `func (db *DB) safe(next uint64) {
	published := db.state.Load()
	copyState := *published
	copyState.SystemRootPageID = next
	db.state.Store(&copyState)
	db.publishSnapshotView()
}`)
	analysis := analyzePublicationGuard(t, source)
	if len(analysis.stateWrites) != 0 {
		t.Fatalf("typed guard rejected copied state mutation: %+v", analysis.stateWrites)
	}
}

func TestPublicationSourceGuardAllowsScalarStateToken(t *testing.T) {
	source := loadPublicationGuardFixture(t, `type StateToken struct {
	RootPageID uint64
	SystemRootPageID uint64
}
func (db *DB) StateToken() (StateToken, bool) {
	view := db.snapshotViewRO.Load()
	if view == nil || view.state == nil {
		return StateToken{}, false
	}
	state := view.state
	return StateToken{RootPageID: state.RootPageID, SystemRootPageID: state.SystemRootPageID}, true
}`)
	analysis := analyzePublicationGuard(t, source)
	if len(analysis.stateWrites) != 0 || len(analysis.mutations) != 0 || len(analysis.escapes) != 0 {
		t.Fatalf("typed guard rejected scalar state token: writes=%+v mutations=%+v escapes=%+v", analysis.stateWrites, analysis.mutations, analysis.escapes)
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
	analysis.stateWrites = publicationGuardFindStateWrites(t, source, dbType, targetTypes["state"], targetTypes["snapshotViewRO"])
	return analysis
}

type publicationGuardTaint uint8

const (
	publicationGuardTaintState publicationGuardTaint = 1 << iota
	publicationGuardTaintSnapshot
	publicationGuardTaintSnapshotView
	publicationGuardTaintStateSlot
	publicationGuardTaintSnapshotViewSlot
)

type publicationGuardTaintAnalysis struct {
	source             *publicationGuardSource
	funcDecls          map[*types.Func]*ast.FuncDecl
	objectTaint        map[types.Object]publicationGuardTaint
	resultTaint        map[*types.Var]publicationGuardTaint
	dbStateType        types.Type
	snapshotType       types.Type
	snapshotViewType   types.Type
	dbStateField       *types.Var
	dbSnapshotField    *types.Var
	viewStateField     *types.Var
	snapshotStateField *types.Var
}

func publicationGuardFindStateWrites(t *testing.T, source *publicationGuardSource, dbType, dbStateType, snapshotViewType types.Type) []publicationGuardStateWrite {
	t.Helper()
	snapshotType := publicationGuardNamedType(t, source.pkg, "Snapshot")
	ta := &publicationGuardTaintAnalysis{
		source:             source,
		funcDecls:          make(map[*types.Func]*ast.FuncDecl),
		objectTaint:        make(map[types.Object]publicationGuardTaint),
		resultTaint:        make(map[*types.Var]publicationGuardTaint),
		dbStateType:        dbStateType,
		snapshotType:       snapshotType,
		snapshotViewType:   snapshotViewType,
		dbStateField:       publicationGuardStructField(t, dbType, "state"),
		dbSnapshotField:    publicationGuardStructField(t, dbType, "snapshotViewRO"),
		viewStateField:     publicationGuardStructField(t, snapshotViewType, "state"),
		snapshotStateField: publicationGuardStructField(t, snapshotType, "state"),
	}
	for _, file := range source.files {
		for _, decl := range file.Decls {
			fnDecl, ok := decl.(*ast.FuncDecl)
			if !ok || fnDecl.Body == nil {
				continue
			}
			fn, _ := source.info.Defs[fnDecl.Name].(*types.Func)
			if fn != nil {
				ta.funcDecls[fn] = fnDecl
			}
		}
	}

	for changed := true; changed; {
		changed = false
		for fn, decl := range ta.funcDecls {
			if ta.propagateFunc(fn, decl) {
				changed = true
			}
		}
	}

	var writes []publicationGuardStateWrite
	for caller, decl := range ta.funcDecls {
		ast.Inspect(decl.Body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if detail, ok := ta.stateWrite(lhs); ok {
						writes = append(writes, publicationGuardStateWrite{caller: caller, pos: lhs.Pos(), detail: detail})
					}
				}
			case *ast.IncDecStmt:
				if detail, ok := ta.stateWrite(n.X); ok {
					writes = append(writes, publicationGuardStateWrite{caller: caller, pos: n.Pos(), detail: detail})
				}
			case *ast.CallExpr:
				callee := publicationGuardCalledFunc(n.Fun, source.info)
				if _, local := ta.funcDecls[callee]; local {
					break
				}
				for _, arg := range n.Args {
					if ta.expr(arg)&publicationGuardTaintState != 0 {
						writes = append(writes, publicationGuardStateWrite{caller: caller, pos: arg.Pos(), detail: "published DBState escapes to a call without an analyzed body"})
					}
				}
			}
			return true
		})
	}
	return writes
}

func (ta *publicationGuardTaintAnalysis) propagateFunc(fn *types.Func, decl *ast.FuncDecl) bool {
	changed := false
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ValueSpec:
			for i, value := range n.Values {
				if i < len(n.Names) {
					changed = ta.addObjectTaint(ta.source.info.Defs[n.Names[i]], ta.expr(value)) || changed
				}
			}
		case *ast.AssignStmt:
			if len(n.Lhs) == len(n.Rhs) {
				for i := range n.Lhs {
					changed = ta.propagateAssignment(n.Lhs[i], n.Rhs[i]) || changed
				}
			}
		case *ast.RangeStmt:
			kind := ta.expr(n.X)
			if ident, ok := n.Key.(*ast.Ident); ok {
				changed = ta.addObjectTaint(ta.identObject(ident), kind) || changed
			}
			if ident, ok := n.Value.(*ast.Ident); ok {
				changed = ta.addObjectTaint(ta.identObject(ident), kind) || changed
			}
		case *ast.CallExpr:
			changed = ta.propagateCall(n) || changed
		case *ast.ReturnStmt:
			sig, _ := fn.Type().(*types.Signature)
			if sig == nil || sig.Results() == nil || len(n.Results) != sig.Results().Len() {
				break
			}
			for i, result := range n.Results {
				resultVar := sig.Results().At(i)
				kind := ta.expr(result)
				if kind != 0 && ta.resultTaint[resultVar]&kind != kind {
					ta.resultTaint[resultVar] |= kind
					changed = true
				}
			}
		}
		return true
	})
	return changed
}

func (ta *publicationGuardTaintAnalysis) propagateAssignment(lhs, rhs ast.Expr) bool {
	kind := ta.expr(rhs)
	if ident, ok := lhs.(*ast.Ident); ok {
		return ta.addObjectTaint(ta.identObject(ident), kind)
	}
	if index, ok := lhs.(*ast.IndexExpr); ok {
		return ta.addExprObjectTaint(index.X, kind)
	}
	sel, ok := lhs.(*ast.SelectorExpr)
	if !ok || kind == 0 {
		return false
	}
	selection := ta.source.info.Selections[sel]
	if selection == nil {
		return false
	}
	changed := ta.addObjectTaint(selection.Obj(), kind)
	if kind&publicationGuardTaintState != 0 {
		switch selection.Obj() {
		case ta.snapshotStateField:
			return ta.addExprObjectTaint(sel.X, publicationGuardTaintSnapshot) || changed
		case ta.viewStateField:
			return ta.addExprObjectTaint(sel.X, publicationGuardTaintSnapshotView) || changed
		}
	}
	return ta.addExprObjectTaint(sel.X, kind) || changed
}

func (ta *publicationGuardTaintAnalysis) propagateCall(call *ast.CallExpr) bool {
	callee := publicationGuardCalledFunc(call.Fun, ta.source.info)
	if callee == nil {
		return false
	}
	sig, _ := callee.Type().(*types.Signature)
	if sig == nil {
		return false
	}
	changed := false
	if sel, ok := publicationGuardUnparen(call.Fun).(*ast.SelectorExpr); ok && sig.Recv() != nil {
		changed = ta.addObjectTaint(sig.Recv(), ta.expr(sel.X)) || changed
	}
	for i, arg := range call.Args {
		if i >= sig.Params().Len() {
			break
		}
		changed = ta.addObjectTaint(sig.Params().At(i), ta.expr(arg)) || changed
	}
	return changed
}

func (ta *publicationGuardTaintAnalysis) expr(expr ast.Expr) publicationGuardTaint {
	switch expr := publicationGuardUnparen(expr).(type) {
	case *ast.Ident:
		object := ta.identObject(expr)
		kind := ta.objectTaint[object]
		if fn, ok := object.(*types.Func); ok {
			sig, _ := fn.Type().(*types.Signature)
			if sig != nil && sig.Results() != nil && sig.Results().Len() == 1 {
				kind |= ta.resultTaint[sig.Results().At(0)]
			}
		}
		return kind
	case *ast.SelectorExpr:
		selection := ta.source.info.Selections[expr]
		if selection == nil {
			return 0
		}
		switch selection.Obj() {
		case ta.dbStateField:
			return publicationGuardTaintStateSlot
		case ta.dbSnapshotField:
			return publicationGuardTaintSnapshotViewSlot
		case ta.snapshotStateField:
			if ta.expr(expr.X)&publicationGuardTaintSnapshot != 0 {
				return publicationGuardTaintState
			}
		case ta.viewStateField:
			if ta.expr(expr.X)&publicationGuardTaintSnapshotView != 0 {
				return publicationGuardTaintState
			}
		}
		if kind := ta.objectTaint[selection.Obj()]; kind != 0 {
			return kind
		}
		if ta.expr(expr.X)&publicationGuardTaintState != 0 && publicationGuardIsPointerTo(expr, ta.source.info, ta.dbStateType) {
			return publicationGuardTaintState
		}
		return 0
	case *ast.CallExpr:
		if sel, ok := publicationGuardUnparen(expr.Fun).(*ast.SelectorExpr); ok && sel.Sel.Name == "Load" {
			slot := ta.expr(sel.X)
			switch {
			case slot&publicationGuardTaintStateSlot != 0:
				return publicationGuardTaintState
			case slot&publicationGuardTaintSnapshotViewSlot != 0:
				return publicationGuardTaintSnapshotView
			}
		}
		if kind := ta.expr(expr.Fun); kind&publicationGuardTaintState != 0 {
			return publicationGuardTaintState
		}
		callee := publicationGuardCalledFunc(expr.Fun, ta.source.info)
		if callee == nil {
			return 0
		}
		sig, _ := callee.Type().(*types.Signature)
		if sig != nil && sig.Results() != nil && sig.Results().Len() == 1 {
			return ta.resultTaint[sig.Results().At(0)]
		}
		return 0
	case *ast.UnaryExpr:
		if expr.Op == token.AND {
			if dereference, ok := publicationGuardUnparen(expr.X).(*ast.StarExpr); ok {
				return ta.expr(dereference.X)
			}
		}
		return ta.expr(expr.X)
	case *ast.StarExpr:
		return ta.expr(expr.X) &^ publicationGuardTaintState
	case *ast.TypeAssertExpr:
		return ta.expr(expr.X)
	case *ast.IndexExpr:
		return ta.expr(expr.X)
	case *ast.IndexListExpr:
		return ta.expr(expr.X)
	case *ast.CompositeLit:
		return ta.compositeTaint(expr)
	case *ast.FuncLit:
		var kind publicationGuardTaint
		ast.Inspect(expr.Body, func(node ast.Node) bool {
			if ret, ok := node.(*ast.ReturnStmt); ok {
				for _, result := range ret.Results {
					kind |= ta.expr(result)
				}
			}
			return true
		})
		return kind
	default:
		return 0
	}
}

func (ta *publicationGuardTaintAnalysis) compositeTaint(lit *ast.CompositeLit) publicationGuardTaint {
	typ := ta.source.info.TypeOf(lit)
	if ptr, ok := types.Unalias(typ).(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	want := publicationGuardTaintState
	switch {
	case types.Identical(types.Unalias(typ), types.Unalias(ta.snapshotType)):
		want = publicationGuardTaintSnapshot
	case types.Identical(types.Unalias(typ), types.Unalias(ta.snapshotViewType)):
		want = publicationGuardTaintSnapshotView
	}
	for _, elt := range lit.Elts {
		value := elt
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			value = kv.Value
		}
		if ta.expr(value)&publicationGuardTaintState != 0 {
			return want
		}
	}
	return 0
}

func (ta *publicationGuardTaintAnalysis) stateWrite(expr ast.Expr) (string, bool) {
	expr = publicationGuardUnparen(expr)
	if star, ok := expr.(*ast.StarExpr); ok && ta.expr(star.X)&publicationGuardTaintState != 0 {
		return "whole published DBState overwritten through pointer alias", true
	}
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || ta.stateAliasExpr(sel.X)&publicationGuardTaintState == 0 {
		return "", false
	}
	selection := ta.source.info.Selections[sel]
	if selection == nil || !publicationGuardFieldBelongsTo(selection.Obj(), ta.dbStateType) {
		return "", false
	}
	return "published DBState field " + sel.Sel.Name + " written through pointer alias", true
}

func (ta *publicationGuardTaintAnalysis) stateAliasExpr(expr ast.Expr) publicationGuardTaint {
	switch expr := publicationGuardUnparen(expr).(type) {
	case *ast.StarExpr:
		return ta.stateAliasExpr(expr.X)
	default:
		return ta.expr(expr)
	}
}

func (ta *publicationGuardTaintAnalysis) addExprObjectTaint(expr ast.Expr, kind publicationGuardTaint) bool {
	switch expr := publicationGuardUnparen(expr).(type) {
	case *ast.Ident:
		return ta.addObjectTaint(ta.identObject(expr), kind)
	case *ast.StarExpr:
		return ta.addExprObjectTaint(expr.X, kind)
	case *ast.IndexExpr:
		return ta.addExprObjectTaint(expr.X, kind)
	case *ast.SelectorExpr:
		selection := ta.source.info.Selections[expr]
		if selection == nil {
			return false
		}
		changed := ta.addObjectTaint(selection.Obj(), kind)
		return ta.addExprObjectTaint(expr.X, kind) || changed
	default:
		return false
	}
}

func publicationGuardIsPointerTo(expr ast.Expr, info *types.Info, target types.Type) bool {
	typ := types.Unalias(info.TypeOf(expr))
	pointer, ok := typ.(*types.Pointer)
	return ok && types.Identical(types.Unalias(pointer.Elem()), types.Unalias(target))
}

func (ta *publicationGuardTaintAnalysis) addObjectTaint(object types.Object, kind publicationGuardTaint) bool {
	if object == nil || kind == 0 || ta.objectTaint[object]&kind == kind {
		return false
	}
	ta.objectTaint[object] |= kind
	return true
}

func (ta *publicationGuardTaintAnalysis) identObject(ident *ast.Ident) types.Object {
	if object := ta.source.info.Defs[ident]; object != nil {
		return object
	}
	return ta.source.info.Uses[ident]
}

func publicationGuardUnparen(expr ast.Expr) ast.Expr {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.X
	}
}

func publicationGuardFieldBelongsTo(object types.Object, owner types.Type) bool {
	field, ok := object.(*types.Var)
	if !ok || !field.IsField() {
		return false
	}
	strct, _ := types.Unalias(owner).Underlying().(*types.Struct)
	if strct == nil {
		return false
	}
	for i := 0; i < strct.NumFields(); i++ {
		if strct.Field(i) == field {
			return true
		}
	}
	return false
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
type DBState struct {
	RootPageID uint64
	SystemRootPageID uint64
}
type snapshotView struct {
	state *DBState
}
type Snapshot struct {
	state *DBState
}
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

func publicationGuardMethodID(receiver, name string) string {
	return publicationGuardPackagePath + ".(*" + receiver + ")." + name
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
