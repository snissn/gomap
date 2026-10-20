package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
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

// This is a supplementary writer-inventory and alias regression guard, not a
// sound analysis of arbitrary Go or a proof of coherent publication. Callgraph
// reachability is existential: ordering, argument identity and every successful
// path require runtime review and discriminating tests. Review field-address
// aliases explicitly; scalar fields do not retain the analyzer's DBState taint.
type publicationGuardSource struct {
	fset  *token.FileSet
	pkg   *types.Package
	files []*ast.File
	// Full checked declarations supplement the smaller callback-body corpus
	// when proving that a private custody payload has no accessor anywhere.
	custodyFiles []*ast.File
	info         *types.Info
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
			publicationGuardPackageFuncID("openReadOnlyNoLockWithRegistryV6"),
			publicationGuardPackageFuncID("openWithLock"),
			publicationGuardDBMethodID("vacuumIndexOnlineRebuildWithCompletionV6"),
		},
		"snapshotViewRO.Store": {
			publicationGuardDBMethodID("clearSnapshotView"),
			publicationGuardDBMethodID("publishSnapshotView"),
		},
		"state.Store": {
			publicationGuardDBMethodID("RefreshValueLogSet"),
			publicationGuardDBMethodID("finalizeCommitLockedWithOptions"),
			publicationGuardPackageFuncID("openReadOnly"),
			publicationGuardPackageFuncID("openReadOnlyNoLockWithRegistryV6"),
			publicationGuardDBMethodID("publishCompactStorageValueLogSet"),
			publicationGuardDBMethodID("publishLeafGenerationState"),
			publicationGuardDBMethodID("publishValueLogSetNoRefresh"),
			publicationGuardDBMethodID("vacuumIndexOnlineRebuildWithCompletionV6"),
			publicationGuardMethodID("rootPublicationVisibleInstallV1", "activate"),
		},
		"state.CompareAndSwap": {publicationGuardDBMethodID("ensureCommandWALRecoverySnapshotView")},
		"state.Swap":           {publicationGuardDBMethodID("initializeRecoveredSnapshotV1")},
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
		t.Errorf("published DBState mutated at %s in %s: %s", source.fset.Position(write.pos), publicationGuardFuncID(write.caller), write.detail)
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

	// The same publication inventory classifies membership coverage. Only
	// bootstrap, normalized point applies, and explicitly bound current-root
	// metadata publications may carry coverage;
	// metadata callers preserve exact unchanged coordinates, replacements drop it.
	wantPublishCoverage := map[string]bool{
		publicationGuardDBMethodID("bootstrapNegativeFilter"):                   true,
		publicationGuardDBMethodID("RefreshValueLogSet"):                        false,
		publicationGuardDBMethodID("ensureCommandWALRecoverySnapshotView"):      false,
		publicationGuardDBMethodID("finalizeCommitLockedWithOptions"):           true,
		publicationGuardPackageFuncID("openReadOnly"):                           false,
		publicationGuardPackageFuncID("openReadOnlyNoLockWithRegistryV6"):       false,
		publicationGuardDBMethodID("initializeRecoveredSnapshotV1"):             false,
		publicationGuardDBMethodID("publishCompactStorageValueLogSet"):          false,
		publicationGuardDBMethodID("publishLeafGenerationState"):                false,
		publicationGuardDBMethodID("publishValueLogSetNoRefresh"):               false,
		publicationGuardDBMethodID("vacuumIndexOnlineRebuildWithCompletionV6"):  false,
		publicationGuardMethodID("rootPublicationVisibleInstallV1", "activate"): true,
	}
	var wantPublishCallers []string
	for name := range wantPublishCoverage {
		wantPublishCallers = append(wantPublishCallers, name)
	}
	sort.Strings(wantPublishCallers)
	for _, file := range source.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			caller, _ := source.info.Defs[fn.Name].(*types.Func)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || publicationGuardCalledFunc(call.Fun, source.info) != analysis.publishSnapshotView {
					return true
				}
				want := 4
				if wantPublishCoverage[publicationGuardFuncID(caller)] {
					want = 5
				}
				if len(call.Args) != want {
					t.Errorf("%s membership coverage arguments=%d want %d", publicationGuardFuncID(caller), len(call.Args), want)
				}
				return true
			})
		}
	}

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
		"function field method callback": `type stateInstaller struct { apply func(*DBState) }
func mutateFromCallback(state *DBState) { state.SystemRootPageID=7 }
func (db *DB) bypass() { opts:=stateInstaller{}; opts.apply=mutateFromCallback; opts.apply(db.state.Load()) }`,
		"function field unknown alternative": `type maybeInstaller struct { apply func(*DBState) }
func knownRead(state *DBState) { _=state.SystemRootPageID }
func (db *DB) bypass(unknown func(*DBState), selectUnknown bool) { opts:=maybeInstaller{};opts.apply=knownRead;if selectUnknown {opts.apply=unknown}; opts.apply(db.state.Load()) }`,
		"interface Is callback": `type stateError struct { state *DBState }
func (e *stateError) Is(error) bool { e.state.SystemRootPageID=9;return false }
func callIs(e interface{Is(error)bool}) {e.Is(nil)}
func (db *DB) bypass() {callIs(&stateError{state:db.state.Load()})}`,
		"unknown interface target callback": `type knownStateError struct {state *DBState}
func (e *knownStateError) Error() string {return "state"}
func (e *knownStateError) Is(error) bool {return false}
func (db *DB) bypass(unknown interface{Is(error)bool}) {unknown.Is(&knownStateError{state:db.state.Load()})}`,
		"interface Unwrap callback": `type unwrapStateError struct { state *DBState }
func (e *unwrapStateError) Unwrap() error { e.state.SystemRootPageID=9;return nil }
func callUnwrap(e interface{Unwrap()error}) {e.Unwrap()}
func (db *DB) bypass() {callUnwrap(&unwrapStateError{state:db.state.Load()})}`,
		"cleanup prior callback": `type priorStateError struct { state *DBState }
func (e *priorStateError) TryLinkPriorCleanupError(error) bool {e.state.SystemRootPageID=9;return true}
func callPrior(e interface{TryLinkPriorCleanupError(error)bool}) {e.TryLinkPriorCleanupError(nil)}
func (db *DB) bypass() {callPrior(&priorStateError{state:db.state.Load()})}`,
		"format error callback": `type formatStateError struct{state *DBState}
func(e *formatStateError) Error()string {e.state.SystemRootPageID=9;return "bad"}
func(db *DB)bypass(){_=fmt.Errorf("owned: %w",&formatStateError{state:db.state.Load()})}`,
		"bodyless error callback": `type opaqueStateError struct{state *DBState}
func(e *opaqueStateError) Error()string
func(db *DB)bypass(){_=fmt.Errorf("owned: %w",&opaqueStateError{state:db.state.Load()})}`,
		"interface unknown alternative": `type safeStateError struct{state *DBState}
func(e *safeStateError) Is(error)bool{return false}
func unknownRetainedError(*DBState) interface{Is(error)bool}
func(db *DB)bypass(choose bool){var target interface{Is(error)bool}=&safeStateError{state:db.state.Load()};if choose{target=unknownRetainedError(db.state.Load())};target.Is(nil)}`,
		"unknown receiver alternative with retained argument": `type readStateError struct{state *DBState}
func(e *readStateError) Error()string{return "read"}
func(e *readStateError) Is(error)bool{return false}
func(db *DB)bypass(unknown interface{Is(error)bool},choose bool){var target interface{Is(error)bool}=&readStateError{state:db.state.Load()};if choose{target=unknown};target.Is(&readStateError{state:db.state.Load()})}`,
		"unknown asserted receiver alternative with retained argument": `type readOnlyHolder struct{state *DBState}
func(db *DB)bypass(unknown any,choose bool){var target any=&readOnlyHolder{state:db.state.Load()};if choose{target=unknown};if callback,ok:=target.(interface{Apply(*DBState)});ok{callback.Apply(db.state.Load())}}`,
		"unknown receiver through helper with retained argument": `type readonlyStateError struct{state *DBState}
func(e *readonlyStateError) Error()string{return "safe"}
func(e *readonlyStateError) Is(error)bool{return false}
func selectReceiver(target interface{Is(error)bool})interface{Is(error)bool}{return target}
func(db *DB)bypass(unknown interface{Is(error)bool},choose bool){var target interface{Is(error)bool}=&readonlyStateError{state:db.state.Load()};if choose{target=unknown};selectReceiver(target).Is(&readonlyStateError{state:db.state.Load()})}`,
		"unretained concrete receiver mutates state argument": `type stateArgumentWriter struct{}
func(*stateArgumentWriter)Apply(state *DBState){state.SystemRootPageID=9}
func(db *DB)bypass(){var target interface{Apply(*DBState)}=&stateArgumentWriter{};target.Apply(db.state.Load())}`,
		"unknown callback escape": `type escapedStateError struct {state *DBState}
func unknownErrorSink(any)
func (db *DB) bypass() {unknownErrorSink(&escapedStateError{state:db.state.Load()})}`,
		"helper returned": `func loadPublishedState(db *DB) *DBState {
	return db.state.Load()
}
func (db *DB) bypass(next uint64) {
	state := loadPublishedState(db)
	state.SystemRootPageID = next
}`,
		"if initializer overwritten in body": `func (db *DB) bypass() {
 state := db.state.Load()
 if state = new(DBState); state != nil { state = db.state.Load(); state.SystemRootPageID = 1 }
}`,
		"if initializer address alias": `func (db *DB) bypass() {
 state := db.state.Load()
 alias := &state
 if state = new(DBState); state != nil { *alias = db.state.Load(); state.SystemRootPageID = 1 }
}`,
		"if initializer closure alias": `func (db *DB) bypass() {
 state := db.state.Load()
 replace := func(){state = db.state.Load()}
 if state = new(DBState); state != nil { replace(); state.SystemRootPageID = 1 }
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
		"assigned struct field alias": `type assignedStateHolder struct { state *DBState }
func (db *DB) bypass(next uint64) {
	holder := assignedStateHolder{}
	holder.state = db.state.Load()
	holder.state.SystemRootPageID = next
}`,
		"assigned container external escape": `type externalStateHolder struct { state *DBState }
func unknownStateHolder(*externalStateHolder)
func (db *DB) bypass(next uint64) {
	holder := externalStateHolder{}
	holder.state = db.state.Load()
	unknownStateHolder(&holder)
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

func TestPublicationSourceGuardAllowsKnownErrorFormatting(t *testing.T) {
	source := loadPublicationGuardFixture(t, `type safeFormatError struct{state *DBState}
func(e *safeFormatError)Error()string{return "safe"}
func(db *DB)safe(){_=fmt.Errorf("owned: %w",&safeFormatError{state:db.state.Load()})}`)
	if writes := analyzePublicationGuard(t, source).stateWrites; len(writes) != 0 {
		t.Fatalf("known formatting callback rejected: %+v", writes)
	}
}

func TestPublicationSourceGuardSeparatesFreshAsOutputFromAssignedValue(t *testing.T) {
	fixtures := map[string]struct {
		body  string
		fresh bool
	}{
		"fresh":              {`var target error; errors.As(owned, &target); unknownErrorSink(target)`, true},
		"fresh compound":     {`var target error; _ = errors.As(owned, &target) && target != nil; unknownErrorSink(target)`, true},
		"initialized":        {`var target error = owned; errors.As(owned, &target)`, false},
		"assignment address": {`var target error; *slotPointer(&target) = errors.As(owned, &target)`, false},
		"var group alias":    {`var (target error; alias = &target); errors.As(owned, &target); _ = alias`, false},
		"aliased":            {`var target error; alias := &target; _ = alias; errors.As(owned, &target)`, false},
		"reused":             {`var target error; errors.As(owned, &target); errors.As(owned, &target)`, false},
		"loop reuse":         {`var target error; for i:=0;i<2;i++ { errors.As(owned, &target) }`, false},
		"closure reads slot": {`var target error; errors.As(func()error{_=target;return owned}(), &target)`, false},
		"backward goto":      {`var target error; again: errors.As(owned, &target); if false {goto again}`, false},
	}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			source := loadPublicationGuardFixture(t, `type retainedAsError struct{state *DBState}
func(*retainedAsError)Error()string{return "retained"}
func unknownErrorSink(error)
func slotPointer(*error)*bool
func(db *DB)check(){owned:=&retainedAsError{state:db.state.Load()};_=owned;`+fixture.body+`}`)
			writes := analyzePublicationGuard(t, source).stateWrites
			outputEscape, assignedEscape := false, false
			for _, write := range writes {
				outputEscape = outputEscape || (strings.Contains(write.detail, "callee=errors.As") && strings.Contains(write.detail, "argument=&target"))
				assignedEscape = assignedEscape || strings.Contains(write.detail, "callee=unknownErrorSink")
			}
			if outputEscape == fixture.fresh {
				t.Fatalf("fresh=%v output escape=%v: %+v", fixture.fresh, outputEscape, writes)
			}
			if fixture.fresh && !assignedEscape {
				t.Fatal("As output lost its subsequently retained original value")
			}
		})
	}
}

func TestPublicationSourceGuardAllowsKnownReceiverReadingStateArgument(t *testing.T) {
	source := loadPublicationGuardFixture(t, `type stateArgumentReader struct{}
func(*stateArgumentReader)Apply(state *DBState){_=state.SystemRootPageID}
func(db *DB)safe(){var target interface{Apply(*DBState)}=&stateArgumentReader{};target.Apply(db.state.Load())}`)
	if writes := analyzePublicationGuard(t, source).stateWrites; len(writes) != 0 {
		t.Fatalf("known read-only receiver rejected: %+v", writes)
	}
}

func TestPublicationSourceGuardAllowsUnaliasedIfInitializerReplacement(t *testing.T) {
	source := loadPublicationGuardFixture(t, `func unknownStateSink(*DBState)
func(db *DB)safe(){state:=db.state.Load(); _=state; if state=new(DBState);state!=nil{unknownStateSink(state)}}`)
	if writes := analyzePublicationGuard(t, source).stateWrites; len(writes) != 0 {
		t.Fatalf("unconditionally replaced local retains an obsolete alias: %+v", writes)
	}
}

func TestPublicationSourceGuardKeepsFieldAliasesSeparateFromContainer(t *testing.T) {
	source := loadPublicationGuardFixture(t, `type holderWithState struct { state *DBState; diagnostic uint64 }
func (holder *holderWithState) diagnosticValue() uint64 { return holder.diagnostic }
func (db *DB) safe(next uint64) {
	holder := holderWithState{}
	holder.state = db.state.Load()
	published := holder.state
	_ = published.RootPageID
	db.systemRootPublishEpoch.Add(holder.diagnosticValue())
}`)
	analysis := analyzePublicationGuard(t, source)
	if len(analysis.stateWrites) != 0 {
		t.Fatalf("typed guard confused a retained state field with its unrelated container: %+v", analysis.stateWrites)
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
	publicationGuardTaintContainer
)

type publicationGuardTaintAnalysis struct {
	source                 *publicationGuardSource
	funcDecls              map[*types.Func]*ast.FuncDecl
	literalTargets         map[*ast.FuncLit]*types.Func
	objectTaint            map[types.Object]publicationGuardTaint
	resultTaint            map[*types.Var]publicationGuardTaint
	functionTargets        map[types.Object]map[*types.Func]struct{}
	unknownFunctionTargets map[types.Object]bool
	functionVariables      map[types.Object]bool
	verifiedSource         map[string]bool
	custodyRefusal         string
	concreteTypes          map[types.Object]map[types.Type]struct{}
	unknownConcreteTypes   map[types.Object]bool
	unknownReason          map[types.Object]string
	interfaceVariables     map[types.Object]bool
	allConcreteTypes       map[types.Object]map[types.Type]struct{}
	allUnknownTypes        map[types.Object]bool
	collectAllOrigins      bool
	strongReadValues       map[*ast.Ident]ast.Expr
	dbStateType            types.Type
	snapshotType           types.Type
	snapshotViewType       types.Type
	dbStateField           *types.Var
	dbSnapshotField        *types.Var
	viewStateField         *types.Var
	snapshotStateField     *types.Var
}

func publicationGuardFindStateWrites(t *testing.T, source *publicationGuardSource, dbType, dbStateType, snapshotViewType types.Type) []publicationGuardStateWrite {
	t.Helper()
	snapshotType := publicationGuardNamedType(t, source.pkg, "Snapshot")
	ta := &publicationGuardTaintAnalysis{
		source:                 source,
		funcDecls:              make(map[*types.Func]*ast.FuncDecl),
		literalTargets:         make(map[*ast.FuncLit]*types.Func),
		objectTaint:            make(map[types.Object]publicationGuardTaint),
		resultTaint:            make(map[*types.Var]publicationGuardTaint),
		functionTargets:        make(map[types.Object]map[*types.Func]struct{}),
		unknownFunctionTargets: make(map[types.Object]bool),
		functionVariables:      make(map[types.Object]bool),
		verifiedSource:         make(map[string]bool),
		concreteTypes:          make(map[types.Object]map[types.Type]struct{}),
		unknownConcreteTypes:   make(map[types.Object]bool),
		unknownReason:          make(map[types.Object]string),
		interfaceVariables:     make(map[types.Object]bool),
		strongReadValues:       make(map[*ast.Ident]ast.Expr),
		dbStateType:            dbStateType,
		snapshotType:           snapshotType,
		snapshotViewType:       snapshotViewType,
		dbStateField:           publicationGuardStructField(t, dbType, "state"),
		dbSnapshotField:        publicationGuardStructField(t, dbType, "snapshotViewRO"),
		viewStateField:         publicationGuardStructField(t, snapshotViewType, "state"),
		snapshotStateField:     publicationGuardStructField(t, snapshotType, "state"),
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
				ast.Inspect(fnDecl.Body, func(node ast.Node) bool {
					lit, ok := node.(*ast.FuncLit)
					if !ok {
						return true
					}
					sig, _ := source.info.TypeOf(lit).(*types.Signature)
					if sig != nil {
						target := types.NewFunc(lit.Pos(), fn.Pkg(), fmt.Sprintf("literal%d", lit.Pos()), sig)
						ta.literalTargets[lit] = target
						ta.funcDecls[target] = &ast.FuncDecl{Type: lit.Type, Body: lit.Body}
					}
					return true
				})
			}
		}
	}

	for _, decl := range ta.funcDecls {
		ta.indexStrongIfReads(decl.Body)
		ta.indexFreshAsOutputReads(decl.Body)
	}

	// A receiver which does not own published state can still receive it as an
	// explicit method argument. Keep the complete receiver alternatives separate
	// from origins of retained aliases; otherwise an unrelated opaque error
	// pollutes Error() while an opaque alternative to a safe receiver is missed.
	ta.collectAllOrigins = true
	ta.propagateToFixedPoint()
	for object := range ta.interfaceVariables {
		if len(ta.concreteTypes[object]) == 0 {
			ta.unknownConcreteTypes[object] = true
		}
	}
	ta.propagateToFixedPoint()
	ta.allConcreteTypes, ta.allUnknownTypes = ta.concreteTypes, ta.unknownConcreteTypes
	ta.concreteTypes = make(map[types.Object]map[types.Type]struct{})
	ta.unknownConcreteTypes = make(map[types.Object]bool)
	ta.unknownReason = make(map[types.Object]string)
	ta.interfaceVariables = make(map[types.Object]bool)
	ta.objectTaint = make(map[types.Object]publicationGuardTaint)
	ta.resultTaint = make(map[*types.Var]publicationGuardTaint)
	ta.collectAllOrigins = false
	for changed := true; changed; {
		changed = false
		for fn, decl := range ta.funcDecls {
			if ta.propagateFunc(fn, decl) {
				changed = true
			}
		}
	}

	// Resolve aliases before marking unknown alternatives. An early unresolved
	// local alias can acquire its original method/literal target on a later
	// fixed-point pass; it is not itself evidence of an opaque callback.
	for object := range ta.interfaceVariables {
		kind := ta.objectTaint[object]
		if variable, ok := object.(*types.Var); ok {
			kind |= ta.resultTaint[variable]
		}
		if kind != 0 && len(ta.concreteTypes[object]) == 0 {
			ta.unknownConcreteTypes[object] = true
			ta.unknownReason[object] = "unresolved " + source.fset.Position(object.Pos()).String()
		}
	}
	for object := range ta.functionVariables {
		if len(ta.functionTargets[object]) == 0 {
			ta.unknownFunctionTargets[object] = true
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
			if _, nested := node.(*ast.FuncLit); nested {
				return false
			}
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
				if _, handled, mutates := ta.reflectionCall(n); handled && mutates {
					writes = append(writes, publicationGuardStateWrite{caller: caller, pos: n.Pos(), detail: "published DBState mutated through reflected Set"})
					break
				}
				if ta.transparentCall(n) {
					break
				}
				callees := ta.dispatchCallees(n)
				allLocal := len(callees) > 0 && !ta.unknownFunctionTargets[ta.exprObject(n.Fun)]
				// A known method with the same interface is not evidence that
				// an unrelated unknown receiver owns this retained argument.
				if sel, ok := publicationGuardUnparen(n.Fun).(*ast.SelectorExpr); ok {
					if direct := publicationGuardCalledFunc(sel, source.info); direct != nil {
						sig, _ := direct.Type().(*types.Signature)
						if sig != nil && sig.Recv() != nil {
							if _, iface := types.Unalias(sig.Recv().Type()).Underlying().(*types.Interface); iface && ta.dispatchReceiverUnknown(n, sel.X) {
								allLocal = false
							}
						}
					}
				}
				for _, callee := range callees {
					if _, local := ta.funcDecls[callee]; !local {
						allLocal = false
					}
				}
				if allLocal {
					break
				}
				if selector, ok := publicationGuardUnparen(n.Fun).(*ast.SelectorExpr); ok && ta.methodReceiverTaint(selector)&(publicationGuardTaintState|publicationGuardTaintContainer) != 0 && !ta.retainedReceiverAnalyzed(selector) {
					writes = append(writes, publicationGuardStateWrite{caller: caller, pos: selector.Pos(), detail: fmt.Sprintf("published DBState receiver escapes to a call without an analyzed body: callee=%s", publicationGuardRender(source.fset, n.Fun)) + ta.originDiagnostic(selector.X)})
				}
				for _, arg := range n.Args {
					if ta.expr(arg)&(publicationGuardTaintState|publicationGuardTaintContainer) != 0 {
						if ta.privateCustodyTargetArgument(caller, n, arg) {
							continue
						}
						writes = append(writes, publicationGuardStateWrite{caller: caller, pos: arg.Pos(), detail: fmt.Sprintf("published DBState escapes to a call without an analyzed body: callee=%s argument=%s", publicationGuardRender(source.fset, n.Fun), publicationGuardRender(source.fset, arg)) + ta.originDiagnostic(arg) + ta.custodyRefusal})
					}
				}
			}
			return true
		})
	}
	return writes
}

// Unknown receiver alternatives need not own the retained alias. Resolve the
// retained receiver separately from all possible receivers used to propagate
// a retained method argument. Every applicable retained method still has its
// actual body analyzed, including mutations in those bodies.
func (ta *publicationGuardTaintAnalysis) retainedReceiverAnalyzed(selector *ast.SelectorExpr) bool {
	if ta.unknownTypes(selector.X) {
		return false
	}
	origins := ta.typesOf(selector.X)
	if len(origins) == 0 {
		return false
	}
	for typ := range origins {
		object, _, _ := types.LookupFieldOrMethod(typ, true, ta.source.pkg, selector.Sel.Name)
		method, ok := object.(*types.Func)
		if !ok {
			// This concrete error does not participate in the checked method
			// assertion, so only its other receiver alternatives can dispatch.
			continue
		}
		if ta.funcDecls[method] == nil {
			return false
		}
	}
	return true
}

func (ta *publicationGuardTaintAnalysis) propagateToFixedPoint() {
	for changed := true; changed; {
		changed = false
		for fn, decl := range ta.funcDecls {
			changed = ta.propagateFunc(fn, decl) || changed
		}
	}
}

// errors.As writes an output slot after reading its input error. A zero local
// declared immediately before this call has no prior retained value. Keep that
// one argument read separate from the value As may subsequently assign. Reuse,
// initialization, aliasing, closure capture and backward control flow refuse
// this bounded proof; output propagation and callback bodies stay analyzed.
func (ta *publicationGuardTaintAnalysis) indexFreshAsOutputReads(body *ast.BlockStmt) {
	backward := false
	ast.Inspect(body, func(n ast.Node) bool {
		if branch, ok := n.(*ast.BranchStmt); ok && branch.Tok == token.GOTO {
			backward = true
		}
		return true
	})
	if backward {
		return
	}
	ast.Inspect(body, func(n ast.Node) bool {
		if _, nested := n.(*ast.FuncLit); nested {
			return false
		}
		block, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i := 1; i < len(block.List); i++ {
			declaration, ok := block.List[i-1].(*ast.DeclStmt)
			if !ok {
				continue
			}
			general, ok := declaration.Decl.(*ast.GenDecl)
			if !ok || general.Tok != token.VAR {
				continue
			}
			var prefix []ast.Node
			switch statement := block.List[i].(type) {
			case *ast.ExprStmt:
				prefix = []ast.Node{statement.X}
			case *ast.IfStmt:
				prefix = []ast.Node{statement.Init, statement.Cond}
			case *ast.AssignStmt:
				for _, rhs := range statement.Rhs {
					prefix = append(prefix, rhs)
				}
			case *ast.ReturnStmt:
				for _, result := range statement.Results {
					prefix = append(prefix, result)
				}
			}
			for _, root := range prefix {
				if root == nil {
					continue
				}
				ast.Inspect(root, func(node ast.Node) bool {
					if _, nested := node.(*ast.FuncLit); nested {
						return false
					}
					call, ok := node.(*ast.CallExpr)
					if !ok || len(call.Args) != 2 {
						return true
					}
					fn := publicationGuardCalledFunc(call.Fun, ta.source.info)
					if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != "errors" || fn.Name() != "As" {
						return true
					}
					address, ok := publicationGuardUnparen(call.Args[1]).(*ast.UnaryExpr)
					if !ok || address.Op != token.AND {
						return true
					}
					local, ok := publicationGuardUnparen(address.X).(*ast.Ident)
					if !ok {
						return true
					}
					object := ta.identObject(local)
					zero := false
					for _, spec := range general.Specs {
						values, ok := spec.(*ast.ValueSpec)
						if !ok || len(values.Values) != 0 {
							continue
						}
						for _, name := range values.Names {
							zero = zero || ta.source.info.Defs[name] == object
						}
					}
					if !zero {
						return true
					}
					isolated := true
					// Another spec in the same var group can expose this slot
					// before the following call, even though it has no initializer.
					ast.Inspect(general, func(use ast.Node) bool {
						if name, ok := use.(*ast.Ident); ok && ta.source.info.Uses[name] == object {
							isolated = false
						}
						return true
					})
					uses := prefix
					if assignment, ok := block.List[i].(*ast.AssignStmt); ok {
						// Assignment address/index operands can run before RHS calls.
						uses = []ast.Node{assignment}
					}
					for _, expression := range uses {
						if expression == nil {
							continue
						}
						ast.Inspect(expression, func(use ast.Node) bool {
							if name, ok := use.(*ast.Ident); ok && name != local && name.Pos() < call.End() && ta.identObject(name) == object {
								isolated = false
							}
							return true
						})
					}
					if isolated {
						ta.strongReadValues[local] = &ast.BasicLit{Kind: token.INT, Value: "0"}
					}
					return true
				})
			}
		}
		return true
	})
}

// An if initializer unconditionally replaces its local before the condition.
// Preserve that value for reads confined to the if when the variable cannot be
// changed through an address, a closure, or another assignment in that if.
// All other local assignments retain the conservative fixed-point union.
func (ta *publicationGuardTaintAnalysis) indexStrongIfReads(body *ast.BlockStmt) {
	ast.Inspect(body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		statement, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		assignment, ok := statement.Init.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != len(assignment.Rhs) {
			return true
		}
		for index, lhs := range assignment.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok {
				continue
			}
			variable, ok := ta.identObject(ident).(*types.Var)
			if !ok || variable.IsField() || variable.Pkg() == nil || variable.Parent() == variable.Pkg().Scope() {
				continue
			}
			safe := true
			ast.Inspect(body, func(candidate ast.Node) bool {
				switch candidate := candidate.(type) {
				case *ast.UnaryExpr:
					if candidate.Op == token.AND && ta.exprObject(candidate.X) == variable {
						safe = false
					}
				case *ast.FuncLit:
					ast.Inspect(candidate.Body, func(inner ast.Node) bool {
						switch inner := inner.(type) {
						case *ast.AssignStmt:
							for _, target := range inner.Lhs {
								if ta.exprObject(target) == variable {
									safe = false
								}
							}
						case *ast.IncDecStmt:
							if ta.exprObject(inner.X) == variable {
								safe = false
							}
						}
						return true
					})
				}
				return true
			})
			var reads []*ast.Ident
			for _, region := range []ast.Node{statement.Cond, statement.Body, statement.Else} {
				if region == nil {
					continue
				}
				ast.Inspect(region, func(candidate ast.Node) bool {
					switch candidate := candidate.(type) {
					case *ast.FuncLit:
						return false
					case *ast.AssignStmt:
						for _, target := range candidate.Lhs {
							if ta.exprObject(target) == variable {
								safe = false
							}
						}
					case *ast.IncDecStmt:
						if ta.exprObject(candidate.X) == variable {
							safe = false
						}
					case *ast.Ident:
						if ta.identObject(candidate) == variable {
							reads = append(reads, candidate)
						}
					}
					return true
				})
			}
			if safe {
				for _, read := range reads {
					ta.strongReadValues[read] = assignment.Rhs[index]
				}
			}
		}
		return true
	})
}

func (ta *publicationGuardTaintAnalysis) callCarriesState(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		if ta.expr(arg)&(publicationGuardTaintState|publicationGuardTaintContainer) != 0 {
			return true
		}
	}
	return false
}

func (ta *publicationGuardTaintAnalysis) withDispatchOrigins(call *ast.CallExpr, body func()) {
	if !ta.collectAllOrigins && ta.allConcreteTypes != nil && ta.callCarriesState(call) {
		retained, unknown := ta.concreteTypes, ta.unknownConcreteTypes
		ta.concreteTypes, ta.unknownConcreteTypes = ta.allConcreteTypes, ta.allUnknownTypes
		defer func() { ta.concreteTypes, ta.unknownConcreteTypes = retained, unknown }()
	}
	body()
}

func (ta *publicationGuardTaintAnalysis) dispatchCallees(call *ast.CallExpr) (result []*types.Func) {
	ta.withDispatchOrigins(call, func() { result = ta.callees(call.Fun) })
	return
}

func (ta *publicationGuardTaintAnalysis) dispatchReceiverUnknown(call *ast.CallExpr, receiver ast.Expr) (result bool) {
	ta.withDispatchOrigins(call, func() { result = ta.unknownTypes(receiver) || len(ta.typesOf(receiver)) == 0 })
	return
}

func (ta *publicationGuardTaintAnalysis) originDiagnostic(expr ast.Expr) string {
	var origins []string
	for typ := range ta.typesOf(expr) {
		origins = append(origins, types.TypeString(typ, publicationGuardQualifier))
	}
	sort.Strings(origins)
	return fmt.Sprintf(" origins=%v unknown=%v reason=%s", origins, ta.unknownTypes(expr), ta.unknownReason[ta.exprObject(expr)])
}

func (ta *publicationGuardTaintAnalysis) methodReceiverTaint(selector *ast.SelectorExpr) publicationGuardTaint {
	if selection := ta.source.info.Selections[selector]; selection != nil && selection.Kind() == types.FieldVal {
		// Reading a function field does not pass the containing struct as an
		// implicit argument. Captured aliases belong to the function value itself.
		return ta.expr(selector)
	}
	return ta.expr(selector.X)
}

func (ta *publicationGuardTaintAnalysis) propagateFunc(fn *types.Func, decl *ast.FuncDecl) bool {
	changed := false
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		switch n := node.(type) {
		case *ast.ValueSpec:
			for i, value := range n.Values {
				if i < len(n.Names) {
					changed = ta.addConcreteTypes(ta.source.info.Defs[n.Names[i]], value) || changed
					changed = ta.addFunctionTargets(ta.source.info.Defs[n.Names[i]], value) || changed
					changed = ta.addObjectTaint(ta.source.info.Defs[n.Names[i]], ta.expr(value)) || changed
				}
			}
		case *ast.AssignStmt:
			if len(n.Rhs) == 1 {
				if call, ok := n.Rhs[0].(*ast.CallExpr); ok && len(n.Lhs) > 1 {
					changed = ta.propagateTuple(n.Lhs, call) || changed
				}
			}
			if len(n.Lhs) == len(n.Rhs) {
				for i := range n.Lhs {
					changed = ta.propagateAssignment(n.Lhs[i], n.Rhs[i]) || changed
				}
			}
			if len(n.Lhs) == 2 && len(n.Rhs) == 1 {
				if assertion, ok := n.Rhs[0].(*ast.TypeAssertExpr); ok {
					changed = ta.propagateAssignment(n.Lhs[0], assertion) || changed
				}
			}
		case *ast.CompositeLit:
			for _, element := range n.Elts {
				if keyed, ok := element.(*ast.KeyValueExpr); ok {
					if name, ok := keyed.Key.(*ast.Ident); ok {
						changed = ta.addConcreteTypes(ta.source.info.Uses[name], keyed.Value) || changed
						changed = ta.addFunctionTargets(ta.source.info.Uses[name], keyed.Value) || changed
					}
				}
			}
		case *ast.RangeStmt:
			kind := ta.expr(n.X)
			if n.Value != nil {
				changed = ta.addConcreteTypes(ta.exprObject(n.Value), n.X) || changed
			}
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
				changed = ta.addConcreteTypes(resultVar, result) || changed
				changed = ta.addFunctionTargets(resultVar, result) || changed
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

// A tuple result keeps each original value's provenance separate. In
// particular, a callback returned by a constructor is not an opaque field.
func (ta *publicationGuardTaintAnalysis) propagateTuple(lhs []ast.Expr, call *ast.CallExpr) bool {
	changed := false
	if fn := publicationGuardCalledFunc(call.Fun, ta.source.info); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "iter" && fn.Name() == "Pull" && len(call.Args) == 1 && ta.verifySource("iter/iter.go", "ae73fb7933353c001ce3f85095ce248848a724e24490444918b467f725082019") {
		// The pinned Pull implementation's next and stop may resume only this
		// original sequence. Analyze that sequence body for either callback edge.
		for _, target := range lhs {
			changed = ta.addFunctionTargets(ta.exprObject(target), call.Args[0]) || changed
		}
		return changed
	}
	for _, fn := range ta.dispatchCallees(call) {
		sig, _ := fn.Type().(*types.Signature)
		if sig == nil || sig.Results().Len() != len(lhs) {
			continue
		}
		for i, target := range lhs {
			original := sig.Results().At(i)
			object := ta.exprObject(target)
			changed = ta.addObjectTaint(object, ta.resultTaint[original]) || changed
			if object == nil {
				continue
			}
			if ta.concreteTypes[object] == nil {
				ta.concreteTypes[object] = make(map[types.Type]struct{})
			}
			for typ := range ta.concreteTypes[original] {
				found := false
				for prior := range ta.concreteTypes[object] {
					if types.Identical(typ, prior) {
						found = true
						break
					}
				}
				if !found {
					ta.concreteTypes[object][typ] = struct{}{}
					changed = true
				}
			}
			if ta.unknownConcreteTypes[original] && !ta.unknownConcreteTypes[object] {
				ta.unknownConcreteTypes[object] = true
				ta.unknownReason[object] = ta.source.fset.Position(call.Pos()).String() + " " + publicationGuardRender(ta.source.fset, call) + " <- " + ta.unknownReason[original]
				changed = true
			}
			if ta.functionTargets[object] == nil {
				ta.functionTargets[object] = make(map[*types.Func]struct{})
			}
			for callback := range ta.functionTargets[original] {
				if _, found := ta.functionTargets[object][callback]; !found {
					ta.functionTargets[object][callback] = struct{}{}
					changed = true
				}
			}
		}
	}
	return changed
}

func (ta *publicationGuardTaintAnalysis) propagateAssignment(lhs, rhs ast.Expr) bool {
	kind := ta.expr(rhs)
	functionChanged := ta.addConcreteTypes(ta.exprObject(lhs), rhs)
	functionChanged = ta.addFunctionTargets(ta.exprObject(lhs), rhs) || functionChanged
	if ident, ok := lhs.(*ast.Ident); ok {
		return ta.addObjectTaint(ta.identObject(ident), kind) || functionChanged
	}
	if index, ok := lhs.(*ast.IndexExpr); ok {
		return ta.addExprObjectTaint(index.X, kind) || functionChanged
	}
	sel, ok := lhs.(*ast.SelectorExpr)
	if !ok || kind == 0 {
		return functionChanged
	}
	selection := ta.source.info.Selections[sel]
	if selection == nil {
		return false
	}
	changed := ta.addObjectTaint(selection.Obj(), kind) || functionChanged
	if kind&publicationGuardTaintState != 0 {
		switch selection.Obj() {
		case ta.snapshotStateField:
			return ta.addExprObjectTaint(sel.X, publicationGuardTaintSnapshot) || changed
		case ta.viewStateField:
			return ta.addExprObjectTaint(sel.X, publicationGuardTaintSnapshotView) || changed
		}
	}
	// A retained field makes the container an escape risk, not a DBState
	// pointer. Keep unrelated fields/method results separate from that alias.
	return ta.addExprObjectTaint(sel.X, publicationGuardTaintContainer) || changed
}

func (ta *publicationGuardTaintAnalysis) propagateCall(call *ast.CallExpr) bool {
	if ta.joinCall(call) {
		return ta.retainErrorOperands(publicationGuardCalledFunc(call.Fun, ta.source.info), "joinError", "errs", call.Args)
	}
	if callbacks, handled := ta.formattingCall(call); handled {
		changed := false
		for method, arg := range callbacks {
			sig := method.Type().(*types.Signature)
			changed = ta.addObjectTaint(sig.Recv(), ta.expr(arg)) || changed
			changed = ta.addConcreteTypes(sig.Recv(), arg) || changed
		}
		fn := publicationGuardCalledFunc(call.Fun, ta.source.info)
		changed = ta.retainErrorOperands(fn, "wrapError", "err", call.Args[1:]) || changed
		changed = ta.retainErrorOperands(fn, "wrapErrors", "errs", call.Args[1:]) || changed
		return changed
	}
	if fn := publicationGuardCalledFunc(call.Fun, ta.source.info); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "fmt" && fn.Name() == "Errorf" {
		return false
	}
	changed := false
	if fn := publicationGuardCalledFunc(call.Fun, ta.source.info); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "errors" && fn.Name() == "As" && len(call.Args) == 2 {
		if target, ok := publicationGuardUnparen(call.Args[1]).(*ast.UnaryExpr); ok && target.Op == token.AND {
			changed = ta.addConcreteTypes(ta.exprObject(target.X), call.Args[0]) || changed
			changed = ta.addExprObjectTaint(target.X, ta.expr(call.Args[0])) || changed
		}
	}
	for _, callee := range ta.dispatchCallees(call) {
		sig, _ := callee.Type().(*types.Signature)
		if sig == nil {
			continue
		}
		if sel, ok := publicationGuardUnparen(call.Fun).(*ast.SelectorExpr); ok && sig.Recv() != nil {
			changed = ta.addObjectTaint(sig.Recv(), ta.expr(sel.X)) || changed
			changed = ta.addConcreteTypes(sig.Recv(), sel.X) || changed
		}
		for i, arg := range call.Args {
			if i >= sig.Params().Len() {
				break
			}
			changed = ta.addConcreteTypes(sig.Params().At(i), arg) || changed
			changed = ta.addObjectTaint(sig.Params().At(i), ta.expr(arg)) || changed
			changed = ta.addFunctionTargets(sig.Params().At(i), arg) || changed
		}
	}
	return changed
}

func (ta *publicationGuardTaintAnalysis) exprObject(expr ast.Expr) types.Object {
	switch e := publicationGuardUnparen(expr).(type) {
	case *ast.Ident:
		return ta.identObject(e)
	case *ast.SelectorExpr:
		if selection := ta.source.info.Selections[e]; selection != nil {
			return selection.Obj()
		}
		return ta.source.info.Uses[e.Sel]
	case *ast.IndexExpr:
		return ta.exprObject(e.X)
	case *ast.SliceExpr:
		return ta.exprObject(e.X)
	}
	return nil
}
func (ta *publicationGuardTaintAnalysis) addFunctionTargets(object types.Object, expr ast.Expr) bool {
	if object == nil {
		return false
	}
	changed := false
	if typ := ta.source.info.TypeOf(expr); typ != nil {
		if _, ok := types.Unalias(typ).Underlying().(*types.Signature); ok {
			ta.functionVariables[object] = true
			if source := ta.exprObject(expr); source != nil {
				if _, direct := source.(*types.Func); !direct {
					ta.functionVariables[source] = true
				}
				if ta.unknownFunctionTargets[source] && !ta.unknownFunctionTargets[object] {
					ta.unknownFunctionTargets[object] = true
					changed = true
				}
			}
		}
	}
	for _, callee := range ta.callees(expr) {
		if ta.functionTargets[object] == nil {
			ta.functionTargets[object] = make(map[*types.Func]struct{})
		}
		if _, found := ta.functionTargets[object][callee]; !found {
			ta.functionTargets[object][callee] = struct{}{}
			changed = true
		}
	}
	return changed
}

// Track concrete origins through the same assignments/parameters/results as
// retained aliases. Interface dispatch must use actual possible receiver types,
// rather than accepting every loaded method with the same spelling.
func (ta *publicationGuardTaintAnalysis) typesOf(expr ast.Expr) map[types.Type]struct{} {
	result := make(map[types.Type]struct{})
	add := func(values map[types.Type]struct{}) {
		for typ := range values {
			result[typ] = struct{}{}
		}
	}
	typ := ta.source.info.TypeOf(expr)
	if basic, ok := typ.(*types.Basic); ok && basic.Kind() == types.UntypedNil {
		return result
	}
	if typ != nil {
		switch types.Unalias(typ).Underlying().(type) {
		case *types.Interface, *types.Slice, *types.Array, *types.Tuple:
		default:
			result[typ] = struct{}{}
			return result
		}
	}
	add(ta.concreteTypes[ta.exprObject(expr)])
	switch e := publicationGuardUnparen(expr).(type) {
	case *ast.CallExpr:
		if _, formatted := ta.formattingCall(e); formatted {
			fn := publicationGuardCalledFunc(e.Fun, ta.source.info)
			for _, name := range []string{"wrapError", "wrapErrors"} {
				if object := fn.Pkg().Scope().Lookup(name); object != nil {
					result[types.NewPointer(object.Type())] = struct{}{}
				}
			}
		} else if ta.joinCall(e) {
			fn := publicationGuardCalledFunc(e.Fun, ta.source.info)
			if named := fn.Pkg().Scope().Lookup("joinError"); named != nil {
				result[types.NewPointer(named.Type())] = struct{}{}
			}
			for _, arg := range e.Args {
				add(ta.typesOf(arg))
			}
		} else {
			for _, fn := range ta.dispatchCallees(e) {
				sig, _ := fn.Type().(*types.Signature)
				if sig != nil && sig.Results().Len() == 1 {
					add(ta.concreteTypes[sig.Results().At(0)])
				}
			}
		}
	case *ast.TypeAssertExpr:
		add(ta.typesOf(e.X))
	case *ast.IndexExpr:
		add(ta.typesOf(e.X))
	case *ast.CompositeLit:
		for _, element := range e.Elts {
			if keyed, ok := element.(*ast.KeyValueExpr); ok {
				add(ta.typesOf(keyed.Value))
			} else {
				add(ta.typesOf(element))
			}
		}
	}
	return result
}
func (ta *publicationGuardTaintAnalysis) unknownTypes(expr ast.Expr) bool {
	// A type-switch guard has no standalone expression type. Its retained
	// alternatives still come from the original operand, rather than an opaque
	// producer introduced by the syntax of the checked assertion.
	if assertion, ok := publicationGuardUnparen(expr).(*ast.TypeAssertExpr); ok {
		return ta.unknownTypes(assertion.X)
	}
	typ := ta.source.info.TypeOf(expr)
	if typ == nil {
		return true
	}
	if _, iface := types.Unalias(typ).Underlying().(*types.Interface); !iface {
		return false
	}
	if ta.unknownConcreteTypes[ta.exprObject(expr)] {
		return true
	}
	switch e := publicationGuardUnparen(expr).(type) {
	case *ast.TypeAssertExpr:
		return ta.unknownTypes(e.X)
	case *ast.CallExpr:
		if ta.joinCall(e) {
			// The pinned Join body returns its concrete joinError container.
			// Unknown retained children belong to its errs field, populated by
			// retainErrorOperands and analyzed when the body unwraps/dispatches.
			// An unrelated opaque operand is not an opaque receiver alternative.
			return false
		}
		if _, formatted := ta.formattingCall(e); formatted {
			return false
		}
		for _, fn := range ta.dispatchCallees(e) {
			if ta.funcDecls[fn] == nil {
				return true
			}
			sig := fn.Type().(*types.Signature)
			if sig.Results().Len() == 1 && ta.unknownConcreteTypes[sig.Results().At(0)] {
				return true
			}
		}
	}
	return false
}
func (ta *publicationGuardTaintAnalysis) addConcreteTypes(object types.Object, expr ast.Expr) bool {
	// Origins describe the retained alias carried on this edge. An unrelated
	// opaque error from (for example) File.Close does not become a published
	// state owner merely because another invocation shares an error parameter.
	// A genuinely opaque call receiving a retained alias is still rejected at
	// that original call, before its result can enter a local wrapper.
	if object == nil || (!ta.collectAllOrigins && ta.expr(expr) == 0) {
		return false
	}
	changed := false
	// In a comma-ok assertion go/types assigns the assertion a tuple type.
	// The receiving variable is still an interface and must retain the opaque
	// alternatives of the asserted operand, as well as its concrete origins.
	expressionType := ta.source.info.TypeOf(expr)
	if _, assertion := publicationGuardUnparen(expr).(*ast.TypeAssertExpr); assertion {
		expressionType = object.Type()
	}
	if typ := expressionType; typ != nil {
		if _, iface := types.Unalias(typ).Underlying().(*types.Interface); iface {
			ta.interfaceVariables[object] = true
			if source := ta.exprObject(expr); source != nil {
				ta.interfaceVariables[source] = true
			}
			if ta.unknownTypes(expr) && !ta.unknownConcreteTypes[object] {
				ta.unknownConcreteTypes[object] = true
				ta.unknownReason[object] = ta.source.fset.Position(expr.Pos()).String() + " " + publicationGuardRender(ta.source.fset, expr) + " <- " + ta.unknownReason[ta.exprObject(expr)]
				changed = true
			}
		}
	}
	for typ := range ta.typesOf(expr) {
		if ta.concreteTypes[object] == nil {
			ta.concreteTypes[object] = make(map[types.Type]struct{})
		}
		found := false
		for prior := range ta.concreteTypes[object] {
			if types.Identical(prior, typ) {
				found = true
				break
			}
		}
		if !found {
			ta.concreteTypes[object][typ] = struct{}{}
			changed = true
		}
	}
	return changed
}

func (ta *publicationGuardTaintAnalysis) callees(expr ast.Expr) []*types.Func {
	if literal, ok := publicationGuardUnparen(expr).(*ast.FuncLit); ok {
		if target := ta.literalTargets[literal]; target != nil {
			return []*types.Func{target}
		}
	}
	direct := publicationGuardCalledFunc(expr, ta.source.info)
	if direct != nil {
		sig, _ := direct.Type().(*types.Signature)
		if sig != nil && sig.Recv() != nil {
			if iface, ok := types.Unalias(sig.Recv().Type()).Underlying().(*types.Interface); ok {
				var matches []*types.Func
				selector, _ := publicationGuardUnparen(expr).(*ast.SelectorExpr)
				if selector == nil {
					return []*types.Func{direct}
				}
				for typ := range ta.typesOf(selector.X) {
					if !types.Implements(typ, iface) {
						continue
					}
					object, _, _ := types.LookupFieldOrMethod(typ, true, direct.Pkg(), direct.Name())
					if method, ok := object.(*types.Func); ok {
						matches = append(matches, method)
					}
				}
				if len(matches) > 0 {
					return matches
				}

			}
		}
		return []*types.Func{direct}
	}
	var matches []*types.Func
	for method := range ta.functionTargets[ta.exprObject(expr)] {
		matches = append(matches, method)
	}
	return matches
}

// Join only retains its original arguments; Go's pinned source body is loaded
// below. Keep this summary parametric so one retained cleanup error does not
// taint every unrelated call to Join in the package.
func (ta *publicationGuardTaintAnalysis) joinCall(call *ast.CallExpr) bool {
	fn := publicationGuardCalledFunc(call.Fun, ta.source.info)
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != "errors" || fn.Name() != "Join" {
		return false
	}
	decl := ta.funcDecls[fn]
	if decl == nil {
		return false
	}
	// Fail closed if the loaded source gains callbacks or indirect writes.
	// The current body only counts arguments and appends them to e.errs.
	safe := true
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncLit, *ast.GoStmt, *ast.DeferStmt, *ast.SendStmt:
			safe = false
		case *ast.CallExpr:
			ident, ok := n.Fun.(*ast.Ident)
			if !ok {
				safe = false
				break
			}
			builtin, ok := ta.source.info.Uses[ident].(*types.Builtin)
			if !ok || (builtin.Name() != "make" && builtin.Name() != "append") {
				safe = false
			}
		case *ast.SelectorExpr:
			ident, ok := n.X.(*ast.Ident)
			if !ok || ident.Name != "e" || n.Sel.Name != "errs" {
				safe = false
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				switch lhs.(type) {
				case *ast.Ident, *ast.SelectorExpr:
				default:
					safe = false
				}
			}
		case *ast.IncDecStmt:
			if _, ok := n.X.(*ast.Ident); !ok {
				safe = false
			}
		}
		return safe
	})
	return safe
}

// Error formatting can invoke Error/String/Format. Analyze every concrete
// retained operand's applicable method; an opaque implementation refuses this
// summary. The formatting/retention behavior is bound to the pinned source.
func (ta *publicationGuardTaintAnalysis) formattingCall(call *ast.CallExpr) (map[*types.Func]ast.Expr, bool) {
	fn := publicationGuardCalledFunc(call.Fun, ta.source.info)
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != "fmt" || fn.Name() != "Errorf" || len(call.Args) == 0 {
		return nil, false
	}
	if literal, ok := call.Args[0].(*ast.BasicLit); !ok || literal.Kind != token.STRING {
		return nil, false
	}
	if !ta.verifySource("fmt/errors.go", "01fd304868493a3aa1198cc79a56274930f687353e9405211ca063c1b8195ea6") {
		return nil, false
	}
	callbacks := make(map[*types.Func]ast.Expr)
	errorInterface := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	for _, arg := range call.Args[1:] {
		if ta.expr(arg)&(publicationGuardTaintState|publicationGuardTaintContainer) == 0 {
			continue
		}
		origins := ta.typesOf(arg)
		if len(origins) == 0 || ta.unknownTypes(arg) {
			return nil, false
		}
		for typ := range origins {
			if !types.Implements(typ, errorInterface) {
				return nil, false
			}
			for _, name := range []string{"Error", "String", "Format"} {
				object, _, _ := types.LookupFieldOrMethod(typ, true, fn.Pkg(), name)
				if method, ok := object.(*types.Func); ok {
					if ta.funcDecls[method] == nil {
						return nil, false
					}
					callbacks[method] = arg
				}
			}
		}
	}
	return callbacks, true
}
func (ta *publicationGuardTaintAnalysis) retainErrorOperands(fn *types.Func, typeName, fieldName string, args []ast.Expr) bool {
	object := fn.Pkg().Scope().Lookup(typeName)
	if object == nil {
		return false
	}
	field, _, _ := types.LookupFieldOrMethod(types.NewPointer(object.Type()), true, fn.Pkg(), fieldName)
	changed := false
	for _, arg := range args {
		changed = ta.addObjectTaint(field, ta.expr(arg)) || changed
		changed = ta.addConcreteTypes(field, arg) || changed
	}
	return changed
}

// These summaries are bound to the audited toolchain source. A source change
// fails closed until the header-only operations are audited again. TypeOf only
// exposes type metadata; ValueOf/Elem preserve the original alias, and Set is
// allowed only when its destination has no published-state alias.
func (ta *publicationGuardTaintAnalysis) verifySource(relative, digest string) bool {
	if verified, found := ta.verifiedSource[relative]; found {
		return verified
	}
	content, err := os.ReadFile(filepath.Join(runtime.GOROOT(), "src", relative))
	verified := err == nil && fmt.Sprintf("%x", sha256.Sum256(content)) == digest
	ta.verifiedSource[relative] = verified
	return verified
}
func (ta *publicationGuardTaintAnalysis) reflectionCall(call *ast.CallExpr) (kind publicationGuardTaint, handled bool, writes bool) {
	fn := publicationGuardCalledFunc(call.Fun, ta.source.info)
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != "internal/reflectlite" {
		return 0, false, false
	}
	if !ta.verifySource("internal/reflectlite/type.go", "5ce048cf76070f0c0734694bc5c15e96886bd44f726c32f5f72ca33a1c5f94dd") || !ta.verifySource("internal/reflectlite/value.go", "37526f8c1c775bf455060cd93c6cfcc2518ba0de38f63d78cdd1def75b597212") {
		return 0, false, false
	}
	sig, _ := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		switch fn.Name() {
		case "TypeOf":
			return 0, true, false
		case "ValueOf":
			if len(call.Args) == 1 {
				return ta.expr(call.Args[0]), true, false
			}
		}
		return 0, false, false
	}
	selector, ok := publicationGuardUnparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return 0, false, false
	}
	if types.TypeString(sig.Recv().Type(), publicationGuardQualifier) != "internal/reflectlite.Value" {
		return 0, false, false
	}
	receiver := ta.expr(selector.X)
	switch fn.Name() {
	case "Elem":
		return receiver, true, false
	case "Type", "Kind", "IsNil":
		return 0, true, false
	case "Set":
		return 0, true, receiver&publicationGuardTaintState != 0
	}
	return 0, false, false
}

func (ta *publicationGuardTaintAnalysis) nilFunction(expr ast.Expr) bool {
	object, ok := ta.exprObject(expr).(*types.Var)
	if !ok || object.Pkg() != ta.source.pkg || object.Exported() || (!object.IsField() && object.Parent() != ta.source.pkg.Scope()) {
		return false
	}
	if _, ok := object.Type().Underlying().(*types.Signature); !ok {
		return false
	}
	declared, safe := false, true
	for _, file := range ta.source.files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				for _, name := range n.Names {
					if ta.source.info.Defs[name] == object {
						declared = true
						if len(n.Values) != 0 {
							safe = false
						}
					}
				}
			case *ast.Field:
				for _, name := range n.Names {
					if ta.source.info.Defs[name] == object {
						declared = true
					}
				}
			case *ast.KeyValueExpr:
				if name, ok := n.Key.(*ast.Ident); ok && ta.source.info.Uses[name] == object {
					safe = false
				}
			case *ast.CompositeLit:
				typ := ta.source.info.TypeOf(n)
				if typ == nil {
					break
				}
				if ptr, ok := typ.(*types.Pointer); ok {
					typ = ptr.Elem()
				}
				if fields, ok := typ.Underlying().(*types.Struct); ok {
					for i, element := range n.Elts {
						if _, keyed := element.(*ast.KeyValueExpr); !keyed && i < fields.NumFields() && fields.Field(i) == object {
							safe = false
						}
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if ta.exprObject(lhs) == object {
						safe = false
					}
				}
			case *ast.UnaryExpr:
				if n.Op == token.AND && ta.exprObject(n.X) == object {
					safe = false
				}
			}
			return safe
		})
	}
	return declared && safe
}

// A checked type assertion can make an interface call unreachable for every
// concrete retained origin. This is distinct from an unknown implementation.
func (ta *publicationGuardTaintAnalysis) emptyInterfaceDispatch(expr ast.Expr) bool {
	selector, ok := publicationGuardUnparen(expr).(*ast.SelectorExpr)
	if !ok || ta.unknownTypes(selector.X) {
		return false
	}
	direct := publicationGuardCalledFunc(expr, ta.source.info)
	if direct == nil {
		return false
	}
	sig, _ := direct.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil {
		return false
	}
	iface, ok := sig.Recv().Type().Underlying().(*types.Interface)
	if !ok {
		return false
	}
	origins := ta.typesOf(selector.X)
	if len(origins) == 0 {
		return false
	}
	for typ := range origins {
		if types.Implements(typ, iface) {
			return false
		}
	}
	return true
}

func (ta *publicationGuardTaintAnalysis) transparentCall(call *ast.CallExpr) bool {
	if fn := publicationGuardCalledFunc(call.Fun, ta.source.info); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "os" && fn.Name() == "IsNotExist" {
		// Pinned underlyingErrorIs unwraps only concrete os wrappers and invokes
		// only scalar syscall.Errno.Is; arbitrary error Is/Unwrap methods do not run.
		var syscallSource, syscallDigest string
		switch runtime.GOOS {
		case "linux", "darwin":
			syscallSource, syscallDigest = "syscall/syscall_unix.go", "31fd5823803feb47f4ea0f13d3e0db5bd3cc6e37467bbebd8abbba8467cb20a7"
		case "windows":
			syscallSource, syscallDigest = "syscall/syscall_windows.go", "f1b22dde6bb980cf036561cee7ece968daa260cb622f32c96cebd52477b7a346"
		}
		if syscallSource != "" && ta.verifySource("os/error.go", "f073f7fb0bad2cd69df450956b4a267b5ecfb519a9089d44fe6b6e2d6f766e18") && ta.verifySource("os/error_errno.go", "3894fbb36a02dbdb543c074a2d7d316be881861dd787bb7eec06a3c074ef650b") && ta.verifySource(syscallSource, syscallDigest) {
			return true
		}
	}
	emptyDispatch := false
	ta.withDispatchOrigins(call, func() { emptyDispatch = ta.emptyInterfaceDispatch(call.Fun) })
	if emptyDispatch {
		return true
	}
	if ta.nilFunction(call.Fun) {
		return true
	}
	if _, handled := ta.formattingCall(call); handled {
		return true
	}
	if _, handled, writes := ta.reflectionCall(call); handled && !writes {
		return true
	}
	if fn := publicationGuardCalledFunc(call.Fun, ta.source.info); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "sync/atomic" && fn.Name() == "Store" {
		if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
			if pointer, ok := sig.Recv().Type().(*types.Pointer); ok {
				if named, ok := pointer.Elem().(*types.Named); ok && named.Obj().Name() == "Pointer" {
					return true
				}
			}
		}
	}
	if selector, ok := publicationGuardUnparen(call.Fun).(*ast.SelectorExpr); ok {
		if name, ok := selector.X.(*ast.Ident); ok {
			if pkg, ok := ta.source.info.Uses[name].(*types.PkgName); ok && pkg.Imported().Path() == "unsafe" {
				switch selector.Sel.Name {
				case "Sizeof", "Alignof", "Offsetof":
					return true
				}
			}
		}
	}
	if ident, ok := publicationGuardUnparen(call.Fun).(*ast.Ident); ok {
		if builtin, ok := ta.source.info.Uses[ident].(*types.Builtin); ok {
			switch builtin.Name() {
			case "append", "len", "cap", "copy", "clear", "delete":
				// These operate on the container/scalar header. They cannot
				// mutate a DBState pointee or invoke a user method.
				return true
			}
		}
	}
	if ta.joinCall(call) {
		return true
	}
	if fn := publicationGuardCalledFunc(call.Fun, ta.source.info); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "unsafe" {
		switch fn.Name() {
		case "Sizeof", "Alignof", "Offsetof":
			return true
		}
	}
	// Exact atomic slots/callers are independently inventoried above. They store
	// the state pointer without mutating its pointee; unknown atomic aliases are
	// still rejected by the slot escape analysis.
	if sel, ok := publicationGuardUnparen(call.Fun).(*ast.SelectorExpr); ok {
		_, _, atomic := publicationGuardAtomicMutation(sel, ta.source.info, map[string]types.Type{"state": ta.dbStateType, "snapshotViewRO": ta.snapshotViewType})
		return atomic
	}
	return false
}

func (ta *publicationGuardTaintAnalysis) expr(expr ast.Expr) publicationGuardTaint {
	switch expr := publicationGuardUnparen(expr).(type) {
	case *ast.Ident:
		if value := ta.strongReadValues[expr]; value != nil {
			return ta.expr(value)
		}
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
		if ta.expr(expr.X)&(publicationGuardTaintState|publicationGuardTaintContainer) != 0 && publicationGuardIsPointerTo(expr, ta.source.info, ta.dbStateType) {
			return publicationGuardTaintState
		}
		return 0
	case *ast.CallExpr:
		if kind, handled, _ := ta.reflectionCall(expr); handled {
			return kind
		}
		if _, formatted := ta.formattingCall(expr); formatted {
			var kind publicationGuardTaint
			for _, arg := range expr.Args[1:] {
				kind |= ta.expr(arg)
			}
			return kind
		}
		if ta.joinCall(expr) {
			var kind publicationGuardTaint
			for _, arg := range expr.Args {
				kind |= ta.expr(arg)
			}
			return kind
		}
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
		if ident, ok := publicationGuardUnparen(expr.Fun).(*ast.Ident); ok {
			if builtin, ok := ta.source.info.Uses[ident].(*types.Builtin); ok && builtin.Name() == "append" {
				var kind publicationGuardTaint
				for _, arg := range expr.Args {
					kind |= ta.expr(arg)
				}
				return kind
			}
		}
		var kind publicationGuardTaint
		for _, callee := range ta.dispatchCallees(expr) {
			sig, _ := callee.Type().(*types.Signature)
			if sig != nil && sig.Results() != nil && sig.Results().Len() == 1 {
				kind |= ta.resultTaint[sig.Results().At(0)]
			}
		}
		return kind
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
	case *ast.SliceExpr:
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
	want := publicationGuardTaintContainer
	switch types.Unalias(typ).Underlying().(type) {
	case *types.Slice, *types.Array, *types.Map:
		want = publicationGuardTaintState
	}
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
		// Track this field without treating every other field of its enclosing
		// object as the same pointer/container. Selector lookup retains this
		// exact field's taint, including nested holders passed to unknown calls.
		return ta.addObjectTaint(selection.Obj(), kind)
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
		Imports    []string
		Dir        string
		GoFiles    []string
		CgoFiles   []string
		Export     string
	}
	exports := make(map[string]string)
	packages := make(map[string]listedPackage)
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
		packages[listed.ImportPath] = listed
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
	fallback := importer.ForCompiler(fset, "gc", lookup)
	// Export data for a dependent package contains its own imported named
	// types. Source-check every local reverse dependency of the body-audited
	// package so all callers share the same original type identities.
	sourcePaths := map[string]bool{"errors": true, "fmt": true, "github.com/snissn/gomap/TreeDB/internal/rootpublication": true, "github.com/snissn/gomap/TreeDB/pager": true, "github.com/snissn/gomap/TreeDB/freelist": true, "github.com/snissn/gomap/TreeDB/internal/bulk": true, "github.com/snissn/gomap/TreeDB/internal/commandwalbarrier": true, "github.com/snissn/gomap/TreeDB/internal/iterator": true}
	for changed := true; changed; {
		changed = false
		for path, listed := range packages {
			if sourcePaths[path] || !strings.HasPrefix(path, "github.com/snissn/gomap/") {
				continue
			}
			for _, dependency := range listed.Imports {
				if sourcePaths[dependency] {
					sourcePaths[path] = true
					changed = true
					break
				}
			}
		}
	}
	checked := make(map[string]*types.Package)
	extraFiles := make([]*ast.File, 0)
	custodyFiles := make([]*ast.File, 0)
	var sourceImporter publicationGuardImporter
	sourceImporter = func(path string) (*types.Package, error) {
		if pkg := checked[path]; pkg != nil {
			return pkg, nil
		}
		if !sourcePaths[path] {
			return fallback.Import(path)
		}
		listed := packages[path]
		var parsed []*ast.File
		for _, name := range listed.GoFiles {
			file, e := parser.ParseFile(fset, filepath.Join(listed.Dir, name), nil, parser.SkipObjectResolution)
			if e != nil {
				return nil, e
			}
			parsed = append(parsed, file)
		}
		config := types.Config{Importer: sourceImporter}
		pkg, e := config.Check(path, fset, parsed, info)
		if e != nil {
			return nil, e
		}
		checked[path] = pkg
		custodyFiles = append(custodyFiles, parsed...)
		if path == "errors" || path == "github.com/snissn/gomap/TreeDB/internal/rootpublication" || path == "github.com/snissn/gomap/TreeDB/internal/bulk" || path == "github.com/snissn/gomap/TreeDB/internal/commandwalbarrier" || path == "github.com/snissn/gomap/TreeDB/internal/iterator" {
			extraFiles = append(extraFiles, parsed...)
		} else if path == "fmt" || path == "github.com/snissn/gomap/TreeDB/internal/valuelog" || path == "github.com/snissn/gomap/TreeDB/pager" || path == "github.com/snissn/gomap/TreeDB/freelist" {
			// Load only the original callback/retention adapter bodies needed at
			// these boundaries, keeping their exact checked type identities.
			for _, file := range parsed {
				for _, declaration := range file.Decls {
					fn, ok := declaration.(*ast.FuncDecl)
					if !ok {
						continue
					}
					object, _ := info.Defs[fn.Name].(*types.Func)
					adapter := fn.Name.Name == "WithStableResourceFile" || fn.Name.Name == "FailCOWCandidateV1"
					errorBody := (path == "fmt" || path == "github.com/snissn/gomap/TreeDB/pager" || path == "github.com/snissn/gomap/TreeDB/internal/valuelog") && (fn.Name.Name == "Errorf" || fn.Name.Name == "Error" || fn.Name.Name == "Unwrap")
					if adapter || errorBody || publicationGuardCustodyMethod(object) {
						extraFiles = append(extraFiles, &ast.File{Decls: []ast.Decl{fn}})
					}
				}
			}
		}
		return pkg, nil
	}
	config := types.Config{Importer: sourceImporter}
	pkg, err := config.Check(publicationGuardPackagePath, fset, files, info)
	if err != nil {
		t.Fatalf("type-check production package: %v", err)
	}
	files = append(files, extraFiles...)
	return &publicationGuardSource{fset: fset, pkg: pkg, files: files, custodyFiles: custodyFiles, info: info}
}

func loadPublicationGuardFixture(t *testing.T, body string) *publicationGuardSource {
	t.Helper()
	const prelude = `package guardfixture
import ("sync/atomic";"fmt";"errors")
var _=fmt.Errorf
var _=errors.As
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

func publicationGuardRender(fset *token.FileSet, node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, fset, node); err != nil {
		return fmt.Sprintf("%T", node)
	}
	return out.String()
}

type publicationGuardImporter func(string) (*types.Package, error)

func (f publicationGuardImporter) Import(path string) (*types.Package, error) { return f(path) }
