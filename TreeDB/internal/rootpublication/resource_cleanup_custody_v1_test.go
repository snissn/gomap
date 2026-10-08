package rootpublication

import (
	"errors"
	"os"
	"testing"
)

var errCleanupCustodyFixtureV1 = errors.New("cleanup custody fixture")

type cleanupCustodyEnvironmentV1 struct {
	calls         int
	ready         bool
	callback      func()
	panicNext     bool
	completeError error
}

func (*cleanupCustodyEnvironmentV1) ReleaseStableResource() {
	panic("checked environment used void callback")
}
func (e *cleanupCustodyEnvironmentV1) AdvanceStableResourceCleanupV1() (StableCleanupOutcomeV1, error) {
	e.calls++
	if e.callback != nil {
		call := e.callback
		e.callback = nil
		call()
	}
	if e.panicNext {
		panic("uncertain cleanup")
	}
	if !e.ready {
		return StableCleanupOutcomeV1{Phase: StableCleanupPendingV1, Debt: StableCleanupExactHoldsV1, PendingRoles: 1}, errCleanupCustodyFixtureV1
	}
	return StableCleanupOutcomeV1{Phase: StableCleanupCompleteV1, ConsumedRoles: 1}, e.completeError
}
func cleanupCustodyTokenV1(t *testing.T, fileSpec StableResourceSpec, env *cleanupCustodyEnvironmentV1) *StableResourceToken {
	t.Helper()
	fileSpec.ReleaseEnvironment = env
	token, err := NewStableResourceToken(fileSpec)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestStableSetCheckedReleaseRetainsUnfinishedRoleAndSkipsConsumedToken(t *testing.T) {
	file := operationLifetimeFile(t)
	firstEnv := &cleanupCustodyEnvironmentV1{ready: true}
	pending := &cleanupCustodyEnvironmentV1{}
	firstSpec := operationLifetimeSpec(file)
	firstSpec.ResourceID = "first"
	firstSpec.Generation = 1
	// Distinct logical generations preserve two real release roles.
	secondSpec := operationLifetimeSpec(operationLifetimeFile(t))
	secondSpec.ResourceID = "second"
	secondSpec.Generation = 2
	first := cleanupCustodyTokenV1(t, firstSpec, firstEnv)
	second := cleanupCustodyTokenV1(t, secondSpec, pending)
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(first); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(second); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err = set.Release(); !errors.Is(err, errCleanupCustodyFixtureV1) {
		t.Fatalf("first release: %v", err)
	}
	if set.Owner() == ResourceOwnerReleased || second.CleanupCompleteV1() || firstEnv.calls != 1 || pending.calls != 1 {
		t.Fatal("set dropped unfinished role")
	}
	pending.ready = true
	if err = set.Release(); err != nil {
		t.Fatal(err)
	}
	if set.Owner() != ResourceOwnerReleased || firstEnv.calls != 1 || pending.calls != 2 {
		t.Fatal("consumed role was replayed")
	}
}

func TestStableBuilderNestedCleanupFailuresKeepBothActualOwners(t *testing.T) {
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	builder := NewStableResourceSetBuilder()
	base, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(base); err != nil {
		t.Fatal(err)
	}
	outerEnv, innerEnv := &cleanupCustodyEnvironmentV1{}, &cleanupCustodyEnvironmentV1{}
	inner := cleanupCustodyTokenV1(t, spec, innerEnv)
	outer := cleanupCustodyTokenV1(t, spec, outerEnv)
	outerEnv.callback = func() {
		if builder.State() != ResourceOwnerBuilder {
			t.Error("callback before state handoff")
		}
		if err := builder.Add(inner); !errors.Is(err, errCleanupCustodyFixtureV1) {
			t.Errorf("nested Add: %v", err)
		}
		if err := builder.AbandonCheckedV1(); !errors.Is(err, ErrStableResourceOperationBusy) {
			t.Errorf("running outer Abandon: %v", err)
		}
	}
	if err = builder.Add(outer); !errors.Is(err, errCleanupCustodyFixtureV1) {
		t.Fatalf("outer Add: %v", err)
	}
	builder.mu.Lock()
	count := 0
	for op := builder.pendingCleanup; op != nil; op = op.next {
		if op.failed {
			count++
		}
	}
	builder.mu.Unlock()
	if count != 2 || outer.CleanupCompleteV1() || inner.CleanupCompleteV1() {
		t.Fatal("nested failed owner overwritten")
	}
	outerEnv.ready = true
	innerEnv.ready = true
	if err = builder.AbandonCheckedV1(); err != nil {
		t.Fatal(err)
	}
	if builder.State() != ResourceOwnerReleased || outerEnv.calls != 2 || innerEnv.calls != 2 {
		t.Fatal("retry replayed or lost a cleanup role")
	}
}

func TestStableBuilderCleanupPanicIsUncertainAndNotReplayed(t *testing.T) {
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	builder := NewStableResourceSetBuilder()
	base, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(base); err != nil {
		t.Fatal(err)
	}
	env := &cleanupCustodyEnvironmentV1{panicNext: true}
	incoming := cleanupCustodyTokenV1(t, spec, env)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("cleanup panic missing")
			}
		}()
		_ = builder.Add(incoming)
	}()
	if err = builder.AbandonCheckedV1(); !errors.Is(err, ErrStableResourceOperationBusy) {
		t.Fatalf("uncertain Abandon: %v", err)
	}
	if env.calls != 1 || builder.pendingCleanup == nil || incoming.CleanupCompleteV1() {
		t.Fatal("uncertain owner was dropped or replayed")
	}
}

func TestStableSetOperationAllowsReentrantStateButRefusesReleaseBeforeEffects(t *testing.T) {
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	var set *StableResourceSet
	calls := 0
	spec.SyncThrough = func(_file *os.File, _ DurableFrontier) error {
		calls++
		if set.Owner() != ResourceOwnerBuilder {
			t.Error("wrong operation owner")
		}
		if err := set.Release(); err == nil {
			t.Error("release entered during admitted operation")
		}
		if set.Owner() == ResourceOwnerReleased {
			t.Error("release changed ownership")
		}
		return nil
	}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err = builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err = set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || token.CleanupCompleteV1() {
		t.Fatal("operation prematurely disposed backing")
	}
	if err = set.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableCheckedTokenReentrantReleaseIsBusyAndRetryConsumesOnce(t *testing.T) {
	env := &cleanupCustodyEnvironmentV1{ready: true}
	token := cleanupCustodyTokenV1(t, operationLifetimeSpec(operationLifetimeFile(t)), env)
	env.callback = func() {
		if err := token.Release(); !errors.Is(err, ErrStableResourceOperationBusy) {
			t.Errorf("running checked Release: %v", err)
		}
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if !token.CleanupCompleteV1() || env.calls != 1 {
		t.Fatal("running callback replayed")
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
}

// Both child effects complete, but their outcomes still belong to the parent.
func TestStableConcatCleanupPreservesCompleteChildErrorWithoutReplay(t *testing.T) {
	leftEnv := &cleanupCustodyEnvironmentV1{ready: true, completeError: errCleanupCustodyFixtureV1}
	rightEnv := &cleanupCustodyEnvironmentV1{ready: true}
	leftSpec := operationLifetimeSpec(operationLifetimeFile(t))
	leftSpec.ResourceID = "concat-left"
	leftSpec.Generation = 1
	rightSpec := operationLifetimeSpec(operationLifetimeFile(t))
	rightSpec.ResourceID = "concat-right"
	rightSpec.Generation = 2
	left := cleanupCustodyTokenV1(t, leftSpec, leftEnv)
	right := cleanupCustodyTokenV1(t, rightSpec, rightEnv)
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(left); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(right); err != nil {
		t.Fatal(err)
	}
	builder.mu.Lock()
	err := builder.promoteEntriesToViewsLocked()
	if err != nil {
		builder.mu.Unlock()
		t.Fatal(err)
	}
	view := builder.kindViews.get(left.kind)
	donor := view.root
	if donor == nil || len(donor.entries) != 2 {
		builder.mu.Unlock()
		t.Fatal("two actual leaf entries required")
	}
	root := concatOwnedStableResourceEntryNodes(newStableResourceEntryLeaf(donor.entries[:1]), newStableResourceEntryLeaf(donor.entries[1:]))
	// Transfer the donor's entry ownership into the two immutable leaf chunks.
	donor.entries = nil
	donor.refs.Store(0)
	view.root = root
	builder.kindViews.set(left.kind, view)
	builder.mu.Unlock()
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err = set.Release(); !errors.Is(err, errCleanupCustodyFixtureV1) {
		t.Fatalf("completed left error lost: %v", err)
	}
	if !root.cleanupCompleteV1() || !left.CleanupCompleteV1() || !right.CleanupCompleteV1() || leftEnv.calls != 1 || rightEnv.calls != 1 {
		t.Fatal("child cleanup did not complete exactly once")
	}
	if err = set.Release(); err != nil {
		t.Fatal(err)
	}
	if set.Owner() != ResourceOwnerReleased || leftEnv.calls != 1 || rightEnv.calls != 1 {
		t.Fatal("completed child replayed on set retry")
	}
}
