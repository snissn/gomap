package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"unsafe"
)

type testStableMetadataAccount struct {
	mu          sync.Mutex
	bytes, refs uint64
	deny        bool
}

var errDeniedStableMetadata = errors.New("denied stable metadata")

func (a *testStableMetadataAccount) ReserveStableMetadata(n uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.deny {
		return errDeniedStableMetadata
	}
	a.bytes += n
	return nil
}
func (a *testStableMetadataAccount) RetainStableMetadata() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refs++
	return nil
}
func (a *testStableMetadataAccount) ReleaseStableMetadata() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.refs == 0 {
		panic("unbalanced test metadata")
	}
	a.refs--
}
func (a *testStableMetadataAccount) retained() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.refs
}
func finiteMetadataTestPlatform(t *testing.T) {
	t.Helper()
	if err := finiteStablePlatform(); err != nil {
		t.Skip(err)
	}
}
func finiteMetadataTestSpec(file *os.File) StableResourceSpec {
	return StableResourceSpec{
		Kind: ResourceOuterLeafLog, LogicalLane: "leaf", ResourceID: "1", Generation: 1,
		DiagnosticPath: "leaf/1", File: file, Reachability: ReachabilityOuterLeafRawPointer,
	}
}

func TestStableMetadataDeniedBeforeHandleOrNamespaceWork(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	a := &testStableMetadataAccount{deny: true}
	if _, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), a); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("resource touched closed handle before debit: %v", err)
	}
	if _, err := NewStableNamespaceCreationProofWithMetadataAccount(file, file, "closed", a); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("proof inspected closed handles before debit: %v", err)
	}
	if _, err := NewStableNamespaceTokenWithMetadataAccount(StableNamespaceSpec{Parent: file, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "closed", DiagnosticPath: "leaf"}, a); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("namespace inspected closed handle before debit: %v", err)
	}
	if a.retained() != 0 || a.bytes != 0 {
		t.Fatal("denied constructor retained accounting")
	}
	// The ordinary constructor still reaches its original closed-handle error.
	if _, err := NewStableResourceToken(finiteMetadataTestSpec(file)); err == nil || errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("ordinary constructor changed: %v", err)
	}
}

func TestStableMetadataTokenCloneAndExactStringOwnership(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	a := &testStableMetadataAccount{}
	backing := make([]byte, 1<<16)
	copy(backing, "leaf/1")
	borrowed := unsafe.String(unsafe.SliceData(backing), len("leaf/1"))
	spec := finiteMetadataTestSpec(file)
	spec.DiagnosticPath = borrowed
	token, err := NewStableResourceTokenWithMetadataAccount(spec, a)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := token.MetadataBacking(); !ok || n == 0 {
		t.Fatal("constructor did not stamp metadata provenance")
	}
	if unsafe.StringData(token.diagnosticPath) == unsafe.StringData(borrowed) {
		t.Fatal("retained borrowed substring backing")
	}
	clone, err := token.cloneSharedPinned("leaf-copy", "2", "leaf/2", token.frontier, token.reachability, nil, nil)
	if err != nil {
		token.Release()
		t.Fatal(err)
	}
	if a.retained() != 2 {
		t.Fatalf("actual token/clone owners: %d", a.retained())
	}
	token.Release()
	if a.retained() != 1 {
		t.Fatal("source release dropped clone accounting")
	}
	if _, err := clone.pinned.Stat(); err != nil {
		t.Fatalf("source release closed clone descriptor: %v", err)
	}
	clone.Release()
	clone.Release()
	if a.retained() != 0 {
		t.Fatal("clone release unbalanced owner")
	}
}

func TestStableMetadataProofBindDenialAndRetainedLifetime(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "leaf"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	a := &testStableMetadataAccount{}
	proof, err := NewStableNamespaceCreationProofWithMetadataAccount(parent, child, "leaf", a)
	if err != nil {
		t.Fatal(err)
	}
	if a.retained() != 1 {
		t.Fatal("proof has no retained debit")
	}
	a.deny = true
	if _, err := proof.Bind(parent, 1, "leaf", "leaf-dir"); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("Bind denial: %v", err)
	}
	if proof.released.Load() || a.retained() != 1 {
		t.Fatal("denied Bind altered retained proof")
	}
	a.deny = false
	namespace, err := proof.Bind(parent, 1, "leaf", "leaf-dir")
	if err != nil {
		proof.Release()
		t.Fatal(err)
	}
	if a.retained() != 2 {
		t.Fatal("derived namespace missing separate owner")
	}
	proof.Release()
	proof.Release()
	if a.retained() != 1 {
		t.Fatal("proof Release dropped namespace owner")
	}
	if _, err := namespace.parent.Stat(); err != nil {
		t.Fatal("proof release closed derived namespace parent")
	}
	namespace.Release()
	namespace.Release()
	if a.retained() != 0 {
		t.Fatal("namespace Release leaked owner")
	}
}

func TestStableMetadataNilCloneRejectsWithoutAccountRead(t *testing.T) {
	var token *StableResourceToken
	if _, err := token.cloneSharedPinned("a", "b", "c", DurableFrontier{}, ReachabilityOuterLeafRawPointer, nil, nil); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nil clone: %v", err)
	}
	if _, err := token.cloneSharedPinnedDirectory("a", "b", "c", DurableFrontier{}, ReachabilityOuterLeafRawPointer, nil, nil, nil); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nil directory clone: %v", err)
	}
}
func TestStableMetadataProofBindCopiesCallerName(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "leaf"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	a := &testStableMetadataAccount{}
	proof, err := NewStableNamespaceCreationProofWithMetadataAccount(parent, child, "leaf", a)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	large := make([]byte, 1<<16)
	copy(large, "leaf")
	borrowed := unsafe.String(unsafe.SliceData(large), 4)
	ns, err := proof.Bind(parent, 1, borrowed, "leaf-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Release()
	if unsafe.StringData(ns.newName) == unsafe.StringData(borrowed) {
		t.Fatal("Bind retained borrowed caller-name backing")
	}
	if ns.newName != "leaf" {
		t.Fatal("Bind changed exact name")
	}
}

func TestStableMetadataProofMismatchDoesNotConsumeCredit(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "leaf"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	a := &testStableMetadataAccount{}
	proof, err := NewStableNamespaceCreationProofWithMetadataAccount(parent, child, "leaf", a)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	before := a.bytes
	if _, err := proof.Bind(parent, 1, "different", "leaf-dir"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("mismatch: %v", err)
	}
	if a.bytes != before || a.retained() != 1 {
		t.Fatal("invalid Bind consumed constructor credit")
	}
}

func TestStableMetadataGenericSetClonesRefuseBeforeObserveOrClone(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := registry.Unobserve(identity); err != nil {
			t.Error("original observation cleanup", err)
		}
	}()
	account := &testStableMetadataAccount{}
	obligation := StableLogicalObligation{Class: "column", Kind: string(ResourceOuterLeafLog), Namespace: "docs", Generation: 1, FileID: 1, Length: 1, Reachability: ReachabilityOuterLeafRawPointer, Digest: [32]byte{1}}
	spec := finiteMetadataTestSpec(file)
	spec.PinRegistry = registry
	spec.LogicalObligations = []StableLogicalObligation{obligation}
	token, err := NewStableResourceTokenWithMetadataAccount(spec, account)
	if err != nil {
		t.Fatal(err)
	}
	source := malformedAccountedSetForTest(t, token)
	var entry stableResourceEntry
	var beforeFile *os.File
	var beforePin *IdentityPin
	// This set intentionally bypassed the admitted constructor. Preserve its
	// refusal contract, then discharge only the fixture's exact original owner.
	// Install cleanup before denial/subtests so Fatal also clears every alias.
	cleanup := func() {
		source.mu.Lock()
		entry = stableResourceEntry{}
		beforeFile, beforePin = nil, nil
		clear(source.entries[:cap(source.entries)])
		source.entries = nil
		source.mu.Unlock()
		if err := token.releaseFrom(ResourceOwnerBuilder); err != nil {
			t.Error("original Builder token cleanup", err)
			return
		}
		if err := source.Release(); err != nil {
			t.Error("empty fabricated set cleanup", err)
		}
	}
	defer cleanup()
	// Credit denial must not be consulted: the generic set route has no owner
	// contract at all. Refusal precedes source staging, Observe and FD cloning.
	account.deny = true
	beforeBytes, beforeRefs := account.bytes, account.retained()
	beforeObservers := registry.ObserverCount(identity)
	cases := []struct {
		name string
		run  func() (*StableResourceSet, error)
	}{
		{"unscoped", func() (*StableResourceSet, error) { return CloneStableResourceSetExcludingKinds(source) }},
		{"scoped", func() (*StableResourceSet, error) {
			return CloneStableResourceSetForLogicalObligations(source, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityOuterLeafRawPointer}, Obligations: []StableLogicalObligation{obligation}})
		}},
		{"mutation", func() (*StableResourceSet, error) {
			next, _, err := CloneStableResourceSetApplyingLogicalObligationMutation(source, StableLogicalObligationMutation{})
			return next, err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, err := tc.run()
			if next != nil {
				next.Release()
			}
			if next != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) {
				t.Fatalf("accounted set clone: next=%v err=%v", next, err)
			}
			if account.bytes != beforeBytes || account.retained() != beforeRefs || registry.ObserverCount(identity) != beforeObservers {
				t.Fatal("refusal mutated credit/retention/registry")
			}
			if _, err := token.ReadAt(nil, 0); err != nil {
				t.Fatalf("refusal damaged original FD: %v", err)
			}
		})
	}
	// Direct builder entry derivation must have the same pre-Observe boundary.
	source.mu.Lock()
	entry = source.entrySnapshotLocked()[0]
	source.mu.Unlock()
	destination := NewStableResourceSetBuilder()
	defer destination.Abandon()
	if err := cloneStableResourceEntryIntoBuilder(destination, &entry); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("entry clone refusal: %v", err)
	}
	if len(destination.entries) != 0 || account.retained() != beforeRefs || registry.ObserverCount(identity) != beforeObservers {
		t.Fatal("entry refusal staged or observed a clone")
	}
	beforePins := registry.PinCount(identity)
	token.metadataMu.Lock()
	beforeFile, beforePin = token.pinned, token.identityPin
	token.metadataMu.Unlock()
	if beforeFile == nil || beforePin == nil || beforePins != 1 || beforeRefs != 2 {
		t.Fatal("fixture does not hold exactly the original FD/pin/token and registry account edges")
	}
	if err := source.Release(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatal("malformed set release did not refuse before effects", err)
	}
	token.metadataMu.Lock()
	unchangedToken := token.pinned == beforeFile && token.identityPin == beforePin && !token.released.Load() && ResourceOwnerState(token.owner.Load()) == ResourceOwnerBuilder
	token.metadataMu.Unlock()
	if source.Owner() != ResourceOwnerBuilder || !unchangedToken || account.bytes != beforeBytes || account.retained() != beforeRefs || registry.ObserverCount(identity) != beforeObservers || registry.PinCount(identity) != beforePins {
		t.Fatal("malformed release changed original custody/credit/pin")
	}
	if _, err := token.ReadAt(nil, 0); err != nil {
		t.Fatal("malformed release damaged original FD", err)
	}
	if _, err := beforeFile.Stat(); err != nil {
		t.Fatal("malformed release closed original pinned handle", err)
	}
	cleanup()
	if account.retained() != 0 || account.bytes != beforeBytes || registry.PinCount(identity) != 0 || registry.ObserverCount(identity) != beforeObservers || source.Owner() != ResourceOwnerReleased || ResourceOwnerState(token.owner.Load()) != ResourceOwnerReleased {
		t.Fatal("fixture cleanup did not discharge exact original edges")
	}
	if err := source.Release(); err != nil {
		t.Fatal("empty fabricated set cleanup not idempotent", err)
	}
	if err := token.releaseFrom(ResourceOwnerBuilder); err != nil || account.retained() != 0 {
		t.Fatal("original token cleanup not idempotent", err)
	}
}

func TestStableMetadataNamespaceCloneRefusesBeforeDup(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file, err := os.Create(filepath.Join(dir, "leaf"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	a := &testStableMetadataAccount{}
	proof, err := NewStableNamespaceCreationProofWithMetadataAccount(parent, file, "leaf", a)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	namespace, err := proof.Bind(parent, 1, "leaf", "leaf-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	beforeBytes, beforeRefs := a.bytes, a.retained()
	a.deny = true
	clone, err := namespace.cloneStable()
	if clone != nil {
		clone.Release()
	}
	if clone != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("namespace clone refusal: clone=%v err=%v", clone, err)
	}
	if a.bytes != beforeBytes || a.retained() != beforeRefs {
		t.Fatal("namespace refusal changed retained credit")
	}
	if _, err := namespace.parent.Stat(); err != nil {
		t.Fatalf("refusal damaged exact original parent: %v", err)
	}
}

func TestStableMetadataEntryRefusalIncludesSecondaryAccount(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ordinary, err := NewStableResourceToken(finiteMetadataTestSpec(file))
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Release()
	account := &testStableMetadataAccount{}
	accounted, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), account)
	if err != nil {
		t.Fatal(err)
	}
	defer accounted.Release()
	// A pinned union can retain an accounted secondary even when its active
	// representative is ordinary. This boundary must inspect all retained pins.
	entry := stableResourceEntry{token: ordinary, pins: []*StableResourceToken{ordinary, accounted}}
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	before := account.retained()
	if err := cloneStableResourceEntryIntoBuilder(builder, &entry); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("secondary account: %v", err)
	}
	if len(builder.entries) != 0 || account.retained() != before {
		t.Fatal("secondary refusal changed destination/owner")
	}
}
