package rootpublication

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func operationLifetimeSpec(file *os.File) StableResourceSpec {
	return StableResourceSpec{Kind: ResourceIndex, LogicalLane: "index", ResourceID: "index", Generation: 1,
		DiagnosticPath: "index.db", File: file, Frontier: DurableFrontier{Bytes: 1},
		Digest: sha256.Sum256([]byte("operation-lifetime")), Reachability: ReachabilityIndexFile}
}
func operationLifetimeFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "index")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}
func operationLifetimeInitializePager(t *testing.T, p *pager.Pager) {
	t.Helper()
	// A real initialized file backs the nonzero frontier and surviving FD read.
	if err := p.GrowTo(1); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
}
func operationLifetimeWait(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("operation waited on its own callback gate")
	}
}

type operationLifetimeConsumer struct{ begins int }

func (c *operationLifetimeConsumer) BeginTerminalRelease() (bool, error) {
	c.begins++
	return true, nil
}
func (*operationLifetimeConsumer) PrepareTerminalRelease([]StableSegmentTerminalGroup) error {
	return nil
}
func (*operationLifetimeConsumer) ValidateSegmentRetention(StableSegmentRetention) error { return nil }
func (*operationLifetimeConsumer) ReleaseSegmentRetention(StableSegmentRetention) error  { return nil }
func (*operationLifetimeConsumer) EndTerminalRelease(bool)                               {}

func TestTokenOperationReleaseWaitsForActualAdmittedAliases(t *testing.T) {
	entered, proceed := make(chan struct{}), make(chan struct{})
	var releases atomic.Int32
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	spec.SyncThrough = func(file *os.File, _ DurableFrontier) error {
		close(entered)
		<-proceed
		var b [1]byte
		_, err := file.ReadAt(b[:], 0)
		return err
	}
	spec.OnRelease = func() { releases.Add(1) }
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- token.SyncThrough() }()
	<-entered
	consumer := &operationLifetimeConsumer{}
	if err := token.ReleaseWithTerminal(consumer); !errors.Is(err, ErrStableResourceOperationBusy) || consumer.begins != 0 {
		t.Fatalf("busy terminal had effects: %v begins=%d", err, consumer.begins)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 0 || token.pinned == nil {
		t.Fatal("release consumed admitted backing")
	}
	if err := token.SyncThrough(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("new operation admitted: %v", err)
	}
	close(proceed)
	operationLifetimeWait(t, done)
	if releases.Load() != 1 || token.pinned != nil || !token.cleanupComplete {
		t.Fatal("last actual operation did not discharge cleanup")
	}
	if token.Kind() != ResourceIndex || token.ResourceID() != "index" {
		t.Fatal("ordinary immutable diagnostics disappeared")
	}
}

func TestTokenOperationReentrantReleaseAndCallbackPanicCustody(t *testing.T) {
	t.Run("reentrant", func(t *testing.T) {
		var token *StableResourceToken
		var calls int
		spec := operationLifetimeSpec(operationLifetimeFile(t))
		spec.OnRelease = func() { calls++ }
		spec.SyncThrough = func(file *os.File, _ DurableFrontier) error {
			if err := token.Release(); err != nil {
				return err
			}
			var b [1]byte
			_, err := file.ReadAt(b[:], 0)
			return err
		}
		var err error
		token, err = NewStableResourceToken(spec)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- token.SyncThrough() }()
		operationLifetimeWait(t, done)
		if calls != 1 || token.Release() != nil {
			t.Fatal("reentrant cleanup replayed")
		}
	})
	for _, phase := range []string{"operation", "release"} {
		t.Run(phase, func(t *testing.T) {
			owner, _ := residentcredit.NewOrdinary(128 << 20)
			scope, _ := owner.NewOrdinaryScope()
			var token *StableResourceToken
			calls := 0
			spec := operationLifetimeSpec(operationLifetimeFile(t))
			spec.CallbackCreator = scope
			if phase == "release" {
				spec.OnRelease = func() { calls++; panic("release fault") }
			} else {
				spec.SyncThrough = func(*os.File, DurableFrontier) error { calls++; _ = token.Release(); panic("operation fault") }
			}
			var err error
			token, err = NewStableResourceToken(spec)
			if err != nil {
				t.Fatal(err)
			}
			owner.Close()
			scope.ReleaseStableMetadata()
			func() {
				defer func() {
					if recover() == nil {
						t.Error("panic swallowed")
					}
				}()
				if phase == "release" {
					_ = token.Release()
				} else {
					_ = token.SyncThrough()
				}
			}()
			if calls != 1 || !token.cleanupUncertain || token.callbackCreator == nil || owner.Stats().Live == 0 {
				t.Fatal("panic lost actual custody")
			}
			if !errors.Is(token.Release(), ErrStableResourceOperationBusy) || calls != 1 {
				t.Fatal("uncertain callback replayed")
			}
			// Explicit test-only debt resolution after checking the nonreplay contract.
			token.metadataMu.Lock()
			token.cleanupUncertain = false
			token.onRelease = nil
			token.sync = func(*os.File, DurableFrontier) error { return nil }
			token.metadataMu.Unlock()
			if err := token.finishRelease(nil); err != nil {
				t.Fatal(err)
			}
			if owner.Stats().Live != 0 {
				t.Fatal("resolved test debt retained creator")
			}
		})
	}
}

func TestTokenConcreteProviderBothClonePathsAfterOriginalOwnerClose(t *testing.T) {
	for _, physical := range []bool{false, true} {
		t.Run(fmt.Sprint(physical), func(t *testing.T) {
			owner, _ := residentcredit.NewOrdinary(128 << 20)
			scope, _ := owner.NewOrdinaryScope()
			path := filepath.Join(t.TempDir(), "index.db")
			p, err := pager.OpenWithOptions(path, page.PageSize*64, pager.OpenOptions{ResidentOwner: owner})
			if err != nil {
				t.Fatal(err)
			}
			operationLifetimeInitializePager(t, p)
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			before := scope.Stats().Bytes
			provider, err := NewStableIndexOperationProvider(p, scope)
			if err != nil {
				t.Fatal(err)
			}
			class, err := StableBackingClassBytes(uint64(unsafe.Sizeof(StableIndexOperationProvider{})), true)
			if err != nil {
				t.Fatal(err)
			}
			if scope.Stats().Bytes-before != class {
				t.Fatal("provider class birth not prepaid")
			}
			spec := operationLifetimeSpec(file)
			spec.CallbackCreator = scope
			spec.CallbackProvider = provider
			registry := NewIdentityPinRegistry()
			spec.PinRegistry = registry
			identity, err := StableIdentityFromFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.Observe(identity); err != nil {
				t.Fatal(err)
			}
			defer registry.Unobserve(identity)
			calls := 0
			spec.OnRelease = func() { calls++ }
			original, err := NewStableResourceToken(spec)
			if err != nil {
				t.Fatal(err)
			}
			provider.ReleaseStableResourceProvider()
			owner.Close()
			clone, err := cloneStableEntryToken(original, "clone", "clone", "clone.db", original.frontier, original.reachability, nil, nil, physical)
			if err != nil {
				t.Fatal(err)
			}
			scope.ReleaseStableMetadata()
			if err := original.Release(); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || provider.refs != 1 || provider.pager == nil || clone.releaseEnvironment == original.releaseEnvironment && clone.releaseEnvironment != nil {
				t.Fatal("original cleanup scrubbed cloned provider or copied callback")
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if err := clone.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			if err := clone.Release(); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || provider.refs != 0 || provider.pager != nil || provider.creator != nil || registry.Stats().ActivePins != 0 || owner.Stats().Live != 0 {
				t.Fatalf("last clone lost or leaked actual closure: %+v", owner.Stats())
			}
		})
	}
}

func TestTokenCallbackOriginNeverBecomesOwnedAfterScrub(t *testing.T) {
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	spec.OnRelease = func() {}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	destination := &testStableMetadataAccount{}
	for _, released := range []bool{false, true} {
		if released {
			if err := token.Release(); err != nil {
				t.Fatal(err)
			}
		}
		before := destination.bytes
		if _, err := prepareStableMetadataTransfer(token, destination); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
			t.Fatalf("callback transfer accepted: %v", err)
		}
		if _, err := NewStableSegmentOwner(token); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
			t.Fatalf("callback owner accepted: %v", err)
		}
		if destination.bytes != before || destination.retained() != 0 {
			t.Fatal("refusal debited destination")
		}
	}
}

func TestBuilderDisposalCallbacksOutsideLinearIndexedAndViewGates(t *testing.T) {
	for _, shape := range []string{"linear", "indexed", "view"} {
		for _, action := range []string{"state", "add", "abandon"} {
			for _, conflict := range []bool{false, true} {
				t.Run(fmt.Sprint(shape, "/", action, "/", conflict), func(t *testing.T) {
					file := operationLifetimeFile(t)
					extraFile := operationLifetimeFile(t)
					builder := NewStableResourceSetBuilder()
					defer builder.Abandon()
					if shape == "indexed" {
						for i := uint64(1); i <= 64; i++ {
							if err := builder.Add(distinctPhysicalTokenFixture(t, file, i)); err != nil {
								t.Fatal(err)
							}
						}
					}
					calls := 0
					callback := func() {
						calls++
						_ = builder.State()
						switch action {
						case "add":
							extra := operationLifetimeSpec(extraFile)
							extra.ResourceID = "extra"
							extra.LogicalLane = "extra"
							token, err := NewStableResourceToken(extra)
							if err != nil {
								panic(err)
							}
							if err = builder.Add(token); err != nil {
								_ = token.Release()
								panic(err)
							}
						case "abandon":
							builder.Abandon()
						}
					}
					firstSpec := operationLifetimeSpec(file)
					if shape == "view" && !conflict {
						firstSpec.OnRelease = callback
					}
					first, err := NewStableResourceToken(firstSpec)
					if err != nil {
						t.Fatal(err)
					}
					if shape == "view" {
						source := freezeAppendMutationResources(t, first)
						if err = builder.Merge(source); err != nil {
							t.Fatal(err)
						}
					} else {
						if err = builder.Add(first); err != nil {
							t.Fatal(err)
						}
					}
					incomingSpec := operationLifetimeSpec(file)
					if shape != "view" || conflict {
						incomingSpec.OnRelease = callback
					}
					if conflict {
						incomingSpec.Digest[0] ^= 1
					}
					incoming, err := NewStableResourceToken(incomingSpec)
					if err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() {
						err := builder.Add(incoming)
						if conflict && errors.Is(err, ErrResourceConflict) {
							err = nil
						}
						done <- err
					}()
					operationLifetimeWait(t, done)
					if calls != 1 {
						t.Fatalf("disposal callback calls=%d", calls)
					}
				})
			}
		}
	}
}

func TestTokenCloneRefusalBalancesOriginalCreatorAndObservation(t *testing.T) {
	owner, _ := residentcredit.NewOrdinary(128 << 20)
	scope, _ := owner.NewOrdinaryScope()
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	spec.CallbackCreator = scope
	spec.PinRegistry = NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(spec.File)
	if err != nil {
		t.Fatal(err)
	}
	if err = spec.PinRegistry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer spec.PinRegistry.Unobserve(identity)
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	beforeScope, beforeRegistry := scope.Stats(), spec.PinRegistry.Stats()
	if _, err := cloneStableEntryToken(token, "clone", "clone", "../bad", token.frontier, token.reachability, nil, nil, true); err == nil {
		t.Fatal("invalid physical clone accepted")
	}
	if scope.Stats().Retained != beforeScope.Retained || spec.PinRegistry.Stats() != beforeRegistry || token.pinnedRefs.Load() != 1 {
		t.Fatal("failed clone lost creator or registry custody")
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	scope.ReleaseStableMetadata()
	owner.Close()
}

func TestBuilderNestedViewMergeDisposalOutsideParentAndChildGates(t *testing.T) {
	file := operationLifetimeFile(t)
	parent := NewStableResourceSetBuilder()
	defer parent.Abandon()
	var child *StableResourceSet
	calls := 0
	callback := func() {
		calls++
		_ = parent.State()
		if child != nil {
			child.mu.Lock()
			child.mu.Unlock()
		}
	}
	leftSpec := operationLifetimeSpec(file)
	leftSpec.OnRelease = callback
	left, err := NewStableResourceToken(leftSpec)
	if err != nil {
		t.Fatal(err)
	}
	original := freezeAppendMutationResources(t, left)
	if err = parent.Merge(original); err != nil {
		t.Fatal(err)
	}
	rightSpec := operationLifetimeSpec(file)
	rightSpec.OnRelease = callback
	right, err := NewStableResourceToken(rightSpec)
	if err != nil {
		t.Fatal(err)
	}
	child = freezeAppendMutationResources(t, right)
	done := make(chan error, 1)
	go func() { done <- parent.Merge(child) }()
	operationLifetimeWait(t, done)
	if calls != 2 {
		t.Fatalf("original view disposal calls=%d", calls)
	}
}

func TestTokenOrdinaryMetadataAdmissionConcurrentRelease(t *testing.T) {
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	spec.OnRelease = func() {}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for j := 0; j < 200; j++ {
				if token.Kind() != ResourceIndex || token.LogicalLane() != "index" || token.ResourceID() != "index" || token.DiagnosticPath() != "index.db" || token.Reachability() != ReachabilityIndexFile {
					t.Error("ordinary immutable metadata changed")
				}
				_, _ = token.MetadataBacking()
				_ = token.Frontier()
				_ = token.LogicalObligations()
				_ = token.Namespace()
				_ = token.RequireMetadataExport()
			}
		}()
	}
	close(start)
	if err = token.Release(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	if !token.callbackBacked || !token.cleanupComplete {
		t.Fatal("terminal scrub lost permanent callback origin")
	}
	if token.Namespace() != nil {
		t.Fatal("released namespace authority escaped")
	}
}

func TestTokenPhaseOneKnownControlClasses(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("pinned 64-bit class inventory")
	}
	for _, entry := range []struct {
		name          string
		raw, expected uint64
	}{
		{"token", uint64(unsafe.Sizeof(StableResourceToken{})), 704},
		{"concrete-provider", uint64(unsafe.Sizeof(StableIndexOperationProvider{})), 32},
		{"clone-observation", uint64(unsafe.Sizeof(stableCloneObservation{})), 64},
	} {
		actual, err := StableBackingClassBytes(entry.raw, true)
		if err != nil || actual != entry.expected {
			t.Fatalf("%s actual class=%d raw=%d expected=%d err=%v", entry.name, actual, entry.raw, entry.expected, err)
		}
	}
}

func TestTokenSharedCloneWithoutRegistryRetainsNilReleaseEnvironment(t *testing.T) {
	tokenCloneWithoutRegistryLifetime(t, false)
}
func TestTokenPhysicalCloneWithoutRegistryRetainsNilReleaseEnvironment(t *testing.T) {
	tokenCloneWithoutRegistryLifetime(t, true)
}
func tokenCloneWithoutRegistryLifetime(t *testing.T, physical bool) {
	t.Helper()
	for _, mode := range []string{"default", "provider", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			withProvider, withLegacy := mode == "provider", mode == "legacy"
			file := operationLifetimeFile(t)
			spec := operationLifetimeSpec(file)
			var p *pager.Pager
			var provider *StableIndexOperationProvider
			var owner *residentcredit.Owner
			var scope *residentcredit.Scope
			calls, flushes, syncs := 0, 0, 0
			if withProvider {
				var err error
				owner, err = residentcredit.NewOrdinary(128 << 20)
				if err != nil {
					t.Fatal(err)
				}
				scope, err = owner.NewOrdinaryScope()
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "provider-index.db")
				p, err = pager.OpenWithOptions(path, page.PageSize*64, pager.OpenOptions{ResidentOwner: owner})
				if err != nil {
					t.Fatal(err)
				}
				operationLifetimeInitializePager(t, p)
				file, err = os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				spec = operationLifetimeSpec(file)
				provider, err = NewStableIndexOperationProvider(p, scope)
				if err != nil {
					t.Fatal(err)
				}
				spec.CallbackCreator = scope
				spec.CallbackProvider = provider
				spec.OnRelease = func() { calls++ }
			}
			if withLegacy {
				spec.OnRelease = func() { calls++ }
				spec.FlushThrough = func(*os.File, DurableFrontier) error { flushes++; return nil }
				spec.SyncThrough = func(file *os.File, _ DurableFrontier) error {
					syncs++
					var b [1]byte
					_, err := file.ReadAt(b[:], 0)
					return err
				}
			}
			var expected [1]byte
			if _, err := file.ReadAt(expected[:], 0); err != nil {
				t.Fatal(err)
			}
			original, err := NewStableResourceToken(spec)
			if err != nil {
				t.Fatal(err)
			}
			if original.identityPin != nil {
				t.Fatal("fixture accidentally has a registry")
			}
			if withProvider {
				provider.ReleaseStableResourceProvider()
				owner.Close()
			}
			clone, err := cloneStableEntryToken(original, "clone", "clone", "clone.db", original.frontier, original.reachability, nil, nil, physical)
			if err != nil {
				t.Fatal(err)
			}
			if clone.releaseEnvironment != nil {
				t.Fatal("nil registry became a nonnil release interface")
			}
			if clone.callbackBacked != (withProvider || withLegacy) {
				t.Fatal("clone fabricated or erased permanent callback origin")
			}
			if withProvider {
				scope.ReleaseStableMetadata()
			}
			refs := original.pinnedRefs
			if err = original.Release(); err != nil {
				t.Fatal(err)
			}
			if withProvider && (calls != 1 || provider.refs != 1 || provider.pager == nil) {
				t.Fatal("original responsibility or shared provider edge lost")
			}
			if physical && refs.Load() != 0 || !physical && refs.Load() != 1 {
				t.Fatal("incorrect source physical reference disposal")
			}
			if withProvider {
				if err = p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			var b [1]byte
			if _, err = clone.ReadAt(b[:], 0); err != nil || b != expected {
				t.Fatalf("surviving exact cloned FD unavailable: %q %v", b, err)
			}
			if err = clone.FlushThrough(); err != nil {
				t.Fatal(err)
			}
			if err = clone.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			if withLegacy && (flushes != 1 || syncs != 1 || calls != 1) {
				t.Fatal("explicit legacy operations changed or original callback copied")
			}
			if err = clone.Release(); err != nil {
				t.Fatal(err)
			}
			if clone.cleanupUncertain || !clone.cleanupComplete || clone.pinned != nil {
				t.Fatal("nil-registry cleanup panicked or retained a physical owner")
			}
			if !physical && refs.Load() != 0 {
				t.Fatal("last shared physical reference leaked")
			}
			if withLegacy && calls != 1 {
				t.Fatal("legacy original release callback replayed")
			}
			if withProvider && (calls != 1 || provider.refs != 0 || provider.pager != nil || provider.creator != nil || owner.Stats().Live != 0) {
				t.Fatal("nil-registry final clone lost original creator semantics")
			}
		})
	}
}
