package rootpublication

import (
	"context"
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This is an exact common Finish lifecycle discriminator with real duplicated
// file handles and registry pins. It deliberately starts at retained Finish;
// it does not qualify primary seal/storage admission or a public native route.
type ownedFinishScopePublisherV6 struct {
	Publisher
	active         bool
	enters, leaves int
}

func (p *ownedFinishScopePublisherV6) EnterOwnedFinishCallbackV6(w *iterator.OrdinalScanWork) (bool, error) {
	if !w.Reserve(2, 128) {
		return false, nil
	}
	p.active = true
	p.enters++
	return true, nil
}
func (p *ownedFinishScopePublisherV6) LeaveOwnedFinishCallbackV6() { p.active = false; p.leaves++ }

func TestOwnedPublicationCallbackScopeDispatchV6(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 4)
}

// Real immutable arena constructor and receipt view use the existing typed
// last-handle cursor. This starts at Finish and qualifies no storage append.
func TestOwnedPublicationConstructedOwnerSameReceiptFinish(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 5)
}
func TestOwnedPublicationConstructedTerminalSameCursor(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 6)
}
func TestOwnedPublicationFinishLastPhysicalOwner(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 0)
}

func TestOwnedPublicationTerminalTakeoverPartialRoot(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 1)
}

func TestOwnedPublicationTerminalTakeoverJoinsActiveCallback(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 2)
}
func TestOwnedPublicationTerminalTakeoverFailureRetainsCursor(t *testing.T) {
	testOwnedPublicationFinishLastPhysicalOwner(t, 3)
}
func testOwnedPublicationFinishLastPhysicalOwner(t *testing.T, mode int) {
	terminal := mode != 0 && mode != 4 && mode != 5
	entered, resume := make(chan struct{}), make(chan struct{})
	injected := errors.New("injected owned Finish failure")
	registry := NewIdentityPinRegistry()
	builder := NewStableResourceSetBuilder(ReachabilityIndexFile)
	tokens := make([]*StableResourceToken, 0, 2)
	auxiliary := make([]*os.File, 0, 2)
	for i, name := range []string{"data", "primary"} {
		f, e := os.Create(filepath.Join(t.TempDir(), name))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.Write([]byte("retained")); e != nil {
			t.Fatal(e)
		}
		identity, e := StableIdentityFromFile(f)
		if e != nil {
			t.Fatal(e)
		}
		if e = registry.Observe(identity); e != nil {
			t.Fatal(e)
		}
		defer registry.Unobserve(identity)
		aux, e := os.Open(f.Name())
		if e != nil {
			t.Fatal(e)
		}
		auxiliary = append(auxiliary, aux)
		ownedReleased := false
		token, e := NewStableResourceToken(StableResourceSpec{Kind: ResourceIndex, ResourceID: name, Generation: uint64(i + 1), DiagnosticPath: name, File: f, Frontier: DurableFrontier{Bytes: 8}, Reachability: ReachabilityIndexFile, PinRegistry: registry, OnRelease: func() { _ = aux.Close() }, OwnedOnRelease: func(w *iterator.OrdinalScanWork) (bool, error) {
			if w == nil {
				return false, ErrResourceOwnership
			}
			if ownedReleased {
				return true, nil
			}
			if !w.Reserve(1, 256) {
				return false, nil
			}
			ownedReleased = true
			return true, aux.Close()
		}})
		_ = f.Close()
		if e != nil {
			t.Fatal(e)
		}
		if e = builder.Add(token); e != nil {
			t.Fatal(e)
		}
		tokens = append(tokens, token)
	}
	set, e := builder.Freeze()
	if e != nil {
		t.Fatal(e)
	}
	retirement, ready, e := PrepareStableResourceRetirementV5(set, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if e != nil || !ready {
		t.Fatalf("prepare %v %v", ready, e)
	}
	for i := 0; i < 32; i++ {
		ready, progress, e := retirement.PrepareStep(&iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20})
		if e != nil || (!ready && !progress) {
			t.Fatalf("prepare step %v %v %v", ready, progress, e)
		}
		if ready {
			break
		}
		if i == 31 {
			t.Fatal("preparation did not progress")
		}
	}
	if e = set.transfer(ResourceOwnerBuilder, ResourceOwnerFinish); e != nil {
		t.Fatal(e)
	}
	calls := 0
	var c *Coordinator
	var scope *ownedFinishScopePublisherV6
	defer func() {
		if mode == 4 && (scope == nil || scope.active || scope.enters == 0 || scope.enters != scope.leaves) {
			t.Error("callback scope cleanup")
		}
	}()
	var transaction *DurableRootTransaction
	transaction = &DurableRootTransaction{owner: ResourceOwnerFinish, phase: durableRootFinishing, owned: &OwnedDurableRootCallbacks{Finish: func(_ DurableRootCallbackInput, actual *StableResourceSet, w *iterator.OrdinalScanWork) (bool, bool, error) {
		if actual != set || w == nil {
			t.Fatal("lost exact charged owner")
		}
		calls++
		if mode == 4 && (scope == nil || !scope.active) {
			t.Fatal("actual callback outside publisher scope")
		}
		if mode == 2 && calls == 2 {
			close(entered)
			<-resume
		}
		if mode == 3 && calls == 2 {
			return false, false, injected
		}
		// Callback must be able to re-enter both owner domains without deadlock.
		if !c.HasOwnedFinishV5() || transaction.Owner() != ResourceOwnerFinish {
			t.Fatal("owner not retained")
		}
		c.mu.Lock()
		c.captureRecoveryResourcesLocked()
		c.mu.Unlock()
		if actual.Owner() != ResourceOwnerFinish || len(c.recoverySets) != 0 || len(c.recoveryDurableRoots) != 0 {
			t.Fatal("recovery stole busy retirement")
		}
		done, progress, e := retirement.Step(w)
		return done, progress, e
	}}}

	var constructedArena *primaryarena.Arena
	var constructedRoot primaryarena.Ref
	if mode == 5 || mode == 6 {
		constructedArena, e = primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "index.db.primary"))
		if e != nil {
			t.Fatal(e)
		}
		defer constructedArena.Close()
		owner, ready, err := NewPrimaryConstructionTransactionV6(constructedArena, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if !ready || err != nil {
			t.Fatal(err)
		}
		actual := owner.(*DurableRootTransaction)
		constructedRoot = actual.PrimaryConstructionReferenceV6()
		image := make([]byte, page.PageSize)
		base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("independent DATA owner"))}
		if err = node.EncodePrimaryDirectory(image, constructedRoot.PageID, 1, base, nil); err != nil {
			t.Fatal(err)
		}
		if ready, err = actual.SealPrimaryConstructionV6(image, [1]*primaryarena.Group{}, &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}); !ready || err != nil {
			t.Fatal(err)
		}
		// Full construction here is ordinary synchronous setup (the seal visits\n\t\t// all49 class slots). This fixture begins at real retained Finish. The same callback closes
		// actual handles; no constructor, state or receipt edge is released in setup.
		actual.owned = transaction.owned
		actual.constructionV6.resources = set
		actual.phase = durableRootFinishing
		actual.constructionV6.candidate.extensions.resourceSet = set
		actual.constructionV6.candidateBound = true
		transaction = actual
	}
	c, e = New(Options{InitialDurableFrontier: NewFrontier(1, 0, 0, 0, 0), Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		t.Error("Finish-only fixture unexpectedly published")
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if e != nil {
		t.Fatal(e)
	}
	defer stopClean(t, c)
	c.mu.Lock()
	c.resourceActivePins[ResourceIndex] = 2
	if mode == 4 {
		scope = &ownedFinishScopePublisherV6{Publisher: c.publisher}
		c.publisher = scope
	}
	receipt := &OwnedPublicationReceiptV5{coordinator: c, transaction: transaction, legacyCandidate: &PreparedRootCandidate{extensions: extensionSlots{resourceSet: set}}, pinCounts: map[ResourceKind]uint64{ResourceIndex: 2}, durable: true}
	if mode == 5 || mode == 6 {
		receipt = &transaction.constructionV6.receipt
		receipt.coordinator, receipt.transaction, receipt.pinCounts, receipt.durable = c, transaction, map[ResourceKind]uint64{ResourceIndex: 2}, true
		if receipt.resourceSetV6() != set || receipt.candidateV6() != transaction.PublicationCandidateV6() {
			t.Fatal("receipt mirrors constructor")
		}
	}
	c.ownedFinish = []*OwnedPublicationReceiptV5{receipt}
	c.mu.Unlock()
	if e = c.WaitThrough(context.Background(), 1); e != nil {
		t.Fatal(e)
	}
	if calls != 0 || set.Owner() != ResourceOwnerFinish {
		t.Fatal("waiter ran Finish")
	}
	if ready, e = c.FinishOwnedPublicationV5(receipt, nil); ready || !errors.Is(e, ErrDurableRootOwnership) {
		t.Fatalf("nil work %v %v", ready, e)
	}
	var takeover *TerminalOwnedFinishV5
	recordLimit := uint64(8)
	if mode == 4 {
		recordLimit = 10
	} // original 8R retirement cut plus two actual scope operations
	for turn := 0; turn < 32; turn++ {
		if mode == 5 || mode == 6 {
			if turn == 0 {
				recordLimit = 9
			} else {
				recordLimit = 32
			}
		}
		work := &iterator.OrdinalScanWork{RecordLimit: recordLimit, ByteLimit: 1 << 20}
		if takeover != nil {
			ready, _, e = takeover.Step(work)
		} else {
			ready, e = c.FinishOwnedPublicationV5(receipt, work)
		}
		if e != nil {
			if mode == 3 && errors.Is(e, injected) {
				if set.Owner() != ResourceOwnerFinish || retirement.token == nil || !c.HasOwnedFinishV5() {
					t.Fatal("failure lost exact cursor")
				}
				continue
			}
			t.Fatal(e)
		}
		if work.Records > recordLimit || work.Bytes > 1<<20 {
			t.Fatal("budget exceeded")
		}
		if ready {
			break
		}
		// Terminal handoff between charged calls must preserve the cursor after
		// its actual rope edge reached zero, before handles/callbacks finish.
		c.mu.Lock()
		c.captureRecoveryResourcesLocked()
		c.mu.Unlock()
		if (set.Owner() != ResourceOwnerFinish && !((mode == 5 || mode == 6) && transaction.phase == durableRootRetiringConstructorV6 && set.Owner() == ResourceOwnerReleased)) || len(c.ownedFinish) != 1 || len(c.recoverySets) != 0 || len(c.recoveryDurableRoots) != 0 {
			t.Fatal("recovery lost partial rope/token/callback debt")
		}
		if turn == 0 && (retirement.current == nil || retirement.current.refs.Load() != 0 || retirement.token == nil) {
			t.Fatal("missing decisive partial-root cut")
		}
		if c.resourceActivePins[ResourceIndex] != 2 {
			t.Fatal("pins removed before real retirement")
		}
		if terminal && turn == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := c.Stop(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if handoff, err := c.TakeRecoveryHandoff(); handoff != nil || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("partial cursor generic handoff %v %v", handoff, err)
			}
			var admitted bool
			if mode == 2 {
				finished := make(chan error, 1)
				go func() {
					_, err := c.FinishOwnedPublicationV5(receipt, &iterator.OrdinalScanWork{RecordLimit: 8, ByteLimit: 1 << 20})
					finished <- err
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("active callback did not enter")
				}
				type result struct {
					owner *TerminalOwnedFinishV5
					ready bool
					err   error
				}
				taken := make(chan result, 1)
				go func() {
					owner, ready, err := c.TakeTerminalOwnedFinishV5(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
					taken <- result{owner, ready, err}
				}()
				select {
				case <-taken:
					close(resume)
					t.Fatal("takeover crossed active callback")
				case <-time.After(50 * time.Millisecond):
				}
				close(resume)
				select {
				case err := <-finished:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("callback reentry deadlocked")
				}
				select {
				case result := <-taken:
					takeover, admitted, e = result.owner, result.ready, result.err
				case <-time.After(5 * time.Second):
					t.Fatal("terminal owner waited for former caller resume")
				}
			} else {
				takeover, admitted, e = c.TakeTerminalOwnedFinishV5(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			}
			if e != nil || !admitted || takeover == nil {
				t.Fatalf("takeover %v %v", admitted, e)
			}
			if done, err := c.FinishOwnedPublicationV5(receipt, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); done || !errors.Is(err, ErrDurableRootOwnership) {
				t.Fatalf("former owner retained authority: %v %v", done, err)
			}
			if done, _, err := takeover.Step(nil); done || !errors.Is(err, ErrDurableRootOwnership) {
				t.Fatalf("nil terminal help: %v %v", done, err)
			}
		}
		if turn == 31 {
			t.Fatal("Finish did not progress")
		}
	}
	if !ready || set.Owner() != ResourceOwnerReleased || transaction.Owner() != ResourceOwnerReleased || registry.Stats().ActivePins != 0 || c.resourceActivePins[ResourceIndex] != 0 || c.HasOwnedFinishV5() {
		t.Fatal("last-owner custody leaked")
	}
	if mode == 5 || mode == 6 {
		if _, err := constructedArena.Get(constructedRoot.PageID); err == nil {
			t.Fatal("constructor root retained after charged Finish")
		}
		if transaction.constructionV6.root != (primaryarena.Ref{}) || !transaction.constructionV6.finishDetached {
			t.Fatal("constructor debt not completed")
		}
	}
	for _, token := range tokens {
		if _, e = token.pinned.Stat(); !errors.Is(e, os.ErrClosed) {
			t.Fatalf("real handle retained: %v", e)
		}
	}
	for _, f := range auxiliary {
		if _, e = f.Stat(); !errors.Is(e, os.ErrClosed) {
			t.Fatalf("typed callback left real handle: %v", e)
		}
	}
	if terminal {
		handoff, err := c.TakeRecoveryHandoff()
		if err != nil || handoff == nil {
			t.Fatalf("completed handoff %v", err)
		}
		handoff.Release()
	}
	oldCalls := calls
	if takeover != nil {
		ready, _, e = takeover.Step(&iterator.OrdinalScanWork{})
	} else {
		ready, e = c.FinishOwnedPublicationV5(receipt, &iterator.OrdinalScanWork{})
	}
	if !ready || e != nil || calls != oldCalls {
		t.Fatal("Finish repeated")
	}
}
