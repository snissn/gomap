package rootpublication

import (
	"errors"
	"testing"
)

type ownedTerminalTestTransition struct {
	roles              []StableTerminalOwnedSet
	tokens             []*StableResourceToken
	commit             func() error
	complete           func() error
	commits, completes int
}

func (tr *ownedTerminalTestTransition) TerminalOwnedResources() ([]StableTerminalOwnedSet, []*StableResourceToken, error) {
	return tr.roles, tr.tokens, nil
}
func (tr *ownedTerminalTestTransition) CommitTerminalPublication() error {
	tr.commits++
	if tr.commit != nil {
		return tr.commit()
	}
	return nil
}
func (tr *ownedTerminalTestTransition) CompleteTerminalPublication() error {
	tr.completes++
	if tr.complete != nil {
		return tr.complete()
	}
	return nil
}

func TestStableTerminalWholeDBGroupRefusalBeforePublication(t *testing.T) {
	finiteMetadataTestPlatform(t)
	account := &testStableMetadataAccount{}
	prefix, prefixHolds := terminalTestOwnedSet(t, account, 1, ResourceOwnerCoordinator)
	oldSlot, oldHolds := terminalTestOwnedSet(t, account, 1, ResourceOwnerBuilder)
	c, a, f := terminalTestReportingCoordinator(t, prefix)
	refused := errors.New("later DB slot has foreign pin")
	consumer := &terminalTestConsumer{prepareErr: refused}
	tr := &ownedTerminalTestTransition{roles: []StableTerminalOwnedSet{{oldSlot, ResourceOwnerBuilder}}}
	refs := account.retained()
	err := c.ReportPublishResultWithTerminalTransition(a, PublishResult{Outcome: PublishSucceeded}, consumer, tr)
	if !errors.Is(err, refused) || tr.commits != 0 || tr.completes != 0 || f.consumes.Load() != 0 || prefixHolds[0].released || oldHolds[0].released || account.retained() != refs {
		t.Fatalf("late owner refusal changed publication/ownership: err=%v commits=%d consumes=%d refs=%d", err, tr.commits, f.consumes.Load(), account.retained())
	}
	if consumer.prepares != 1 || len(consumer.groups) != 0 {
		t.Fatal("group session did not prepare exactly once and end")
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	consumer.prepareErr = nil
	if err = handoff.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
	if err = oldSlot.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
	if account.retained() != 0 || f.consumes.Load() != 0 {
		t.Fatal("recovery consumed an unpublished prefix or leaked actual owners")
	}
}

func TestStableTerminalCommittedPartialDBCleanupKeepsOwnerAndCOWOnce(t *testing.T) {
	finiteMetadataTestPlatform(t)
	account := &testStableMetadataAccount{}
	prefix, prefixHolds := terminalTestOwnedSet(t, account, 1, ResourceOwnerCoordinator)
	previous, previousHolds := terminalTestOwnedSet(t, account, 1, ResourceOwnerBuilder)
	c, a, f := terminalTestReportingCoordinator(t, prefix)
	failed := errors.New("DB namespace persistence")
	consumer := &terminalTestConsumer{}
	tr := &ownedTerminalTestTransition{roles: []StableTerminalOwnedSet{{previous, ResourceOwnerBuilder}}}
	tr.commit = func() error {
		if consumer.prepares != 1 || len(consumer.groups) != 2 || f.consumes.Load() != 0 {
			t.Fatal("publication was not dominated by complete group")
		}
		_ = c.Stats()
		return nil
	}
	consumer.release = func() {
		_ = c.Stats()
		if prefixHolds[0].released {
			consumer.fail = failed
		}
	}
	err := c.ReportPublishResultWithTerminalTransition(a, PublishResult{Outcome: PublishSucceeded}, consumer, tr)
	if !errors.Is(err, failed) || tr.commits != 1 || tr.completes != 0 || f.consumes.Load() != 1 || !prefixHolds[0].released || previousHolds[0].released || previous.Owner() != ResourceOwnerBuilder || account.retained() == 0 {
		t.Fatalf("committed cleanup lost real unfinished owner: err=%v consume=%d", err, f.consumes.Load())
	}
	if err = c.ReportPublishResultWithTerminalTransition(a, PublishResult{Outcome: PublishSucceeded}, consumer, tr); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatal("replayed consumed publication", err)
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if handoff.DurableRootLen() != 0 {
		t.Fatal("consumed COW re-entered recovery")
	}
	consumer.fail = nil
	consumer.release = nil
	if err = handoff.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
	if err = previous.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
	if account.retained() != 0 || f.consumes.Load() != 1 || !previousHolds[0].released {
		t.Fatal("cleanup-only recovery changed COW count or leaked owner")
	}
}

func TestStableTerminalAliasRoleRejectedBeforeCreditAndPublication(t *testing.T) {
	finiteMetadataTestPlatform(t)
	account := &testStableMetadataAccount{}
	set, holds := terminalTestOwnedSet(t, account, 1, ResourceOwnerCoordinator)
	c, a, f := terminalTestReportingCoordinator(t, set)
	before := account.bytes
	tr := &ownedTerminalTestTransition{roles: []StableTerminalOwnedSet{{set, ResourceOwnerCoordinator}}}
	consumer := &terminalTestConsumer{}
	if err := c.ReportPublishResultWithTerminalTransition(a, PublishResult{Outcome: PublishSucceeded}, consumer, tr); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal(err)
	}
	if account.bytes != before || consumer.prepares != 0 || f.consumes.Load() != 0 || tr.commits != 0 || holds[0].released {
		t.Fatal("borrowed role alias changed credit or authority")
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if err = handoff.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
}
