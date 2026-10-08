package rootpublication

import (
	"errors"
	"unsafe"
)

// PublisherTerminalReporter is an optional concrete synchronous scheduler
// bridge. It must report this exact attempt once. Any terminal consumer belongs
// to that call only. Arbitrary notification callbacks cannot run on this worker:
// Close/Stop would otherwise join the worker invoking the callback.
type PublisherTerminalReporter interface {
	ReportRootPublishTerminal(*Coordinator, PublishAttempt, PublishResult) error
}

func consumeDurableRootGroupUnlessFinished(group durableRootGroupExtension, finished bool) error {
	if finished {
		return nil
	}
	return consumeDurableRootGroup(group)
}

// Complete-set validation dominates every scratch birth and deletion gate.
// The selected finite flat closure has no shadow entries, secondary pins or
// generic borrowed views. Ordinary closures keep the existing engine semantics.
func inspectStableTerminalSet(set *StableResourceSet, consumer StableSegmentTerminalConsumer) (int, StableMetadataAccount, error) {
	if set == nil || set.Owner() == ResourceOwnerReleased {
		return 0, nil, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.terminalReleasing {
		return 0, nil, ErrResourceOwnership
	}
	if set.metadataAccount == nil {
		return 0, nil, requireOrdinaryStableResourceInputs(nil, nil, set)
	}
	if !set.finiteMetadata || set.kindViews != nil || set.extras != nil {
		return 0, nil, ErrStableMetadataShapeUnsupported
	}
	count := 0
	for i := range set.entries {
		e := &set.entries[i]
		if e.token == nil || len(e.pins) != 0 || e.pinIndex != nil {
			return 0, nil, ErrStableMetadataShapeUnsupported
		}
		for j := 0; j < i; j++ {
			if set.entries[j].token == e.token {
				return 0, nil, ErrResourceOwnership
			}
		}
		if ResourceOwnerState(e.token.owner.Load()) == ResourceOwnerReleased {
			continue
		}
		if ResourceOwnerState(e.token.owner.Load()) != set.Owner() {
			return 0, nil, ErrResourceOwnership
		}
		if err := validateStableSegmentTerminalRetention(e.token.segmentRetention, consumer); err != nil {
			return 0, nil, err
		}
		count++
	}
	return count, set.metadataAccount, nil
}

func prepareStableTerminalSetGroup(pending []pendingEntry, sets []*StableResourceSet, consumer StableSegmentTerminalConsumer) error {
	count := 0
	var account StableMetadataAccount
	total := len(pending) + len(sets)
	for i := 0; i < total; i++ {
		var set *StableResourceSet
		if i < len(pending) {
			set = pending[i].candidate.resourceSet()
		} else {
			set = sets[i-len(pending)]
		}
		if set != nil && set.Owner() != ResourceOwnerReleased {
			expected := ResourceOwnerRecovery
			if i < len(pending) {
				expected = ResourceOwnerCoordinator
			}
			if set.Owner() != expected {
				return ErrResourceOwnership
			}
			for j := 0; j < i; j++ {
				var prior *StableResourceSet
				if j < len(pending) {
					prior = pending[j].candidate.resourceSet()
				} else {
					prior = sets[j-len(pending)]
				}
				if set == prior {
					return ErrResourceOwnership
				}
			}
		}
		n, a, err := inspectStableTerminalSet(set, consumer)
		if err != nil {
			return err
		}
		if n > int(^uint(0)>>1)-count {
			return ErrStableMetadataShapeUnsupported
		}
		count += n
		if account == nil {
			account = a
		}
	}
	if count == 0 {
		return nil
	}
	n, err := StableBackingClassBytes(uint64(count)*uint64(unsafe.Sizeof((*StableResourceToken)(nil))), true)
	if err != nil {
		return err
	}
	if err = account.ReserveStableMetadata(n); err != nil {
		return err
	}
	tokens := make([]*StableResourceToken, 0, count)
	for i := 0; i < total; i++ {
		var set *StableResourceSet
		if i < len(pending) {
			set = pending[i].candidate.resourceSet()
		} else {
			set = sets[i-len(pending)]
		}
		if set == nil || set.Owner() == ResourceOwnerReleased {
			continue
		}
		set.mu.Lock()
		if set.metadataAccount != nil {
			for j := range set.entries {
				if ResourceOwnerState(set.entries[j].token.owner.Load()) != ResourceOwnerReleased {
					tokens = append(tokens, set.entries[j].token)
				}
			}
		}
		set.mu.Unlock()
	}
	return prepareStableSegmentTerminalTokens(tokens, consumer)
}

func finishStablePendingTerminal(entries []pendingEntry, candidate *PreparedRootCandidate, consumer StableSegmentTerminalConsumer) error {
	return finishStablePendingTerminalTransition(entries, candidate, consumer, nil)
}

func finishStablePendingTerminalTransition(entries []pendingEntry, candidate *PreparedRootCandidate, consumer StableSegmentTerminalConsumer, transition StableTerminalTransition) error {
	var scratch *StableTerminalScratch
	if prepared, ok := transition.(StableTerminalPreparedTransition); ok {
		scratch = prepared.TerminalScratch()
	}
	// This defer runs after EndTerminalRelease: a consumer plan must release its
	// borrowed scratch before the arena scrubs typed references.
	if scratch != nil {
		defer scratch.End()
	}
	joined := false
	if consumer != nil {
		var err error
		joined, err = consumer.BeginTerminalRelease()
		if err != nil {
			return err
		}
		defer consumer.EndTerminalRelease(joined)
	}
	var extra []StableTerminalOwnedSet
	var loose []*StableResourceToken
	if transition != nil {
		var err error
		extra, loose, err = transition.TerminalOwnedResources()
		if err != nil {
			return err
		}
	}
	var roles []StableTerminalOwnedSet
	if scratch != nil {
		var err error
		roles, err = scratch.combine(entries, extra)
		if err != nil {
			return err
		}
	} else {
		roles = make([]StableTerminalOwnedSet, 0, len(entries)+len(extra))
		for _, entry := range entries {
			roles = append(roles, StableTerminalOwnedSet{entry.candidate.resourceSet(), ResourceOwnerCoordinator})
		}
		roles = append(roles, extra...)
	}
	if err := prepareStableTerminalOwnedGroup(roles, loose, consumer, scratch); err != nil {
		return err
	}
	if transition != nil {
		if err := transition.CommitTerminalPublication(); err != nil {
			return err
		}
	}
	group := candidate.durableRootGroup()
	if err := consumeDurableRootGroup(group); err != nil {
		cause := errors.Join(ErrPublisherProtocol, err)
		return errors.Join(cause, failDurableRootGroup(group, cause))
	}
	if err := ReleasePreparedStableTerminalOwnedGroup(roles, loose, consumer); err != nil {
		return err
	}
	if transition != nil {
		return transition.CompleteTerminalPublication()
	}
	return nil
}
