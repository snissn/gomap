package rootpublication

import "unsafe"

// StableTerminalOwnedSet names one actual owned role, not a borrowed slot view.
// The whole group is validated before any allocation, gate or ownership effect.
type StableTerminalOwnedSet struct {
	Set   *StableResourceSet
	Owner ResourceOwnerState
}

// StableTerminalTransition is a trusted concrete, synchronous publication
// bridge. It is passed on the reporter stack and is never retained by an owner.
type StableTerminalTransition interface {
	TerminalOwnedResources() ([]StableTerminalOwnedSet, []*StableResourceToken, error)
	CommitTerminalPublication() error
	CompleteTerminalPublication() error
}

func PrepareStableTerminalOwnedGroup(sets []StableTerminalOwnedSet, loose []*StableResourceToken, consumer StableSegmentTerminalConsumer) error {
	return prepareStableTerminalOwnedGroup(sets, loose, consumer, nil)
}

// PrepareStableTerminalOwnedGroupWithScratch is the trusted synchronous
// shutdown/direct variant. Begin/End and the owner reservation remain with the
// concrete caller; this supplies storage, never publication authority.
func PrepareStableTerminalOwnedGroupWithScratch(sets []StableTerminalOwnedSet, loose []*StableResourceToken, consumer StableSegmentTerminalConsumer, scratch *StableTerminalScratch) error {
	if scratch == nil {
		return ErrStableMetadataShapeUnsupported
	}
	return prepareStableTerminalOwnedGroup(sets, loose, consumer, scratch)
}

func prepareStableTerminalOwnedGroup(sets []StableTerminalOwnedSet, loose []*StableResourceToken, consumer StableSegmentTerminalConsumer, scratch *StableTerminalScratch) error {
	if scratch != nil && (!scratch.inUse || scratch.closed || len(sets) > scratch.capacity.Roles || len(loose) > scratch.capacity.LooseTokens) {
		return ErrStableMetadataShapeUnsupported
	}
	// Actual owner reservations exclude mutation of these closures while
	// preparing. Never hold two set mutexes while validating aliases.
	for i, role := range sets {
		if role.Set == nil || role.Set.Owner() == ResourceOwnerReleased {
			continue
		}
		for k := 0; ; k++ {
			role.Set.mu.Lock()
			if k >= len(role.Set.entries) {
				role.Set.mu.Unlock()
				break
			}
			token := role.Set.entries[k].token
			role.Set.mu.Unlock()
			if token == nil {
				continue
			}
			for _, looseToken := range loose {
				if token == looseToken {
					return ErrResourceOwnership
				}
			}
			for j := 0; j < i; j++ {
				other := sets[j].Set
				if other == nil || other == role.Set || other.Owner() == ResourceOwnerReleased {
					continue
				}
				other.mu.Lock()
				duplicate := false
				for _, prior := range other.entries {
					if prior.token == token {
						duplicate = true
						break
					}
				}
				other.mu.Unlock()
				if duplicate {
					return ErrResourceOwnership
				}
			}
		}
	}
	count := 0
	var account StableMetadataAccount
	for i, role := range sets {
		if role.Set == nil || role.Set.Owner() == ResourceOwnerReleased {
			continue
		}
		if role.Set.Owner() != role.Owner {
			return ErrResourceOwnership
		}
		for j := 0; j < i; j++ {
			if sets[j].Set == role.Set {
				return ErrResourceOwnership
			}
		}
		n, a, err := inspectStableTerminalSet(role.Set, consumer)
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
	for i, token := range loose {
		if token == nil || ResourceOwnerState(token.owner.Load()) == ResourceOwnerReleased {
			continue
		}
		if ResourceOwnerState(token.owner.Load()) != ResourceOwnerToken {
			return ErrResourceOwnership
		}
		for j := 0; j < i; j++ {
			if loose[j] == token {
				return ErrResourceOwnership
			}
		}
		if err := validateStableSegmentTerminalRetention(token.segmentRetention, consumer); err != nil {
			return err
		}
		if token.metadataAccount != nil {
			count++
			if account == nil {
				account = token.metadataAccount
			}
		} else if token.finiteMetadata {
			return ErrStableMetadataShapeUnsupported
		}
	}
	if count == 0 {
		return nil
	}
	var tokens []*StableResourceToken
	if scratch != nil {
		if !scratch.inUse || scratch.closed || count > cap(scratch.tokens) {
			return ErrStableMetadataShapeUnsupported
		}
		tokens = scratch.tokens[:0]
	} else {
		n, err := StableBackingClassBytes(uint64(count)*uint64(unsafe.Sizeof((*StableResourceToken)(nil))), true)
		if err != nil {
			return err
		}
		if err = account.ReserveStableMetadata(n); err != nil {
			return err
		}
		tokens = make([]*StableResourceToken, 0, count)
	}
	for _, role := range sets {
		if role.Set == nil || role.Set.Owner() == ResourceOwnerReleased {
			continue
		}
		role.Set.mu.Lock()
		if role.Set.metadataAccount != nil {
			for _, e := range role.Set.entries {
				if ResourceOwnerState(e.token.owner.Load()) != ResourceOwnerReleased {
					tokens = append(tokens, e.token)
				}
			}
		}
		role.Set.mu.Unlock()
	}
	for _, token := range loose {
		if token != nil && token.metadataAccount != nil && ResourceOwnerState(token.owner.Load()) != ResourceOwnerReleased {
			tokens = append(tokens, token)
		}
	}
	for i, token := range tokens {
		for j := 0; j < i; j++ {
			if tokens[j] == token {
				return ErrResourceOwnership
			}
		}
	}
	return prepareStableSegmentTerminalTokensWithScratch(tokens, consumer, scratch)
}

// These releases use the already prepared complete group. They do not Begin,
// Prepare or End again, and preserve the real unfinished owner on any failure.
func ReleasePreparedStableTerminalSet(role StableTerminalOwnedSet, consumer StableSegmentTerminalConsumer) error {
	return role.Set.releaseFromWithTerminal(role.Owner, consumer)
}
func ReleasePreparedStableTerminalToken(token *StableResourceToken, consumer StableSegmentTerminalConsumer) error {
	return token.releaseFromWithTerminal(ResourceOwnerToken, consumer)
}
func ReleasePreparedStableTerminalOwnedGroup(sets []StableTerminalOwnedSet, tokens []*StableResourceToken, consumer StableSegmentTerminalConsumer) error {
	for _, role := range sets {
		if err := ReleasePreparedStableTerminalSet(role, consumer); err != nil {
			return err
		}
	}
	for _, token := range tokens {
		if err := ReleasePreparedStableTerminalToken(token, consumer); err != nil {
			return err
		}
	}
	return nil
}
