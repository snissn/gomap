package rootpublication

import (
	"errors"
	"unsafe"
)

var (
	ErrStableTerminalConsumerRequired = errors.New("stable registration release requires an exact terminal consumer")
	ErrStableTerminalConsumerMismatch = errors.New("stable registration terminal consumer does not match retained incarnation")
)

// The consumer is passed on the synchronous terminal call stack. Tokens, sets
// and registrar holds must never store it. Begin/End join the existing Manager
// close boundary; validation of the complete group precedes ownership effects.
type StableSegmentTerminalConsumer interface {
	BeginTerminalRelease() (bool, error)
	PrepareTerminalRelease([]StableSegmentTerminalGroup) error
	ValidateSegmentRetention(StableSegmentRetention) error
	ReleaseSegmentRetention(StableSegmentRetention) error
	EndTerminalRelease(bool)
}

// A finite registrar hold exposes only scalar registration lifetime. Generic
// terminal calls receive an observable refusal while a live consumer is needed.
// Closed exact cells can discharge after the actual Manager has completed Close.
type StableSegmentTerminalRetention interface {
	StableSegmentRetention
	ValidateTerminalRelease(StableSegmentTerminalConsumer) error
	ReleaseWithTerminal(StableSegmentTerminalConsumer) error
}

func validateStableSegmentTerminalRetention(retention StableSegmentRetention, consumer StableSegmentTerminalConsumer) error {
	if checked, ok := retention.(StableSegmentTerminalRetention); ok {
		return checked.ValidateTerminalRelease(consumer)
	}
	return nil
}
func releaseStableSegmentTerminalRetention(retention StableSegmentRetention, consumer StableSegmentTerminalConsumer) error {
	if retention == nil {
		return nil
	}
	if checked, ok := retention.(StableSegmentTerminalRetention); ok {
		return checked.ReleaseWithTerminal(consumer)
	}
	return retention.Release()
}

// StableSegmentTerminalGroup is caller-stack preparation data, never retained
// by a token, set or coordinator. OwnedPins are the concrete complete group
// closure; the consumer verifies exact registry membership before mutation.
type StableSegmentTerminalGroup struct {
	Retention StableSegmentRetention
	OwnedPins []*IdentityPin
}

// prepareStableSegmentTerminalTokens accounts all actual scratch births before
// constructing the transient plan. Selected finite sets use direct owned arrays;
// generic view/rope paths remain outside finite admission.
func prepareStableSegmentTerminalTokens(tokens []*StableResourceToken, consumer StableSegmentTerminalConsumer) error {
	return prepareStableSegmentTerminalTokensWithScratch(tokens, consumer, nil)
}

func prepareStableSegmentTerminalTokensWithScratch(tokens []*StableResourceToken, consumer StableSegmentTerminalConsumer, scratch *StableTerminalScratch) error {
	count := 0
	for _, token := range tokens {
		if token == nil {
			return ErrResourceOwnership
		}
		token.metadataMu.Lock()
		if token.activeOperations != 0 || token.cleanupRunning || token.cleanupUncertain {
			token.metadataMu.Unlock()
			return ErrStableResourceOperationBusy
		}
		retention := token.segmentRetention
		token.metadataMu.Unlock()
		if retention != nil {
			if err := validateStableSegmentTerminalRetention(retention, consumer); err != nil {
				return err
			}
			count++
		}
	}
	if count == 0 {
		return nil
	}
	if consumer == nil {
		return nil
	}
	var account StableMetadataAccount
	for _, token := range tokens {
		token.metadataMu.Lock()
		retention, tokenAccount := token.segmentRetention, token.metadataAccount
		token.metadataMu.Unlock()
		if retention != nil {
			if tokenAccount == nil {
				return ErrStableMetadataShapeUnsupported
			}
			if account == nil {
				account = tokenAccount
			}
		}
	}
	var groups []StableSegmentTerminalGroup
	var pins []*IdentityPin
	if scratch != nil {
		if !scratch.inUse || scratch.closed || count > cap(scratch.groups) || len(tokens) > cap(scratch.pins)/2 {
			return ErrStableMetadataShapeUnsupported
		}
		groups = scratch.groups[:0]
		pins = scratch.pins[:0]
	} else {
		var plan stableBackingSizePlan
		plan.add(uint64(count)*uint64(unsafe.Sizeof(StableSegmentTerminalGroup{})), true)
		plan.add(uint64(len(tokens))*2*uint64(unsafe.Sizeof((*IdentityPin)(nil))), true)
		if plan.err != nil {
			return plan.err
		}
		if err := account.ReserveStableMetadata(plan.bytes); err != nil {
			return err
		}
		groups = make([]StableSegmentTerminalGroup, 0, count)
		pins = make([]*IdentityPin, 0, len(tokens)*2)
	}
	for _, token := range tokens {
		token.metadataMu.Lock()
		retention, identityPin, owner := token.segmentRetention, token.identityPin, token.segmentOwner
		live := !token.released.Load()
		token.metadataMu.Unlock()
		if retention == nil {
			continue
		}
		start := len(pins)
		if live && identityPin != nil {
			pins = append(pins, identityPin)
		}
		if owner != nil {
			owner.mu.Lock()
			refs := int64(0)
			for _, other := range tokens {
				other.metadataMu.Lock()
				sameOwner := other.segmentOwner == owner
				other.metadataMu.Unlock()
				if sameOwner {
					refs++
				}
			}
			var sourcePin *IdentityPin
			if owner.token != nil {
				owner.token.metadataMu.Lock()
				sourcePin = owner.token.identityPin
				owner.token.metadataMu.Unlock()
			}
			if owner.token != nil && owner.refs.Load() == refs && sourcePin != nil {
				// Add the source pin once, after proving ALL actual owner refs drain here.
				first := true
				for _, group := range groups {
					for _, pin := range group.OwnedPins {
						if pin == sourcePin {
							first = false
						}
					}
				}
				if first {
					pins = append(pins, sourcePin)
				}
			}
			owner.mu.Unlock()
		}
		groups = append(groups, StableSegmentTerminalGroup{Retention: retention, OwnedPins: pins[start:len(pins)]})
	}
	return consumer.PrepareTerminalRelease(groups)
}
