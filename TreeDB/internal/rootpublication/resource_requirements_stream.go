package rootpublication

// StableLogicalObligationRequirementStream is an immutable, replayable view of
// one original producer's complete requirements. Walk order is unrestricted.
// Lookup resolves logical identity, including a conflicting full value when
// integrity metadata differs. Identical declarations are idempotent.
//
// Both methods borrow their results only until the callback/next lookup. Walk
// and Lookup require independent, originally admitted scratch: a lookup can
// run while Walk's emitted value remains borrowed. The caller retains the exact
// captured snapshot, immutable overlay and receipt through finalization. Neither
// method may recapture, publish, or retain a callback. This representation is
// supported only by the original selected metadata owner.
type StableLogicalObligationRequirementStream interface {
	Walk(func(StableLogicalObligation) error) (StableResourceClosureWork, error)
	Lookup(StableLogicalObligation) (StableLogicalObligation, bool, error)
}

// Resolve the union without collecting retained history. Comparing full values
// preserves conflicting-integrity rejection across ordinary and streamed inputs.
func lookupRequirement(r StableLogicalObligationRequirements, key StableLogicalObligation) (value StableLogicalObligation, found bool, err error) {
	accept := func(o StableLogicalObligation) error {
		if stableLogicalObligationKey(o) != stableLogicalObligationKey(key) || !requirementContainsScope(r, o) {
			return ErrResourceConflict
		}
		if err := validateStableLogicalObligation(o, o.Reachability); err != nil {
			return err
		}
		if found && (value.Checksum != o.Checksum || value.Digest != o.Digest) {
			return ErrResourceConflict
		}
		// Keep identity strings from the still-live input, never a prior Lookup's
		// scratch while invoking another source using that same workspace.
		value, found = key, true
		value.Checksum, value.Digest = o.Checksum, o.Digest
		return nil
	}
	for _, o := range r.Obligations {
		if stableLogicalObligationKey(o) == stableLogicalObligationKey(key) {
			if err := accept(o); err != nil {
				return StableLogicalObligation{}, false, err
			}
		}
	}
	for _, stream := range r.Streams {
		if stream == nil {
			return StableLogicalObligation{}, false, ErrResourceOwnership
		}
		o, present, err := stream.Lookup(key)
		if err != nil {
			return StableLogicalObligation{}, false, err
		}
		if present {
			if err := accept(o); err != nil {
				return StableLogicalObligation{}, false, err
			}
		}
	}
	return value, found, nil
}

func requirementHasValue(r StableLogicalObligationRequirements, o StableLogicalObligation) (bool, error) {
	if len(r.Streams) == 0 {
		return requirementDesired(r, o) >= 0, nil
	}
	value, found, err := lookupRequirement(r, o)
	return found && value == o, err
}

// The source's exact point lookup also checks conflicting duplicate identities
// within an unordered walk. Identical repeated declarations are harmless, as in
// NormalizeStableLogicalObligationRequirements. No uniqueness array is needed.
func walkRequirementValues(r StableLogicalObligationRequirements, work *StableResourceClosureWork, visit func(StableLogicalObligation) error) error {
	emit := func(o StableLogicalObligation) error {
		if !requirementContainsScope(r, o) {
			return ErrResourceConflict
		}
		if err := validateStableLogicalObligation(o, o.Reachability); err != nil {
			return err
		}
		got, found, err := lookupRequirement(r, o)
		if err != nil {
			return err
		}
		if !found || got != o {
			return ErrResourceConflict
		}
		work.RequirementObligationsInspected++
		return visit(o)
	}
	for _, o := range r.Obligations {
		if err := emit(o); err != nil {
			return err
		}
	}
	for _, stream := range r.Streams {
		if stream == nil {
			return ErrResourceOwnership
		}
		streamWork, err := stream.Walk(emit)
		work.Add(streamWork)
		if err != nil {
			return err
		}
	}
	return nil
}

// The original per-entry index/directory owns uniqueness within each entry.
// Probe every physical owner so an identical identity claimed by two owners is
// still rejected. The admitted physical census and one original codec are the
// only scratch; retained obligation count never sizes an allocation.
func uniqueSelectedRequirementOwner(views *resourceKindBacking, scope *ownedDirectoryCodecScopeV2, o StableLogicalObligation, work *StableResourceClosureWork) error {
	count := 0
	var lookupErr error
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		work.AggregateMembershipProbes++
		actual, found, err := lookupOwnedEntryObligationWithScopeV2(entry, o, nil, scope)
		if err != nil {
			lookupErr = err
			return false
		}
		if found {
			count++
			if actual != o || count > 1 {
				lookupErr = ErrResourceConflict
				return false
			}
		}
		return true
	})
	if lookupErr != nil {
		return lookupErr
	}
	if count != 1 {
		return ErrUnresolvedResource
	}
	return nil
}
