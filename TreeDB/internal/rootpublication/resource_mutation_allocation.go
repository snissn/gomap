package rootpublication

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

// Mutation input is borrowed only for the synchronous construction. Exact
// duplicate declarations have normalization's idempotent semantics; conflicting
// keys and an addition/removal of the same value are refused before effects.
func validateBorrowedMutation(m StableLogicalObligationMutation) error {
	if err := validateBorrowedRequirements(StableLogicalObligationRequirements{ScopedFields: m.ScopedFields, Obligations: m.Added}); err != nil {
		return err
	}
	if err := validateBorrowedRequirements(StableLogicalObligationRequirements{ScopedFields: m.ScopedFields, Obligations: m.Removed}); err != nil {
		return err
	}
	for _, removed := range m.Removed {
		for _, added := range m.Added {
			if removed == added {
				return ErrResourceConflict
			}
		}
	}
	return nil
}
func firstMutationValue(values []StableLogicalObligation, i int) bool {
	for j := 0; j < i; j++ {
		if values[j] == values[i] {
			return false
		}
	}
	return true
}
func mutationField(m StableLogicalObligationMutation, field ReachabilityField) bool {
	for _, candidate := range m.ScopedFields {
		if candidate == field {
			return true
		}
	}
	return false
}
func validateBorrowedMutationFinal(m StableLogicalObligationMutation, r StableLogicalObligationRequirements) error {
	if err := validateBorrowedMutation(m); err != nil {
		return err
	}
	for _, field := range m.ScopedFields {
		if !requirementFieldScoped(r, field) {
			return ErrUnresolvedResource
		}
	}
	for _, o := range m.Added {
		if !requirementContainsScope(r, o) || requirementDesired(r, o) < 0 {
			return ErrUnresolvedResource
		}
	}
	for _, o := range m.Removed {
		if !requirementContainsScope(r, o) || requirementDesired(r, o) >= 0 {
			return ErrResourceConflict
		}
	}
	return nil
}

// The source's actual immutable descriptor owns its index/summary/physical
// edges through the certificate call. An empty selected set needs no backing.
func borrowSelectedMutationSource(source *StableResourceSet) (resourceKindBorrow, error) {
	if source == nil {
		return resourceKindBorrow{}, nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if !source.selectedKindViewsAccessibleLocked() {
		return resourceKindBorrow{}, ErrResourceOwnership
	}
	if source.kindViews == nil {
		if len(source.entries) != 0 {
			return resourceKindBorrow{}, ErrResourceOwnership
		}
		return resourceKindBorrow{}, nil
	}
	return source.kindViews.borrow()
}
func selectedFieldCommitment(views *resourceKindBacking, field ReachabilityField, excluded []ResourceKind) (stableLogicalObligationCommitment, bool, error) {
	var result stableLogicalObligationCommitment
	complete := true
	var err error
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil || token.released.Load() {
			err = ErrResourceOwnership
			return false
		}
		if resourceKindExcluded(token.kind, excluded) {
			return true
		}
		count, ok := entry.logicalObligations.commitmentCount()
		if !ok || count != uint64(entry.logicalObligations.count) || (count != 0 && !entry.logicalObligations.hasCommitments()) {
			complete = false
			return false
		}
		next := entry.logicalObligations.commitment(field)
		if ^uint64(0)-result.count < next.count {
			complete = false
			return false
		}
		result.add(next)
		return true
	})
	return result, complete, err
}

// Only immutable entry summaries are aggregated. No retained obligation
// history, generic entry snapshot, opaque map, or new ownership is constructed.
func certifySelectedMutation(source *StableResourceSet, m StableLogicalObligationMutation, r StableLogicalObligationRequirements, excluded []ResourceKind) (bool, error) {
	if err := validateBorrowedMutation(m); err != nil {
		return false, err
	}
	if len(r.ScopedNamespaces) != 0 || len(m.ScopedFields) == 0 || r.commitments == nil {
		return false, nil
	}
	count, ok := stableLogicalObligationCommitmentCount(r.commitments)
	if !ok || count != uint64(len(r.Obligations)) {
		return false, nil
	}
	borrow, err := borrowSelectedMutationSource(source)
	if err != nil {
		return false, err
	}
	defer borrow.release()
	for _, field := range m.ScopedFields {
		final, ok := r.commitments[field]
		if !ok {
			return false, nil
		}
		expected, complete, err := selectedFieldCommitment(borrow.backing, field, excluded)
		if err != nil || !complete {
			return false, err
		}
		for i, o := range m.Removed {
			if o.Reachability == field && firstMutationValue(m.Removed, i) {
				if !expected.removeObligation(o) {
					return false, nil
				}
			}
		}
		for i, o := range m.Added {
			if o.Reachability == field && firstMutationValue(m.Added, i) {
				expected.addObligation(o)
			}
		}
		if expected != final {
			return false, nil
		}
	}
	return true, nil
}

// Producer validation prices its exact duplicate-membership scratch before
// allocation. Input declarations are borrowed; the output never retains them.
func validateSelectedAppendViews(views *resourceKindBacking, m StableLogicalObligationMutation) (StableLogicalObligationMutation, StableResourceClosureWork, error) {
	work := StableResourceClosureWork{RequirementFieldsInspected: uint64(len(m.ScopedFields)), RequirementObligationsInspected: uint64(len(m.Added) + len(m.Removed))}
	if err := validateBorrowedMutation(m); err != nil {
		return m, work, err
	}
	if len(m.Removed) != 0 {
		return m, work, ErrResourceConflict
	}
	if views == nil || views.allocation == nil {
		if len(m.Added) != 0 {
			return m, work, ErrUnresolvedResource
		}
		return m, work, nil
	}
	scratch, err := newResourceAllocation(views.allocation.owner, retainedalloc.AllocationCharge(uint64(len(m.Added))))
	if err != nil {
		return m, work, err
	}
	seen := make([]bool, len(m.Added))
	defer func() { clear(seen); seen = nil; scratch.drop(); scratch.refund() }()
	var validationErr error
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		work.SourceEntriesInspected++
		if entry.outgoing == nil || entry.logicalObligations.directory != nil {
			validationErr = ErrResourceOwnership
			return false
		}
		for _, field := range entry.reachability.cells {
			if field.reachable && mutationField(m, field.field) && entry.logicalObligations.commitment(field.field).count == 0 {
				validationErr = ErrUnresolvedResource
				return false
			}
		}
		for o := range entry.logicalObligations.selectedValues() {
			work.SourceObligationsInspected++
			index := -1
			for i, announced := range m.Added {
				if announced == o {
					index = i
					break
				}
			}
			if !mutationField(m, o.Reachability) || index < 0 || seen[index] {
				validationErr = ErrResourceConflict
				return false
			}
			seen[index] = true
		}
		return true
	})
	if validationErr != nil {
		return m, work, validationErr
	}
	for i := range m.Added {
		if firstMutationValue(m.Added, i) && !seen[i] {
			return m, work, ErrUnresolvedResource
		}
	}
	work.NewlyAdmittedEntries = uint64(stableResourceKindViewCount(views))
	for i := range m.Added {
		if firstMutationValue(m.Added, i) {
			work.NewlyAdmittedObligations++
			work.LogicalObligationNormalizations++
		}
	}
	return m, work, nil
}

// Selected views have constructor-complete admitted membership indexes and
// flat obligations. Every kind is probed in canonical cell order without a
// detached kinds slice or directory parser scratch.
func selectedViewsAdmit(views *resourceKindBacking, entry, predecessor *stableResourceEntry, excluded []ResourceKind, work *StableResourceClosureWork) (bool, bool, error) {
	for _, cell := range viewsCells(views) {
		if !resourceKindExcluded(cell.kind, excluded) && !stableResourceViewLogicalMembershipComplete(cell.view) {
			return false, false, nil
		}
	}
	if entry == nil || entry.outgoing == nil || entry.logicalObligations.directory != nil {
		return false, false, ErrResourceOwnership
	}
	for o := range entry.logicalObligations.selectedValues() {
		for _, cell := range viewsCells(views) {
			if resourceKindExcluded(cell.kind, excluded) {
				continue
			}
			if cell.view.directory != nil {
				return false, false, nil
			}
			existing, found := findStableLogicalObligationIndex(cell.view.logicalMembership, o, work)
			if !found {
				continue
			}
			if predecessor != nil && predecessor.token.kind == cell.kind {
				owned, present, err := predecessor.logicalObligations.lookup(o, nil)
				if err != nil {
					return false, true, err
				}
				if present && owned == existing && existing == o {
					continue
				}
			}
			return false, true, nil
		}
	}
	return true, true, nil
}
func viewsCells(views *resourceKindBacking) []resourceKindCell {
	if views == nil {
		return nil
	}
	return views.cells
}
func certifySelectedAppend(source, producer *StableResourceSet, m StableLogicalObligationMutation, excluded []ResourceKind) (StableResourceClosureWork, bool, error) {
	producerBorrow, err := borrowSelectedMutationSource(producer)
	if err != nil {
		return StableResourceClosureWork{}, false, err
	}
	defer producerBorrow.release()
	_, work, err := validateSelectedAppendViews(producerBorrow.backing, m)
	if err != nil {
		// An exact eligibility decline needs the caller's complete fallback;
		// admission and lifetime failure must remain visible.
		if err == ErrResourceConflict || err == ErrUnresolvedResource {
			return work, false, nil
		}
		return work, false, err
	}
	if len(m.ScopedFields) == 0 {
		return work, false, nil
	}
	base, err := borrowSelectedMutationSource(source)
	if err != nil {
		return work, false, err
	}
	defer base.release()
	matches := true
	rangeStableResourceKindViews(producerBorrow.backing, func(entry *stableResourceEntry) bool {
		if resourceKindExcluded(entry.token.kind, excluded) {
			return true
		}
		var previous *stableResourceEntry
		if view, ok := base.backing.lookup(entry.token.kind); ok {
			previous = findStableResourceLogical(view.logical, entry.token.logicalKey())
		}
		if previous != nil && previous.token.physicalIdentityKey() != entry.token.physicalIdentityKey() {
			matches = false
			return false
		}
		admitted, complete, e := selectedViewsAdmit(base.backing, entry, previous, excluded, &work)
		err = e
		matches = admitted && complete
		return err == nil && matches
	})
	if err != nil || !matches {
		return work, false, err
	}
	for _, field := range m.ScopedFields {
		// Base completeness is independently checked even for empty additions.
		_, complete, e := selectedFieldCommitment(base.backing, field, excluded)
		if e != nil || !complete {
			return work, false, e
		}
		actual, complete, e := selectedFieldCommitment(producerBorrow.backing, field, nil)
		if e != nil || !complete {
			return work, false, e
		}
		var declared stableLogicalObligationCommitment
		for i, o := range m.Added {
			if o.Reachability == field && firstMutationValue(m.Added, i) {
				declared.addObligation(o)
			}
		}
		if actual != declared {
			return work, false, nil
		}
	}
	return work, true, nil
}

func cloneSelectedMutation(source *StableResourceSet, m StableLogicalObligationMutation, excluded []ResourceKind) (*StableResourceSet, StableResourceClosureWork, error) {
	work := StableResourceClosureWork{RequirementFieldsInspected: uint64(len(m.ScopedFields)), RequirementObligationsInspected: uint64(len(m.Added) + len(m.Removed))}
	if err := validateBorrowedMutation(m); err != nil {
		return nil, work, err
	}
	for i := range m.Added {
		if firstMutationValue(m.Added, i) {
			work.LogicalObligationNormalizations++
		}
	}
	for i := range m.Removed {
		if firstMutationValue(m.Removed, i) {
			work.LogicalObligationNormalizations++
			work.RemovedObligations++
		}
	}
	if len(m.Removed) == 0 {
		cloned, shared, err := cloneStableResourceSetKindView(source, excluded...)
		if err != nil {
			return nil, work, err
		}
		if shared {
			work.CloneOperations = 1
			if cloned != nil {
				work.PhysicalRootShares = uint64(cloned.kindViews.len())
			}
			return cloned, work, nil
		}
		return cloneSelectedResourceRequirements(source, StableLogicalObligationRequirements{}, excluded, work)
	}
	return cloneSelectedResourceFiltered(source, StableLogicalObligationRequirements{}, m.Removed, excluded, work)
}
