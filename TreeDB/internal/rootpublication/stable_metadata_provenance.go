package rootpublication

import "reflect"

// inheritStableMetadataAccount is the single constructor provenance boundary.
// A namespace loan carries its exact owner into every derived resource token;
// an explicit different owner cannot acquire that loan.
func inheritStableMetadataAccount(account StableMetadataAccount, namespace *StableNamespaceToken) (StableMetadataAccount, error) {
	if namespace == nil {
		return account, nil
	}
	namespace.mu.Lock()
	defer namespace.mu.Unlock()
	if namespace.metadataAccount == nil {
		if account != nil {
			return nil, ErrStableMetadataShapeUnsupported
		}
		return nil, nil
	}
	inherited := namespace.metadataAccount
	if account == nil {
		return inherited, nil
	}
	if !reflect.ValueOf(account).Comparable() || !reflect.ValueOf(inherited).Comparable() || account != inherited {
		return nil, ErrStableMetadataShapeUnsupported
	}
	return account, nil
}

// RequireMetadataExport rejects generic backing exports from finite owners.
// Legacy getters return unavailable zero/nil on this error. Identity, digest
// and numeric frontier accessors remain independent values. A successful check
// grants no new lifetime or pin; ordinary ownership contracts still apply.
func (token *StableResourceToken) RequireMetadataExport() error {
	if token == nil {
		return nil
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	return token.requireMetadataExportLocked()
}
func (token *StableResourceToken) requireMetadataExportLocked() error {
	if token.finiteMetadata || token.metadataAccount != nil {
		return ErrStableMetadataShapeUnsupported
	}
	if token.namespace != nil {
		token.namespace.mu.Lock()
		accounted := token.namespace.metadataAccount != nil
		token.namespace.mu.Unlock()
		if accounted {
			return ErrStableMetadataShapeUnsupported
		}
	}
	return nil
}

// requireOrdinaryStableResourceInputs is an allocation-free full closure scan.
// Callers hold their established builder/set locks. It checks every operand,
// including pins omitted by coalescing or a representative selection. Finite
// inputs cannot enter the ordinary engine through Add/Merge/Freeze, so their
// absence remains invariant after a frozen input's ownership lock is released.
func requireOrdinaryStableResourceInputs(entries []stableResourceEntry, views stableKindViews, sets ...*StableResourceSet) error {
	for i := range entries {
		if stableResourceEntryHasMetadataAccount(&entries[i]) {
			return ErrStableMetadataShapeUnsupported
		}
	}
	if err := rejectAccountedStableResourceViews(views); err != nil {
		return err
	}
	for _, set := range sets {
		if set == nil {
			continue
		}
		if set.finiteMetadata || set.metadataAccount != nil {
			return ErrStableMetadataShapeUnsupported
		}
		if !set.ordinaryMetadata {
			if err := requireOrdinaryStableResourceInputs(set.entries, set.kindViews); err != nil {
				return err
			}
			set.ordinaryMetadata = true
		}
	}
	return nil
}

// RequireMetadataExport applies the same full input policy to generic set
// diagnostics and borrowed outputs, including internally malformed mixed sets.
func (set *StableResourceSet) RequireMetadataExport() error {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return requireOrdinaryStableResourceInputs(nil, nil, set)
}

func requireOrdinaryStableResourceSets(sets ...*StableResourceSet) error {
	for _, set := range sets {
		if err := set.RequireMetadataExport(); err != nil {
			return err
		}
	}
	return nil
}

func requireOrdinaryStableResourceBuilderInputs(builder *StableResourceSetBuilder, sets ...*StableResourceSet) error {
	if builder != nil {
		builder.mu.Lock()
		err := builder.requireOrdinaryMetadataLocked()
		builder.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return requireOrdinaryStableResourceSets(sets...)
}

// A trusted ordinary constructor starts with this invariant. Every admission
// edge rejects incoming finite provenance before ownership or backing work.
// Unknown objects must certify their complete closure. Internal uncertainty
// must clear the stamp before calling an engine operation. This is provenance,
// never a map-capacity, retained-byte or request-ledger certificate.
func (builder *StableResourceSetBuilder) requireOrdinaryMetadataLocked() error {
	if builder.ordinaryMetadata {
		return nil
	}
	if err := requireOrdinaryStableResourceInputs(builder.entries, builder.kindViews); err != nil {
		return err
	}
	builder.ordinaryMetadata = true
	return nil
}

func requireOrdinaryStableResourceOperationLocked(builder *StableResourceSetBuilder, sets ...*StableResourceSet) error {
	if err := builder.requireOrdinaryMetadataLocked(); err != nil {
		return err
	}
	return requireOrdinaryStableResourceInputs(nil, nil, sets...)
}
