package rootpublication

import "errors"

type originalCleanupPriorLinker interface {
	error
	LinkPriorCleanupError(error)
}
type OriginalCleanupPriorBinder interface {
	originalCleanupPriorLinker
	OriginalCleanupPriorAvailable() bool
	TryLinkPriorCleanupError(error) bool
}

type originalCleanupPriorBindingOwner interface {
	error
	OriginalCleanupPriorBinding() OriginalCleanupPriorBinding
}

// Select an original empty edge without mutating the graph. Arbitrary wrapping,
// Is/As and cycle traversal happens before entering the DB publication lock.
func SelectOriginalCleanupPrior(incoming, prior error) OriginalCleanupPriorBinding {
	if incoming == nil {
		return OriginalCleanupPriorBinding{}
	}
	if original, ok := incoming.(originalCleanupPriorBindingOwner); ok {
		binding := original.OriginalCleanupPriorBinding()
		if binding.Available() && !errors.Is(prior, original) {
			return binding
		}
	}
	switch wrapped := incoming.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			if original := SelectOriginalCleanupPrior(child, prior); original.Valid() {
				return original
			}
		}
	case interface{ Unwrap() error }:
		if original := SelectOriginalCleanupPrior(wrapped.Unwrap(), prior); original.Valid() {
			return original
		}
	}
	var original originalCleanupPriorBindingOwner
	if errors.As(incoming, &original) {
		binding := original.OriginalCleanupPriorBinding()
		if binding.Available() && !errors.Is(prior, original) {
			return binding
		}
	}
	return OriginalCleanupPriorBinding{}
}

// IsOriginalCleanupError identifies an original receipt before applying the
// caller's legacy generic-error policy. Error traversal must run outside locks.
func IsOriginalCleanupError(incoming error) bool {
	var original originalCleanupPriorLinker
	return incoming != nil && errors.As(incoming, &original)
}
