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

// Select an original empty edge without mutating the graph. Arbitrary wrapping,
// Is/As and cycle traversal happens before entering the DB publication lock.
func SelectOriginalCleanupPrior(incoming, prior error) OriginalCleanupPriorBinder {
	if incoming == nil {
		return nil
	}
	if original, ok := incoming.(OriginalCleanupPriorBinder); ok &&
		original.OriginalCleanupPriorAvailable() && !errors.Is(prior, original) {
		return original
	}
	switch wrapped := incoming.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			if original := SelectOriginalCleanupPrior(child, prior); original != nil {
				return original
			}
		}
	case interface{ Unwrap() error }:
		if original := SelectOriginalCleanupPrior(wrapped.Unwrap(), prior); original != nil {
			return original
		}
	}
	var original OriginalCleanupPriorBinder
	if errors.As(incoming, &original) && original.OriginalCleanupPriorAvailable() &&
		!errors.Is(prior, original) {
		return original
	}
	return nil
}

// IsOriginalCleanupError identifies an original receipt before applying the
// caller's legacy generic-error policy. Error traversal must run outside locks.
func IsOriginalCleanupError(incoming error) bool {
	var original originalCleanupPriorLinker
	return incoming != nil && errors.As(incoming, &original)
}
