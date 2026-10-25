package collections

import (
	"errors"
	"fmt"
)

// ErrMutationNotAccepted identifies an immutable insert rejected during staging,
// before canonical assignment or logical publication. A caller may split a
// group of independent requests after this result. It must never infer this
// guarantee merely from the absence of ErrCommitAmbiguous on a generic error.
var ErrMutationNotAccepted = errors.New("collections: mutation not accepted")

func mutationNotAcceptedError(err error) error {
	return fmt.Errorf("%w: %w", ErrMutationNotAccepted, err)
}
