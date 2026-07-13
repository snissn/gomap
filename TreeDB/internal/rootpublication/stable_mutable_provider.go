package rootpublication

import "context"

// CapturedMutableResource is the provider-owned snapshot required to build a
// mutable append token. Implementations must keep callbacks bound to one open
// file or immutable generation; DiagnosticPath is never callback authority.
type CapturedMutableResource interface {
	StableIdentity() string
	StableGeneration() uint64
	StableDiagnosticPath() string
	Frontier() uint64
	FlushThrough(context.Context, uint64) error
	SyncThrough(context.Context, uint64) error
}

// NewMutableAppendToken adapts a captured provider snapshot into the exact
// StableResourceToken contract. release owns every descriptor and deletion pin
// associated with snapshot and is transferred to the returned token.
func NewMutableAppendToken(
	kind StableResourceKind,
	namespace string,
	reachableBy string,
	snapshot CapturedMutableResource,
	namespaceToken StableNamespaceToken,
	release func(),
) (StableResourceToken, error) {
	lease := NewStableResourceLease(release)
	if snapshot == nil {
		lease.Release()
		return StableResourceToken{}, ErrInvalidCandidate
	}
	token := StableResourceToken{
		Kind:           kind,
		Namespace:      namespace,
		Identity:       snapshot.StableIdentity(),
		Generation:     snapshot.StableGeneration(),
		DiagnosticPath: snapshot.StableDiagnosticPath(),
		Frontier:       snapshot.Frontier(),
		ReachableBy:    reachableBy,
		Provider:       lease,
		MutableAppend:  true,
		NamespaceToken: namespaceToken,
		FlushThrough:   snapshot.FlushThrough,
		SyncThrough:    snapshot.SyncThrough,
		Lease:          lease,
	}
	if err := token.validate(); err != nil {
		lease.Release()
		return StableResourceToken{}, err
	}
	return token, nil
}
