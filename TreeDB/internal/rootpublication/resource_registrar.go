package rootpublication

import (
	"fmt"
	"sort"
	"sync"
)

// RegisteredResourceID is an ID whose complete transitive resource set has
// been merged and frozen. The fields remain private so producer code cannot
// manufacture publication-ready IDs independently of their stable resources.
type RegisteredResourceID struct {
	field ReachabilityField
	value string
}

func (id RegisteredResourceID) Field() ReachabilityField { return id.field }

func (id RegisteredResourceID) Value() string { return id.value }

// StableCompositeRegistrar joins child resource sets before exposing any of
// their IDs. It is intentionally a one-way builder: successful registration
// consumes each child set and Freeze is the only operation that yields IDs.
type StableCompositeRegistrar struct {
	mu       sync.Mutex
	builder  *StableResourceSetBuilder
	required map[ReachabilityField]struct{}
	pending  map[ReachabilityField]string
	closed   bool
}

func NewStableCompositeRegistrar(required ...ReachabilityField) *StableCompositeRegistrar {
	requiredSet := make(map[ReachabilityField]struct{}, len(required))
	for _, field := range required {
		if field != "" {
			requiredSet[field] = struct{}{}
		}
	}
	return &StableCompositeRegistrar{
		builder:  NewStableResourceSetBuilder(required...),
		required: requiredSet,
		pending:  make(map[ReachabilityField]string),
	}
}

// RegisterChild consumes child only after its complete resource set proves it
// covers field and can be merged without conflicts. Recording the ID is the
// final step, so a failed merge can never leave a publication-ready ID behind.
func (registrar *StableCompositeRegistrar) RegisterChild(field ReachabilityField, id string, child *StableResourceSet) error {
	if registrar == nil || child == nil || field == "" || id == "" {
		return ErrResourceOwnership
	}
	policy, ok := StableResourcePolicyFor(field)
	if !ok {
		return fmt.Errorf("%w: unknown reachability field %q", ErrUnresolvedResource, field)
	}
	if !policy.Registerable {
		return fmt.Errorf("%w: %s is %s", ErrResourceExcluded, field, policy.Classification)
	}

	registrar.mu.Lock()
	defer registrar.mu.Unlock()
	if registrar.closed {
		return ErrResourceOwnership
	}
	if existing, ok := registrar.pending[field]; ok && existing != id {
		return fmt.Errorf("%w: field %q already registered as %q", ErrResourceConflict, field, existing)
	}
	if !child.covers(field) {
		return fmt.Errorf("%w: child %q does not cover reachability field %q", ErrUnresolvedResource, id, field)
	}
	if err := registrar.builder.Merge(child); err != nil {
		return err
	}
	registrar.pending[field] = id
	return nil
}

// Freeze is the sole publication boundary. Missing child registrations are
// checked before freezing the underlying builder so callers can still abandon
// it and release every transferred token deterministically.
func (registrar *StableCompositeRegistrar) Freeze() (*StableResourceSet, []RegisteredResourceID, error) {
	if registrar == nil {
		return nil, nil, ErrResourceOwnership
	}
	registrar.mu.Lock()
	defer registrar.mu.Unlock()
	if registrar.closed {
		return nil, nil, ErrResourceOwnership
	}
	for field := range registrar.required {
		if _, ok := registrar.pending[field]; !ok {
			return nil, nil, fmt.Errorf("%w: missing registered child ID for %q", ErrUnresolvedResource, field)
		}
	}
	set, err := registrar.builder.Freeze()
	if err != nil {
		return nil, nil, err
	}
	ids := make([]RegisteredResourceID, 0, len(registrar.pending))
	for field, value := range registrar.pending {
		ids = append(ids, RegisteredResourceID{field: field, value: value})
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].field != ids[j].field {
			return ids[i].field < ids[j].field
		}
		return ids[i].value < ids[j].value
	})
	registrar.closed = true
	return set, ids, nil
}

func (registrar *StableCompositeRegistrar) Abandon() {
	if registrar == nil {
		return
	}
	registrar.mu.Lock()
	if registrar.closed {
		registrar.mu.Unlock()
		return
	}
	registrar.closed = true
	builder := registrar.builder
	registrar.mu.Unlock()
	builder.Abandon()
}
