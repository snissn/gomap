package collections

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// VectorPartitionStorageBarrierV1 serializes durable vector-partition
// namespace mutation with snapshot export for one DB root. It is deliberately
// root-scoped (not collection-scoped): snapshots copy column_assets and
// vector_partitions together.
type vectorPartitionStorageBarrierEntryV1 struct {
	gate chan struct{}
	refs int
}

var vectorPartitionStorageBarriersV1 = struct {
	sync.Mutex
	entries map[string]*vectorPartitionStorageBarrierEntryV1
}{entries: make(map[string]*vectorPartitionStorageBarrierEntryV1)}

// WithVectorPartitionStorageBarrierV1 is non-reentrant for a root: fn must
// not invoke it again for the same root, or it will wait on its own mutation.
func WithVectorPartitionStorageBarrierV1(root string, fn func() error) error {
	return WithVectorPartitionStorageBarrierWithContextV1(context.Background(), root, fn)
}

// WithVectorPartitionStorageBarrierWithContextV1 makes waiting for the
// root-scoped barrier cancellation-aware. Once fn starts it remains
// responsible for observing ctx itself.
func WithVectorPartitionStorageBarrierWithContextV1(ctx context.Context, root string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical, err := canonicalVectorPartitionStorageRootV1(root)
	if err != nil {
		return err
	}
	vectorPartitionStorageBarriersV1.Lock()
	entry := vectorPartitionStorageBarriersV1.entries[canonical]
	if entry == nil {
		entry = &vectorPartitionStorageBarrierEntryV1{gate: make(chan struct{}, 1)}
		entry.gate <- struct{}{}
		vectorPartitionStorageBarriersV1.entries[canonical] = entry
	}
	entry.refs++
	vectorPartitionStorageBarriersV1.Unlock()
	select {
	case <-ctx.Done():
		vectorPartitionStorageBarriersV1.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(vectorPartitionStorageBarriersV1.entries, canonical)
		}
		vectorPartitionStorageBarriersV1.Unlock()
		return ctx.Err()
	case <-entry.gate:
		if err := ctx.Err(); err != nil {
			entry.gate <- struct{}{}
			vectorPartitionStorageBarriersV1.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(vectorPartitionStorageBarriersV1.entries, canonical)
			}
			vectorPartitionStorageBarriersV1.Unlock()
			return err
		}
	}
	defer func() {
		entry.gate <- struct{}{}
		vectorPartitionStorageBarriersV1.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(vectorPartitionStorageBarriersV1.entries, canonical)
		}
		vectorPartitionStorageBarriersV1.Unlock()
	}()
	return fn()
}

func canonicalVectorPartitionStorageRootV1(root string) (string, error) {
	if root == "" {
		return "", errors.New("collections: empty vector partition barrier root")
	}
	canonical, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(canonical); resolveErr == nil {
		canonical = resolved
	}
	return canonical, nil
}

// tryVectorPartitionStorageBarrier admits maintenance only when no snapshot or
// asset publication owns this root. Unlike the foreground helper it never waits.
func tryVectorPartitionStorageBarrier(root string) (func(), bool) {
	canonical, err := canonicalVectorPartitionStorageRootV1(root)
	if err != nil || !vectorPartitionStorageBarriersV1.TryLock() {
		return nil, false
	}
	entry := vectorPartitionStorageBarriersV1.entries[canonical]
	if entry == nil {
		entry = &vectorPartitionStorageBarrierEntryV1{gate: make(chan struct{}, 1)}
		entry.gate <- struct{}{}
		vectorPartitionStorageBarriersV1.entries[canonical] = entry
	}
	select {
	case <-entry.gate:
		entry.refs++
		vectorPartitionStorageBarriersV1.Unlock()
		return func() {
			entry.gate <- struct{}{}
			vectorPartitionStorageBarriersV1.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(vectorPartitionStorageBarriersV1.entries, canonical)
			}
			vectorPartitionStorageBarriersV1.Unlock()
		}, true
	default:
		vectorPartitionStorageBarriersV1.Unlock()
		return nil, false
	}
}

// VectorPrepareStableCaptureV1 pins the genuine DB generation before waiting
// for the root storage barrier. Copies share the same retirement state.
type VectorPrepareStableCaptureV1 struct {
	state *vectorPrepareStableCaptureStateV1
}
type vectorPrepareStableCaptureStateV1 struct {
	mu   sync.Mutex
	db   *backenddb.DB
	root string
	pin  *backenddb.Snapshot
}

// VectorPrepareStorageOwnerV1 is borrowed only during WithStorageBarrierV1.
// Copies share callback expiry; retaining a pointer never extends authority.
type VectorPrepareStorageOwnerV1 struct {
	state *vectorPrepareStorageOwnerStateV1
}
type vectorPrepareStorageOwnerStateV1 struct {
	mu     sync.Mutex
	db     *backenddb.DB
	root   string
	pin    *backenddb.Snapshot
	active bool
}

func AcquireVectorPrepareStableCaptureV1(db *backenddb.DB) (*VectorPrepareStableCaptureV1, error) {
	if db == nil {
		return nil, errCollectionDBNil
	}
	root, err := canonicalVectorPartitionStorageRootV1(db.Dir())
	if err != nil {
		return nil, err
	}
	pin := db.AcquireStableSnapshot()
	if pin == nil {
		return nil, backenddb.ErrClosed
	}
	return &VectorPrepareStableCaptureV1{state: &vectorPrepareStableCaptureStateV1{db: db, root: root, pin: pin}}, nil
}

// Close waits for an active callback and retires the snapshot once.
func (capture *VectorPrepareStableCaptureV1) Close() {
	if capture == nil || capture.state == nil {
		return
	}
	state := capture.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pin != nil {
		state.pin.Close()
		state.pin = nil
	}
}

func (capture *VectorPrepareStableCaptureV1) WithStorageBarrierV1(ctx context.Context, fn func(*VectorPrepareStorageOwnerV1) error) error {
	if capture == nil || capture.state == nil || fn == nil {
		return errors.New("collections: vector prepare capture unavailable")
	}
	state := capture.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pin == nil {
		return errors.New("collections: vector prepare capture expired")
	}
	root, err := canonicalVectorPartitionStorageRootV1(state.db.Dir())
	if err != nil || root != state.root {
		return errors.New("collections: vector prepare capture root changed")
	}
	return WithVectorPartitionStorageBarrierWithContextV1(ctx, state.root, func() error {
		ownerState := &vectorPrepareStorageOwnerStateV1{db: state.db, root: state.root, pin: state.pin, active: true}
		owner := &VectorPrepareStorageOwnerV1{state: ownerState}
		defer func() {
			ownerState.mu.Lock()
			ownerState.active = false
			ownerState.pin = nil
			ownerState.mu.Unlock()
		}()
		return fn(owner)
	})
}

// ValidateDBV1 checks identity without exposing the snapshot. The collection
// executor additionally holds the owner mutex throughout each borrowed use.
func (owner *VectorPrepareStorageOwnerV1) ValidateDBV1(db *backenddb.DB) error {
	if owner == nil || owner.state == nil {
		return errors.New("collections: vector prepare storage owner unavailable")
	}
	state := owner.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.validateDBV1(db)
}

func (state *vectorPrepareStorageOwnerStateV1) validateDBV1(db *backenddb.DB) error {
	if !state.active || state.pin == nil {
		return errors.New("collections: vector prepare storage owner expired")
	}
	if db == nil || db != state.db {
		return errors.New("collections: vector prepare storage owner DB mismatch")
	}
	root, err := canonicalVectorPartitionStorageRootV1(db.Dir())
	if err != nil || root != state.root {
		return errors.New("collections: vector prepare storage owner root mismatch")
	}
	return nil
}

func (owner *VectorPrepareStorageOwnerV1) withDBV1(db *backenddb.DB, fn func(*backenddb.Snapshot) error) error {
	if owner == nil || owner.state == nil {
		return errors.New("collections: vector prepare storage owner unavailable")
	}
	state := owner.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.validateDBV1(db); err != nil {
		return err
	}
	return fn(state.pin)
}
