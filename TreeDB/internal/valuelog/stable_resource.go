package valuelog

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication/osadapter"
)

// SetStableResourcePinRegistry injects the DB-scoped identity gate shared by
// every manager that can delete the same physical segment files.
func (m *Manager) SetStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	if m == nil || registry == nil {
		return fmt.Errorf("valuelog: nil stable resource pin registry")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stableResourcePins != nil && m.stableResourcePins != registry {
		return fmt.Errorf("valuelog: stable resource pin registry already installed")
	}
	if m.stableResourcePins == registry {
		return nil
	}
	observed := make([]*File, 0, len(m.files))
	for _, f := range m.files {
		if err := m.observeStableFileWithRegistryLocked(f, registry); err != nil {
			for _, prior := range observed {
				_ = registry.Unobserve(prior.stableIdentity)
				prior.stableObserved = false
			}
			return err
		}
		observed = append(observed, f)
	}
	m.stableResourcePins = registry
	return nil
}

func (m *Manager) observeStableFileLocked(f *File) error {
	if m == nil || m.stableResourcePins == nil || f == nil || f.stableObserved {
		return nil
	}
	return m.observeStableFileWithRegistryLocked(f, m.stableResourcePins)
}

func (m *Manager) observeStableFileWithRegistryLocked(f *File, registry *rootpublication.IdentityPinRegistry) error {
	if f == nil || registry == nil || f.stableObserved {
		return nil
	}
	identity, err := m.stableIdentityForFile(f)
	if errors.Is(err, osadapter.ErrUnsupportedPlatform) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := registry.Observe(identity); err != nil {
		return err
	}
	f.stableObserved = true
	return nil
}

func (m *Manager) unobserveStableFileLocked(f *File) error {
	if m == nil || m.stableResourcePins == nil || f == nil || !f.stableObserved {
		return nil
	}
	f.stableObserved = false
	return m.stableResourcePins.Unobserve(f.stableIdentity)
}

// RegisterStableResourceToken binds a token to the manager's already-open
// segment handle. The shared registry is pinned before path validation so a
// second manager cannot begin deletion during registration.
func (m *Manager) RegisterStableResourceToken(
	id uint32,
	flushThrough func(file *os.File, frontier uint64) error,
	spec rootpublication.StableResourceSpec,
) (*rootpublication.StableResourceToken, error) {
	if m == nil || flushThrough == nil {
		return nil, fmt.Errorf("valuelog: stable resource registration unavailable")
	}
	m.mu.RLock()
	f := m.files[id]
	registry := m.stableResourcePins
	if f == nil || f.IsZombie.Load() {
		m.mu.RUnlock()
		return nil, &fileNotFoundError{id: id}
	}
	if registry == nil {
		m.mu.RUnlock()
		return nil, fmt.Errorf("valuelog: stable resource pin registry not installed")
	}
	// Keep this manager's descriptor alive until the shared identity pin has
	// transferred into the returned token.
	f.RefCount.Add(1)
	m.mu.RUnlock()
	defer m.releaseStableRegistrationRef(f)

	identity, err := osadapter.ResourceIdentity(f.File)
	if err != nil {
		return nil, fmt.Errorf("valuelog: inspect stable segment %d: %w", id, err)
	}
	if err := registry.Pin(identity); err != nil {
		return nil, fmt.Errorf("valuelog: reserve stable segment %d: %w", id, err)
	}
	m.mu.Lock()
	if cur := m.files[id]; cur == f {
		f.stableIdentity = identity
		f.stableIdentitySet = true
	}
	m.mu.Unlock()

	var reservationMu sync.Mutex
	reserved := true
	releaseReservation := func() error {
		reservationMu.Lock()
		defer reservationMu.Unlock()
		if !reserved {
			return nil
		}
		reserved = false
		return registry.Release(identity)
	}
	defer func() { _ = releaseReservation() }()

	// A completed delete followed by stale-manager registration would otherwise
	// pin an unlinked inode. Validate the diagnostic namespace once while the
	// shared pin excludes every normal deleter. Durable operations never reopen
	// this path.
	openInfo, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	pathInfo, err := os.Stat(f.Path)
	if err != nil || !os.SameFile(openInfo, pathInfo) {
		if err == nil {
			err = rootpublication.ErrResourceConflict
		}
		return nil, fmt.Errorf("valuelog: segment %d namespace no longer names captured identity: %w", id, err)
	}

	hooks := osadapter.ResourceHooks{
		FlushThrough: flushThrough,
		Pin: func(got rootpublication.StableIdentity) error {
			reservationMu.Lock()
			defer reservationMu.Unlock()
			if got != identity || !reserved {
				return rootpublication.ErrResourceConflict
			}
			// Transfer the already-counted reservation into token ownership.
			reserved = false
			return nil
		},
		Release: func(got rootpublication.StableIdentity) error {
			if got != identity {
				return rootpublication.ErrResourceConflict
			}
			return registry.Release(identity)
		},
	}
	token, err := osadapter.RegisterResourceToken(f.File, hooks, spec)
	if err != nil {
		return nil, err
	}
	return token, nil
}

func (m *Manager) releaseStableRegistrationRef(f *File) {
	if m == nil || f == nil {
		return
	}
	if f.RefCount.Add(-1) == 0 && f.IsZombie.Load() {
		m.scheduleZombieDelete(f)
	}
}

func (m *Manager) stableDeleteLease(f *File) (*rootpublication.IdentityDeleteLease, error) {
	if m == nil || f == nil || m.stableResourcePins == nil {
		return nil, nil
	}
	identity, err := m.stableIdentityForFile(f)
	if err != nil {
		if errors.Is(err, osadapter.ErrUnsupportedPlatform) {
			// No stable OS token can exist on this platform.
			return nil, nil
		}
		return nil, err
	}
	lease, err := m.stableResourcePins.BeginDeleteAt(identity, f.Path)
	if errors.Is(err, rootpublication.ErrResourcePinned) {
		return nil, &filePinnedError{id: f.ID, op: "delete stable resource"}
	}
	return lease, err
}

// validateStableDeletePath proves that the diagnostic namespace still names
// the open identity covered by lease. The shared registry serializes every
// process-local deleter of this path from BeginDeleteAt through unlink. An
// external process can still replace the pathname after validation; callers
// therefore never treat a detected mismatch as permission to unlink.
func validateStableDeletePath(f *File) error {
	if f == nil || f.File == nil || f.Path == "" {
		return fmt.Errorf("%w: invalid value-log delete target", rootpublication.ErrInvalidStableResource)
	}
	openInfo, err := f.File.Stat()
	if err != nil {
		return err
	}
	pathInfo, err := os.Stat(f.Path)
	if err != nil {
		return err
	}
	if !os.SameFile(openInfo, pathInfo) {
		return fmt.Errorf("%w: value-log path no longer names captured identity", rootpublication.ErrResourceConflict)
	}
	return nil
}

func (m *Manager) stableIdentityForFile(f *File) (rootpublication.StableIdentity, error) {
	if f == nil {
		return rootpublication.StableIdentity{}, osadapter.ErrInvalidOpenHandle
	}
	if f.stableIdentitySet {
		return f.stableIdentity, nil
	}
	identity, err := osadapter.ResourceIdentity(f.File)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	// Callers either hold m.mu or keep f alive through a local ref. Repeating a
	// benign assignment of the same immutable identity is safe.
	f.stableIdentity = identity
	f.stableIdentitySet = true
	return identity, nil
}

func (m *Manager) stableWaitUnpinned(f *File) <-chan struct{} {
	ready := make(chan struct{})
	if m == nil || f == nil || m.stableResourcePins == nil {
		close(ready)
		return ready
	}
	identity, err := m.stableIdentityForFile(f)
	if err != nil {
		close(ready)
		return ready
	}
	return m.stableResourcePins.WaitUnpinned(identity)
}

func (m *Manager) deleteZombieFile(f *File) error {
	if m == nil || f == nil {
		return nil
	}
	m.mu.Lock()
	cur, exists := m.files[f.ID]
	if !exists || cur != f || f.RefCount.Load() != 0 || !f.IsZombie.Load() {
		m.mu.Unlock()
		return nil
	}
	lease, err := m.stableDeleteLease(f)
	if errors.Is(err, ErrFilePinned) {
		m.mu.Unlock()
		m.scheduleZombieDelete(f)
		return nil
	}
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if err := validateStableDeletePath(f); err != nil {
		abortStableDeleteLease(lease)
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	unlinkErr := removeSegmentFileWithRetry(f.Path)
	if unlinkErr != nil {
		abortStableDeleteLease(lease)
		if isWindowsSharingViolationError(unlinkErr) {
			m.scheduleZombieDelete(f)
			return nil
		}
		return unlinkErr
	}
	commitStableDeleteLease(lease)
	closeErr := f.Close()
	m.mu.Lock()
	if cur, exists := m.files[f.ID]; exists && cur == f && f.RefCount.Load() == 0 && f.IsZombie.Load() {
		delete(m.files, f.ID)
		_ = m.unobserveStableFileLocked(f)
	}
	m.mu.Unlock()
	return closeErr
}

func (m *Manager) scheduleZombieDelete(f *File) {
	if m == nil || f == nil || !f.retryDeletePending.CompareAndSwap(false, true) {
		return
	}
	go m.retryZombieDelete(f)
}

func abortStableDeleteLease(lease *rootpublication.IdentityDeleteLease) {
	if lease != nil {
		lease.Abort()
	}
}

func commitStableDeleteLease(lease *rootpublication.IdentityDeleteLease) {
	if lease != nil {
		lease.CommitDeleted()
	}
}
