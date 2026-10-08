package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"sync"
	"unsafe"
)

// This concrete producer-only hold has no File, Manager or callback edge.
// Acquisition is private: generic retention APIs keep their ordinary semantics.
type finiteSegmentRetention struct {
	mu       sync.Mutex
	cell     *segmentRegistrationCell
	metadata rootpublication.StableMetadataAccount
	census   rootpublication.BackingCensus
	released bool
}

// StableSegmentDeletionSync is supplied transiently by the concrete DB terminal
// consumer. It must synchronize/poison without invoking user notification.
// Neither a hold nor an existing retry worker stores this interface.
type StableSegmentDeletionSync interface {
	SyncStableSegmentDeletion(string, durabilitycut.Resource) error
}

func (m *Manager) acquireFiniteSegmentRetention(id uint32, maximum uint64, account rootpublication.StableMetadataAccount) (*finiteSegmentRetention, error) {
	return m.acquireFiniteSegmentRetentionForIdentity(id, rootpublication.StableIdentity{}, maximum, account)
}

// AcquireStableSegmentRetentionForIdentity is the transient installed producer
// bridge. Its result retains only the actual scalar registration closure; no
// Manager, File, decoder, callback or producer is stored in it.
func (m *Manager) AcquireStableSegmentRetentionForIdentity(id uint32, identity rootpublication.StableIdentity, maximum uint64, account rootpublication.StableMetadataAccount) (rootpublication.StableSegmentRetention, rootpublication.BackingCensus, error) {
	if identity == (rootpublication.StableIdentity{}) {
		return nil, rootpublication.BackingCensus{}, rootpublication.ErrResourceConflict
	}
	h, err := m.acquireFiniteSegmentRetentionForIdentity(id, identity, maximum, account)
	if err != nil {
		return nil, rootpublication.BackingCensus{}, err
	}
	return h, h.census, nil
}

func (m *Manager) acquireFiniteSegmentRetentionForIdentity(id uint32, identity rootpublication.StableIdentity, maximum uint64, account rootpublication.StableMetadataAccount) (*finiteSegmentRetention, error) {
	if m == nil || account == nil || maximum == 0 {
		return nil, ErrFiniteWriterLoan
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.files[id]
	if m.closing || f == nil || f.IsZombie.Load() || f.deletionAdmissions != 0 || uint64(len(m.files)) > maximum {
		return nil, ErrFiniteWriterLoan
	}
	if identity != (rootpublication.StableIdentity{}) && !rootpublication.SamePhysicalIdentity(identity, f.stableIdentity) {
		return nil, rootpublication.ErrResourceConflict
	}
	cell := f.RefCount.cell
	if cell == nil || cell.closed.Load() || cell.classBytes == 0 || cell.id != id || cell.generation == 0 || cell.incarnation == nil || cell.incarnation != m.registrationIncarnation || cell.incarnation.classBytes == 0 {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	n, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(finiteSegmentRetention{})), true)
	if err != nil {
		return nil, err
	}
	if cell.classBytes > ^uint64(0)-n || cell.incarnation.classBytes > ^uint64(0)-n-cell.classBytes {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	bytes := n + cell.classBytes + cell.incarnation.classBytes
	if err = account.ReserveStableMetadata(bytes); err != nil {
		return nil, err
	}
	if err = account.RetainStableMetadata(); err != nil {
		return nil, err
	}
	hold := &finiteSegmentRetention{cell: cell, metadata: account, census: rootpublication.BackingCensus{LiveClassBytes: bytes, AllocatedClassBytes: bytes, LiveAllocations: 3, Allocations: 3}}
	cell.refs.Add(1)
	return hold, nil
}

func (h *finiteSegmentRetention) RetainedBackingCensus() (rootpublication.BackingCensus, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return rootpublication.BackingCensus{}, rootpublication.ErrResourceOwnership
	}
	return h.census, nil
}
func (h *finiteSegmentRetention) ValidateTerminalRelease(consumer rootpublication.StableSegmentTerminalConsumer) error {
	h.mu.Lock()
	if h.released {
		h.mu.Unlock()
		return nil
	}
	closed := h.cell.closed.Load() && (h.cell.incarnation.closed.Load() || h.cell.terminalClosed.Load())
	h.mu.Unlock()
	if consumer != nil {
		return consumer.ValidateSegmentRetention(h)
	}
	if closed {
		return nil
	}
	return rootpublication.ErrStableTerminalConsumerRequired
}
func (h *finiteSegmentRetention) Release() error { return h.ReleaseWithTerminal(nil) }
func (h *finiteSegmentRetention) ReleaseWithTerminal(consumer rootpublication.StableSegmentTerminalConsumer) error {
	if consumer != nil {
		return consumer.ReleaseSegmentRetention(h)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return nil
	}
	if !h.cell.closed.Load() || (!h.cell.incarnation.closed.Load() && !h.cell.terminalClosed.Load()) {
		return rootpublication.ErrStableTerminalConsumerRequired
	}
	h.finishLocked()
	return nil
}
func (h *finiteSegmentRetention) finishLocked() {
	h.cell.refs.Add(-1)
	h.finishMetadataLocked()
}

// The actual shared reference has already been decremented. Never debit it a
// second time while dropping the hold and its retained account.
func (h *finiteSegmentRetention) finishMetadataLocked() {
	h.released = true
	h.cell = nil
	account := h.metadata
	h.metadata = nil
	h.census = rootpublication.BackingCensus{}
	account.ReleaseStableMetadata()
}

// Begin/End share the actual Close join. A completed closed incarnation needs
// no live join; an in-progress Close is an observable refusal, never authority.
func (m *Manager) BeginTerminalRelease() (bool, error) {
	if m == nil {
		return false, rootpublication.ErrStableTerminalConsumerRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registrationIncarnation == nil {
		return false, rootpublication.ErrStableTerminalConsumerMismatch
	}
	if m.registrationIncarnation.closed.Load() {
		return false, nil
	}
	if m.closing {
		return false, ErrFiniteWriterLoan
	}
	m.retryWorkers.Add(1)
	return true, nil
}
func (m *Manager) EndTerminalRelease(joined bool) {
	if joined {
		m.retryWorkers.Done()
	}
}
func (m *Manager) validateFiniteRetentionLocked(h *finiteSegmentRetention) (*File, error) {
	cell := h.cell
	if h.released {
		return nil, nil
	}
	if cell == nil || cell.incarnation != m.registrationIncarnation || cell.generation == 0 {
		return nil, rootpublication.ErrStableTerminalConsumerMismatch
	}
	if cell.closed.Load() && (cell.incarnation.closed.Load() || cell.terminalClosed.Load()) {
		return nil, nil
	}
	f := m.files[cell.id]
	if f == nil || f.RefCount.cell != cell || cell.refs.Load() <= 0 || f.deletionAdmissions != 0 {
		return nil, rootpublication.ErrStableTerminalConsumerMismatch
	}
	return f, nil
}
func (m *Manager) ValidateStableSegmentRetention(retention rootpublication.StableSegmentRetention) error {
	h, ok := retention.(*finiteSegmentRetention)
	if !ok || m == nil {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.validateFiniteRetentionLocked(h)
	return err
}

// Last-zombie deletion requires the selected checked deletion plan, which is
// prepared for the entire terminal group before pins/ownership are changed.
// Until that plan is attached, this branch refuses BEFORE decrementing the
// registration or releasing its actual account. Non-last and non-zombie holds
// use the same ordinary count and completed Manager-close cells discharge.
func (m *Manager) ReleaseStableSegmentRetention(retention rootpublication.StableSegmentRetention, syncer StableSegmentDeletionSync) error {
	h, ok := retention.(*finiteSegmentRetention)
	if !ok || m == nil {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, err := m.validateFiniteRetentionLocked(h)
	if err != nil {
		return err
	}
	if f == nil {
		h.finishLocked()
		return nil
	}
	if !h.releaseNonterminalReferenceLocked(f) {
		return errors.Join(rootpublication.ErrStableMetadataShapeUnsupported, rootpublication.ErrStableTerminalConsumerRequired)
	}
	return nil
}

// Both h.mu and Manager.mu are held. The Manager lock excludes MarkZombie and
// new ordinary snapshots; ordinary Set.Release still decrements this SAME cell
// atomically without that lock. For zombies, CAS proves this decrement cannot
// drop the final reference even if an ordinary release races it. A last zombie
// remains actually held until a prepared deletion lease completes.
func (h *finiteSegmentRetention) releaseNonterminalReferenceLocked(f *File) bool {
	if !f.IsZombie.Load() {
		h.cell.refs.Add(-1)
		h.finishMetadataLocked()
		return true
	}
	for {
		refs := h.cell.refs.Load()
		if refs <= 1 {
			return false
		}
		if h.cell.refs.CompareAndSwap(refs, refs-1) {
			h.finishMetadataLocked()
			return true
		}
	}
}
