package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// StableSegmentTerminalPlan belongs only to the synchronous concrete consumer.
// End closes it before reporting/ACK and before notification. It is never
// installed in any registrar cell, token, set, coordinator or retry worker.
type StableSegmentTerminalPlan struct {
	manager             *Manager
	account             rootpublication.StableMetadataAccount
	entries             []stableSegmentTerminalEntry
	snapshotSet         *Set // transient actual caller-owned Set; never retained by a resource
	snapshotSetReleased bool
	closed              bool
	storage             *StableSegmentTerminalStorage
}
type stableSegmentTerminalEntry struct {
	hold          *finiteSegmentRetention
	file          *File
	lease         *rootpublication.IdentityDeleteLease
	preparedLease rootpublication.StableTerminalDeleteLease
	last          bool
	done          bool
	admitted      bool
}

func (m *Manager) PrepareStableSegmentTerminalRelease(groups []rootpublication.StableSegmentTerminalGroup) (*StableSegmentTerminalPlan, error) {
	return m.prepareStableSegmentTerminalRelease(groups, nil)
}

// PrepareStableSegmentTerminalReleaseWithSnapshotSet includes the one actual
// File edge owned by this Set, independently of its number of Set aliases.
// The caller keeps both the Set reference and every group hold until the plan
// has released them. Set and Manager are operational call-stack bindings only.
func (m *Manager) PrepareStableSegmentTerminalReleaseWithSnapshotSet(groups []rootpublication.StableSegmentTerminalGroup, set *Set) (*StableSegmentTerminalPlan, error) {
	if set == nil {
		return nil, rootpublication.ErrStableTerminalConsumerMismatch
	}
	return m.prepareStableSegmentTerminalRelease(groups, set)
}

func (m *Manager) prepareStableSegmentTerminalRelease(groups []rootpublication.StableSegmentTerminalGroup, set *Set) (*StableSegmentTerminalPlan, error) {
	return m.prepareStableSegmentTerminalReleaseWithStorage(groups, set, nil)
}

func (m *Manager) prepareStableSegmentTerminalReleaseWithStorage(groups []rootpublication.StableSegmentTerminalGroup, set *Set, storage *StableSegmentTerminalStorage) (*StableSegmentTerminalPlan, error) {
	if m == nil || len(groups) == 0 {
		return nil, rootpublication.ErrStableTerminalConsumerMismatch
	}
	// Hold locks precede Manager.mu everywhere. Address ordering is only lock
	// order over actually retained objects, never an ownership/identity key.
	for i := range groups {
		h, ok := groups[i].Retention.(*finiteSegmentRetention)
		if !ok || h == nil {
			return nil, rootpublication.ErrStableTerminalConsumerMismatch
		}
		for j := 0; j < i; j++ {
			if groups[j].Retention == h {
				return nil, rootpublication.ErrResourceOwnership
			}
		}
	}
	var prior uintptr
	for range groups {
		var next *finiteSegmentRetention
		for i := range groups {
			h := groups[i].Retention.(*finiteSegmentRetention)
			address := uintptr(unsafe.Pointer(h))
			if address > prior && (next == nil || address < uintptr(unsafe.Pointer(next))) {
				next = h
			}
		}
		next.mu.Lock()
		prior = uintptr(unsafe.Pointer(next))
	}
	defer func() {
		for i := range groups {
			groups[i].Retention.(*finiteSegmentRetention).mu.Unlock()
		}
	}()
	m.mu.Lock()
	defer m.mu.Unlock()
	var account rootpublication.StableMetadataAccount
	totalPins := 0
	// Complete shape/ref/identity preflight occurs before account, allocation or
	// deletion-admission effects. All actual holds must appear exactly once.
	for i := range groups {
		h, ok := groups[i].Retention.(*finiteSegmentRetention)
		if !ok || h == nil {
			return nil, rootpublication.ErrStableTerminalConsumerMismatch
		}
		for j := 0; j < i; j++ {
			if groups[j].Retention == h {
				return nil, rootpublication.ErrResourceOwnership
			}
		}
		_, err := m.validateFiniteRetentionLocked(h)
		if h.released {
			err = rootpublication.ErrResourceOwnership
		}
		if account == nil {
			account = h.metadata
		}
		if err != nil {
			return nil, err
		}
		if len(groups[i].OwnedPins) > int(^uint(0)>>1)-totalPins {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		totalPins += len(groups[i].OwnedPins)
	}
	// Every immutable Set file must have a real extra group hold. This makes
	// even the last Set decrement nonterminal for each File. Ordinary alias
	// releases can race the Set CAS but cannot bypass these actual holds.
	if set != nil {
		if !set.retentionKnown || set.RefCount.Load() <= 0 {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		for id, f := range set.Files {
			if f == nil || f.ID != id || f.RefCount.cell == nil {
				return nil, rootpublication.ErrStableTerminalConsumerMismatch
			}
			found := false
			for i := range groups {
				h := groups[i].Retention.(*finiteSegmentRetention)
				if h.cell == f.RefCount.cell {
					found = true
					break
				}
			}
			if !found {
				return nil, rootpublication.ErrStableTerminalConsumerMismatch
			}
		}
	}
	var p *StableSegmentTerminalPlan
	var pins []*rootpublication.IdentityPin
	if storage != nil {
		if storage.closed || storage.inUse || len(groups) > len(storage.entries) || totalPins > cap(storage.pins) {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		account = storage.account
		if err := account.RetainStableMetadata(); err != nil {
			return nil, err
		}
		storage.inUse = true
		storage.plan = StableSegmentTerminalPlan{manager: m, account: account, snapshotSet: set, entries: storage.entries[:len(groups)], storage: storage}
		p = &storage.plan
		pins = storage.pins[:0]
	} else {
		n, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(StableSegmentTerminalPlan{})), true)
		if err != nil {
			return nil, err
		}
		entriesBytes, err := rootpublication.StableBackingClassBytes(uint64(len(groups))*uint64(unsafe.Sizeof(stableSegmentTerminalEntry{})), true)
		if err != nil {
			return nil, err
		}
		pinsBytes, err := rootpublication.StableBackingClassBytes(uint64(totalPins)*uint64(unsafe.Sizeof((*rootpublication.IdentityPin)(nil))), true)
		if err != nil || ^uint64(0)-n < entriesBytes || ^uint64(0)-n-entriesBytes < pinsBytes {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		if err = account.ReserveStableMetadata(n + entriesBytes + pinsBytes); err != nil {
			return nil, err
		}
		if err = account.RetainStableMetadata(); err != nil {
			return nil, err
		}
		p = &StableSegmentTerminalPlan{manager: m, account: account, snapshotSet: set, entries: make([]stableSegmentTerminalEntry, len(groups))}
		pins = make([]*rootpublication.IdentityPin, 0, totalPins)
	}
	abort := func(err error) (*StableSegmentTerminalPlan, error) {
		for i := range p.entries {
			if p.entries[i].lease != nil {
				p.entries[i].lease.Abort()
			}
			p.entries[i].preparedLease.Abort()
		}
		clear(p.entries)
		if p.storage != nil {
			clear(p.storage.pins[:cap(p.storage.pins)])
			p.storage.inUse = false
			p.storage = nil
		}
		p.entries = nil
		p.snapshotSet = nil
		p.manager = nil
		p.account = nil
		p.closed = true
		account.ReleaseStableMetadata()
		return nil, err
	}
	for i := range groups {
		h := groups[i].Retention.(*finiteSegmentRetention)
		f, err := m.validateFiniteRetentionLocked(h)
		if err != nil {
			return abort(err)
		}
		p.entries[i] = stableSegmentTerminalEntry{hold: h, file: f}
		if f == nil || !f.IsZombie.Load() {
			continue
		}
		first := true
		refs := int64(0)
		for j := range groups {
			other := groups[j].Retention.(*finiteSegmentRetention)
			if other.cell == h.cell {
				refs++
				if j < i {
					first = false
				}
			}
		}
		if set != nil && set.Files[f.ID] == f {
			// A Set owns ONE File reference, not one per Set alias. If its
			// aliases drain before terminal release this edge must be covered.
			refs++
		}
		if h.cell.refs.Load() != refs || !first {
			continue
		}
		// MarkZombie already installed exact retirement identity/parent under the
		// Manager lock. No path-open/callback constructor is substituted here.
		if m.stableResourcePins == nil || !f.stableObserved || f.stableNamespace == "" || f.retirementIdentity == (rootpublication.StableIdentity{}) {
			return abort(rootpublication.ErrStableMetadataShapeUnsupported)
		}
		f.retirementParentMu.Lock()
		hasParent := f.retirementParent != nil
		f.retirementParentMu.Unlock()
		if !hasParent {
			return abort(rootpublication.ErrStableMetadataShapeUnsupported)
		}
		start := len(pins)
		for j := range groups {
			other := groups[j].Retention.(*finiteSegmentRetention)
			if other.cell == h.cell {
				pins = append(pins, groups[j].OwnedPins...)
			}
		}
		if storage != nil {
			if storage.reservation == nil || !storage.reservation.MatchesRegistry(m.stableResourcePins) {
				return abort(rootpublication.ErrStableMetadataShapeUnsupported)
			}
			lease, err := storage.reservation.PrepareTerminalDeleteAt(f.stableIdentity, f.stableNamespace, pins[start:len(pins)])
			if err != nil {
				return abort(err)
			}
			p.entries[i].preparedLease = lease
		} else {
			lease, err := m.stableResourcePins.PrepareTerminalDeleteAt(f.stableIdentity, f.stableNamespace, pins[start:len(pins)], account)
			if err != nil {
				return abort(err)
			}
			p.entries[i].lease = lease
		}
		p.entries[i].last = true
	}
	// Only after ALL registry reservations succeed does Manager admission change.
	for i := range p.entries {
		if p.entries[i].last {
			p.entries[i].file.deletionAdmissions++
			p.entries[i].admitted = true
		}
	}
	return p, nil
}

// ReleaseSnapshotSet consumes exactly the caller's existing Set edge.
// No pins, group holds, FD or account are changed here. Before the last Set CAS
// every File is checked, and extra actual group holds keep each decrement from
// reaching zero. Subsequent I/O failure remains recoverable on those holds.
func (p *StableSegmentTerminalPlan) ReleaseSnapshotSet() error {
	if p == nil || p.closed || p.snapshotSet == nil {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	if p.snapshotSetReleased {
		return nil
	}
	m := p.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	set := p.snapshotSet
	for {
		refs := set.RefCount.Load()
		if refs <= 0 {
			return rootpublication.ErrResourceOwnership
		}
		if refs > 1 {
			if set.RefCount.CompareAndSwap(refs, refs-1) {
				p.snapshotSetReleased = true
				return nil
			}
			continue
		}
		for id, f := range set.Files {
			if f == nil || f.ID != id || f.RefCount.cell == nil || f.RefCount.Load() < 2 {
				return rootpublication.ErrStableTerminalConsumerMismatch
			}
			found := false
			for i := range p.entries {
				if p.entries[i].hold.cell == f.RefCount.cell && !p.entries[i].done && !p.entries[i].hold.released {
					found = true
					break
				}
			}
			if !found {
				return rootpublication.ErrStableTerminalConsumerMismatch
			}
		}
		if !set.RefCount.CompareAndSwap(1, 0) {
			continue
		}
		for _, f := range set.Files {
			f.RefCount.Add(-1)
		}
		p.snapshotSetReleased = true
		return nil
	}
}

func (p *StableSegmentTerminalPlan) ValidateSegmentRetention(retention rootpublication.StableSegmentRetention) error {
	if p == nil || p.closed {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	for i := range p.entries {
		if p.entries[i].hold == retention {
			return nil
		}
	}
	return rootpublication.ErrStableTerminalConsumerMismatch
}
func (p *StableSegmentTerminalPlan) ReleaseSegmentRetention(retention rootpublication.StableSegmentRetention, syncer StableSegmentDeletionSync) error {
	if err := p.ValidateSegmentRetention(retention); err != nil {
		return err
	}
	var entry *stableSegmentTerminalEntry
	for i := range p.entries {
		if p.entries[i].hold == retention {
			entry = &p.entries[i]
			break
		}
	}
	if entry.done {
		return nil
	}
	if p.snapshotSet != nil && !p.snapshotSetReleased {
		return rootpublication.ErrStableTerminalConsumerRequired
	}
	h := entry.hold
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		entry.done = true
		return nil
	}
	m := p.manager
	m.mu.Lock()
	// Manager.Close may finish after preparation. The exact shared scalar cell
	// proves actual handle closure; no live Manager lookup is then necessary.
	if h.cell.closed.Load() && (h.cell.incarnation.closed.Load() || h.cell.terminalClosed.Load()) {
		h.finishLocked()
		entry.done = true
		m.mu.Unlock()
		return nil
	}
	f := entry.file
	if f == nil {
		if h.cell.closed.Load() && (h.cell.incarnation.closed.Load() || h.cell.terminalClosed.Load()) {
			h.finishLocked()
			entry.done = true
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	if m.files[h.cell.id] != f || f.RefCount.cell != h.cell {
		m.mu.Unlock()
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	var deletion *stableSegmentTerminalEntry
	for i := range p.entries {
		if p.entries[i].file == f && p.entries[i].last {
			deletion = &p.entries[i]
			break
		}
	}
	// Ordinary Set.Release can change the shared count without Manager.mu.
	// Only a successful non-last CAS (or a lock-protected non-zombie release)
	// drops this hold without IO. A stale preparation never authorizes dropping
	// an actual last zombie reference or its account.
	if h.releaseNonterminalReferenceLocked(f) {
		entry.done = true
		m.mu.Unlock()
		return nil
	}
	if deletion == nil {
		m.mu.Unlock()
		return errors.Join(rootpublication.ErrStableMetadataShapeUnsupported, rootpublication.ErrStableTerminalConsumerRequired)
	}
	if h.cell.refs.Load() != 1 {
		m.mu.Unlock()
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	if syncer == nil {
		m.mu.Unlock()
		return rootpublication.ErrStableTerminalConsumerRequired
	}
	var drainedErr error
	if deletion.preparedLease.Valid() {
		drainedErr = deletion.preparedLease.CheckDrained()
	} else {
		drainedErr = deletion.lease.CheckDrained()
	}
	if drainedErr != nil {
		m.mu.Unlock()
		return drainedErr
	}
	identity := f.retirementIdentity
	m.mu.Unlock()
	deleted, unlinkErr := closeAndRemoveStableSegmentFileResult(f, identity)
	if !deleted {
		// Keep this REAL hold/account/ref recoverable. No ordinary retry worker may
		// store the transient consumer; Close will abort gate/admission for reprepare.
		return unlinkErr
	}
	syncErr := syncer.SyncStableSegmentDeletion(f.Path, segmentNamespaceResource(f.Path))
	// A namespace sync/poison failure still retains the holder. A retry uses
	// File.deleteCompleted and repeats synchronous namespace persistence.
	if err := errors.Join(unlinkErr, syncErr); err != nil {
		return err
	}
	m.mu.Lock()
	// A registry bookkeeping refusal still keeps the actual File/hold/account.
	// Clear attribution only after the existing registry acknowledges Unobserve.
	if f.stableObserved {
		if err := m.stableResourcePins.Unobserve(f.stableIdentity); err != nil {
			m.mu.Unlock()
			return err
		}
		f.stableObserved = false
	}
	if deletion.preparedLease.Valid() {
		deletion.preparedLease.CommitDeleted()
		deletion.preparedLease = rootpublication.StableTerminalDeleteLease{}
	} else {
		deletion.lease.CommitDeleted()
		deletion.lease = nil
	}
	parent := m.forgetSegmentLocked(f)
	m.mu.Unlock()
	parentErr := closeRetirementParent(parent)
	// os.File.Close completes its real handle release even when it reports a
	// close error. Physical deletion and namespace persistence have succeeded.
	// Preserve the actual scalar holder/ref/account on that observable error;
	// its later cleanup drops metadata only, with no Manager or COW retry.
	h.cell.terminalClosed.Store(true)
	if parentErr != nil {
		return parentErr
	}
	h.finishLocked()
	entry.done = true
	return nil
}

// Close synchronously aborts every unfinished reservation and drops plan-only
// credit. Remaining holds retain their own exact cell/account for Stop/handoff.
func (p *StableSegmentTerminalPlan) Close() {
	if p == nil || p.closed {
		return
	}
	m := p.manager
	m.mu.Lock()
	for i := range p.entries {
		e := &p.entries[i]
		if e.lease != nil {
			e.lease.Abort()
			e.lease = nil
		}
		e.preparedLease.Abort()
		e.preparedLease = rootpublication.StableTerminalDeleteLease{}
		if e.admitted {
			e.file.deletionAdmissions--
			e.admitted = false
		}
	}
	m.mu.Unlock()
	clear(p.entries)
	if p.storage != nil {
		clear(p.storage.pins[:cap(p.storage.pins)])
		p.storage.inUse = false
		p.storage = nil
	}
	p.entries = nil
	account := p.account
	p.account = nil
	p.manager = nil
	p.snapshotSet = nil
	p.closed = true
	account.ReleaseStableMetadata()
}
