package valuelog

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// SnapshotSetTerminalRetentions is the actual Snapshot-owned terminal control.
// Its persistent closure contains only scalar registration holds and credit;
// Manager, Set, File, deletion sync and callbacks are supplied on the stack.
// The control creator remains retained until every actual hold has drained.
type SnapshotSetTerminalRetentions struct {
	mu          sync.Mutex
	holds       []*finiteSegmentRetention
	account     rootpublication.StableMetadataAccount
	backing     uint64
	setReleased bool
	closed      bool
}

// AcquireSnapshotSetTerminalRetentions protects every exact File already
// reached by an immutable caller-owned Set. The whole shape and all allocation
// classes are admitted before allocation, reference or observer effects.
func (m *Manager) AcquireSnapshotSetTerminalRetentions(set *Set, maximum uint64, account rootpublication.StableMetadataAccount) (*SnapshotSetTerminalRetentions, error) {
	if m == nil || set == nil || account == nil || maximum == 0 || !reflect.TypeOf(account).Comparable() {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || !set.retentionKnown || set.RefCount.Load() <= 0 || uint64(len(set.Files)) > maximum || uint64(len(m.files)) > maximum {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	controlClass, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(SnapshotSetTerminalRetentions{})), true)
	if err != nil {
		return nil, err
	}
	arrayClass, err := rootpublication.StableBackingClassBytes(uint64(len(set.Files))*uint64(unsafe.Sizeof((*finiteSegmentRetention)(nil))), true)
	if err != nil || ^uint64(0)-controlClass < arrayClass {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	holdClass, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(finiteSegmentRetention{})), true)
	if err != nil {
		return nil, err
	}
	total := controlClass + arrayClass
	for id, f := range set.Files {
		if f == nil || f.ID != id || m.files[id] != f || f.deletionAdmissions != 0 || f.closed.Load() || f.RefCount.Load() <= 0 {
			return nil, rootpublication.ErrStableTerminalConsumerMismatch
		}
		cell := f.RefCount.cell
		if cell == nil || cell.id != id || cell.generation == 0 || cell.closed.Load() || cell.incarnation != m.registrationIncarnation || cell.classBytes == 0 || cell.incarnation == nil || cell.incarnation.classBytes == 0 {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		if ^uint64(0)-holdClass < cell.classBytes || ^uint64(0)-holdClass-cell.classBytes < cell.incarnation.classBytes {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		n := holdClass + cell.classBytes + cell.incarnation.classBytes
		if ^uint64(0)-total < n {
			return nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		total += n
	}
	if err := account.ReserveStableMetadata(total); err != nil {
		return nil, err
	}
	// Retains are prepared as one complete closure. A failed retain never
	// allocates a control or changes the real Set/File references.
	retains := 0
	for i := 0; i <= len(set.Files); i++ {
		if err := account.RetainStableMetadata(); err != nil {
			for retains > 0 {
				account.ReleaseStableMetadata()
				retains--
			}
			return nil, err
		}
		retains++
	}
	control := &SnapshotSetTerminalRetentions{holds: make([]*finiteSegmentRetention, 0, len(set.Files)), account: account, backing: total}
	for _, f := range set.Files {
		cell := f.RefCount.cell
		n := holdClass + cell.classBytes + cell.incarnation.classBytes
		h := &finiteSegmentRetention{cell: cell, metadata: account, census: rootpublication.BackingCensus{LiveClassBytes: n, AllocatedClassBytes: n, LiveAllocations: 3, Allocations: 3}}
		cell.refs.Add(1)
		control.holds = append(control.holds, h)
	}
	return control, nil
}

func (c *SnapshotSetTerminalRetentions) TerminalAccount() rootpublication.StableMetadataAccount {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.account
}

func (c *SnapshotSetTerminalRetentions) BackingBytes() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.backing
}
func (c *SnapshotSetTerminalRetentions) RequireAccount(account rootpublication.StableMetadataAccount) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || account == nil || !reflect.TypeOf(account).Comparable() || c.account != account {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	return nil
}

// Count and Registration expose only exact scalar identity. The caller may
// build its prepaid transient terminal group without another file registry.
func (c *SnapshotSetTerminalRetentions) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0
	}
	return len(c.holds)
}
func (c *SnapshotSetTerminalRetentions) Registration(index int) (uint32, rootpublication.StableSegmentRetention, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || index < 0 || index >= len(c.holds) {
		return 0, nil, rootpublication.ErrResourceOwnership
	}
	h := c.holds[index]
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released || h.cell == nil {
		return 0, nil, rootpublication.ErrResourceOwnership
	}
	return h.cell.id, h, nil
}

// TerminalRegistration distinguishes a completed actual hold explicitly;
// nil never authorizes a missing/unknown registration.
func (c *SnapshotSetTerminalRetentions) TerminalRegistration(index int) (uint32, rootpublication.StableSegmentRetention, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || index < 0 || index >= len(c.holds) {
		return 0, nil, false, rootpublication.ErrResourceOwnership
	}
	h := c.holds[index]
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return 0, nil, true, nil
	}
	if h.cell == nil {
		return 0, nil, false, rootpublication.ErrStableTerminalConsumerMismatch
	}
	return h.cell.id, h, false, nil
}

// SetReleased is the exact cleanup phase, not a reference-count inference.
// It survives a failed plan so a retry cannot consume the Set twice.
func (c *SnapshotSetTerminalRetentions) SetReleased() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setReleased
}

func (c *SnapshotSetTerminalRetentions) ReleaseSnapshotSet(plan *StableSegmentTerminalPlan) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || plan == nil || plan.closed {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	if c.setReleased {
		return nil
	}
	// Bind this exact control closure to the transient plan before any effect.
	for _, h := range c.holds {
		found := false
		for i := range plan.entries {
			if plan.entries[i].hold == h {
				found = true
				break
			}
		}
		if !found {
			return rootpublication.ErrStableTerminalConsumerMismatch
		}
	}
	if err := plan.ReleaseSnapshotSet(); err != nil {
		return err
	}
	c.setReleased = true
	return nil
}

// ValidateDrained proves the remaining metadata close is nonfallible without
// retiring creator credit before the actual Snapshot/index pins are finalized.
func (c *SnapshotSetTerminalRetentions) ValidateDrained() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	for _, h := range c.holds {
		h.mu.Lock()
		released := h.released
		h.mu.Unlock()
		if !released {
			return rootpublication.ErrStableTerminalConsumerRequired
		}
	}
	return nil
}

// Close is checked and never drops unfinished holds. Its caller owns the
// actual cleanup reservation; no destructor, worker or finalizer substitutes
// for successful release through the existing terminal plan.
func (c *SnapshotSetTerminalRetentions) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	for _, h := range c.holds {
		h.mu.Lock()
		released := h.released
		h.mu.Unlock()
		if !released {
			return rootpublication.ErrStableTerminalConsumerRequired
		}
	}
	clear(c.holds)
	c.holds = nil
	account := c.account
	c.account = nil
	c.closed = true
	account.ReleaseStableMetadata()
	return nil
}
