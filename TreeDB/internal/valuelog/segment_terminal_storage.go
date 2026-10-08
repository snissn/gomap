package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// StableSegmentTerminalStorage holds actual pre-WAL plan backing for the one
// serial runtime reporter. An idle storage has no Manager, File, Set, pin or
// lease edge. Only the resident creator survives constructor admission.
// Registry deletion gates and IO/error births require their own admission;
// this array certificate does not grant finite publication authority.
type StableSegmentTerminalStorage struct {
	plan          StableSegmentTerminalPlan
	reservation   *rootpublication.StableTerminalDeleteReservation
	entries       []stableSegmentTerminalEntry
	pins          []*rootpublication.IdentityPin
	account       rootpublication.StableMetadataAccount
	bytes         uint64
	inUse, closed bool
}

// StableSegmentTerminalStorageClassBytes plans the actual inline control and
// both full-capacity arrays without credit effects or constructor births.
func StableSegmentTerminalStorageClassBytes(groups, pins int) (uint64, error) {
	if groups <= 0 || pins < 0 || groups > int(^uint(0)>>1)/int(unsafe.Sizeof(stableSegmentTerminalEntry{})) || pins > int(^uint(0)>>1)/int(unsafe.Sizeof((*rootpublication.IdentityPin)(nil))) {
		return 0, rootpublication.ErrStableMetadataShapeUnsupported
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(StableSegmentTerminalStorage{})), true)
	if err != nil {
		return 0, err
	}
	entries, err := rootpublication.StableBackingClassBytes(uint64(groups)*uint64(unsafe.Sizeof(stableSegmentTerminalEntry{})), true)
	if err != nil {
		return 0, err
	}
	pinBytes, err := rootpublication.StableBackingClassBytes(uint64(pins)*uint64(unsafe.Sizeof((*rootpublication.IdentityPin)(nil))), true)
	if err != nil || entries > ^uint64(0)-control || pinBytes > ^uint64(0)-control-entries {
		return 0, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return control + entries + pinBytes, nil
}

func NewStableSegmentTerminalStorage(groups, pins int, request, resident rootpublication.StableMetadataAccount) (*StableSegmentTerminalStorage, error) {
	if request == nil || resident == nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	n, err := StableSegmentTerminalStorageClassBytes(groups, pins)
	if err != nil {
		return nil, err
	}
	if err = request.ReserveStableMetadata(n); err != nil {
		return nil, err
	}
	if err = resident.ReserveStableMetadata(n); err != nil {
		return nil, err
	}
	if err = resident.RetainStableMetadata(); err != nil {
		return nil, err
	}
	return &StableSegmentTerminalStorage{entries: make([]stableSegmentTerminalEntry, groups), pins: make([]*rootpublication.IdentityPin, 0, pins), account: resident, bytes: n}, nil
}

// NewStableSegmentTerminalStorageWithReservation takes custody only on
// success. This is the concrete pre-WAL constructor; an ordinary array-only
// storage can never silently construct a finite deletion gate after storage.
func NewStableSegmentTerminalStorageWithReservation(groups, pins int, reservation *rootpublication.StableTerminalDeleteReservation, request, resident rootpublication.StableMetadataAccount) (*StableSegmentTerminalStorage, error) {
	if reservation == nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	s, err := NewStableSegmentTerminalStorage(groups, pins, request, resident)
	if err != nil {
		return nil, err
	}
	s.reservation = reservation
	return s, nil
}

// NewStableTerminalDeleteReservation validates actual installed registrar
// bindings under Manager.mu before reaching the existing registry lock. No
// pathname discovery, ref increment, FD birth or Manager edge is retained.
func (m *Manager) NewStableTerminalDeleteReservation(bindings []rootpublication.StableTerminalDeleteBinding, request, resident rootpublication.StableMetadataAccount) (*rootpublication.StableTerminalDeleteReservation, error) {
	if m == nil || len(bindings) == 0 {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.stableResourcePins == nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	for _, binding := range bindings {
		found := false
		for _, f := range m.files {
			if f != nil && f.RefCount.Load() > 0 && f.stableObserved && f.stableNamespace == binding.Namespace && rootpublication.SamePhysicalIdentity(f.stableIdentity, binding.Identity) {
				found = true
				break
			}
		}
		if !found {
			return nil, rootpublication.ErrStableTerminalConsumerMismatch
		}
	}
	return m.stableResourcePins.NewStableTerminalDeleteReservation(bindings, request, resident)
}

func (s *StableSegmentTerminalStorage) RetainRegistryLoan(request, resident rootpublication.StableMetadataAccount) (*rootpublication.StableTerminalRegistryLoan, error) {
	if s == nil || s.closed || s.reservation == nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return s.reservation.RetainRegistryLoan(request, resident)
}

// ValidateStableTerminalDeleteBindings must run under the producer/command
// serializers before storage. It certifies exact installed registrar bindings;
// it grants no future rotation or unregistered physical-owner exemption.
func (m *Manager) ValidateStableTerminalDeleteBindings(storage *StableSegmentTerminalStorage, bindings []rootpublication.StableTerminalDeleteBinding) error {
	if m == nil || storage == nil || storage.closed || storage.reservation == nil || len(bindings) == 0 {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || !storage.reservation.MatchesRegistry(m.stableResourcePins) {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	for _, binding := range bindings {
		found := false
		for _, f := range m.files {
			if f != nil && f.RefCount.Load() > 0 && f.stableObserved && f.stableNamespace == binding.Namespace && rootpublication.SamePhysicalIdentity(f.stableIdentity, binding.Identity) {
				found = true
				break
			}
		}
		if !found {
			return rootpublication.ErrStableTerminalConsumerMismatch
		}
	}
	return storage.reservation.RequireBindings(bindings)
}

func (s *StableSegmentTerminalStorage) ClassBytes() uint64 {
	if s == nil {
		return 0
	}
	return s.bytes
}
func (s *StableSegmentTerminalStorage) Close() error {
	if s == nil || s.closed {
		return nil
	}
	if s.inUse {
		return rootpublication.ErrResourceOwnership
	}
	if s.reservation != nil {
		if err := s.reservation.Close(); err != nil {
			return err
		}
		s.reservation = nil
	}
	clear(s.entries)
	clear(s.pins[:cap(s.pins)])
	s.entries = nil
	s.pins = nil
	s.plan = StableSegmentTerminalPlan{}
	a := s.account
	s.account = nil
	s.closed = true
	a.ReleaseStableMetadata()
	return nil
}

// The storage argument never supplies Manager authority. The exact consumer
// still binds the installed Manager and performs complete-group validation.
func (m *Manager) PrepareStableSegmentTerminalReleaseWithStorage(groups []rootpublication.StableSegmentTerminalGroup, storage *StableSegmentTerminalStorage) (*StableSegmentTerminalPlan, error) {
	if storage == nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return m.prepareStableSegmentTerminalReleaseWithStorage(groups, nil, storage)
}
