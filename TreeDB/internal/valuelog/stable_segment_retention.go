package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"sync"
	"unsafe"
)

// StableSegmentRetention is a concrete reference to the already registered
// File. Acquisition and membership checks share Manager.mu; eviction and zombie
// deletion both require File.RefCount==0. No snapshot map is constructed.
type StableSegmentRetention struct {
	manager  *Manager
	file     *File
	metadata rootpublication.StableMetadataAccount
	once     sync.Once
}

func (m *Manager) AcquireStableSegmentRetention(id uint32, maxRegisteredFiles uint64, metadata rootpublication.StableMetadataAccount) (*StableSegmentRetention, error) {
	if m == nil || maxRegisteredFiles == 0 {
		return nil, ErrFiniteWriterLoan
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.files[id]
	if m.closing || f == nil || f.IsZombie.Load() || f.deletionAdmissions != 0 || uint64(len(m.files)) > maxRegisteredFiles {
		return nil, ErrFiniteWriterLoan
	}
	if metadata != nil {
		n, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(StableSegmentRetention{})), true)
		if err != nil {
			return nil, err
		}
		if err = metadata.ReserveStableMetadata(n); err != nil {
			return nil, err
		}
		if err = metadata.RetainStableMetadata(); err != nil {
			return nil, err
		}
	}
	hold := &StableSegmentRetention{manager: m, file: f, metadata: metadata}
	f.RefCount.Add(1)
	return hold, nil
}

// The real registration lifetime is implemented, but File/Manager/cache/map
// backing does not yet have a complete constructor stamp and growth ledger.
// Returning wrapper sizeof as a full closure would silently waive those
// families. The selected finite collector therefore still refuses this source.
func (h *StableSegmentRetention) RetainedBackingCensus() (rootpublication.BackingCensus, error) {
	return rootpublication.BackingCensus{}, rootpublication.ErrStableMetadataShapeUnsupported
}
func (h *StableSegmentRetention) Release() error {
	if h == nil {
		return nil
	}
	h.once.Do(func() {
		m, f, a := h.manager, h.file, h.metadata
		h.manager = nil
		h.file = nil
		h.metadata = nil
		if f != nil && f.RefCount.Add(-1) == 0 && f.IsZombie.Load() {
			_ = m.deleteZombieFile(f)
		}
		if a != nil {
			a.ReleaseStableMetadata()
		}
	})
	return nil
}

// RecordStableSegmentFrontier runs only under the existing writer append
// serializer. It records a cached immutable exact owner; no generic token or
// diagnostic alias is exported and no per-append capture token is born.
func (w *Writer) RecordStableSegmentFrontier(collector *rootpublication.StableSegmentFrontierCollector, manager *Manager, maxRegisteredFiles uint64, metadata *FiniteStableMetadata, resident rootpublication.StableMetadataAccount, synced bool) error {
	if w == nil || collector == nil || manager == nil || metadata == nil || resident == nil {
		return ErrFiniteWriterLoan
	}
	if err := metadata.RequireRootPublicationHooks(); err != nil {
		return err
	}
	if w.stableSegmentOwner == nil || w.stableSegmentFileID != w.fileID || w.stableSegmentRegistration.Reachability != rootpublication.ReachabilityOuterLeafRawPointer || !w.stableResourceObserved || w.stableSegmentRegistration.PinRegistry != manager.StableResourcePinRegistry() {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	// The registrar bridge runs after source/registry admission and installs the
	// real hold atomically with respect to collector termination. Generic legacy
	// Manager/File retention remains unchanged.
	return collector.RecordFromRegistrar(w.stableSegmentOwner, rootpublication.DurableFrontier{Bytes: uint64(w.Size())}, synced, manager, w.fileID, maxRegisteredFiles)
}
