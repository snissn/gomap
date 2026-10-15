package valuelog

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"unsafe"
)

// SegmentRegistrationWork retains the original read-handle constructor until
// it can transfer the completed File into the SAME Manager. Callers retain it
// on refusal/error and close it before releasing their containing lane.
type SegmentRegistrationWork struct {
	manager       *Manager
	path          string
	id            uint32
	open          segmentOpenWork
	metadata      *retainedalloc.Owner
	charge        uint64
	phase         uint8
	identity      rootpublication.StableIdentity
	identityReady bool
	namespace     string
	err           error
	cleanupErr    error
}

func (m *Manager) RegisterSegmentWithWorkV6(path string, id uint32, s *SegmentRegistrationWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil {
		return false, errors.New("nil retained segment registration")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.err != nil {
		return false, s.err
	}
	if s.phase == 5 {
		return true, nil
	}
	if m.closing {
		return false, os.ErrClosed
	}
	if s.manager != nil && (s.manager != m || s.path != path || s.id != id) {
		return false, rootpublication.ErrResourceConflict
	}
	reserve := func(r, b uint64) (bool, error) { return leafAppendReserve(w, r, b) }
	fail := func(e error) (bool, error) { s.err = e; return false, e }
	switch s.phase {
	case 0:
		// Existing registration is a fixed identity check, never a reopened alias.
		if f := m.files[id]; f != nil {
			if ok, e := reserve(3, uint64(4*len(path)+512)); !ok || e != nil {
				return false, e
			}
			if filepath.Clean(f.Path) != filepath.Clean(path) {
				return fail(rootpublication.ErrResourceConflict)
			}
			a, e1 := f.File.Stat()
			b, e2 := os.Stat(path)
			if e1 != nil || e2 != nil {
				return fail(errors.Join(e1, e2))
			}
			if !os.SameFile(a, b) {
				return fail(rootpublication.ErrResourceConflict)
			}
			s.manager, s.path, s.id, s.phase = m, path, id, 5
			return true, nil
		}
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(File{}))) + retainedalloc.AllocationCharge(2*uint64(len(path))+128+3*retirementParentRetentionEnvelope)
		if ok, e := reserve(1, charge); !ok || e != nil {
			return false, e
		}
		if m.producerMetadata != nil {
			if e := m.producerMetadata.AddPending(charge); e != nil {
				return fail(e)
			}
		}
		s.manager, s.path, s.id, s.metadata, s.charge = m, path, id, m.producerMetadata, charge
		s.open = segmentOpenWork{path: path, id: id, dictLookup: m.dictLookup, templateLookup: m.templateLookup, templateOpts: m.templateDecodeOpts, templateCache: m.templateDefCache}
	case 1:
		ready, e := s.open.step(w)
		if e != nil {
			return fail(e)
		}
		if !ready {
			return false, nil
		}
		s.open.file.producerMetadata, s.open.file.producerCharge = s.metadata, s.charge
	case 2:
		if ok, e := reserve(5, uint64(8*len(path)+2048)); !ok || e != nil {
			return false, e
		}
		if e := validateRegisteredSegmentAuthority(s.open.file); e != nil {
			if m.stableResourcePins != nil || !errors.Is(e, rootpublication.ErrStableIdentityUnsupported) {
				return fail(e)
			}
		}
	case 3:
		f := s.open.file
		if m.stableResourcePins != nil {
			if !s.identityReady {
				if ok, e := reserve(2, uint64(2*len(path)+512)); !ok || e != nil {
					return false, e
				}
				identity, e := rootpublication.StableIdentityFromFile(f.File)
				if e != nil {
					return fail(e)
				}
				namespace, e := stableSegmentNamespace(f.registeredParentIdentity, filepath.Base(path))
				if e != nil {
					return fail(e)
				}
				s.identity, s.namespace, s.identityReady = identity, namespace, true
			}
			ready, e := m.stableResourcePins.ObserveWithWorkV6(s.identity, w)
			if e != nil {
				return fail(e)
			}
			if !ready {
				return false, nil
			}
			f.stableIdentity = s.identity
			f.stableObserved = true
			f.stableNamespace = s.namespace
		}
	case 4:
		// Conservative whole-table allocation admission covers Go map growth. It is
		// checked under the original Manager lock against actual current size.
		count := uint64(len(m.files) + 1)
		if ok, e := reserve(2+uint64(len(m.files)), count*64+uint64(unsafe.Sizeof(File{}))); !ok || e != nil {
			return false, e
		}
		if m.files == nil {
			m.files = make(map[uint32]*File, 16)
		}
		if m.files[id] != nil {
			return fail(fmt.Errorf("%w: concurrent segment registration", rootpublication.ErrResourceConflict))
		}
		f := s.open.file
		if s.metadata != nil && f.constructionParents == ([2]*os.File{}) {
			temporary := uint64(2 * retirementParentRetentionEnvelope)
			s.charge -= temporary
			f.producerCharge = s.charge
			s.metadata.RemovePending(temporary)
		}
		m.fileAliases.Add(1)
		f.manager = m
		configureRegisteredFile(m, f)
		m.files[id] = f
		s.open.file = nil
		s.metadata = nil
		s.charge = 0
	}
	s.phase++
	return s.phase == 5, nil
}
func configureRegisteredFile(m *Manager, f *File) {
	f.setGroupedFrameCacheBudget(m.groupedFrameCacheBudget)
	f.setGroupedFrameCacheMaxBytes(m.groupedFrameCacheMaxBytes)
	f.setGroupedFrameCacheEntries(m.groupedFrameCacheEntries)
	f.setGroupedFrameCacheMaxRawBytes(m.groupedFrameCacheMaxRaw)
}

// Close uses the original physical receipt. A failed close retains the same
// File and charge; it is never retried or silently transferred to another owner.
func (s *SegmentRegistrationWork) Close() error {
	if s == nil || s.phase == 5 {
		return nil
	}
	if s.cleanupErr != nil {
		return s.cleanupErr
	}
	f := s.open.file
	if f == nil {
		if s.metadata != nil {
			s.metadata.RemovePending(s.charge)
			s.metadata = nil
			s.charge = 0
		}
		return nil
	}
	var err error
	for i := range f.constructionParents {
		err = errors.Join(err, f.closeConstructionParent(i))
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		s.cleanupErr = err
		return err
	}
	if f.stableObserved && s.manager.stableResourcePins != nil {
		if err := s.manager.stableResourcePins.Unobserve(f.stableIdentity); err != nil {
			s.cleanupErr = err
			if s.metadata != nil {
				s.metadata.CleanupFailed()
			}
			return err
		}
		f.stableObserved = false
	}
	// The File's manager edge has not been installed; its producer accounting is
	// still this original registration's responsibility.
	f.producerMetadata = nil
	f.producerCharge = 0
	s.open.file = nil
	f = nil
	metadata, charge := s.metadata, s.charge
	s.metadata = nil
	s.charge = 0
	if metadata != nil {
		metadata.RemovePending(charge)
	}
	return nil
}

func (m *Manager) PromoteCurrentWritableReplacingWithWorkV6(id, previous uint32, w *iterator.OrdinalScanWork) (bool, error) {
	if m == nil {
		return true, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	count := uint64(len(m.currentWritableByLane))
	if ok, e := leafAppendReserve(w, 2+count, (count+1)*64+256); !ok || e != nil {
		return false, e
	}
	return true, m.promoteCurrentWritableReplacingLocked(id, previous)
}

// CurrentSegmentRegistrationWork is the original register/promote transition.
// Phase 2 leaves any backend-specific manifest completion to that producer.
type CurrentSegmentRegistrationWork struct {
	Segment SegmentRegistrationWork
	Phase   uint8
}

func (m *Manager) RegisterCurrentSegmentWithWorkV6(path string, id, previous uint32, s *CurrentSegmentRegistrationWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s.Phase >= 2 {
		return true, nil
	}
	if s.Phase == 0 {
		ready, e := m.RegisterSegmentWithWorkV6(path, id, &s.Segment, w)
		if e != nil || !ready {
			return false, e
		}
		s.Phase = 1
	}
	ready, e := m.PromoteCurrentWritableReplacingWithWorkV6(id, previous, w)
	if e != nil || !ready {
		return false, e
	}
	s.Phase = 2
	return true, nil
}
