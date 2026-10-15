package valuelog

import (
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"unsafe"
)

// WriterCreationWork is retained by the original lane while the original
// writer is private. No current-writer handle escapes before construction and
// registration finish. Ordinary construction drains these same stages.
type WriterCreationWork struct {
	Writer                 *Writer
	Path                   string
	FileID                 uint32
	Registry               *rootpublication.IdentityPinRegistry
	Owner                  *retainedalloc.Owner
	SyncDirectory          bool
	Sync                   func(*os.File) error
	phase                  uint8
	created, identityReady bool
	identity               rootpublication.StableIdentity
	Err                    error
}

func (s *WriterCreationWork) Step(w *iterator.OrdinalScanWork) (bool, error) {
	if s.Err != nil {
		return false, s.Err
	}
	if s.phase == 7 {
		return true, nil
	}
	reserve := func(r, b uint64) (bool, error) { return leafAppendReserve(w, r, b) }
	fail := func(e error) (bool, error) { s.Err = e; return false, e }
	switch s.phase {
	case 0:
		fileCharge, e := rootpublication.StableFileMetadataCharge(uint64(len(s.Path)))
		if e != nil {
			return fail(e)
		}
		parentCharge, e := rootpublication.StableFileMetadataCharge(uint64(len(filepath.Dir(s.Path))))
		if e != nil {
			return fail(e)
		}
		const prefixCapacity = FrameHeaderSize + (MaxFrameK * 8) + ((MaxFrameK + 1) * 4)
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Writer{}))) + fileCharge + parentCharge + retainedalloc.AllocationCharge(uint64(prefixCapacity))
		if ok, e := reserve(2, charge+uint64(unsafe.Sizeof(Writer{}))); !ok || e != nil {
			return false, e
		}
		owner := s.Owner
		if owner == nil && s.Registry != nil {
			owner = s.Registry.MetadataOwner()
		}
		if owner != nil {
			if e := owner.AddPending(charge); e != nil {
				return fail(e)
			}
		}
		s.Writer = &Writer{metadata: owner, metadataCharge: charge, fileMetadataCharge: fileCharge, parentMetadataCharge: parentCharge, syncFn: s.Sync}
		s.Writer.cleanupFailure.writer = s.Writer
	case 1:
		if ok, e := reserve(2, uint64(4*len(s.Path)+512)); !ok || e != nil {
			return false, e
		}
		f, created, e := openLogFile(s.Path)
		s.Writer.f, s.created = f, created
		if e != nil {
			return fail(e)
		}
	case 2:
		if ok, e := reserve(4, uint64(6*len(s.Path)+1024)); !ok || e != nil {
			return false, e
		}
		s.Writer.stableParent, s.Writer.stableParentErr = captureStableValueLogParent(s.Path, s.Writer.f)
		if s.Writer.stableParentErr != nil {
			// The original constructor's unsupported-parent path is retained. An
			// actual failed Close remains on the original Writer cleanup receipt.
			if s.Writer.stableParent != nil {
				s.Writer.parentCloseAttempted = true
				if e := s.Writer.stableParent.Close(); e != nil {
					s.Writer.closeMu.Lock()
					s.Writer.addCleanupFailureLocked(e)
					s.Writer.closeMu.Unlock()
					return fail(s.Writer.stableParentErr)
				}
				s.Writer.stableParent = nil
			}
		}
	case 3:
		if s.Registry != nil {
			if !s.identityReady {
				if ok, e := reserve(1, 256); !ok || e != nil {
					return false, e
				}
				identity, e := rootpublication.StableIdentityFromFile(s.Writer.f)
				if e != nil {
					return fail(e)
				}
				s.identity, s.identityReady = identity, true
			}
			ready, e := s.Registry.ObserveWithWorkV6(s.identity, w)
			if e != nil {
				return fail(e)
			}
			if !ready {
				return false, nil
			}
			s.Writer.stableResourcePins = s.Registry
			s.Writer.stableResourceIdentity = s.identity
			s.Writer.stableResourceObserved = true
		}
	case 4:
		// Fixed link/identity/duplicate/sync operands of the original namespace
		// proof; no file-content scan or directory enumeration is performed.
		bytes := uint64(16*len(s.Path) + 8192)
		if ok, e := reserve(12, bytes); !ok || e != nil {
			return false, e
		}
		proof, unsupported, uncertified, e := s.Writer.prepareStableCreationProof(s.Writer.stableParent, s.Writer.f, s.Path, s.created, s.SyncDirectory, s.Registry != nil)
		s.Writer.creationProof = proof
		if e != nil {
			return fail(e)
		}
		s.Writer.creationUnsupported, s.Writer.creationUncertified = unsupported, uncertified
	case 5:
		if ok, e := reserve(1, 256); !ok || e != nil {
			return false, e
		}
		info, e := s.Writer.f.Stat()
		if e != nil {
			return fail(e)
		}
		s.Writer.size = info.Size()
	case 6:
		const prefixCapacity = FrameHeaderSize + (MaxFrameK * 8) + ((MaxFrameK + 1) * 4)
		if ok, e := reserve(2, uint64(prefixCapacity)+uint64(unsafe.Sizeof(Writer{}))); !ok || e != nil {
			return false, e
		}
		v := s.Writer
		v.fileID = s.FileID
		v.appendMax = defaultBufferSize
		v.rawWritevMinAvgBytes = defaultRawWritevMinAvgBytes
		v.rawWritevMinBatchRecs = defaultRawWritevMinBatchRecords
		v.prefixBuf = make([]byte, 0, prefixCapacity)
		v.dictFrameEncodeLevel = zstd.SpeedFastest
		v.blockCodec = BlockCodecSnappy
		v.clock = RealClock{}
		v.keepSafetyMargin = DefaultKeepSafetyMargin
	}
	s.phase++
	return s.phase == 7, nil
}
func (s *WriterCreationWork) Close() error {
	if s == nil || s.Writer == nil {
		return nil
	}
	return s.Writer.Close()
}
