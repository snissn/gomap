package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	templ "github.com/snissn/gomap/TreeDB/template"
	"unsafe"
)

// segmentOpenWork is the original read-handle constructor. Its parent handles
// remain in the original File until their checked, once-only close completes.
type segmentOpenWork struct {
	path            string
	id              uint32
	dictLookup      DictLookup
	templateLookup  TemplateLookup
	templateOpts    templ.DecodeOptions
	templateCache   *templateDefCache
	file            *File
	phase           uint8
	supportedParent bool
	err             error
}

func (s *segmentOpenWork) step(w *iterator.OrdinalScanWork) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if s.phase == 7 {
		return true, nil
	}
	reserve := func(r, b uint64) (bool, error) { return leafAppendReserve(w, r, b) }
	fail := func(e error) (bool, error) { s.err = e; return false, e }
	switch s.phase {
	case 0:
		if ok, e := reserve(1, uint64(unsafe.Sizeof(File{}))+uint64(2*len(s.path))); !ok || e != nil {
			return false, e
		}
		s.file = &File{ID: s.id, Path: s.path, dictLookup: s.dictLookup, templateLookup: s.templateLookup, templateDecodeOpts: s.templateOpts, templateDefCache: s.templateCache,
			compactLeafPayloadAllowed: allowsCompactLeafLogPayload(s.id, s.path), compactLeafPayloadAllowedSet: true,
			groupedFrameCacheEntries: defaultGroupedFrameCacheEntries, groupedFrameCacheMaxRaw: defaultGroupedFrameCacheMaxRawBytes, groupedFrameCacheMaxBytes: defaultGroupedFrameCacheMaxBytes}
		s.file.mmapData.Store([]byte(nil))
	case 1:
		if ok, e := reserve(4, uint64(6*len(s.path)+1024)); !ok || e != nil {
			return false, e
		}
		parent, identity, e := openRegisteredSegmentParent(s.path)
		s.file.constructionParents[0] = parent
		s.file.registeredParentIdentity = identity
		if e != nil && !errors.Is(e, rootpublication.ErrStableIdentityUnsupported) {
			return fail(e)
		}
		s.supportedParent = e == nil && parent != nil
	case 2:
		if ok, e := reserve(1, uint64(2*len(s.path)+512)); !ok || e != nil {
			return false, e
		}
		f, e := openSegmentReadHandle(s.path)
		s.file.File = f
		if e != nil {
			return fail(e)
		}
	case 3:
		if s.supportedParent {
			if ok, e := reserve(6, uint64(10*len(s.path)+2048)); !ok || e != nil {
				return false, e
			}
			if e := validateRegisteredSegmentParent(s.path, s.file, s.file.constructionParents[0], s.file.registeredParentIdentity); e != nil {
				return fail(e)
			}
		}
	case 4:
		if ok, e := reserve(1, 256); !ok || e != nil {
			return false, e
		}
		if info, e := s.file.File.Stat(); e == nil {
			s.file.noteVerifiedFileSize(info.Size())
		}
	case 5, 6:
		if ok, e := reserve(1, 128); !ok || e != nil {
			return false, e
		}
		if e := s.file.closeConstructionParent(int(s.phase) - 5); e != nil {
			return fail(e)
		}
	}
	s.phase++
	return s.phase == 7, nil
}
