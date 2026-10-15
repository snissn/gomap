package primaryarena

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// DirectorySealWorkV6 belongs to the original admitted native producer. It
// captures one private image and validates the SAME format and original group
// custody across finite returns; the final write installs the original bank.
// Caller retains its original group edges until completion or cancellation.
type DirectorySealWorkV6 struct {
	self        *DirectorySealWorkV6
	arena       *Arena
	root        Ref
	owner       ReadRootConstructionV6
	groups      [1]*Group
	image       [page.PageSize]byte
	decode      node.PrimaryDirectoryDecodeWorkV6
	directory   node.PrimaryDirectoryView
	certificate *ReadRootCertificateV6
	index       int
	phase       uint8
	failure     error
}

func (s *DirectorySealWorkV6) Step(a *Arena, r Ref, image []byte, groups [1]*Group, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	if s == nil || a == nil || owner == nil || w == nil {
		return nil, false, ErrStale
	}
	if s.self != nil && (s.self != s || s.arena != a || s.root != r || s.owner != owner || s.groups != groups) {
		return nil, false, ErrStale
	}
	if s.failure != nil {
		return nil, false, s.failure
	}
	cert, ready, err := s.step(a, r, image, groups, owner, w)
	if err != nil {
		s.failure = err
	}
	return cert, ready, err
}
func (s *DirectorySealWorkV6) step(a *Arena, r Ref, image []byte, groups [1]*Group, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	if s.phase == 0 {
		if !w.Reserve(1, 2*page.PageSize+2*uint64(unsafe.Sizeof(*s))) {
			return nil, false, nil
		}
		if len(image) != page.PageSize || page.DecodeHeader(image).PageID != r.PageID {
			return nil, false, ErrFormat
		}
		s.self, s.arena, s.root, s.owner, s.groups = s, a, r, owner, groups
		copy(s.image[:], image)
		s.phase = 1
	}
	if s.phase == 1 {
		ready, err := s.decode.Step(s.image[:], w)
		if !ready || err != nil {
			return nil, false, err
		}
		s.directory, _ = s.decode.Result()
		s.phase = 2
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b, observed := a.validWithWorkV6(r, w)
	if !observed {
		return nil, false, nil
	}
	if b == nil || b.class != Directory || b.construction != owner || !a.capsuleFormat {
		return nil, false, ErrStale
	}
	if s.phase == 5 {
		if !b.sealed || b.readCertificate != s.certificate {
			return nil, false, ErrStale
		}
		return s.certificate, true, nil
	}
	if b.sealed {
		return nil, false, ErrStale
	}
	for s.phase == 2 && s.index < s.directory.Count() {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+2*uint64(unsafe.Sizeof(Group{}))+2*uint64(unsafe.Sizeof(bank{}))) {
			return nil, false, nil
		}
		entry, err := s.directory.Entry(s.index)
		if err != nil {
			return nil, false, err
		}
		if !entry.InlineAbsence() {
			g := groups[int(entry.ClassSlot)/groupSize]
			if g == nil || g.owner != a || g.refs == 0 {
				return nil, false, ErrStale
			}
			component := g.componentAt(int(entry.ClassSlot) % groupSize)
			cb := a.valid(component)
			if component.PageID != entry.Operand.Ref.Page || cb == nil || !cb.sealed || cb.digest != entry.Operand.Digest {
				return nil, false, ErrFormat
			}
		}
		s.index++
	}
	if s.phase == 2 {
		s.index, s.phase = 0, 3
	}
	for s.phase == 3 && s.index < groupCount*groupSize {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Ref{}))+2*uint64(unsafe.Sizeof(Group{}))) {
			return nil, false, nil
		}
		g := groups[s.index/groupSize]
		if g != nil && (g.owner != a || g.refs == 0) {
			return nil, false, ErrStale
		}
		if g != nil && s.index >= s.directory.Count() && g.componentAt(s.index%groupSize) != (Ref{}) {
			return nil, false, ErrFormat
		}
		s.index++
	}
	if s.phase == 3 {
		s.phase = 4
	}
	// No validated private operand changes after these final checks. Original
	// group references protect every previously checked component across yields.
	if !w.Reserve(2, 4*page.PageSize+2*uint64(unsafe.Sizeof(bank{}))+2*uint64(unsafe.Sizeof(Group{}))+2*uint64(unsafe.Sizeof(ReadRootCertificateV6{}))) {
		return nil, false, nil
	}
	for _, g := range groups {
		if g != nil && (g.owner != a || g.refs == 0 || g.refs == ^uint64(0)) {
			return nil, false, ErrStale
		}
	}
	if ready, err := a.prepareReadRootCertificateLockedV6(b, w); !ready || err != nil {
		return nil, false, err
	}
	physical, ready, err := a.writeBundleLocked(r, s.image[:], nil, w)
	if !ready || err != nil {
		return nil, false, err
	}
	// SAME nonfallible bank/certificate/edge installation as ordinary sealing.
	certificate := a.installSealedDirectoryLockedV6(b, r, s.directory, groups, physical, owner, 0)
	for _, g := range groups {
		if g != nil {
			a.counters.EdgeWrites++
		}
	}
	if Local(r.PageID) < pager.PrimaryReadRootLocalBaseV6 {
		a.counters.PagesWritten++
	}
	a.counters.EdgeReads += uint64(s.directory.Count()) + uint64(groupCount*groupSize)
	s.certificate, s.phase = certificate, 5
	return s.certificate, true, nil
}
