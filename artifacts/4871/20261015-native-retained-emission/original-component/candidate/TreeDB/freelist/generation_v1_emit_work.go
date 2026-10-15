package freelist

import (
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

type statePageWalkFrameV1 struct {
	node  *stateNode
	child int
	phase uint8
}

// Both traversals reuse fixed depth-bounded controls. They own no arena whose
// reset could invalidate generation-shared nodes or emitted candidate pages.
type statePageCountWorkV1 struct {
	frames      [chunkTrieDepth + 2]statePageWalkFrameV1
	root        *stateNode
	depth       int
	count       uint64
	begun, done bool
}

func (s *statePageCountWorkV1) step(root *stateNode, w *iterator.OrdinalScanWork) (bool, error) {
	if !s.begun {
		s.root, s.begun = root, true
		s.frames[0].node = root
		s.depth = 1
	}
	if s.root != root {
		return false, ErrGenerationFormat
	}
	for s.depth > 0 {
		f := &s.frames[s.depth-1]
		if f.phase == 0 {
			if ok, e := appendReserve(w, 1, 64); !ok || e != nil {
				return false, e
			}
			n := f.node
			if n == nil || n.freeCount+n.retiredCount == 0 || n.pageID != 0 {
				s.depth--
				continue
			}
			s.count++
			if n.chunk != nil {
				s.depth--
				continue
			}
			f.phase = 1
		}
		if f.child == len(f.node.child) {
			s.depth--
			continue
		}
		if ok, e := appendReserve(w, 1, 8+uint64(unsafe.Sizeof(statePageWalkFrameV1{}))); !ok || e != nil {
			return false, e
		}
		child := f.node.child[f.child]
		f.child++
		if child == nil {
			continue
		}
		if s.depth == len(s.frames) {
			return false, ErrGenerationFormat
		}
		s.frames[s.depth] = statePageWalkFrameV1{node: child}
		s.depth++
	}
	s.done = true
	return true, nil
}

type stateRecordingWriteWorkV1 struct {
	id        uint64
	output    []byte
	mapInsert stateMapInsertWorkV1
	phase     uint8
}

func (s *recordingSink) writeWithWork(id uint64, data []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.sink == nil || len(data) != page.PageSize {
		return false, ErrGenerationFormat
	}
	// Foreign synchronous sinks and callbacks retain their original ordinary
	// semantics. A finite caller must use the actual production validation sink
	// and typed original ledger transition, never price an opaque callback.
	concrete, builtin := s.sink.(*CandidatePageSinkV1)
	if w != nil && (!builtin || s.beforeFirstWrite != nil) {
		return false, ErrGenerationFormat
	}
	c := &s.work
	if c.phase != 0 && (c.id != id || len(c.output) != len(data) || &c.output[0] != &data[0]) {
		return false, ErrGenerationFormat
	}
	if c.phase == 0 {
		c.id, c.output, c.phase = id, data, 1
	}
	for {
		switch c.phase {
		case 1:
			if !s.writeStarted {
				if ok, e := appendReserve(w, 1, 64); !ok || e != nil {
					return false, e
				}
				if s.ledger != nil {
					if e := s.ledger.markTailWriteAttempted(s.candidateID); e != nil {
						return false, e
					}
				} else if s.beforeFirstWrite != nil {
					if e := s.beforeFirstWrite(); e != nil {
						return false, e
					}
				}
				s.writeStarted = true
			}
			c.phase = 2
		case 2:
			if builtin {
				if concrete == nil {
					return false, ErrGenerationFormat
				}
				if concrete.pageIDs == nil {
					if ok, e := appendReserve(w, 1, 48); !ok || e != nil {
						return false, e
					}
					concrete.pageIDs = make(map[uint64]struct{})
				}
				if ok, e := appendReserve(w, 1, 16); !ok || e != nil {
					return false, e
				}
				if _, exists := concrete.pageIDs[id]; exists {
					return false, ErrGenerationFormat
				}
				// Continue the SAME map admission cursor from this exact admitted probe.
				// The candidate sink is private and cannot accept another page mid-write.
				c.mapInsert = stateMapInsertWorkV1{owner: &concrete.pageIDs, key: id, count: uint64(len(concrete.pageIDs)), phase: 1}
				c.phase = 3
			} else {
				// Arbitrary ordinary sinks may retain or mutate input. The isolated copy
				// has the original lifetime, while the candidate keeps its own page.
				isolated := append([]byte(nil), data...)
				if e := s.sink.WritePage(id, isolated); e != nil {
					return false, e
				}
				c.phase = 4
			}
		case 3:
			if ready, e := c.mapInsert.step(&concrete.pageIDs, id, w); !ready || e != nil {
				return false, e
			}
			c.phase = 4
		case 4:
			records, bytes := uint64(1), uint64(unsafe.Sizeof(PageImageV1{}))
			if len(s.pages) == cap(s.pages) {
				records += uint64(len(s.pages))
				bytes *= 2 * uint64(len(s.pages)+1)
			}
			if ok, e := appendReserve(w, records, bytes); !ok || e != nil {
				return false, e
			}
			s.pages = append(s.pages, PageImageV1{id, data})
			*c = stateRecordingWriteWorkV1{}
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}

type statePageEmissionWorkV1 struct {
	frames      [chunkTrieDepth + 2]statePageWalkFrameV1
	root        *stateNode
	next        *uint64
	sink        *recordingSink
	generation  uint64
	depth       int
	output      []byte
	encoder     statePageEncodeWorkV1
	begun, done bool
	firstErr    error
}

func (s *statePageEmissionWorkV1) step(root *stateNode, generation uint64, next *uint64, sink *recordingSink, w *iterator.OrdinalScanWork) (bool, error) {
	if s.firstErr != nil {
		return false, s.firstErr
	}
	ready, e := s.advance(root, generation, next, sink, w)
	if e != nil {
		s.firstErr = e
	}
	return ready, e
}
func (s *statePageEmissionWorkV1) advance(root *stateNode, generation uint64, next *uint64, sink *recordingSink, w *iterator.OrdinalScanWork) (bool, error) {
	if next == nil || sink == nil || generation == 0 {
		return false, ErrGenerationFormat
	}
	if w != nil {
		if _, ok := sink.sink.(*CandidatePageSinkV1); !ok || sink.beforeFirstWrite != nil {
			return false, ErrGenerationFormat
		}
	}
	if !s.begun {
		s.root, s.generation, s.next, s.sink = root, generation, next, sink
		s.frames[0].node = root
		s.depth, s.begun = 1, true
	}
	if s.root != root || s.generation != generation || s.next != next || s.sink != sink {
		return false, ErrGenerationFormat
	}
	for s.depth > 0 {
		f := &s.frames[s.depth-1]
		n := f.node
		switch f.phase {
		case 0:
			if ok, e := appendReserve(w, 1, 64); !ok || e != nil {
				return false, e
			}
			if n == nil || n.freeCount+n.retiredCount == 0 || n.pageID != 0 {
				s.depth--
				continue
			}
			if n.chunk != nil {
				f.phase = 2
			} else {
				f.phase = 1
			}
		case 1:
			if f.child == len(n.child) {
				f.phase = 2
				continue
			}
			if ok, e := appendReserve(w, 1, 8+uint64(unsafe.Sizeof(statePageWalkFrameV1{}))); !ok || e != nil {
				return false, e
			}
			child := n.child[f.child]
			f.child++
			if child == nil {
				continue
			}
			if s.depth == len(s.frames) {
				return false, ErrGenerationFormat
			}
			s.frames[s.depth] = statePageWalkFrameV1{node: child}
			s.depth++
		case 2:
			if ok, e := appendReserve(w, 1, page.PageSize+16+uint64(unsafe.Sizeof(statePageEncodeWorkV1{}))); !ok || e != nil {
				return false, e
			}
			// These bytes escape to CandidatePageWriterV1, which may retain the view.
			// They cannot come from resettable scratch at Prepare/publication.
			s.output = make([]byte, page.PageSize)
			s.encoder = statePageEncodeWorkV1{}
			n.pageID = *next
			*next++
			if n.chunk != nil {
				n.chunk.pageID = n.pageID
			}
			f.phase = 3
		case 3:
			var ready bool
			var e error
			if n.chunk != nil {
				ready, e = s.encoder.chunkPage(s.output, n.pageID, generation, n.chunk, w)
			} else {
				ready, e = s.encoder.indexPage(s.output, n.pageID, generation, n, w)
			}
			if !ready || e != nil {
				return false, e
			}
			n.checksum = binary.LittleEndian.Uint32(s.output[8:12])
			if n.chunk != nil {
				n.chunk.checksum = n.checksum
			}
			f.phase = 4
		case 4:
			if ready, e := sink.writeWithWork(n.pageID, s.output, w); !ready || e != nil {
				return false, e
			}
			// Only controls are reused; the completed candidate/sink owns the output.
			s.output = nil
			s.encoder = statePageEncodeWorkV1{}
			s.depth--
		default:
			return false, ErrGenerationFormat
		}
	}
	s.done = true
	return true, nil
}
