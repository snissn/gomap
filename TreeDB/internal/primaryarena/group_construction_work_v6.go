package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// GroupConstructionWorkV6 builds the SAME flat group one admitted component
// edge at a time. The original parent owns Group from its first nonnil return,
// including on refusal/error; cancellation drops it on that parent's queue.
type GroupConstructionWorkV6 struct {
	self  *GroupConstructionWorkV6
	arena *Arena
	group *Group
	refs  [groupSize]Ref
	index int
	done  bool
}

func (s *GroupConstructionWorkV6) Step(a *Arena, refs [groupSize]Ref, w *iterator.OrdinalScanWork) (*Group, bool, error) {
	if s == nil || a == nil || w == nil {
		return nil, false, ErrStale
	}
	if s.self != nil && (s.self != s || s.arena != a) {
		return nil, false, ErrStale
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.group == nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(Group{}))) {
			return nil, false, nil
		}
		g, err := a.newGroup(Group{owner: a, refs: 1})
		if err != nil {
			return nil, false, err
		}
		s.self, s.arena, s.group, s.refs = s, a, g, refs
		a.counters.GroupsCreated++
	}
	if s.group.owner != a || s.group.refs == 0 {
		return s.group, false, ErrStale
	}
	if s.done {
		return s.group, true, nil
	}
	for s.index < groupSize {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Ref{}))+2*uint64(unsafe.Sizeof(bank{}))) {
			return s.group, false, nil
		}
		r := s.refs[s.index]
		if r != (Ref{}) {
			b := a.valid(r)
			if b == nil || !b.sealed || b.class != Component || b.refs == ^uint64(0) {
				return s.group, false, ErrStale
			}
			b.refs++
			s.group.components[s.index] = r
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
		}
		s.index++
	}
	s.done = true
	return s.group, true, nil
}
