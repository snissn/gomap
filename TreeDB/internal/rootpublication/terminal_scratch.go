package rootpublication

import "unsafe"

// StableTerminalCapacity is an immutable whole-runtime cleanup envelope. It
// counts actual owned roles and tokens, including retired seals and previous
// visible owners; it is unrelated to token-birth or registered-file ceilings.
type StableTerminalCapacity struct {
	Roles, ExtraRoles, LooseTokens, Tokens int
}

// StableTerminalScratch belongs to the existing serial runtime. Only its
// resident creator survives admission; request credit is never stored. Its
// typed aliases exist during one checked terminal invocation and are cleared
// before that invocation leaves its Manager join, including error/panic paths.
// No mutex is needed: the actual reporter reservation provides serialization.
type StableTerminalScratch struct {
	capacity      StableTerminalCapacity
	account       StableMetadataAccount
	bytes         uint64
	roles, extra  []StableTerminalOwnedSet
	loose, tokens []*StableResourceToken
	groups        []StableSegmentTerminalGroup
	pins          []*IdentityPin
	inUse, closed bool
}

// StableTerminalScratchClassBytes plans the same individual backing births as
// the constructor, without allocating or claiming credit. It is a class census,
// not publication or ownership eligibility.
func StableTerminalScratchClassBytes(c StableTerminalCapacity) (uint64, error) {
	if c.Roles <= 0 || c.ExtraRoles < 0 || c.ExtraRoles > c.Roles || c.LooseTokens < 0 || c.Tokens < c.LooseTokens || c.Tokens > int(^uint(0)>>1)/int(unsafe.Sizeof(StableSegmentTerminalGroup{})) || c.Roles > int(^uint(0)>>1)/int(unsafe.Sizeof(StableTerminalOwnedSet{})) {
		return 0, ErrStableMetadataShapeUnsupported
	}
	var p stableBackingSizePlan
	p.add(uint64(unsafe.Sizeof(StableTerminalScratch{})), true)
	p.add(uint64(c.Roles)*uint64(unsafe.Sizeof(StableTerminalOwnedSet{})), true)
	p.add(uint64(c.ExtraRoles)*uint64(unsafe.Sizeof(StableTerminalOwnedSet{})), true)
	p.add(uint64(c.LooseTokens)*uint64(unsafe.Sizeof((*StableResourceToken)(nil))), true)
	p.add(uint64(c.Tokens)*uint64(unsafe.Sizeof((*StableResourceToken)(nil))), true)
	p.add(uint64(c.Tokens)*uint64(unsafe.Sizeof(StableSegmentTerminalGroup{})), true)
	p.add(uint64(c.Tokens)*2*uint64(unsafe.Sizeof((*IdentityPin)(nil))), true)
	return p.bytes, p.err
}

func NewStableTerminalScratch(c StableTerminalCapacity, request, resident StableMetadataAccount) (*StableTerminalScratch, error) {
	if request == nil || resident == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	n, err := StableTerminalScratchClassBytes(c)
	if err != nil {
		return nil, err
	}
	// Debit is cumulative, even if destination admission subsequently fails.
	if err := request.ReserveStableMetadata(n); err != nil {
		return nil, err
	}
	if err := resident.ReserveStableMetadata(n); err != nil {
		return nil, err
	}
	if err := resident.RetainStableMetadata(); err != nil {
		return nil, err
	}
	return &StableTerminalScratch{capacity: c, account: resident, bytes: n,
		roles: make([]StableTerminalOwnedSet, 0, c.Roles), extra: make([]StableTerminalOwnedSet, 0, c.ExtraRoles),
		loose: make([]*StableResourceToken, 0, c.LooseTokens), tokens: make([]*StableResourceToken, 0, c.Tokens),
		groups: make([]StableSegmentTerminalGroup, 0, c.Tokens), pins: make([]*IdentityPin, 0, c.Tokens*2)}, nil
}

func (s *StableTerminalScratch) ClassBytes() uint64 {
	if s == nil {
		return 0
	}
	return s.bytes
}
func (s *StableTerminalScratch) Capacity() StableTerminalCapacity {
	if s == nil {
		return StableTerminalCapacity{}
	}
	return s.capacity
}

// Begin is invoked only after the scheduler's exact finish reservation. The
// returned extra/loose arrays are preparation data, never exported publicly.
func (s *StableTerminalScratch) Begin() ([]StableTerminalOwnedSet, []*StableResourceToken, error) {
	if s == nil || s.closed || s.inUse {
		return nil, nil, ErrResourceOwnership
	}
	s.inUse = true
	return s.extra[:0], s.loose[:0], nil
}
func (s *StableTerminalScratch) combine(pending []pendingEntry, extra []StableTerminalOwnedSet) ([]StableTerminalOwnedSet, error) {
	if s == nil || !s.inUse || s.closed || len(extra) > s.capacity.ExtraRoles || len(pending) > s.capacity.Roles-len(extra) {
		return nil, ErrStableMetadataShapeUnsupported
	}
	s.roles = s.roles[:0]
	for _, entry := range pending {
		s.roles = append(s.roles, StableTerminalOwnedSet{entry.candidate.resourceSet(), ResourceOwnerCoordinator})
	}
	s.roles = append(s.roles, extra...)
	return s.roles, nil
}

// End must follow consumer.EndTerminalRelease, which removes every plan-only
// reference first. It changes no token/set ownership; failed owners remain in
// the runtime's real fields. Full capacities clear aliases, not only lengths.
func (s *StableTerminalScratch) End() {
	if s == nil || !s.inUse {
		return
	}
	clear(s.roles[:cap(s.roles)])
	clear(s.extra[:cap(s.extra)])
	clear(s.loose[:cap(s.loose)])
	clear(s.tokens[:cap(s.tokens)])
	clear(s.groups[:cap(s.groups)])
	clear(s.pins[:cap(s.pins)])
	s.roles = s.roles[:0]
	s.extra = s.extra[:0]
	s.loose = s.loose[:0]
	s.tokens = s.tokens[:0]
	s.groups = s.groups[:0]
	s.pins = s.pins[:0]
	s.inUse = false
}
func (s *StableTerminalScratch) Close() error {
	if s == nil || s.closed {
		return nil
	}
	if s.inUse {
		return ErrResourceOwnership
	}
	s.roles = nil
	s.extra = nil
	s.loose = nil
	s.tokens = nil
	s.groups = nil
	s.pins = nil
	a := s.account
	s.account = nil
	s.closed = true
	a.ReleaseStableMetadata()
	return nil
}

// StableTerminalPreparedTransition adds constructor-owned scratch to the same
// synchronous transition. No consumer or transition is stored in this arena.
type StableTerminalPreparedTransition interface {
	StableTerminalTransition
	TerminalScratch() *StableTerminalScratch
}
