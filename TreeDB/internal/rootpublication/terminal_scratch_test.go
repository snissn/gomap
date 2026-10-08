package rootpublication

import (
	"testing"
)

type terminalScratchCredit struct {
	bytes uint64
	refs  int
	deny  bool
}

func (a *terminalScratchCredit) ReserveStableMetadata(n uint64) error {
	if a.deny {
		return ErrStableMetadataShapeUnsupported
	}
	a.bytes += n
	return nil
}
func (a *terminalScratchCredit) RetainStableMetadata() error { a.refs++; return nil }
func (a *terminalScratchCredit) ReleaseStableMetadata() {
	if a.refs <= 0 {
		panic("unbalanced scratch test creator")
	}
	a.refs--
}

func TestStableTerminalScratchJoinedFailureClearsCompleteTypedCapacity(t *testing.T) {
	request, resident := &terminalScratchCredit{}, &terminalScratchCredit{}
	s, err := NewStableTerminalScratch(StableTerminalCapacity{Roles: 3, ExtraRoles: 2, LooseTokens: 2, Tokens: 4}, request, resident)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Begin(); err != nil {
		t.Fatal(err)
	}
	// Populate the tail, beyond every live length, as a previous larger retry
	// would. A joined smaller failure must clear those retained aliases too.
	token := &StableResourceToken{}
	set := &StableResourceSet{}
	pin := &IdentityPin{}
	s.roles[:cap(s.roles)][2] = StableTerminalOwnedSet{Set: set}
	s.extra[:cap(s.extra)][1] = StableTerminalOwnedSet{Set: set}
	s.loose[:cap(s.loose)][1] = token
	s.tokens[:cap(s.tokens)][3] = token
	s.pins[:cap(s.pins)][7] = pin
	s.groups[:cap(s.groups)][3] = StableSegmentTerminalGroup{OwnedPins: []*IdentityPin{pin}}
	if err = s.Close(); err != ErrResourceOwnership || resident.refs != 1 {
		t.Fatal("active invocation released resident scratch", err)
	}
	s.End()
	for _, v := range s.roles[:cap(s.roles)] {
		if v.Set != nil {
			t.Fatal("roles tail retained an owner")
		}
	}
	for _, v := range s.extra[:cap(s.extra)] {
		if v.Set != nil {
			t.Fatal("extra tail retained an owner")
		}
	}
	for _, v := range s.tokens[:cap(s.tokens)] {
		if v != nil {
			t.Fatal("token tail retained an owner")
		}
	}
	for _, v := range s.groups[:cap(s.groups)] {
		if v.Retention != nil || v.OwnedPins != nil {
			t.Fatal("group tail retained a registrar/pin")
		}
	}
	for _, v := range s.pins[:cap(s.pins)] {
		if v != nil {
			t.Fatal("pin tail retained a registry")
		}
	}
	if _, _, err = s.Begin(); err != nil {
		t.Fatal("retry could not reuse scrubbed storage", err)
	}
	s.End()
	if err = s.Close(); err != nil || resident.refs != 0 || request.bytes == 0 {
		t.Fatal("scratch creator did not survive until exact close", err)
	}
}

func TestStableTerminalScratchOversizedShapeRefusesBeforeCredit(t *testing.T) {
	request, resident := &terminalScratchCredit{}, &terminalScratchCredit{}
	if s, err := NewStableTerminalScratch(StableTerminalCapacity{Roles: 1, Tokens: int(^uint(0) >> 1)}, request, resident); s != nil || err == nil {
		t.Fatal("overflow accepted", err)
	}
	if request.bytes != 0 || resident.bytes != 0 || resident.refs != 0 {
		t.Fatal("shape refusal had credit effects")
	}
}
