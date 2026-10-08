package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

type terminalArenaCredit struct {
	bytes uint64
	refs  int
	deny  bool
}

func (a *terminalArenaCredit) ReserveStableMetadata(n uint64) error {
	if a.deny {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	a.bytes += n
	return nil
}
func (a *terminalArenaCredit) RetainStableMetadata() error { a.refs++; return nil }
func (a *terminalArenaCredit) ReleaseStableMetadata() {
	if a.refs <= 0 {
		panic("unbalanced terminal test credit")
	}
	a.refs--
}
func terminalArenaLimits() *PreparedRootPublicationLimits {
	return &PreparedRootPublicationLimits{MaxVisibleMembers: 2, MaxSeals: 2, MaxAllocatorDebt: 4, MaxVisibleResources: 2, MaxSealPrefixEntries: 4}
}
func terminalArenaRuntime() (*DB, *rootPublicationRuntimeV1) {
	db := &DB{}
	r := &rootPublicationRuntimeV1{db: db, visibleMembers: make(map[uint64]*rootPublicationVisibleMemberV1)}
	db.rootPublication = r
	return db, r
}

func TestStableTerminalArenaCreatorFirstOperationRetiresWhileRuntimeLives(t *testing.T) {
	db, r := terminalArenaRuntime()
	request, creator, first := &terminalArenaCredit{}, &terminalArenaCredit{}, &terminalArenaCredit{}
	_, one, err := db.admitRootPublicationTerminalV1(terminalArenaLimits(), request, creator, first)
	if err != nil {
		t.Fatal(err)
	}
	arena := r.terminalArena
	creatorBytes := creator.bytes
	secondRequest, second := &terminalArenaCredit{}, &terminalArenaCredit{}
	_, two, err := db.admitRootPublicationTerminalV1(terminalArenaLimits(), secondRequest, creator, second)
	if err != nil {
		t.Fatal(err)
	}
	if r.terminalArena != arena || creator.bytes != creatorBytes {
		t.Fatal("second operation recreated runtime storage")
	}
	r.mu.Lock()
	one.closeLocked()
	if first.refs != 0 || creator.refs == 0 || arena.loans != 1 {
		t.Fatal("first operation retired the runtime creator or surviving borrower")
	}
	if err = r.closeTerminalArenaLocked(); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal("runtime closed backing still loaned", err)
	}
	two.closeLocked()
	if err = r.closeTerminalArenaLocked(); err != nil {
		t.Fatal(err)
	}
	r.mu.Unlock()
	if creator.refs != 0 || second.refs != 0 || r.terminalArena != nil || arena.scratch != nil || arena.managerStorage != nil {
		t.Fatal("exact last edges did not release actual backing")
	}
	if request.bytes == 0 || secondRequest.bytes == 0 {
		t.Fatal("terminal loan did not debit both source operations")
	}
}

func TestStableTerminalArenaDestinationFailureKeepsCumulativeDebitAndResidentCreator(t *testing.T) {
	db, r := terminalArenaRuntime()
	request, creator, borrower := &terminalArenaCredit{}, &terminalArenaCredit{}, &terminalArenaCredit{deny: true}
	_, loan, err := db.admitRootPublicationTerminalV1(terminalArenaLimits(), request, creator, borrower)
	if loan != nil || err == nil || request.bytes == 0 || borrower.refs != 0 {
		t.Fatal("destination refusal lost cumulative source debit or acquired a loan", err)
	}
	if r.terminalArena == nil || r.terminalArena.loans != 0 || creator.refs == 0 {
		t.Fatal("actual already-born runtime creator disappeared on destination refusal")
	}
	r.mu.Lock()
	err = r.closeTerminalArenaLocked()
	r.mu.Unlock()
	if err != nil || creator.refs != 0 {
		t.Fatal("failed first borrower prevented exact creator teardown", err)
	}
}

func TestStableTerminalArenaEnvelopeRefusesForeignBirthBeforeActivation(t *testing.T) {
	db, r := terminalArenaRuntime()
	_, loan, err := db.admitRootPublicationTerminalV1(terminalArenaLimits(), &terminalArenaCredit{}, &terminalArenaCredit{}, &terminalArenaCredit{})
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.visibleMembers[1] = &rootPublicationVisibleMemberV1{}
	r.visibleMembers[2] = &rootPublicationVisibleMemberV1{}
	before := len(r.visibleMembers)
	err = r.checkTerminalEnvelopeLocked(nil, true, false)
	if err == nil || len(r.visibleMembers) != before {
		t.Fatal("foreign member exceeded the immutable admitted envelope", err)
	}
	clear(r.visibleMembers)
	loan.closeLocked()
	err = r.closeTerminalArenaLocked()
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// Test-only independent resident scopes exercise the same cumulative birth and
// whole-scope last-reference contract. This is a refusal witness for terminal
// backing alone; it grants no native caller or complete metadata certificate.
type terminalArenaResidentPool struct{ live, limit uint64 }
type terminalArenaResidentScope struct {
	pool *terminalArenaResidentPool
	held uint64
	refs int
}

func (s *terminalArenaResidentScope) ReserveStableMetadata(n uint64) error {
	if n > s.pool.limit-s.pool.live {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	s.pool.live += n
	s.held += n
	return nil
}
func (s *terminalArenaResidentScope) RetainStableMetadata() error { s.refs++; return nil }
func (s *terminalArenaResidentScope) ReleaseStableMetadata() {
	if s.refs <= 0 {
		panic("unbalanced horizon witness scope")
	}
	s.refs--
	if s.refs == 0 {
		s.pool.live -= s.held
		s.held = 0
	}
}
func TestStableTerminalArenaFullHorizonRefusesExcessLiveLoans(t *testing.T) {
	limits := terminalArenaLimits()
	limits.MaxVisibleMembers = 64
	limits.MaxSeals = 64
	limits.MaxAllocatorDebt = 64
	limits.MaxVisibleResources = 64
	limits.MaxSealPrefixEntries = 8192
	planned, err := terminalArenaClassBytesV1(limits)
	if err != nil {
		t.Fatal(err)
	}
	capacity, err := terminalArenaEnvelopeV1(limits)
	if err != nil || capacity.Roles != 327 || capacity.LooseTokens != 66 || capacity.Tokens != 20994 {
		t.Fatal("actual whole-runtime geometry", capacity, err)
	}
	// This raw lower bound excludes role arrays, controls, class rounding,
	// member controls, registry history/reservations and every other family.
	lower := uint64(capacity.Tokens) * 176
	if planned < lower || planned > ^uint64(0)/65 || planned*65 <= 128<<20 {
		t.Fatal("64 resident loans falsely certified", planned, lower)
	}
	db, r := terminalArenaRuntime()
	pool := &terminalArenaResidentPool{limit: 128 << 20}
	creator := &terminalArenaResidentScope{pool: pool}
	var loans [64]*rootPublicationTerminalLoanV1
	accepted := 0
	for i := range loans {
		request := &terminalArenaCredit{}
		borrower := &terminalArenaResidentScope{pool: pool}
		_, loan, e := db.admitRootPublicationTerminalV1(limits, request, creator, borrower)
		if e != nil {
			if loan != nil || borrower.refs != 0 || request.bytes == 0 || creator.refs == 0 || r.terminalArena.loans != accepted {
				t.Fatal("refusal dropped actual held owners or source debit", e)
			}
			break
		}
		if r.terminalArena.bytes != planned {
			t.Fatal("census diverged from actual constructor backing", r.terminalArena.bytes, planned)
		}
		loans[i] = loan
		accepted++
	}
	if accepted == 0 || accepted == 64 || pool.live > pool.limit {
		t.Fatal("finite horizon did not refuse", accepted, pool.live)
	}
	r.mu.Lock()
	for _, loan := range loans {
		loan.closeLocked()
	}
	err = r.closeTerminalArenaLocked()
	r.mu.Unlock()
	if err != nil || pool.live != 0 || creator.refs != 0 {
		t.Fatal("refused horizon retained actual completed loans", err, pool.live)
	}
}
