// Package residentcredit owns the shared scalar DB resident ledger. It keeps
// ordinary birth census and strictly bounded selected scopes in the same owner.
// It grants no constructor-closure, platform or finite publication certificate.
package residentcredit

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

var ErrLimit = errors.New("residentcredit: retained backing limit or terminal owner")

// Request is borrowed synchronously before births; Owner and Scope never store it.
type Request interface{ ReserveStableMetadata(uint64) error }

// Resident scopes are actual per-object destination owners. The existing fixed
// metadata allowance bounds their retained backing; request Reserve callbacks
// still debit the same backing as a retained loan when a request borrows it.
// A destination is reserved and retained before pre-WAL transfer preparation.
type Owner struct {
	mu           sync.Mutex
	limit        uint64
	live         uint64
	births       uint64
	control      uint64
	refs         uint64 // DB owner plus each live destination scope, not each scope copy
	closed       bool
	ordinary     bool
	strictScopes uint64
	classKnown   bool
}
type Scope struct {
	owner    *Owner
	bytes    uint64
	retained uint64
	closed   bool
	strict   bool
}

// New prepays the independent destination owner itself.
// Its constructor has no retained DB, collection or request-input references.
// The explicit ceiling comes from the selected whole-resident source profile.
func New(limit uint64) (*Owner, error) {
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Owner{})), true)
	if err != nil {
		return nil, err
	}
	if control > limit {
		return nil, ErrLimit
	}
	return &Owner{limit: limit, live: control, births: control, control: control, refs: 1, classKnown: true}, nil
}

// releaseOwnerLocked drops one actual owner edge. The root control remains
// charged through DB close whenever a retained physical cut still reaches it.
func (c *Owner) releaseOwnerLocked() {
	if c.refs == 0 {
		panic("residentcredit: resident owner imbalance")
	}
	c.refs--
	if c.refs == 0 {
		if !c.closed || c.live != c.control {
			panic("residentcredit: resident closure imbalance")
		}
		c.live -= c.control
		c.control = 0
	}
}

func (c *Owner) NewScope() (*Scope, error) {
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Scope{})), true)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.classKnown || c.refs == 0 || c.refs == math.MaxUint64 || c.strictScopes == math.MaxUint64 || c.live > c.limit || control > c.limit-c.live || control > math.MaxUint64-c.births {
		return nil, ErrLimit
	}
	c.live += control
	c.births += control
	c.refs++
	c.strictScopes++
	return &Scope{owner: c, bytes: control, retained: 1, strict: true}, nil
}
func (f *Scope) ReserveStableMetadata(n uint64) error {
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed || n > math.MaxUint64-c.live || n > math.MaxUint64-c.births || n > math.MaxUint64-f.bytes ||
		(!c.ordinary || f.strict || c.strictScopes != 0) && (c.live > c.limit || n > c.limit-c.live) {
		return ErrLimit
	}
	c.live += n
	c.births += n
	f.bytes += n
	return nil
}
func (f *Scope) RetainStableMetadata() error {
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed || f.retained == math.MaxUint64 {
		return ErrLimit
	}
	f.retained++
	return nil
}
func (f *Scope) ReleaseStableMetadata() {
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || f.retained == 0 {
		panic("residentcredit: resident credit imbalance")
	}
	f.retained--
	if f.retained == 0 {
		if f.bytes > c.live {
			panic("residentcredit: resident byte imbalance")
		}
		if f.strict {
			c.strictScopes--
		}
		c.live -= f.bytes
		f.bytes = 0
		f.closed = true
		c.releaseOwnerLocked()
	}
}

// The allocator retains this SAME factory-created resident scope. A request
// reserve facet is supplied separately and transiently by each allocation
// operation; these methods do not introduce an allocator-specific ledger.
func (f *Scope) ReserveAllocation(n uint64) error {
	return f.ReserveStableMetadata(n)
}
func (f *Scope) RetainAllocationCredit() error {
	return f.RetainStableMetadata()
}
func (f *Scope) ReleaseAllocationCredit() {
	f.ReleaseStableMetadata()
}

func (c *Owner) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Closing DB admission prevents new creation. Existing physical cuts keep
	// their actual scoped backing charged until the last consumer releases it.
	if c.closed {
		return
	}
	c.closed = true
	c.releaseOwnerLocked()
}

// NewOrdinary installs the same owner before DB constructors. Ordinary census
// does not impose a selected cap. Unknown class layouts stay ordinary-only.
func NewOrdinary(limit uint64) (*Owner, error) {
	if limit == 0 {
		return nil, ErrLimit
	}
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Owner{})), true)
	known := err == nil
	if !known {
		control = uint64(unsafe.Sizeof(Owner{}))
	}
	return &Owner{limit: limit, live: control, births: control, control: control, refs: 1, ordinary: true, classKnown: known}, nil
}

// NewOrdinaryScope owns actual ordinary constructor backing. Foreign births
// respect the selected total envelope while any strict scope remains live.
func (c *Owner) NewOrdinaryScope() (*Scope, error) {
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Scope{})), true)
	if err != nil {
		control = uint64(unsafe.Sizeof(Scope{}))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.ordinary || c.closed || c.refs == 0 || c.refs == math.MaxUint64 || control > math.MaxUint64-c.live || control > math.MaxUint64-c.births ||
		c.strictScopes != 0 && (c.live > c.limit || control > c.limit-c.live) {
		return nil, ErrLimit
	}
	c.live += control
	c.births += control
	c.refs++
	return &Scope{owner: c, bytes: control, retained: 1}, nil
}

// NewRoles prepays BOTH actual scope controls before either birth. Failed
// request reservations are cumulative, with no hidden first scope allocation.
func (c *Owner) NewRoles(request Request) (*Scope, *Scope, error) {
	if c == nil || request == nil {
		return nil, nil, ErrLimit
	}
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Scope{})), true)
	if err != nil || control > math.MaxUint64/2 {
		return nil, nil, ErrLimit
	}
	if err := request.ReserveStableMetadata(control); err != nil {
		return nil, nil, err
	}
	if err := request.ReserveStableMetadata(control); err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.classKnown || c.refs == 0 || c.refs > math.MaxUint64-2 || c.strictScopes > math.MaxUint64-2 || c.live > c.limit ||
		2*control > c.limit-c.live || 2*control > math.MaxUint64-c.births {
		return nil, nil, ErrLimit
	}
	c.live += 2 * control
	c.births += 2 * control
	c.refs += 2
	c.strictScopes += 2
	return &Scope{owner: c, bytes: control, retained: 1, strict: true}, &Scope{owner: c, bytes: control, retained: 1, strict: true}, nil
}

// Stats are caller-owned values, never an aliased control export or authority.
type Stats struct {
	Limit, Live, Births, Control, Refs, StrictScopes uint64
	Closed, Ordinary, ClassKnown                     bool
}
type ScopeStats struct {
	Bytes, Retained uint64
	Closed, Strict  bool
}

func (c *Owner) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{c.limit, c.live, c.births, c.control, c.refs, c.strictScopes, c.closed, c.ordinary, c.classKnown}
}
func (f *Scope) Stats() ScopeStats {
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	return ScopeStats{f.bytes, f.retained, f.closed, f.strict}
}
func (f *Scope) OwnerStats() Stats { return f.owner.Stats() }
func (f *Scope) SameOwner(other *Scope) bool {
	return f != nil && other != nil && f.owner == other.owner
}
func (c *Owner) Limit() uint64 { return c.limit }

// OwnsScope validates an actual live intrinsic scope without allocation or a
// borrowed request. It never reattributes an existing creator or proves bytes.
func (c *Owner) OwnsScope(scope *Scope) bool {
	if c == nil || scope == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return scope.owner == c && !scope.closed && scope.retained != 0
}

// RetainOriginalLifetime extends only a live, ordinary constructor edge. DB
// admission may already be closed; no strict Scope, new Scope or dead edge can
// be revived. The caller must still census each known birth before allocation.
func (f *Scope) RetainOriginalLifetime() error {
	if f == nil {
		return ErrLimit
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.ordinary || f.strict || f.closed || f.retained == 0 || c.refs == 0 || f.retained == math.MaxUint64 {
		return ErrLimit
	}
	f.retained++
	return nil
}

// ReserveOriginalLifetime charges known backing on the SAME retained ordinary
// constructor Scope after admission closes. Live strict borrowers still bound
// all shared growth. This grants neither platform nor hidden-closure coverage.
func (f *Scope) ReserveOriginalLifetime(n uint64) error {
	if f == nil {
		return ErrLimit
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.ordinary || f.strict || f.closed || f.retained == 0 || c.refs == 0 || n > math.MaxUint64-c.live || n > math.MaxUint64-c.births || n > math.MaxUint64-f.bytes ||
		c.strictScopes != 0 && (c.live > c.limit || n > c.limit-c.live) {
		return ErrLimit
	}
	c.live += n
	c.births += n
	f.bytes += n
	return nil
}

// PrivateSnapshotRoleV1 is chosen before any holder/capsule birth. Exported
// Snapshot handles never use this private final-edge control.
type PrivateSnapshotRoleV1 uint8

const (
	PrivatePointReadV1 PrivateSnapshotRoleV1 = iota + 1
	PrivateScanV1
)

// PrivateSnapshotLayoutV1 contains fixed intrinsic constructor sizes, not an
// allocation list or a caller-supplied refund amount. All members are scanned.
type PrivateSnapshotLayoutV1 struct{ Holder, Completion, Point, State, Index uint64 }

// PrivateSnapshotBackingV1 aliases one immutable, private pointer control.
// Release is a final-edge claim; copying the interface never copies the claim.
type PrivateSnapshotBackingV1 interface{ ReleasePrivateSnapshotBackingV1() }
type privateSnapshotBackingV1 struct {
	scope    *Scope
	role     PrivateSnapshotRoleV1
	classes  PrivateSnapshotLayoutV1
	control  uint64
	released atomic.Bool
}

func (f *Scope) ReservePrivateSnapshotBackingV1(role PrivateSnapshotRoleV1, raw PrivateSnapshotLayoutV1) (PrivateSnapshotBackingV1, error) {
	if f == nil || raw.Holder == 0 ||
		role == PrivatePointReadV1 && (raw.Point == 0 || raw.State != 0 || raw.Index != 0) ||
		role == PrivateScanV1 && (raw.Point != 0 || raw.State == 0) ||
		role != PrivatePointReadV1 && role != PrivateScanV1 {
		return nil, ErrLimit
	}
	var classes PrivateSnapshotLayoutV1
	// Fixed scalar fields only: neither caller scratch nor a class slice escapes.
	var err error
	if classes.Holder, err = allocclass.ClassBytes(raw.Holder, true); err != nil {
		return nil, ErrLimit
	}
	// Completion zero denotes control stored inline in the charged Holder.
	// A nonzero value remains a distinct actual constructor allocation.
	if raw.Completion != 0 {
		if classes.Completion, err = allocclass.ClassBytes(raw.Completion, true); err != nil {
			return nil, ErrLimit
		}
	}
	if raw.Point != 0 {
		if classes.Point, err = allocclass.ClassBytes(raw.Point, true); err != nil {
			return nil, ErrLimit
		}
	}
	if raw.State != 0 {
		if classes.State, err = allocclass.ClassBytes(raw.State, true); err != nil {
			return nil, ErrLimit
		}
	}
	if raw.Index != 0 {
		if classes.Index, err = allocclass.ClassBytes(raw.Index, true); err != nil {
			return nil, ErrLimit
		}
	}
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(privateSnapshotBackingV1{})), true)
	if err != nil {
		return nil, ErrLimit
	}
	total := control
	for _, n := range [...]uint64{classes.Holder, classes.Completion, classes.Point, classes.State, classes.Index} {
		if n > math.MaxUint64-total {
			return nil, ErrLimit
		}
		total += n
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.ordinary || f.strict || f.closed || f.retained == 0 || c.refs == 0 || f.retained == math.MaxUint64 ||
		total > math.MaxUint64-c.live || total > math.MaxUint64-c.births || total > math.MaxUint64-f.bytes ||
		c.strictScopes != 0 && (c.live > c.limit || total > c.limit-c.live) {
		return nil, ErrLimit
	}
	c.live += total
	c.births += total
	f.bytes += total
	f.retained++
	return &privateSnapshotBackingV1{scope: f, role: role, classes: classes, control: control}, nil
}
func (b *privateSnapshotBackingV1) ReleasePrivateSnapshotBackingV1() {
	if b == nil || !b.released.CompareAndSwap(false, true) {
		return
	}
	f := b.scope
	if f == nil {
		return
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	n := b.control + b.classes.Holder + b.classes.Completion + b.classes.Point + b.classes.State + b.classes.Index
	if f.closed || f.retained == 0 || n > f.bytes || n > c.live {
		panic("residentcredit: private backing imbalance")
	}
	b.scope = nil
	b.control = 0
	b.classes = PrivateSnapshotLayoutV1{}
	c.live -= n
	f.bytes -= n
	f.retained--
	// Duplicate aliases see the atomic terminal claim without reading cleared
	// Scope/class fields; the terminal control retains no owner graph.
	if f.retained == 0 {
		if f.strict {
			c.strictScopes--
		}
		c.live -= f.bytes
		f.bytes = 0
		f.closed = true
		c.releaseOwnerLocked()
	}
}
