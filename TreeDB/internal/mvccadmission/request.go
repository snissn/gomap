package mvccadmission

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

var ErrRequestInvalid = errors.New("mvcc: maintenance request is no longer owned by this Store")

// Request is an immutable, comparable observation of the SAME live Store
// capability. It owns no snapshot, publisher, physical resources or history.
// Capturing it does not certify a floor drain or prepare a mutation: the native
// producer must prove those operations on its actual retained operands.
type Request struct {
	owner          *Capability
	authority      *Authority
	foreign, floor uint64
}

// CaptureRequest reserves before observing this original owner's admission
// state. A refusal touches no owner and manufactures no replacement capability.
// floor must be read from the Store's actual admitted metadata observation.
func (c *Capability) CaptureRequest(floor uint64, w *iterator.OrdinalScanWork) (Request, bool, error) {
	if c == nil || floor == 0 {
		return Request{}, false, ErrRequestInvalid
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(Request{}))+uint64(unsafe.Sizeof(Capability{}))+uint64(unsafe.Sizeof(Authority{}))) {
		return Request{}, false, nil
	}
	a := c.authority
	if a == nil {
		return Request{}, false, ErrRequestInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || c.closed || a.owner != c {
		return Request{}, false, ErrRequestInvalid
	}
	return Request{owner: c, authority: a, foreign: a.foreign, floor: floor}, true, nil
}

func (r Request) Floor() uint64 { return r.floor }

func (r Request) matchesLocked(a *Authority) bool {
	return a != nil && r.authority == a && r.owner != nil &&
		r.owner.authority == a && !a.closed && !r.owner.closed &&
		a.owner == r.owner && a.foreign == r.foreign && r.floor != 0
}

// Validate prices an actual owner observation. Passing this predicate grants
// no cut authority; the producer rechecks the request under its original Cut.
func (r Request) Validate(w *iterator.OrdinalScanWork) (bool, error) {
	if r.authority == nil {
		return false, ErrRequestInvalid
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(Request{}))+uint64(unsafe.Sizeof(Capability{}))+uint64(unsafe.Sizeof(Authority{}))) {
		return false, nil
	}
	a := r.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if !r.matchesLocked(a) {
		return false, ErrRequestInvalid
	}
	return true, nil
}

// ObservePrune validates before PONR on the SAME ordinary producer cut. The
// selected physical key must be an exact obsolete-version operand from the
// native selector, never a generic delete or metadata key. The selector still
// proves anchor/tombstone and current physical revision semantics separately.
// A rejected observation makes accepted foreign work sticky, just as Refuse.
func (c *Cut) ObservePrune(r Request, key []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if c.authority == nil {
		return false, ErrRequestInvalid
	}
	if w == nil || !w.Reserve(1, uint64(len(key))+uint64(unsafe.Sizeof(Request{}))+uint64(unsafe.Sizeof(Capability{}))+uint64(unsafe.Sizeof(Authority{}))) {
		return false, nil
	}
	prefix, valid := mvcckey.VersionPrefix(key)
	valid = valid && len(key)-len(prefix) == 8
	if valid {
		timestamp := ^binary.BigEndian.Uint64(key[len(key)-8:])
		valid = timestamp != 0 && timestamp <= r.floor
	}
	valid = valid && r.matchesLocked(c.authority)
	c.observed = true
	c.qualified = c.qualified && valid
	if !valid {
		return false, ErrRequestInvalid
	}
	return true, nil
}
