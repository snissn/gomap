package mvccadmission

import (
	"bytes"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// Protection is semantic preparation authority, not a physical root, lookup,
// absence, resource or publication certificate. Store issues it only after a
// successful durable floor and separately completed pre-floor drain, while
// holding both Store maintenance and commit exclusion. It never pins a pager.
type Protection struct {
	request       Request
	floor, serial uint64
}

// FloorDrain binds the original incarnation/foreign/protected serial BEFORE
// the separate cut. It carries no physical certificate or assertion that a
// drain completed. Confirmation may never recapture a later authority.
type FloorDrain struct {
	request       Request
	floor, serial uint64
}

func (c *Capability) CaptureFloorDrain(floor uint64, w *iterator.OrdinalScanWork) (*FloorDrain, bool, error) {
	if w == nil {
		return nil, false, iterator.ErrOrdinalScanIncomplete
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(FloorDrain{}))+2*uint64(unsafe.Sizeof(Authority{}))) {
		return nil, false, nil
	}
	if c == nil || c.authority == nil || floor == 0 {
		return nil, true, iterator.ErrOrdinalScanStale
	}
	a := c.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || c.closed || a.owner != c || floor < a.protectedFloor {
		return nil, true, iterator.ErrOrdinalScanStale
	}
	return &FloorDrain{request: Request{owner: c, foreign: a.foreign}, floor: floor, serial: a.protectedSerial}, true, nil
}

// ConfirmDrainedFloor is called by the Store only after successful completion
// of the separately admitted cut. Raw/owner/protected changes during or after
// that cut refuse issuance, even when its returned storage result is success.
func (c *Capability) ConfirmDrainedFloor(d *FloorDrain, w *iterator.OrdinalScanWork) (*Protection, bool, error) {
	if w == nil {
		return nil, false, iterator.ErrOrdinalScanIncomplete
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(FloorDrain{}))+2*uint64(unsafe.Sizeof(Authority{}))+uint64(unsafe.Sizeof(Protection{}))) {
		return nil, false, nil
	}
	if c == nil || c.authority == nil || d == nil || d.request.owner != c {
		return nil, true, iterator.ErrOrdinalScanStale
	}
	a := c.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || c.closed || a.owner != c || a.foreign != d.request.foreign || a.protectedSerial != d.serial || d.floor < a.protectedFloor {
		return nil, true, iterator.ErrOrdinalScanStale
	}
	a.protectedFloor = d.floor
	return &Protection{request: d.request, floor: d.floor, serial: d.serial}, true, nil
}
func (p *Protection) Request() Request {
	if p == nil {
		return Request{}
	}
	return p.request
}
func (p *Protection) Floor() uint64 {
	if p == nil {
		return 0
	}
	return p.floor
}
func (p *Protection) validateLocked(a *Authority) error {
	if p == nil || p.request.owner == nil || p.request.owner.authority != a || a.closed || p.request.owner.closed || a.owner != p.request.owner || a.foreign != p.request.foreign || a.protectedSerial != p.serial || a.protectedFloor < p.floor {
		return iterator.ErrOrdinalScanStale
	}
	return nil
}
func (p *Protection) ValidateWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, iterator.ErrOrdinalScanIncomplete
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Protection{}))+2*uint64(unsafe.Sizeof(Authority{}))) {
		return false, nil
	}
	if p == nil || p.request.owner == nil {
		return true, iterator.ErrOrdinalScanStale
	}
	a := p.request.owner.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	return true, p.validateLocked(a)
}

// ProtectedVersion retains exact immutable semantic operands. Revision is an
// expected logical revision supplied by an actual read; the fresh storage
// adapter must independently read and match it and adopt current value custody.
// This object never authorizes stale BaseKeyProof or old current-output reuse.
type ProtectedVersion struct {
	protection *Protection
	key        []byte
	revision   page.EntryRevision
}

func (p *Protection) BindVersion(key []byte, revision page.EntryRevision, w *iterator.OrdinalScanWork) (*ProtectedVersion, bool, error) {
	if w == nil {
		return nil, false, iterator.ErrOrdinalScanIncomplete
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(ProtectedVersion{}))+2*uint64(len(key))) {
		return nil, false, nil
	}
	prefix, ok := mvcckey.VersionPrefix(key)
	if !ok || len(key)-len(prefix) != 8 || revision == page.LegacyEntryRevision || p == nil {
		return nil, true, iterator.ErrOrdinalScanStale
	}
	timestamp := ^binary.BigEndian.Uint64(key[len(key)-8:])
	if timestamp == 0 || timestamp > p.floor {
		return nil, true, iterator.ErrOrdinalScanStale
	}
	ready, err := p.ValidateWithWork(w)
	if !ready || err != nil {
		return nil, ready, err
	}
	return &ProtectedVersion{protection: p, key: append([]byte(nil), key...), revision: revision}, true, nil
}
func (v *ProtectedVersion) MatchesCurrent(key []byte, revision page.EntryRevision, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, iterator.ErrOrdinalScanIncomplete
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(ProtectedVersion{}))+2*uint64(len(key))) {
		return false, nil
	}
	if v == nil || v.protection == nil || !bytes.Equal(v.key, key) || v.revision != revision {
		return true, iterator.ErrOrdinalScanStale
	}
	return v.protection.ValidateWithWork(w)
}

// ObserveProtectedDeletion consumes an exact freshly matched semantic operand
// under this real publication cut. The storage caller must also own current
// physical winner/resource/complete-output checks. No publication happens here.
func (c *Cut) ObserveProtectedDeletion(v *ProtectedVersion, key []byte, revision page.EntryRevision) error {
	a := c.authority
	if a == nil || v == nil || v.protection == nil || !bytes.Equal(v.key, key) || v.revision != revision {
		return iterator.ErrOrdinalScanStale
	}
	if err := v.protection.validateLocked(a); err != nil {
		return err
	}
	if c.protectedObserved || c.observed {
		return iterator.ErrOrdinalScanStale
	}
	c.observed = true
	c.qualified = true
	c.protectedObserved = true
	c.protected = v.protection
	return nil
}
