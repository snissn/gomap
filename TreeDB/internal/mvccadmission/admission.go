// Package mvccadmission carries in-memory Store input to a real logical
// publication. It contains no key history, storage generation or reader lease.
package mvccadmission

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// Authority belongs to one live DB incarnation. Its address is the incarnation
// identity. The supported single-Store namespace has one actual active owner.
type Authority struct {
	mu      sync.Mutex
	owner   *Capability
	foreign uint64
	closed  bool
}

// Capability is issued once for one individual Store, never shared by Stores.
type Capability struct {
	authority *Authority
	closed    bool
}

// Input borrows the validated caller-owned physical key only until the
// synchronous publication cut. Storage must not retain it as publication state.
type Input struct {
	owner            *Capability
	key              []byte
	timestamp, floor uint64
	floorTransition  bool
}

// Summary is constant-size producer-owned input authority before compaction.
// It retains no keys/history; unknown or mixed input stays sticky until release.
func (i Input) Present() bool { return i.owner != nil }

// BindOwnedKey validates the original producer operands before transferring
// this input's temporary borrow to an independently owned equal key. A queue
// may outlive its caller after a stop result; it must never retain caller bytes.
// Mismatched input remains unqualified instead of manufacturing a new proof.
func (i Input) BindOwnedKey(original, owned []byte) Input {
	if !inputMatchesKey(original, i) || !bytes.Equal(original, owned) {
		return Input{}
	}
	i.key = owned
	return i
}

type Summary struct {
	owner               *Capability
	observed, qualified bool
	minTimestamp        uint64
	floorOnly           bool
}

var discardFloorMetadataKey = [...]byte{0x00, 'T', 'D', 'B', 'M', 'V', 'C', 'C', 0x00, 'M', 0x01, 'd', 'f'}

// FloorInput is only the exact Store metadata publication, never a version
// write or an issuer. Neither relaxed nor durable metadata alone certifies a completed pre-floor drain.
func (c *Capability) FloorInput(key, record []byte, floor uint64) Input {
	if c == nil || floor == 0 || !bytes.Equal(key, discardFloorMetadataKey[:]) || len(record) != 9 || record[0] != 1 || binary.BigEndian.Uint64(record[1:]) != floor {
		return Input{}
	}
	return Input{owner: c, key: key, floor: floor, floorTransition: true}
}
func inputMatchesKey(key []byte, input Input) bool {
	if input.floorTransition {
		return input.owner != nil && input.floor != 0 && bytes.Equal(input.key, key) && bytes.Equal(key, discardFloorMetadataKey[:])
	}
	prefix, ok := mvcckey.VersionPrefix(key)
	return ok && len(key)-len(prefix) == 8 && input.timestamp != 0 &&
		^binary.BigEndian.Uint64(key[len(key)-8:]) == input.timestamp &&
		input.timestamp > input.floor && bytes.Equal(input.key, key) && input.owner != nil
}
func (s *Summary) Observe(key []byte, input Input) {
	valid := inputMatchesKey(key, input)
	if !s.observed {
		s.owner = input.owner
		s.qualified = valid
		s.observed = true
		s.minTimestamp = input.timestamp
		s.floorOnly = input.floorTransition
		return
	}
	s.qualified = s.qualified && valid && s.owner == input.owner && s.floorOnly == input.floorTransition
	if input.timestamp < s.minTimestamp {
		s.minTimestamp = input.timestamp
	}
}
func (s *Summary) Refuse()       { s.observed = true; s.qualified = false }
func (s Summary) Observed() bool { return s.observed }

// Merge carries pre-compaction observations into the same real candidate.
// It cannot overwrite a previously unqualified or differently owned input.
func (s *Summary) Merge(in Summary) {
	if !in.observed {
		return
	}
	if !s.observed {
		*s = in
		return
	}
	s.qualified = s.qualified && in.qualified && s.owner == in.owner && s.floorOnly == in.floorOnly
	if in.minTimestamp < s.minTimestamp {
		s.minTimestamp = in.minTimestamp
	}
}

// Issue replaces the individual Store owner, invalidating input from the previous Store. No registration or publication history grows.
func (a *Authority) Issue() *Capability {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if a.owner != nil {
		a.invalidateLocked()
		a.owner.closed = true
	}
	c := &Capability{authority: a}
	a.owner = c
	return c
}

func (a *Authority) invalidateLocked() {
	if a.foreign == ^uint64(0) {
		a.closed = true
		return
	}
	a.foreign++
}

func (c *Capability) Input(key []byte, timestamp, floor uint64) Input {
	return Input{owner: c, key: key, timestamp: timestamp, floor: floor}
}

// Cut is a short ordinary mutation cut, under the real producer's publication
// exclusion. It must end before that producer releases its publication locks.
// It is neither prepared output nor authority that may survive a return.
type Cut struct {
	authority           *Authority
	qualified, observed bool
}

// Begin precedes the producer's non-fallible accepted postimage. Observe is
// performed before PONR; Commit does no validation or allocation after PONR.
func (a *Authority) Begin() Cut {
	a.mu.Lock()
	return Cut{authority: a, qualified: !a.closed}
}

func (c *Cut) Observe(key []byte, input Input) {
	a := c.authority
	if a == nil {
		return
	}
	c.observed = true
	owner := input.owner
	valid := inputMatchesKey(key, input) &&
		owner.authority == a && !owner.closed && a.owner == owner
	c.qualified = c.qualified && valid
}

// ObserveSummary consumes exact pre-compaction input under the live owner lock.
func (c *Cut) ObserveSummary(s Summary) {
	if c.authority == nil || !s.observed {
		return
	}
	c.observed = true
	owner := s.owner
	c.qualified = c.qualified && s.qualified && owner != nil &&
		owner.authority == c.authority && !owner.closed && c.authority.owner == owner
}

// Refuse records an actual non-Store operation in this accepted group.
func (c *Cut) Refuse() { c.observed = true; c.qualified = false }

// Commit changes only sticky terminal authority and unlocks. Even a legal
// publication after a foreign one cannot restore an earlier input epoch.
func (c *Cut) Commit() {
	a := c.authority
	if a == nil {
		return
	}
	if c.observed && !c.qualified {
		a.invalidateLocked()
	}
	c.authority = nil
	a.mu.Unlock()
}
func (c *Cut) Abort() {
	a := c.authority
	if a == nil {
		return
	}
	c.authority = nil
	a.mu.Unlock()
}

func (c *Capability) Close() {
	if c == nil || c.authority == nil {
		return
	}
	a := c.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	if a.owner == c {
		a.invalidateLocked()
		a.owner = nil
	}
}
func (a *Authority) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		a.invalidateLocked()
		a.closed = true
	}
	if a.owner != nil {
		a.owner.closed = true
		a.owner = nil
	}
}
