package mvccadmission

import (
	"bytes"
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// LogicalMutation is a borrowed, typed Store operand. Timestamp is the
// application timestamp, not an EntryRevision or a publication sequence.
// Delete denotes an MVCC tombstone; a nil or empty Value otherwise is present.
// The producer must own any bytes retained after its synchronous return.
type LogicalMutation struct {
	Key, Value []byte
	Timestamp  uint64
	Delete     bool
}

// LogicalInput binds one Store's admitted floor to the exact borrowed batch.
// It grants no publication identity and owns neither storage nor a read cut.
// A queued producer must BindOwned before it can outlive the caller's borrow.
type LogicalInput struct {
	owner     *Capability
	mutations []LogicalMutation
	floor     uint64
}

func (c *Capability) LogicalInput(mutations []LogicalMutation, floor uint64) LogicalInput {
	return LogicalInput{owner: c, mutations: mutations, floor: floor}
}

func (i LogicalInput) Present() bool { return i.owner != nil }
func (i LogicalInput) Floor() uint64 { return i.floor }

// Matches validates the exact borrowed operands, not live authority. The
// producer still validates the original owner under its actual publication Cut.
func (i LogicalInput) Matches(mutations []LogicalMutation) bool { return i.matches(mutations) }

// LogicalTimestamp exposes exact pre-compaction Store input to a native
// producer. Key shape alone cannot grant this qualification. This method does
// not certify a live owner; the ordinary accepted Cut still consumes the input.
func (i Input) LogicalTimestamp(key []byte) (uint64, bool) {
	if i.floorTransition || !inputMatchesKey(key, i) {
		return 0, false
	}
	return i.timestamp, true
}

func sameLogicalMutation(a, b LogicalMutation) bool {
	return a.Timestamp == b.Timestamp && a.Delete == b.Delete &&
		bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
}

func (i LogicalInput) matches(mutations []LogicalMutation) bool {
	if i.owner == nil || len(mutations) == 0 || len(mutations) != len(i.mutations) {
		return false
	}
	for n := range mutations {
		m := mutations[n]
		if m.Timestamp == 0 || m.Timestamp <= i.floor || !sameLogicalMutation(m, i.mutations[n]) {
			return false
		}
		if _, err := mvcckey.EncodedLen(m.Key); err != nil {
			return false
		}
	}
	return true
}

// BindOwned validates the original operands before transferring their borrow
// to an independently owned equal batch. Substitution stays unqualified.
// The caller must own both the slice and every retained Key/Value backing array.
func (i LogicalInput) BindOwned(owned []LogicalMutation) LogicalInput {
	if !i.matches(owned) {
		return LogicalInput{}
	}
	i.mutations = owned
	return i
}

// BindPhysical projects one admitted operand into the existing canonical
// physical producer. It validates the complete key without decoding or
// allocating. The returned Input borrows physical until publication; a queue
// must apply the existing Input.BindOwnedKey rule before retaining that key.
func (i LogicalInput) BindPhysical(index int, physical []byte) Input {
	if i.owner == nil || index < 0 || index >= len(i.mutations) {
		return Input{}
	}
	m := i.mutations[index]
	if m.Timestamp == 0 || m.Timestamp <= i.floor {
		return Input{}
	}
	prefix, valid := mvcckey.VersionPrefix(physical)
	if !valid || len(physical)-len(prefix) != mvcckey.TimestampSize ||
		^binary.BigEndian.Uint64(physical[len(prefix):]) != m.Timestamp {
		return Input{}
	}
	var markerStorage [16]byte
	marker := mvcckey.AppendNamespaceLower(markerStorage[:0])
	position := len(marker)
	bodyEnd := len(prefix) - 2
	for _, b := range m.Key {
		if position >= bodyEnd || physical[position] != b {
			return Input{}
		}
		position++
		if b == 0 {
			if position >= bodyEnd || physical[position] != 0xff {
				return Input{}
			}
			position++
		}
	}
	if position != bodyEnd {
		return Input{}
	}
	return Input{owner: i.owner, key: physical, timestamp: m.Timestamp, floor: i.floor}
}

// ObserveLogical records the exact pre-compaction batch without constructing
// physical MVCC keys or value envelopes. The ordinary producer subsequently
// validates this summary under its original Authority cut.
func (s *Summary) ObserveLogical(mutations []LogicalMutation, input LogicalInput) {
	valid := input.matches(mutations)
	minTimestamp := uint64(0)
	for n := range mutations {
		if n == 0 || mutations[n].Timestamp < minTimestamp {
			minTimestamp = mutations[n].Timestamp
		}
	}
	in := Summary{owner: input.owner, observed: true, qualified: valid, minTimestamp: minTimestamp}
	s.Merge(in)
}

// ObserveLogical checks the exact operands and SAME live Store authority before
// the producer's point of no return. False must be handled before installation;
// Commit still makes any accepted unqualified operation sticky, as for Input.
func (c *Cut) ObserveLogical(mutations []LogicalMutation, input LogicalInput) bool {
	if c.authority == nil {
		return false
	}
	owner := input.owner
	valid := input.matches(mutations) && owner.authority == c.authority &&
		!owner.closed && c.authority.owner == owner
	c.observed = true
	c.qualified = c.qualified && valid
	return valid
}
