package mvccadmission

import (
	"bytes"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func TestLogicalInputOriginalAuthorityAndOwnedBinding(t *testing.T) {
	var authority, foreign Authority
	owner := authority.Issue()
	mutations := []LogicalMutation{
		{Key: []byte{'a', 0, 'b'}, Value: []byte("one"), Timestamp: 8},
		{Key: nil, Timestamp: math.MaxUint64, Delete: true},
	}
	input := owner.LogicalInput(mutations, 7)
	owned := make([]LogicalMutation, len(mutations))
	for n, m := range mutations {
		owned[n] = m
		owned[n].Key, owned[n].Value = bytes.Clone(m.Key), bytes.Clone(m.Value)
	}
	bound := input.BindOwned(owned)
	if !bound.Present() || bound.Floor() != 7 {
		t.Fatal("equal owned batch lost admission")
	}
	mutations[0].Key[0] = 'z'
	cut := authority.Begin()
	if !cut.ObserveLogical(owned, bound) {
		t.Fatal("owned proof retained original caller key")
	}
	cut.Commit()
	foreignCut := foreign.Begin()
	if foreignCut.ObserveLogical(owned, bound) {
		t.Fatal("foreign authority accepted proof")
	}
	foreignCut.Abort()
	owner.Close()
	cut = authority.Begin()
	if cut.ObserveLogical(owned, bound) {
		t.Fatal("closed owner accepted proof")
	}
	cut.Abort()
}

func TestLogicalInputRejectsSubstitutionAndFloor(t *testing.T) {
	var authority Authority
	owner := authority.Issue()
	batch := []LogicalMutation{{Key: []byte("key"), Value: []byte("v"), Timestamp: 9}}
	input := owner.LogicalInput(batch, 8)
	for _, substitute := range []LogicalMutation{
		{Key: []byte("other"), Value: []byte("v"), Timestamp: 9},
		{Key: []byte("key"), Value: []byte("x"), Timestamp: 9},
		{Key: []byte("key"), Value: []byte("v"), Timestamp: 10},
		{Key: []byte("key"), Value: []byte("v"), Timestamp: 9, Delete: true},
	} {
		if input.BindOwned([]LogicalMutation{substitute}).Present() {
			t.Fatalf("accepted substituted operand %+v", substitute)
		}
	}
	for _, floor := range []uint64{9, math.MaxUint64} {
		cut := authority.Begin()
		if cut.ObserveLogical(batch, owner.LogicalInput(batch, floor)) {
			t.Fatalf("accepted timestamp at/below floor %d", floor)
		}
		cut.Abort()
	}
	var summary Summary
	summary.ObserveLogical(batch, input)
	summary.Refuse()
	summary.ObserveLogical(batch, input)
	cut := authority.Begin()
	cut.ObserveSummary(summary)
	if cut.qualified {
		t.Fatal("logical observation erased a prior refusal")
	}
	cut.Abort()
}

func TestLogicalInputBindPhysicalExactCanonicalProjection(t *testing.T) {
	var authority Authority
	owner := authority.Issue()
	for _, key := range [][]byte{nil, {}, []byte("key"), {0, 'x', 0, 0xff}} {
		for _, ts := range []uint64{1, math.MaxUint64} {
			batch := []LogicalMutation{{Key: key, Timestamp: ts}}
			input := owner.LogicalInput(batch, 0)
			physical, err := mvcckey.Encode(key, ts)
			if err != nil {
				t.Fatal(err)
			}
			projected := input.BindPhysical(0, physical)
			if !projected.Present() {
				t.Fatalf("rejected canonical key %x timestamp %d", key, ts)
			}
			cut := authority.Begin()
			cut.Observe(physical, projected)
			if !cut.qualified {
				t.Fatal("physical proof did not reach original authority")
			}
			cut.Abort()
			wrongKey, _ := mvcckey.Encode(append(bytes.Clone(key), 'z'), ts)
			if input.BindPhysical(0, wrongKey).Present() || input.BindPhysical(-1, physical).Present() || input.BindPhysical(1, physical).Present() {
				t.Fatal("accepted wrong logical key or operand index")
			}
			corrupt := bytes.Clone(physical)
			corrupt[len(corrupt)-1] ^= 1
			if input.BindPhysical(0, corrupt).Present() {
				t.Fatal("accepted wrong timestamp")
			}
		}
	}
}
