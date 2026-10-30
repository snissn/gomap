// Package recoverytrace provides an opt-in, bounded diagnostic ledger. It has
// no persistence authority, callbacks, file borrows, or output operations.
package recoverytrace

import (
	"sync/atomic"
	"time"
)

const MaxRecords = 4096
const LabelBytes = 160

// Label owns a bounded copy. Truncated makes incomplete names explicit; names
// are diagnostic only and never replace the captured stable identity.
type Label struct {
	Bytes     [LabelBytes]byte
	Length    uint16
	Truncated bool
}

func Text(s string) Label {
	var label Label
	n := copy(label.Bytes[:], s)
	label.Length = uint16(n)
	label.Truncated = n != len(s)
	return label
}
func (label Label) String() string { return string(label.Bytes[:label.Length]) }

type Stage uint8

const (
	Fixture Stage = iota + 1
	StableOpen
	ReplayClassify
	ReplayRepair
	ReplayFrame
	Admission
	DurableFence
	ExternalFlush
	ExternalSync
	Materialize
	IndexSync
	PhysicalSync
	ReopenedClose
	SourceClose
	SnapshotFreeze
	PhysicalCapture
)

type Outcome uint8

const (
	Started Outcome = iota + 1
	Returned
	ReturnedError
	Marked
)

// Frontier is an owned summary of the original canonical frontier. RIDSetDigest
// and cardinality bind the sparse membership; no RID slice is borrowed here.
type Frontier struct {
	Bytes, MaxLSN, MaxRID    uint64
	RIDSetDigest             [32]byte
	RIDCount, RIDMin, RIDMax uint64
}

type Event struct {
	Sequence, Parent                             uint64
	ElapsedNanos                                 int64
	Stage                                        Stage
	Outcome                                      Outcome
	Root, Kind, ResourceID, Path                 Label
	LSN, CommitSeq, Generation                   uint64
	IdentityPlatform                             Label
	IdentityVolume                               uint64
	IdentityObject                               [16]byte
	IdentityGeneration                           uint64
	Required, Certified                          Frontier
	HasCertified, HandlePresent, OwnedOperations bool
}

type slot struct {
	ready atomic.Bool
	event Event
}

// Ledger never overwrites a published slot. Reading concurrently with producers
// is safe after ready's acquire load. A reserved but unpublished slot is omitted,
// not treated as a returned operation. Overflow increments Dropped explicitly.
type Ledger struct {
	slots              []slot
	start              time.Time
	attempted, dropped atomic.Uint64
	installed          atomic.Bool
}

var active atomic.Pointer[Ledger]

func New(capacity int) *Ledger {
	if capacity < 1 || capacity > MaxRecords {
		return nil
	}
	return &Ledger{slots: make([]slot, capacity), start: time.Now()}
}

// Install is single-owner and refuses overlap or reuse. Restore removes only
// this installation. Producers that already loaded the ledger may finish their
// bounded writes after Restore; restore is not a producer join.
func Install(ledger *Ledger) (restore func(), ok bool) {
	if ledger == nil || !ledger.installed.CompareAndSwap(false, true) {
		return nil, false
	}
	if !active.CompareAndSwap(nil, ledger) {
		return nil, false
	}
	return func() { active.CompareAndSwap(ledger, nil) }, true
}

func Active() *Ledger { return active.Load() }

func (ledger *Ledger) Record(event Event) uint64 {
	if ledger == nil {
		return 0
	}
	sequence := ledger.attempted.Add(1)
	if sequence > uint64(len(ledger.slots)) {
		ledger.dropped.Add(1)
		return 0
	}
	event.Sequence = sequence
	event.ElapsedNanos = time.Since(ledger.start).Nanoseconds()
	target := &ledger.slots[sequence-1]
	target.event = event
	target.ready.Store(true)
	return sequence
}

func (ledger *Ledger) End(stage Stage, parent uint64, failed bool) {
	outcome := Returned
	if failed {
		outcome = ReturnedError
	}
	ledger.Record(Event{Stage: stage, Outcome: outcome, Parent: parent})
}

func (ledger *Ledger) Capacity() int     { return len(ledger.slots) }
func (ledger *Ledger) Attempted() uint64 { return ledger.attempted.Load() }
func (ledger *Ledger) Dropped() uint64   { return ledger.dropped.Load() }
func (ledger *Ledger) Read(index int) (Event, bool) {
	if index < 0 || index >= len(ledger.slots) || !ledger.slots[index].ready.Load() {
		return Event{}, false
	}
	return ledger.slots[index].event, true
}
