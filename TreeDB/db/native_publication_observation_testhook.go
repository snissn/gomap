package db

import (
	"errors"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// NativeInstalledObservationForTest contains only the original publication's
// scalar receipt. It grants no snapshot, seal, retry, or mutation authority.
type NativeInstalledObservationForTest struct {
	AssignedLSN, LogicalSequence                                  uint64
	Consumed, Assigned, LogicalInstalled, WALCompleted, WALStable bool
}

// NativeInstallObserverForTest is an opt-in, singleton diagnostic registration.
// Its mailbox is never closed: a racing publisher may retain this registration
// after Close. Close removes discovery; it does not wait for a publisher.
type NativeInstallObserverForTest struct {
	database *DB
	kind     commitlog.CommandKind
	lsn      uint64
	events   chan NativeInstalledObservationForTest
	dropped  atomic.Bool
}

var nativeInstallObserverForTest atomic.Pointer[NativeInstallObserverForTest]

func NewNativeInstallObserverForTest(database *DB, kind commitlog.CommandKind, lsn uint64) (*NativeInstallObserverForTest, error) {
	if database == nil || lsn == 0 {
		return nil, errors.New("invalid native install observation identity")
	}
	o := &NativeInstallObserverForTest{database: database, kind: kind, lsn: lsn, events: make(chan NativeInstalledObservationForTest, 1)}
	if !nativeInstallObserverForTest.CompareAndSwap(nil, o) {
		return nil, errors.New("native install observer already registered")
	}
	return o, nil
}
func (o *NativeInstallObserverForTest) Events() <-chan NativeInstalledObservationForTest {
	return o.events
}
func (o *NativeInstallObserverForTest) Dropped() bool { return o.dropped.Load() }
func (o *NativeInstallObserverForTest) Close()        { nativeInstallObserverForTest.CompareAndSwap(o, nil) }

// ObserveNativeInstalledForTest is called after real acknowledgement, before
// exact-seal Checkpoint. It never blocks, allocates, calls a callback, inspects a
// transaction, or changes publication results. A full mailbox invalidates the
// witness instead of slowing a producer. Never invoke it as assignment evidence.
func (db *DB) ObserveNativeInstalledForTest(intent *CommandWALIntent, result NativePublicationResult) {
	o := nativeInstallObserverForTest.Load()
	if o == nil || o.database != db || intent == nil || intent.inner.kind != o.kind || result.AssignedLSN != o.lsn ||
		!result.Consumed || !result.Assigned || !result.LogicalInstalled || result.Uncertain || result.LogicalSequence == 0 {
		return
	}
	event := NativeInstalledObservationForTest{AssignedLSN: result.AssignedLSN, LogicalSequence: result.LogicalSequence, Consumed: result.Consumed, Assigned: result.Assigned, LogicalInstalled: result.LogicalInstalled, WALCompleted: result.WALCompleted, WALStable: result.WALStable}
	select {
	case o.events <- event:
	default:
		o.dropped.Store(true)
	}
}
