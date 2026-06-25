// Package commandwalapply exposes the narrow internal boundary between future
// R3a deterministic-entry lowering and TreeDB's local command WAL.
package commandwalapply

import (
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// RecoverabilityStatus names the local recovery boundary reached by an apply
// step. These are local TreeDB states, not Raft consensus states.
type RecoverabilityStatus string

const (
	StatusLocallyApplied         RecoverabilityStatus = "locally_applied"
	StatusLocallyWALRecoverable  RecoverabilityStatus = "locally_wal_recoverable"
	StatusLocallyRootRecoverable RecoverabilityStatus = "locally_root_recoverable"
)

// LoweredFrameClass identifies which already-classified lowering path produced
// a frame. This spike accepts only the scoped no-op class so it cannot widen the
// replicated command set ahead of the R3a contract and harness PRs.
type LoweredFrameClass uint8

const (
	LoweredFrameClassTestNoop LoweredFrameClass = iota + 1
)

// ApplyMetadata is the explicit metadata slot future R3a apply code will carry
// beside a lowered frame. It is intentionally empty here; this package must not
// define #3038's entry/result/idempotency contract or #3040's apply metadata.
type ApplyMetadata struct{}

// LoweredFrame is the command-WAL payload produced after deterministic-entry
// classification and lowering. It is not a native-wire request and does not
// encode a Raft log entry.
type LoweredFrame struct {
	Class         LoweredFrameClass
	Kind          commitlog.CommandKind
	Scope         commitlog.CommandScope
	PayloadFormat commitlog.PayloadFormat
	Payload       []byte
}

// Options controls local command-WAL durability for this apply boundary.
type Options struct {
	Sync bool
}

// Handle is the append token required to publish the same local command-WAL
// frame after the normal executor has made the command locally visible.
type Handle struct {
	intent *backenddb.CommandWALIntent
	lsn    uint64
}

// LSN returns the local command-WAL sequence number assigned at append time.
func (h Handle) LSN() uint64 {
	return h.lsn
}

// Result describes the local apply/recoverability boundary reached.
type Result struct {
	LSN               uint64
	Status            RecoverabilityStatus
	AppliedCommandLSN uint64
}

// TestNoopFrame returns the only frame class accepted by this issue's spike: a
// canonical empty RawKVBatch command. It exercises append/finalize without
// accepting user mutations or bypassing collection/catalog executors.
func TestNoopFrame() (LoweredFrame, error) {
	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		return LoweredFrame{}, err
	}
	return LoweredFrame{
		Class:         LoweredFrameClassTestNoop,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
		Payload:       payload,
	}, nil
}

// Append validates and appends a lowered local command-WAL frame. It does not
// publish roots or AppliedCommandLSN; callers must run the normal executor and
// then call Finalize with the returned handle.
func Append(db *backenddb.DB, frame LoweredFrame, _ ApplyMetadata, opts Options) (Handle, Result, error) {
	if db == nil {
		return Handle{}, Result{}, backenddb.ErrClosed
	}
	if !db.CommandWALEnabled() {
		return Handle{}, Result{}, fmt.Errorf("%w: command wal apply requires command_wal_v1", backenddb.ErrCommandWALUnsupported)
	}
	if err := validateLoweredFrame(frame); err != nil {
		return Handle{}, Result{}, err
	}
	intent, err := db.NewCommandWALIntent(frame.Kind, frame.Scope, frame.PayloadFormat, frame.Payload)
	if err != nil {
		return Handle{}, Result{}, err
	}
	if intent == nil {
		return Handle{}, Result{}, fmt.Errorf("%w: command wal apply intent unavailable", backenddb.ErrCommandWALUnsupported)
	}
	lsn, err := db.AppendCommandWALIntent(intent, opts.Sync)
	if err != nil {
		return Handle{}, Result{}, err
	}
	applied := uint64(0)
	if state := db.State(); state != nil {
		applied = state.AppliedCommandLSN
	}
	return Handle{intent: intent, lsn: lsn}, Result{
		LSN:               lsn,
		Status:            StatusLocallyWALRecoverable,
		AppliedCommandLSN: applied,
	}, nil
}

// Finalize publishes the current roots with AppliedCommandLSN covering the
// command frame represented by handle. The no-op frame used by this spike has no
// mutation effects, but the publish path is the same root/AppliedLSN boundary
// future executor-backed commands must use.
func Finalize(db *backenddb.DB, handle Handle, _ ApplyMetadata, opts Options) (Result, error) {
	if db == nil {
		return Result{}, backenddb.ErrClosed
	}
	if handle.intent == nil || handle.lsn == 0 {
		return Result{}, fmt.Errorf("%w: command wal apply finalize missing appended frame", backenddb.ErrCommandWALRejected)
	}
	if err := db.PublishCommandWALNoop(handle.intent, opts.Sync); err != nil {
		return Result{}, err
	}
	applied := uint64(0)
	if state := db.State(); state != nil {
		applied = state.AppliedCommandLSN
	}
	return Result{
		LSN:               handle.lsn,
		Status:            StatusLocallyRootRecoverable,
		AppliedCommandLSN: applied,
	}, nil
}

// ApplyNoop appends and finalizes the scoped no-op frame. It exists only to
// test the boundary end-to-end without adding accepted replicated commands.
func ApplyNoop(db *backenddb.DB, frame LoweredFrame, meta ApplyMetadata, opts Options) (Result, error) {
	handle, _, err := Append(db, frame, meta, opts)
	if err != nil {
		return Result{}, err
	}
	return Finalize(db, handle, meta, opts)
}

func validateLoweredFrame(frame LoweredFrame) error {
	switch frame.Class {
	case LoweredFrameClassTestNoop:
		return validateTestNoopFrame(frame)
	default:
		return fmt.Errorf("%w: command wal apply frame class %d is not accepted", backenddb.ErrCommandWALUnsupported, frame.Class)
	}
}

func validateTestNoopFrame(frame LoweredFrame) error {
	if frame.Kind != commitlog.CommandKindRawKVBatch ||
		frame.Scope != commitlog.CommandScopeRawKV ||
		frame.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		return fmt.Errorf("%w: command wal apply test frame has unsupported identity kind=%d scope=%d format=%d", backenddb.ErrCommandWALUnsupported, frame.Kind, frame.Scope, frame.PayloadFormat)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(frame.Payload)
	if err != nil {
		return fmt.Errorf("%w: malformed command wal apply test frame: %v", backenddb.ErrCommandWALRejected, err)
	}
	if len(ops) != 0 {
		return fmt.Errorf("%w: command wal apply test frame must be an empty RawKVBatch", backenddb.ErrCommandWALUnsupported)
	}
	return nil
}
