// Package commandwalapply exposes the narrow internal boundary between future
// R3a deterministic-entry lowering and TreeDB's local command WAL.
package commandwalapply

import (
	"bytes"
	"fmt"
	"sync"

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
// a frame. Classes are intentionally explicit so future command widening cannot
// accidentally piggyback on a generic command-WAL append path.
type LoweredFrameClass uint8

const (
	LoweredFrameClassTestNoop LoweredFrameClass = iota + 1
	LoweredFrameClassCatalogCreateCollection
	LoweredFrameClassCollectionInsertBatchByID
	LoweredFrameClassCollectionDeleteBatchByID
	LoweredFrameClassCollectionUpdateBatchByID
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
	db      *backenddb.DB
	intent  *backenddb.CommandWALIntent
	lsn     uint64
	staging *stagingGuard
}

// LSN returns the local command-WAL sequence number assigned at append time.
func (h Handle) LSN() uint64 {
	return h.lsn
}

// CommandWALIntent returns the pre-appended local command-WAL intent. Normal
// executors use it to publish roots and AppliedCommandLSN for the same frame.
func (h Handle) CommandWALIntent() *backenddb.CommandWALIntent {
	return h.intent
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

// CatalogCreateCollectionFrame returns the first accepted catalog mutation
// frame for R3a apply. Payload must be the canonical commitlog
// CatalogCreateCollection v1 payload produced by the collections catalog path.
func CatalogCreateCollectionFrame(payload []byte) (LoweredFrame, error) {
	frame := LoweredFrame{
		Class:         LoweredFrameClassCatalogCreateCollection,
		Kind:          commitlog.CommandKindCatalogCreateCollection,
		Scope:         commitlog.CommandScopeCatalog,
		PayloadFormat: commitlog.PayloadFormatCatalogCreateCollectionV1,
		Payload:       payload,
	}
	if err := validateCatalogCreateCollectionFrame(frame); err != nil {
		return LoweredFrame{}, err
	}
	return frame, nil
}

// CollectionInsertBatchByIDFrame returns the accepted collection insert
// mutation frame. Payload must be the canonical command-WAL payload produced
// from deterministic document IDs and documents, without local physical IDs.
func CollectionInsertBatchByIDFrame(payload []byte) (LoweredFrame, error) {
	frame := LoweredFrame{
		Class:         LoweredFrameClassCollectionInsertBatchByID,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
		Payload:       payload,
	}
	if err := validateCollectionInsertBatchByIDFrame(frame); err != nil {
		return LoweredFrame{}, err
	}
	return frame, nil
}

// CollectionDeleteBatchByIDFrame returns the accepted collection delete
// mutation frame. Payload must be the canonical command-WAL payload produced
// from deterministic document IDs.
func CollectionDeleteBatchByIDFrame(payload []byte) (LoweredFrame, error) {
	frame := LoweredFrame{
		Class:         LoweredFrameClassCollectionDeleteBatchByID,
		Kind:          commitlog.CommandKindCollectionDeleteBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionDeleteBatchByIDV1,
		Payload:       payload,
	}
	if err := validateCollectionDeleteBatchByIDFrame(frame); err != nil {
		return LoweredFrame{}, err
	}
	return frame, nil
}

// CollectionUpdateBatchByIDFrame returns the accepted collection replacement
// mutation frame. Payload must be the canonical command-WAL payload containing
// final replacement documents keyed by deterministic document IDs.
func CollectionUpdateBatchByIDFrame(payload []byte) (LoweredFrame, error) {
	frame := LoweredFrame{
		Class:         LoweredFrameClassCollectionUpdateBatchByID,
		Kind:          commitlog.CommandKindCollectionUpdateBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionUpdateBatchByIDV1,
		Payload:       payload,
	}
	if err := validateCollectionUpdateBatchByIDFrame(frame); err != nil {
		return LoweredFrame{}, err
	}
	return frame, nil
}

// Append validates and appends a lowered local command-WAL frame. It does not
// publish roots or AppliedCommandLSN; callers must run the normal executor and
// then call Finalize with the returned handle.
func Append(db *backenddb.DB, frame LoweredFrame, _ ApplyMetadata, opts Options) (Handle, Result, error) {
	if db == nil {
		return Handle{}, Result{}, backenddb.ErrClosed
	}
	if !db.CommandWALEnabled() {
		return Handle{}, Result{}, fmt.Errorf("%w: command wal apply requires command_wal_v2", backenddb.ErrCommandWALUnsupported)
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
	applyAppendMu.Lock()
	defer applyAppendMu.Unlock()
	applied := appliedCommandLSN(db)
	if err := checkContiguousAppendReady(db, applied); err != nil {
		return Handle{}, Result{}, err
	}
	staging := newStagingGuard(db.LockCommandWALStaging())
	applied = appliedCommandLSN(db)
	if err := checkContiguousAppendReady(db, applied); err != nil {
		staging.release()
		return Handle{}, Result{}, err
	}
	lsn, err := db.AppendStagedCommandWALIntent(intent, opts.Sync)
	if err != nil {
		staging.release()
		return Handle{}, Result{}, err
	}
	return Handle{db: db, intent: intent, lsn: lsn, staging: staging}, Result{
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
	if handle.db != db {
		return Result{}, fmt.Errorf("%w: command wal apply finalize handle belongs to a different DB", backenddb.ErrCommandWALRejected)
	}
	if handle.staging != nil {
		defer handle.staging.release()
	}
	if applied := appliedCommandLSN(db); applied >= handle.lsn {
		return Result{
			LSN:               handle.lsn,
			Status:            StatusLocallyRootRecoverable,
			AppliedCommandLSN: applied,
		}, nil
	}
	if err := db.PublishStagedCommandWALNoop(handle.intent, opts.Sync); err != nil {
		return Result{}, err
	}
	applied := appliedCommandLSN(db)
	return Result{
		LSN:               handle.lsn,
		Status:            StatusLocallyRootRecoverable,
		AppliedCommandLSN: applied,
	}, nil
}

// Abort releases an appended apply handle that cannot be finalized by the
// caller. If the frame is still beyond AppliedCommandLSN, the open DB handle is
// poisoned so the next writer fails closed and reopen recovery owns the gap.
func Abort(db *backenddb.DB, handle Handle) {
	if handle.staging != nil {
		defer handle.staging.release()
	}
	if db == nil || handle.db != db || handle.intent == nil || handle.lsn == 0 {
		return
	}
	if appliedCommandLSN(db) < handle.lsn {
		db.MarkCommandWALIntentRecoveryRequired(handle.intent)
	}
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
	case LoweredFrameClassCatalogCreateCollection:
		return validateCatalogCreateCollectionFrame(frame)
	case LoweredFrameClassCollectionInsertBatchByID:
		return validateCollectionInsertBatchByIDFrame(frame)
	case LoweredFrameClassCollectionDeleteBatchByID:
		return validateCollectionDeleteBatchByIDFrame(frame)
	case LoweredFrameClassCollectionUpdateBatchByID:
		return validateCollectionUpdateBatchByIDFrame(frame)
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
	canonical, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		return fmt.Errorf("%w: command wal apply canonical test frame unavailable: %v", backenddb.ErrCommandWALRejected, err)
	}
	if !bytes.Equal(frame.Payload, canonical) {
		return fmt.Errorf("%w: command wal apply test frame must be the canonical empty RawKVBatch", backenddb.ErrCommandWALRejected)
	}
	return nil
}

func validateCatalogCreateCollectionFrame(frame LoweredFrame) error {
	if frame.Kind != commitlog.CommandKindCatalogCreateCollection ||
		frame.Scope != commitlog.CommandScopeCatalog ||
		frame.PayloadFormat != commitlog.PayloadFormatCatalogCreateCollectionV1 {
		return fmt.Errorf("%w: command wal apply catalog create frame has unsupported identity kind=%d scope=%d format=%d", backenddb.ErrCommandWALUnsupported, frame.Kind, frame.Scope, frame.PayloadFormat)
	}
	if _, err := commitlog.DecodeCatalogCreateCollectionPayload(frame.Payload); err != nil {
		return fmt.Errorf("%w: malformed command wal apply catalog create frame: %v", backenddb.ErrCommandWALRejected, err)
	}
	return nil
}

func validateCollectionInsertBatchByIDFrame(frame LoweredFrame) error {
	if frame.Kind != commitlog.CommandKindCollectionInsertBatchByID ||
		frame.Scope != commitlog.CommandScopeCollection ||
		frame.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
		return fmt.Errorf("%w: command wal apply collection insert frame has unsupported identity kind=%d scope=%d format=%d", backenddb.ErrCommandWALUnsupported, frame.Kind, frame.Scope, frame.PayloadFormat)
	}
	if _, err := commitlog.DecodeCollectionInsertBatchByIDPayload(frame.Payload); err != nil {
		return fmt.Errorf("%w: malformed command wal apply collection insert frame: %v", backenddb.ErrCommandWALRejected, err)
	}
	return nil
}

func validateCollectionDeleteBatchByIDFrame(frame LoweredFrame) error {
	if frame.Kind != commitlog.CommandKindCollectionDeleteBatchByID ||
		frame.Scope != commitlog.CommandScopeCollection ||
		frame.PayloadFormat != commitlog.PayloadFormatCollectionDeleteBatchByIDV1 {
		return fmt.Errorf("%w: command wal apply collection delete frame has unsupported identity kind=%d scope=%d format=%d", backenddb.ErrCommandWALUnsupported, frame.Kind, frame.Scope, frame.PayloadFormat)
	}
	if _, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(frame.Payload); err != nil {
		return fmt.Errorf("%w: malformed command wal apply collection delete frame: %v", backenddb.ErrCommandWALRejected, err)
	}
	return nil
}

func validateCollectionUpdateBatchByIDFrame(frame LoweredFrame) error {
	if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID ||
		frame.Scope != commitlog.CommandScopeCollection ||
		frame.PayloadFormat != commitlog.PayloadFormatCollectionUpdateBatchByIDV1 {
		return fmt.Errorf("%w: command wal apply collection update frame has unsupported identity kind=%d scope=%d format=%d", backenddb.ErrCommandWALUnsupported, frame.Kind, frame.Scope, frame.PayloadFormat)
	}
	if _, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload); err != nil {
		return fmt.Errorf("%w: malformed command wal apply collection update frame: %v", backenddb.ErrCommandWALRejected, err)
	}
	return nil
}

var applyAppendMu sync.Mutex

type stagingGuard struct {
	once   sync.Once
	unlock func()
}

func newStagingGuard(unlock func()) *stagingGuard {
	if unlock == nil {
		unlock = func() {}
	}
	return &stagingGuard{unlock: unlock}
}

func (g *stagingGuard) release() {
	if g == nil {
		return
	}
	g.once.Do(g.unlock)
}

func checkContiguousAppendReady(db *backenddb.DB, applied uint64) error {
	next := db.CommandWALNextLSN()
	if next == 0 {
		return fmt.Errorf("%w: command wal apply next LSN unavailable", backenddb.ErrCommandWALUnsupported)
	}
	if next != applied+1 {
		return fmt.Errorf("%w: command wal apply has outstanding frame next_lsn=%d applied_lsn=%d", backenddb.ErrCommandWALRejected, next, applied)
	}
	return nil
}

func appliedCommandLSN(db *backenddb.DB) uint64 {
	if state, ok := db.StateToken(); ok {
		return state.AppliedCommandLSN
	}
	return 0
}
