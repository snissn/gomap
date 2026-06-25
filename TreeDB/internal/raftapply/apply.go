package raftapply

import (
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// LocalDurabilityBoundaryV1 names the local durability boundary a committed
// deterministic entry is allowed to use before it becomes locally visible.
type LocalDurabilityBoundaryV1 string

const (
	// LocalDurabilityCommandWALV1 requires a local command-WAL frame before any
	// visible mutation, result/idempotency record, or apply-progress advance.
	LocalDurabilityCommandWALV1 LocalDurabilityBoundaryV1 = "local-command-wal-v1"
)

// ApplyMetadataV1 is explicit per-entry metadata carried beside committed
// deterministic entry bytes. None of these fields are native-wire request
// structs or handler inputs.
type ApplyMetadataV1 struct {
	EntryID                 raftentry.ApplyEntryID
	LocalDurabilityBoundary LocalDurabilityBoundaryV1
	SyncLocalCommandWAL     bool

	ScopeRule       raftentry.ScopeRuleV1
	DatabaseScope   string
	CatalogScope    string
	RequestMetadata raftentry.RequestMetadataV1
	ExpectedTarget  *raftentry.TargetIdentityV1
}

// CommandWALApplySeam is the local command-WAL append/finalize boundary added
// by commandwalapply. Rejection tests inject a counting seam to prove this
// boundary is not called before fail-closed validation completes.
type CommandWALApplySeam interface {
	Append(*backenddb.DB, commandwalapply.LoweredFrame, commandwalapply.ApplyMetadata, commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error)
	Finalize(*backenddb.DB, commandwalapply.Handle, commandwalapply.ApplyMetadata, commandwalapply.Options) (commandwalapply.Result, error)
}

type defaultCommandWALApplySeam struct{}

func (defaultCommandWALApplySeam) Append(db *backenddb.DB, frame commandwalapply.LoweredFrame, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error) {
	return commandwalapply.Append(db, frame, meta, opts)
}

func (defaultCommandWALApplySeam) Finalize(db *backenddb.DB, handle commandwalapply.Handle, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Result, error) {
	return commandwalapply.Finalize(db, handle, meta, opts)
}

// Options wires the harness to deterministic decode limits, fake or durable
// stores, and the command-WAL seam. Nil stores are allowed for early tests but
// mean duplicates/progress cannot be durably recorded.
type Options struct {
	DecodeLimits        nativewire.Limits
	ProgressStore       ApplyProgressStore
	ResultStore         ApplyResultStore
	CommandWALApplySeam CommandWALApplySeam
}

// Harness applies committed deterministic entry bytes to one local DB handle.
type Harness struct {
	db       *backenddb.DB
	opts     Options
	walApply CommandWALApplySeam
}

// NewHarness constructs an R3a apply harness for committed deterministic bytes.
func NewHarness(db *backenddb.DB, opts Options) *Harness {
	walApply := opts.CommandWALApplySeam
	if walApply == nil {
		walApply = defaultCommandWALApplySeam{}
	}
	return &Harness{db: db, opts: opts, walApply: walApply}
}

// ApplyCommittedEntryV1 applies committed deterministic entry bytes through a
// short-lived harness.
func ApplyCommittedEntryV1(db *backenddb.DB, entryBytes []byte, meta ApplyMetadataV1, opts Options) (raftentry.ApplyResultV1, error) {
	return NewHarness(db, opts).ApplyCommittedEntryV1(entryBytes, meta)
}

// ApplyCommittedEntryV1 decodes, validates, classifies, and applies the first
// R3a command slice through local command WAL and normal catalog executors.
func (h *Harness) ApplyCommittedEntryV1(entryBytes []byte, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	if h == nil {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("raftapply: nil harness"))
	}
	if err := h.preflightLocalBoundary(meta); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(raftentry.CommandDigestV1{}, code, err)
	}
	if err := validateApplyEntryID(meta.EntryID); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(raftentry.CommandDigestV1{}, code, err)
	}

	decodeOpts := raftentry.DecodeOptions{
		Limits:          h.opts.DecodeLimits,
		ScopeRule:       meta.ScopeRule,
		DatabaseScope:   meta.DatabaseScope,
		CatalogScope:    meta.CatalogScope,
		ApplyEntryID:    meta.EntryID,
		RequestMetadata: meta.RequestMetadata,
		ExpectedTarget:  meta.ExpectedTarget,
	}
	entry, err := raftentry.DecodeCommandEntryV1(entryBytes, decodeOpts)
	if err != nil {
		code := codeFromDecodeError(err)
		digest, _ := raftentry.ValidateCommandDigestInputV1(entryBytes, decodeOpts)
		return reject(digest, code, err)
	}

	if h.opts.ResultStore != nil {
		record, ok, err := h.opts.ResultStore.LookupApplyResult(meta.EntryID)
		if err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(entry.Digest, code, err)
		}
		if ok {
			if record.CommandDigest != entry.Digest {
				return reject(entry.Digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("raftapply: apply entry %d/%d digest conflicts with existing result", meta.EntryID.Term, meta.EntryID.Index))
			}
			return record.Result, nil
		}
	}
	if h.opts.ProgressStore != nil {
		if err := h.opts.ProgressStore.CheckCanApply(meta.EntryID); err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(entry.Digest, code, err)
		}
	}

	if entry.Target.CommandID != nativewire.CommandCreateCollection {
		return reject(entry.Digest, raftentry.ErrorUnsupportedCommandV1, fmt.Errorf("raftapply: %s is not accepted by create-collection slice", entry.Row.NativeWireCommand))
	}
	if err := h.preflightApplyRecords(meta.EntryID, entry.Digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(entry.Digest, code, err)
	}
	result, err := h.applyCreateCollectionV1(entry, meta)
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(entry.Digest, code, err)
	}
	if h.opts.ResultStore != nil {
		if err := h.opts.ResultStore.RecordApplyResult(ApplyResultRecordV1{
			EntryID:       meta.EntryID,
			CommandDigest: entry.Digest,
			Result:        result,
		}); err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(entry.Digest, code, err)
		}
	}
	if h.opts.ProgressStore != nil {
		if err := h.opts.ProgressStore.RecordApplied(ApplyProgressRecordV1{
			EntryID:       meta.EntryID,
			CommandDigest: entry.Digest,
		}); err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(entry.Digest, code, err)
		}
	}
	return result, nil
}

func (h *Harness) preflightLocalBoundary(meta ApplyMetadataV1) error {
	if meta.LocalDurabilityBoundary != LocalDurabilityCommandWALV1 {
		if meta.LocalDurabilityBoundary == "" {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing local durability boundary")
		}
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "unsupported local durability boundary %q", meta.LocalDurabilityBoundary)
	}
	if h.db == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "nil DB cannot provide local command WAL durability")
	}
	if !h.db.CommandWALEnabled() {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "local command WAL is disabled")
	}
	if err := h.db.CheckStorageMaintenanceReady(); err != nil {
		if errors.Is(err, backenddb.ErrReadOnly) {
			return codedError(raftentry.ErrorReadOnlyV1, "read-only DB rejects deterministic apply")
		}
		if errors.Is(err, backenddb.ErrClosed) {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "closed DB rejects deterministic apply")
		}
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "local command WAL boundary is not writable: %v", err)
	}
	return nil
}

func (h *Harness) preflightApplyRecords(id raftentry.ApplyEntryID, digest raftentry.CommandDigestV1) error {
	if h.opts.ResultStore != nil {
		if err := h.opts.ResultStore.CheckCanRecordApplyResult(ApplyResultRecordV1{
			EntryID:       id,
			CommandDigest: digest,
		}); err != nil {
			return err
		}
	}
	if h.opts.ProgressStore != nil {
		if err := h.opts.ProgressStore.CheckCanRecordApplied(ApplyProgressRecordV1{
			EntryID:       id,
			CommandDigest: digest,
		}); err != nil {
			return err
		}
	}
	return nil
}

func validateApplyEntryID(id raftentry.ApplyEntryID) error {
	if id.Term == 0 || id.Index == 0 {
		return codedError(raftentry.ErrorMalformedEntryV1, "apply entry id must have non-zero term and index")
	}
	return nil
}

func codeFromDecodeError(err error) raftentry.DeterministicErrorCodeV1 {
	if code, ok := raftentry.ErrorCodeOf(err); ok {
		return code
	}
	return raftentry.ErrorMalformedEntryV1
}

func reject(digest raftentry.CommandDigestV1, code raftentry.DeterministicErrorCodeV1, err error) (raftentry.ApplyResultV1, error) {
	if code == "" {
		code = raftentry.ErrorMalformedEntryV1
	}
	result := raftentry.ApplyResultV1{
		Status:                 statusForCode(code),
		CommandDigest:          digest,
		DeterministicErrorCode: code,
	}
	return result, &Error{Code: code, Err: err}
}

func statusForCode(code raftentry.DeterministicErrorCodeV1) raftentry.ApplyStatusV1 {
	switch code {
	case raftentry.ErrorMalformedEntryV1:
		return raftentry.ApplyStatusRejectedMalformed
	case raftentry.ErrorUnsupportedCommandV1,
		raftentry.ErrorUnsupportedVersionV1,
		raftentry.ErrorUnsupportedFeatureV1,
		raftentry.ErrorUnknownRequiredFieldV1,
		raftentry.ErrorUnsupportedScopeRuleV1:
		return raftentry.ApplyStatusRejectedUnsupported
	case raftentry.ErrorRejectedConflictV1:
		return raftentry.ApplyStatusRejectedConflict
	default:
		return raftentry.ApplyStatusDeterministicGuardFailure
	}
}
