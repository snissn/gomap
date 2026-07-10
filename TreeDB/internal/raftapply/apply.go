package raftapply

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
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

type FaultPointV1 string

const (
	FaultBeforeLocalWALAppendV1             FaultPointV1 = "before-local-wal-append-v1"
	FaultAfterLocalWALAppendBeforeVisibleV1 FaultPointV1 = "after-local-wal-append-before-visible-v1"
	FaultAfterVisibleBeforeResultRecordV1   FaultPointV1 = "after-visible-before-result-record-v1"
	FaultAfterResultRecordBeforeProgressV1  FaultPointV1 = "after-result-record-before-progress-v1"
	FaultAfterProgressRecordV1              FaultPointV1 = "after-progress-record-v1"
)

type ApplyFaultContextV1 struct {
	EntryID           raftentry.ApplyEntryID
	CommandDigest     raftentry.CommandDigestV1
	AppliedCommandLSN uint64
}

type FaultInjector interface {
	InjectApplyFault(FaultPointV1, ApplyFaultContextV1) error
}

// ApplyMetadataV1 is explicit per-entry metadata carried beside committed
// deterministic entry bytes. None of these fields are native-wire request
// structs or handler inputs.
type ApplyMetadataV1 struct {
	EntryID                 raftentry.ApplyEntryID
	LocalDurabilityBoundary LocalDurabilityBoundaryV1
	SyncLocalCommandWAL     bool

	// CurrentCatalogVersion is the caller-observed local catalog version used
	// to enforce deterministic expected-catalog-version guards before mutation.
	CurrentCatalogVersion    uint64
	HasCurrentCatalogVersion bool

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
	Abort(*backenddb.DB, commandwalapply.Handle)
}

type defaultCommandWALApplySeam struct{}

func (defaultCommandWALApplySeam) Append(db *backenddb.DB, frame commandwalapply.LoweredFrame, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error) {
	return commandwalapply.Append(db, frame, meta, opts)
}

func (defaultCommandWALApplySeam) Finalize(db *backenddb.DB, handle commandwalapply.Handle, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Result, error) {
	return commandwalapply.Finalize(db, handle, meta, opts)
}

func (defaultCommandWALApplySeam) Abort(db *backenddb.DB, handle commandwalapply.Handle) {
	commandwalapply.Abort(db, handle)
}

// Options wires the harness to deterministic decode limits, fake or durable
// stores, and the command-WAL seam. Nil stores are allowed for early tests but
// mean duplicates/progress cannot be durably recorded.
type Options struct {
	DecodeLimits        nativewire.Limits
	ProgressStore       ApplyProgressStore
	ResultStore         ApplyResultStore
	CommandWALApplySeam CommandWALApplySeam
	FaultInjector       FaultInjector
}

// Harness applies committed deterministic entry bytes to one local DB handle.
type Harness struct {
	db                *backenddb.DB
	opts              Options
	walApply          CommandWALApplySeam
	collectionManager *collections.CollectionManager
	logicalDigestV1Fn func(LogicalDigestOptionsV1) (LogicalDigestV1, error)
}

// NewHarness constructs an R3a apply harness for committed deterministic bytes.
func NewHarness(db *backenddb.DB, opts Options) *Harness {
	walApply := opts.CommandWALApplySeam
	if walApply == nil {
		walApply = defaultCommandWALApplySeam{}
	}
	var manager *collections.CollectionManager
	if db != nil {
		manager = collections.NewCommandWALReplayCollectionManager(db)
	}
	return &Harness{db: db, opts: opts, walApply: walApply, collectionManager: manager}
}

func (h *Harness) replayCollectionManager() *collections.CollectionManager {
	if h == nil {
		return nil
	}
	if h.collectionManager != nil {
		return h.collectionManager
	}
	if h.db == nil {
		return nil
	}
	return collections.NewCommandWALReplayCollectionManager(h.db)
}

// ApplyCommittedEntryV1 applies committed deterministic entry bytes through a
// short-lived harness.
func ApplyCommittedEntryV1(db *backenddb.DB, entryBytes []byte, meta ApplyMetadataV1, opts Options) (raftentry.ApplyResultV1, error) {
	return NewHarness(db, opts).ApplyCommittedEntryV1(entryBytes, meta)
}

// PreflightCommandEntryV1 validates deterministic/catalog apply acceptability
// without assigning, recording, or advancing an apply entry ID.
func PreflightCommandEntryV1(db *backenddb.DB, entryBytes []byte, meta ApplyMetadataV1, opts Options) (PreflightResultV1, error) {
	return NewHarness(db, opts).PreflightCommandEntryV1(entryBytes, meta)
}

// PreflightDecodedCommandEntryV1 validates an entry that the submitter already
// decoded with the same deterministic limits used for this apply boundary.
func PreflightDecodedCommandEntryV1(db *backenddb.DB, entry raftentry.CommandEntryV1, meta ApplyMetadataV1, opts Options) (PreflightResultV1, error) {
	return NewHarness(db, opts).PreflightDecodedCommandEntryV1(entry, meta)
}

type PreflightResultV1 struct {
	KnownIdempotencyReplay bool
}

// PreflightCommandEntryV1 decodes and checks the local deterministic boundary
// a command would need to pass before visible apply. It does not append local
// command-WAL frames, record idempotency/results, or advance apply progress.
func (h *Harness) PreflightCommandEntryV1(entryBytes []byte, meta ApplyMetadataV1) (PreflightResultV1, error) {
	if h == nil {
		return PreflightResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil harness")
	}
	if err := h.preflightLocalBoundary(meta); err != nil {
		return PreflightResultV1{}, err
	}
	entry, err := raftentry.DecodeCommandEntryV1(entryBytes, raftentry.DecodeOptions{
		Limits:          h.opts.DecodeLimits,
		ScopeRule:       meta.ScopeRule,
		DatabaseScope:   meta.DatabaseScope,
		CatalogScope:    meta.CatalogScope,
		RequestMetadata: meta.RequestMetadata,
		ExpectedTarget:  meta.ExpectedTarget,
	})
	if err != nil {
		return PreflightResultV1{}, err
	}
	return h.preflightDecodedCommandEntryV1(entry, meta)
}

// PreflightDecodedCommandEntryV1 checks a caller-provided decoded entry against
// the local deterministic boundary without re-decoding entry bytes.
func (h *Harness) PreflightDecodedCommandEntryV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (PreflightResultV1, error) {
	if h == nil {
		return PreflightResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil harness")
	}
	if err := h.preflightLocalBoundary(meta); err != nil {
		return PreflightResultV1{}, err
	}
	return h.preflightDecodedCommandEntryV1(entry, meta)
}

func (h *Harness) preflightDecodedCommandEntryV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (PreflightResultV1, error) {
	if ok, err := h.preflightKnownIdempotencyReplayV1(entry); err != nil || ok {
		return PreflightResultV1{KnownIdempotencyReplay: ok}, err
	}
	switch entry.Target.CommandID {
	case nativewire.CommandCreateCollection:
		expectedCatalogVersion, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
		if err != nil {
			return PreflightResultV1{}, err
		}
		collectionMeta, payload, err := lowerCreateCollectionV1(entry)
		if err != nil {
			return PreflightResultV1{}, err
		}
		alreadyExisting, err := h.preflightCreateCollectionV1(collectionMeta, payload)
		if err != nil {
			return PreflightResultV1{}, err
		}
		if !alreadyExisting {
			if err := checkCatalogVersionGuardV1(meta, expectedCatalogVersion); err != nil {
				return PreflightResultV1{}, err
			}
		}
		return PreflightResultV1{}, nil
	case nativewire.CommandInsertBatch, nativewire.CommandReplaceBatch, nativewire.CommandDeleteBatch, nativewire.CommandUpdateBSONSet:
		expectedCatalogVersion, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
		if err != nil {
			return PreflightResultV1{}, err
		}
		if err := checkCatalogVersionGuardV1(meta, expectedCatalogVersion); err != nil {
			return PreflightResultV1{}, err
		}
		mutation, err := lowerCollectionMutationV1(entry, h.opts.DecodeLimits)
		if err != nil {
			return PreflightResultV1{}, err
		}
		if err := h.preflightCollectionMutationOnlyV1(&mutation); err != nil {
			return PreflightResultV1{}, err
		}
		return PreflightResultV1{}, nil
	default:
		return PreflightResultV1{}, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: %s is not accepted by R3a apply", entry.Row.NativeWireCommand)
	}
}

func (h *Harness) preflightKnownIdempotencyReplayV1(entry raftentry.CommandEntryV1) (bool, error) {
	if h == nil || h.opts.ResultStore == nil || len(entry.IdempotencyKey) == 0 {
		return false, nil
	}
	record, ok, err := h.opts.ResultStore.LookupApplyResultByIdempotencyKey(entry.IdempotencyKey)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if record.CommandDigest != entry.Digest {
		return false, codedError(raftentry.ErrorRejectedConflictV1, "raftapply: preflight idempotency key conflicts with existing result")
	}
	if err := h.requireApplyRecordCoverage(record.AppliedCommandLSN); err != nil {
		return false, err
	}
	return true, nil
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
			if err := h.requireApplyRecordCoverage(record.AppliedCommandLSN); err != nil {
				code, _ := ErrorCodeOf(err)
				return recoveryRequired(entry.Digest, code, err)
			}
			if h.opts.ProgressStore != nil {
				progressDigest := record.ProgressLogicalDigestV1
				if progressDigest == (LogicalDigestV1{}) {
					progressDigest = LogicalDigestV1(record.Result.ResultDigest)
				}
				if err := h.opts.ProgressStore.RecordApplied(ApplyProgressRecordV1{
					EntryID:           meta.EntryID,
					CommandDigest:     entry.Digest,
					AppliedCommandLSN: record.AppliedCommandLSN,
					LogicalDigestV1:   progressDigest,
				}); err != nil {
					code, _ := ErrorCodeOf(err)
					return recoveryRequired(entry.Digest, code, err)
				}
			}
			return record.Result, nil
		}
		record, ok, err = h.opts.ResultStore.LookupApplyResultByIdempotencyKey(entry.IdempotencyKey)
		if err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(entry.Digest, code, err)
		}
		if ok {
			if record.CommandDigest != entry.Digest {
				return reject(entry.Digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("raftapply: idempotency key conflicts with existing result for apply entry %d/%d", meta.EntryID.Term, meta.EntryID.Index))
			}
			if err := h.requireApplyRecordCoverage(record.AppliedCommandLSN); err != nil {
				code, _ := ErrorCodeOf(err)
				return recoveryRequired(entry.Digest, code, err)
			}
			if err := h.preflightApplyRecords(meta.EntryID, entry.Digest, entry.IdempotencyKey); err != nil {
				code, _ := ErrorCodeOf(err)
				return reject(entry.Digest, code, err)
			}
			duplicateLSN := h.appliedCommandLSN()
			if duplicateLSN < record.AppliedCommandLSN {
				err := codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: duplicate apply metadata coverage %d trails original result coverage %d", duplicateLSN, record.AppliedCommandLSN)
				code, _ := ErrorCodeOf(err)
				return recoveryRequired(entry.Digest, code, err)
			}
			logical, err := h.logicalDigestForProgressV1(meta)
			if err != nil {
				code, _ := ErrorCodeOf(err)
				return recoveryRequired(entry.Digest, code, err)
			}
			duplicate := record.Result
			duplicate.Status = raftentry.ApplyStatusAlreadyApplied
			duplicate.AffectedCount = 0
			duplicate.MatchedCount = 0
			if err := h.opts.ResultStore.RecordApplyResult(ApplyResultRecordV1{
				EntryID:                 meta.EntryID,
				CommandDigest:           entry.Digest,
				IdempotencyKey:          entry.IdempotencyKey,
				AppliedCommandLSN:       duplicateLSN,
				ProgressLogicalDigestV1: logical,
				Result:                  duplicate,
			}); err != nil {
				code, _ := ErrorCodeOf(err)
				return recoveryRequired(entry.Digest, code, err)
			}
			if h.opts.ProgressStore != nil {
				if err := h.opts.ProgressStore.RecordApplied(ApplyProgressRecordV1{
					EntryID:           meta.EntryID,
					CommandDigest:     entry.Digest,
					AppliedCommandLSN: duplicateLSN,
					LogicalDigestV1:   logical,
				}); err != nil {
					code, _ := ErrorCodeOf(err)
					return recoveryRequired(entry.Digest, code, err)
				}
			}
			return duplicate, nil
		}
	}
	if h.opts.ProgressStore != nil {
		if err := h.opts.ProgressStore.CheckCanApply(meta.EntryID); err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(entry.Digest, code, err)
		}
	}

	if err := h.preflightApplyRecords(meta.EntryID, entry.Digest, entry.IdempotencyKey); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(entry.Digest, code, err)
	}
	if err := h.injectFault(FaultBeforeLocalWALAppendV1, meta.EntryID, entry.Digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(entry.Digest, code, err)
	}
	var result raftentry.ApplyResultV1
	switch entry.Target.CommandID {
	case nativewire.CommandCreateCollection:
		result, err = h.applyCreateCollectionV1(entry, meta)
	case nativewire.CommandInsertBatch, nativewire.CommandReplaceBatch, nativewire.CommandDeleteBatch, nativewire.CommandUpdateBSONSet:
		result, err = h.applyCollectionMutationV1(entry, meta)
	default:
		return reject(entry.Digest, raftentry.ErrorUnsupportedCommandV1, fmt.Errorf("raftapply: %s is not accepted by R3a apply", entry.Row.NativeWireCommand))
	}
	if err != nil {
		if result.Status == raftentry.ApplyStatusRecoveryRequired {
			return result, err
		}
		code, _ := ErrorCodeOf(err)
		return reject(entry.Digest, code, err)
	}
	appliedLSN := h.appliedCommandLSN()
	if err := h.requireApplyRecordCoverage(appliedLSN); err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	if err := h.injectFault(FaultAfterVisibleBeforeResultRecordV1, meta.EntryID, entry.Digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	if h.opts.ResultStore != nil {
		if err := h.opts.ResultStore.RecordApplyResult(ApplyResultRecordV1{
			EntryID:                 meta.EntryID,
			CommandDigest:           entry.Digest,
			IdempotencyKey:          entry.IdempotencyKey,
			AppliedCommandLSN:       appliedLSN,
			ProgressLogicalDigestV1: LogicalDigestV1(result.ResultDigest),
			Result:                  result,
		}); err != nil {
			code, _ := ErrorCodeOf(err)
			return recoveryRequired(entry.Digest, code, err)
		}
	}
	if err := h.injectFault(FaultAfterResultRecordBeforeProgressV1, meta.EntryID, entry.Digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	if h.opts.ProgressStore != nil {
		if err := h.opts.ProgressStore.RecordApplied(ApplyProgressRecordV1{
			EntryID:           meta.EntryID,
			CommandDigest:     entry.Digest,
			AppliedCommandLSN: appliedLSN,
			LogicalDigestV1:   LogicalDigestV1(result.ResultDigest),
		}); err != nil {
			code, _ := ErrorCodeOf(err)
			return recoveryRequired(entry.Digest, code, err)
		}
	}
	if err := h.injectFault(FaultAfterProgressRecordV1, meta.EntryID, entry.Digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
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

func (h *Harness) preflightApplyRecords(id raftentry.ApplyEntryID, digest raftentry.CommandDigestV1, idempotencyKey []byte) error {
	if h.opts.ResultStore != nil {
		if err := h.opts.ResultStore.CheckCanRecordApplyResult(ApplyResultRecordV1{
			EntryID:        id,
			CommandDigest:  digest,
			IdempotencyKey: idempotencyKey,
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

func (h *Harness) appliedCommandLSN() uint64 {
	if h == nil || h.db == nil {
		return 0
	}
	state, ok := h.db.StateToken()
	if !ok {
		return 0
	}
	return state.AppliedCommandLSN
}

func (h *Harness) logicalDigestForProgressV1(meta ApplyMetadataV1) (LogicalDigestV1, error) {
	return h.logicalDigestV1(LogicalDigestOptionsV1{
		ScopeRule:     meta.ScopeRule,
		DatabaseScope: meta.DatabaseScope,
		CatalogScope:  meta.CatalogScope,
	})
}

func (h *Harness) requireApplyRecordCoverage(appliedLSN uint64) error {
	if appliedLSN == 0 {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: apply metadata has no local AppliedCommandLSN coverage")
	}
	if h != nil && h.db != nil {
		state, ok := h.db.StateToken()
		if !ok || state.AppliedCommandLSN < appliedLSN {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: apply metadata AppliedCommandLSN %d outruns local coverage %d", appliedLSN, state.AppliedCommandLSN)
		}
	}
	return nil
}

func (h *Harness) injectFault(point FaultPointV1, id raftentry.ApplyEntryID, digest raftentry.CommandDigestV1) error {
	if h == nil || h.opts.FaultInjector == nil {
		return nil
	}
	if err := h.opts.FaultInjector.InjectApplyFault(point, ApplyFaultContextV1{
		EntryID:           id,
		CommandDigest:     digest,
		AppliedCommandLSN: h.appliedCommandLSN(),
	}); err != nil {
		if _, ok := ErrorCodeOf(err); ok {
			return err
		}
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: injected fault at %s: %v", point, err)
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

func recoveryRequired(digest raftentry.CommandDigestV1, code raftentry.DeterministicErrorCodeV1, err error) (raftentry.ApplyResultV1, error) {
	if code == "" {
		code = raftentry.ErrorUnsafeDurabilityModeV1
	}
	result := raftentry.ApplyResultV1{
		Status:                 raftentry.ApplyStatusRecoveryRequired,
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
