package raftfsm

import (
	"bytes"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

type EntryTypeV1 string

const (
	EntryTypeCommandEntryV1 EntryTypeV1 = "command-entry-v1"
)

// CommittedEntryV1 is the deterministic payload delivered by the single-group
// Raft log after commitment.
type CommittedEntryV1 struct {
	Type  EntryTypeV1
	Term  uint64
	Index uint64
	Bytes []byte

	// CurrentCatalogVersion is the caller-observed local catalog version for
	// deterministic catalog guards. Missing guard context is left missing so
	// raftapply can fail closed before a visible mutation.
	CurrentCatalogVersion    uint64
	HasCurrentCatalogVersion bool
	SyncLocalCommandWAL      bool
	RequestMetadata          raftentry.RequestMetadataV1
	ExpectedTarget           *raftentry.TargetIdentityV1
}

// Options configures one local single-group FSM instance.
type Options struct {
	DB      *backenddb.DB
	Cluster raftcluster.Config

	DecodeLimits nativewire.Limits
	StoreOptions raftapply.DurableApplyStoreOptions

	ScopeRule     raftentry.ScopeRuleV1
	DatabaseScope string
	CatalogScope  string
}

// FSM applies committed deterministic entries to one local DB.
type FSM struct {
	db          *backenddb.DB
	metadataDir string

	decodeLimits nativewire.Limits
	storeOptions raftapply.DurableApplyStoreOptions
	scopeRule    raftentry.ScopeRuleV1
	database     string
	catalog      string
	cluster      raftcluster.ResolvedConfig

	progress *raftapply.DurableApplyProgressStore
	results  *raftapply.DurableApplyResultStore
	closed   bool
}

type Error struct {
	Code raftentry.DeterministicErrorCodeV1
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return "raftfsm: <nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("raftfsm: %s", e.Code)
	}
	return fmt.Sprintf("raftfsm: %s: %v", e.Code, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func ErrorCodeOf(err error) (raftentry.DeterministicErrorCodeV1, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Code, true
	}
	return raftapply.ErrorCodeOf(err)
}

// Open creates the durable progress/result stores for one single-group FSM.
func Open(opts Options) (*FSM, error) {
	if opts.DB == nil {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "nil DB")
	}
	cluster, err := raftcluster.Validate(opts.Cluster)
	if err != nil {
		return nil, errors.Join(codedError(raftentry.ErrorUnsafeDurabilityModeV1, "invalid raftcluster config"), err)
	}
	metadataDir := cluster.Layout.ApplyDir
	if metadataDir == "" {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing raftcluster apply dir")
	}
	progress, err := raftapply.OpenDurableApplyProgressStore(metadataDir, opts.StoreOptions)
	if err != nil {
		return nil, err
	}
	results, err := raftapply.OpenDurableApplyResultStore(metadataDir, opts.StoreOptions)
	if err != nil {
		_ = progress.Close()
		return nil, err
	}
	return &FSM{
		db:           opts.DB,
		metadataDir:  metadataDir,
		decodeLimits: opts.DecodeLimits,
		storeOptions: opts.StoreOptions,
		scopeRule:    opts.ScopeRule,
		database:     opts.DatabaseScope,
		catalog:      opts.CatalogScope,
		cluster:      cluster,
		progress:     progress,
		results:      results,
	}, nil
}

func (f *FSM) Close() error {
	if f == nil || f.closed {
		return nil
	}
	f.closed = true
	return errors.Join(f.progress.Close(), f.results.Close())
}

func (f *FSM) LastApplied() (raftentry.ApplyEntryID, bool) {
	if f == nil || f.progress == nil {
		return raftentry.ApplyEntryID{}, false
	}
	return f.progress.LastApplied()
}

func (f *FSM) LogicalDigestV1(opts raftapply.LogicalDigestOptionsV1) (raftapply.LogicalDigestV1, error) {
	if f == nil || f.db == nil {
		return raftapply.LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "nil FSM DB")
	}
	return raftapply.LogicalDigestV1ForDB(f.db, opts)
}

// ApplyCommittedEntriesV1 applies entries in caller order and stops at the
// first fail-closed rejection.
func (f *FSM) ApplyCommittedEntriesV1(entries []CommittedEntryV1) ([]raftentry.ApplyResultV1, error) {
	results := make([]raftentry.ApplyResultV1, 0, len(entries))
	for _, entry := range entries {
		result, err := f.ApplyCommittedEntryV1(entry)
		results = append(results, result)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func (f *FSM) ApplyCommittedEntryV1(entry CommittedEntryV1) (raftentry.ApplyResultV1, error) {
	if f == nil || f.closed {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is closed"))
	}
	if entry.Type != EntryTypeCommandEntryV1 {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsupportedVersionV1, fmt.Errorf("unsupported committed entry type %q", entry.Type))
	}
	id := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
	if err := validateCommittedID(id); err != nil {
		return reject(commandDigest(entry.Bytes, f.decodeOptions(entry, id)), raftentry.ErrorMalformedEntryV1, err)
	}
	if err := f.checkCommittedOrder(id); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(commandDigest(entry.Bytes, f.decodeOptions(entry, id)), code, err)
	}
	meta, err := f.applyMetadata(entry, id)
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(commandDigest(entry.Bytes, f.decodeOptions(entry, id)), code, err)
	}
	return raftapply.ApplyCommittedEntryV1(f.db, entry.Bytes, meta, raftapply.Options{
		DecodeLimits:  f.decodeLimits,
		ProgressStore: f.progress,
		ResultStore:   f.results,
	})
}

func (f *FSM) applyMetadata(entry CommittedEntryV1, id raftentry.ApplyEntryID) (raftapply.ApplyMetadataV1, error) {
	return raftapply.ApplyMetadataV1{
		EntryID:                  id,
		LocalDurabilityBoundary:  raftapply.LocalDurabilityCommandWALV1,
		SyncLocalCommandWAL:      entry.SyncLocalCommandWAL,
		CurrentCatalogVersion:    entry.CurrentCatalogVersion,
		HasCurrentCatalogVersion: entry.HasCurrentCatalogVersion,
		ScopeRule:                f.scopeRule,
		DatabaseScope:            f.database,
		CatalogScope:             f.catalog,
		RequestMetadata:          cloneRequestMetadataV1(entry.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(entry.ExpectedTarget),
	}, nil
}

func (f *FSM) decodeOptions(entry CommittedEntryV1, id raftentry.ApplyEntryID) raftentry.DecodeOptions {
	if f == nil {
		return raftentry.DecodeOptions{
			ApplyEntryID:    id,
			RequestMetadata: cloneRequestMetadataV1(entry.RequestMetadata),
			ExpectedTarget:  cloneExpectedTargetV1(entry.ExpectedTarget),
		}
	}
	return raftentry.DecodeOptions{
		Limits:          f.decodeLimits,
		ScopeRule:       f.scopeRule,
		DatabaseScope:   f.database,
		CatalogScope:    f.catalog,
		ApplyEntryID:    id,
		RequestMetadata: cloneRequestMetadataV1(entry.RequestMetadata),
		ExpectedTarget:  cloneExpectedTargetV1(entry.ExpectedTarget),
	}
}

func (f *FSM) checkCommittedOrder(id raftentry.ApplyEntryID) error {
	last, ok := f.progress.LastApplied()
	if !ok {
		if id.Index != 1 {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply entry starts at index %d; want 1", id.Index)
		}
		return nil
	}
	if id.Index < last.Index {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry index %d is below last applied %d", id.Index, last.Index)
	}
	if id.Index > last.Index+1 {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry index gap: got %d after %d", id.Index, last.Index)
	}
	if id.Index == last.Index {
		if id.Term != last.Term {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply entry term %d conflicts with last applied term %d at index %d", id.Term, last.Term, id.Index)
		}
		return nil
	}
	if id.Term < last.Term {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry term %d is below last applied term %d", id.Term, last.Term)
	}
	return nil
}

func validateCommittedID(id raftentry.ApplyEntryID) error {
	if id.Term == 0 || id.Index == 0 {
		return fmt.Errorf("apply entry id must have non-zero term and index")
	}
	return nil
}

func commandDigest(src []byte, opts raftentry.DecodeOptions) raftentry.CommandDigestV1 {
	if len(src) == 0 {
		return raftentry.CommandDigestV1{}
	}
	digest, err := raftentry.ValidateCommandDigestInputV1(src, opts)
	if err == nil {
		return digest
	}
	return raftentry.CommandDigestV1ForBytes(bytes.Clone(src), opts)
}

func cloneRequestMetadataV1(meta raftentry.RequestMetadataV1) raftentry.RequestMetadataV1 {
	meta.TraceContext = bytes.Clone(meta.TraceContext)
	return meta
}

func cloneExpectedTargetV1(target *raftentry.TargetIdentityV1) *raftentry.TargetIdentityV1 {
	if target == nil {
		return nil
	}
	cloned := target.Clone()
	return &cloned
}

func codedError(code raftentry.DeterministicErrorCodeV1, format string, args ...any) error {
	return &Error{Code: code, Err: fmt.Errorf(format, args...)}
}

func reject(digest raftentry.CommandDigestV1, code raftentry.DeterministicErrorCodeV1, err error) (raftentry.ApplyResultV1, error) {
	if code == "" {
		code = raftentry.ErrorMalformedEntryV1
	}
	return raftentry.ApplyResultV1{
		Status:                 statusForCode(code),
		CommandDigest:          digest,
		DeterministicErrorCode: code,
	}, &Error{Code: code, Err: err}
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
