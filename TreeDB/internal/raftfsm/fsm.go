package raftfsm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

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

	// SnapshotRestoreDBOptions is used when InstallRaftSnapshotV1 has to
	// discard the current local DB handle and reopen the restored snapshot.
	// Dir is always replaced with Cluster.Dir's resolved main DB directory.
	SnapshotRestoreDBOptions backenddb.Options

	ScopeRule     raftentry.ScopeRuleV1
	DatabaseScope string
	CatalogScope  string
}

// FSM applies committed deterministic entries to one local DB.
type FSM struct {
	mu sync.RWMutex

	db          *backenddb.DB
	metadataDir string

	decodeLimits nativewire.Limits
	storeOptions raftapply.DurableApplyStoreOptions
	restoreDB    backenddb.Options
	scopeRule    raftentry.ScopeRuleV1
	database     string
	catalog      string
	cluster      raftcluster.ResolvedConfig

	progress *raftapply.DurableApplyProgressStore
	results  *raftapply.DurableApplyResultStore
	sideDBs  func() error
	ownsDB   bool
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
	if err := cleanupAbandonedRaftSnapshotArchivesV1(cluster.Layout.SnapshotDir); err != nil {
		return nil, errors.Join(codedError(raftentry.ErrorUnsafeDurabilityModeV1, "cleanup abandoned raft snapshot archives"), err)
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
	if err := validateProgressCoverage(opts.DB, progress, results); err != nil {
		_ = errors.Join(progress.Close(), results.Close())
		return nil, err
	}
	return &FSM{
		db:           opts.DB,
		metadataDir:  metadataDir,
		decodeLimits: opts.DecodeLimits,
		storeOptions: opts.StoreOptions,
		restoreDB:    opts.SnapshotRestoreDBOptions,
		scopeRule:    opts.ScopeRule,
		database:     opts.DatabaseScope,
		catalog:      opts.CatalogScope,
		cluster:      cluster,
		progress:     progress,
		results:      results,
	}, nil
}

func (f *FSM) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	var errs []error
	if f.progress != nil {
		errs = append(errs, f.progress.Close())
	}
	if f.results != nil {
		errs = append(errs, f.results.Close())
	}
	if f.ownsDB && f.db != nil {
		errs = append(errs, f.db.Close())
	}
	if f.sideDBs != nil {
		errs = append(errs, f.sideDBs())
		f.sideDBs = nil
	}
	return errors.Join(errs...)
}

func (f *FSM) AllowsInitialIndexGapV1() bool {
	return f != nil && f.storeOptions.AllowInitialIndexGap
}

func (f *FSM) LastApplied() (raftentry.ApplyEntryID, bool) {
	if f == nil {
		return raftentry.ApplyEntryID{}, false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f == nil || f.progress == nil {
		return raftentry.ApplyEntryID{}, false
	}
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil || !ok {
		return raftentry.ApplyEntryID{}, false
	}
	return record.EntryID, true
}

func (f *FSM) ValidateAppliedPrefixV1(entries []CommittedEntryV1) (raftentry.ApplyResultV1, error) {
	if f == nil {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is not open"))
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is closed"))
	}
	if f.db == nil || f.progress == nil || f.results == nil {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is not open"))
	}
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(raftentry.CommandDigestV1{}, code, err)
	}
	if !ok {
		if len(entries) == 0 {
			localLSN, err := localAppliedCommandLSN(f.db)
			if err != nil {
				code, _ := ErrorCodeOf(err)
				return reject(raftentry.CommandDigestV1{}, code, err)
			}
			resultCount := f.results.Len()
			if localLSN != 0 || resultCount != 0 {
				err := fmt.Errorf("applied prefix is empty but FSM has local AppliedCommandLSN coverage %d and %d durable results without applied progress", localLSN, resultCount)
				return reject(raftentry.CommandDigestV1{}, raftentry.ErrorRejectedConflictV1, err)
			}
			return raftentry.ApplyResultV1{}, nil
		}
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix has %d entries but FSM has no applied progress", len(entries)))
	}
	progressCount := f.progress.Len()
	if len(entries) != progressCount {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix length %d does not match durable progress count %d", len(entries), progressCount))
	}
	if len(entries) == 0 {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix is empty but FSM last applied index is %d", record.EntryID.Index))
	}
	var previous raftentry.ApplyEntryID
	for i, entry := range entries {
		id := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
		digest := commandDigest(entry.Bytes, f.decodeOptions(entry, id))
		if err := validateCommittedID(id); err != nil {
			return reject(digest, raftentry.ErrorMalformedEntryV1, err)
		}
		if i == 0 {
			if id.Index != 1 && !f.storeOptions.AllowInitialIndexGap {
				return reject(digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix starts at index %d; want 1", id.Index))
			}
		} else {
			if id.Index <= previous.Index {
				return reject(digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix entry index %d at offset %d is not after previous index %d", id.Index, i, previous.Index))
			}
			if id.Term < previous.Term {
				return reject(digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix entry term %d at offset %d is below previous term %d", id.Term, i, previous.Term))
			}
		}
		progress, ok, err := f.progress.LookupApplyProgress(id)
		if err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(digest, code, err)
		}
		if !ok {
			return reject(digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("missing durable progress for applied prefix entry %d/%d", id.Term, id.Index))
		}
		if err := validateApplyProgressCoverage(f.db, progress); err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(digest, code, err)
		}
		if progress.CommandDigest != digest {
			return reject(digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix progress digest conflicts at %d/%d", id.Term, id.Index))
		}
		stored, ok, err := f.results.LookupApplyResult(id)
		if err != nil {
			code, _ := ErrorCodeOf(err)
			return reject(digest, code, err)
		}
		if !ok {
			return reject(digest, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("missing durable result for applied prefix entry %d/%d", id.Term, id.Index))
		}
		if stored.CommandDigest != digest {
			return reject(digest, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix digest conflicts at %d/%d", id.Term, id.Index))
		}
		previous = id
	}
	if previous != record.EntryID {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorRejectedConflictV1, fmt.Errorf("applied prefix last entry %d/%d does not match durable last applied %d/%d", previous.Term, previous.Index, record.EntryID.Term, record.EntryID.Index))
	}
	return raftentry.ApplyResultV1{}, nil
}

func (f *FSM) LogicalDigestV1(opts raftapply.LogicalDigestOptionsV1) (raftapply.LogicalDigestV1, error) {
	if f == nil {
		return raftapply.LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "nil FSM DB")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.logicalDigestV1Locked(opts)
}

func (f *FSM) logicalDigestV1Locked(opts raftapply.LogicalDigestOptionsV1) (raftapply.LogicalDigestV1, error) {
	if f != nil && f.closed {
		return raftapply.LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
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
	if f == nil {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is not open"))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is closed"))
	}
	if f.db == nil || f.progress == nil || f.results == nil {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsafeDurabilityModeV1, fmt.Errorf("FSM is not open"))
	}
	if entry.Type != EntryTypeCommandEntryV1 {
		return reject(raftentry.CommandDigestV1{}, raftentry.ErrorUnsupportedVersionV1, fmt.Errorf("unsupported committed entry type %q", entry.Type))
	}
	id := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
	if err := validateCommittedID(id); err != nil {
		return reject(commandDigest(entry.Bytes, f.decodeOptions(entry, id)), raftentry.ErrorMalformedEntryV1, err)
	}
	digest := commandDigest(entry.Bytes, f.decodeOptions(entry, id))
	if err := f.checkCommittedOrder(id, digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(digest, code, err)
	}
	if err := f.requireStoredResultForLocalCoverageGap(id, digest); err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(digest, code, err)
	}
	meta, err := f.applyMetadata(entry, id)
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return reject(digest, code, err)
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

func (f *FSM) checkCommittedOrder(id raftentry.ApplyEntryID, digest raftentry.CommandDigestV1) error {
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		return err
	}
	if !ok {
		if id.Index != 1 && !f.storeOptions.AllowInitialIndexGap {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply entry starts at index %d; want 1", id.Index)
		}
		localLSN, err := localAppliedCommandLSN(f.db)
		if err != nil {
			return err
		}
		if localLSN != 0 || f.results.Len() != 0 {
			if _, ok, err := f.results.LookupApplyResult(id); err != nil {
				return err
			} else if !ok {
				return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing durable result for apply entry %d/%d while local coverage exists without progress metadata", id.Term, id.Index)
			}
		}
		return nil
	}
	last := record.EntryID
	if id.Index < last.Index {
		stored, ok, err := f.results.LookupApplyResult(id)
		if err != nil {
			return err
		}
		if !ok {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply entry index %d is below last applied %d and is not a durable exact replay", id.Index, last.Index)
		}
		if stored.CommandDigest != digest {
			return codedError(raftentry.ErrorRejectedConflictV1, "replayed apply entry digest conflicts at %d/%d", id.Term, id.Index)
		}
		progress, ok, err := f.progress.LookupApplyProgress(id)
		if err != nil {
			return err
		}
		if !ok {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing durable progress for replayed apply entry %d/%d below last applied %d/%d", id.Term, id.Index, last.Term, last.Index)
		}
		if err := validateApplyProgressCoverage(f.db, progress); err != nil {
			return err
		}
		if progress.CommandDigest != digest {
			return codedError(raftentry.ErrorRejectedConflictV1, "replayed apply progress digest conflicts at %d/%d", id.Term, id.Index)
		}
		return nil
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

func (f *FSM) lastAppliedProgressRecord() (raftapply.ApplyProgressRecordV1, bool, error) {
	if f == nil || f.db == nil || f.progress == nil {
		return raftapply.ApplyProgressRecordV1{}, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	record, ok := f.progress.LastAppliedRecord()
	if !ok {
		return raftapply.ApplyProgressRecordV1{}, false, nil
	}
	if err := validateApplyProgressCoverage(f.db, record); err != nil {
		return raftapply.ApplyProgressRecordV1{}, false, err
	}
	return record, true, nil
}

func validateProgressCoverage(db *backenddb.DB, progress *raftapply.DurableApplyProgressStore, results *raftapply.DurableApplyResultStore) error {
	if progress == nil {
		return nil
	}
	localLSN, err := localAppliedCommandLSN(db)
	if err != nil {
		return err
	}
	record, ok := progress.LastAppliedRecord()
	if !ok {
		if localLSN != 0 && (results == nil || results.Len() == 0) {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing apply progress metadata for local AppliedCommandLSN coverage %d without durable result metadata", localLSN)
		}
		return nil
	}
	if record.AppliedCommandLSN > localLSN {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "apply progress metadata AppliedCommandLSN %d outruns local coverage %d", record.AppliedCommandLSN, localLSN)
	}
	if record.AppliedCommandLSN < localLSN && (results == nil || results.Len() <= progress.Len()) {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "local AppliedCommandLSN coverage %d outruns apply progress metadata %d without durable result metadata beyond progress", localLSN, record.AppliedCommandLSN)
	}
	return nil
}

func localAppliedCommandLSN(db *backenddb.DB) (uint64, error) {
	if db == nil {
		return 0, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "nil FSM DB")
	}
	state, ok := db.StateToken()
	if !ok {
		return 0, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM DB state unavailable")
	}
	return state.AppliedCommandLSN, nil
}

func validateApplyProgressCoverage(db *backenddb.DB, record raftapply.ApplyProgressRecordV1) error {
	localLSN, err := localAppliedCommandLSN(db)
	if err != nil {
		return err
	}
	if record.AppliedCommandLSN > localLSN {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "apply progress metadata AppliedCommandLSN %d outruns local coverage %d", record.AppliedCommandLSN, localLSN)
	}
	return nil
}

func (f *FSM) requireStoredResultForLocalCoverageGap(id raftentry.ApplyEntryID, digest raftentry.CommandDigestV1) error {
	if f == nil || f.db == nil || f.progress == nil || f.results == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	localLSN, err := localAppliedCommandLSN(f.db)
	if err != nil {
		return err
	}
	record, ok := f.progress.LastAppliedRecord()
	if !ok {
		if localLSN == 0 && f.results.Len() == 0 {
			return nil
		}
		return f.requireStoredResultCoveredByLocalLSN(id, digest, localLSN, 0)
	}
	if err := validateApplyProgressCoverage(f.db, record); err != nil {
		return err
	}
	if localLSN <= record.AppliedCommandLSN || id.Index <= record.EntryID.Index {
		return nil
	}
	return f.requireStoredResultCoveredByLocalLSN(id, digest, localLSN, record.AppliedCommandLSN)
}

func (f *FSM) requireStoredResultCoveredByLocalLSN(id raftentry.ApplyEntryID, digest raftentry.CommandDigestV1, localLSN, progressLSN uint64) error {
	record, ok, err := f.results.LookupApplyResult(id)
	if err != nil {
		return err
	}
	if !ok {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "local AppliedCommandLSN coverage %d outruns apply progress metadata %d without durable result for apply entry %d/%d", localLSN, progressLSN, id.Term, id.Index)
	}
	if record.CommandDigest != digest {
		return codedError(raftentry.ErrorRejectedConflictV1, "durable result digest conflicts with apply entry %d/%d", id.Term, id.Index)
	}
	if record.AppliedCommandLSN > localLSN {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "durable result AppliedCommandLSN %d outruns local coverage %d for apply entry %d/%d", record.AppliedCommandLSN, localLSN, id.Term, id.Index)
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
	meta.ClusterRouteMembers = append([]string(nil), meta.ClusterRouteMembers...)
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
