package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

var (
	ErrVectorPartitionSplitInsertCapacityV1 = errors.New("collections: split insert checkpoint capacity exhausted")
	ErrVectorPartitionSplitInsertConflictV1 = errors.New("collections: split insert receipt conflicts")
)

const splitInsertStateMaxBytesV1 = 384 << 10

// VectorPartitionSplitInsertReceiptV1 retains the original source mutation
// identity separately from the target Raft position and the live graph revision.
type VectorPartitionSplitInsertReceiptV1 struct {
	Attempt      [sha256.Size]byte
	Digest       [sha256.Size]byte
	ID           []byte
	SourceTerm   uint64
	SourceIndex  uint64
	TargetTerm   uint64
	TargetIndex  uint64
	LiveRevision uint64
}

type vectorPartitionSplitInsertStateV1 struct {
	Version   uint32
	Pending   *commitlog.SplitVectorInsertV1
	Completed []VectorPartitionSplitInsertReceiptV1
}

type splitInsertPublicationV1 struct {
	key             string
	previous        []byte
	previousPresent bool
	next            []byte
	appendedPtrs    []page.ValuePtr
}

func splitInsertStateKeyV1(v commitlog.SplitVectorInsertV1) string {
	// Length-delimited JSON avoids ambiguous collection/index/group names.
	identity, _ := json.Marshal([]any{v.Collection, v.Index, v.Generation, v.SourceGroup, v.TargetGroup, v.CatalogEpoch, v.CatalogDigest})
	digest := sha256.Sum256(identity)
	return splitInsertCollectionPrefixV1(v.Collection) + hex.EncodeToString(digest[:])
}

func splitInsertCollectionPrefixV1(collection string) string {
	digest := sha256.Sum256([]byte(collection))
	return "vector_partition_split_insert_v1:" + hex.EncodeToString(digest[:]) + ":"
}

// VectorPartitionSplitInsertLogicalStateV1 includes durable pending/receipt
// semantics in apply and snapshot convergence. Physical root IDs are excluded.
// At most 64 fixed identities are read; unsupported identity churn fails closed.
func (c *Collection) VectorPartitionSplitInsertLogicalStateV1(ctx context.Context) ([][]byte, error) {
	if c == nil || c.db == nil || c.db.IsClosing() {
		return nil, backenddb.ErrClosed
	}
	// Split state can only be created with column publication enabled. The
	// option survives index removal, so retained receipts are still hashed.
	if !columnStoreWriteEnabled(c.meta) {
		return nil, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	state, ok := snap.StateToken()
	if !ok {
		return nil, backenddb.ErrClosed
	}
	if state.SystemRootPageID == 0 {
		return nil, nil
	}
	prefix := []byte(splitInsertCollectionPrefixV1(c.name))
	it, err := snap.IteratorAtRootWithOptions(state.SystemRootPageID, prefix, prefixEnd(prefix), backenddb.IteratorOptions{IncludeTombstones: true})
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var records [][]byte
	inspected := 0
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(it.UnsafeKey(), prefix) {
			break
		}
		if inspected >= commitlog.SplitVectorInsertMaxAttemptsV1 {
			return nil, ErrVectorPartitionSplitInsertCapacityV1
		}
		inspected++
		if !it.IsDeleted() {
			raw := it.ValueCopy(nil)
			if _, err := decodeSplitInsertStateV1(raw, true); err != nil {
				return nil, err
			}
			records = append(records, bytes.Clone(it.UnsafeKey()), raw)
		}
		it.Next()
	}
	return records, it.Error()
}

// New identities must fit the same collection-wide bound used by logical
// convergence. Count tombstones too; reviving an existing key adds no slot.
func (c *Collection) preflightSplitInsertIdentityV1(snap *backenddb.Snapshot, key string) error {
	prefix := []byte(splitInsertCollectionPrefixV1(c.name))
	state, ok := snap.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	if state.SystemRootPageID == 0 {
		return nil
	}
	it, err := snap.IteratorAtRootWithOptions(state.SystemRootPageID, prefix, prefixEnd(prefix), backenddb.IteratorOptions{IncludeTombstones: true})
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	count, existing := 0, false
	for it.Valid() && bytes.HasPrefix(it.UnsafeKey(), prefix) {
		count++
		if count > commitlog.SplitVectorInsertMaxAttemptsV1 {
			return ErrVectorPartitionSplitInsertCapacityV1
		}
		existing = existing || bytes.Equal(it.UnsafeKey(), []byte(key))
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if count == commitlog.SplitVectorInsertMaxAttemptsV1 && !existing {
		return ErrVectorPartitionSplitInsertCapacityV1
	}
	return nil
}

// A duplicate durable command still consumes its WAL coverage. Metadata-only
// publication changes the state token without changing canonical generation.
func (c *Collection) publishSplitInsertNoopV1(intent *backenddb.CommandWALIntent) error {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord.partitionLivePublishMu.Lock()
	defer coord.partitionLivePublishMu.Unlock()
	if err := c.db.PublishCommandWALNoop(intent, false); err != nil {
		if backenddb.CommitPublicationAccepted(err) {
			c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		}
		return err
	}
	return c.recordReconciledVectorIndexCoverage(c.registeredVectorPartitionLiveCarriersV1())
}

func decodeSplitInsertStateV1(raw []byte, present bool) (vectorPartitionSplitInsertStateV1, error) {
	state := vectorPartitionSplitInsertStateV1{Version: 1}
	if !present {
		return state, nil
	}
	if len(raw) == 0 || len(raw) > splitInsertStateMaxBytesV1 {
		return state, ErrVectorPartitionSplitInsertCapacityV1
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return state, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF || state.Version != 1 || len(state.Completed) > commitlog.SplitVectorInsertMaxAttemptsV1 {
		return state, ErrVectorPartitionSplitInsertConflictV1
	}
	if state.Pending != nil {
		if err := state.Pending.ValidateV1(); err != nil || state.Pending.Operation != "source" || state.Pending.SourceTerm == 0 || state.Pending.SourceIndex == 0 {
			return state, ErrVectorPartitionSplitInsertConflictV1
		}
	}
	for i, receipt := range state.Completed {
		if receipt.Attempt == ([sha256.Size]byte{}) || receipt.Digest == ([sha256.Size]byte{}) ||
			len(receipt.ID) == 0 || len(receipt.ID) > commitlog.SplitVectorInsertMaxIdentityBytesV1 ||
			receipt.SourceTerm == 0 || receipt.SourceIndex == 0 || receipt.TargetTerm == 0 || receipt.TargetIndex == 0 || receipt.LiveRevision == 0 ||
			(i > 0 && bytes.Compare(state.Completed[i-1].Attempt[:], receipt.Attempt[:]) >= 0) {
			return state, ErrVectorPartitionSplitInsertConflictV1
		}
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(raw, canonical) {
		return state, ErrVectorPartitionSplitInsertConflictV1
	}
	return state, nil
}

func (c *Collection) readSplitInsertStateV1(v commitlog.SplitVectorInsertV1) (vectorPartitionSplitInsertStateV1, []byte, bool, error) {
	if c == nil || c.db == nil || c.name != v.Collection {
		return vectorPartitionSplitInsertStateV1{}, nil, false, ErrVectorIndexPartitionLiveUnavailableV1
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return vectorPartitionSplitInsertStateV1{}, nil, false, backenddb.ErrClosed
	}
	raw, present, err := getSystemValue(snap, splitInsertStateKeyV1(v))
	raw = bytes.Clone(raw)
	closeErr := snap.Close()
	if err != nil || closeErr != nil {
		return vectorPartitionSplitInsertStateV1{}, nil, false, errors.Join(err, closeErr)
	}
	state, err := decodeSplitInsertStateV1(raw, present)
	return state, raw, present, err
}

func splitInsertReceiptV1(v commitlog.SplitVectorInsertV1) (VectorPartitionSplitInsertReceiptV1, error) {
	digest, err := v.DigestV1()
	if err != nil {
		return VectorPartitionSplitInsertReceiptV1{}, err
	}
	return VectorPartitionSplitInsertReceiptV1{
		Attempt: sha256.Sum256(v.Attempt), Digest: digest, ID: bytes.Clone(v.ID),
		SourceTerm: v.SourceTerm, SourceIndex: v.SourceIndex,
		TargetTerm: v.TargetTerm, TargetIndex: v.TargetIndex, LiveRevision: v.LiveRevision,
	}, nil
}

func splitInsertFindReceiptV1(state vectorPartitionSplitInsertStateV1, v commitlog.SplitVectorInsertV1) (VectorPartitionSplitInsertReceiptV1, bool, error) {
	attempt := sha256.Sum256(v.Attempt)
	digest, err := v.DigestV1()
	if err != nil {
		return VectorPartitionSplitInsertReceiptV1{}, false, err
	}
	for _, receipt := range state.Completed {
		if receipt.Attempt != attempt {
			continue
		}
		if receipt.Digest != digest || !bytes.Equal(receipt.ID, v.ID) {
			return VectorPartitionSplitInsertReceiptV1{}, false, ErrVectorPartitionSplitInsertConflictV1
		}
		return receipt, true, nil
	}
	return VectorPartitionSplitInsertReceiptV1{}, false, nil
}

// VectorPartitionSplitInsertStateV1 returns owned bytes for the bounded durable
// slot and exact receipt. Callers must still fence current DB and fresh ACTIVE;
// this local metadata is never lifecycle or authority proof.
func (c *Collection) VectorPartitionSplitInsertStateV1(v commitlog.SplitVectorInsertV1) (*commitlog.SplitVectorInsertV1, VectorPartitionSplitInsertReceiptV1, bool, error) {
	state, _, _, err := c.readSplitInsertStateV1(v)
	if err != nil {
		return nil, VectorPartitionSplitInsertReceiptV1{}, false, err
	}
	receipt, found, err := splitInsertFindReceiptV1(state, v)
	return state.Pending, receipt, found, err
}

func (c *Collection) newSplitInsertPublicationV1(v commitlog.SplitVectorInsertV1, state vectorPartitionSplitInsertStateV1, previous []byte, present bool) (*splitInsertPublicationV1, error) {
	next, err := json.Marshal(state)
	if err != nil || len(next) > splitInsertStateMaxBytesV1 {
		return nil, errors.Join(ErrVectorPartitionSplitInsertCapacityV1, err)
	}
	if _, err := decodeSplitInsertStateV1(next, true); err != nil {
		return nil, err
	}
	return &splitInsertPublicationV1{key: splitInsertStateKeyV1(v), previous: bytes.Clone(previous), previousPresent: present, next: next}, nil
}

// appendSplitInsertSystemDeltaV1 extends an existing publication; it never
// performs an independent metadata commit after a row or graph publication.
func (c *Collection) appendSplitInsertSystemDeltaV1(it iterator.UnsafeIterator, publication *splitInsertPublicationV1) (iterator.UnsafeIterator, error) {
	if publication == nil {
		return it, nil
	}
	fail := func(err error) (iterator.UnsafeIterator, error) {
		if it != nil {
			_ = it.Close()
		}
		return nil, err
	}
	base, ok := it.(*systemTargetIterator)
	if !ok || base.idx != 0 {
		return fail(errors.New("collections: split insert requires untouched bounded system delta"))
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return fail(backenddb.ErrClosed)
	}
	previous, present, err := getSystemValue(snap, publication.key)
	if err == nil && (present != publication.previousPresent || !bytes.Equal(previous, publication.previous)) {
		err = errConcurrentRootModification(c.name, "split_insert_v1")
	}
	if err == nil && !present && bytes.HasPrefix([]byte(publication.key), []byte(splitInsertCollectionPrefixV1(c.name))) {
		err = c.preflightSplitInsertIdentityV1(snap, publication.key)
	}
	closeErr := snap.Close()
	if err != nil || closeErr != nil {
		return fail(errors.Join(err, closeErr))
	}
	entry := systemTargetEntry{key: []byte(publication.key), value: bytes.Clone(publication.next)}
	inlineCapacity := max(0, page.PageSize-node.NodeHeaderSize-node.DirectoryEntrySize-7-page.EntryRevisionSize-len(entry.key))
	if len(entry.value) > inlineCapacity {
		// Each builder attempt owns fresh pointers until the atomic root publisher
		// consumes them. Keep pre-publication failures pinned until operation exit.
		ptrs, appendErr := c.db.AppendValueLogValues([][]byte{entry.value})
		if appendErr != nil {
			return fail(appendErr)
		}
		publication.appendedPtrs = append(publication.appendedPtrs, ptrs...)
		if len(ptrs) != 1 {
			return fail(errors.New("collections: split insert value-log pointer count mismatch"))
		}
		entry.value, entry.ptr, entry.flags = nil, ptrs[0], node.FlagPointer
	}
	base.entries = append(base.entries, entry)
	sort.Slice(base.entries, func(i, j int) bool { return bytes.Compare(base.entries[i].key, base.entries[j].key) < 0 })
	for i := 1; i < len(base.entries); i++ {
		if bytes.Equal(base.entries[i-1].key, base.entries[i].key) {
			return fail(fmt.Errorf("collections: split insert system key collision"))
		}
	}
	return base, nil
}

// PendingVectorPartitionSplitInsertV1 reads at most the single durable slot.
// The caller supplies only the fixed identity; no scan or unbounded materialization
// is required on startup or on one retry tick.
func (c *Collection) PendingVectorPartitionSplitInsertV1(identity commitlog.SplitVectorInsertV1) (*commitlog.SplitVectorInsertV1, error) {
	state, _, _, err := c.readSplitInsertStateV1(identity)
	return state.Pending, err
}

// PreflightVectorPartitionSplitInsertV1 rejects capacity/conflict before append.
// Apply repeats the same checks and the root publisher compares exact prior
// state, so a racing writer cannot turn this observation into authority.
func (c *Collection) PreflightVectorPartitionSplitInsertV1(ctx context.Context, v commitlog.SplitVectorInsertV1) error {
	return c.preflightVectorPartitionSplitInsertV1(ctx, v, false)
}

func (c *Collection) preflightVectorPartitionSplitInsertV1(ctx context.Context, v commitlog.SplitVectorInsertV1, admitted bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := v.ValidateV1(); err != nil {
		return err
	}
	if c == nil || c.db == nil || c.name != v.Collection {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	meta := c.Meta()
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON || !columnStoreWriteEnabled(meta) {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	if v.Operation == "source" {
		var vector []float32
		var err error
		if admitted {
			vector, err = c.validatedVectorFromDocumentPreparedV1(v.Index, DocumentFormatJSON, v.Document)
		} else {
			vector, err = c.ValidatedVectorFromDocumentV1(v.Index, DocumentFormatJSON, v.Document)
		}
		if err != nil || len(vector) != len(v.Vector) {
			return errors.Join(ErrVectorPartitionSplitInsertConflictV1, err)
		}
		for i := range vector {
			if math.Float32bits(vector[i]) != math.Float32bits(v.Vector[i]) {
				return ErrVectorPartitionSplitInsertConflictV1
			}
		}
	} else {
		def, ok := findVectorIndex(meta.VectorIndexes, v.Index)
		if !ok || def.Strategy != VectorIndexStrategyColumnGraph || def.Dimensions != len(v.Vector) {
			return ErrVectorPartitionSplitInsertConflictV1
		}
	}
	state, _, present, err := c.readSplitInsertStateV1(v)
	if err != nil {
		return err
	}
	if !present {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return backenddb.ErrClosed
		}
		err = c.preflightSplitInsertIdentityV1(snap, splitInsertStateKeyV1(v))
		if err := errors.Join(err, snap.Close()); err != nil {
			return err
		}
	}
	receipt, known, err := splitInsertFindReceiptV1(state, v)
	if err != nil {
		return err
	}
	if known {
		if v.SourceTerm != 0 && (receipt.SourceTerm != v.SourceTerm || receipt.SourceIndex != v.SourceIndex) {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		if v.Operation == "clear" && (receipt.TargetTerm != v.TargetTerm || receipt.TargetIndex != v.TargetIndex || receipt.LiveRevision != v.LiveRevision) {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		return nil
	}
	if len(state.Completed) >= commitlog.SplitVectorInsertMaxAttemptsV1 {
		return ErrVectorPartitionSplitInsertCapacityV1
	}
	if v.Operation == "source" {
		if state.Pending != nil {
			pendingDigest, err := state.Pending.DigestV1()
			digest, digestErr := v.DigestV1()
			if err != nil || digestErr != nil || pendingDigest != digest || !bytes.Equal(state.Pending.Attempt, v.Attempt) {
				return ErrVectorPartitionSplitInsertCapacityV1
			}
			return nil
		}
		document, err := c.Get(v.ID)
		if err != nil {
			return err
		}
		if document != nil {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		if admitted {
			return nil
		} // The pre-owner phase restored carriers; owned admission prevents replacement.
		// Restore checkpointed local carriers before canonical roots change.
		// This existing replay seam never scans rows or builds a graph.
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return backenddb.ErrClosed
		}
		catalog, err := c.catalogForSnapshot(snap)
		if err == nil && catalog == nil {
			err = errCollectionNotFound
		}
		if err == nil {
			err = c.loadVectorPartitionLiveCarriersForReplayV1(catalog)
		}
		return errors.Join(err, snap.Close())
	}
	if v.Operation == "clear" {
		if state.Pending == nil {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		pendingDigest, err := state.Pending.DigestV1()
		digest, digestErr := v.DigestV1()
		if err != nil || digestErr != nil || pendingDigest != digest ||
			state.Pending.SourceTerm != v.SourceTerm || state.Pending.SourceIndex != v.SourceIndex {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		return nil
	}
	if state.Pending != nil {
		return ErrVectorPartitionSplitInsertConflictV1
	}
	var manifest VectorPartitionManifestV1
	if admitted {
		manifest, err = c.activeVectorPartitionManifestMutationLockedV1(ctx, v.Index)
		if err == nil && manifest.Generation != v.Generation {
			err = ErrVectorPartitionSplitInsertConflictV1
		}
	} else {
		manifest, err = c.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(ctx, v.Index, v.Generation)
	}
	if err != nil {
		return err
	}
	// Replicated apply may restore a persisted carrier, never independently
	// publish a missing binding before the command's own atomic WAL intent.
	if !admitted {
		if _, err := c.NewPreparedVectorPartitionGenerationReplicatedLiveSearchOpenPlanWithContextV1(ctx, manifest); err != nil {
			return err
		}
	} else if err := c.validateCurrentVectorPartitionLiveBindingV1(ctx, manifest); err != nil {
		return err
	}
	idx := c.registeredVectorIndex(v.Index)
	if idx == nil || !idx.vectorPartitionLiveBindingCurrentV1(manifest) {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.Lock()
	err = idx.preflightVectorPartitionMutationBatchLocked([][]byte{v.ID}, [][]float32{v.Vector})
	idx.mu.Unlock()
	return err
}

// InsertVectorPartitionSplitSourceWithCommandWALIntentV1 publishes the canonical
// row, its durable retry slot, and existing local carrier maintenance together.
// The supplied source position is the actual applying FSM entry, never ReadIndex.
func (c *Collection) InsertVectorPartitionSplitSourceWithCommandWALIntentV1(ctx context.Context, v commitlog.SplitVectorInsertV1, intent *backenddb.CommandWALIntent) error {
	return c.insertVectorPartitionSplitSourceWithOwnerV1(ctx, v, intent, nil)
}

func (c *Collection) insertVectorPartitionSplitSourceWithOwnerV1(ctx context.Context, v commitlog.SplitVectorInsertV1, intent *backenddb.CommandWALIntent, owner *CommandWALAdmittedCollection) error {
	if intent == nil || v.Operation != "source" || v.SourceTerm == 0 || v.SourceIndex == 0 {
		return ErrVectorPartitionSplitInsertConflictV1
	}
	if err := c.preflightVectorPartitionSplitInsertV1(ctx, v, owner != nil); err != nil {
		return err
	}
	state, previous, present, err := c.readSplitInsertStateV1(v)
	if err != nil {
		return err
	}
	_, known, err := splitInsertFindReceiptV1(state, v)
	if err != nil {
		return err
	}
	if known || state.Pending != nil {
		if state.Pending != nil && (state.Pending.SourceTerm != v.SourceTerm || state.Pending.SourceIndex != v.SourceIndex) {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		if owner == nil {
			unlockAdmission := c.lockNativeVectorAdmissionWrite()
			defer unlockAdmission()
			unlockCoverage := c.lockVectorIndexCoveragePersistence()
			defer unlockCoverage()
			unlockMutation := c.lockMutation()
			defer unlockMutation.Unlock()
		}
		return c.publishSplitInsertNoopV1(intent)
	}
	state.Pending = &v
	publication, err := c.newSplitInsertPublicationV1(v, state, previous, present)
	if err != nil {
		return err
	}
	defer func() { c.db.ReleaseValueLogValues(publication.appendedPtrs) }()
	var admission *collectionCommandWALAdmission
	if owner == nil {
		unlockSchema := c.lockCollectionSchemaRead()
		defer unlockSchema()
		admission = c.lockCollectionCommandWALAdmission()
		defer admission.unlock()
	} else {
		admission = owner.admission
	}
	if normalizedDocumentFormat(c.meta.Options.DocumentFormat) != DocumentFormatJSON || !columnStoreWriteEnabled(c.meta) {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	resultIDs, err := c.insertBatchWithCommandWALIntentSchemaLocked([][]byte{v.ID}, [][]byte{v.Document}, false, nil, intent, insertBatchExecutionOptions{admission: admission, borrowMutation: owner != nil, returnResultIDs: true, splitInsert: publication})
	if err == nil {
		err = commitAmbiguousError("split source insert vector maintenance", c.notifyVectorIndexesUpsert(resultIDs))
	}
	return c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// ProjectVectorPartitionSplitInsertWithCommandWALIntentV1 publishes only native
// graph deltas and their exact receipt. It never inserts a canonical target row.
func (c *Collection) ProjectVectorPartitionSplitInsertWithCommandWALIntentV1(ctx context.Context, v commitlog.SplitVectorInsertV1, targetTerm, targetIndex uint64, intent *backenddb.CommandWALIntent) (VectorPartitionSplitInsertReceiptV1, error) {
	return c.projectVectorPartitionSplitInsertWithOwnerV1(ctx, v, targetTerm, targetIndex, intent, nil)
}

func (c *Collection) projectVectorPartitionSplitInsertWithOwnerV1(ctx context.Context, v commitlog.SplitVectorInsertV1, targetTerm, targetIndex uint64, intent *backenddb.CommandWALIntent, owner *CommandWALAdmittedCollection) (VectorPartitionSplitInsertReceiptV1, error) {
	var zero VectorPartitionSplitInsertReceiptV1
	if intent == nil || v.Operation != "project" || targetTerm == 0 || targetIndex == 0 {
		return zero, ErrVectorPartitionSplitInsertConflictV1
	}
	if err := c.preflightVectorPartitionSplitInsertV1(ctx, v, owner != nil); err != nil {
		return zero, err
	}
	if owner == nil {
		unlockSchema := c.lockCollectionSchemaRead()
		defer unlockSchema()
		unlockAdmission := c.lockNativeVectorAdmissionWrite()
		defer unlockAdmission()
		unlockCoverage := c.lockVectorIndexCoveragePersistence()
		defer unlockCoverage()
		unlockMutation := c.lockMutation()
		defer unlockMutation.Unlock()
	}
	if err := c.flushBufferedWritesWithCoverageLocked(); err != nil {
		return zero, err
	}
	state, previous, present, err := c.readSplitInsertStateV1(v)
	if err != nil {
		return zero, err
	}
	if receipt, known, err := splitInsertFindReceiptV1(state, v); err != nil {
		return zero, err
	} else if known {
		if receipt.SourceTerm != v.SourceTerm || receipt.SourceIndex != v.SourceIndex {
			return zero, ErrVectorPartitionSplitInsertConflictV1
		}
		return receipt, c.publishSplitInsertNoopV1(intent)
	}
	if state.Pending != nil || len(state.Completed) >= commitlog.SplitVectorInsertMaxAttemptsV1 {
		return zero, ErrVectorPartitionSplitInsertCapacityV1
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return zero, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return zero, err
	}
	if catalog == nil {
		return zero, errCollectionNotFound
	}
	input := columnWritePublishInput{meta: catalog.meta, catalog: catalog, baseCommitSeq: snapshotCommitSeq(snap), baseSystemRoot: snapshotSystemRoot(snap), commandWALIntent: intent, splitProjection: &v}
	specs, err := c.vectorPartitionLiveReplaySpecsV1(input)
	if err != nil || len(specs) != 1 || specs[0].definition.Name != v.Index {
		return zero, errors.Join(ErrVectorIndexPartitionLiveUnavailableV1, err)
	}
	unlockPublication, err := c.lockVectorPartitionLiveReplayPublicationV2(specs)
	if err != nil {
		return zero, err
	}
	defer unlockPublication()
	attempt, err := c.buildVectorPartitionLiveReplayAttemptV1(input, specs)
	if err != nil {
		return zero, err
	}
	accepted := false
	defer func() {
		if !accepted {
			attempt.discard()
		} else {
			attempt.closeResources()
		}
	}()
	candidate := attempt.entries[0].candidate
	candidate.mu.RLock()
	liveRevision := candidate.partitionLive.revision
	candidate.mu.RUnlock()
	complete := v
	complete.Operation = "clear"
	complete.TargetTerm, complete.TargetIndex, complete.LiveRevision = targetTerm, targetIndex, liveRevision
	receipt, err := splitInsertReceiptV1(complete)
	if err != nil {
		return zero, err
	}
	state.Completed = append(state.Completed, receipt)
	sort.Slice(state.Completed, func(i, j int) bool {
		return bytes.Compare(state.Completed[i].Attempt[:], state.Completed[j].Attempt[:]) < 0
	})
	input.splitInsert, err = c.newSplitInsertPublicationV1(v, state, previous, present)
	if err != nil {
		return zero, err
	}
	defer func() { c.db.ReleaseValueLogValues(input.splitInsert.appendedPtrs) }()
	entry := &attempt.entries[0]
	policy, err := collectionRootStoragePolicyForDB(c.db, catalog.meta, entry.spec.rootName)
	if err != nil {
		return zero, err
	}
	entry.iter = entry.publish.NewIterator(nil, nil)
	input.rootNames = []string{entry.spec.rootName}
	input.baseRootIDs = map[string]uint64{entry.spec.rootName: entry.spec.baseRoot}
	newSystemRoot, roots, err := c.publishRootDeltaGroupWithoutColumn([]backenddb.OrderedRootDeltaPublishInput{{BaseRoot: entry.spec.baseRoot, Iter: entry.iter, StoragePolicy: policy}}, input)
	if err != nil {
		if backenddb.CommitPublicationAccepted(err) {
			accepted = true
			attempt.invalidateAcceptedFailureV2()
		}
		return zero, err
	}
	accepted = true
	if len(roots) != 1 {
		attempt.invalidateAcceptedFailureV2()
		return zero, unexpectedOrderedRootCountError(c.name, 1, len(roots))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, input.rootNames, roots)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if err := c.installVectorPartitionLiveReplayAttemptV1(attempt, input.rootNames, roots); err != nil {
		attempt.invalidateAcceptedFailureV2()
		return zero, commitAmbiguousError("split projection carrier handoff", err)
	}
	return receipt, nil
}

// CompleteVectorPartitionSplitInsertWithCommandWALIntentV1 retires the single
// source retry slot only with an exact committed target receipt.
func (c *Collection) CompleteVectorPartitionSplitInsertWithCommandWALIntentV1(ctx context.Context, v commitlog.SplitVectorInsertV1, intent *backenddb.CommandWALIntent) error {
	return c.completeVectorPartitionSplitInsertWithOwnerV1(ctx, v, intent, nil)
}

func (c *Collection) completeVectorPartitionSplitInsertWithOwnerV1(ctx context.Context, v commitlog.SplitVectorInsertV1, intent *backenddb.CommandWALIntent, owner *CommandWALAdmittedCollection) error {
	if intent == nil || v.Operation != "clear" {
		return ErrVectorPartitionSplitInsertConflictV1
	}
	if err := c.preflightVectorPartitionSplitInsertV1(ctx, v, owner != nil); err != nil {
		return err
	}
	if owner == nil {
		unlockSchema := c.lockCollectionSchemaRead()
		defer unlockSchema()
		unlockAdmission := c.lockNativeVectorAdmissionWrite()
		defer unlockAdmission()
		unlockCoverage := c.lockVectorIndexCoveragePersistence()
		defer unlockCoverage()
		unlockMutation := c.lockMutation()
		defer unlockMutation.Unlock()
	}
	state, previous, present, err := c.readSplitInsertStateV1(v)
	if err != nil {
		return err
	}
	if receipt, known, err := splitInsertFindReceiptV1(state, v); err != nil {
		return err
	} else if known {
		if receipt.SourceTerm != v.SourceTerm || receipt.SourceIndex != v.SourceIndex || receipt.TargetTerm != v.TargetTerm || receipt.TargetIndex != v.TargetIndex || receipt.LiveRevision != v.LiveRevision {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		return c.publishSplitInsertNoopV1(intent)
	}
	if state.Pending == nil || len(state.Completed) >= commitlog.SplitVectorInsertMaxAttemptsV1 {
		return ErrVectorPartitionSplitInsertConflictV1
	}
	pendingDigest, pendingErr := state.Pending.DigestV1()
	completedDigest, completedErr := v.DigestV1()
	if pendingErr != nil || completedErr != nil || pendingDigest != completedDigest || state.Pending.SourceTerm != v.SourceTerm || state.Pending.SourceIndex != v.SourceIndex {
		return ErrVectorPartitionSplitInsertConflictV1
	}
	receipt, err := splitInsertReceiptV1(v)
	if err != nil {
		return err
	}
	state.Pending = nil
	state.Completed = append(state.Completed, receipt)
	sort.Slice(state.Completed, func(i, j int) bool {
		return bytes.Compare(state.Completed[i].Attempt[:], state.Completed[j].Attempt[:]) < 0
	})
	publication, err := c.newSplitInsertPublicationV1(v, state, previous, present)
	if err != nil {
		return err
	}
	defer func() { c.db.ReleaseValueLogValues(publication.appendedPtrs) }()
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	baseSystemRoot, baseCommitSeq := snapshotSystemRoot(snap), snapshotCommitSeq(snap)
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord.partitionLivePublishMu.Lock()
	defer coord.partitionLivePublishMu.Unlock()
	var newSystemRoot uint64
	err = c.withCommandWALPublishCoordinatorForIntent(intent, func() error {
		var publishErr error
		newSystemRoot, _, publishErr = c.db.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, func() error {
			current := c.db.AcquireSnapshot()
			if current == nil {
				return backenddb.ErrClosed
			}
			defer func() { _ = current.Close() }()
			if snapshotSystemRoot(current) != baseSystemRoot || snapshotCommitSeq(current) != baseCommitSeq {
				return ErrConcurrentMutation
			}
			return nil
		}, intent, func(_ backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
			return c.appendSplitInsertSystemDeltaV1(&systemTargetIterator{}, publication)
		})
		return publishErr
	})
	if err != nil {
		if backenddb.CommitPublicationAccepted(err) {
			c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		}
		return err
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		return commitAmbiguousError("split clear catalog", err)
	}
	if catalog != nil {
		c.rememberCatalogAtSystemRoot(newSystemRoot, catalog)
		c.noteWriteDomainCatalog(newSystemRoot, catalog)
	}
	if err := c.recordReconciledVectorIndexCoverage(c.registeredVectorPartitionLiveCarriersV1()); err != nil {
		return commitAmbiguousError("split clear carrier state", err)
	}
	return nil
}

func replaySplitVectorInsertCommandWALV1(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	v, err := commitlog.DecodeSplitVectorInsertPayloadV1(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	collection, err := newCommandWALReplayCollectionManager(db).openCollectionWithCommandWALIntent(v.Collection, intent)
	if err != nil {
		return err
	}
	switch v.Operation {
	case "source":
		if v.SourceTerm == 0 || v.SourceIndex == 0 {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		return collection.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(context.Background(), v, intent)
	case "project":
		if v.TargetTerm == 0 || v.TargetIndex == 0 {
			return ErrVectorPartitionSplitInsertConflictV1
		}
		_, err = collection.ProjectVectorPartitionSplitInsertWithCommandWALIntentV1(context.Background(), v, v.TargetTerm, v.TargetIndex, intent)
		return err
	case "clear":
		return collection.CompleteVectorPartitionSplitInsertWithCommandWALIntentV1(context.Background(), v, intent)
	default:
		return ErrVectorPartitionSplitInsertConflictV1
	}
}
