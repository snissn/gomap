package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/tree"
)

type colocatedVectorMutationPublicationV1 struct {
	wal                  commitlog.ColocatedVectorMutationWALV1
	manifest             VectorPartitionManifestV1
	key, usageKey        string
	previousUsage        []byte
	usagePresent         bool
	count, retainedBytes uint64
	previousChain        [sha256.Size]byte
	outcome              commitlog.ColocatedVectorMutationOutcomeV1
}

func colocatedVectorMutationAdmissionV1(a *collectionCommandWALAdmission) *colocatedVectorMutationPublicationV1 {
	if a == nil {
		return nil
	}
	return a.colocated
}

func validateColocatedVectorMutationUpdatePlanV1(a *collectionCommandWALAdmission, plan *updateBatchPlan) error {
	p := colocatedVectorMutationAdmissionV1(a)
	if p == nil {
		return nil
	}
	if plan == nil || len(plan.results) != 1 || p.wal.Delete {
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	matched, affected := uint64(0), uint64(0)
	if plan.results[0].Matched {
		matched = 1
	}
	if plan.results[0].Modified {
		affected = 1
	}
	if matched != p.wal.Matched || affected != p.wal.Affected {
		return ErrVectorIndexPartitionLiveMismatchV1
	}
	return nil
}

// Scoped replacements compare the same reconstructed JSON content as their
// visibility proof. Column reads may reorder fields and round declared FP32
// values; neither is a modification. The original command bytes stay intact.
func colocatedVectorMutationUpdateItemsV1(meta CollectionMeta, v commitlog.ColocatedVectorMutationWALV1, ids, documents [][]byte) ([]UpdateBatchItem, error) {
	if v.Delete || len(ids) != 1 || len(documents) != 1 || !bytes.Equal(ids[0], v.ID) || !bytes.Equal(documents[0], v.Document) {
		return nil, ErrVectorIndexPartitionLiveMismatchV1
	}
	id, document := bytes.Clone(ids[0]), bytes.Clone(documents[0])
	return []UpdateBatchItem{{DocumentID: id, Update: func(current []byte) ([]byte, bool, error) {
		if current == nil {
			return nil, false, nil
		}
		equal, err := vectorPartitionLiveDocumentEqualV1(meta, id, document, current)
		if err != nil {
			return nil, false, err
		}
		if equal {
			return current, false, nil
		}
		return document, true, nil
	}}}, nil
}

func colocatedVectorMutationCollectionPrefixV1(collection string) string {
	digest := sha256.Sum256([]byte(collection))
	return "vector_colocated_outcome_v1:" + hex.EncodeToString(digest[:]) + ":"
}

func colocatedVectorMutationKeysV1(collection string, attempt []byte) (string, string) {
	prefix := colocatedVectorMutationCollectionPrefixV1(collection)

	digest := sha256.Sum256(attempt)
	return prefix + "attempt:" + hex.EncodeToString(digest[:]), prefix + "usage"
}

// ReadVectorPartitionColocatedOutcomeV1 is an exact bounded lookup. Only a
// matching witness covered by this handle's durable command-WAL state is usable.
// Callers must additionally fence the FSM's current DB and ACTIVE placement.
func (c *Collection) ReadVectorPartitionColocatedOutcomeV1(scope commitlog.ColocatedVectorMutationScopeV1, attempt []byte, digest [sha256.Size]byte) (commitlog.ColocatedVectorMutationOutcomeV1, bool, error) {
	var zero commitlog.ColocatedVectorMutationOutcomeV1
	if c == nil || c.db == nil || scope.ValidateV1() != nil || len(attempt) == 0 || len(attempt) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 {
		return zero, false, ErrVectorIndexPartitionLiveUnavailableV1
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return zero, false, backenddb.ErrClosed
	}
	defer snap.Close()
	key, usageKey := colocatedVectorMutationKeysV1(c.collectionName(), attempt)
	raw, found, err := getSystemValue(snap, key)
	if err != nil || !found {
		return zero, false, err
	}
	v, err := commitlog.DecodeColocatedVectorMutationOutcomeV1(raw)
	state, ok := snap.StateToken()
	if err != nil || !ok || v.ScopeDigest != scope.Digest || (digest != ([sha256.Size]byte{}) && v.CommandDigest != digest) || state.AppliedCommandLSN < v.AppliedCommandLSN {
		return zero, false, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
	}
	usage, present, err := getSystemValue(snap, usageKey)
	count, _, _, summaryErr := decodeColocatedVectorMutationSummaryV1(usage)
	if err != nil || !present || summaryErr != nil || v.Ordinal > count {
		return zero, false, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err, summaryErr)
	}
	return v, true, nil
}

// PrepareVectorPartitionColocatedMutationV1 derives original counts while the
// actual prepared source owner holds admission. Term/index are supplied only by
// the applying FSM. A replay validates the WAL's retained counts against this
// source; a covered outcome is handled before entering this method.
func (owner *CommandWALAdmittedCollection) PrepareVectorPartitionColocatedMutationV1(ctx context.Context, v commitlog.ColocatedVectorMutationWALV1, replay bool) (commitlog.ColocatedVectorMutationWALV1, error) {
	if err := v.ValidateV1(); err != nil {
		return v, err
	}
	return owner.prepareVectorPartitionColocatedMutationV1(ctx, v, replay, true)
}

func (owner *CommandWALAdmittedCollection) PreflightVectorPartitionColocatedMutationV1(ctx context.Context, v commitlog.ColocatedVectorMutationWALV1) error {
	_, err := owner.prepareVectorPartitionColocatedMutationV1(ctx, v, false, false)
	return err
}

func (owner *CommandWALAdmittedCollection) prepareVectorPartitionColocatedMutationV1(ctx context.Context, v commitlog.ColocatedVectorMutationWALV1, replay, publish bool) (commitlog.ColocatedVectorMutationWALV1, error) {
	if err := owner.validate(); err != nil {
		return v, err
	}
	if err := v.ValidateInputsV1(); err != nil {
		return v, err
	}
	c := owner.collection
	if v.Collection != c.collectionName() {
		return v, ErrVectorIndexPartitionLiveMismatchV1
	}
	// Replicated catalog admission selects this exact prepared generation; the
	// standalone local ActiveGeneration pointer is not cluster authority. The
	// prepared owner already holds mutation admission, so use the same READY
	// selector as the replicated-live router without reacquiring that lock.
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return v, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
	}
	loaded, present, err := store.loadVectorPartitionLifecycleAuthorityWithContextV1(ctx, c.name, v.Scope.Index)
	if err != nil {
		return v, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
	}
	entry, ready := loaded.state.Generations[v.Scope.Generation]
	if !present || !ready || entry.Manifest == nil || entry.Scope != nil || entry.Deleting || entry.Manifest.State != "ready" {
		return v, ErrVectorIndexPartitionLiveMismatchV1
	}
	manifest, err := vectorPartitionLifecycleManifestWithContextV1(ctx, loaded.state, v.Scope.Generation, false)
	if err != nil {
		return v, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
	}
	if len(manifest.Placements) != int(manifest.PartitionCount) {
		return v, ErrVectorIndexPartitionLiveMismatchV1
	}
	for i, placement := range manifest.Placements {
		if placement.PartitionID != uint32(i) || placement.GroupID != v.Scope.OwnerGroup {
			return v, ErrVectorIndexPartitionLiveMismatchV1
		}
	}
	if err := c.validateCurrentVectorPartitionLiveBindingV1(ctx, manifest); err != nil {
		return v, err
	}
	// Indexed schemas normalize BufferedIndexedWrites even when ColumnStore
	// writes publish synchronously. Prepared admission flushed buffered work;
	// these physical-column mutations use the immediate WAL/root publication.
	if !columnStoreWriteEnabled(c.meta) || !VectorPartitionLiveDocumentProofSupportedV1(c.meta) {
		return v, ErrVectorIndexPartitionLiveUnavailableV1
	}
	matched, affected := uint64(0), uint64(0)
	if v.Delete {
		current, err := owner.Get(v.ID)
		if err != nil {
			return v, err
		}
		if current != nil {
			affected = 1
		}
	} else {
		items, err := colocatedVectorMutationUpdateItemsV1(c.meta, v, [][]byte{v.ID}, [][]byte{v.Document})
		if err != nil {
			return v, err
		}
		owned, err := prepareUpdateBatchItems(items)
		if err != nil {
			return v, err
		}
		plan, err := c.buildUpdateBatchPlan(owned, updateBatchModeAny, false, nil)
		if err != nil {
			return v, err
		}
		if plan == nil {
			return v, ErrVectorIndexPartitionLiveUnavailableV1
		}
		if len(plan.results) != 1 {
			plan.close()
			return v, ErrVectorIndexPartitionLiveMismatchV1
		}
		if plan.results[0].Matched {
			matched = 1
		}
		if plan.results[0].Modified {
			affected = 1
		}
		plan.close()
	}
	if replay && (v.Matched != matched || v.Affected != affected) {
		return v, ErrVectorIndexPartitionLiveMismatchV1
	}
	v.Matched, v.Affected = matched, affected
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return v, backenddb.ErrClosed
	}
	defer snap.Close()
	key, usageKey := colocatedVectorMutationKeysV1(c.collectionName(), v.Attempt)
	_, found, err := getSystemValue(snap, key)
	if err != nil || found {
		return v, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
	}
	raw, present, err := getSystemValue(snap, usageKey)
	if err != nil {
		return v, err
	}
	var count, retained uint64
	var chain [sha256.Size]byte
	if present {
		count, retained, chain, err = decodeColocatedVectorMutationSummaryV1(raw)
		if err != nil {
			return v, err
		}
	}
	additional := uint64(len(key) + commitlog.ColocatedVectorMutationOutcomeBytesV1)
	if !present {
		additional += uint64(len(usageKey) + colocatedVectorMutationSummaryBytesV1)
	}
	if count >= commitlog.ColocatedVectorMutationMaxOutcomesV1 || retained > commitlog.ColocatedVectorMutationMaxMetadataBytesV1 || additional > commitlog.ColocatedVectorMutationMaxMetadataBytesV1-retained {
		return v, ErrVectorIndexPartitionLiveCapacityV1
	}
	if affected != 0 {
		idx := c.registeredVectorIndex(v.Scope.Index)
		if idx == nil {
			return v, ErrVectorIndexPartitionLiveUnavailableV1
		}
		idx.mu.RLock()
		if idx.partitionLive == nil {
			idx.mu.RUnlock()
			return v, ErrVectorIndexPartitionLiveUnavailableV1
		}
		_, known := idx.partitionLive.ownerV1(string(v.ID))
		owners, ownerBytes, nodes, nodeBytes := 0, uint64(0), 0, uint64(0)
		if !known {
			owners, ownerBytes = 1, uint64(len(v.ID))
		}
		if !v.Delete {
			nodes, nodeBytes = 1, uint64(len(v.ID))
		}
		fits := idx.partitionLive.ownerCountV1()+owners <= vectorIndexPartitionLiveMaxMutatedIDsV1 && idx.partitionLiveNodeCountLocked()+nodes <= idx.partitionLiveNodeCapacityLocked() && idx.partitionLiveBytesFitLocked(nodes, owners, nodeBytes, ownerBytes)
		idx.mu.RUnlock()
		if !fits {
			return v, ErrVectorIndexPartitionLiveCapacityV1
		}
	}
	if publish {
		owner.admission.colocated = &colocatedVectorMutationPublicationV1{wal: v, manifest: manifest, key: key, usageKey: usageKey, previousUsage: bytes.Clone(raw), usagePresent: present, count: count + 1, retainedBytes: retained + additional, previousChain: chain}
	}
	return v, nil
}

func (c *Collection) appendColocatedVectorMutationSystemDeltaV1(it iterator.UnsafeIterator, p *colocatedVectorMutationPublicationV1, attempt *vectorPartitionLiveReplayAttemptV1, lsn uint64) (iterator.UnsafeIterator, error) {
	if p == nil {
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
		return fail(ErrVectorIndexPartitionLiveMismatchV1)
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return fail(backenddb.ErrClosed)
	}
	defer snap.Close()
	_, present, err := getSystemValue(snap, p.key)
	if err != nil || present {
		return fail(errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err))
	}
	previous, present, err := getSystemValue(snap, p.usageKey)
	if err != nil || present != p.usagePresent || !bytes.Equal(previous, p.previousUsage) {
		return fail(errors.Join(ErrConcurrentMutation, err))
	}
	var status VectorIndexPartitionLiveStatusV1
	if p.wal.Affected != 0 {
		if attempt == nil {
			return fail(ErrVectorIndexPartitionLiveMismatchV1)
		}
		for _, entry := range attempt.entries {
			if entry.spec.definition.Name != p.wal.Scope.Index {
				continue
			}
			live := entry.candidate.partitionLive
			if live != nil {
				status = VectorIndexPartitionLiveStatusV1{Generation: live.generation, Coverage: live.coverage, Revision: live.revision}
			}
		}
	} else {
		idx := c.registeredVectorIndex(p.wal.Scope.Index)
		if idx != nil {
			idx.mu.RLock()
			if live := idx.partitionLive; live != nil && !live.invalid && live.bindingDurable {
				status = VectorIndexPartitionLiveStatusV1{Generation: live.generation, Coverage: live.coverage, Revision: live.revision}
			}
			idx.mu.RUnlock()
		}
	}
	if status.Generation != p.wal.Scope.Generation {
		return fail(ErrVectorIndexPartitionLiveMismatchV1)
	}
	p.outcome = commitlog.ColocatedVectorMutationOutcomeV1{ScopeDigest: p.wal.Scope.Digest, CommandDigest: p.wal.CommandDigest, ExpectedCatalogVersion: p.wal.ExpectedCatalogVersion, Term: p.wal.Term, Index: p.wal.Index, Matched: p.wal.Matched, Affected: p.wal.Affected, Coverage: status.Coverage, Revision: status.Revision, Ordinal: p.count, AppliedCommandLSN: lsn}
	raw, err := commitlog.EncodeColocatedVectorMutationOutcomeV1(p.outcome)
	if err != nil {
		return fail(err)
	}
	chain := appendColocatedVectorMutationChainV1(p.previousChain, colocatedVectorMutationRecordHashV1([]byte(p.key), raw))
	usage := encodeColocatedVectorMutationSummaryV1(p.count, p.retainedBytes, chain)
	base.entries = append(base.entries, systemTargetEntry{key: []byte(p.key), value: raw}, systemTargetEntry{key: []byte(p.usageKey), value: usage})
	sort.Slice(base.entries, func(i, j int) bool { return bytes.Compare(base.entries[i].key, base.entries[j].key) < 0 })
	for i := 1; i < len(base.entries); i++ {
		if bytes.Equal(base.entries[i-1].key, base.entries[i].key) {
			return fail(ErrVectorIndexPartitionLiveMismatchV1)
		}
	}
	return base, nil
}

func (c *Collection) publishColocatedVectorMutationNoopV1(intent *backenddb.CommandWALIntent, admission *collectionCommandWALAdmission) error {
	if admission == nil || admission.colocated == nil {
		return c.db.PublishStagedCommandWALNoop(intent, false)
	}
	p := admission.colocated
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord.partitionLivePublishMu.Lock()
	defer coord.partitionLivePublishMu.Unlock()
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer snap.Close()
	state, ok := snap.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	newSystemRoot, _, err := c.db.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, func() error {
		current, ok := c.db.StateToken()
		if !ok || current != state {
			return ErrConcurrentMutation
		}
		return nil
	}, intent, func(ctx backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		return c.appendColocatedVectorMutationSystemDeltaV1(&systemTargetIterator{}, p, nil, ctx.AppliedCommandLSN)
	})
	if err != nil {
		if backenddb.CommitPublicationAccepted(err) {
			c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		}
		return err
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return commitAmbiguousError("colocated outcome catalog", err)
	}
	if catalog != nil {
		c.rememberCatalogAtSystemRoot(newSystemRoot, catalog)
		c.noteWriteDomainCatalog(newSystemRoot, catalog)
	}
	// Metadata-only publication advances the system root without changing the
	// source/live revision. Refresh only the process-local matching state fence.
	if err := c.validateCurrentVectorPartitionLiveBindingV1(context.Background(), p.manifest); err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		return commitAmbiguousError("colocated outcome carrier state", err)
	}
	return nil
}

// ProvePreparedVectorPartitionColocatedMutationV1 uses only the source owner's
// frozen scope/counts. It must run before releasing prepared admission.
func (owner *CommandWALAdmittedCollection) ProvePreparedVectorPartitionColocatedMutationV1(ctx context.Context, matched, affected int64) (VectorIndexPartitionLiveStatusV1, error) {
	if err := owner.validate(); err != nil {
		return VectorIndexPartitionLiveStatusV1{}, err
	}
	p := owner.admission.colocated
	if p == nil {
		return VectorIndexPartitionLiveStatusV1{}, ErrVectorIndexPartitionLiveUnavailableV1
	}
	return owner.ProveVectorPartitionColocatedMutationV1(ctx, p.manifest, p.wal.ID, p.wal.Document, p.wal.Delete, matched, affected)
}

// The summary is authoritative only because it shares the witness/source/live
// SystemRoot publication. Full recomputation occurs at reopen/snapshot boundaries.
const colocatedVectorMutationSummaryBytesV1 = 8 + 8 + 8 + sha256.Size

func encodeColocatedVectorMutationSummaryV1(count, retained uint64, chain [sha256.Size]byte) []byte {
	b := binary.LittleEndian.AppendUint64(nil, 1)
	b = binary.LittleEndian.AppendUint64(b, count)
	b = binary.LittleEndian.AppendUint64(b, retained)
	return append(b, chain[:]...)
}

func decodeColocatedVectorMutationSummaryV1(raw []byte) (count, retained uint64, chain [sha256.Size]byte, err error) {
	if len(raw) != colocatedVectorMutationSummaryBytesV1 || binary.LittleEndian.Uint64(raw) != 1 {
		return 0, 0, chain, ErrVectorIndexPartitionLiveMismatchV1
	}
	count, retained = binary.LittleEndian.Uint64(raw[8:]), binary.LittleEndian.Uint64(raw[16:])
	copy(chain[:], raw[24:])
	if count == 0 || count > commitlog.ColocatedVectorMutationMaxOutcomesV1 || retained == 0 || retained > commitlog.ColocatedVectorMutationMaxMetadataBytesV1 || chain == ([sha256.Size]byte{}) {
		err = ErrVectorIndexPartitionLiveMismatchV1
	}
	return
}

// Physical command LSNs differ across replicas and are excluded from the chain.
func colocatedVectorMutationRecordHashV1(key, raw []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap:colocated-outcome-record:v1\x00"))
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(key)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(key)
	_, _ = h.Write(raw[:len(raw)-8])
	var zero [8]byte
	_, _ = h.Write(zero[:])
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

func appendColocatedVectorMutationChainV1(previous, record [sha256.Size]byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap:colocated-outcome-chain:v1\x00"))
	_, _ = h.Write(previous[:])
	_, _ = h.Write(record[:])
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

// VectorPartitionColocatedMutationLogicalStateV1 reads a fixed-size summary for
// ordinary/scoped apply and retry digests; it never walks retained witnesses.
func (c *Collection) VectorPartitionColocatedMutationLogicalStateV1(ctx context.Context) ([][]byte, error) {
	state, _, err := c.colocatedVectorMutationLogicalStateV1(ctx, false)
	return state, err
}

// VerifyVectorPartitionColocatedMutationLogicalStateV1 validates the full bounded
// witness set at immutable snapshot and FSM reopen trust boundaries. It returns
// exactly the same logical summary as the live path.
func (c *Collection) VerifyVectorPartitionColocatedMutationLogicalStateV1(ctx context.Context) ([][]byte, commitlog.ColocatedVectorMutationOutcomeV1, error) {
	return c.colocatedVectorMutationLogicalStateV1(ctx, true)
}

func (c *Collection) colocatedVectorMutationLogicalStateV1(ctx context.Context, verify bool) ([][]byte, commitlog.ColocatedVectorMutationOutcomeV1, error) {
	var latest commitlog.ColocatedVectorMutationOutcomeV1
	if c == nil || c.db == nil {
		return nil, latest, backenddb.ErrClosed
	}
	if !columnStoreWriteEnabled(c.meta) {
		return nil, latest, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, latest, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, latest, backenddb.ErrClosed
	}
	defer snap.Close()
	state, ok := snap.StateToken()
	if !ok {
		return nil, latest, backenddb.ErrClosed
	}
	if state.SystemRootPageID == 0 {
		return nil, latest, nil
	}
	prefix := []byte(colocatedVectorMutationCollectionPrefixV1(c.collectionName()))
	usageKey := string(prefix) + "usage"
	summary, present, err := getSystemValue(snap, usageKey)
	if err != nil {
		return nil, latest, err
	}
	var count, retained uint64
	var declaredChain [sha256.Size]byte
	if present {
		count, retained, declaredChain, err = decodeColocatedVectorMutationSummaryV1(summary)
		if err != nil {
			return nil, latest, err
		}
	}
	if !verify {
		if !present {
			return nil, latest, nil
		}
		return [][]byte{[]byte(usageKey), summary}, latest, nil
	}
	attemptPrefix := append(bytes.Clone(prefix), []byte("attempt:")...)
	it, err := snap.IteratorAtRootWithOptions(state.SystemRootPageID, prefix, prefixEnd(prefix), backenddb.IteratorOptions{IncludeTombstones: true})
	if errors.Is(err, tree.ErrKeyNotFound) && !present {
		return nil, latest, nil
	}
	if err != nil {
		return nil, latest, err
	}
	defer it.Close()
	// At the supported ceiling this retains 2 MiB of hashes, never the 32 MiB
	// witness population. Ordinals make recomputation independent of key order.
	hashes := make([][sha256.Size]byte, int(count))
	var observed, bytesSeen uint64
	usageSeen := false
	for it.Valid() && bytes.HasPrefix(it.UnsafeKey(), prefix) {
		if err := ctx.Err(); err != nil {
			return nil, latest, err
		}
		if it.IsDeleted() {
			return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
		}
		key, raw := it.UnsafeKey(), it.ValueCopy(nil)
		bytesSeen += uint64(len(key) + len(raw))
		if bytesSeen > commitlog.ColocatedVectorMutationMaxMetadataBytesV1 {
			return nil, latest, ErrVectorIndexPartitionLiveCapacityV1
		}
		if string(key) == usageKey {
			if usageSeen || !present || !bytes.Equal(raw, summary) {
				return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
			}
			usageSeen = true
		} else {
			if !present || len(key) != len(attemptPrefix)+64 || !bytes.HasPrefix(key, attemptPrefix) {
				return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
			}
			for _, digit := range key[len(attemptPrefix):] {
				if !((digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f')) {
					return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
				}
			}
			v, err := commitlog.DecodeColocatedVectorMutationOutcomeV1(raw)
			if err != nil || v.AppliedCommandLSN > state.AppliedCommandLSN || v.Ordinal > count {
				return nil, latest, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
			}
			if hashes[v.Ordinal-1] != ([sha256.Size]byte{}) {
				return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
			}
			hashes[v.Ordinal-1] = colocatedVectorMutationRecordHashV1(key, raw)
			if v.AppliedCommandLSN > latest.AppliedCommandLSN {
				latest = v
			}
			observed++
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, latest, err
	}
	if !present {
		if bytesSeen != 0 {
			return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
		}
		return nil, latest, nil
	}
	if !usageSeen || observed != count || bytesSeen != retained {
		return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
	}
	var chain [sha256.Size]byte
	for _, hash := range hashes {
		if hash == ([sha256.Size]byte{}) {
			return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
		}
		chain = appendColocatedVectorMutationChainV1(chain, hash)
	}
	if chain != declaredChain {
		return nil, latest, ErrVectorIndexPartitionLiveMismatchV1
	}
	return [][]byte{[]byte(usageKey), summary}, latest, nil
}
