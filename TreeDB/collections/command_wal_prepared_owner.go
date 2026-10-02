package collections

import (
	"context"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// CommandWALAdmittedCollection is usable only inside its prepared callback.
// It refers to the original collection and actual operation-owned locks. The
// callback must finalize or abort its appended frame before returning.
type CommandWALAdmittedCollection struct {
	collection           *Collection
	admission            *collectionCommandWALAdmission
	partitionStorageHeld bool
	preparation          *VectorPartitionPrepareCompletionV1
	partitionSourcePin   *backenddb.Snapshot
	partitionCapture     *backenddb.StableResourceCaptureLease
	replayOperation      *backenddb.CommandWALIntent
	staging              *commandwalapply.StagingGuard
}

// WithPreparedCommandWALMutation owns schema, vector coverage/admission and
// mutation before the caller appends a frame, through its apply and Finalize or
// Abort. Pre-assignment foreign drains unwind only this operation's leases.
func (c *Collection) WithPreparedCommandWALMutation(apply func(*CommandWALAdmittedCollection) error) error {
	return c.withPreparedCommandWALMutation(c.lockVectorIndexCoverageMutation, false, apply)
}

func (c *Collection) withPreparedCommandWALMutation(acquire func() func(), coveragePersistence bool, apply func(*CommandWALAdmittedCollection) error) error {
	return c.withPreparedCommandWALMutationAndReplayIntent(acquire, coveragePersistence, nil, apply)
}
func (c *Collection) withPreparedCommandWALMutationAndReplayIntent(acquire func() func(), coveragePersistence bool, replay *backenddb.CommandWALIntent, apply func(*CommandWALAdmittedCollection) error) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	if apply == nil {
		return errors.New("collections: prepared command WAL callback is nil")
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	admissionState := collectionCommandWALAdmission{collection: c, acquire: acquire, release: acquire(), prepared: true}
	admission := &admissionState
	defer admission.unlock()
	mutation := c.lockMutation()
	held := true
	defer func() {
		if held {
			mutation.Unlock()
		}
	}()
	previousAdmissionMutation, previousAdmissionHeld := admission.bindMutation(&mutation, &held)
	defer admission.restoreMutation(previousAdmissionMutation, previousAdmissionHeld)
	if err := c.ensureWriteDomainOpen(); err != nil {
		return err
	}
	var err error
	_, fromReplay := replay.ReplayAssignedLSN()
	if fromReplay {
		if err := c.db.ValidateCommandWALReplayOperationV1(replay); err != nil {
			return err
		}
		err = c.flushBufferedWritesWithRawPublishStateAndCoverage(false, coveragePersistence, true, false)
	} else if coveragePersistence {
		err = c.flushBufferedWritesWithCoverageLocked()
	} else {
		err = c.flushBufferedWritesWithVectorAdmissionLocked()
	}
	if err != nil {
		return err
	}
	owner := &CommandWALAdmittedCollection{collection: c, admission: admission, replayOperation: replay}
	if !fromReplay && c.db.CommandWALEnabled() {
		actualGuard, err := c.lockCommandWALStagingGuardWithAdmission(admission)
		if err != nil {
			return err
		}
		staging, err := commandwalapply.NewStagingGuard(c.db, actualGuard)
		if err != nil {
			actualGuard.Release()
			return err
		}
		defer staging.Release()
		owner.staging = staging
	}
	return apply(owner)
}

func (owner *CommandWALAdmittedCollection) validate() error {
	if owner == nil || owner.collection == nil || owner.admission == nil || owner.admission.release == nil || owner.admission.mutationLocked == nil || !*owner.admission.mutationLocked {
		return errors.New("collections: prepared command WAL owner is no longer active")
	}
	if owner.replayOperation != nil {
		if err := owner.collection.db.ValidateCommandWALReplayOperationV1(owner.replayOperation); err != nil {
			return err
		}
	}
	return owner.collection.ensureWriteDomainOpen()
}

// CommandWALAppendOptions transfers this callback's already-drained staging
// lease to the local append handle. Callers must pass these options to Append;
// ordinary Append would recursively acquire the non-reentrant staging mutex.
func (owner *CommandWALAdmittedCollection) CommandWALAppendOptions(sync bool) (commandwalapply.Options, error) {
	if err := owner.validate(); err != nil {
		return commandwalapply.Options{}, err
	}
	if owner.staging == nil {
		return commandwalapply.Options{}, errors.New("collections: prepared command WAL staging guard is missing")
	}
	return commandwalapply.Options{Sync: sync, Staging: owner.staging}, nil
}

func (owner *CommandWALAdmittedCollection) Meta() CollectionMeta { return owner.collection.Meta() }
func (owner *CommandWALAdmittedCollection) MetaView() CollectionMeta {
	return owner.collection.MetaView()
}
func (owner *CommandWALAdmittedCollection) Get(id []byte) ([]byte, error) {
	if err := owner.validate(); err != nil {
		return nil, err
	}
	return owner.collection.Get(id)
}
func (owner *CommandWALAdmittedCollection) PreflightCommandWALMutation(operation ColumnPublishOperation) error {
	if err := owner.validate(); err != nil {
		return err
	}
	c := owner.collection
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		return err
	}
	return requireColumnStoreWriteOperationSupported(c.meta, operation)
}
func (owner *CommandWALAdmittedCollection) PreflightInsertBatchConflicts(ids, documents [][]byte, trusted bool) error {
	if err := owner.validate(); err != nil {
		return err
	}
	return owner.collection.preflightInsertBatchConflictsLocked(ids, documents, trusted)
}
func (owner *CommandWALAdmittedCollection) PreflightReplaceBatchConflicts(ids, documents [][]byte) error {
	if err := owner.validate(); err != nil {
		return err
	}
	items, err := replaceBatchUpdateItems(ids, documents)
	if err != nil {
		return err
	}
	owned, err := prepareUpdateBatchItems(items)
	if err != nil {
		return err
	}
	return owner.collection.preflightReplaceBatchConflictsLocked(owned)
}
func (owner *CommandWALAdmittedCollection) PrepareBSONSetUpdateBatchCommandWAL(items []BSONSetUpdateBatchItem) ([]UpdateBatchResult, []commitlog.CollectionDocument, error) {
	if err := owner.validate(); err != nil {
		return nil, nil, err
	}
	c := owner.collection
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return nil, nil, err
	}
	owned, err := prepareBSONSetUpdateBatchItems(items)
	if err != nil {
		return nil, nil, err
	}
	if len(owned) == 0 {
		return nil, nil, nil
	}
	plan, err := c.buildUpdateBatchPlan(owned, updateBatchModeAny, false, nil)
	if err != nil {
		return nil, nil, err
	}
	defer plan.close()
	return cloneBSONSetUpdateBatchResults(plan.results), cloneBSONSetUpdateCommandWALDocuments(plan.commandWALDocuments), nil
}
func (owner *CommandWALAdmittedCollection) InsertBatchWithCommandWALIntent(ids, documents [][]byte, trusted bool, intent *backenddb.CommandWALIntent) ([][]byte, error) {
	if err := owner.validate(); err != nil {
		return nil, err
	}
	if intent == nil {
		return nil, errors.New("collections: admitted insert requires command WAL intent")
	}
	c := owner.collection
	result, err := c.insertBatchWithCommandWALIntentSchemaLocked(ids, documents, trusted, nil, intent, insertBatchExecutionOptions{admission: owner.admission, borrowMutation: true, returnResultIDs: true})
	if err == nil {
		err = commitAmbiguousError("admitted insert vector maintenance", c.notifyVectorIndexesUpsert(result))
	}
	return result, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}
func (owner *CommandWALAdmittedCollection) replaceBatch(ids, documents [][]byte, intent *backenddb.CommandWALIntent) ([]UpdateBatchResult, error) {
	if err := owner.validate(); err != nil {
		return nil, err
	}
	if intent == nil {
		return nil, errors.New("collections: admitted update requires command WAL intent")
	}
	c := owner.collection
	items, err := replaceBatchUpdateItems(ids, documents)
	if err != nil {
		return nil, err
	}
	owned, err := prepareUpdateBatchItems(items)
	if err != nil {
		return nil, err
	}
	if err := c.requireColumnStoreCommandWAL(c.meta, intent); err != nil {
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationUpdate); err != nil {
		return nil, err
	}
	results, batched, err := c.updateBatchOwnedItemsWithCommandWALIntent(owned, updateBatchModeAny, intent, owner.admission)
	if err == nil && !batched {
		err = errors.New("collections: admitted update unexpectedly unbatched")
	}
	if err == nil {
		err = commitAmbiguousError("admitted update vector maintenance", c.notifyVectorIndexesUpdateBatch(items, results))
	}
	return results, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}
func (owner *CommandWALAdmittedCollection) ReplaceBatchWithCommandWALIntent(ids, documents [][]byte, intent *backenddb.CommandWALIntent) (int, int, error) {
	results, err := owner.replaceBatch(ids, documents, intent)
	if err != nil {
		return 0, 0, err
	}
	matched, modified := 0, 0
	for _, result := range results {
		if result.Matched {
			matched++
		}
		if result.Modified {
			modified++
		}
	}
	return matched, modified, nil
}
func (owner *CommandWALAdmittedCollection) UpdateBSONSetBatchWithCommandWALIntent(items []BSONSetUpdateBatchItem, documents []commitlog.CollectionDocument, intent *backenddb.CommandWALIntent) ([]UpdateBatchResult, error) {
	if err := owner.validate(); err != nil {
		return nil, err
	}
	if err := owner.collection.validateBSONSetDocumentFormat(); err != nil {
		return nil, err
	}
	if _, err := prepareBSONSetUpdateBatchItems(items); err != nil {
		return nil, err
	}
	if err := validateBSONSetCommandWALDocumentsForItems(items, documents); err != nil {
		return nil, err
	}
	ids, retained := bsonSetCommandWALDocumentsBatchInput(documents)
	return owner.replaceBatch(ids, retained, intent)
}
func (owner *CommandWALAdmittedCollection) DeleteBatchWithCommandWALIntent(ids [][]byte, intent *backenddb.CommandWALIntent) (int, error) {
	if err := owner.validate(); err != nil {
		return 0, err
	}
	if intent == nil {
		return 0, errors.New("collections: admitted delete requires command WAL intent")
	}
	owned, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(owned))
	for _, id := range owned {
		if len(id) == 0 {
			return 0, errors.New("collections: document id cannot be empty")
		}
		if _, ok := seen[string(id)]; ok {
			return 0, ErrDuplicateDocumentID
		}
		seen[string(id)] = struct{}{}
	}
	c := owner.collection
	deleted, err := c.deleteBatchWithCommandWALIntent(owned, intent, owner.admission)
	if err == nil && deleted > 0 {
		err = commitAmbiguousError("admitted delete vector maintenance", c.notifyVectorIndexesDelete(owned))
	}
	return deleted, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// The pre-owner phase restores persisted carriers. Locked validation below
// checks the current binding without recursively acquiring native admission.
func (c *Collection) WithPreparedCommandWALSplitMutationV1(ctx context.Context, v commitlog.SplitVectorInsertV1, apply func(*CommandWALAdmittedCollection) error) error {
	if err := c.PreflightVectorPartitionSplitInsertV1(ctx, v); err != nil {
		return err
	}
	acquire := c.lockVectorIndexCoverageMutation
	persistence := v.Operation != "source"
	if persistence {
		acquire = func() func() {
			releaseAdmission := c.lockNativeVectorAdmissionWrite()
			releaseCoverage := c.lockVectorIndexCoveragePersistence()
			return func() { releaseCoverage(); releaseAdmission() }
		}
	}
	return c.withPreparedCommandWALMutation(acquire, persistence, func(owner *CommandWALAdmittedCollection) error {
		if err := c.preflightVectorPartitionSplitInsertV1(ctx, v, true); err != nil {
			return err
		}
		return apply(owner)
	})
}
func (owner *CommandWALAdmittedCollection) InsertVectorPartitionSplitSourceWithCommandWALIntentV1(ctx context.Context, v commitlog.SplitVectorInsertV1, intent *backenddb.CommandWALIntent) error {
	if err := owner.validate(); err != nil {
		return err
	}
	return owner.collection.insertVectorPartitionSplitSourceWithOwnerV1(ctx, v, intent, owner)
}
func (owner *CommandWALAdmittedCollection) ProjectVectorPartitionSplitInsertWithCommandWALIntentV1(ctx context.Context, v commitlog.SplitVectorInsertV1, term, index uint64, intent *backenddb.CommandWALIntent) (VectorPartitionSplitInsertReceiptV1, error) {
	if err := owner.validate(); err != nil {
		return VectorPartitionSplitInsertReceiptV1{}, err
	}
	return owner.collection.projectVectorPartitionSplitInsertWithOwnerV1(ctx, v, term, index, intent, owner)
}
func (owner *CommandWALAdmittedCollection) CompleteVectorPartitionSplitInsertWithCommandWALIntentV1(ctx context.Context, v commitlog.SplitVectorInsertV1, intent *backenddb.CommandWALIntent) error {
	if err := owner.validate(); err != nil {
		return err
	}
	return owner.collection.completeVectorPartitionSplitInsertWithOwnerV1(ctx, v, intent, owner)
}
