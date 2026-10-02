package collections

import (
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
)

// collectionCommandWALAdmission belongs to one executing operation. Queued
// requests never retain it. Release runs the complete coverage finalizer;
// acquire establishes a fresh baseline and active/search coverage state.
type collectionCommandWALAdmission struct {
	collection     *Collection
	release        func()
	acquire        func() func()
	prepared       bool
	mutation       *collectionMutationUnlock
	mutationLocked *bool
}

// Ordinary admission is an operation-local value. Only prepared owners need
// to retain a custom reacquire function.
func (c *Collection) lockCollectionCommandWALAdmission() collectionCommandWALAdmission {
	return collectionCommandWALAdmission{collection: c, release: c.lockVectorIndexCoverageMutation()}
}

func (a *collectionCommandWALAdmission) unlock() {
	if a != nil && a.release != nil {
		a.release()
		a.release = nil
	}
}

// drainBeforeAssignment is called only after the owning raw/teardown guard has
// unwound. The mutation pointers name the caller's actual deferred lock state.
// In particular, neither mutation nor domain.mu may remain held during Drain:
// synchronous publication can wait for an async publisher in the same domain.
func (a *collectionCommandWALAdmission) drainBeforeAssignment(intent *backenddb.CommandWALIntent, mutation *collectionMutationUnlock, mutationLocked *bool, drain *commandwalbarrier.PendingDrain, revalidate func() error) error {
	if a == nil || a.collection == nil || drain == nil || (intent != nil && intent.AssignedLSN() != 0) {
		return fmt.Errorf("%w: admission handoff requires an owned unassigned operation", backenddb.ErrCommandWALContextMissingFrame)
	}
	held := mutation != nil && mutationLocked != nil && *mutationLocked
	if held {
		mutation.Unlock()
		*mutationLocked = false
	}
	a.unlock()
	err := drain.Drain()
	// Restore the owner's leases even on rejection so existing cleanup and
	// accepted-publication invalidation retain their original ownership.
	if a.acquire != nil {
		a.release = a.acquire()
	} else {
		a.release = a.collection.lockVectorIndexCoverageMutation()
	}
	if held {
		*mutation = a.collection.lockMutation()
		*mutationLocked = true
	}
	if err != nil {
		return err
	}
	if err := a.collection.ensureWriteDomainOpen(); err != nil {
		return err
	}
	if err := a.collection.db.CheckCommandWALPublishReady(); err != nil {
		return err
	}
	if revalidate != nil {
		return revalidate()
	}
	return nil
}

func (c *Collection) lockCommandWALStagingWithAdmission(a *collectionCommandWALAdmission, intent *backenddb.CommandWALIntent, revalidate func() error) (func(), error) {
	if a == nil {
		return c.lockCommandWALStagingAfterForeignDrain()
	}
	for {
		unlockRaw := c.db.LockCommandWALStaging()
		err := c.db.CheckCommandWALPublishReady()
		if err == nil {
			err = c.drainCommandWALStageCoordinatorBeforeMutationWithHeldRawPublishLock()
		}
		if err == nil {
			return unlockRaw, nil
		}
		unlockRaw()
		drain := collectionCommandWALPendingDrain(err)
		if drain == nil {
			return nil, err
		}
		if err := a.drainBeforeAssignment(intent, a.mutation, a.mutationLocked, drain, revalidate); err != nil {
			return nil, err
		}
	}
}

// The append owner of an assigned intent retains its guard. Only an ordinary
// unassigned operation can hand a foreign prefix back to its admission owner.
func (c *Collection) withCommandWALPublishCoordinatorAdmission(intent *backenddb.CommandWALIntent, a *collectionCommandWALAdmission, revalidate func() error, publish func() error) (err error) {
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return publish()
	}
	if a == nil || (intent != nil && intent.StagedForPublish()) {
		return c.withCommandWALPublishCoordinatorForIntent(intent, publish)
	}
	defer func() { err = collectionCommandWALPublicationError(c.db, intent, err) }()
	for {
		unlockRaw := c.db.LockCommandWALPublish()
		if err := c.db.CheckCommandWALPublishReady(); err != nil {
			unlockRaw()
			return err
		}
		unlock, err := c.lockCommandWALPublishCoordinatorWithHeldRawPublishLock()
		if err == nil {
			defer unlockRaw()
			defer unlock()
			if revalidate != nil {
				if err := revalidate(); err != nil {
					return err
				}
			}
			return publish()
		}
		unlockRaw()
		drain := collectionCommandWALPendingDrain(err)
		if drain == nil {
			return err
		}
		if err := a.drainBeforeAssignment(intent, a.mutation, a.mutationLocked, drain, revalidate); err != nil {
			return err
		}
	}
}

// bindMutation names the actual defer-owned state; it never copies a mutex.
// The prior binding is returned by value for deferred restoration.
func (a *collectionCommandWALAdmission) bindMutation(mutation *collectionMutationUnlock, held *bool) (*collectionMutationUnlock, *bool) {
	if a == nil {
		return nil, nil
	}
	previousMutation, previousHeld := a.mutation, a.mutationLocked
	a.mutation, a.mutationLocked = mutation, held
	return previousMutation, previousHeld
}

func (a *collectionCommandWALAdmission) restoreMutation(mutation *collectionMutationUnlock, held *bool) {
	if a != nil {
		a.mutation, a.mutationLocked = mutation, held
	}
}

func (c *Collection) withMutationLockForCommandWALStagingAdmission(raw *func(), intent *backenddb.CommandWALIntent, a *collectionCommandWALAdmission, fn func() error) error {
	mutation, err := c.lockMutationForCommandWALStaging(raw, intent)
	if err != nil {
		return err
	}
	held := true
	defer func() {
		if held {
			mutation.Unlock()
		}
	}()
	previousAdmissionMutation, previousAdmissionHeld := a.bindMutation(&mutation, &held)
	defer a.restoreMutation(previousAdmissionMutation, previousAdmissionHeld)
	return fn()
}

func (c *Collection) withMutationLockAdmission(a *collectionCommandWALAdmission, fn func() error) error {
	if a != nil && a.prepared && a.mutationLocked != nil && *a.mutationLocked {
		return fn()
	}
	return c.withMutationLockForCommandWALStagingAdmission(nil, nil, a, fn)
}

func collectionCommandWALAdmissionArgument(admissions []*collectionCommandWALAdmission) *collectionCommandWALAdmission {
	if len(admissions) == 0 {
		return nil
	}
	return admissions[0]
}

func (c *Collection) notifyAdmittedUpdateResults(items []updateBatchItem, results []UpdateBatchResult) error {
	ids := make([][]byte, 0, len(results))
	for i, result := range results {
		if result.Modified && i < len(items) {
			ids = append(ids, items[i].DocumentID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return commitAmbiguousError("update vector index maintenance", c.notifyVectorIndexesUpsert(ids))
}

// Distinct handles can share a write domain while retaining handle-local ad-hoc
// runtimes. Notify each affected handle before releasing execution admission.
func notifyUpdateCombineResults(batch []collectionUpdateCombineRequest, results []UpdateBatchResult) error {
	byCollection := make(map[*Collection][][]byte)
	for i, req := range batch {
		if i < len(results) && results[i].Modified {
			byCollection[req.collection] = append(byCollection[req.collection], req.documentIDBytes())
		}
	}
	var result error
	for collection, ids := range byCollection {
		err := commitAmbiguousError("combined update vector maintenance", collection.notifyVectorIndexesUpsert(ids))
		result = errors.Join(result, collection.invalidateVectorIndexCoverageOnAcceptedMutation(err))
	}
	return result
}
