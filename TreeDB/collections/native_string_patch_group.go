package collections

import (
	"bytes"
	"math"
	"slices"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// This participant extends the existing command-WAL coordinator. It owns no
// durable identity until the ordinary root publisher accepts the complete cut.
const nativeStringPatchMaxRequests = 4

var nativeStringPatchBeforeSealTestHook atomic.Pointer[func()]
var nativeStringPatchInputCopiedTestHook atomic.Pointer[func()]
var nativeStringPatchAfterPrepareTestHook atomic.Pointer[func()]
var nativeStringPatchAdmissionWaitingTestHook atomic.Pointer[func()]

type nativeStringPatchAdmission struct {
	coord  *collectionCommandWALCoordinator
	ticket uint64
	joined bool
}

type nativeStringPatchGroup struct {
	domain   *collectionWriteDomain
	requests [nativeStringPatchMaxRequests]*nativeStringPatchRequest
	count    int
	sealed   bool
}

type nativeStringPatchRequest struct {
	owned      *nativePreparedStringPatchInput
	collection *Collection
	schema     uint64
	input      []TypedStringPatch
	result     TypedStringPatchResult
	err        error
	completed  bool
}

func (c *Collection) acquireNativeStringPatchAdmission() (nativeStringPatchAdmission, error) {
	coord := c.writeDomain.commandWALCoordinatorForDomain(c.db)
	if coord == nil {
		return nativeStringPatchAdmission{}, backenddb.ErrClosed
	}
	coord.mu.Lock()
	defer coord.mu.Unlock()
	for coord.nativePatchAdmitted == nativeStringPatchMaxRequests && !coord.nativePatchClosed {
		if hook := nativeStringPatchAdmissionWaitingTestHook.Load(); hook != nil {
			(*hook)()
		}
		coord.condLocked().Wait()
	}
	if coord.nativePatchClosed {
		return nativeStringPatchAdmission{}, backenddb.ErrClosed
	}
	if coord.nativePatchNextTicket == math.MaxUint64 {
		return nativeStringPatchAdmission{}, ErrPreparedInsertResourceLimit
	}
	a := nativeStringPatchAdmission{coord: coord, ticket: coord.nativePatchNextTicket}
	coord.nativePatchNextTicket++
	coord.nativePatchAdmitted++
	return a, nil
}

func (a *nativeStringPatchAdmission) release() {
	if a == nil || a.coord == nil {
		return
	}
	coord := a.coord
	coord.mu.Lock()
	// Invalid input still retires its ordered position before another caller can
	// pass it. No input is copied while waiting for an admission slot.
	if !a.joined {
		for a.ticket != coord.nativePatchNextJoin && !coord.nativePatchClosed {
			coord.condLocked().Wait()
		}
		if !coord.nativePatchClosed {
			coord.nativePatchNextJoin++
		}
	}
	if coord.nativePatchAdmitted == 0 {
		panic("collections: native patch admission imbalance")
	}
	coord.nativePatchAdmitted--
	coord.condLocked().Broadcast()
	coord.mu.Unlock()
	a.coord = nil
}

func (coord *collectionCommandWALCoordinator) closeNativeStringPatchAdmission() {
	coord.mu.Lock()
	coord.nativePatchClosed = true
	coord.condLocked().Broadcast()
	coord.mu.Unlock()
}

func (group *nativeStringPatchGroup) canAdd(request *nativeStringPatchRequest) bool {
	if group == nil || group.sealed || group.count == nativeStringPatchMaxRequests || request == nil || len(request.input) == 0 ||
		group.domain != request.collection.writeDomain {
		return false
	}
	first := group.requests[0]
	if group.count != 0 && (first.schema != request.schema || !first.collection.SameCachedCatalog(request.collection)) {
		return false
	}
	for i := 0; i < group.count; i++ {
		a, b := group.requests[i].input, request.input
		for x, y := 0, 0; x < len(a) && y < len(b); {
			cmp := bytes.Compare(a[x].ID, b[y].ID)
			if cmp == 0 {
				return false
			}
			if cmp < 0 {
				x++
			} else {
				y++
			}
		}
	}
	return true
}

func (c *Collection) joinNativeStringPatchGroup(a *nativeStringPatchAdmission, owned *nativePreparedStringPatchInput, schema uint64) (TypedStringPatchResult, error) {
	if owned == nil || owned.credit == nil || owned.state.Load() != 1 {
		return TypedStringPatchResult{}, ErrPreparedInsertResourceLimit
	}
	class, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(nativeStringPatchRequest{})), true)
	if err != nil {
		return TypedStringPatchResult{}, err
	}
	if err = owned.credit.facets[nativeRequestSourceCredit].reserve(class); err != nil {
		return TypedStringPatchResult{}, err
	}
	request := &nativeStringPatchRequest{collection: c, schema: schema, input: owned.input, owned: owned}
	coord := a.coord
	coord.mu.Lock()
	for {
		if coord.nativePatchClosed {
			coord.mu.Unlock()
			return TypedStringPatchResult{}, backenddb.ErrClosed
		}
		if a.ticket != coord.nativePatchNextJoin {
			coord.condLocked().Wait()
			continue
		}
		group := coord.nativePatchGroup
		if group != nil && !group.canAdd(request) {
			// A conflicting participant closes this prefix. Keeping its join ticket
			// prevents later compatible callers from overtaking its serial position.
			group.sealed = true
			coord.condLocked().Wait()
			continue
		}
		leader := group == nil
		if leader {
			class, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(nativeStringPatchGroup{})), true)
			if err == nil {
				err = owned.credit.facets[nativeRequestSourceCredit].reserve(class)
			}
			if err != nil {
				coord.mu.Unlock()
				return TypedStringPatchResult{}, err
			}
			group = &nativeStringPatchGroup{domain: c.writeDomain}
			coord.nativePatchGroup = group
		}
		group.requests[group.count] = request
		group.count++
		a.joined = true
		coord.nativePatchNextJoin++
		coord.condLocked().Broadcast()
		coord.mu.Unlock()
		if leader {
			if hook := nativeStringPatchBeforeSealTestHook.Load(); hook != nil {
				(*hook)()
			}
			timer := time.NewTimer(collectionUpdateCombineInlineQuietPeriod)
			<-timer.C
			coord.mu.Lock()
			group.sealed = true
			coord.mu.Unlock()
			results, handled := group.execute()
			if !handled {
				// All combined preparation has been discarded and all locks released.
				// Preserve each caller's atomic boundary without cloning its input again.
				for i := 0; i < group.count; i++ {
					queued := group.requests[i]
					results[i].result, results[i].err = queued.collection.patchTypedStringsBatch(queued.input, queued.schema, nil, nil)
				}
			}
			coord.mu.Lock()
			for i := 0; i < group.count; i++ {
				queued := group.requests[i]
				queued.result, queued.err = results[i].result, results[i].err
				queued.input = nil
				queued.owned = nil
				queued.completed = true
				group.requests[i] = nil
			}
			if coord.nativePatchGroup != group {
				panic("collections: native patch prefix ownership changed")
			}
			coord.nativePatchGroup = nil
			coord.condLocked().Broadcast()
			coord.mu.Unlock()
		}
		coord.mu.Lock()
		for !request.completed {
			coord.condLocked().Wait()
		}
		result, err := request.result, request.err
		coord.mu.Unlock()
		return result, err
	}
}

type nativeStringPatchOutcome struct {
	result TypedStringPatchResult
	err    error
}

func (group *nativeStringPatchGroup) execute() (results [nativeStringPatchMaxRequests]nativeStringPatchOutcome, handled bool) {
	if group == nil || group.count <= 0 || group.count > nativeStringPatchMaxRequests {
		return results, false
	}
	for i := 0; i < group.count; i++ {
		if group.requests[i] == nil || group.requests[i].owned == nil || group.requests[i].owned.credit == nil {
			return group.errorResults(ErrPreparedInsertResourceLimit), true
		}
	}
	if err := ensureNativePublicationResidentOwner(group.requests[0].collection.db, &group.requests[0].owned.credit.facets[nativeRequestSourceCredit]); err != nil {
		return group.errorResults(err), true
	}
	if group.count == 1 {
		request := group.requests[0]
		results[0].result, results[0].err = request.collection.patchTypedStringsBatch(request.input, request.schema, nil, nil)
		return results, true
	}
	// Combine only headers borrowing the four canonical owned inputs. This work
	// happens outside the serialized root-mutation region.
	total := 0
	for i := 0; i < group.count; i++ {
		if len(group.requests[i].input) > math.MaxInt-total {
			return results, false
		}
		total += len(group.requests[i].input)
	}
	class, err := rootpublication.StableBackingClassBytes(uint64(total)*uint64(unsafe.Sizeof(TypedStringPatch{})), true)
	if err != nil {
		return group.errorResults(err), true
	}
	if err = group.requests[0].owned.credit.facets[nativeRequestSourceCredit].reserve(class); err != nil {
		return group.errorResults(err), true
	}
	input := make([]TypedStringPatch, 0, total)
	for i := 0; i < group.count; i++ {
		input = append(input, group.requests[i].input...)
	}
	slices.SortFunc(input, func(a, b TypedStringPatch) int { return bytes.Compare(a.ID, b.ID) })
	c := group.requests[0].collection
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	admissionState := c.lockCollectionCommandWALAdmission()
	admission := &admissionState
	defer admission.unlock()
	if err := c.ensureWriteDomainOpen(); err != nil {
		return group.errorResults(err), true
	}
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return group.errorResults(err), true
	}
	if err := validateTypedStringPatchMeta(c.MetaView(), group.requests[0].schema); err != nil {
		return results, false
	}
	if err := c.requireColumnStoreCommandWAL(c.MetaView(), nil); err != nil {
		return group.errorResults(err), true
	}
	if err := c.db.CheckCommandWALPublishReady(); err != nil {
		return group.errorResults(err), true
	}
	for i := 1; i < group.count; i++ {
		if !c.SameCachedCatalog(group.requests[i].collection) {
			return results, false
		}
	}
	mutation := c.lockMutation()
	held := true
	defer func() {
		if held {
			mutation.Unlock()
		}
	}()
	previousMutation, previousHeld := admission.bindMutation(&mutation, &held)
	defer admission.restoreMutation(previousMutation, previousHeld)
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return group.errorResults(err), true
	}
	// The immutable cut is prepared without owning the mutation lock. The
	// existing publisher revalidates every root before assigning any WAL identity.
	mutation.Unlock()
	held = false
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		plan, payload, _, err := c.buildTypedStringPatchPlanForGroup(input, group.requests[0].schema, group)
		if err != nil {
			return results, false
		}
		for i := 0; i < group.count; i++ {
			results[i] = nativeStringPatchOutcome{}
		}
		for pos, patch := range input {
			owner := group.ownerOf(patch.ID)
			if owner < 0 {
				plan.close()
				panic("collections: native patch lost canonical owner")
			}
			if plan.results[pos].Matched {
				results[owner].result.MatchedCount++
			}
			if plan.results[pos].Modified {
				results[owner].result.ModifiedCount++
			}
		}
		var intents [nativeStringPatchMaxRequests]*backenddb.CommandWALIntent
		intentCount := 0
		for i := 0; i < group.count; i++ {
			if results[i].result.ModifiedCount == 0 {
				continue
			}
			participant := payload
			participant.Documents = make([]commitlog.CollectionTypedStringPatch, 0, results[i].result.ModifiedCount)
			for _, document := range payload.Documents {
				if group.ownerOf(document.ID) == i {
					participant.Documents = append(participant.Documents, document)
				}
			}
			raw, e := commitlog.EncodeCollectionTypedStringsPayload(participant)
			if e == nil {
				intents[intentCount], e = c.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionTypedStringsPatchV1, raw)
			}
			if e != nil {
				plan.close()
				return group.errorResults(e), true
			}
			intentCount++
		}
		if intentCount == 0 {
			plan.close()
			return results, true
		}
		intent := intents[0]
		if intentCount > 1 {
			intent, err = c.db.NewCommandWALDurablePrefixGroupIntent(intents[:intentCount])
			if err != nil {
				plan.close()
				return group.errorResults(err), true
			}
		}
		if hook := nativeStringPatchAfterPrepareTestHook.Load(); hook != nil {
			(*hook)()
		}
		mutation = c.lockMutation()
		held = true
		_, err = c.publishUpdateBatchPlanLocked(plan, intent, admission)
		plan.close()
		if held {
			mutation.Unlock()
			held = false
		}
		if intent.AssignedLSN() == 0 && isRetriableCollectionMutationError(err) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		for i := 0; i < group.count; i++ {
			results[i].err = err
		}
		return results, true
	}
	return group.errorResults(collectionMutationRetryExhausted(lastErr)), true
}

func (group *nativeStringPatchGroup) errorResults(err error) (results [nativeStringPatchMaxRequests]nativeStringPatchOutcome) {
	for i := 0; i < group.count; i++ {
		results[i].err = err
	}
	return results
}

func (group *nativeStringPatchGroup) ownerOf(id []byte) int {
	for i := 0; i < group.count; i++ {
		input := group.requests[i].input
		pos, found := slices.BinarySearchFunc(input, id, func(p TypedStringPatch, id []byte) int { return bytes.Compare(p.ID, id) })
		if found && pos < len(input) {
			return i
		}
	}
	return -1
}

// A union may relax unique replacement ownership only inside the originating
// atomic request. Cross-request handoffs require serial execution, never a union.
func (group *nativeStringPatchGroup) checkUniqueBoundaries(plan *updateBatchPlan, changed []preparedBatchUpdate, runtimes []indexRuntime) error {
	if !collectionMetaHasSecondaryUniqueIndex(plan.meta) {
		return nil
	}
	scratch := make([]preparedBatchUpdate, 0, len(changed))
	for i := 0; i < group.count; i++ {
		scratch = scratch[:0]
		for _, update := range changed {
			if group.ownerOf(update.documentID) == i {
				scratch = append(scratch, update)
			}
		}
		replacements := batchUniqueReplacementOwners(runtimes, scratch)
		for _, update := range scratch {
			if err := rejectReplaceUniqueConflictsOrdered(plan.snap, plan.catalog, runtimes, update, replacements); err != nil {
				return err
			}
		}
	}
	return nil
}
