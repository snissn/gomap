package collections

import (
	"bytes"
	"errors"
	"math"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// These hooks are package-test observation points for group formation and the
// installed-before-ack boundary.
var typedUpsertGroupBeforeSealTestHook atomic.Pointer[func()]
var typedUpsertGroupAfterInstallTestHook atomic.Pointer[func()]

type typedUpsertGroup struct {
	domain    *collectionWriteDomain
	requests  []*typedUpsertGroupRequest
	ids       map[string]struct{}
	documents int
	bytes     int64
	maxDocs   int
	maxBytes  int64
	sealed    bool
}

type typedUpsertGroupRequest struct {
	collection *Collection
	ids        [][]byte
	retained   [][]byte
	projection *trustedFloat32Projection
	intent     *backenddb.CommandWALIntent
	ownedBytes int64
	leader     bool
	done       chan typedUpsertGroupResult
}

type typedUpsertGroupResult struct {
	updated int
	handled bool
	stats   CollectionInsertStats
	err     error
}

// TryUpsertTypedBatchGroup attempts the bounded concurrent insert-only typed
// path. handled=false means no command LSN was assigned and the caller must
// run UpsertTypedBatchWithStats through its existing exclusive admission path.
func (c *Collection) TryUpsertTypedBatchGroup(ids, retained [][]byte, columns []TypedColumnBatch) (updated int, handled bool, stats CollectionInsertStats, err error) {
	if c == nil || c.db == nil {
		return 0, true, stats, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, true, stats, err
	}
	if len(ids) == 0 {
		return 0, false, stats, nil
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer func() { unlockSchema() }()
	if hook := typedSourceBeforeAdmissionTestHook.Load(); hook != nil {
		(*hook)(c)
	}
	admissionState := c.lockCollectionCommandWALAdmission()
	admission := &admissionState
	defer admission.unlock()
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return 0, true, stats, err
	}
	if !c.commandWALActive(nil) || c.db.ResolvedProfile() != backenddb.ProfileCommandWALDurable {
		return 0, false, stats, nil
	}
	meta := c.Meta()
	if collectionMetaHasSecondaryUniqueIndex(meta) {
		return 0, false, stats, nil
	}
	if err := c.requireColumnStoreCommandWAL(meta, nil); err != nil {
		return 0, true, stats, err
	}
	maxDocs, maxBytes := typedUpsertGroupLimits(meta)
	ownedBytes := typedUpsertGroupInputBytes(ids, retained, columns)
	if ownedBytes < 0 || len(ids) > maxDocs || ownedBytes > maxBytes {
		return 0, false, stats, nil
	}

	ownedIDs, ownedRetained := cloneTypedUpsertGroupInput(ids, retained)
	projection, err := newTrustedTypedProjection(meta, ownedIDs, ownedRetained, columns)
	if err != nil {
		if errors.Is(err, ErrDuplicateDocumentID) {
			return 0, false, stats, nil
		}
		return 0, true, stats, err
	}
	docs, err := collectionDocumentsFromBatchInput(ownedIDs, ownedRetained)
	if err != nil {
		return 0, true, stats, err
	}
	intent, err := c.newCollectionReplaceSourceCommandWALIntent(ownedIDs, docs, nil, projection)
	if err != nil {
		return 0, true, stats, err
	}
	if intent == nil || int64(intent.PayloadBytes()) > math.MaxInt64-ownedBytes {
		return 0, false, stats, nil
	}
	ownedBytes += int64(intent.PayloadBytes())

	request := &typedUpsertGroupRequest{
		collection: c,
		ids:        ownedIDs,
		retained:   ownedRetained,
		projection: projection,
		intent:     intent,
		ownedBytes: ownedBytes,
		done:       make(chan typedUpsertGroupResult, 1),
	}
	coord := c.writeDomain.commandWALCoordinatorForDomain(c.db)
	if coord == nil {
		return 0, false, stats, nil
	}
	// Preserve insert-only pre-queue eligibility without lending locks to
	// waiting requests. Execution repeats the check under its fresh owner.
	ingressMutation, locked := c.tryLockMutation()
	if !locked {
		return 0, false, stats, nil
	}
	absent, err := typedUpsertGroupIDsAbsent(c, ownedIDs)
	ingressMutation.Unlock()
	if err != nil {
		return 0, true, stats, err
	}
	if !absent {
		return 0, false, stats, nil
	}
	admission.unlock()
	unlockSchema()
	unlockSchema = func() {}
	coord.mu.Lock()
	group := coord.typedUpsertGroup
	if group == nil {
		group = &typedUpsertGroup{domain: c.writeDomain, ids: make(map[string]struct{}, len(ids)), maxDocs: maxDocs, maxBytes: maxBytes}
		if !group.add(request) {
			coord.mu.Unlock()
			return 0, false, stats, nil
		}
		request.leader = true
		coord.typedUpsertGroup = group
	} else if group.domain != c.writeDomain || !group.add(request) {
		coord.mu.Unlock()
		return 0, false, stats, nil
	}
	coord.mu.Unlock()

	if request.leader {
		if hook := typedUpsertGroupBeforeSealTestHook.Load(); hook != nil {
			(*hook)()
		}
		timer := time.NewTimer(collectionUpdateCombineInlineQuietPeriod)
		<-timer.C
		coord.mu.Lock()
		group.sealed = true
		requests := append([]*typedUpsertGroupRequest(nil), group.requests...)
		coord.mu.Unlock()
		results := group.execute(requests)
		coord.mu.Lock()
		if coord.typedUpsertGroup == group {
			coord.typedUpsertGroup = nil
		}
		if cond := coord.condLocked(); cond != nil {
			cond.Broadcast()
		}
		coord.mu.Unlock()
		for i, queued := range requests {
			queued.done <- results[i]
		}
	}
	result := <-request.done
	return result.updated, result.handled, result.stats, result.err
}

func (group *typedUpsertGroup) canAdd(request *typedUpsertGroupRequest) bool {
	if group == nil || request == nil || group.sealed || len(request.ids) == 0 ||
		len(request.ids) > group.maxDocs-group.documents || request.ownedBytes > group.maxBytes-group.bytes {
		return false
	}
	if len(group.requests) != 0 && !group.requests[0].collection.SameCachedCatalog(request.collection) {
		return false
	}
	for _, id := range request.ids {
		if _, exists := group.ids[string(id)]; exists {
			return false
		}
	}
	return true
}

func (group *typedUpsertGroup) add(request *typedUpsertGroupRequest) bool {
	if !group.canAdd(request) {
		return false
	}
	for _, id := range request.ids {
		group.ids[string(id)] = struct{}{}
	}
	group.requests = append(group.requests, request)
	group.documents += len(request.ids)
	group.bytes += request.ownedBytes
	return true
}

func (group *typedUpsertGroup) execute(requests []*typedUpsertGroupRequest) []typedUpsertGroupResult {
	if len(requests) == 0 {
		return nil
	}
	c := requests[0].collection
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	admissionState := c.lockCollectionCommandWALAdmission()
	admission := &admissionState
	defer admission.unlock()
	if err := c.ensureWriteDomainOpen(); err != nil {
		return typedUpsertGroupErrorResults(requests, err)
	}
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return typedUpsertGroupErrorResults(requests, err)
	}
	if !c.commandWALActive(nil) || c.db.ResolvedProfile() != backenddb.ProfileCommandWALDurable || collectionMetaHasSecondaryUniqueIndex(c.Meta()) {
		return make([]typedUpsertGroupResult, len(requests))
	}
	if err := c.requireColumnStoreCommandWAL(c.Meta(), nil); err != nil {
		return typedUpsertGroupErrorResults(requests, err)
	}
	meta := c.Meta()
	maxDocs, maxBytes := typedUpsertGroupLimits(meta)
	if group.documents > maxDocs || group.bytes > maxBytes {
		return make([]typedUpsertGroupResult, len(requests))
	}
	for _, request := range requests {
		if err := validateTypedProjectionMeta(meta, request.projection); err != nil {
			return typedUpsertGroupErrorResults(requests, err)
		}
	}

	mutation := c.lockMutation()
	held := true
	defer func() {
		if held {
			mutation.Unlock()
		}
	}()
	previousAdmissionMutation, previousAdmissionHeld := admission.bindMutation(&mutation, &held)
	defer admission.restoreMutation(previousAdmissionMutation, previousAdmissionHeld)
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return typedUpsertGroupErrorResults(requests, err)
	}

	ids, retained, projection, intents := combineTypedUpsertGroupRequests(requests)
	var stats CollectionInsertStats
	stats.Documents = len(ids)
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		planStarted := time.Now()
		plan, err := c.buildSourceReplacementPlan(ids, ids, retained, nil, requests[0].intent, nil, projection, true)
		stats.SourceReplacementPlan += time.Since(planStarted)
		if err != nil {
			if isRetriableCollectionMutationError(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			return typedUpsertGroupErrorResults(requests, err)
		}
		if plan.deleteCount != 0 || plan.unchangedCount != 0 {
			plan.close()
			return make([]typedUpsertGroupResult, len(requests))
		}
		publishIntent := requests[0].intent
		if len(intents) > 1 {
			publishIntent, err = c.db.NewCommandWALDurablePrefixGroupIntent(intents)
			if err != nil {
				plan.close()
				return typedUpsertGroupErrorResults(requests, err)
			}
		}
		plan.commandWAL = publishIntent
		hooks := &sourcePublicationHooks{afterPublish: func() error {
			if hook := typedUpsertGroupAfterInstallTestHook.Load(); hook != nil {
				(*hook)()
			}
			return nil
		}}
		publishStarted := time.Now()
		publishErr := c.publishSourceReplacementPlan(plan, hooks, &stats, admission)
		stats.Publish += time.Since(publishStarted)
		plan.close()
		if publishIntent.AssignedLSN() == 0 && isRetriableCollectionMutationError(publishErr) {
			lastErr = publishErr
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		published := publishErr == nil || backenddb.CommitPublicationAccepted(publishErr) || errors.Is(publishErr, ErrCommitAmbiguous)
		if !published {
			return typedUpsertGroupErrorResults(requests, publishErr)
		}
		var notifyErr error
		byCollection := make(map[*Collection][][]byte)
		for _, request := range requests {
			byCollection[request.collection] = append(byCollection[request.collection], request.ids...)
		}
		for collection, changedIDs := range byCollection {
			if err := collection.reconcileVectorIndexes(changedIDs); err != nil {
				collection.invalidateRegisteredVectorIndexDocumentCoverage()
				notifyErr = errors.Join(notifyErr, commitAmbiguousError("typed upsert group vector maintenance", err))
			}
		}
		finalErr := c.invalidateVectorIndexCoverageOnAcceptedMutation(errors.Join(publishErr, notifyErr))
		results := make([]typedUpsertGroupResult, len(requests))
		for i, request := range requests {
			requestStats := CollectionInsertStats{Documents: len(request.ids)}
			if request.leader {
				requestStats = stats
				requestStats.Documents = len(request.ids)
			}
			results[i] = typedUpsertGroupResult{handled: true, stats: requestStats, err: finalErr}
		}
		return results
	}
	return typedUpsertGroupErrorResults(requests, collectionMutationRetryExhausted(lastErr))
}

func typedUpsertGroupErrorResults(requests []*typedUpsertGroupRequest, err error) []typedUpsertGroupResult {
	results := make([]typedUpsertGroupResult, len(requests))
	for i, request := range requests {
		results[i] = typedUpsertGroupResult{handled: true, stats: CollectionInsertStats{Documents: len(request.ids)}, err: err}
	}
	return results
}

func combineTypedUpsertGroupRequests(requests []*typedUpsertGroupRequest) (ids, retained [][]byte, projection *trustedFloat32Projection, intents []*backenddb.CommandWALIntent) {
	rows := 0
	for _, request := range requests {
		rows += len(request.ids)
	}
	first := requests[0].projection
	projection = &trustedFloat32Projection{
		columns:      first.columns,
		schemaHash:   first.schemaHash,
		typedRows:    make(map[string][]columnDeclaredValue, rows),
		retainedJSON: make([][]byte, 0, rows),
	}
	ids = make([][]byte, 0, rows)
	retained = make([][]byte, 0, rows)
	intents = make([]*backenddb.CommandWALIntent, 0, len(requests))
	for _, request := range requests {
		ids = append(ids, request.ids...)
		retained = append(retained, request.retained...)
		projection.retainedJSON = append(projection.retainedJSON, request.retained...)
		for id, values := range request.projection.typedRows {
			projection.typedRows[id] = values
		}
		intents = append(intents, request.intent)
	}
	return ids, retained, projection, intents
}

func typedUpsertGroupIDsAbsent(c *Collection, ids [][]byte) (bool, error) {
	for _, id := range ids {
		if _, found, err := c.GetInto(id, nil); err != nil {
			return false, err
		} else if found {
			return false, nil
		}
	}
	return true, nil
}

func cloneTypedUpsertGroupInput(ids, retained [][]byte) ([][]byte, [][]byte) {
	ownedIDs := make([][]byte, len(ids))
	ownedRetained := make([][]byte, len(retained))
	for i := range ids {
		ownedIDs[i] = bytes.Clone(ids[i])
	}
	for i := range retained {
		ownedRetained[i] = bytes.Clone(retained[i])
	}
	return ownedIDs, ownedRetained
}

func typedUpsertGroupInputBytes(ids, retained [][]byte, columns []TypedColumnBatch) int64 {
	var total int64
	add := func(n int) bool {
		if n < 0 || int64(n) > math.MaxInt64-total {
			return false
		}
		total += int64(n)
		return true
	}
	for _, values := range [][][]byte{ids, retained} {
		for _, value := range values {
			if !add(len(value)) {
				return -1
			}
		}
	}
	for _, column := range columns {
		for _, value := range column.Strings {
			if !add(len(value)) {
				return -1
			}
		}
		for _, vector := range column.Float32Vectors {
			if len(vector) > math.MaxInt/4 || !add(len(vector)*4) {
				return -1
			}
		}
	}
	return total
}

func typedUpsertGroupLimits(meta CollectionMeta) (int, int64) {
	maxDocs := DefaultIndexedWriteMemtableDirectBatchDocuments
	if configured := meta.Options.BufferedIndexedWriteMaxDocuments; configured > 0 && configured < maxDocs {
		maxDocs = configured
	}
	maxBytes := columnPhysicalAssetSegmentTargetBytes
	if configured := meta.Options.BufferedIndexedWriteMaxBytes; configured > 0 && configured < maxBytes {
		maxBytes = configured
	}
	return maxDocs, maxBytes
}
