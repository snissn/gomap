package treedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultPublicCommandWALGroupDelay      = 200 * time.Microsecond
	defaultPublicCommandWALGroupMaxCommits = 64
	defaultPublicCommandWALGroupMaxBytes   = 1 << 20
)

type publicCommandWALGroupCommit struct {
	mu                   sync.Mutex
	cond                 sync.Cond
	wake                 chan struct{}
	capturedPrefix       backenddb.CommandWALCapturedPrefix // exclusively owned by the active leader
	timer                *time.Timer
	leaderActive         bool
	nextTicket           uint64
	completedTicket      uint64
	publishTicket        uint64
	terminalErr          error
	pendingSingleton     uint64    // an idle mutation can form an immediate one-ticket epoch
	pendingDeadline      time.Time // collection starts at first registration, including prior I/O
	pendingSealed        bool      // caller-owned aggregate already completed its collection window
	pendingCount         int
	pendingCommits       int
	pendingBytes         int
	pendingForceTickets  []uint64
	pendingTriggered     bool
	retryableErrors      map[uint64]error
	delay                time.Duration
	maxCommits           int
	maxBytes             int
	testBeforeSync       func(groupSize int)
	testAfterRegister    func(ticket uint64, durable bool)
	testAfterWait        func(ticket uint64)
	testBeforeLeaderWait func()
	durableIntents       atomic.Int64

	attempts             atomic.Uint64
	groups               atomic.Uint64
	commits              atomic.Uint64
	participants         atomic.Uint64
	dependencyAttempts   atomic.Uint64
	dependenciesCovered  atomic.Uint64
	leaders              atomic.Uint64
	followers            atomic.Uint64
	syncs                atomic.Uint64
	errors               atomic.Uint64
	forced               atomic.Uint64
	triggerTimeout       atomic.Uint64
	triggerCommits       atomic.Uint64
	triggerBytes         atomic.Uint64
	triggerExplicit      atomic.Uint64
	triggerSealed        atomic.Uint64
	groupSizeMax         atomic.Uint64
	dependencyCoveredMax atomic.Uint64
	waitNsTotal          atomic.Uint64
	waitNsMax            atomic.Uint64
	bypassRelaxedNoGroup atomic.Uint64
	bypassSoloDurable    atomic.Uint64
}

type publicCommandWALPublication struct{ ticket uint64 }

// This transient registration belongs to one caller until its ACK wait. The
// published state remains the original compact ticket, not a second queue.
type publicCommandWALRegistration struct {
	publicCommandWALPublication
	leader, awaiting, observeWait, durable bool
}

const publicCommandWALFastRelaxedTicket = ^uint64(0)
const publicCommandWALFastRelaxedAppendedTicket = publicCommandWALFastRelaxedTicket - 1

func (tdb *DB) forcePublicCommandWALGroupCommit() error {
	if tdb == nil || !tdb.commandWALCached {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	tdb.commandWALPublicOperationGate.RLock()
	tdb.commandWALPublicPublishMu.Lock()
	ticket, leader, err := tdb.commandWALGroupCommit.register(0, true, true)
	tdb.commandWALPublicPublishMu.Unlock()
	tdb.commandWALPublicOperationGate.RUnlock()
	if err != nil {
		return err
	}
	_, err = tdb.commandWALGroupCommit.awaitRegistered(tdb, ticket, leader, true, true)
	return err
}

func (tdb *DB) appendAndRegisterPublicCommandWAL(bytes int, preparedBytes *int, durable bool, pendingBookkeeping, groupCommitWait *time.Duration, groupWaitObserved *bool, appendFrame func(sync bool) (uint64, error)) (publicCommandWALPublication, error) {
	return tdb.appendAndRegisterPublicCommandWALCapture(bytes, preparedBytes, durable, pendingBookkeeping, groupCommitWait, groupWaitObserved, appendFrame, nil, nil)
}

func (tdb *DB) appendAndRegisterPublicCommandWALCapture(bytes int, preparedBytes *int, durable bool, pendingBookkeeping, groupCommitWait *time.Duration, groupWaitObserved *bool, appendFrame func(sync bool) (uint64, error), prepareCapture func() ([]byte, error), captureTiming *backenddb.CommandWALRequestTiming) (publicCommandWALPublication, error) {
	if durable && tdb != nil && tdb.commandWALCached && tdb.backend != nil && appendFrame != nil &&
		tdb.cached != nil && !tdb.cached.COWMode() {
		return tdb.appendAndCaptureOrdinaryPublicCommandWAL(bytes, preparedBytes, pendingBookkeeping, groupCommitWait, groupWaitObserved, appendFrame, prepareCapture, captureTiming)
	}
	registration, err := tdb.registerPublicCommandWAL(bytes, preparedBytes, durable, pendingBookkeeping, appendFrame)
	if err != nil {
		return registration.publicCommandWALPublication, err
	}
	return tdb.awaitPublicCommandWAL(registration, groupCommitWait, groupWaitObserved)
}

// appendAndCaptureOrdinaryPublicCommandWAL enters the existing leader directly
// only when exclusive append admission can freeze a truly idle ordinary epoch.
// COW retains its separate register/install/await path and typed staging guard.
func (tdb *DB) appendAndCaptureOrdinaryPublicCommandWAL(bytes int, preparedBytes *int, pendingBookkeeping, groupCommitWait *time.Duration, groupWaitObserved *bool, appendFrame func(bool) (uint64, error), prepareCapture func() ([]byte, error), captureTiming *backenddb.CommandWALRequestTiming) (publicCommandWALPublication, error) {
	group := &tdb.commandWALGroupCommit
	group.durableIntents.Add(1)
	intentPending := true
	releaseIntent := func() {
		if intentPending {
			intentPending = false
			group.durableIntents.Add(-1)
		}
	}
	defer releaseIntent()

	var publication publicCommandWALRegistration
	var batch publicCommandWALCapturedGroup
	captured := false
	defer func() {
		if recovered := recover(); recovered != nil {
			if captured {
				group.failPanickedLeader(tdb)
			}
			panic(recovered)
		}
	}()
	appended := false
	var err error
	var waitStart time.Time
	func() {
		if !tdb.commandWALPublicOperationGate.TryLock() {
			return
		}
		defer tdb.commandWALPublicOperationGate.Unlock()
		tdb.commandWALPublicPublishMu.Lock()
		defer tdb.commandWALPublicPublishMu.Unlock()
		group.mu.Lock()
		eligible := group.idleDurableCaptureEligibleLocked()
		group.mu.Unlock()
		if !eligible {
			return
		}
		// The backend continuation runs under the original append admission.
		// It chooses only this existing coordinator epoch; it mints no lease.
		selected := false
		recordAppend := func(lsn uint64) {
			if lsn != 0 {
				start := time.Now()
				tdb.recordPublicCommandWALPendingLSN(lsn)
				if pendingBookkeeping != nil {
					*pendingBookkeeping += time.Since(start)
				}
			}
		}
		selectCapture := func(lsn uint64) bool {
			selected = true
			recordAppend(lsn)
			group.mu.Lock()
			eligible = group.idleDurableCaptureEligibleLocked()
			if eligible {
				group.initLocked()
				publication.ticket, err = group.allocateTicketLocked()
				if err == nil {
					group.leaderActive = true
					group.leaders.Add(1)
					batch = publicCommandWALCapturedGroup{count: 1, commits: 1, lastTicket: publication.ticket}
					captured = true
				}
			}
			group.mu.Unlock()
			if captured {
				if groupWaitObserved != nil {
					*groupWaitObserved = true
				}
				waitStart = time.Now()
			}
			return captured
		}
		// Only this call may append. A demand change after assignment joins
		// the same append to pending collection; it never resubmits it.
		appended = true
		var lsn uint64
		var appendErr error
		if prepareCapture != nil {
			group.capturedPrefix = backenddb.CommandWALCapturedPrefix{}
			var barrier backenddb.CommandWALBarrierResult
			var requestTiming backenddb.CommandWALRequestTiming
			lsn, requestTiming, barrier, appendErr = tdb.backend.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(prepareCapture, &group.capturedPrefix, selectCapture, captureTiming != nil)
			if captureTiming != nil {
				// Assignment and wait observations were recorded in the scoped
				// selector before the backend append partitions were returned.
				requestTiming.PostAppendPendingLSNBookkeeping = captureTiming.PostAppendPendingLSNBookkeeping
				requestTiming.PostAppendPendingLSNBookkeepingObserved = true
				requestTiming.GroupCommitWaitObserved = captureTiming.GroupCommitWaitObserved
				*captureTiming = requestTiming
			}
			if captured {
				batch.barrier, batch.err = barrier, appendErr
				if appendErr == nil {
					batch.target = &group.capturedPrefix
				}
				if barrier.LSN != 0 {
					tdb.recordPublicCommandWALPendingLSN(barrier.LSN)
				}
				return
			}
		} else {
			lsn, appendErr = appendFrame(true)
		}
		if !selected {
			if appendErr != nil || lsn == 0 {
				recordAppend(lsn)
			} else {
				selectCapture(lsn)
			}
		}
		if appendErr != nil {
			err = appendErr
		}
		if err != nil || lsn == 0 {
			return
		}
		if !eligible {
			if preparedBytes != nil {
				bytes = *preparedBytes
			}
			publication.ticket, publication.leader, err = group.registerCollected(bytes, true, false, false)
			publication.awaiting, publication.observeWait, publication.durable = err == nil, true, true
		}
		if captured {
			group.captureGroup(tdb, &batch)
		}

	}()
	if !appended {
		publication, err = tdb.registerPublicCommandWALWithIntent(bytes, preparedBytes, true, true, pendingBookkeeping, appendFrame)
	}
	if !captured {
		releaseIntent()
		if err != nil {
			return publication.publicCommandWALPublication, err
		}
		return tdb.awaitPublicCommandWAL(publication, groupCommitWait, groupWaitObserved)
	}

	releaseIntent()
	ready, completionErr := group.syncAndCompleteGroup(tdb, &batch, publication.ticket)
	// Completion retired this epoch; a successor may now own leaderActive.
	captured = false
	ticket := publication.ticket
	var elapsed time.Duration
	if ready {
		err = completionErr
		elapsed = group.observeWaitSince(waitStart)
		if err != nil {
			ticket = 0
		}
	} else {
		// A previous publication or an installed wait hook still belongs to
		// the original await path. The captured epoch is already synced.
		ticket, elapsed, err = group.awaitRegisteredSince(tdb, ticket, false, false, true, waitStart)
	}
	if groupCommitWait != nil {
		*groupCommitWait += elapsed
	}
	return publicCommandWALPublication{ticket: ticket}, err
}

// registerPublicCommandWAL appends/registers without awaiting another request's
// durability or publication. COW calls it only after admission and installs its
// complete cut before invoking awaitPublicCommandWAL.
func (tdb *DB) registerPublicCommandWAL(bytes int, preparedBytes *int, durable bool, pendingBookkeeping *time.Duration, appendFrame func(sync bool) (uint64, error)) (publicCommandWALRegistration, error) {
	return tdb.registerPublicCommandWALWithIntent(bytes, preparedBytes, durable, false, pendingBookkeeping, appendFrame)
}

// intentRegistered means the caller already owns its announced durable intent.
// COW announces before cache admission; ordinary fusion retains one intent across
// preappend fallback. Both release it before any physical ACK wait.
func (tdb *DB) registerPublicCommandWALWithIntent(bytes int, preparedBytes *int, durable, intentRegistered bool, pendingBookkeeping *time.Duration, appendFrame func(bool) (uint64, error)) (publicCommandWALRegistration, error) {
	return tdb.registerPublicCommandWALCollected(bytes, preparedBytes, durable, intentRegistered, false, pendingBookkeeping, appendFrame)
}

// collectionSealed is scheduling metadata from a caller-owned aggregate. It
// closes collection without adding a force ticket or changing prefix/ACK rules.
func (tdb *DB) registerPublicCommandWALCollected(bytes int, preparedBytes *int, durable, intentRegistered, collectionSealed bool, pendingBookkeeping *time.Duration, appendFrame func(bool) (uint64, error)) (publicCommandWALRegistration, error) {
	var publication publicCommandWALRegistration
	if tdb == nil || !tdb.commandWALCached || tdb.backend == nil || appendFrame == nil {
		return publication, ErrClosed
	}
	if !durable && tdb.beginFastRelaxedPublicCommandWALPublication() {
		publication.ticket = publicCommandWALFastRelaxedTicket
		leasePending := true
		defer func() {
			if leasePending {
				tdb.commandWALPublicOperationGate.RUnlock()
			}
		}()
		lsn, err := appendFrame(false)
		if lsn != 0 {
			publication.ticket = publicCommandWALFastRelaxedAppendedTicket
			start := time.Now()
			tdb.recordPublicCommandWALPendingLSN(lsn)
			if pendingBookkeeping != nil {
				*pendingBookkeeping += time.Since(start)
			}
		}
		leasePending = false
		return publication, err
	}
	durableIntentPending := false
	releaseDurableIntent := func() {
		if durableIntentPending {
			durableIntentPending = false
			tdb.commandWALGroupCommit.durableIntents.Add(-1)
		}
	}
	if durable && !intentRegistered {
		tdb.commandWALGroupCommit.durableIntents.Add(1)
		durableIntentPending = true
		defer releaseDurableIntent()
	}
	var ticket uint64
	var leader bool
	var err error
	func() {
		tdb.commandWALPublicOperationGate.RLock()
		defer tdb.commandWALPublicOperationGate.RUnlock()
		tdb.commandWALPublicPublishMu.Lock()
		defer tdb.commandWALPublicPublishMu.Unlock()

		group := &tdb.commandWALGroupCommit
		group.mu.Lock()
		singleton := durable && group.idleDurableCaptureEligibleLocked()
		group.mu.Unlock()
		// Queued writes retain relaxed prefix debt. Only a truly idle mutation
		// emits its own durable marker; a successor epoch closes debt once.
		lsn, appendErr := appendFrame(singleton)
		err = appendErr
		if lsn != 0 {
			start := time.Now()
			tdb.recordPublicCommandWALPendingLSN(lsn)
			if pendingBookkeeping != nil {
				*pendingBookkeeping += time.Since(start)
			}
		}
		if err == nil && lsn != 0 {
			if preparedBytes != nil {
				bytes = *preparedBytes
			}
			ticket, leader, err = group.registerCollected(bytes, durable, false, collectionSealed)
			if err == nil && singleton && leader {
				group.mu.Lock()
				group.pendingSingleton = ticket
				group.mu.Unlock()
			}
		}
	}()
	if durable {
		releaseDurableIntent()
	}
	if err != nil || ticket == 0 {
		return publication, err
	}
	publication.ticket = ticket
	publication.leader = leader
	publication.awaiting = true
	publication.observeWait = true
	publication.durable = durable
	return publication, nil
}

func (tdb *DB) awaitPublicCommandWAL(publication publicCommandWALRegistration, groupCommitWait *time.Duration, groupWaitObserved *bool) (publicCommandWALPublication, error) {
	if !publication.awaiting {
		return publication.publicCommandWALPublication, nil
	}
	if publication.observeWait {
		if groupWaitObserved != nil {
			*groupWaitObserved = true
		}
		if hook := tdb.commandWALGroupCommit.testAfterRegister; hook != nil {
			hook(publication.ticket, publication.durable)
		}
	}
	waitStart := time.Now()
	ticket, err := tdb.commandWALGroupCommit.awaitRegistered(tdb, publication.ticket, publication.leader, false, publication.observeWait)
	if groupCommitWait != nil && publication.observeWait {
		*groupCommitWait += time.Since(waitStart)
	}
	publication.ticket = ticket
	publication.awaiting = false
	return publication.publicCommandWALPublication, err
}

func (tdb *DB) beginFastRelaxedPublicCommandWALPublication() bool {
	if tdb == nil {
		return false
	}
	tdb.commandWALPublicOperationGate.RLock()
	group := &tdb.commandWALGroupCommit
	group.mu.Lock()
	idle := group.fastRelaxedPublicationIdleLocked()
	group.mu.Unlock()
	if !idle {
		tdb.commandWALPublicOperationGate.RUnlock()
	}
	return idle
}

func (group *publicCommandWALGroupCommit) fastRelaxedPublicationIdleLocked() bool {
	if group == nil || group.terminalErr != nil || group.durableIntents.Load() != 0 ||
		group.leaderActive || group.pendingCount != 0 {
		return false
	}
	if group.cond.L == nil {
		return group.nextTicket == 0 && group.completedTicket == 0 && group.publishTicket == 0
	}
	return group.completedTicket == group.nextTicket && group.publishTicket == group.nextTicket+1
}

// This only selects the representation and immediate epoch. Physical sync
// always belongs to the common captured-prefix leader, outside append gates.
func (group *publicCommandWALGroupCommit) idleDurableCaptureEligibleLocked() bool {
	if group.terminalErr != nil || group.durableIntents.Load() != 1 || group.leaderActive || group.pendingCount != 0 {
		return false
	}
	if (group.delay != 0 && group.delay != defaultPublicCommandWALGroupDelay) ||
		(group.maxCommits != 0 && group.maxCommits != defaultPublicCommandWALGroupMaxCommits) ||
		(group.maxBytes != 0 && group.maxBytes != defaultPublicCommandWALGroupMaxBytes) ||
		group.testBeforeSync != nil || group.testAfterRegister != nil || group.testAfterWait != nil || group.testBeforeLeaderWait != nil {
		return false
	}
	if group.cond.L == nil {
		return group.nextTicket == 0 && group.completedTicket == 0 && group.publishTicket == 0
	}
	return group.completedTicket == group.nextTicket && group.publishTicket == group.nextTicket+1
}

func (group *publicCommandWALGroupCommit) register(bytes int, durable, force bool) (ticket uint64, leader bool, err error) {
	return group.registerCollected(bytes, durable, force, false)
}

func (group *publicCommandWALGroupCommit) registerCollected(bytes int, durable, force, collectionSealed bool) (ticket uint64, leader bool, err error) {
	if group == nil {
		return 0, false, ErrClosed
	}
	if bytes < 0 {
		bytes = 0
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	group.initLocked()
	if group.terminalErr != nil {
		return 0, false, group.terminalErr
	}
	ticket, err = group.allocateTicketLocked()
	if err != nil {
		return 0, false, err
	}
	if !durable && !group.leaderActive && group.pendingCount == 0 {
		group.bypassRelaxedNoGroup.Add(1)
		group.completedTicket = ticket
		group.cond.Broadcast()
		return ticket, false, nil
	}
	group.initWakeLocked()
	if group.pendingCount == 0 {
		group.pendingDeadline = time.Now().Add(group.delay)
	}
	group.pendingCount++
	group.pendingSealed = group.pendingSealed || collectionSealed
	if durable && !force {
		group.pendingCommits++
	} else if force {
		group.pendingForceTickets = append(group.pendingForceTickets, ticket)
	}
	group.pendingBytes += bytes
	leader = durable && !group.leaderActive
	if leader {
		group.leaderActive = true
		group.leaders.Add(1)
	} else {
		group.followers.Add(1)
	}
	switch {
	case force:
		group.forced.Add(1)
		group.triggerLocked(&group.triggerExplicit)
	case collectionSealed:
		group.triggerLocked(&group.triggerSealed)
	case group.pendingCount >= group.maxCommits:
		group.triggerLocked(&group.triggerCommits)
	case group.pendingBytes >= group.maxBytes:
		group.triggerLocked(&group.triggerBytes)
	}
	return ticket, leader, nil
}

func (group *publicCommandWALGroupCommit) allocateTicketLocked() (uint64, error) {
	if group.terminalErr != nil {
		return 0, group.terminalErr
	}
	if group.nextTicket == publicCommandWALFastRelaxedAppendedTicket-1 {
		group.terminalErr = fmt.Errorf("treedb: command wal publication ticket space exhausted")
		return 0, group.terminalErr
	}
	group.nextTicket++
	return group.nextTicket, nil
}

func (group *publicCommandWALGroupCommit) awaitRegistered(tdb *DB, ticket uint64, leader, force, observeWait bool) (uint64, error) {
	if group == nil || tdb == nil || tdb.backend == nil || ticket == 0 {
		return 0, ErrClosed
	}
	ticket, _, err := group.awaitRegisteredSince(tdb, ticket, leader, force, observeWait, time.Now())
	return ticket, err
}

func (group *publicCommandWALGroupCommit) awaitRegisteredSince(tdb *DB, ticket uint64, leader, force, observeWait bool, enqueued time.Time) (uint64, time.Duration, error) {
	if leader {
		group.runLeader(tdb)
	}

	group.mu.Lock()
	for (group.completedTicket < ticket || group.publishTicket != ticket) && group.terminalErr == nil {
		if !group.leaderActive && group.pendingCount != 0 && group.completedTicket < ticket {
			group.leaderActive = true
			group.leaders.Add(1)
			group.mu.Unlock()
			group.runLeader(tdb)
			group.mu.Lock()
			continue
		}
		group.cond.Wait()
	}
	err, retryableCompletion := group.retryableErrors[ticket]
	if retryableCompletion {
		delete(group.retryableErrors, ticket)
	} else {
		err = group.terminalErr
	}
	hook := group.testAfterWait
	if force && group.terminalErr == nil {
		group.publishTicket++
		group.cond.Broadcast()
	}
	group.mu.Unlock()
	if hook != nil {
		hook(ticket)
	}
	var elapsed time.Duration
	if observeWait {
		elapsed = group.observeWaitSince(enqueued)
	}
	if err != nil {
		return 0, elapsed, err
	}
	return ticket, elapsed, nil
}

// One sample feeds both coordinator counters and the enclosing request phase.
// The interval includes capture, physical sync, publication readiness and hooks.
func (group *publicCommandWALGroupCommit) observeWaitSince(enqueued time.Time) time.Duration {
	elapsed := time.Since(enqueued)
	waitNs := uint64(elapsed.Nanoseconds())
	group.waitNsTotal.Add(waitNs)
	publicCommandWALGroupAtomicMax(&group.waitNsMax, waitNs)
	return elapsed
}

func (group *publicCommandWALGroupCommit) initLocked() {
	group.initPublicationLocked()
	if group.delay <= 0 {
		group.delay = defaultPublicCommandWALGroupDelay
	}
	if group.maxCommits <= 0 {
		group.maxCommits = defaultPublicCommandWALGroupMaxCommits
	}
	if group.maxBytes <= 0 {
		group.maxBytes = defaultPublicCommandWALGroupMaxBytes
	}
}

func (group *publicCommandWALGroupCommit) initPublicationLocked() {
	if group.cond.L == nil {
		group.cond.L = &group.mu
		group.publishTicket = 1
	}
}

func (group *publicCommandWALGroupCommit) initWakeLocked() {
	if group.wake == nil {
		group.wake = make(chan struct{}, 1)
	}
}

func (tdb *DB) closePublicCommandWALGroupCommit() {
	if tdb == nil {
		return
	}
	group := &tdb.commandWALGroupCommit
	group.mu.Lock()
	for group.leaderActive {
		// A leader may retain the shared timer and wake channel outside
		// group.mu. Wake it and keep those resources alive until it reports
		// completion, so teardown cannot strand its select.
		select {
		case group.wake <- struct{}{}:
		default:
		}
		group.cond.Wait()
	}
	group.mu.Unlock()
	tdb.commandWALPublicOperationGate.Lock()
	defer tdb.commandWALPublicOperationGate.Unlock()
	group.mu.Lock()
	if group.timer != nil {
		if !group.timer.Stop() {
			select {
			case <-group.timer.C:
			default:
			}
		}
		group.timer = nil
	}
	group.wake = nil
	group.mu.Unlock()
}

func (tdb *DB) finishPublicCommandWALGroupPublication(publication publicCommandWALPublication, publishErr error) {
	if tdb == nil {
		return
	}
	switch publication.ticket {
	case publicCommandWALFastRelaxedTicket:
		tdb.commandWALPublicOperationGate.RUnlock()
		return
	case publicCommandWALFastRelaxedAppendedTicket:
		if publishErr != nil {
			if tdb.backend != nil {
				tdb.backend.MarkCommandWALRecoveryRequired()
			}
			group := &tdb.commandWALGroupCommit
			group.mu.Lock()
			if group.terminalErr == nil {
				group.errors.Add(1)
				group.terminalErr = publishErr
				group.cond.Broadcast()
			}
			group.mu.Unlock()
		}
		tdb.commandWALPublicOperationGate.RUnlock()
		return
	}
	ticket := publication.ticket
	if ticket == 0 {
		return
	}
	if publishErr != nil && tdb.backend != nil {
		tdb.backend.MarkCommandWALRecoveryRequired()
	}
	group := &tdb.commandWALGroupCommit
	group.mu.Lock()
	if group.terminalErr != nil {
		group.mu.Unlock()
		return
	}
	if publishErr != nil {
		group.errors.Add(1)
		group.terminalErr = publishErr
		group.completedTicket = group.nextTicket
		group.pendingCount = 0
		group.pendingDeadline = time.Time{}
		group.pendingSealed = false
		group.pendingCommits = 0
		group.pendingBytes = 0
		group.pendingForceTickets = group.pendingForceTickets[:0]
		group.pendingTriggered = false
		group.cond.Broadcast()
	} else if group.publishTicket == ticket {
		group.publishTicket++
		group.cond.Broadcast()
	}
	group.mu.Unlock()
}

func (group *publicCommandWALGroupCommit) triggerLocked(counter *atomic.Uint64) {
	if group.pendingTriggered {
		return
	}
	group.pendingTriggered = true
	counter.Add(1)
	select {
	case group.wake <- struct{}{}:
	default:
	}
}

// publicCommandWALCapturedGroup is the active leader's stack-local epoch,
// detached from the one pending ledger. Its capture uses group-owned storage.
type publicCommandWALCapturedGroup struct {
	count, commits int
	lastTicket     uint64
	forceTickets   []uint64
	target         *backenddb.CommandWALCapturedPrefix
	barrier        backenddb.CommandWALBarrierResult
	err            error
}

func (group *publicCommandWALGroupCommit) runLeader(tdb *DB) {
	group.mu.Lock()
	group.initLocked()
	group.initWakeLocked()
	// An epoch containing only relaxed followers needs ordered completion,
	// not a collection delay or a new physical durability certificate.
	relaxedOnly := group.pendingCommits == 0 && len(group.pendingForceTickets) == 0
	immediate := relaxedOnly || group.pendingSealed || (group.pendingCount == 1 && group.pendingSingleton == group.nextTicket && group.durableIntents.Load() == 0)
	var delay time.Duration
	if !immediate {
		delay = time.Until(group.pendingDeadline)
		immediate = delay <= 0
	}
	wake := group.wake
	timer := group.timer
	if !immediate {
		// Immediate cuts never use a timer. A previous collector consumes or
		// stops its timer before returning, so only a new collection needs to
		// create or reset it. Keep the first-registration deadline unchanged:
		// demand arriving before this decision can still require collection.
		if timer == nil {
			timer = time.NewTimer(delay)
			group.timer = timer
		} else {
			timer.Reset(delay)
		}
	}
	leaderWaitHook := group.testBeforeLeaderWait
	group.mu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			group.failPanickedLeader(tdb)
			panic(recovered)
		}
	}()

	if leaderWaitHook != nil {
		leaderWaitHook()
	}
	// A sealed native aggregate has already spent its collection window upstream.
	// Generic successors spend only the remaining first-registration budget; prior
	// physical I/O never starts another full collection delay.
	if !immediate {
		select {
		case <-timer.C:
			group.triggerTimeout.Add(1)
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

	}
	// Public append and ticket registration use the same outer mutex. Holding
	// it through capture binds the tickets to an exact complete WAL prefix.
	// The physical barrier runs after these gates release.
	var batch publicCommandWALCapturedGroup
	func() {
		tdb.commandWALPublicOperationGate.Lock()
		defer tdb.commandWALPublicOperationGate.Unlock()
		tdb.commandWALPublicPublishMu.Lock()
		defer tdb.commandWALPublicPublishMu.Unlock()
		group.mu.Lock()
		batch = group.detachPendingLocked()
		hook := group.testBeforeSync
		group.mu.Unlock()
		if batch.count == 0 {
			group.mu.Lock()
			group.leaderActive = false
			group.cond.Broadcast()
			group.mu.Unlock()
			return
		}
		if batch.commits == 0 && len(batch.forceTickets) == 0 {
			group.mu.Lock()
			if group.terminalErr != nil {
				group.completedTicket = group.nextTicket
			} else {
				group.completedTicket = batch.lastTicket
			}
			group.bypassRelaxedNoGroup.Add(uint64(batch.count))
			group.leaderActive = false
			group.cond.Broadcast()
			group.mu.Unlock()
			return
		}

		if hook != nil {
			hook(batch.count)
		}
		group.captureGroup(tdb, &batch)
	}()
	if batch.count == 0 || (batch.commits == 0 && len(batch.forceTickets) == 0) {
		return
	}
	group.syncAndCompleteGroup(tdb, &batch, 0)
}

func (group *publicCommandWALGroupCommit) detachPendingLocked() publicCommandWALCapturedGroup {
	batch := publicCommandWALCapturedGroup{
		count: group.pendingCount, commits: group.pendingCommits,
		lastTicket:   group.nextTicket,
		forceTickets: append([]uint64(nil), group.pendingForceTickets...),
	}
	group.pendingSingleton = 0
	group.pendingCount = 0
	group.pendingDeadline = time.Time{}
	group.pendingSealed = false
	group.pendingCommits = 0
	group.pendingBytes = 0
	group.pendingForceTickets = group.pendingForceTickets[:0]
	group.pendingTriggered = false
	select {
	case <-group.wake:
	default:
	}
	return batch
}

// captureGroup requires the existing exclusive public operation and publish
// gates. Backend admission, producer drains and actual frontier sampling remain
// unchanged; only the active leader may reuse capturedPrefix.
func (group *publicCommandWALGroupCommit) captureGroup(tdb *DB, batch *publicCommandWALCapturedGroup) {
	// The previous leader completed Sync and all deferred releases before it
	// cleared leaderActive. This storage is never exposed to public waiters.
	group.capturedPrefix = backenddb.CommandWALCapturedPrefix{}
	if tdb.cached != nil && tdb.cached.COWMode() {
		batch.target, batch.barrier, batch.err = tdb.backend.CaptureCommandWALPrefixInto(&group.capturedPrefix, tdb.txnOwner)
	} else {
		batch.target, batch.barrier, batch.err = tdb.backend.CaptureCommandWALPrefixInto(&group.capturedPrefix, nil)
	}

	if batch.barrier.LSN != 0 {
		tdb.recordPublicCommandWALPendingLSN(batch.barrier.LSN)
	}
}

// failPanickedLeader runs only after capture gates and any successful target's
// Sync release defers have unwound. An assigned epoch cannot be retried or leave
// its waiters/Close behind an active leader after a capture or Sync panic.
func (group *publicCommandWALGroupCommit) failPanickedLeader(tdb *DB) {
	tdb.backend.MarkCommandWALRecoveryRequired()
	group.errors.Add(1)
	group.mu.Lock()
	if group.terminalErr == nil {
		group.terminalErr = ErrRecoveryRequired
	}
	group.completedTicket = group.nextTicket
	group.leaderActive = false
	group.cond.Broadcast()
	group.mu.Unlock()
}

// syncAndCompleteGroup runs after every public append gate is released. A
// nonzero ordinaryTicket belongs only to the fused ordinary, non-force caller.
// Its original ACK predicate can be checked under this same completion lock;
// generic and force callers retain awaitRegistered and its retryable errors.
func (group *publicCommandWALGroupCommit) syncAndCompleteGroup(tdb *DB, batch *publicCommandWALCapturedGroup, ordinaryTicket uint64) (ready bool, err error) {
	group.syncs.Add(1)
	if batch.err == nil {
		batch.barrier, batch.err = batch.target.Sync()
	}
	retryableForceOnlyErr := batch.err != nil && batch.target == nil && batch.barrier.LSN == 0 && batch.commits == 0
	if batch.err != nil {
		group.errors.Add(1)
		if !retryableForceOnlyErr {
			tdb.backend.MarkCommandWALRecoveryRequired()
		}
	}
	group.observeCompletedBatch(
		batch.count,
		batch.commits,
		batch.barrier.AttemptedDependencyEntries,
		batch.barrier.CoveredDependencyEntries,
		batch.err,
	)

	group.mu.Lock()
	if retryableForceOnlyErr {
		if group.retryableErrors == nil {
			group.retryableErrors = make(map[uint64]error, len(batch.forceTickets))
		}
		for _, ticket := range batch.forceTickets {
			group.retryableErrors[ticket] = batch.err
		}
		group.completedTicket = batch.lastTicket
	} else if group.terminalErr == nil && batch.err != nil {
		group.terminalErr = batch.err
	}
	if group.terminalErr != nil {
		group.completedTicket = group.nextTicket
	} else if !retryableForceOnlyErr {
		group.completedTicket = batch.lastTicket
	}
	group.leaderActive = false
	group.cond.Broadcast()
	if ordinaryTicket != 0 && group.completedTicket >= ordinaryTicket && group.publishTicket == ordinaryTicket && group.testAfterWait == nil {
		ready, err = true, group.terminalErr
	}
	group.mu.Unlock()
	return ready, err
}

func (group *publicCommandWALGroupCommit) observeCompletedBatch(
	batchCount, batchCommits int,
	attemptedDependencies, coveredDependencies uint64,
	err error,
) {
	if batchCount == 0 {
		return
	}
	group.attempts.Add(1)
	group.dependencyAttempts.Add(attemptedDependencies)
	if err != nil {
		return
	}
	group.groups.Add(1)
	group.commits.Add(uint64(batchCommits))
	group.participants.Add(uint64(batchCount))
	group.dependenciesCovered.Add(coveredDependencies)
	publicCommandWALGroupAtomicMax(&group.groupSizeMax, uint64(batchCount))
	publicCommandWALGroupAtomicMax(&group.dependencyCoveredMax, coveredDependencies)
}

func publicCommandWALGroupAtomicMax(dst *atomic.Uint64, value uint64) {
	for current := dst.Load(); value > current; current = dst.Load() {
		if dst.CompareAndSwap(current, value) {
			return
		}
	}
}

func (tdb *DB) publicCommandWALGroupStatsInto(stats map[string]string) {
	if tdb == nil || stats == nil || !tdb.commandWALCached {
		return
	}
	group := &tdb.commandWALGroupCommit
	stats["treedb.command_wal.group_commit.attempts_total"] = fmt.Sprintf("%d", group.attempts.Load())
	stats["treedb.command_wal.group_commit.groups_total"] = fmt.Sprintf("%d", group.groups.Load())
	stats["treedb.command_wal.group_commit.commits_total"] = fmt.Sprintf("%d", group.commits.Load())
	stats["treedb.command_wal.group_commit.participants_total"] = fmt.Sprintf("%d", group.participants.Load())
	stats["treedb.command_wal.group_commit.dependency_entries_attempted_total"] = fmt.Sprintf("%d", group.dependencyAttempts.Load())
	stats["treedb.command_wal.group_commit.dependency_entries_covered_total"] = fmt.Sprintf("%d", group.dependenciesCovered.Load())
	stats["treedb.command_wal.group_commit.leaders_total"] = fmt.Sprintf("%d", group.leaders.Load())
	stats["treedb.command_wal.group_commit.followers_total"] = fmt.Sprintf("%d", group.followers.Load())
	stats["treedb.command_wal.group_commit.syncs_total"] = fmt.Sprintf("%d", group.syncs.Load())
	stats["treedb.command_wal.group_commit.errors_total"] = fmt.Sprintf("%d", group.errors.Load())
	stats["treedb.command_wal.group_commit.forced_total"] = fmt.Sprintf("%d", group.forced.Load())
	stats["treedb.command_wal.group_commit.group_size_max"] = fmt.Sprintf("%d", group.groupSizeMax.Load())
	stats["treedb.command_wal.group_commit.dependency_entries_covered_max"] = fmt.Sprintf("%d", group.dependencyCoveredMax.Load())
	stats["treedb.command_wal.group_commit.wait_ns_total"] = fmt.Sprintf("%d", group.waitNsTotal.Load())
	stats["treedb.command_wal.group_commit.wait_ns_max"] = fmt.Sprintf("%d", group.waitNsMax.Load())
	stats["treedb.command_wal.group_commit.trigger.timeout_total"] = fmt.Sprintf("%d", group.triggerTimeout.Load())
	stats["treedb.command_wal.group_commit.trigger.commit_limit_total"] = fmt.Sprintf("%d", group.triggerCommits.Load())
	stats["treedb.command_wal.group_commit.trigger.byte_limit_total"] = fmt.Sprintf("%d", group.triggerBytes.Load())
	stats["treedb.command_wal.group_commit.trigger.explicit_sync_total"] = fmt.Sprintf("%d", group.triggerExplicit.Load())
	stats["treedb.command_wal.group_commit.trigger.sealed_collection_total"] = fmt.Sprintf("%d", group.triggerSealed.Load())
	stats["treedb.command_wal.group_commit.fallbacks_total"] = "0"
	stats["treedb.command_wal.group_commit.fallback.reason.coordinator_unavailable_total"] = "0"
	stats["treedb.command_wal.group_commit.bypasses_total"] = fmt.Sprintf("%d", group.bypassRelaxedNoGroup.Load()+group.bypassSoloDurable.Load())
	stats["treedb.command_wal.group_commit.bypass.reason.relaxed_race_no_group_total"] = fmt.Sprintf("%d", group.bypassRelaxedNoGroup.Load())
	stats["treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total"] = fmt.Sprintf("%d", group.bypassSoloDurable.Load())
}
