package treedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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
	timer                *time.Timer
	leaderActive         bool
	nextTicket           uint64
	completedTicket      uint64
	publishTicket        uint64
	terminalErr          error
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

	groups          atomic.Uint64
	commits         atomic.Uint64
	leaders         atomic.Uint64
	followers       atomic.Uint64
	syncs           atomic.Uint64
	errors          atomic.Uint64
	forced          atomic.Uint64
	triggerTimeout  atomic.Uint64
	triggerCommits  atomic.Uint64
	triggerBytes    atomic.Uint64
	triggerExplicit atomic.Uint64
	groupSizeMax    atomic.Uint64
	waitNsTotal     atomic.Uint64
	waitNsMax       atomic.Uint64
}

type publicCommandWALPublication struct {
	ticket      uint64
	fastRelaxed bool
}

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
	_, err = tdb.commandWALGroupCommit.awaitRegistered(tdb, ticket, leader, true)
	return err
}

func (tdb *DB) appendAndRegisterPublicCommandWAL(bytes int, preparedBytes *int, durable bool, pendingBookkeeping, groupCommitWait *time.Duration, appendFrame func() (uint64, error)) (publicCommandWALPublication, error) {
	var publication publicCommandWALPublication
	if tdb == nil || !tdb.commandWALCached || tdb.backend == nil || appendFrame == nil {
		return publication, ErrClosed
	}
	if !durable && tdb.beginFastRelaxedPublicCommandWALPublication() {
		publication.fastRelaxed = true
		lsn, err := appendFrame()
		if lsn != 0 {
			start := time.Now()
			tdb.recordPublicCommandWALPendingLSN(lsn)
			if pendingBookkeeping != nil {
				*pendingBookkeeping += time.Since(start)
			}
		}
		return publication, err
	}
	if durable {
		tdb.commandWALGroupCommit.durableIntents.Add(1)
	}
	tdb.commandWALPublicOperationGate.RLock()
	tdb.commandWALPublicPublishMu.Lock()
	lsn, err := appendFrame()
	if lsn != 0 {
		start := time.Now()
		tdb.recordPublicCommandWALPendingLSN(lsn)
		if pendingBookkeeping != nil {
			*pendingBookkeeping += time.Since(start)
		}
	}
	var ticket uint64
	var leader bool
	if err == nil && lsn != 0 {
		if preparedBytes != nil {
			bytes = *preparedBytes
		}
		ticket, leader, err = tdb.commandWALGroupCommit.register(bytes, durable, false)
	}
	tdb.commandWALPublicPublishMu.Unlock()
	tdb.commandWALPublicOperationGate.RUnlock()
	if durable {
		tdb.commandWALGroupCommit.durableIntents.Add(-1)
	}
	if err != nil || ticket == 0 {
		return publication, err
	}
	publication.ticket = ticket
	if hook := tdb.commandWALGroupCommit.testAfterRegister; hook != nil {
		hook(ticket, durable)
	}
	waitStart := time.Now()
	ticket, err = tdb.commandWALGroupCommit.awaitRegistered(tdb, ticket, leader, false)
	if groupCommitWait != nil {
		*groupCommitWait += time.Since(waitStart)
	}
	publication.ticket = ticket
	return publication, err
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

func (group *publicCommandWALGroupCommit) register(bytes int, durable, force bool) (ticket uint64, leader bool, err error) {
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
	group.nextTicket++
	ticket = group.nextTicket
	if !durable {
		if group.pendingCount == 0 {
			group.completedTicket = ticket
			group.cond.Broadcast()
		}
		return ticket, false, nil
	}
	group.initLeaderResourcesLocked()
	group.pendingCount++
	if !force {
		group.pendingCommits++
	} else {
		group.pendingForceTickets = append(group.pendingForceTickets, ticket)
	}
	group.pendingBytes += bytes
	leader = !group.leaderActive
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
	case group.pendingCommits >= group.maxCommits:
		group.triggerLocked(&group.triggerCommits)
	case group.pendingBytes >= group.maxBytes:
		group.triggerLocked(&group.triggerBytes)
	}
	return ticket, leader, nil
}

func (group *publicCommandWALGroupCommit) awaitRegistered(tdb *DB, ticket uint64, leader, force bool) (uint64, error) {
	if group == nil || tdb == nil || tdb.backend == nil || ticket == 0 {
		return 0, ErrClosed
	}
	enqueued := time.Now()
	if leader {
		group.runLeader(tdb)
	}

	group.mu.Lock()
	for (group.completedTicket < ticket || group.publishTicket != ticket) && group.terminalErr == nil {
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
	waitNs := uint64(time.Since(enqueued).Nanoseconds())
	group.waitNsTotal.Add(waitNs)
	publicCommandWALGroupAtomicMax(&group.waitNsMax, waitNs)
	if err != nil {
		return 0, err
	}
	return ticket, nil
}

func (group *publicCommandWALGroupCommit) initLocked() {
	if group.cond.L == nil {
		group.cond.L = &group.mu
		group.publishTicket = 1
	}
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

func (group *publicCommandWALGroupCommit) initLeaderResourcesLocked() {
	if group.wake == nil {
		group.wake = make(chan struct{}, 1)
	}
	if group.timer == nil {
		group.timer = time.NewTimer(time.Hour)
		if !group.timer.Stop() {
			<-group.timer.C
		}
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
	if publication.fastRelaxed {
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

func (group *publicCommandWALGroupCommit) runLeader(tdb *DB) {
	group.mu.Lock()
	group.initLocked()
	group.initLeaderResourcesLocked()
	delay := group.delay
	wake := group.wake
	timer := group.timer
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
	leaderWaitHook := group.testBeforeLeaderWait
	group.mu.Unlock()

	if leaderWaitHook != nil {
		leaderWaitHook()
	}
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

	// Public append and ticket registration use the same outer mutex. Holding
	// it from snapshot through the backend barrier makes the batch an exact WAL
	// prefix: no frame can land between the snapshot and the barrier.
	tdb.commandWALPublicOperationGate.Lock()
	tdb.commandWALPublicPublishMu.Lock()
	group.mu.Lock()
	batchCount := group.pendingCount
	batchCommits := group.pendingCommits
	batchLastTicket := group.nextTicket
	batchForceTickets := append([]uint64(nil), group.pendingForceTickets...)
	group.pendingCount = 0
	group.pendingCommits = 0
	group.pendingBytes = 0
	group.pendingForceTickets = group.pendingForceTickets[:0]
	group.pendingTriggered = false
	select {
	case <-group.wake:
	default:
	}
	hook := group.testBeforeSync
	group.mu.Unlock()
	if batchCount == 0 {
		group.mu.Lock()
		group.leaderActive = false
		group.cond.Broadcast()
		group.mu.Unlock()
		tdb.commandWALPublicPublishMu.Unlock()
		tdb.commandWALPublicOperationGate.Unlock()
		return
	}

	if hook != nil {
		hook(batchCount)
	}
	group.syncs.Add(1)
	barrierLSN, err := tdb.syncPublicCommandWALDirect()
	retryableForceOnlyErr := err != nil && barrierLSN == 0 && batchCommits == 0
	if err != nil {
		group.errors.Add(1)
		if !retryableForceOnlyErr {
			tdb.backend.MarkCommandWALRecoveryRequired()
		}
	}
	group.observeCompletedBatch(batchCount, batchCommits)

	group.mu.Lock()
	if retryableForceOnlyErr {
		if group.retryableErrors == nil {
			group.retryableErrors = make(map[uint64]error, len(batchForceTickets))
		}
		for _, ticket := range batchForceTickets {
			group.retryableErrors[ticket] = err
		}
		group.completedTicket = batchLastTicket
	} else if group.terminalErr == nil && err != nil {
		group.terminalErr = err
	}
	if group.terminalErr != nil {
		group.completedTicket = group.nextTicket
	} else if !retryableForceOnlyErr {
		group.completedTicket = batchLastTicket
	}
	group.leaderActive = false
	group.cond.Broadcast()
	group.mu.Unlock()
	tdb.commandWALPublicPublishMu.Unlock()
	tdb.commandWALPublicOperationGate.Unlock()
}

func (group *publicCommandWALGroupCommit) observeCompletedBatch(batchCount, batchCommits int) {
	if batchCount == 0 {
		return
	}
	group.groups.Add(1)
	group.commits.Add(uint64(batchCommits))
	publicCommandWALGroupAtomicMax(&group.groupSizeMax, uint64(batchCount))
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
	stats["treedb.command_wal.group_commit.groups_total"] = fmt.Sprintf("%d", group.groups.Load())
	stats["treedb.command_wal.group_commit.commits_total"] = fmt.Sprintf("%d", group.commits.Load())
	stats["treedb.command_wal.group_commit.leaders_total"] = fmt.Sprintf("%d", group.leaders.Load())
	stats["treedb.command_wal.group_commit.followers_total"] = fmt.Sprintf("%d", group.followers.Load())
	stats["treedb.command_wal.group_commit.syncs_total"] = fmt.Sprintf("%d", group.syncs.Load())
	stats["treedb.command_wal.group_commit.errors_total"] = fmt.Sprintf("%d", group.errors.Load())
	stats["treedb.command_wal.group_commit.forced_total"] = fmt.Sprintf("%d", group.forced.Load())
	stats["treedb.command_wal.group_commit.group_size_max"] = fmt.Sprintf("%d", group.groupSizeMax.Load())
	stats["treedb.command_wal.group_commit.wait_ns_total"] = fmt.Sprintf("%d", group.waitNsTotal.Load())
	stats["treedb.command_wal.group_commit.wait_ns_max"] = fmt.Sprintf("%d", group.waitNsMax.Load())
	stats["treedb.command_wal.group_commit.trigger.timeout_total"] = fmt.Sprintf("%d", group.triggerTimeout.Load())
	stats["treedb.command_wal.group_commit.trigger.commit_limit_total"] = fmt.Sprintf("%d", group.triggerCommits.Load())
	stats["treedb.command_wal.group_commit.trigger.byte_limit_total"] = fmt.Sprintf("%d", group.triggerBytes.Load())
	stats["treedb.command_wal.group_commit.trigger.explicit_sync_total"] = fmt.Sprintf("%d", group.triggerExplicit.Load())
}
