package db

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// This consumer exists only during one synchronous terminal call. Neither the
// coordinator, registrar cell, candidate nor retry worker may retain it.
type stableSegmentTerminalConsumerV1 struct {
	db      *DB
	manager *valuelog.Manager
	plan    *valuelog.StableSegmentTerminalPlan
	storage *valuelog.StableSegmentTerminalStorage
	ended   bool
}

func (consumer *stableSegmentTerminalConsumerV1) BeginTerminalRelease() (bool, error) {
	if consumer.manager == nil {
		// Ordinary closures need no registrar. Finite retention validation below
		// refuses the missing exact Manager before any ownership effects.
		return false, nil
	}
	return consumer.manager.BeginTerminalRelease()
}

func (consumer *stableSegmentTerminalConsumerV1) PrepareTerminalRelease(groups []rootpublication.StableSegmentTerminalGroup) error {
	if consumer.manager == nil {
		return rootpublication.ErrStableTerminalConsumerRequired
	}
	if consumer.plan != nil {
		return rootpublication.ErrResourceOwnership
	}
	var plan *valuelog.StableSegmentTerminalPlan
	var err error
	if consumer.storage != nil {
		plan, err = consumer.manager.PrepareStableSegmentTerminalReleaseWithStorage(groups, consumer.storage)
	} else {
		plan, err = consumer.manager.PrepareStableSegmentTerminalRelease(groups)
	}
	if err != nil {
		return err
	}
	consumer.plan = plan
	return nil
}

func (consumer *stableSegmentTerminalConsumerV1) ValidateSegmentRetention(retention rootpublication.StableSegmentRetention) error {
	if consumer.manager == nil {
		return rootpublication.ErrStableTerminalConsumerRequired
	}
	if consumer.plan != nil {
		return consumer.plan.ValidateSegmentRetention(retention)
	}
	return consumer.manager.ValidateStableSegmentRetention(retention)
}

func (consumer *stableSegmentTerminalConsumerV1) ReleaseSegmentRetention(retention rootpublication.StableSegmentRetention) error {
	if consumer.plan == nil {
		return rootpublication.ErrStableTerminalConsumerRequired
	}
	return consumer.plan.ReleaseSegmentRetention(retention, consumer)
}

func (consumer *stableSegmentTerminalConsumerV1) EndTerminalRelease(joined bool) {
	if consumer.ended {
		return
	}
	consumer.ended = true
	if consumer.plan != nil {
		consumer.plan.Close()
		consumer.plan = nil
	}
	if consumer.manager != nil {
		consumer.manager.EndTerminalRelease(joined)
	}
}

func (consumer *stableSegmentTerminalConsumerV1) SyncStableSegmentDeletion(path string, resource durabilitycut.Resource) error {
	err := syncDeletionNamespaceDirectory(filepath.Dir(path), resource)
	if err == nil {
		return nil
	}
	err = fmt.Errorf("persist terminal segment deletion: %w", err)
	if consumer.db != nil {
		consumer.db.publicationPoisoned.Store(true)
		consumer.db.recordTerminalErrorV1(err)
	}
	return err
}

// A terminal reporter runs on Coordinator.run. NotifyError may call DB.Close,
// whose Stop joins that same goroutine. Record poison without invoking arbitrary
// callbacks; the checked report and foreground WaitThrough return the error.
func (db *DB) recordTerminalErrorV1(err error) {
	if db == nil || err == nil {
		return
	}
	db.bgErrMu.Lock()
	if db.bgErr == nil {
		db.bgErr = publicRootPublicationErrorV1(err)
	}
	db.bgErrMu.Unlock()
}

func (runtime *rootPublicationRuntimeV1) ReportRootPublishTerminal(coordinator *rootpublication.Coordinator, attempt rootpublication.PublishAttempt, result rootpublication.PublishResult) error {
	runtime.mu.Lock()
	if runtime.terminalReporting {
		runtime.mu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	runtime.terminalReporting = true
	arena := runtime.terminalArena
	runtime.mu.Unlock()
	defer func() { runtime.mu.Lock(); runtime.terminalReporting = false; runtime.mu.Unlock() }()
	consumer := stableSegmentTerminalConsumerV1{db: runtime.db, manager: runtime.db.valueLogManager}
	if arena != nil {
		consumer.storage = arena.managerStorage
	}
	err := coordinator.ReportPublishResultWithTerminalTransition(attempt, result, &consumer, runtime)
	if err != nil {
		runtime.db.publicationPoisoned.Store(true)
		runtime.mu.Lock()
		if runtime.poison == nil {
			runtime.poison = err
		}
		runtime.mu.Unlock()
		runtime.db.recordTerminalErrorV1(err)
	}
	return err
}

// Append only actual owned roles. Borrowed seal.base slot views and a member's
// adopted current-visible alias are not additional cleanup owners.
func appendTerminalOwnedSetV1(roles []rootpublication.StableTerminalOwnedSet, set *rootpublication.StableResourceSet) []rootpublication.StableTerminalOwnedSet {
	if set == nil || set.Owner() == rootpublication.ResourceOwnerReleased {
		return roles
	}
	for _, role := range roles {
		if role.Set == set {
			return roles
		}
	}
	return append(roles, rootpublication.StableTerminalOwnedSet{Set: set, Owner: set.Owner()})
}
func (runtime *rootPublicationRuntimeV1) TerminalOwnedResources() ([]rootpublication.StableTerminalOwnedSet, []*rootpublication.StableResourceToken, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	seal := runtime.activeSeal
	if seal == nil || !seal.storageComplete || seal.committed {
		return nil, nil, rootpublication.ErrResourceOwnership
	}
	var roles []rootpublication.StableTerminalOwnedSet
	var tokens []*rootpublication.StableResourceToken
	if arena := runtime.terminalArena; arena != nil {
		if err := runtime.checkTerminalEnvelopeLocked(nil, false, false); err != nil {
			return nil, nil, err
		}
		if slot := seal.base.slotResources[seal.target]; slot != nil && slot.Len() > arena.resources {
			return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		var err error
		roles, tokens, err = arena.scratch.Begin()
		if err != nil {
			return nil, nil, err
		}
	}
	roles = appendTerminalOwnedSetV1(roles, seal.base.slotResources[seal.target])
	for _, retired := range runtime.seals {
		if retired != seal && retired.latestSequence <= seal.latestSequence {
			roles = appendTerminalOwnedSetV1(roles, retired.resources)
			roles = appendTerminalOwnedSetV1(roles, retired.overwrittenResources)
			if retired.token != nil {
				tokens = append(tokens, retired.token)
			}
		}
	}
	if seal.token != nil {
		tokens = append(tokens, seal.token)
	}
	for seq, member := range runtime.visibleMembers {
		if seq <= seal.latestSequence {
			roles = appendTerminalOwnedSetV1(roles, member.previousResources)
		}
	}
	return roles, tokens, nil
}
func (runtime *rootPublicationRuntimeV1) CommitTerminalPublication() error {
	runtime.mu.Lock()
	seal := runtime.activeSeal
	runtime.mu.Unlock()
	_, err := runtime.commitPublishedSeal(seal)
	return err
}
func (runtime *rootPublicationRuntimeV1) CompleteTerminalPublication() error {
	runtime.mu.Lock()
	seal := runtime.activeSeal
	runtime.mu.Unlock()
	return runtime.completePublishedSealTerminal(seal)
}

// Prepared cleanup is outside all engine locks. The exact executing pending
// candidate prevents overlap; only post-IO scalar bookkeeping reacquires the gate.
func (db *DB) releasePreparedDurableRootCandidateV1(candidate *durableRootPublishCandidateV1, consumer *stableSegmentTerminalConsumerV1) error {
	roles := appendTerminalOwnedSetV1(nil, candidate.overwrittenResources)
	loose := []*rootpublication.StableResourceToken{candidate.token}
	err := rootpublication.ReleasePreparedStableTerminalOwnedGroup(roles, loose, consumer)
	if err == nil {
		err = candidate.completeTerminalRelease()
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	if err != nil {
		_, err = db.poisonDurableRootCandidateV1(candidate, err)
		return err
	}
	// The executing DB reservation persists through consumer End; its caller
	// clears the resolved pending field only after End has released all gates.
	return nil
}

func appendCandidateTerminalOwnersV1(roles []rootpublication.StableTerminalOwnedSet, tokens []*rootpublication.StableResourceToken, candidate *durableRootPublishCandidateV1) ([]rootpublication.StableTerminalOwnedSet, []*rootpublication.StableResourceToken) {
	if candidate == nil || candidate.released {
		return roles, tokens
	}
	roles = appendTerminalOwnedSetV1(roles, candidate.resources)
	roles = appendTerminalOwnedSetV1(roles, candidate.overwrittenResources)
	if candidate.token != nil {
		tokens = append(tokens, candidate.token)
	}
	return roles, tokens
}

// Teardown has stopped the worker/builders. Retain actual ownership fields until
// all terminal facets finish; saved Manager stays on this synchronous stack.
func (db *DB) releaseCloseTerminalOwnersV1(manager *valuelog.Manager, metadataOnly bool) error {
	var roles []rootpublication.StableTerminalOwnedSet
	var tokens []*rootpublication.StableResourceToken
	runtime := db.rootPublication
	var arena *rootPublicationTerminalArenaV1
	if runtime != nil {
		runtime.mu.Lock()
		arena = runtime.terminalArena
		if arena != nil {
			if runtime.terminalReporting {
				runtime.mu.Unlock()
				return rootpublication.ErrResourceOwnership
			}
			if err := runtime.checkTerminalEnvelopeLocked(nil, false, false); err != nil {
				runtime.mu.Unlock()
				return err
			}
			var err error
			roles, tokens, err = arena.scratch.Begin()
			if err != nil {
				runtime.mu.Unlock()
				return err
			}
		}
		runtime.mu.Unlock()
	}
	if arena != nil {
		defer arena.scratch.End()
	}
	db.durablePublishMu.Lock()
	if arena != nil {
		if len(db.durableRoot.ambiguous) > 1 {
			db.durablePublishMu.Unlock()
			return rootpublication.ErrStableMetadataShapeUnsupported
		}
		check := func(s *rootpublication.StableResourceSet) bool { return s == nil || s.Len() <= arena.resources }
		for _, s := range db.durableRoot.slotResources {
			if !check(s) {
				db.durablePublishMu.Unlock()
				return rootpublication.ErrStableMetadataShapeUnsupported
			}
		}
		checkCandidate := func(c *durableRootPublishCandidateV1) bool {
			return c == nil || check(c.resources) && check(c.overwrittenResources)
		}
		if !checkCandidate(db.durableRoot.pending) {
			db.durablePublishMu.Unlock()
			return rootpublication.ErrStableMetadataShapeUnsupported
		}
		for _, c := range db.durableRoot.ambiguous {
			if !checkCandidate(c) {
				db.durablePublishMu.Unlock()
				return rootpublication.ErrStableMetadataShapeUnsupported
			}
		}
	}
	if pending := db.durableRoot.pending; pending != nil && pending.executing {
		db.durablePublishMu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	for _, set := range db.durableRoot.slotResources {
		roles = appendTerminalOwnedSetV1(roles, set)
	}
	roles, tokens = appendCandidateTerminalOwnersV1(roles, tokens, db.durableRoot.pending)
	for _, candidate := range db.durableRoot.ambiguous {
		roles, tokens = appendCandidateTerminalOwnersV1(roles, tokens, candidate)
	}
	db.durablePublishMu.Unlock()
	if runtime != nil {
		runtime.mu.Lock()
		roles = appendTerminalOwnedSetV1(roles, runtime.visibleResources)
		for _, member := range runtime.visibleMembers {
			roles = appendTerminalOwnedSetV1(roles, member.previousResources)
			if !member.resourcesAdopted {
				roles = appendTerminalOwnedSetV1(roles, member.resources)
			}
		}
		for _, seal := range runtime.seals {
			roles = appendTerminalOwnedSetV1(roles, seal.resources)
			roles = appendTerminalOwnedSetV1(roles, seal.overwrittenResources)
			if seal.token != nil {
				tokens = append(tokens, seal.token)
			}
		}
		runtime.mu.Unlock()
	}
	handoff := db.rootTerminalHandoff
	if handoff != nil {
		if arena != nil {
			if handoff.Len() > arena.members {
				return rootpublication.ErrStableMetadataShapeUnsupported
			}
			var err error
			roles, err = handoff.AppendTerminalOwnedRoles(roles)
			if err != nil {
				return err
			}
			for _, role := range roles {
				if role.Set != nil && role.Set.Len() > arena.resources {
					return rootpublication.ErrStableMetadataShapeUnsupported
				}
			}
		} else {
			for _, set := range handoff.Sets() {
				roles = appendTerminalOwnedSetV1(roles, set)
			}
		}
	}
	var consumer rootpublication.StableSegmentTerminalConsumer
	concrete := stableSegmentTerminalConsumerV1{db: db, manager: manager}
	if arena != nil {
		concrete.storage = arena.managerStorage
	}
	joined := false
	if !metadataOnly {
		consumer = &concrete
		var err error
		joined, err = consumer.BeginTerminalRelease()
		if err != nil {
			return err
		}
		defer consumer.EndTerminalRelease(joined)
	}
	var prepareErr error
	if arena != nil {
		prepareErr = rootpublication.PrepareStableTerminalOwnedGroupWithScratch(roles, tokens, consumer, arena.scratch)
	} else {
		prepareErr = rootpublication.PrepareStableTerminalOwnedGroup(roles, tokens, consumer)
	}
	if prepareErr != nil {
		return prepareErr
	}
	if err := rootpublication.ReleasePreparedStableTerminalOwnedGroup(roles, tokens, consumer); err != nil {
		return err
	}
	if handoff != nil {
		if err := handoff.ReleasePreparedWithTerminal(consumer); err != nil {
			return err
		}
	}
	if consumer != nil {
		consumer.EndTerminalRelease(joined)
	}
	if arena != nil {
		arena.scratch.End()
	}
	// Release fields only after actual checked terminal ownership resolved.
	var terminalErr error
	db.durablePublishMu.Lock()
	if candidate := db.durableRoot.pending; candidate != nil {
		terminalErr = errors.Join(terminalErr, candidate.completeTerminalRelease())
	}
	for _, candidate := range db.durableRoot.ambiguous {
		terminalErr = errors.Join(terminalErr, candidate.completeTerminalRelease())
	}
	if terminalErr == nil {
		db.durableRoot.slotResources = [2]*rootpublication.StableResourceSet{}
		db.durableRoot.pending = nil
		db.durableRoot.ambiguous = nil
	}
	db.durablePublishMu.Unlock()
	if terminalErr != nil {
		return terminalErr
	}
	if runtime != nil {
		runtime.mu.Lock()
		for _, seal := range runtime.seals {
			seal.resources = nil
			seal.overwrittenResources = nil
			seal.token = nil
			seal.released = true
			seal.clearTerminalBacking()
		}
		for _, member := range runtime.visibleMembers {
			member.previousResources = nil
			member.resources = nil
			// Allocator remains attached until after runtime/handoff discharge.
			if member.prepared != nil {
				if err := member.prepared.ClearTerminalBackingV1(); err != nil {
					terminalErr = errors.Join(terminalErr, err)
				}
			}
			if terminalErr == nil {
				member.prepared = nil
				member.install = nil
				member.preparedLimits = nil
				member.terminalLoan.closeLocked()
				member.terminalLoan = nil
			}
		}
		if terminalErr == nil {
			runtime.visibleResources = nil
			runtime.seals = nil
			runtime.activeSeal = nil
			clear(runtime.visibleMembers)
			terminalErr = runtime.closeTerminalArenaLocked()
		}
		runtime.mu.Unlock()
	}
	if terminalErr != nil {
		return terminalErr
	}
	db.rootPublication = nil
	db.rootTerminalHandoff = nil
	return nil
}
