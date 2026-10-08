package db

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

func TestStableTerminalDBRoleCollectionPreservesCurrentAndBorrowedSlots(t *testing.T) {
	released := [3]int{}
	makeSet := func(i int) *rootpublication.StableResourceSet {
		return stableContractResourceSet(t, stableContractDescriptor{generation: uint64(i + 1), kind: rootpublication.ResourceValueLog, reachability: rootpublication.ReachabilityUserRoot, frontier: 16, onRelease: func() { released[i]++ }})
	}
	oldSlot, previous, current := makeSet(0), makeSet(1), makeSet(2)
	seal := &rootPublicationSealV1{storageComplete: true, latestSequence: 2, target: 0}
	seal.base.slotResources = [2]*rootpublication.StableResourceSet{oldSlot, current}
	runtime := &rootPublicationRuntimeV1{activeSeal: seal, seals: []*rootPublicationSealV1{seal}, visibleResources: current, visibleMembers: map[uint64]*rootPublicationVisibleMemberV1{
		1: {previousResources: oldSlot, resources: previous, resourcesAdopted: true},
		2: {previousResources: previous, resources: current, resourcesAdopted: true},
	}}
	roles, tokens, err := runtime.TerminalOwnedResources()
	if err != nil {
		t.Fatal(err)
	}
	if len(roles) != 2 || len(tokens) != 0 {
		t.Fatalf("actual retired ownership: %d %d", len(roles), len(tokens))
	}
	if err = rootpublication.PrepareStableTerminalOwnedGroup(roles, tokens, nil); err != nil {
		t.Fatal(err)
	}
	if err = rootpublication.ReleasePreparedStableTerminalOwnedGroup(roles, tokens, nil); err != nil {
		t.Fatal(err)
	}
	if released != [3]int{1, 1, 0} || current.Owner() == rootpublication.ResourceOwnerReleased {
		t.Fatalf("borrowed/current slot was released or old role duplicated: %v", released)
	}
	if err = current.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableTerminalDBSavedManagerCloseDischargesActualOwners(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	if err = database.SetSync([]byte("terminal-owner"), bytes.Repeat([]byte("v"), 32)); err != nil {
		t.Fatal(err)
	}
	saved := database.valueLogManager
	if saved == nil {
		t.Fatal("missing actual value-log Manager")
	}
	runtime := database.rootPublication
	if runtime == nil {
		t.Fatal("missing actual publisher")
	}
	// Close really clears db.valueLogManager before terminal grouping, retaining
	// the exact saved Manager on its stack and the runtime on the DB until done.
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if database.valueLogManager != nil || database.rootPublication != nil || database.rootTerminalHandoff != nil {
		t.Fatal("Close retained resolved runtime ownership")
	}
	if runtime.visibleResources != nil || len(runtime.seals) != 0 || len(runtime.visibleMembers) != 0 {
		t.Fatal("saved-manager Close discarded unfinished runtime fields")
	}
	if err = saved.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStableTerminalDBRepeatedSlotReplacementDurableReopen(t *testing.T) {
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i <= 4; i++ {
		if err = database.SetSync([]byte("slot-terminal"), bytes.Repeat([]byte{i}, 32)); err != nil {
			t.Fatal(err)
		}
		runtime := database.rootPublication
		runtime.mu.Lock()
		retained := len(runtime.visibleMembers) + len(runtime.seals) + len(runtime.debt)
		runtime.mu.Unlock()
		if retained != 0 {
			t.Fatalf("completed publication retained logical or cleanup debt: %d", retained)
		}
		for _, set := range database.durableRoot.slotResources {
			if set != nil && set.Owner() == rootpublication.ResourceOwnerReleased {
				t.Fatal("new durable slot aliases a released cleanup owner")
			}
		}
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Get([]byte("slot-terminal"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{4}, 32)) {
		t.Fatal("new durable authority lost across checked terminal slot replacements")
	}
}

func TestStableTerminalDBControlCensusRefusesOverflow(t *testing.T) {
	census, err := terminalCallerBackingCensusV1(2, 2, 1, 7, 3)
	if err != nil || census.Allocations != 7 || census.AllocatedClassBytes == 0 || census.LiveClassBytes != census.AllocatedClassBytes {
		t.Fatalf("actual controls/scratch census: %+v %v", census, err)
	}
	if _, err = terminalCallerBackingCensusV1(^uint64(0), 0, 0, 0, 0); err == nil {
		t.Fatal("unrepresentable retained control census admitted")
	}
}

// The real direct entry must publish once, retain exact pending ownership during
// cleanup, and permit reentrant gate acquisition without retaining engine locks.
func TestStableTerminalDirectCandidateUnlockedCleanupAndReservation(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	idx := database.idx.Load()
	database.durablePublishMu.Lock()
	database.rootReuseMu.Lock()
	next := database.meta
	next.CommitSeq = database.durableRoot.record.CommitSeq + 1
	resources, _, err := database.prepareDependencyDirectoryV2(idx, next.CommitSeq, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := database.prepareDurableRootCandidateV1(idx, next, nil, resources, false)
	if err != nil {
		t.Fatal(err)
	}
	releases := 0
	old := stableContractResourceSet(t, stableContractDescriptor{generation: 71, kind: rootpublication.ResourceValueLog, reachability: rootpublication.ReachabilityUserRoot, frontier: 16, onRelease: func() {
		releases++
		if !database.durablePublishMu.TryLock() {
			t.Error("terminal callback retained durable publish lock")
		} else {
			database.durablePublishMu.Unlock()
		}
		if !database.rootReuseMu.TryLock() {
			t.Error("terminal callback retained root reuse lock")
		} else {
			database.rootReuseMu.Unlock()
		}
		if database.durableRoot.pending != candidate || !candidate.executing || !candidate.committed {
			t.Error("actual pending/executing owner absent during cleanup")
		}
		if _, err := database.executeDurableRootCandidateV1(candidate); !errors.Is(err, rootpublication.ErrResourceOwnership) {
			t.Errorf("reentrant candidate execution: %v", err)
		}
	}})
	candidate.base.slotResources[candidate.target] = old
	database.durableRoot.slotResources[candidate.target] = old
	database.durableRoot.pending = candidate
	database.rootReuseMu.Unlock()
	database.durablePublishMu.Unlock()
	published, err := database.executeDurableRootCandidateV1(candidate)
	if err != nil {
		t.Fatal(err)
	}
	database.meta = published
	if releases != 1 || !candidate.released || candidate.executing || database.durableRoot.pending != nil {
		t.Fatal("terminal owner/reservation not discharged exactly once")
	}
	sequence := database.durableRoot.record.CommitSeq
	if _, err = database.executeDurableRootCandidateV1(candidate); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal("completed candidate replay allowed", err)
	}
	if database.durableRoot.record.CommitSeq != sequence || releases != 1 {
		t.Fatal("completed candidate replay changed durable authority")
	}
}

func TestStableTerminalDirectPrecommitFailureClearsExecutingAndRetainsRetry(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	idx := database.idx.Load()
	database.durablePublishMu.Lock()
	database.rootReuseMu.Lock()
	next := database.meta
	next.CommitSeq = database.durableRoot.record.CommitSeq + 1
	resources, _, err := database.prepareDependencyDirectoryV2(idx, next.CommitSeq, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := database.prepareDurableRootCandidateV1(idx, next, nil, resources, false)
	if err != nil {
		t.Fatal(err)
	}
	database.durableRoot.pending = candidate
	database.rootReuseMu.Unlock()
	database.durablePublishMu.Unlock()
	database.testFailWriteMeta.Store(true)
	if _, err = database.executeDurableRootCandidateV1(candidate); err == nil {
		t.Fatal("pre-meta failure ignored")
	}
	if candidate.executing || candidate.committed || candidate.released || database.durableRoot.pending != candidate {
		t.Fatal("precommit refusal lost exact retry owner or execution reservation")
	}
	database.testFailWriteMeta.Store(false)
	published, err := database.retryPendingDurableRootV1()
	if err != nil {
		t.Fatal(err)
	}
	database.meta = published
	if !candidate.committed || !candidate.released || candidate.executing || database.durableRoot.pending != nil {
		t.Fatal("same candidate retry did not finish")
	}
}
