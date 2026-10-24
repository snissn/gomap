package treedb

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

func TestOrderedCollectionSealedHandoffSkipsSecondWindow(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		name := "known-size"
		if incremental {
			name = "incremental"
		}
		t.Run(name, func(t *testing.T) {
			opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_ordered"
			db, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			// A second engine window would stall for an hour. Both caller-owned
			// submission paths have already sealed their cut; neither is a force.
			// The nondefault delay disqualifies the ordinary idle-singleton bypass.
			db.commandWALGroupCommit.delay = time.Hour
			rows := []mvccadmission.LogicalMutation{{Key: []byte("sealed"), Value: []byte("value"), Timestamp: 1}}
			input := db.NewMVCCAdmission().LogicalInput(rows, 0)
			var completion LogicalMVCCCompletion
			if incremental {
				var builder LogicalMVCCBuilder
				if err := db.BeginLogicalMVCC(&builder); err != nil {
					t.Fatal(err)
				}
				defer builder.Abort()
				if err := builder.Append(rows, input); err != nil {
					t.Fatal(err)
				}
				if err := builder.Submit(true, &completion); err != nil {
					t.Fatal(err)
				}
			} else if err := db.SubmitLogicalMVCC(rows, input, true, &completion); err != nil {
				t.Fatal(err)
			}
			group := &db.commandWALGroupCommit
			group.mu.Lock()
			sealed, forces, singleton := group.pendingSealed, len(group.pendingForceTickets), group.pendingSingleton
			// A broken handoff must still drain its completion before DB.Close.
			if !sealed {
				group.triggerLocked(&group.triggerSealed)
			}
			group.mu.Unlock()
			done := make(chan error, 1)
			go func() { done <- completion.Complete() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				group.mu.Lock()
				group.triggerLocked(&group.triggerSealed)
				group.mu.Unlock()
				<-done
				t.Fatal("sealed native cut started a second collection window")
			}
			if !sealed || forces != 0 || singleton != 0 {
				t.Fatalf("sealed=%v force tickets=%d idle singleton=%d", sealed, forces, singleton)
			}
			if group.triggerTimeout.Load() != 0 || group.forced.Load() != 0 || group.groups.Load() != 1 {
				t.Fatalf("timeouts=%d forced=%d groups=%d", group.triggerTimeout.Load(), group.forced.Load(), group.groups.Load())
			}
		})
	}
}

func TestPublicCommandWALCollectionBudgetAgesFromRegistration(t *testing.T) {
	var group publicCommandWALGroupCommit
	group.delay = time.Hour
	before := time.Now()
	if _, _, err := group.register(1, true, false); err != nil {
		t.Fatal(err)
	}
	first := group.pendingDeadline
	if first.Before(before.Add(time.Hour)) || first.After(time.Now().Add(time.Hour)) {
		t.Fatal("deadline not established at registration")
	}
	if _, _, err := group.register(1, true, false); err != nil {
		t.Fatal(err)
	}
	if group.pendingDeadline != first {
		t.Fatal("arrival restarted existing collection window")
	}
	group.timer.Stop()

	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.commandWALGroupCommit.delay = time.Hour
	db.commandWALGroupCommit.testAfterRegister = func(_ uint64, _ bool) {
		// Model collection time already spent while the previous barrier ran.
		g := &db.commandWALGroupCommit
		g.mu.Lock()
		g.pendingDeadline = time.Now().Add(-time.Second)
		g.mu.Unlock()
	}
	done := make(chan error, 1)
	go func() {
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte("aged"), []byte("value")); err != nil {
			done <- err
			return
		}
		done <- b.WriteSync()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		g := &db.commandWALGroupCommit
		g.mu.Lock()
		g.triggerLocked(&g.triggerSealed)
		g.mu.Unlock()
		<-done
		t.Fatal("leader takeover restarted an expired collection budget")
	}
}
