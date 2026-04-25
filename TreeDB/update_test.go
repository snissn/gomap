package treedb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestUpdateConcurrentSetMembershipNoLostUpdates(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const workers = 64
	key := []byte("members")
	start := make(chan struct{})
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			member := fmt.Sprintf("member-%02d", i)
			errs <- db.Update(key, func(old []byte) (treedb.UpdateResult, error) {
				members := make(map[string]struct{}, workers)
				if old != nil {
					var existing []string
					if err := json.Unmarshal(old, &existing); err != nil {
						return treedb.UpdateResult{}, err
					}
					for _, name := range existing {
						members[name] = struct{}{}
					}
				}
				members[member] = struct{}{}

				next := make([]string, 0, len(members))
				for name := range members {
					next = append(next, name)
				}
				sort.Strings(next)
				encoded, err := json.Marshal(next)
				if err != nil {
					return treedb.UpdateResult{}, err
				}
				return treedb.SetUpdate(encoded), nil
			})
		}(i)
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
	}

	raw, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal members: %v", err)
	}
	if len(got) != workers {
		t.Fatalf("member count mismatch got=%d want=%d members=%v", len(got), workers, got)
	}
	for i, member := range got {
		want := fmt.Sprintf("member-%02d", i)
		if member != want {
			t.Fatalf("member[%d] = %q, want %q", i, member, want)
		}
	}
}

func TestUpdateSetDeleteNoop(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("key")
	if err := db.Set(key, []byte("seed")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Update(key, func(old []byte) (treedb.UpdateResult, error) {
		if !bytes.Equal(old, []byte("seed")) {
			t.Fatalf("noop old = %q, want seed", old)
		}
		old[0] = 'X'
		return treedb.NoopUpdate(), nil
	}); err != nil {
		t.Fatalf("noop Update: %v", err)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get after noop: %v", err)
	}
	if !bytes.Equal(got, []byte("seed")) {
		t.Fatalf("noop changed value got=%q want seed", got)
	}

	if err := db.Update(key, func(old []byte) (treedb.UpdateResult, error) {
		if !bytes.Equal(old, []byte("seed")) {
			t.Fatalf("delete old = %q, want seed", old)
		}
		return treedb.DeleteUpdate(), nil
	}); err != nil {
		t.Fatalf("delete Update: %v", err)
	}
	got, err = db.Get(key)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("delete left value %q", got)
	}

	if err := db.Update(key, func(old []byte) (treedb.UpdateResult, error) {
		if old != nil {
			t.Fatalf("set old = %q, want nil", old)
		}
		return treedb.SetUpdate([]byte("restored")), nil
	}); err != nil {
		t.Fatalf("set Update: %v", err)
	}
	got, err = db.Get(key)
	if err != nil {
		t.Fatalf("Get after set: %v", err)
	}
	if !bytes.Equal(got, []byte("restored")) {
		t.Fatalf("set value got=%q want restored", got)
	}
}

func TestUpdatePreservesEmptyValuePresence(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("empty")
	if err := db.Set(key, []byte{}); err != nil {
		t.Fatalf("Set empty: %v", err)
	}
	if err := db.Update(key, func(old []byte) (treedb.UpdateResult, error) {
		if old == nil {
			t.Fatalf("old = nil, want non-nil empty slice for present empty value")
		}
		if len(old) != 0 {
			t.Fatalf("old len=%d, want 0", len(old))
		}
		return treedb.SetUpdate([]byte("present-empty")), nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("present-empty")) {
		t.Fatalf("value got=%q want present-empty", got)
	}
}

func TestUpdateCallbackCanReenterSameKeyWithoutDeadlock(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("reentrant")
	if err := db.Set(key, []byte("seed")); err != nil {
		t.Fatalf("Set seed: %v", err)
	}

	done := make(chan error, 1)
	calls := 0
	go func() {
		done <- db.Update(key, func(old []byte) (treedb.UpdateResult, error) {
			calls++
			switch string(old) {
			case "seed":
				if err := db.Update(key, func(innerOld []byte) (treedb.UpdateResult, error) {
					if !bytes.Equal(innerOld, []byte("seed")) {
						return treedb.UpdateResult{}, fmt.Errorf("inner old = %q, want seed", innerOld)
					}
					return treedb.SetUpdate([]byte("inner")), nil
				}); err != nil {
					return treedb.UpdateResult{}, err
				}
				return treedb.NoopUpdate(), nil
			case "inner":
				return treedb.SetUpdate([]byte("outer")), nil
			default:
				return treedb.UpdateResult{}, fmt.Errorf("outer old = %q, want seed or inner", old)
			}
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Update deadlocked during reentrant same-key callback")
	}

	if calls != 2 {
		t.Fatalf("outer callback calls=%d want 2", calls)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("outer")) {
		t.Fatalf("value got=%q want outer", got)
	}
}

func TestUpdateSyncPointerValuePersistsAfterCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	key := []byte("large")
	value := bytes.Repeat([]byte("value-log-update-"), 256)
	if err := db.UpdateSync(key, func(old []byte) (treedb.UpdateResult, error) {
		if old != nil {
			t.Fatalf("initial old = %q, want nil", old)
		}
		return treedb.SetUpdate(value), nil
	}); err != nil {
		t.Fatalf("UpdateSync: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("reopen value mismatch got_len=%d want_len=%d", len(got), len(value))
	}
}
