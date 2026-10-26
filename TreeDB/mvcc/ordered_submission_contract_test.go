package mvcc

import (
	"reflect"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// This source-identical control can be overlaid on the preceding blocking
// producer: reflection selects only the optional owned handoff, otherwise the
// established public blocking commit. No production authority is fabricated.
func TestPublicOrderedSubmissionHandoffContract(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("stable")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyFileSync {
			once.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	defer unblock()
	submitted := make(chan error, 1)
	completed := make(chan error, 1)
	go func() {
		groups := []CommitGroup{{Timestamp: 2, Mutations: []Mutation{{Key: []byte("next"), Value: []byte("owned")}}}}
		method := reflect.ValueOf(store).MethodByName("SubmitCommitGroupAt")
		if !method.IsValid() {
			err := store.CommitGroupAt(groups, CommitDurable)
			submitted <- err
			completed <- err
			return
		}
		record := reflect.New(method.Type().In(2).Elem())
		result := method.Call([]reflect.Value{reflect.ValueOf(groups), reflect.ValueOf(CommitDurable), record})
		var err error
		if !result[0].IsNil() {
			err = result[0].Interface().(error)
		}
		submitted <- err
		if err == nil {
			result = record.MethodByName("Complete").Call(nil)
			if !result[0].IsNil() {
				err = result[0].Interface().(error)
			}
		}
		completed <- err
	}()
	// Both routes reach the same physical dependency boundary. Submission must
	// already have returned while completion independently drives this barrier.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("did not reach captured-prefix sync")
	}
	select {
	case err := <-submitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		unblock()
		<-submitted
		<-completed
		t.Fatal("public producer cannot hand off installed command before physical acknowledgement")
	}
	select {
	case err := <-completed:
		t.Fatalf("acknowledged while physical sync held: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("completion did not progress")
	}
}
