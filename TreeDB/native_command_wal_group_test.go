package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeCommandWALGroupIndependentBSONUpdates(t *testing.T) {
	for _, sameCollection := range []bool{false, true} {
		t.Run(fmt.Sprintf("sameCollection=%t", sameCollection), func(t *testing.T) { testNativeCommandWALGroupBSONUpdates(t, sameCollection) })
	}
}

func testNativeCommandWALGroupBSONUpdates(t *testing.T, sameCollection bool) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := os.Getenv("GOMAP_NATIVE_SHARED_GROUP_CRASH_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	defer manager.Close()
	const waiters = 4
	rows := make([]*collections.Collection, waiters)
	seed, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
	next, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	for i := range rows {
		name := fmt.Sprintf("rows%d", i)
		if sameCollection && i > 0 {
			rows[i] = rows[0]
			continue
		}
		if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "n", Field: "n", ValueType: collections.IndexValueBSONOrderedV2}}}); err != nil {
			t.Fatal(err)
		}
		rows[i], err = manager.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		ids := [][]byte{[]byte("a")}
		docs := [][]byte{seed}
		if sameCollection {
			ids = [][]byte{[]byte("a0"), []byte("a1"), []byte("a2"), []byte("a3")}
			docs = [][]byte{seed, seed, seed, seed}
		}
		if _, err := rows[i].InsertBatch(ids, docs); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := database.Stats()
	beforeLSN := backend.CommandWALNextLSN()
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	beforeSequence := capture.LogicalSequence()
	capture.Close()
	database.commandWALGroupCommit.delay = time.Second
	database.commandWALGroupCommit.maxCommits = waiters
	database.commandWALGroupCommit.maxBytes = 1 << 30
	atSync := make(chan int, 1)
	release := make(chan struct{})
	defer close(release)
	database.commandWALGroupCommit.testBeforeSync = func(n int) { atSync <- n; <-release }
	errs := make(chan error, waiters)
	for i, c := range rows {
		go func(i int, c *collections.Collection) {
			id := []byte("a")
			if sameCollection {
				id = []byte(fmt.Sprintf("a%d", i))
			}
			_, _, err := c.Update(id, func([]byte) ([]byte, bool, error) { return next, true, nil })
			errs <- err
		}(i, c)
	}
	select {
	case n := <-atSync:
		if n != waiters {
			t.Fatalf("group size=%d want%d", n, waiters)
		}
	case err := <-errs:
		t.Fatalf("native ACK escaped shared durable barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("native group did not form")
	}
	if got := backend.CommandWALNextLSN(); got != beforeLSN+waiters {
		t.Fatalf("independent assigned prefix=%d want%d", got, beforeLSN+waiters)
	}
	capture, err = backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	sequence := capture.LogicalSequence()
	capture.Close()
	if sequence != beforeSequence+waiters {
		t.Fatalf("independent installations=%d want%d", sequence, beforeSequence+waiters)
	}
	select {
	case err := <-errs:
		t.Fatalf("ACK before sync: %v", err)
	default:
	}
	release <- struct{}{}
	for range waiters {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.commits_total", waiters)
	if os.Getenv("GOMAP_NATIVE_SHARED_GROUP_CRASH_DIR") != "" {
		fmt.Printf("NATIVE GROUP ACK barrier=%d applied=%d\n", backend.CommandWALNextLSN()-1, backend.State().AppliedCommandLSN)
		os.Exit(97)
	}
}

func nativeCommandWALGroupFixture(t *testing.T, rotate bool) (*DB, *collections.Collection) {
	t.Helper()
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	if rotate {
		opts.CommandWALSegmentTargetBytes = 1
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "n", Field: "n", ValueType: collections.IndexValueBSONOrderedV2}}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{doc, doc}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return database, c
}

func TestNativeCommandWALGroupFailurePoisonsInstalledWaiters(t *testing.T) {
	for _, point := range []string{"dependency", "barrier-assigned"} {
		t.Run(point, func(t *testing.T) {
			database, c := nativeCommandWALGroupFixture(t, true)
			database.commandWALGroupCommit.delay = time.Second
			database.commandWALGroupCommit.maxCommits = 2
			database.commandWALGroupCommit.maxBytes = 1 << 30
			before := database.Stats()
			beforeNext := database.backend.CommandWALNextLSN()
			cutErr := errors.New("native shared durable failure")
			var appends atomic.Int32
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				if point == "dependency" && event.Point == durabilitycut.BeforeDependencyFileSync {
					return cutErr
				}
				if point == "barrier-assigned" && event.Point == durabilitycut.AfterDependencyAppend && appends.Add(1) == 3 {
					return cutErr
				}
				return nil
			})
			doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
			errs := make(chan error, 2)
			var callbacks atomic.Int32
			for _, id := range []string{"a", "b"} {
				go func(id string) {
					_, _, err := c.Update([]byte(id), func([]byte) ([]byte, bool, error) { callbacks.Add(1); return doc, true, nil })
					errs <- err
				}(id)
			}
			for range 2 {
				if err := <-errs; !errors.Is(err, cutErr) || !errors.Is(err, collections.ErrCommitAmbiguous) {
					t.Errorf("waiter error=%v want%v", err, cutErr)
				}
			}
			restore()
			if callbacks.Load() != 2 {
				t.Fatalf("callbacks=%d want2", callbacks.Load())
			}
			wantNext := beforeNext + 2
			if point == "barrier-assigned" {
				wantNext++
			}
			if got := database.backend.CommandWALNextLSN(); got != wantNext {
				t.Fatalf("assigned next=%d want%d", got, wantNext)
			}
			if got := publicStatUint64(t, database, "treedb.command_wal.durable_wal_lsn"); got >= beforeNext {
				t.Fatalf("failed barrier advanced stable frontier=%d nextbefore=%d", got, beforeNext)
			}
			for _, id := range []string{"a", "b"} {
				got, err := c.Get([]byte(id))
				if err != nil || !bytes.Equal(got, doc) {
					t.Fatalf("installed %s=%x/%v", id, got, err)
				}
			}
			if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { t.Error("callback after poison"); return doc, true, nil }); err == nil {
				t.Fatal("mutation after poison succeeded")
			}
			requirePublicStatDelta(t, before, database.Stats(), "treedb.command_wal.group_commit.errors_total", 1)
		})
	}
}

func TestNativeCommandWALGroupSingletonAndMaintenance(t *testing.T) {
	for _, maintenance := range []string{"singleton", "checkpoint", "close"} {
		t.Run(maintenance, func(t *testing.T) {
			database, c := nativeCommandWALGroupFixture(t, false)
			if maintenance == "singleton" {
				before := database.Stats()
				doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
				done := make(chan error, 1)
				go func() {
					_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
					done <- err
				}()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("singleton group timeout")
				}
				requirePublicStatDelta(t, before, database.Stats(), "treedb.command_wal.file_sync.calls_total", 1)
				return
			}
			database.commandWALGroupCommit.delay = time.Second
			database.commandWALGroupCommit.maxCommits = 2
			atSync, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			database.commandWALGroupCommit.testBeforeSync = func(int) { close(atSync); <-release }
			writes := make(chan error, 2)
			doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
			for _, id := range []string{"a", "b"} {
				go func(id string) {
					_, _, err := c.Update([]byte(id), func([]byte) ([]byte, bool, error) { return doc, true, nil })
					writes <- err
				}(id)
			}
			select {
			case <-atSync:
			case <-time.After(3 * time.Second):
				t.Fatal("group not forming")
			}
			done := make(chan error, 1)
			go func() {
				if maintenance == "checkpoint" {
					done <- database.Checkpoint()
				} else {
					done <- database.Close()
				}
			}()
			select {
			case err := <-done:
				t.Fatalf("%s escaped forming group:%v", maintenance, err)
			case <-time.After(20 * time.Millisecond):
			}
			once.Do(func() { close(release) })
			for range 2 {
				select {
				case err := <-writes:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("write deadlocked")
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("maintenance deadlocked")
			}
		})
	}
}

func TestNativeCommandWALGroupCrashRecovery(t *testing.T) {
	if os.Getenv("GOMAP_NATIVE_SHARED_GROUP_CRASH_DIR") != "" {
		testNativeCommandWALGroupBSONUpdates(t, true)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeCommandWALGroupCrashRecovery$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_NATIVE_SHARED_GROUP_CRASH_DIR="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("crash child:%v\n%s", err, output)
	}
	var barrier, basis uint64
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "NATIVE GROUP ACK") {
			if _, err := fmt.Sscanf(line, "NATIVE GROUP ACK barrier=%d applied=%d", &barrier, &basis); err != nil {
				t.Fatal(err)
			}
		}
	}
	if barrier <= basis {
		t.Fatalf("child did not preserve unapplied acknowledged group: %s", output)
	}
	database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	defer manager.Close()
	c, err := manager.OpenCollection("rows0")
	if err != nil {
		t.Fatal(err)
	}
	next, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	ids, err := c.FindByIndexValue("n", bson.Raw(next).Lookup("n"))
	if err != nil || len(ids) != 4 {
		t.Fatalf("recovered exact index=%q/%v", ids, err)
	}
	for _, id := range ids {
		got, err := c.Get(id)
		if err != nil || !bytes.Equal(got, next) {
			t.Fatalf("recovered %s=%x/%v", id, got, err)
		}
	}
	if got := backend.State().AppliedCommandLSN; got != barrier {
		t.Fatalf("recovered applied=%d wantbarrier%d", got, barrier)
	}
}

func TestNativeCommandWALGroupAssignedInsertFailureIsAmbiguous(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	beforeNext := database.backend.CommandWALNextLSN()
	beforeSequence := database.Stats()["treedb.cache.cow.txn.logical_seq"]
	failure := errors.New("native insert assigned before installation")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend {
			return failure
		}
		return nil
	})
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(3)}})
	_, err := c.InsertBatch([][]byte{[]byte("assigned")}, [][]byte{doc})
	restore()
	if !errors.Is(err, failure) || !errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("assigned insert error=%v", err)
	}
	if database.backend.CommandWALNextLSN() != beforeNext+1 || database.Stats()["treedb.cache.cow.txn.logical_seq"] != beforeSequence {
		t.Fatal("assigned failure lost original identity or installed a logical successor")
	}
	if got, err := c.Get([]byte("assigned")); err != nil || got != nil {
		t.Fatalf("failed uninstalled insert=%x/%v", got, err)
	}
	if _, err := c.InsertBatch([][]byte{[]byte("retry")}, [][]byte{doc}); err == nil {
		t.Fatal("assigned failure left handle usable")
	}
}
