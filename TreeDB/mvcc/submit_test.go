package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func openSubmitDB(t *testing.T) (*treedb.DB, treedb.Options) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.MemtableShards = 4
	opts.FlushThreshold = 1 << 30
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return db, opts
}

// Submit must install each aggregate while the predecessor's exact durability
// target is blocked. Neither progress nor Close may depend on observing A.
func TestSubmitCommitGroupAtOwnedProgressAndExactACK(t *testing.T) {
	db, opts := openSubmitDB(t)
	defer db.Close()
	s := New(db)
	entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	unblock := func(i int) { once[i].Do(func() { close(release[i]) }) }
	defer func() { unblock(0); unblock(1) }()
	var syncs atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			i := int(syncs.Add(1)) - 1
			if i < 2 {
				close(entered[i])
				<-release[i]
			}
		}
		return nil
	})
	defer restore()
	submit := func(ts uint64) *treedb.CommitFuture {
		t.Helper()
		f, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: ts, Mutations: []Mutation{{Key: []byte("k"), Value: []byte{byte(ts)}}, {Key: []byte("pair"), Value: []byte{byte(ts)}}}}}, CommitDurable)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	a := submit(1)
	select {
	case <-entered[0]:
	case <-time.After(3 * time.Second):
		t.Fatal("owned worker did not start sync without Wait")
	}
	b, c := submit(2), submit(3)
	for _, f := range []*treedb.CommitFuture{a, b, c} {
		select {
		case <-f.Done():
			t.Fatal("ACK before durability")
		default:
		}
	}
	for _, key := range [][]byte{[]byte("k"), []byte("pair")} {
		got, err := s.GetAt(key, 3)
		if err != nil || !bytes.Equal(got.Value, []byte{3}) {
			t.Fatalf("successor aggregate not installed: %+v %v", got, err)
		}
	}
	unblock(0)
	select {
	case <-entered[1]:
	case <-time.After(3 * time.Second):
		t.Fatal("successor epoch did not progress")
	}
	select {
	case <-a.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("A completion waited for successor")
	}
	select {
	case <-b.Done():
		t.Fatal("B covered by predecessor's sync")
	default:
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("physical syncs before Close=%d, want2", got)
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close bypassed accepted work: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock(1)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not drain abandoned futures")
	}
	for _, f := range []*treedb.CommitFuture{b, c} {
		if err := f.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := New(reopened).GetAt([]byte("k"), 3)
	if err != nil || !bytes.Equal(got.Value, []byte{3}) {
		t.Fatalf("reopen=%+v %v", got, err)
	}
}

func TestSubmitCommitGroupAtFailurePoisonsAllOwnedWork(t *testing.T) {
	db, _ := openSubmitDB(t)
	defer db.Close()
	s := New(db)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	injected := errors.New("submit ambiguous physical sync")
	var calls atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && calls.Add(1) == 1 {
			close(entered)
			<-release
			return injected
		}
		return nil
	})
	defer restore()
	var futures []*treedb.CommitFuture
	for i := 1; i <= 3; i++ {
		f, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: uint64(i), Mutations: []Mutation{{Key: []byte("k"), Value: []byte{byte(i)}}}}}, CommitDurable)
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, f)
		if i == 1 {
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("sync not started")
			}
		}
	}
	unblock()
	for _, f := range futures {
		select {
		case <-f.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("poison did not drain queued future")
		}
		if err := f.Wait(); !errors.Is(err, injected) || !errors.Is(err, ErrStorage) {
			t.Fatalf("future lost error classification: %v", err)
		}
		if f.Wait() != f.Wait() {
			t.Fatal("future result changed")
		}
	}
	if _, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 4, Mutations: []Mutation{{Key: []byte("later")}}}}, CommitDurable); err == nil {
		t.Fatal("poison admitted later submission")
	}
}

func TestSubmitCommitGroupAtFloorWriterCannotBlockCompletion(t *testing.T) {
	db, _ := openSubmitDB(t)
	defer db.Close()
	s := New(db)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var syncs atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	defer restore()
	f, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 3, Mutations: []Mutation{{Key: []byte("k"), Value: []byte("three")}}}}, CommitDurable)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("sync did not start")
	}
	floorDone := make(chan error, 1)
	go func() { floorDone <- s.AdvanceDiscardFloor(2, CommitDurable) }()
	// Wait until a queued floor writer excludes additional readers. This uses
	// the real floor mutex; no completion hook grants artificial progress.
	deadline := time.Now().Add(3 * time.Second)
	for s.mu.TryRLock() {
		s.mu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("floor writer did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-floorDone:
		t.Fatalf("floor bypassed lease: %v", err)
	default:
	}
	unblock()
	select {
	case <-f.Done():
		if err := f.Wait(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("floor writer blocked independent completer")
	}
	select {
	case err := <-floorDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("floor lease not automatically released")
	}
}

func TestSubmitCommitGroupAtSaturationCloseAndOwnership(t *testing.T) {
	db, _ := openSubmitDB(t)
	defer db.Close()
	s := New(db)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var syncs atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	defer restore()
	var futures []*treedb.CommitFuture
	for i := 1; i <= 64; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		f, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: uint64(i), Mutations: []Mutation{{Key: []byte("k"), Value: value}}}}, CommitDurable)
		if err != nil {
			t.Fatal(err)
		}
		// Submit has transferred bytes, not a caller-owned backing array.
		for n := range value {
			value[n] = 'x'
		}
		futures = append(futures, f)
		if i == 1 {
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("sync did not start")
			}
		}
	}
	got, err := s.GetAt([]byte("k"), 64)
	if err != nil || !bytes.Equal(got.Value, []byte("value-64")) {
		t.Fatalf("caller reuse changed accepted bytes: %+v %v", got, err)
	}
	saturated := make(chan error, 1)
	go func() {
		_, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 65, Mutations: []Mutation{{Key: []byte("k"), Value: []byte("unaccepted")}}}}, CommitDurable)
		saturated <- err
	}()
	select {
	case err := <-saturated:
		t.Fatalf("capacity did not bound accepted work: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-saturated:
		if !errors.Is(err, treedb.ErrClosed) {
			t.Fatalf("blocked submission not rejected on Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close waited on saturated lifecycle admission")
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close failed to drain accepted work")
	}
	for _, f := range futures {
		if err := f.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSubmitCommitGroupAtCompatibilityAndValidation(t *testing.T) {
	for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
		db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
		s := New(db)
		f, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 1, Mutations: []Mutation{{Key: []byte("k"), Value: []byte("v")}}}}, mode)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-f.Done():
		default:
			t.Fatal("legacy fallback unexpectedly asynchronous")
		}
		if err := f.Wait(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: []Mutation{{Key: nil}, {Key: []byte{}}}}}, mode); !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("duplicate validation changed: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var nilStore *Store
	if _, err := nilStore.SubmitCommitGroupAt(nil, CommitMode(99)); !errors.Is(err, ErrInvalidCommitMode) {
		t.Fatal(err)
	}
	if _, err := nilStore.SubmitCommitGroupAt(nil, CommitDurable); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
}
