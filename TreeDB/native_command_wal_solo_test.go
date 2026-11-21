package treedb

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeCommandWALSoloDurableUsesCapturedSync(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	nextLSN := database.backend.CommandWALNextLSN()
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil }); err != nil {
		t.Fatal(err)
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total", 0)
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("singleton assigned %d frames, want one canonical command", got-nextLSN)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("durable announcements after ACK=%d", got)
	}
}

func TestNativeCommandWALDurableGroupOwnsBarrierAtCapacity(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxSources = 4
	opts.COWMemtableLimits.MaxGenerations = 8
	openRows := func() (*DB, *collections.CollectionManager) {
		t.Helper()
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
		return database, manager
	}
	database, manager := openRows()
	t.Logf("command WAL container=%s", database.Stats()["treedb.command_wal.writer.container"])
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	prefix, _, err := database.backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	initial := prefix.LogicalFrontiers()
	if initial.C != initial.W || database.backend.CommandWALNextLSN() != initial.C+1 || prefix.AcceptedPendingCount() != 0 {
		t.Fatalf("fixture requires an empty stable opening prefix, got %+v", initial)
	}
	var blockers []rootpublication.AcceptedTicket
	cancel := func(ticket rootpublication.AcceptedTicket) {
		t.Helper()
		if err := ticket.CancelAccepted(); err != nil {
			t.Errorf("release private slot: %v", err)
		}
	}
	releaseBlockers := func() {
		for _, ticket := range blockers {
			cancel(ticket)
		}
		blockers = nil
	}
	defer releaseBlockers()
	capacity := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
	for i := 0; i < capacity-2; i++ {
		ticket, err := prefix.ReserveAccepted(false)
		if err != nil {
			t.Fatalf("hold ordinary slot %d/%d: %v", i+1, capacity, err)
		}
		blockers = append(blockers, ticket)
	}

	group := &database.commandWALGroupCommit
	group.testBeforeSync = func(int) {} // Make each native command use queued capture.
	reachedCompletion := make(chan struct{})
	resumeCompletion := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopCompletion := func() { stopOnce.Do(func() { close(stop) }) }
	var joined []<-chan struct{}
	defer func() {
		stopCompletion()
		for _, done := range joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("native write did not join after completion release")
			}
		}
	}()
	group.testBeforeNativeComplete = func() {
		select {
		case reachedCompletion <- struct{}{}:
		case <-stop:
			return
		}
		select {
		case <-resumeCompletion:
		case <-stop:
		}
	}
	firstID, secondID := []byte("first"), []byte("second")
	firstDoc := nativeBSONDocument(t, bson.D{{Key: "_id", Value: "first"}, {Key: "n", Value: int64(1)}})
	secondDoc := nativeBSONDocument(t, bson.D{{Key: "_id", Value: "second"}, {Key: "n", Value: int64(2)}})
	start := func(id, doc []byte) <-chan error {
		t.Helper()
		done := make(chan error, 1)
		exited := make(chan struct{})
		joined = append(joined, exited)
		go func() {
			defer close(exited)
			_, err := rows.InsertBatchValidatedBSON([][]byte{id}, [][]byte{doc})
			done <- err
		}()
		select {
		case <-reachedCompletion:
		case err := <-done:
			t.Fatalf("%s returned before installed completion: %v", id, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not reach installed completion", id)
		}
		return done
	}
	finish := func(done <-chan error) error {
		t.Helper()
		resumeCompletion <- struct{}{}
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("group did not finish")
			return nil
		}
	}
	requireRow := func(rows *collections.Collection, id, want []byte) {
		t.Helper()
		got, err := rows.Get(id)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s = %v, %v, want %v", id, bson.Raw(got), err, bson.Raw(want))
		}
	}
	requireAssigned := func(opening rootpublication.LogicalFrontiers, id, doc []byte) rootpublication.LogicalFrontiers {
		t.Helper()
		frontiers := prefix.LogicalFrontiers()
		if frontiers.C != opening.C+1 || frontiers.V != opening.V+1 || frontiers.W != opening.W ||
			database.backend.CommandWALNextLSN() != frontiers.C+1 {
			t.Fatalf("%s must install once before ACK: opening=%+v assigned=%+v next=%d", id, opening, frontiers, database.backend.CommandWALNextLSN())
		}
		requireRow(rows, id, doc)
		return frontiers
	}
	requireBarrier := func(assigned rootpublication.LogicalFrontiers) rootpublication.LogicalFrontiers {
		t.Helper()
		stable := prefix.LogicalFrontiers()
		if stable.C != assigned.C+1 || stable.V != assigned.V || stable.W != stable.C ||
			database.backend.CommandWALNextLSN() != stable.C+1 {
			t.Fatalf("ACK must own one stable root-neutral barrier: assigned=%+v ack=%+v next=%d", assigned, stable, database.backend.CommandWALNextLSN())
		}
		return stable
	}

	first := start(firstID, firstDoc)
	firstAssigned := requireAssigned(initial, firstID, firstDoc)
	lastOrdinary, err := prefix.ReserveAccepted(false)
	if err != nil && !errors.Is(err, rootpublication.ErrAcceptedCapacity) {
		t.Fatalf("claim last ordinary slot during first capture: %v", err)
	}
	if lastOrdinary.Valid() {
		defer func() { cancel(lastOrdinary) }()
	}
	if err := finish(first); err != nil {
		t.Fatalf("first ACK: %v", err)
	}
	firstStable := requireBarrier(firstAssigned)
	if lastOrdinary.Valid() {
		// Stock finds its only maintenance slot at capture. It remains in
		// the accepted FIFO despite W now covering the actual barrier.
		maintenance, err := prefix.ReserveAccepted(true)
		if maintenance.Valid() {
			cancel(maintenance)
		}
		if !errors.Is(err, rootpublication.ErrAcceptedCapacity) {
			t.Fatalf("first acknowledged barrier did not retain maintenance: %v", err)
		}
		cancel(lastOrdinary)
		lastOrdinary = rootpublication.AcceptedTicket{}
		t.Logf("first barrier retained maintenance with stable W=%d", firstStable.W)
	} else {
		t.Logf("first group protected the last ordinary slot, stable W=%d", firstStable.W)
	}
	held, err := rows.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	requireHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{firstID, secondID}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 2 || !got.Results[0].Found ||
			!bytes.Equal(got.Results[0].Document, firstDoc) || got.Results[1].Found {
			t.Fatalf("held cut must retain only first document: %+v, %v", got, err)
		}
	}
	requireHeld()
	second := start(secondID, secondDoc)
	secondAssigned := requireAssigned(firstStable, secondID, secondDoc)
	if err := finish(second); err != nil {
		stalled := prefix.LogicalFrontiers()
		next := database.backend.CommandWALNextLSN()
		requireRow(rows, secondID, secondDoc)
		requireHeld()
		stopCompletion()
		thirdID := []byte("should-not-assign")
		thirdDoc := nativeBSONDocument(t, bson.D{{Key: "_id", Value: string(thirdID)}, {Key: "n", Value: int64(3)}})
		_, subsequent := rows.InsertBatchValidatedBSON([][]byte{thirdID}, [][]byte{thirdDoc})
		continuedNext := database.backend.CommandWALNextLSN()
		releaseBlockers()
		_ = held.Close()
		_ = manager.Close()
		closeErr := database.Close()
		t.Fatalf("second ACK failed after installation: err=%v (ambiguous=%t capacity=%t), assigned=%+v stalled=%+v next=%d, continued=%v (recovery=%t next=%d), close=%v",
			err, errors.Is(err, collections.ErrCommitAmbiguous), errors.Is(err, rootpublication.ErrAcceptedCapacity),
			secondAssigned, stalled, next, subsequent, errors.Is(subsequent, ErrRecoveryRequired) || errors.Is(subsequent, collections.ErrRecoveryRequired), continuedNext, closeErr)
	}
	requireBarrier(secondAssigned)
	requireHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	requireHeld()
	requireRow(rows, firstID, firstDoc)
	requireRow(rows, secondID, secondDoc)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	releaseBlockers()
	if count := prefix.AcceptedPendingCount(); count != 0 {
		t.Fatalf("checkpoint retained %d accepted entries", count)
	}
	closeRows := func(database *DB, manager *collections.CollectionManager) {
		t.Helper()
		cache := database.cached
		pins := database.backend.ValueLogIdentityPinRegistry()
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		requirePublicCOWFinalZero(t, cache)
		if stats := pins.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("close retained value-log pins: %+v", stats)
		}
	}
	closeRows(database, manager)
	reopened, reopenedManager := openRows()
	reopenedRows, err := reopenedManager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	requireRow(reopenedRows, firstID, firstDoc)
	requireRow(reopenedRows, secondID, secondDoc)
	closeRows(reopened, reopenedManager)
}

func TestPublicCOWDurableGroupAfterRelaxedReplay(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALRelaxed, t.TempDir())
	source, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if err := source.Set([]byte("replayed"), []byte("relaxed")); err != nil {
		t.Fatal(err)
	}
	if got := source.backend.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("source checkpointed before the crash image: applied=%d", got)
	}
	image := filepath.Join(t.TempDir(), "relaxed-replay")
	if err := copyPublicCommandWALCrashImage(opts.Dir, image); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Dir = image
	recovered, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	requireRawKVValue(t, recovered, []byte("replayed"), []byte("relaxed"))
	prefix, _, err := recovered.backend.PrivateCOWAcceptedPrefix(recovered.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	opening := prefix.LogicalFrontiers()
	if opening.C != 1 || opening.W != 0 {
		t.Fatalf("fixture requires an opening recovered LSN ahead of W: %+v", opening)
	}
	if err := recovered.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := prefix.LogicalFrontiers(); got.C != opening.C || got.W != opening.W {
		t.Fatalf("same-session checkpoint gave unexpected WAL credit: %+v", got)
	}
	recovered.commandWALGroupCommit.testBeforeSync = func(int) {}
	t.Logf("command WAL container=%s, recovered opening=%+v", recovered.Stats()["treedb.command_wal.writer.container"], opening)
	if err := recovered.SetSync([]byte("synced"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	if got := prefix.LogicalFrontiers(); got.C != opening.C+2 || got.V != opening.V+1 || got.W != got.C ||
		recovered.backend.CommandWALNextLSN() != got.C+1 {
		t.Fatalf("public raw sync must prove its own barrier after relaxed replay: %+v", got)
	}
	requireRawKVValue(t, recovered, []byte("synced"), []byte("durable"))
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicCOWDurableGroupBarrierRefusalAndRefund(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxSources = 4
	opts.COWMemtableLimits.MaxGenerations = 8
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	prefix, _, err := database.backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	var blockers []rootpublication.AcceptedTicket
	cancel := func(ticket rootpublication.AcceptedTicket) {
		t.Helper()
		if err := ticket.CancelAccepted(); err != nil {
			t.Error(err)
		}
	}
	defer func() {
		for _, ticket := range blockers {
			cancel(ticket)
		}
	}()
	capacity := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
	for i := 0; i < capacity-1; i++ {
		ticket, err := prefix.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		blockers = append(blockers, ticket)
	}
	assertLastFree := func() {
		t.Helper()
		ticket, err := prefix.ReserveAccepted(false)
		if err != nil {
			t.Fatalf("last ordinary slot did not return: %v", err)
		}
		cancel(ticket)
	}

	next := database.backend.CommandWALNextLSN()
	opening := prefix.LogicalFrontiers()
	key, value := []byte("raw"), []byte("first value")
	if err := database.SetSync(key, value); !errors.Is(err, memtable.ErrCOWCapacity) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("last ordinary slot must refuse before assignment: %v", err)
	}
	if got := prefix.LogicalFrontiers(); got != opening || database.backend.CommandWALNextLSN() != next {
		t.Fatalf("preassignment refusal changed prefix: opening=%+v got=%+v", opening, got)
	}
	if got, err := database.Get(key); err != nil || got != nil {
		t.Fatalf("preassignment refusal exposed the raw value: %q, %v", got, err)
	}
	assertLastFree()

	probeError := errors.New("unassigned original native callback refused")
	for _, panicAppend := range []bool{false, true} {
		func() {
			completion, err := database.PrepareNativeCommandWAL(1, true)
			if err != nil {
				t.Fatal(err)
			}
			defer completion.ReleaseNativeCommandWAL()
			if panicAppend {
				defer func() {
					if got := recover(); got != probeError {
						t.Errorf("callback panic=%v", got)
					}
				}()
			}
			err = completion.RegisterNativeCommandWAL(func(bool) (uint64, error) {
				ticket, err := prefix.ReserveAccepted(false)
				if ticket.Valid() {
					cancel(ticket)
				}
				if !errors.Is(err, rootpublication.ErrAcceptedCapacity) {
					t.Errorf("original callback started without custody of the remaining slot: %v", err)
				}
				if panicAppend {
					panic(probeError)
				}
				return 0, probeError
			})
			if err != probeError {
				t.Errorf("callback refusal=%v", err)
			}
		}()
		assertLastFree()
	}
	cancel(blockers[len(blockers)-1])
	blockers = blockers[:len(blockers)-1]
	if err := database.SetSync(key, value); err != nil {
		t.Fatal(err)
	}
	if got := prefix.LogicalFrontiers(); got.C != next || got.W != next || got.V != opening.V+1 ||
		database.backend.CommandWALNextLSN() != next+1 {
		t.Fatalf("idle raw admission must preserve a one-frame durable ACK: %+v", got)
	}
	assertLastFree()
	requireRawKVValue(t, database, key, value)
}

func TestPublicCOWDurableGroupOwnsBarrierForRelaxedTail(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALRelaxed, t.TempDir())
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxSources = 4
	opts.COWMemtableLimits.MaxGenerations = 8
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	prefix, _, err := database.backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	opening := prefix.LogicalFrontiers()
	capacity := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
	for range capacity - 3 {
		ticket, err := prefix.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := ticket.CancelAccepted(); err != nil {
				t.Error(err)
			}
		}()
	}
	value := []byte("value")
	submit := func(key string, durable bool) *commandWALPublicBatch {
		t.Helper()
		batch := database.NewBatch().(*commandWALPublicBatch)
		t.Cleanup(func() { _ = batch.Close() })
		if err := batch.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
		if err := batch.submitCOWWrite(durable); err != nil {
			t.Fatal(err)
		}
		return batch
	}
	first := submit("marker", true)
	group := &database.commandWALGroupCommit
	group.mu.Lock()
	singleton := group.pendingSingleton
	group.mu.Unlock()
	if singleton == 0 || singleton != first.cowState.registration.ticket {
		t.Fatal("first write did not use the idle captured-marker path")
	}
	tail := submit("relaxed-tail", false)
	if before := prefix.LogicalFrontiers(); before.C != opening.C+2 || before.W != opening.W {
		t.Fatalf("relaxed tail must install before capture of the initial marker: %+v", before)
	}
	// The two raw publications coalesce after installation. Hold the tail's
	// returned slot to verify the barrier's original reservation separately.
	coalescedSlot, err := prefix.ReserveAccepted(false)
	if err != nil {
		t.Fatalf("installed raw tail did not return its coalesced slot: %v", err)
	}
	defer func() {
		if err := coalescedSlot.CancelAccepted(); err != nil {
			t.Error(err)
		}
	}()
	ticket, err := prefix.ReserveAccepted(false)
	if ticket.Valid() {
		_ = ticket.CancelAccepted()
	}
	if !errors.Is(err, rootpublication.ErrAcceptedCapacity) {
		t.Fatalf("another producer could take the authenticated barrier slot: %v", err)
	}
	if err := first.completeCOWWrite(true, nil); err != nil {
		t.Fatal(err)
	}
	if err := tail.completeCOWWrite(false, nil); err != nil {
		t.Fatal(err)
	}
	if got := prefix.LogicalFrontiers(); got.C != opening.C+3 || got.W != got.C || got.V != opening.V+2 {
		t.Fatalf("captured endpoint missed its trailing relaxed participant or barrier: %+v", got)
	}
	requireRawKVValue(t, database, []byte("marker"), value)
	requireRawKVValue(t, database, []byte("relaxed-tail"), value)
}

// No group configuration or eligibility-disabling observer is changed here.
// The completion observer holds callers only after their producer/raw gates end.
func TestNativeCommandWALAnnouncesBeforeProducerSerialization(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	seed, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
	if _, err := c.InsertBatch([][]byte{[]byte("c"), []byte("d")}, [][]byte{seed, seed}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := database.Stats()
	admitted, completed := make(chan struct{}, 4), make(chan struct{}, 4)
	releaseAdmission, releaseCompletion := make(chan struct{}), make(chan struct{})
	var admissionOnce, completionOnce sync.Once
	defer admissionOnce.Do(func() { close(releaseAdmission) })
	defer completionOnce.Do(func() { close(releaseCompletion) })
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admitted <- struct{}{}
			<-releaseAdmission
		}
		return nil
	})
	defer restore()
	database.commandWALGroupCommit.testBeforeNativeComplete = func() { completed <- struct{}{}; <-releaseCompletion }
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	errs := make(chan error, 4)
	for _, id := range []string{"a", "b", "c", "d"} {
		go func(id string) {
			_, _, err := c.Update([]byte(id), func([]byte) ([]byte, bool, error) { return doc, true, nil })
			errs <- err
		}(id)
	}
	for range 4 {
		select {
		case <-admitted:
		case <-time.After(5 * time.Second):
			t.Fatal("native requests did not reach admission")
		}
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 4 {
		t.Fatalf("announcements before producer=%d want4", got)
	}
	admissionOnce.Do(func() { close(releaseAdmission) })
	for range 4 {
		select {
		case <-completed:
		case <-time.After(5 * time.Second):
			t.Fatal("native requests did not release producer gates")
		}
	}
	group := &database.commandWALGroupCommit
	group.mu.Lock()
	pending := group.pendingCount
	group.mu.Unlock()
	if pending != 4 || group.durableIntents.Load() != 0 {
		t.Fatalf("pending=%d announcements=%d", pending, group.durableIntents.Load())
	}
	requirePublicStatDelta(t, before, database.Stats(), "treedb.command_wal.file_sync.calls_total", 0)
	completionOnce.Do(func() { close(releaseCompletion) })
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total", 0)
}

func TestNativeCommandWALAnnouncementReleasedBeforeAssignment(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	nextLSN := database.backend.CommandWALNextLSN()
	failure := errors.New("native admission refused")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			return failure
		}
		return nil
	})
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
	restore()
	if !errors.Is(err, failure) || errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("preassignment error=%v", err)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 || database.backend.CommandWALNextLSN() != nextLSN {
		t.Fatalf("refusal announcements=%d next=%d want%d", got, database.backend.CommandWALNextLSN(), nextLSN)
	}
	// A callback panic exercises the coordinator's deferred gates and the same
	// completion's withdrawal, without claiming native producer panic recovery.
	completion, err := database.PrepareNativeCommandWAL(1, true)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer completion.ReleaseNativeCommandWAL()
		defer func() {
			if recover() == nil {
				t.Error("append did not panic")
			}
		}()
		_ = completion.RegisterNativeCommandWAL(func(bool) (uint64, error) { panic("append panic") })
	}()
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("panic retained announcements=%d", got)
	}
	if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil }); err != nil {
		t.Fatalf("write after unassigned refusal/panic=%v", err)
	}
}

// The captured singleton installs before its final fence. A failed fence owns
// the original assigned identity and installed but unacknowledged mutation.
func TestNativeCommandWALSoloAssignedSyncFailureRetainsIdentity(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	nextLSN := database.backend.CommandWALNextLSN()
	failure := errors.New("native solo command sync failed")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			return failure
		}
		return nil
	})
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
	restore()
	if !errors.Is(err, failure) || !errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("assigned sync failure=%v", err)
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("assigned next=%d want%d", got, nextLSN+1)
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("ambiguous installed row=%x error=%v", got, err)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("durable announcements after failure=%d", got)
	}
	if got := statMapUint64(t, database.Stats(), "treedb.command_wal.durable_wal_lsn"); got != statMapUint64(t, before, "treedb.command_wal.durable_wal_lsn") {
		t.Fatalf("failed final fence advanced durable frontier=%d", got)
	}
	if _, _, err := c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { t.Error("callback after assigned failure"); return doc, true, nil }); err == nil {
		t.Fatal("write after assigned failure succeeded")
	}
}

// No collector policy is changed: a collector override would disable the
// first singleton's eligibility and silently exercise the queued fallback.
func TestNativeCommandWALSoloCapturedFenceReleasesGatesBeforeACK(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	group := &database.commandWALGroupCommit
	group.mu.Lock()
	beforeTimer := group.timer
	group.mu.Unlock()
	nextLSN := database.backend.CommandWALNextLSN()
	entered, release := make(chan struct{}), make(chan struct{})
	successorAppended := make(chan struct{})
	successorInstalled := make(chan struct{})
	var installed atomic.Int32
	group.testBeforeNativeComplete = func() {
		if installed.Add(1) == 2 {
			close(successorInstalled)
		}
	}
	var unblockOnce, holdOnce, successorOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(release) }) }
	first, second := make(chan error, 1), make(chan error, 1)
	firstJoined, secondJoined := make(chan struct{}), make(chan struct{})
	secondStarted := false
	t.Cleanup(func() {
		unblock()
		joins := []<-chan struct{}{firstJoined}
		if secondStarted {
			joins = append(joins, secondJoined)
		}
		for _, joined := range joins {
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("native completion did not join after fence release")
			}
		}
	})
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.LSN == nextLSN+1 {
			successorOnce.Do(func() { close(successorAppended) })
		}
		if e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN == nextLSN {
			if e.StablePath == nil {
				return errors.New("captured native fence has no retained file")
			}
			holdOnce.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	go func() {
		defer close(firstJoined)
		_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
		first <- err
	}()
	select {
	case <-entered:
	case err := <-first:
		t.Fatalf("native ACK preceded captured fence: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("singleton did not reach its actual captured fence")
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("singleton prefix next=%d want%d", got, nextLSN+1)
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("captured fence preceded complete native installation: %x/%v", got, err)
	}
	if got := statMapUint64(t, database.Stats(), "treedb.command_wal.durable_wal_lsn"); got != statMapUint64(t, before, "treedb.command_wal.durable_wal_lsn") {
		t.Fatalf("held final fence advanced durable frontier=%d", got)
	}
	if !database.commandWALPublicOperationGate.TryLock() {
		t.Fatal("captured sync retained public operation gate")
	}
	database.commandWALPublicOperationGate.Unlock()
	group.mu.Lock()
	timer := group.timer
	pendingBarrier := group.pendingBarrier.Valid()
	group.mu.Unlock()
	if timer != beforeTimer || pendingBarrier {
		t.Fatal("idle native singleton left a timer or pending barrier during its file fence")
	}
	requirePublicStatDelta(t, before, database.Stats(), "treedb.command_wal.group_commit.trigger.timeout_total", 0)
	secondStarted = true
	go func() {
		defer close(secondJoined)
		_, _, err := c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
		second <- err
	}()
	select {
	case <-successorAppended: // real assignment requires BOTH producer and raw gates
	case err := <-second:
		t.Fatalf("successor returned before assigning under held sync: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("captured sync retained producer or raw publisher gate")
	}
	select {
	case <-successorInstalled:
	case <-time.After(5 * time.Second):
		t.Fatal("successor did not install while the previous captured file fence was held")
	}
	group.mu.Lock()
	pendingBarrier = group.pendingBarrier.Valid()
	pending := group.pendingCommits
	leaderActive := group.leaderActive
	group.mu.Unlock()
	if !leaderActive || pending != 1 || !pendingBarrier {
		t.Fatalf("successor epoch lost its own barrier during prior sync: leader=%t commits=%d reserved=%t", leaderActive, pending, pendingBarrier)
	}
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			t.Fatalf("ACK overtook captured fence: %v", err)
		default:
		}
	}
	unblock()
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("native ACK did not complete after physical fence")
		}
	}
}
