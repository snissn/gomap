package collections

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Four public callers must retain four logical commands while sharing a
// dependency-closed durable prefix. This is a structural test, not a timing
// qualification or a substitute for the 64-request performance protocol.
func TestNativeStringPatchConcurrentDurablePrefix(t *testing.T) {
	dir, db, first := r1MutationOpen5059(t, true)
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	rows := make([]map[string]any, 4)
	for i := range rows {
		rows[i] = r1MutationRow5059(i)
	}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := first.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := first.Meta().Options.ColumnStore.SchemaHash
	held, err := first.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	original := make(map[string]map[string]any, 4)
	known := r1MutationKnown5059()
	for _, row := range rows {
		original[row["id"].(string)] = r1MutationCopy5059(row)
	}
	r1MutationRemember5059(known, rows...)
	var callers [4]*Collection
	callers[0] = first
	for i := 1; i < len(callers); i++ {
		callers[i], err = first.writeDomain.manager.OpenCollection("r1")
		if err != nil {
			t.Fatal(err)
		}
	}
	seal := nativeStringPatchHoldFormation(t, first)
	beforeSyncs := typedGroupStatUint64(t, db.Stats(), "treedb.command_wal.file_sync.calls_total")
	beforeFrames := len(collectionCommandWALFrames(t, dir))
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(len(callers))
	type outcome struct {
		i      int
		result TypedStringPatchResult
		err    error
	}
	done := make(chan outcome, len(callers))
	for i, col := range callers {
		go func(i int, col *Collection) {
			request := []TypedStringPatch{{ID: ids[i], Edits: []TypedStringEdit{{Column: "bio", Value: fmt.Sprintf("prefix bio %d", i)}, {Column: "city", Value: fmt.Sprintf("prefix city %d", i)}, {Column: "email", Value: fmt.Sprintf("prefix-%d@example.test", i)}}}}
			ready.Done()
			<-start
			result, err := col.PatchTypedStringsBatch(request, schema)
			done <- outcome{i, result, err}
		}(i, col)
	}
	ready.Wait()
	close(start)
	seal(4)
	for range callers {
		select {
		case got := <-done:
			if got.err != nil || got.result.MatchedCount != 1 || got.result.ModifiedCount != 1 {
				t.Fatalf("caller %d: %+v %v", got.i, got.result, got.err)
			}
			rows[got.i]["bio"] = fmt.Sprintf("prefix bio %d", got.i)
			rows[got.i]["city"] = fmt.Sprintf("prefix city %d", got.i)
			rows[got.i]["email"] = fmt.Sprintf("prefix-%d@example.test", got.i)
		case <-time.After(30 * time.Second):
			t.Fatal("native prefix callers did not drain")
		}
	}
	want := make(map[string]map[string]any, 4)
	for _, row := range rows {
		want[row["id"].(string)] = row
	}
	r1MutationRemember5059(known, rows...)
	r1MutationAssert5059(t, first, want, known)
	r1LifecycleAssert5060(t, held, original, known)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	syncs := typedGroupStatUint64(t, db.Stats(), "treedb.command_wal.file_sync.calls_total") - beforeSyncs
	var commands, barriers int
	for _, frame := range collectionCommandWALFrames(t, dir)[beforeFrames:] {
		if frame.Kind == commitlog.CommandKindCollectionUpdateBatchByID {
			commands++
		}
		if frame.Kind == commitlog.CommandKindDurablePrefixBarrier {
			barriers++
		}
	}
	if commands != 4 {
		t.Fatalf("individual native commands=%d want=4", commands)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	current, err := NewCollectionManager(reopened).OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	r1MutationAssert5059(t, current, want, known)
	if syncs > 2 || barriers == 0 {
		t.Fatalf("missing shared native durable prefix: physical WAL syncs=%d want<=2, prefix barriers=%d want>=1", syncs, barriers)
	}
}

// A union plan must not make two independently invalid requests valid by
// exchanging their unique owners across request boundaries.
func TestNativeStringPatchConcurrentUniqueRequestBoundaries(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	rows := []map[string]any{r1MutationRow5059(0), r1MutationRow5059(1)}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	seal := nativeStringPatchHoldFormation(t, col)
	before := db.State()
	beforeLSN := db.CommandWALNextLSN()
	start := make(chan struct{})
	done := make(chan error, 2)
	for i := range rows {
		go func(i int) {
			<-start
			_, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[i], Edits: []TypedStringEdit{{Column: "email", Value: rows[1-i]["email"].(string)}}}}, schema)
			done <- err
		}(i)
	}
	close(start)
	seal(2)
	for range rows {
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("cross-request unique swap accepted")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("conflicting native callers did not drain")
		}
	}
	if after := db.State(); after.CommitSeq != before.CommitSeq || after.SystemRootPageID != before.SystemRootPageID || db.CommandWALNextLSN() != beforeLSN {
		t.Fatal("invalid request changed authority or assigned a WAL identity")
	}
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, rows...)
	r1MutationAssert5059(t, col, map[string]map[string]any{string(ids[0]): rows[0], string(ids[1]): rows[1]}, known)
}

// An Expected revision failure is local to its own request. No successful
// disjoint participant may inherit another participant's pre-append failure.
func TestNativeStringPatchConcurrentRevisionFailureIsolation(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	rows := []map[string]any{r1MutationRow5059(0), r1MutationRow5059(1)}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	refs, err := view.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	view.Close()
	if err != nil {
		t.Fatal(err)
	}
	stale := refs.Results[0].RowRef
	stale.PartID++
	schema := col.Meta().Options.ColumnStore.SchemaHash
	seal := nativeStringPatchHoldFormation(t, col)
	start := make(chan struct{})
	type result struct {
		bad   bool
		value TypedStringPatchResult
		err   error
	}
	done := make(chan result, 2)
	go func() {
		<-start
		v, e := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Expected: &stale, Edits: []TypedStringEdit{{Column: "bio", Value: "must not install"}}}}, schema)
		done <- result{true, v, e}
	}()
	go func() {
		<-start
		v, e := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[1], Edits: []TypedStringEdit{{Column: "bio", Value: "valid independent request"}}}}, schema)
		done <- result{false, v, e}
	}()
	close(start)
	seal(2)
	for range 2 {
		select {
		case got := <-done:
			if got.bad {
				if !errors.Is(got.err, ErrTypedStringPatchConflict) {
					t.Fatalf("stale revision result %+v", got)
				}
			} else if got.err != nil || got.value.ModifiedCount != 1 {
				t.Fatalf("valid participant inherited another request's failure: %+v", got)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("revision-isolation callers did not drain")
		}
	}
	rows[1]["bio"] = "valid independent request"
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, rows...)
	r1MutationAssert5059(t, col, map[string]map[string]any{string(ids[0]): rows[0], string(ids[1]): rows[1]}, known)
}

func nativeStringPatchHoldFormation(t *testing.T, col *Collection) func(int) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	hook := func() { <-release }
	if !nativeStringPatchBeforeSealTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("native prefix hook already installed")
	}
	t.Cleanup(func() { once.Do(func() { close(release) }); nativeStringPatchBeforeSealTestHook.Store(nil) })
	return func(want int) {
		deadline := time.Now().Add(30 * time.Second)
		coord := col.writeDomain.commandWALCoordinatorForDomain(col.db)
		for time.Now().Before(deadline) {
			coord.mu.Lock()
			group := coord.nativePatchGroup
			count := 0
			if group != nil {
				count = group.count
			}
			coord.mu.Unlock()
			if count >= want {
				once.Do(func() { close(release) })
				return
			}
			time.Sleep(time.Millisecond)
		}
		once.Do(func() { close(release) })
		t.Fatal("native callers did not reach formation boundary")
	}
}

func TestNativeStringPatchFifthCallerWaitsBeforeInputCopy(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	rows := make([]map[string]any, 5)
	for i := range rows {
		rows[i] = r1MutationRow5059(i)
	}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	release := make(chan struct{})
	var once sync.Once
	hook := func() { <-release }
	if !nativeStringPatchBeforeSealTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("native hook already installed")
	}
	defer func() { once.Do(func() { close(release) }); nativeStringPatchBeforeSealTestHook.Store(nil) }()
	copied := make(chan struct{}, 5)
	copyHook := func() { copied <- struct{}{} }
	if !nativeStringPatchInputCopiedTestHook.CompareAndSwap(nil, &copyHook) {
		t.Fatal("native copy hook already installed")
	}
	defer nativeStringPatchInputCopiedTestHook.Store(nil)
	done := make(chan error, 5)
	for i := 0; i < 4; i++ {
		go func(i int) {
			_, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[i], Edits: []TypedStringEdit{{Column: "bio", Value: "bounded prefix"}}}}, schema)
			done <- err
		}(i)
	}
	for range 4 {
		select {
		case <-copied:
		case <-time.After(30 * time.Second):
			t.Fatal("four admitted inputs did not reach copy")
		}
	}
	entered := make(chan struct{})
	go func() {
		close(entered)
		_, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[4], Edits: []TypedStringEdit{{Column: "bio", Value: "after prefix"}}}}, schema)
		done <- err
	}()
	<-entered
	select {
	case <-copied:
		t.Fatal("fifth input copied before an admission slot was released")
	case <-time.After(10 * time.Millisecond):
	}
	coord := col.writeDomain.commandWALCoordinatorForDomain(db)
	coord.mu.Lock()
	admitted := coord.nativePatchAdmitted
	coord.mu.Unlock()
	if admitted != 4 {
		t.Fatalf("admitted requests=%d want=4", admitted)
	}
	once.Do(func() { close(release) })
	for range 5 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("bounded native callers did not drain")
		}
	}
	select {
	case <-copied:
	case <-time.After(30 * time.Second):
		t.Fatal("fifth caller failed to acquire released slot")
	}
	coord.mu.Lock()
	admitted = coord.nativePatchAdmitted
	coord.mu.Unlock()
	if admitted != 0 {
		t.Fatalf("native admission slots leaked: %d", admitted)
	}
}

func TestNativeStringPatchGroupNoopAndMissingResults(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	rows := []map[string]any{r1MutationRow5059(0), r1MutationRow5059(1)}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	seal := nativeStringPatchHoldFormation(t, col)
	requests := [][]TypedStringPatch{
		{{ID: ids[0], Edits: []TypedStringEdit{{Column: "bio", Value: rows[0]["bio"].(string)}}}},
		{{ID: []byte("missing-native-id"), Edits: []TypedStringEdit{{Column: "bio", Value: "ignored"}}}},
		{{ID: ids[1], Edits: []TypedStringEdit{{Column: "bio", Value: "only real change"}}}},
	}
	type outcome struct {
		i      int
		result TypedStringPatchResult
		err    error
	}
	done := make(chan outcome, len(requests))
	for i, input := range requests {
		go func(i int, input []TypedStringPatch) {
			r, e := col.PatchTypedStringsBatch(input, schema)
			done <- outcome{i, r, e}
		}(i, input)
	}
	seal(3)
	wants := [3]TypedStringPatchResult{{MatchedCount: 1}, {}, {MatchedCount: 1, ModifiedCount: 1}}
	for range requests {
		select {
		case got := <-done:
			if got.err != nil || got.result != wants[got.i] {
				t.Fatalf("participant %d: %+v %v", got.i, got.result, got.err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("noop native prefix did not drain")
		}
	}
}

func TestNativeStringPatchInvalidInputRetiresOrderedSlot(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	ids, retained, columns := r1MutationBatch5059(t, r1MutationRow5059(0))
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	input := []TypedStringPatch{
		{ID: []byte("duplicate"), Edits: []TypedStringEdit{{Column: "bio", Value: "first"}}},
		{ID: []byte("duplicate"), Edits: []TypedStringEdit{{Column: "bio", Value: "second"}}},
	}
	if _, err := col.PatchTypedStringsBatch(input, schema); !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("invalid owned input: %v", err)
	}
	coord := col.writeDomain.commandWALCoordinatorForDomain(db)
	coord.mu.Lock()
	admitted, join, ticket := coord.nativePatchAdmitted, coord.nativePatchNextJoin, coord.nativePatchNextTicket
	coord.mu.Unlock()
	if admitted != 0 || join != ticket {
		t.Fatalf("invalid input stranded FIFO position: admitted=%d join=%d ticket=%d", admitted, join, ticket)
	}
	if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: []byte("missing"), Edits: []TypedStringEdit{{Column: "bio", Value: "valid"}}}}, schema); err != nil {
		t.Fatal(err)
	}
}

func TestNativeStringPatchPreparationRevalidatesConcurrentRoot(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	rows := []map[string]any{r1MutationRow5059(0)}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	prepared := make(chan struct{})
	resume := make(chan struct{})
	var resumed sync.Once
	var paused atomic.Bool
	hook := func() {
		if paused.CompareAndSwap(false, true) {
			close(prepared)
			<-resume
		}
	}
	if !nativeStringPatchAfterPrepareTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("native prepare hook already installed")
	}
	defer func() { resumed.Do(func() { close(resume) }); nativeStringPatchAfterPrepareTestHook.Store(nil) }()
	done := make(chan error, 1)
	schema := col.Meta().Options.ColumnStore.SchemaHash
	go func() {
		_, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "bio", Value: "prepared native edit"}}}}, schema)
		done <- err
	}()
	select {
	case <-prepared:
	case <-time.After(30 * time.Second):
		t.Fatal("native input failed to prepare")
	}
	// This operation must finish while native preparation is paused. Its root
	// change must invalidate the earlier native cut before WAL identity assignment.
	other := make(chan error, 1)
	go func() {
		_, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.tag": "concurrent metadata"}, nil, metadataGeneration4769(col))
		other <- err
	}()
	select {
	case err := <-other:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("native preparation retained mutation ownership")
	}
	resumed.Do(func() { close(resume) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("native stale-cut retry did not drain")
	}
	rows[0]["bio"] = "prepared native edit"
	rows[0]["meta"] = map[string]any{"tag": "concurrent metadata"}
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, rows...)
	r1MutationAssert5059(t, col, map[string]map[string]any{string(ids[0]): rows[0]}, known)
}

// Shutdown must wake a real public caller blocked before copying. The held
// structural admissions isolate this boundary from publication or disk timing.
func TestNativeStringPatchCloseWakesBeforeCopyWaiter(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	ids, retained, columns := r1MutationBatch5059(t, r1MutationRow5059(0))
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	coord := col.writeDomain.commandWALCoordinatorForDomain(db)
	var held [nativeStringPatchMaxRequests]nativeStringPatchAdmission
	for i := range held {
		var err error
		held[i], err = col.acquireNativeStringPatchAdmission()
		if err != nil {
			t.Fatal(err)
		}
		defer held[i].release()
	}
	waiting := make(chan struct{}, 1)
	hook := func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	if !nativeStringPatchAdmissionWaitingTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("native wait hook installed")
	}
	defer nativeStringPatchAdmissionWaitingTestHook.Store(nil)
	var copied atomic.Uint32
	copyHook := func() { copied.Add(1) }
	if !nativeStringPatchInputCopiedTestHook.CompareAndSwap(nil, &copyHook) {
		t.Fatal("native copy hook installed")
	}
	defer nativeStringPatchInputCopiedTestHook.Store(nil)
	done := make(chan error, 1)
	schema := col.Meta().Options.ColumnStore.SchemaHash
	go func() {
		_, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "bio", Value: "never copied"}}}}, schema)
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(30 * time.Second):
		t.Fatal("public caller never waited for credit")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, backenddb.ErrClosed) {
			t.Fatalf("closed wait returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("DB close stranded public admission waiter")
	}
	if copied.Load() != 0 {
		t.Fatal("shutdown waiter copied input")
	}
	for i := range held {
		held[i].release()
	}
	coord.mu.Lock()
	closed, admitted, group := coord.nativePatchClosed, coord.nativePatchAdmitted, coord.nativePatchGroup
	coord.mu.Unlock()
	if !closed || admitted != 0 || group != nil {
		t.Fatalf("closed admission did not drain: closed=%t admitted=%d group=%v", closed, admitted, group)
	}
}

func TestNativeStringPatchConflictingIDsPreserveAdmissionOrder(t *testing.T) {
	dir, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	row := r1MutationRow5059(0)
	ids, retained, columns := r1MutationBatch5059(t, row)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	before := len(collectionCommandWALFrames(t, dir))
	ready, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func() { once.Do(func() { close(ready) }); <-release }
	if !nativeStringPatchBeforeSealTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("native seal hook installed")
	}
	var released sync.Once
	defer func() { released.Do(func() { close(release) }); nativeStringPatchBeforeSealTestHook.Store(nil) }()
	coord := col.writeDomain.commandWALCoordinatorForDomain(db)
	schema := col.Meta().Options.ColumnStore.SchemaHash
	done := make(chan error, 3)
	launch := func(i int) {
		go func() {
			_, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "bio", Value: fmt.Sprintf("FIFO %d", i)}}}}, schema)
			done <- err
		}()
	}
	launch(0)
	select {
	case <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("first admitted request never formed")
	}
	waitTicket := func(want uint64) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			coord.mu.Lock()
			n := coord.nativePatchNextTicket
			coord.mu.Unlock()
			if n >= want {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("later input did not take its ordered slot")
	}
	launch(1)
	waitTicket(2)
	launch(2)
	waitTicket(3)
	released.Do(func() { close(release) })
	for range 3 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("conflicting admission did not drain")
		}
	}
	var values []string
	for _, frame := range collectionCommandWALFrames(t, dir)[before:] {
		if frame.PayloadFormat != commitlog.PayloadFormatCollectionTypedStringsPatchV1 {
			continue
		}
		payload, err := commitlog.DecodeCollectionTypedStringsPayload(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(payload.Documents) != 1 || len(payload.Documents[0].Edits) != 1 {
			t.Fatal("conflicting request boundary changed")
		}
		values = append(values, payload.Documents[0].Edits[0].Value)
	}
	if len(values) != 3 || values[0] != "FIFO 0" || values[1] != "FIFO 1" || values[2] != "FIFO 2" {
		t.Fatalf("individual command order=%v", values)
	}
	row["bio"] = "FIFO 2"
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, row)
	r1MutationAssert5059(t, col, map[string]map[string]any{string(ids[0]): row}, known)
}

func TestNativeStringPatchGroupAmbiguousACKAndReplay(t *testing.T) {
	dir, db, col := r1MutationOpen5059(t, true)
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	rows := make([]map[string]any, nativeStringPatchMaxRequests)
	old := make(map[string]map[string]any, len(rows))
	for i := range rows {
		rows[i] = r1MutationRow5059(i)
		old[rows[i]["id"].(string)] = r1MutationCopy5059(rows[i])
	}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, rows...)
	beforeFrames := len(collectionCommandWALFrames(t, dir))
	fault := errors.New("native group post-WAL-sync failure")
	var fired atomic.Bool
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL &&
			event.Point == durabilitycut.AfterDependencyFileSync &&
			fired.CompareAndSwap(false, true) {
			return fault
		}
		return nil
	})
	defer func() {
		if restore != nil {
			restore()
		}
	}()
	seal := nativeStringPatchHoldFormation(t, col)
	type outcome struct {
		result TypedStringPatchResult
		err    error
	}
	done := make(chan outcome, len(rows))
	for i := range rows {
		go func(i int) {
			result, e := col.PatchTypedStringsBatch([]TypedStringPatch{{
				ID: ids[i], Edits: []TypedStringEdit{{Column: "bio", Value: fmt.Sprintf("replay grouped %d", i)}},
			}}, schema)
			done <- outcome{result, e}
		}(i)
	}
	seal(len(rows))
	for range rows {
		select {
		case got := <-done:
			if !errors.Is(got.err, ErrCommitAmbiguous) || !errors.Is(got.err, fault) {
				t.Fatalf("group caller lost shared ambiguous outcome: %+v", got)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("ambiguous native group failed to drain callers")
		}
	}
	restore()
	restore = nil
	if !fired.Load() {
		t.Fatal("WAL fault did not fire")
	}
	coord := col.writeDomain.commandWALCoordinatorForDomain(db)
	coord.mu.Lock()
	admitted, group := coord.nativePatchAdmitted, coord.nativePatchGroup
	coord.mu.Unlock()
	if admitted != 0 || group != nil {
		t.Fatalf("ambiguous group retained admission: admitted=%d group=%p", admitted, group)
	}
	nextLSN := db.CommandWALNextLSN()
	if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0],
		Edits: []TypedStringEdit{{Column: "bio", Value: "must not retry poisoned DB"}},
	}}, schema); err == nil {
		t.Fatal("poisoned DB accepted another native request")
	}
	if got := db.CommandWALNextLSN(); got != nextLSN {
		t.Fatalf("poisoned refusal assigned new WAL identity: got=%d want=%d", got, nextLSN)
	}
	commands := 0
	for _, frame := range collectionCommandWALFrames(t, dir)[beforeFrames:] {
		if frame.Kind == commitlog.CommandKindCollectionUpdateBatchByID {
			commands++
		}
	}
	if commands != len(rows) {
		t.Fatalf("ambiguous group command count=%d want=%d (no automatic retry)", commands, len(rows))
	}
	r1LifecycleAssert5060(t, held, old, known)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	current, err := NewCollectionManager(reopened).OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]map[string]any, len(rows))
	for i, row := range rows {
		row["bio"] = fmt.Sprintf("replay grouped %d", i)
		want[row["id"].(string)] = row
	}
	r1MutationRemember5059(known, rows...)
	r1MutationAssert5059(t, current, want, known)
}

func TestNativeStringPatchPublicInputHasOneCreditedLifetime(t *testing.T) {
	_, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	row := r1MutationRow5059(0)
	ids, retained, columns := r1MutationBatch5059(t, row)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	request := []TypedStringPatch{{ID: append([]byte(nil), ids[0]...), Edits: []TypedStringEdit{{Column: "bio", Value: "credited canonical value"}}}}
	seal := nativeStringPatchHoldFormation(t, col)
	done := make(chan error, 1)
	go func() { _, err := col.PatchTypedStringsBatch(request, schema); done <- err }()
	coord := col.writeDomain.commandWALCoordinatorForDomain(col.db)
	var credit *nativeRequestCredit
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		coord.mu.Lock()
		group := coord.nativePatchGroup
		if group != nil && group.count == 1 {
			owned := group.requests[0].owned
			if owned != nil {
				credit = owned.credit
			}
		}
		coord.mu.Unlock()
		if credit != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if credit == nil {
		t.Fatal("public caller never attached canonical owned input")
	}
	before := credit.snapshot()
	if before.Closed || before.Retired || before.Debited[nativeRequestSourceCredit] == 0 || before.Reserved[nativeRequestMetadataCredit] != 0 || before.Reserved[nativeRequestAllocatorCredit] != 0 {
		t.Fatal("input lifetime duplicated publisher tranches or lacks credit", before)
	}
	request[0].ID[0] ^= 0x7f
	request[0].Edits[0].Value = "caller mutation after admission"
	seal(1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after := credit.snapshot()
	if !after.Closed || !after.Retired || after.Retained != 0 || after.Debited[nativeRequestSourceCredit] < before.Debited[nativeRequestSourceCredit] {
		t.Fatal("terminal input credit lifetime", after)
	}
	row["bio"] = "credited canonical value"
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, row)
	r1MutationAssert5059(t, col, map[string]map[string]any{string(ids[0]): row}, known)
}
