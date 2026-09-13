package collections

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestTypedUpsertGroupSharesDurablePrefixAndRootPublication(t *testing.T) {
	dir, db, first := openTypedMinimaCollection(t)
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	second, err := first.writeDomain.manager.OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}

	beforeState := db.State()
	beforeSyncs := typedGroupStatUint64(t, db.Stats(), "treedb.command_wal.file_sync.calls_total")
	beforeFrames := collectionCommandWALFrames(t, dir)

	firstQueued := make(chan struct{})
	releaseFirst := make(chan struct{})
	afterInstall := make(chan struct{})
	releaseAck := make(chan struct{})
	var firstOnce, installOnce sync.Once
	queueHook := func() {
		firstOnce.Do(func() {
			close(firstQueued)
			<-releaseFirst
		})
	}
	installHook := func() {
		installOnce.Do(func() {
			close(afterInstall)
			<-releaseAck
		})
	}
	if !typedUpsertGroupBeforeSealTestHook.CompareAndSwap(nil, &queueHook) {
		t.Fatal("typed group queue hook already installed")
	}
	defer typedUpsertGroupBeforeSealTestHook.CompareAndSwap(&queueHook, nil)
	if !typedUpsertGroupAfterInstallTestHook.CompareAndSwap(nil, &installHook) {
		t.Fatal("typed group install hook already installed")
	}
	defer typedUpsertGroupAfterInstallTestHook.CompareAndSwap(&installHook, nil)

	type result struct {
		updated int
		handled bool
		stats   CollectionInsertStats
		err     error
	}
	done := make(chan result, 2)
	write := func(col *Collection, id, user string) {
		updated, handled, stats, err := col.TryUpsertTypedBatchGroup(
			[][]byte{[]byte(id)},
			[][]byte{[]byte(`{"id":"` + id + `"}`)},
			[]TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
				{Name: "content", Strings: []string{"grouped"}},
				{Name: "user", Strings: []string{user}},
				{Name: "path", Strings: []string{"p/" + id}},
			},
		)
		done <- result{updated: updated, handled: handled, stats: stats, err: err}
	}
	go write(first, "group-a", "u1")
	select {
	case <-firstQueued:
	case <-time.After(10 * time.Second):
		t.Fatal("first typed request did not enter group formation")
	}
	go write(second, "group-b", "u2")

	coord := first.writeDomain.commandWALCoordinatorForDomain(db)
	deadline := time.Now().Add(10 * time.Second)
	for {
		coord.mu.Lock()
		joined := coord.typedUpsertGroup != nil && len(coord.typedUpsertGroup.requests) == 2
		coord.mu.Unlock()
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second typed request did not join the group")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseFirst)

	select {
	case <-afterInstall:
	case <-time.After(20 * time.Second):
		t.Fatal("typed group did not reach installed-before-ack boundary")
	}
	select {
	case got := <-done:
		t.Fatalf("caller returned before coherent root installation: %+v", got)
	default:
	}
	for _, id := range [][]byte{[]byte("group-a"), []byte("group-b")} {
		doc, err := first.Get(id)
		if err != nil || !bytes.Contains(doc, []byte(`"content":"grouped"`)) {
			t.Fatalf("document %q not visible at installed-before-ack boundary: %s err=%v", id, doc, err)
		}
	}
	close(releaseAck)
	sharedPublications := 0
	for range 2 {
		select {
		case got := <-done:
			if got.err != nil || !got.handled || got.updated != 0 {
				t.Fatalf("grouped upsert result=%+v", got)
			}
			if got.stats.Documents != 1 {
				t.Fatalf("grouped caller documents=%d want=1", got.stats.Documents)
			}
			if got.stats.Publish != 0 {
				sharedPublications++
			}
		case <-time.After(20 * time.Second):
			t.Fatal("grouped typed caller did not finish")
		}
	}
	if sharedPublications != 1 {
		t.Fatalf("callers with shared publication timing=%d want=1", sharedPublications)
	}

	afterState := db.State()
	if afterState.CommitSeq != beforeState.CommitSeq+1 {
		t.Fatalf("commit seq=%d want one coherent publication after %d", afterState.CommitSeq, beforeState.CommitSeq)
	}
	if got := typedGroupStatUint64(t, db.Stats(), "treedb.command_wal.file_sync.calls_total"); got != beforeSyncs+1 {
		t.Fatalf("command WAL file syncs=%d want=%d", got, beforeSyncs+1)
	}
	frames := collectionCommandWALFrames(t, dir)[len(beforeFrames):]
	var mutations, barriers int
	for _, frame := range frames {
		switch frame.Kind {
		case commitlog.CommandKindCollectionReplaceSourceByID:
			mutations++
		case commitlog.CommandKindDurablePrefixBarrier:
			barriers++
		}
	}
	if mutations != 2 || barriers != 1 {
		t.Fatalf("group frames: mutations=%d barriers=%d total=%d", mutations, barriers, len(frames))
	}
	for _, id := range [][]byte{[]byte("group-a"), []byte("group-b")} {
		doc, err := first.Get(id)
		if err != nil || !bytes.Contains(doc, []byte(`"content":"grouped"`)) {
			t.Fatalf("installed document %q=%s err=%v", id, doc, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	reopenedCollection, err := NewCollectionManager(reopened).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range [][]byte{[]byte("group-a"), []byte("group-b")} {
		doc, err := reopenedCollection.Get(id)
		if err != nil || !bytes.Contains(doc, []byte(`"content":"grouped"`)) {
			t.Fatalf("reopened document %q=%s err=%v", id, doc, err)
		}
	}
}

func TestTypedUpsertGroupDeclinesPersistedUpdateBeforeQueue(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	ids, retained, columns := typedGroupBatch("existing", "before", "u1")
	if updated, _, err := col.UpsertTypedBatchWithStats(ids, retained, columns); err != nil || updated != 0 {
		t.Fatalf("seed typed upsert updated=%d err=%v", updated, err)
	}

	beforeState := db.State()
	beforeNextLSN := db.CommandWALNextLSN()
	var queued atomic.Bool
	hook := func() { queued.Store(true) }
	if !typedUpsertGroupBeforeSealTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("typed group queue hook already installed")
	}
	defer typedUpsertGroupBeforeSealTestHook.CompareAndSwap(&hook, nil)

	_, handled, _, err := col.TryUpsertTypedBatchGroup(typedGroupBatch("existing", "after", "u2"))
	if err != nil || handled {
		t.Fatalf("persisted update group handled=%t err=%v want pre-queue decline", handled, err)
	}
	if queued.Load() {
		t.Fatal("persisted update entered accepted group")
	}
	if got := db.CommandWALNextLSN(); got != beforeNextLSN {
		t.Fatalf("persisted update next LSN=%d want unchanged %d", got, beforeNextLSN)
	}
	if got := db.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("persisted update commit seq=%d want unchanged %d", got, beforeState.CommitSeq)
	}
	doc, err := col.Get([]byte("existing"))
	if err != nil || !bytes.Contains(doc, []byte(`"content":"before"`)) {
		t.Fatalf("persisted update changed document before fallback: %s err=%v", doc, err)
	}
}

func TestTypedUpsertGroupDeclinesOverlappingIDBeforeAdmission(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	other, err := col.writeDomain.manager.OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}

	firstQueued := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	hook := func() {
		once.Do(func() {
			close(firstQueued)
			<-releaseFirst
		})
	}
	if !typedUpsertGroupBeforeSealTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("typed group queue hook already installed")
	}
	defer typedUpsertGroupBeforeSealTestHook.CompareAndSwap(&hook, nil)

	type result struct {
		handled bool
		err     error
	}
	firstDone := make(chan result, 1)
	go func() {
		_, handled, _, err := col.TryUpsertTypedBatchGroup(typedGroupBatch("same", "first", "u1"))
		firstDone <- result{handled: handled, err: err}
	}()
	select {
	case <-firstQueued:
	case <-time.After(10 * time.Second):
		t.Fatal("first typed request did not enter group formation")
	}
	beforeNextLSN := db.CommandWALNextLSN()
	_, handled, _, err := other.TryUpsertTypedBatchGroup(typedGroupBatch("same", "second", "u2"))
	if err != nil || handled {
		t.Fatalf("overlapping request handled=%t err=%v want pre-admission decline", handled, err)
	}
	if got := db.CommandWALNextLSN(); got != beforeNextLSN {
		t.Fatalf("overlapping request next LSN=%d want unchanged %d", got, beforeNextLSN)
	}
	close(releaseFirst)
	select {
	case got := <-firstDone:
		if got.err != nil || !got.handled {
			t.Fatalf("admitted singleton result=%+v", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("admitted singleton did not complete")
	}
	doc, err := col.Get([]byte("same"))
	if err != nil || !bytes.Contains(doc, []byte(`"content":"first"`)) {
		t.Fatalf("admitted singleton document=%s err=%v", doc, err)
	}
}

func typedGroupBatch(id, content, user string) ([][]byte, [][]byte, []TypedColumnBatch) {
	return [][]byte{[]byte(id)}, [][]byte{[]byte(`{"id":"` + id + `"}`)}, []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{content}},
		{Name: "user", Strings: []string{user}},
		{Name: "path", Strings: []string{"p/" + id}},
	}
}

func typedGroupStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		t.Fatalf("stat %s=%q: %v", key, stats[key], err)
	}
	return value
}
