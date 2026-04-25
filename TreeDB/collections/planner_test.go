package collections

import (
	"bytes"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestInsertBatchPlanner_EmitsRootLocalRunsForPrimaryIndexStateAndSecondaryRoots(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", unique: true},
			{name: "city", field: "city"},
		},
	}

	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if got, want := len(plan.runs), 4; got != want {
		t.Fatalf("runs len=%d want %d", got, want)
	}

	primary := mustFindRun(t, plan, collectionRootPrimary, "")
	primaryEntries := collectRunEntries(t, primary)
	assertEntryKeys(t, primaryEntries, "u1", "u2")
	if got, want := string(primaryEntries[0].value), `{"email":"ada@example.com","city":"hnl"}`; got != want {
		t.Fatalf("primary u1 value=%q want %q", got, want)
	}
	if got, want := string(primaryEntries[1].value), `{"email":"grace@example.com","city":"hnl"}`; got != want {
		t.Fatalf("primary u2 value=%q want %q", got, want)
	}

	state := mustFindRun(t, plan, collectionRootIndexState, "")
	stateEntries := collectRunEntries(t, state)
	assertEntryKeys(t, stateEntries, "u1", "u2")
	for _, entry := range stateEntries {
		if len(entry.value) == 0 {
			t.Fatalf("index-state entry for %q is empty", entry.key)
		}
	}

	email := mustFindRun(t, plan, collectionRootSecondary, "email")
	emailEntries := collectRunEntries(t, email)
	if got, want := len(emailEntries), 2; got != want {
		t.Fatalf("email index entries=%d want %d", got, want)
	}
	assertSortedEntries(t, emailEntries)
	if !bytes.HasSuffix(emailEntries[0].key, []byte("u1")) || !bytes.HasSuffix(emailEntries[1].key, []byte("u2")) {
		t.Fatalf("email entries should be value-prefix plus document id, got %q", entryKeys(emailEntries))
	}

	city := mustFindRun(t, plan, collectionRootSecondary, "city")
	cityEntries := collectRunEntries(t, city)
	if got, want := len(cityEntries), 2; got != want {
		t.Fatalf("city index entries=%d want %d", got, want)
	}
	assertSortedEntries(t, cityEntries)

	if got, want := len(plan.uniqueProbeRuns), 1; got != want {
		t.Fatalf("unique probe runs=%d want %d", got, want)
	}
	if got, want := plan.uniqueProbeRuns[0].indexName, "email"; got != want {
		t.Fatalf("unique probe index=%q want %q", got, want)
	}
	if got, want := len(plan.uniqueProbeRuns[0].prefixes), 2; got != want {
		t.Fatalf("unique probe prefixes=%d want %d", got, want)
	}
	for _, prefix := range plan.uniqueProbeRuns[0].prefixes {
		if bytes.Contains(prefix, []byte("u1")) || bytes.Contains(prefix, []byte("u2")) {
			t.Fatalf("unique probe prefix contains a document id: %q", prefix)
		}
	}
}

func TestInsertBatchPlanner_PreservesCallerVisibleResultOrdering(t *testing.T) {
	planner := insertBatchPlanner{collection: "users"}
	ids := [][]byte{[]byte("u3"), []byte("u1"), []byte("u2")}

	plan, err := planner.planInsertBatch(ids, [][]byte{
		[]byte(`{"name":"third"}`),
		[]byte(`{"name":"first"}`),
		[]byte(`{"name":"second"}`),
	})
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if len(plan.resultIDs) != len(ids) {
		t.Fatalf("result ids len=%d want %d", len(plan.resultIDs), len(ids))
	}
	for i := range ids {
		if !bytes.Equal(plan.resultIDs[i], ids[i]) {
			t.Fatalf("result id[%d]=%q want %q", i, plan.resultIDs[i], ids[i])
		}
	}

	primary := mustFindRun(t, plan, collectionRootPrimary, "")
	primaryEntries := collectRunEntries(t, primary)
	assertEntryKeys(t, primaryEntries, "u1", "u2", "u3")
	if got, want := plan.stats.payloadBuilds, 3; got != want {
		t.Fatalf("payload builds=%d want %d", got, want)
	}
}

func TestInsertBatchPlanner_FailFastDuplicatesBeforePayloadConstruction(t *testing.T) {
	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}

	_, err := planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate document id") {
		t.Fatalf("err=%v want duplicate document id", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}

	builds = 0
	planner.indexes = []indexDefinition{{name: "email", field: "email", unique: true}}
	_, err = planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"same@example.com"}`), []byte(`{"email":"same@example.com"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want unique index conflict", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}
}

func TestInsertBatchPlanner_RejectsPersistedDocumentIDBeforePayloadConstruction(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	primary := newCollectionRunTable(1)
	primary.SetSteal([]byte("u1"), []byte(`{"email":"seed@example.com"}`))
	primary.Freeze()
	_, rootIDs, err := d.PublishOrderedRootGroup(nil, []backenddb.OrderedRootPublishInput{{
		Iter: primary.NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("publish primary seed: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}
	_, err = planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"dup@example.com"}`), []byte(`{"email":"new@example.com"}`)},
		insertBatchPreflight{
			snapshot:      snap,
			primaryRootID: rootIDs[0],
		},
	)
	if err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("err=%v want persisted document conflict", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}
}

func TestInsertBatchPlanner_RejectsPersistedUniqueValueBeforePayloadConstruction(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	seedKey, err := indexEntryKey([]byte("s:seed@example.com"), []byte("seed"))
	if err != nil {
		t.Fatalf("seed index key: %v", err)
	}
	secondary := newCollectionRunTable(1)
	secondary.SetSteal(seedKey, nil)
	secondary.Freeze()
	_, rootIDs, err := d.PublishOrderedRootGroup(nil, []backenddb.OrderedRootPublishInput{{
		Iter: secondary.NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("publish secondary seed: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", unique: true},
		},
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}
	_, err = planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"seed@example.com"}`)},
		insertBatchPreflight{
			snapshot: snap,
			uniqueIndexRootIDs: map[string]uint64{
				"email": rootIDs[0],
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want persisted unique conflict", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}
}

func TestInsertBatchPlanner_PreflightUsesRootBatchProbes(t *testing.T) {
	probe := &recordingRootSnapshotProbe{}
	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", unique: true},
		},
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}

	_, err := planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
		insertBatchPreflight{
			snapshot:      probe,
			primaryRootID: 42,
			uniqueIndexRootIDs: map[string]uint64{
				"email": 77,
			},
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if got, want := probe.hasAnySortedCalls, 1; got != want {
		t.Fatalf("HasAnySortedAtRoot calls=%d want %d", got, want)
	}
	if got, want := probe.hasPrefixesCalls, 1; got != want {
		t.Fatalf("HasPrefixesAtRoot calls=%d want %d", got, want)
	}
	if got, want := probe.lastHasAnySortedRootID, uint64(42); got != want {
		t.Fatalf("HasAnySortedAtRoot root=%d want %d", got, want)
	}
	if got, want := probe.lastHasPrefixesRootID, uint64(77); got != want {
		t.Fatalf("HasPrefixesAtRoot root=%d want %d", got, want)
	}
	if got, want := byteMatrixStrings(probe.lastHasAnySortedKeys), []string{"u1", "u2"}; !equalStrings(got, want) {
		t.Fatalf("HasAnySortedAtRoot keys=%q want %q", got, want)
	}
	if got, want := len(probe.lastHasPrefixesPrefixes), 2; got != want {
		t.Fatalf("HasPrefixesAtRoot prefixes=%d want %d", got, want)
	}
	if builds != 2 {
		t.Fatalf("payload builds=%d want 2", builds)
	}
}

func TestInsertBatchPlanner_PublishesRunsThroughGroupedOrderedRootPublisher(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", unique: true},
		},
	}
	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}

	rootIDs, err := plan.publishRootRuns(d, nil)
	if err != nil {
		t.Fatalf("publish root runs: %v", err)
	}
	if got, want := len(rootIDs), len(plan.runs); got != want {
		t.Fatalf("root IDs len=%d want %d", got, want)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	primaryRoot := rootIDs[mustFindRunIndex(t, plan, collectionRootPrimary, "")]
	entry, err := snap.GetEntryAtRoot(primaryRoot, []byte("u1"))
	if err != nil {
		t.Fatalf("primary u1 lookup: %v", err)
	}
	if got, want := string(entry.Value), `{"email":"ada@example.com"}`; got != want {
		t.Fatalf("primary u1 value=%q want %q", got, want)
	}

	stateRoot := rootIDs[mustFindRunIndex(t, plan, collectionRootIndexState, "")]
	stateEntry, err := snap.GetEntryAtRoot(stateRoot, []byte("u2"))
	if err != nil {
		t.Fatalf("index-state u2 lookup: %v", err)
	}
	if len(stateEntry.Value) == 0 {
		t.Fatalf("index-state u2 is empty")
	}

	emailRoot := rootIDs[mustFindRunIndex(t, plan, collectionRootSecondary, "email")]
	it, err := snap.IteratorAtRoot(emailRoot, nil, nil)
	if err != nil {
		t.Fatalf("email index iterator: %v", err)
	}
	defer func() { _ = it.Close() }()
	seen := 0
	for it.Valid() {
		seen++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("email index iterator error: %v", err)
	}
	if got, want := seen, 2; got != want {
		t.Fatalf("email index entries=%d want %d", got, want)
	}
}

type runEntry struct {
	key   []byte
	value []byte
}

type recordingRootSnapshotProbe struct {
	hasAnySortedCalls       int
	hasPrefixesCalls        int
	lastHasAnySortedRootID  uint64
	lastHasPrefixesRootID   uint64
	lastHasAnySortedKeys    [][]byte
	lastHasPrefixesPrefixes [][]byte
}

func (p *recordingRootSnapshotProbe) HasAnySortedAtRoot(rootID uint64, keys [][]byte) (bool, error) {
	p.hasAnySortedCalls++
	p.lastHasAnySortedRootID = rootID
	p.lastHasAnySortedKeys = cloneByteMatrix(keys)
	return false, nil
}

func (p *recordingRootSnapshotProbe) HasPrefixesAtRoot(rootID uint64, prefixes [][]byte) ([]bool, error) {
	p.hasPrefixesCalls++
	p.lastHasPrefixesRootID = rootID
	p.lastHasPrefixesPrefixes = cloneByteMatrix(prefixes)
	return make([]bool, len(prefixes)), nil
}

func mustFindRun(t *testing.T, plan *insertBatchPlan, kind collectionRootKind, indexName string) collectionRootRun {
	t.Helper()
	for _, run := range plan.runs {
		if run.kind == kind && run.indexName == indexName {
			return run
		}
	}
	t.Fatalf("missing run kind=%d index=%q", kind, indexName)
	return collectionRootRun{}
}

func mustFindRunIndex(t *testing.T, plan *insertBatchPlan, kind collectionRootKind, indexName string) int {
	t.Helper()
	for i, run := range plan.runs {
		if run.kind == kind && run.indexName == indexName {
			return i
		}
	}
	t.Fatalf("missing run kind=%d index=%q", kind, indexName)
	return -1
}

func collectRunEntries(t *testing.T, run collectionRootRun) []runEntry {
	t.Helper()
	if run.table == nil {
		t.Fatalf("run %q has nil table", run.name)
	}
	it := run.table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	var entries []runEntry
	for it.Valid() {
		entries = append(entries, runEntry{
			key:   it.KeyCopy(nil),
			value: it.ValueCopy(nil),
		})
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterate run %q: %v", run.name, err)
	}
	return entries
}

func assertEntryKeys(t *testing.T, entries []runEntry, want ...string) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("entries len=%d want %d keys=%q", len(entries), len(want), entryKeys(entries))
	}
	for i := range want {
		if got := string(entries[i].key); got != want[i] {
			t.Fatalf("entry key[%d]=%q want %q; all keys=%q", i, got, want[i], entryKeys(entries))
		}
	}
}

func assertSortedEntries(t *testing.T, entries []runEntry) {
	t.Helper()
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].key, entries[i].key) > 0 {
			t.Fatalf("entries not sorted: %q", entryKeys(entries))
		}
	}
}

func entryKeys(entries []runEntry) [][]byte {
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = entries[i].key
	}
	return keys
}

func cloneByteMatrix(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}

func byteMatrixStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = string(in[i])
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
