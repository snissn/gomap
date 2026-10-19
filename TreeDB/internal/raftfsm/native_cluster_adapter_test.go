package raftfsm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// Exercise the actual cluster adapter's decoded preflight and committed apply
// against the ordinary public COW owner, rather than a legacy DB or a mock FSM.
func TestNativeClusterAdapterPreflightLifecycle(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := filepath.Join(t.TempDir(), "db")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	var database *treedb.DB
	var backend *backenddb.DB
	var fsm *FSM
	var manager *collections.CollectionManager
	var held *collections.CollectionReadView
	var budget interface{ Stats() memtable.COWStats }
	closeOriginal := func() {
		t.Helper()
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
			held = nil
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if fsm != nil {
			if err := fsm.Close(); err != nil {
				t.Error(err)
			}
			fsm = nil
		}
		if database != nil {
			if backend == nil || budget == nil {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
				database = nil
				return
			}
			registry := backend.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Error(err)
			}
			database = nil
			if s := budget.Stats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
				t.Errorf("original final COW resources=%+v", s)
			}
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Errorf("original final registry=%+v", s)
			}
		}
	}
	t.Cleanup(closeOriginal)
	openOriginal := func() {
		t.Helper()
		var err error
		database, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		var ok bool
		budget, ok = capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
		if !ok {
			t.Fatal("original allocator stats unavailable")
		}
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		// Use the real durable FSM stores, with their ordinary sync behavior.
		fsm, err = Open(Options{DB: backend, Cluster: validFSMClusterConfig(dir)})
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(backend)
	}
	openOriginal()
	sequence := func() uint64 {
		t.Helper()
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		seq := capture.LogicalSequence()
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		return seq
	}
	// An advisory preflight must release all staged work without claiming a local
	// WAL identity, logical successor, physical seal, or durable apply progress.
	preflight := func(raw []byte, rejected bool) raftcluster.CommandEntryPreflightResultV1 {
		t.Helper()
		decoded, err := raftentry.DecodeCommandEntryV1(raw, raftentry.DecodeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		next, seq := backend.CommandWALNextLSN(), sequence()
		state, ok := backend.StateToken()
		if !ok {
			t.Fatal("missing original state token")
		}
		last, hasLast := fsm.LastApplied()
		result, err := fsm.PreflightCommandEntryV1(context.Background(), raftcluster.CommandEntryPreflightRequestV1{
			GroupID: fsm.cluster.GroupID, NodeID: fsm.cluster.NodeID,
			EntryBytes: raw, DecodedEntry: decoded,
			CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true,
			SyncLocalCommandWAL: true,
		})
		after, afterOK := backend.StateToken()
		afterLast, afterHasLast := fsm.LastApplied()
		if backend.CommandWALNextLSN() != next || sequence() != seq || !afterOK || after != state || afterLast != last || afterHasLast != hasLast {
			t.Fatalf("preflight claimed identity/state: next=%d/%d sequence=%d/%d state=%+v/%+v last=%+v/%+v", backend.CommandWALNextLSN(), next, sequence(), seq, after, state, afterLast, last)
		}
		if rejected {
			if code, ok := ErrorCodeOf(err); err == nil || !ok || code != raftentry.ErrorRejectedConflictV1 {
				t.Fatalf("preflight conflict code=(%s,%v) err=%v", code, ok, err)
			}
		} else if err != nil {
			t.Fatalf("native cluster decoded preflight: %v", err)
		}
		return result
	}
	apply := func(index uint64, raw []byte, affected int64) raftentry.ApplyResultV1 {
		t.Helper()
		preflight(raw, false)
		next, seq := backend.CommandWALNextLSN(), sequence()
		result, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, index, raw))
		if err != nil {
			t.Fatalf("committed entry %d: %v result=%+v", index, err, result)
		}
		assertApplied(t, result, raftentry.ApplyStatusApplied, affected)
		if backend.CommandWALNextLSN() != next+1 || backend.State().AppliedCommandLSN != next || sequence() != seq+1 {
			t.Fatalf("entry %d did not finish exactly one native sealed successor", index)
		}
		id := raftentry.ApplyEntryID{Term: 2, Index: index}
		assertLastApplied(t, fsm, id)
		record, found, err := fsm.results.LookupApplyResult(id)
		if err != nil || !found || record.Result != result || record.AppliedCommandLSN != next {
			t.Fatalf("durable result=%+v found=%v err=%v", record, found, err)
		}
		progress, found, err := fsm.progress.LookupApplyProgress(id)
		if err != nil || !found || progress.AppliedCommandLSN != next || progress.CommandDigest != result.CommandDigest {
			t.Fatalf("durable progress=%+v found=%v err=%v", progress, found, err)
		}
		return result
	}
	// The maintained v1 metadata format: append one unique scalar index to the
	// existing zero-index payload. No new command or catalog encoding is used.
	payload := testCreateCollectionMetaPayload("users", testCreateCollectionMetaOptions{documentFormat: uint64(nativewire.DocumentFormatJSON)})
	payload = binary.AppendUvarint(payload[:len(payload)-1], 1)
	payload = appendTestString(payload, "city")
	payload = appendTestString(payload, "city")
	payload = binary.AppendUvarint(payload, 1) // string
	payload = append(payload, 1, 0)            // unique, not multikey
	payload = binary.AppendUvarint(payload, 0) // default storage policy
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("native-cluster:create")},
		{ID: nativewire.SectionCollectionMeta, Bytes: payload},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	create, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatal(err)
	}
	apply(1, create, 1)
	collection, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("u1"), []byte("u2")}
	originals := [][]byte{[]byte(`{"id":"u1","city":"hnl"}`), []byte(`{"id":"u2","city":"sea"}`)}
	insert := deterministicInsertBatchEntry(t, "users", "native-cluster:insert", nativewire.DocumentFormatJSON, ids, originals)
	apply(2, insert, 2)
	held, err = collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	assertDocument := func(got, want []byte) {
		t.Helper()
		var actual, expected any
		if json.Unmarshal(got, &actual) != nil || json.Unmarshal(want, &expected) != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("document=%s want %s", got, want)
		}
	}
	checkHeld := func() {
		t.Helper()
		response, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 {
			t.Fatalf("held read=%+v err=%v", response, err)
		}
		for i, result := range response.Results {
			if !result.Found {
				t.Fatalf("held row %d missing", i)
			}
			assertDocument(result.Document, originals[i])
			result.Document[0] = '!'
		}
	}
	checkHeld()
	// An existing primary ID, a new unique owner collision, and an indexed
	// replacement collision must refuse without local or FSM identity. Duplicate
	// IDs within one request are rejected earlier by the canonical encoder.
	preflight(deterministicInsertBatchEntry(t, "users", "native-cluster:existing-primary", nativewire.DocumentFormatJSON,
		[][]byte{[]byte("u1")}, [][]byte{[]byte(`{"id":"u1","city":"sfo"}`)}), true)
	preflight(deterministicInsertBatchEntry(t, "users", "native-cluster:unique", nativewire.DocumentFormatJSON,
		[][]byte{[]byte("u3")}, [][]byte{[]byte(`{"id":"u3","city":"hnl"}`)}), true)
	preflight(deterministicReplaceBatchEntry(t, "users", "native-cluster:bad-replace", nativewire.DocumentFormatJSON,
		[][]byte{[]byte("u1")}, [][]byte{[]byte(`{"id":"u1","city":"sea"}`)}), true)
	// Advisory validation also accepts a genuine no-op without installing it.
	preflight(deterministicReplaceBatchEntry(t, "users", "native-cluster:noop", nativewire.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("missing")}, [][]byte{originals[0], []byte(`{"id":"missing","city":"x"}`)}), false)
	swapped := [][]byte{[]byte(`{"id":"u1","city":"sea"}`), []byte(`{"id":"u2","city":"hnl"}`)}
	replace := deterministicReplaceBatchEntry(t, "users", "native-cluster:handoff", nativewire.DocumentFormatJSON, ids, swapped)
	preflight(replace, false)
	for i, id := range ids {
		got, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		assertDocument(got, originals[i])
	}
	result := apply(3, replace, 2)
	if result.MatchedCount != 2 {
		t.Fatalf("unique handoff matched=%d want 2", result.MatchedCount)
	}
	for i, id := range ids {
		got, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		assertDocument(got, swapped[i])
	}
	for _, query := range []struct{ city, id string }{{"sea", "u1"}, {"hnl", "u2"}} {
		got, err := collection.FindByIndexValue("city", query.city)
		if err != nil || len(got) != 1 || string(got[0]) != query.id {
			t.Fatalf("index %s=%q err=%v", query.city, got, err)
		}
	}
	deletion := deterministicDeleteBatchEntry(t, "users", "native-cluster:delete", [][]byte{[]byte("u2"), []byte("missing")})
	deleteResult := apply(4, deletion, 1)
	checkRows := func() {
		t.Helper()
		got, err := collection.Get([]byte("u1"))
		if err != nil {
			t.Fatal(err)
		}
		assertDocument(got, swapped[0])
		got[0] = '!'
		if got, err := collection.Get([]byte("u2")); err != nil || got != nil {
			t.Fatalf("deleted row=%s err=%v", got, err)
		}
		if got, err := collection.FindByIndexValue("city", "hnl"); err != nil || len(got) != 0 {
			t.Fatalf("deleted unique owner=%q err=%v", got, err)
		}
	}
	checkRows()
	checkHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkRows()
	checkHeld()
	// Unsupported metadata cannot be admitted merely because its name is absent.
	unsupported := deterministicCreateCollectionEntryWithOptions(t, "bson", "native-cluster:unsupported", testCreateCollectionMetaOptions{documentFormat: uint64(nativewire.DocumentFormatBSON)})
	decoded, err := raftentry.DecodeCommandEntryV1(unsupported, raftentry.DecodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next, seq := backend.CommandWALNextLSN(), sequence()
	state, stateOK := backend.StateToken()
	if !stateOK {
		t.Fatal("missing original state token")
	}
	_, err = fsm.PreflightCommandEntryV1(context.Background(), raftcluster.CommandEntryPreflightRequestV1{
		GroupID: fsm.cluster.GroupID, NodeID: fsm.cluster.NodeID, EntryBytes: unsupported, DecodedEntry: decoded,
		CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true, SyncLocalCommandWAL: true,
	})
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("unsupported native create preflight=%v", err)
	}
	after, ok := backend.StateToken()
	if backend.CommandWALNextLSN() != next || sequence() != seq || !ok || after != state {
		t.Fatal("unsupported create claimed native state")
	}
	if _, err := manager.OpenCollection("bson"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("unsupported create became visible: %v", err)
	}
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 2, Index: 4})
	closeOriginal()
	openOriginal()
	collection, err = manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	checkRows()
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 2, Index: 4})
	next, seq = backend.CommandWALNextLSN(), sequence()
	if p := preflight(deletion, false); !p.KnownIdempotencyReplay {
		t.Fatal("durable retry was not recognized during preflight")
	}
	duplicate, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 4, deletion))
	if err != nil || duplicate != deleteResult || backend.CommandWALNextLSN() != next || sequence() != seq {
		t.Fatalf("durable retry=%+v err=%v next=%d/%d", duplicate, err, backend.CommandWALNextLSN(), next)
	}
	closeOriginal()
}
