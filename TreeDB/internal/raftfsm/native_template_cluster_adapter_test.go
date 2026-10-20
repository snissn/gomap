package raftfsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
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

// The real FSM must lower hash-keyed template packets through the public native
// owner. Advisory preflight must never reserve its local or distributed identity.
func TestNativeTemplateClusterAdapterPreflightLifecycle(t *testing.T) {
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

	create := deterministicCreateCollectionEntryWithOptions(t, "templates", "native-template:create", testCreateCollectionMetaOptions{documentFormat: uint64(nativewire.DocumentFormatTemplateV1)})
	apply(1, create, 1)
	collection, err := manager.OpenCollection("templates")
	if err != nil {
		t.Fatal(err)
	}
	firstRecord, firstDoc := nativeClusterTemplatePacket(t, `{"city":"hnl"}`)
	insert := deterministicMutationEntry(t, nativewire.CommandInsertBatch, "templates", "native-template:first", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("u1")}, [][]byte{firstDoc}, []nativewire.Section{{ID: nativewire.SectionTemplateRecords, Bytes: nativewire.AppendByteVector(nil, firstRecord)}})
	apply(2, insert, 1)
	held, err = collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if materializer != nil {
			if err := materializer.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	checkDocument := func(stored []byte, want string) {
		t.Helper()
		if !bytes.HasPrefix(stored, []byte("TD1D")) {
			t.Fatalf("stored template=%x", stored)
		}
		actual, err := materializer.StoredDocumentJSON(stored)
		if err != nil {
			t.Fatal(err)
		}
		var got, expected any
		if json.Unmarshal(actual, &got) != nil || json.Unmarshal([]byte(want), &expected) != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("materialized=%s want %s", actual, want)
		}
	}
	checkHeld := func() {
		t.Helper()
		response, err := held.FetchDocumentsByID([][]byte{[]byte("u1"), []byte("u2")}, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 || !response.Results[0].Found || response.Results[1].Found {
			t.Fatalf("held=%+v err=%v", response, err)
		}
		checkDocument(response.Results[0].Document, `{"city":"hnl"}`)
		response.Results[0].Document[0] ^= 1
	}
	checkHeld()
	secondRecord, secondDoc := nativeClusterTemplatePacket(t, `{"city":"sea"}`)
	if !bytes.Equal(secondRecord, firstRecord) {
		t.Fatal("fixture must reuse the original template hash")
	}
	// This request has no sidecar: the original captured resolver must supply the
	// definition installed by the preceding command, never a collection-local ID.
	reuse := deterministicInsertBatchEntry(t, "templates", "native-template:reuse", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("u2")}, [][]byte{secondDoc})
	reuseResult := apply(3, reuse, 1)
	checkCurrent := func() {
		t.Helper()
		for _, row := range []struct{ id, json string }{{"u1", `{"city":"hnl"}`}, {"u2", `{"city":"sea"}`}} {
			stored, err := collection.Get([]byte(row.id))
			if err != nil {
				t.Fatal(err)
			}
			checkDocument(stored, row.json)
			stored[0] ^= 1
		}
	}
	checkCurrent()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkHeld()
	checkCurrent()
	if err := materializer.Close(); err != nil {
		t.Fatal(err)
	}
	materializer = nil
	closeOriginal()
	openOriginal()
	collection, err = manager.OpenCollection("templates")
	if err != nil {
		t.Fatal(err)
	}
	materializer, err = collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	checkCurrent()
	next, seq := backend.CommandWALNextLSN(), sequence()
	retry, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 3, reuse))
	if err != nil {
		t.Fatal(err)
	}
	if retry != reuseResult {
		t.Fatalf("durable retry=%+v want %+v", retry, reuseResult)
	}
	if backend.CommandWALNextLSN() != next || sequence() != seq {
		t.Fatal("durable retry assigned another native identity")
	}
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 2, Index: 3})
	if err := materializer.Close(); err != nil {
		t.Fatal(err)
	}
	materializer = nil
	closeOriginal()
}

// Encode with the maintained public encoder, then expose exactly the existing
// deterministic wire's hash-keyed TD1D plus canonical TD1T sidecar. Stored numeric
// template IDs are never manufactured or accepted as request authority.
func nativeClusterTemplatePacket(t *testing.T, document string) ([]byte, []byte) {
	t.Helper()
	input, err := collections.EncodeTemplateV1DocumentJSON([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(input, []byte("TD1I")) {
		t.Fatalf("encoder input=%x", input)
	}
	count, n := binary.Uvarint(input[4:])
	if n <= 0 || count != 1 {
		t.Fatalf("fixture needs one template definition: count=%d bytes=%d", count, n)
	}
	cursor := 4 + n
	if len(input)-cursor < sha256.Size {
		t.Fatal("missing input hash")
	}
	hash := input[cursor : cursor+sha256.Size]
	cursor += sha256.Size
	size, n := binary.Uvarint(input[cursor:])
	if n <= 0 {
		t.Fatal("missing record length")
	}
	cursor += n
	if size > uint64(len(input)-cursor) {
		t.Fatal("truncated record")
	}
	record := bytes.Clone(input[cursor : cursor+int(size)])
	cursor += int(size)
	digest := sha256.Sum256(record)
	if !bytes.Equal(hash, digest[:]) || !bytes.HasPrefix(record, []byte("TD1T")) {
		t.Fatal("noncanonical template record")
	}
	root := input[cursor:]
	if len(root) < 4+sha256.Size || !bytes.HasPrefix(root, []byte("TD1H")) || !bytes.Equal(root[4:4+sha256.Size], hash) {
		t.Fatalf("unexpected encoder root=%x", root)
	}
	wire := bytes.Clone(root)
	copy(wire[:4], "TD1D")
	return record, wire
}
