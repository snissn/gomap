package raftfsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// The real FSM must lower hash-keyed template packets through the public native
// owner. Advisory preflight must never reserve its local or distributed identity.
func TestNativeTemplateClusterAdapterPreflightLifecycle(t *testing.T) {
	runNativeTemplateClusterAdapter(t, filepath.Join(t.TempDir(), "db"), false)
}
func runNativeTemplateClusterAdapter(t *testing.T, dir string, crash bool) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
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

	payload := testCreateCollectionMetaPayload("templates", testCreateCollectionMetaOptions{documentFormat: uint64(nativewire.DocumentFormatTemplateV1)})
	payload = binary.AppendUvarint(payload[:len(payload)-1], 1)
	payload = appendTestString(payload, "city")
	payload = appendTestString(payload, "city")
	payload = binary.AppendUvarint(payload, 1) // string
	payload = append(payload, 1, 0)            // unique, scalar
	payload = binary.AppendUvarint(payload, 0)
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("native-template:create")},
		{ID: nativewire.SectionCollectionMeta, Bytes: payload},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	command, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	create, err := nativewire.AppendDeterministicEntry(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	apply(1, create, 1)
	collection, err := manager.OpenCollection("templates")
	if err != nil {
		t.Fatal(err)
	}
	const firstJSON = `{"city":"hnl","values":[null,false,true,1.25,{"nested":"value"}],"big":9007199254740993}`
	firstRecord, _ := nativeClusterTemplatePacket(t, firstJSON)
	// The current exported self-contained TD1I carries TD1H and all nested
	// definitions. Subsequent hash-only input must reuse that exact dictionary.
	firstDoc, err := collections.EncodeTemplateV1DocumentJSON([]byte(firstJSON))
	if err != nil {
		t.Fatal(err)
	}
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
	checkDocument := func(reader *collections.StoredDocumentJSONMaterializer, stored []byte, want string) {
		t.Helper()
		if !bytes.HasPrefix(stored, []byte("TD1D")) {
			t.Fatalf("stored template=%x", stored)
		}
		actual, err := reader.StoredDocumentJSON(stored)
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
		checkDocument(materializer, response.Results[0].Document, firstJSON)
		actual, err := materializer.StoredDocumentJSON(response.Results[0].Document)
		if err != nil || !bytes.Contains(actual, []byte("9007199254740993")) {
			t.Fatalf("held lexical JSON number=%s err=%v", actual, err)
		}
		response.Results[0].Document[0] ^= 1
	}
	checkHeld()
	secondRecord, secondDoc := nativeClusterTemplatePacket(t, `{"city":"sea","values":[null,false,true,1.25,{"nested":"value"}],"big":9007199254740993}`)
	if !bytes.Equal(secondRecord, firstRecord) {
		t.Fatal("fixture must reuse the original template hash")
	}
	// This request has no sidecar: the original captured resolver must supply the
	// definition installed by the preceding command, never a collection-local ID.
	reuse := deterministicInsertBatchEntry(t, "templates", "native-template:reuse", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("u2")}, [][]byte{secondDoc})
	apply(3, reuse, 1)
	// Unknown hashes and malformed values with a known persisted hash must
	// refuse before acquiring either local WAL or distributed apply identity.
	unknown := bytes.Clone(secondDoc)
	clear(unknown[4:36])
	preflight(deterministicInsertBatchEntry(t, "templates", "native-template:unknown", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("bad")}, [][]byte{unknown}), true)
	malformed := bytes.Clone(secondDoc[:36])
	preflight(deterministicInsertBatchEntry(t, "templates", "native-template:malformed", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("bad")}, [][]byte{malformed}), true)
	// Wire validation admits the length-delimited number structurally; the
	// original captured preparer must reject its invalid lexical bytes before
	// any local/distributed identity or durable result/progress is acquired.
	invalidRecord, invalidNumber := nativeClusterTemplatePacket(t, `{"city":"invalid","revision":1}`)
	invalidNumber[len(invalidNumber)-1] = 'x'
	preflight(deterministicMutationEntry(t, nativewire.CommandInsertBatch, "templates", "native-template:invalid-number", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("bad")}, [][]byte{invalidNumber}, []nativewire.Section{{ID: nativewire.SectionTemplateRecords, Bytes: nativewire.AppendByteVector(nil, invalidRecord)}}), true)
	// Hash references precede a new dictionary carried by an absent final row.
	// Matched rows still need that definition in their original canonical frame.
	replacementRecord, u1 := nativeClusterTemplatePacket(t, `{"city":"sea","revision":1}`)
	_, u2 := nativeClusterTemplatePacket(t, `{"city":"hnl","revision":1}`)
	_, absent := nativeClusterTemplatePacket(t, `{"city":"unused","revision":1}`)
	inlineAbsent := append([]byte("TD1I"), 1)
	digest := sha256.Sum256(replacementRecord)
	inlineAbsent = append(inlineAbsent, digest[:]...)
	inlineAbsent = binary.AppendUvarint(inlineAbsent, uint64(len(replacementRecord)))
	inlineAbsent = append(inlineAbsent, replacementRecord...)
	inlineAbsent = append(inlineAbsent, absent...)
	replace := deterministicMutationEntry(t, nativewire.CommandReplaceBatch, "templates", "native-template:handoff", nativewire.DocumentFormatTemplateV1,
		[][]byte{[]byte("u1"), []byte("u2"), []byte("zz-missing")}, [][]byte{u1, u2, inlineAbsent}, []nativewire.Section{
			{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)},
		})
	if crash {
		// The original durable native append invokes this existing observer before
		// installation/seal/result/progress. It may not reenter or lock the DB.
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if basis := backend.State().AppliedCommandLSN; basis != 3 {
			t.Fatalf("basis=%d", basis)
		}
		preflight(replace, false)
		committed := CommittedEntryFromClusterV1(clusterCommittedCommand(2, 4, replace))
		meta, err := fsm.applyMetadata(committed, raftentry.ApplyEntryID{Term: 2, Index: 4})
		if err != nil {
			t.Fatal(err)
		}
		_, err = raftapply.ApplyCommittedEntryV1(fsm.db, committed.Bytes, meta, raftapply.Options{
			DecodeLimits: fsm.decodeLimits, ProgressStore: fsm.progress, ResultStore: fsm.results,
			FaultInjector: nativeTemplateExitAfterAppend{},
		})
		t.Fatalf("unapplied observer did not exit: %v", err)
	}
	if result := apply(4, replace, 2); result.MatchedCount != 2 {
		t.Fatalf("matched=%d", result.MatchedCount)
	}
	for _, query := range []struct{ city, id string }{{"sea", "u1"}, {"hnl", "u2"}} {
		got, err := collection.FindByIndexValue("city", query.city)
		if err != nil || len(got) != 1 || string(got[0]) != query.id {
			t.Fatalf("unique owner=%q err=%v", got, err)
		}
	}
	deletion := deterministicDeleteBatchEntry(t, "templates", "native-template:delete", [][]byte{[]byte("u2")})
	deleteResult := apply(5, deletion, 1)
	checkCurrent := func() {
		t.Helper()
		current, err := collection.NewStoredDocumentJSONMaterializer()
		if err != nil {
			t.Fatal(err)
		}
		defer current.Close()
		for _, row := range []struct{ id, json string }{{"u1", `{"city":"sea","revision":1}`}} {
			stored, err := collection.Get([]byte(row.id))
			if err != nil {
				t.Fatal(err)
			}
			checkDocument(current, stored, row.json)
			stored[0] ^= 1
		}
		if stored, err := collection.Get([]byte("u2")); err != nil || stored != nil {
			t.Fatalf("deleted=%x err=%v", stored, err)
		}
		if owners, err := collection.FindByIndexValue("city", "hnl"); err != nil || len(owners) != 0 {
			t.Fatalf("deleted owner=%q err=%v", owners, err)
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
	retry, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 5, deletion))
	if err != nil {
		t.Fatal(err)
	}
	if retry != deleteResult {
		t.Fatalf("durable retry=%+v want %+v", retry, deleteResult)
	}
	if backend.CommandWALNextLSN() != next || sequence() != seq {
		t.Fatal("durable retry assigned another native identity")
	}
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 2, Index: 5})
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
	if n <= 0 || count == 0 || count > uint64(len(input)) {
		t.Fatalf("invalid template definitions: count=%d bytes=%d", count, n)
	}
	cursor := 4 + n
	recordsStart := cursor
	for i := uint64(0); i < count; i++ {
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
		record := input[cursor : cursor+int(size)]
		cursor += int(size)
		digest := sha256.Sum256(record)
		if !bytes.Equal(hash, digest[:]) || !bytes.HasPrefix(record, []byte("TD1T")) {
			t.Fatal("noncanonical template record")
		}
	}
	root := input[cursor:]
	if len(root) < 4+sha256.Size || !bytes.HasPrefix(root, []byte("TD1H")) {
		t.Fatalf("unexpected encoder root=%x", root)
	}
	// The encoder sorts definitions by hash. Locate the authoritative root
	// record by the root's hash, never by its position among nested definitions.
	var rootRecord []byte
	cursor = recordsStart
	for i := uint64(0); i < count; i++ {
		hash := input[cursor : cursor+sha256.Size]
		cursor += sha256.Size
		size, n := binary.Uvarint(input[cursor:])
		cursor += n
		if bytes.Equal(hash, root[4:4+sha256.Size]) {
			rootRecord = bytes.Clone(input[cursor : cursor+int(size)])
		}
		cursor += int(size)
	}
	if rootRecord == nil {
		t.Fatal("missing root definition")
	}
	wire := bytes.Clone(root)
	copy(wire[:4], "TD1D")
	return rootRecord, wire
}

// This is the maintained apply fault seam, with the same non-reentrant observer
// contract as production. SyncLocalCommandWAL=true already synced the real frame.
type nativeTemplateExitAfterAppend struct{}

func (nativeTemplateExitAfterAppend) InjectApplyFault(point raftapply.FaultPointV1, ctx raftapply.ApplyFaultContextV1) error {
	if point == raftapply.FaultAfterLocalWALAppendBeforeVisibleV1 {
		if ctx.AppliedCommandLSN != 3 || ctx.EntryID != (raftentry.ApplyEntryID{Term: 2, Index: 4}) {
			fmt.Fprintf(os.Stderr, "unexpected template append LSN=%d\n", ctx.AppliedCommandLSN)
			os.Exit(98)
		}
		fmt.Fprintln(os.Stdout, "TEMPLATE APPLY UNAPPLIED basis=3 entry=2/4")
		os.Exit(97)
	}
	return nil
}

func TestNativeTemplateClusterAdapterUnappliedReplay(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	if dir := os.Getenv("GOMAP_TEMPLATE_APPLY_CHILD_DIR"); dir != "" {
		runNativeTemplateClusterAdapter(t, dir, true)
		return
	}
	dir := filepath.Join(t.TempDir(), "db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeTemplateClusterAdapterUnappliedReplay$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_TEMPLATE_APPLY_CHILD_DIR="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("TEMPLATE APPLY UNAPPLIED basis=3 entry=2/4")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("child: %v\n%s", err, output)
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	before := workstats.Read().Replay
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if database != nil {
			_ = database.Close()
		}
	})
	after := workstats.Read().Replay
	if after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
		t.Fatalf("replay: %+v -> %+v", before, after)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	if backend.State().AppliedCommandLSN != 4 {
		t.Fatalf("replayed applied=%d", backend.State().AppliedCommandLSN)
	}
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	budget, ok := capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
	if !ok {
		t.Fatal("original allocator stats unavailable")
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	registry := backend.ValueLogIdentityPinRegistry()
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() {
		if manager != nil {
			_ = manager.Close()
		}
	})
	collection, err := manager.OpenCollection("templates")
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if materializer != nil {
			_ = materializer.Close()
		}
	})
	for _, row := range []struct{ id, city string }{{"u1", "sea"}, {"u2", "hnl"}} {
		stored, err := collection.Get([]byte(row.id))
		if err != nil {
			t.Fatal(err)
		}
		actual, err := materializer.StoredDocumentJSON(stored)
		if err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			City     string `json:"city"`
			Revision int    `json:"revision"`
		}
		if err = json.Unmarshal(actual, &decoded); err != nil || decoded.City != row.city || decoded.Revision != 1 {
			t.Fatalf("replayed %s=%s err=%v", row.id, actual, err)
		}
		owners, err := collection.FindByIndexValue("city", row.city)
		if err != nil || len(owners) != 1 || string(owners[0]) != row.id {
			t.Fatalf("replayed owner=%q err=%v", owners, err)
		}
	}
	if absent, err := collection.Get([]byte("zz-missing")); err != nil || absent != nil {
		t.Fatalf("absent=%x err=%v", absent, err)
	}
	if err = materializer.Close(); err != nil {
		t.Fatal(err)
	}
	materializer = nil
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if s := budget.Stats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
		t.Fatalf("original final COW resources=%+v", s)
	}
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("original final registry=%+v", s)
	}
}
