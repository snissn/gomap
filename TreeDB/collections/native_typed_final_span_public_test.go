package collections

import (
	"bytes"
	"os"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Four original public insert commands retain their own logical identities.
// Their graph-free physical cut should publish one final collection plan. All
// semantic and owner-lifetime oracles precede the final amplification assertion.
func TestNativeTypedFinalInsertSpanPublic(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	opts := nativeTypedGroupOptions(t.TempDir())
	var owner *treedb.DB
	var backend *backenddb.DB
	var manager *CollectionManager
	var col, noise *Collection
	var held *CollectionReadView
	var budget interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	}
	closeAll := func() {
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
		if owner != nil {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
			owner = nil
			if budget != nil {
				if s := budget.Stats(); s.TotalBytes != 0 || s.ReservedBytes != 0 || s.ExternalLeases != 0 || s.Sources != 0 || s.Views != 0 || s.Generations != 0 {
					t.Errorf("original finite owner after Close: %+v", s)
				}
			}
			if backend == nil {
				return
			}
			pins := backend.ValueLogIdentityPinRegistry()
			if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
				t.Errorf("stable custody after Close: %+v: %v", s, pins.CleanupError())
			}
		}
	}
	t.Cleanup(closeAll)
	open := func(create bool) {
		t.Helper()
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(backend)
		if create {
			_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{
				DocumentFormat: DocumentFormatJSON,
				ColumnStore: &ColumnStoreConfig{
					Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
					Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
				},
			}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "noise", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
				t.Fatal(err)
			}
		}
		col, err = manager.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		noise, err = manager.OpenCollection("noise")
		if err != nil {
			t.Fatal(err)
		}
		read, err := col.nativeJSON.Capture("docs")
		if err != nil {
			t.Fatal(err)
		}
		budget = read.capture.NativeAllocator().(interface {
			Stats() memtable.COWStats
			Limits() memtable.COWLimits
		})
		if err = read.Close(); err != nil {
			t.Fatal(err)
		}
		if err = budget.Limits().Validate(); err != nil {
			t.Fatalf("original finite owner limits: %v", err)
		}
	}
	open(true)
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	docs := [][]byte{
		[]byte(`{"embedding":[1,0],"content":"first"}`),
		[]byte(`{"embedding":[0,1],"content":"second"}`),
		[]byte(`{"embedding":[1,1],"content":"third"}`),
		[]byte(`{"embedding":[2,1],"content":"fourth"}`),
	}
	vectors := [][]float32{{1, 0}, {0, 1}, {1, 1}, {2, 1}}
	lsns := make([]uint64, len(ids))
	for i := range ids {
		lsns[i] = backend.CommandWALNextLSN()
		got, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids[i:i+1], docs[i:i+1], "embedding", VectorMetricCosine, vectors[i:i+1])
		if err != nil || len(got) != 1 || !bytes.Equal(got[0], ids[i]) || stats.Documents != 1 {
			t.Fatalf("original insert %d: ids=%q stats=%+v error=%v", i, got, stats, err)
		}
		if next := backend.CommandWALNextLSN(); next != lsns[i]+1 {
			t.Fatalf("insert %d did not retain one original command: before=%d next=%d", i, lsns[i], next)
		}
		if i == 1 {
			held, err = col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// A genuinely later command in another namespace must not become this
	// collection's physical recovery frontier.
	noiseID, noiseDoc := []byte("later"), []byte(`{"content":"unrelated later command"}`)
	noiseLSN := backend.CommandWALNextLSN()
	if got, err := noise.InsertBatch([][]byte{noiseID}, [][]byte{noiseDoc}); err != nil || len(got) != 1 || !bytes.Equal(got[0], noiseID) {
		t.Fatalf("unrelated public insert: ids=%q error=%v", got, err)
	}
	if noiseLSN <= lsns[3] || backend.CommandWALNextLSN() != noiseLSN+1 {
		t.Fatal("missing distinct later unrelated command")
	}
	// Inspect the original immutable chain, not a reconstructed census. Four
	// distinct surviving insert deltas must reach the one checkpoint below.
	assertChain := func(read *NativeJSONRead, count int) {
		t.Helper()
		d := read.domain.domain.typed
		for i := count - 1; i >= 0; i-- {
			if d == nil || d.appliedLSN != lsns[i] || d.memberOrdinal != 0 || d.operation != ColumnPublishOperationInsert || len(d.rows) != 1 || !bytes.Equal(d.rows[0].ID, ids[i]) || d.rows[0].Deleted || len(d.deleted) != 0 || d.data == nil || d.data.graph != nil || d.data.source != nil {
				t.Fatalf("original insert delta %d: %+v", i, d)
			}
			d = d.previous
		}
		if d != nil {
			t.Fatal("unexpected earlier typed delta")
		}
	}
	read, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	assertChain(read, 4)
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	assertChain(held.nativeRead, 2)
	// Observe real durable envelopes before checkpoint can retire their files.
	seen := make(map[uint64]bool)
	for _, frame := range collectionCommandWALFrames(t, backend.Dir()) {
		for i, lsn := range lsns {
			if frame.LSN != lsn {
				continue
			}
			if seen[lsn] || frame.Kind != commitlog.CommandKindCompositeMutation || frame.PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
				t.Fatalf("original insert envelope: %+v", frame)
			}
			members := 0
			err = commitlog.ScanCompositeMutationPayload(frame.Payload, func(m commitlog.CompositeMutationMember) error {
				members++
				if m.Incarnation == 0 || m.Kind != commitlog.CommandKindCollectionInsertBatchByID || m.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
					t.Fatalf("original insert member: %+v", m)
				}
				payload, e := commitlog.DecodeCollectionInsertBatchByIDPayload(m.Payload)
				if e != nil || payload.Collection != "docs" || len(payload.Documents) != 1 || !bytes.Equal(payload.Documents[0].ID, ids[i]) || !bytes.Equal(payload.Documents[0].Document, docs[i]) {
					t.Fatalf("original canonical payload %d: %+v: %v", i, payload, e)
				}
				return nil
			})
			if err != nil || members != 1 {
				t.Fatalf("original singleton insert: members=%d error=%v", members, err)
			}
			seen[lsn] = true
		}
	}
	if len(seen) != len(lsns) {
		t.Fatalf("original insert WAL identities observed=%d want=%d", len(seen), len(lsns))
	}
	assertHeld := func() {
		t.Helper()
		got, e := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
		if e != nil || len(got.Results) != len(ids) {
			t.Fatalf("held earlier cut: %+v: %v", got, e)
		}
		for i, result := range got.Results {
			if result.Found != (i < 2) || (i < 2 && !bytes.Equal(result.Document, docs[i])) || (i >= 2 && result.Document != nil) {
				t.Fatalf("held earlier row %d changed: %+v", i, result)
			}
		}
		assertChain(held.nativeRead, 2)
	}
	assertHeld()
	var syncs atomic.Int64
	originalSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
		syncs.Add(1)
		return originalSync(file)
	}
	defer func() { syncColumnAssetSegmentFileForPublish = originalSync }()
	nextBefore := backend.CommandWALNextLSN()
	err = owner.Checkpoint()
	contentSyncs := syncs.Load()
	syncColumnAssetSegmentFileForPublish = originalSync
	if err != nil {
		t.Fatal(err)
	}
	if backend.CommandWALNextLSN() != nextBefore {
		t.Fatal("physical checkpoint synthesized or reassigned command WAL identities")
	}
	assertLatest := func() {
		t.Helper()
		for i, id := range ids {
			got, e := col.Get(id)
			if e != nil || !bytes.Equal(got, docs[i]) {
				t.Fatalf("latest %q: %s: %v", id, got, e)
			}
		}
		if got, e := noise.Get(noiseID); e != nil || !bytes.Equal(got, noiseDoc) {
			t.Fatalf("unrelated latest: %s: %v", got, e)
		}
		view, e := col.OpenCollectionReadView()
		if e != nil {
			t.Fatal(e)
		}
		meta, e := view.Meta()
		if closeErr := view.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		cfg := meta.Options.ColumnStore
		if e != nil || cfg == nil || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || *cfg.ActiveManifest != *cfg.RecoveryAuthoritativeManifest || cfg.RecoveryAuthoritativeAppliedCommandLSN != lsns[3] {
			t.Fatalf("target physical frontier must be original target LSN %d, not unrelated %d: %+v: %v", lsns[3], noiseLSN, cfg, e)
		}
		if col.registeredVectorIndex("vec") != nil {
			t.Fatal("graph-free insert span installed HNSW")
		}
	}
	assertLatest()
	assertHeld()
	closeAll()
	if t.Failed() {
		return
	}
	open(false)
	assertLatest()
	closeAll()
	if t.Failed() {
		return
	}
	// Deliberately terminal: baseline RED must first satisfy every original
	// identity, held-cut, exact-document, fresh-reopen and Close-zero oracle.
	if contentSyncs != 1 {
		t.Fatalf("graph-free final insert span content syncs=%d want=1 for four distinct original insert members", contentSyncs)
	}
}

// Both measured spans begin on a nonempty physical basis. A view of the first
// result must survive the second span without adopting its rows or metadata.
func TestNativeTypedFinalInsertSpanRepeatedPhysicalBasis(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	opts := nativeTypedGroupOptions(t.TempDir())
	var owner *treedb.DB
	var backend *backenddb.DB
	var manager *CollectionManager
	var col, noise *Collection
	var held *CollectionReadView
	closeAll := func(check bool) {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
			held = nil
		}
		if owner != nil {
			if check {
				finishNativeTypedGroup(t, owner, backend, manager, col)
			} else {
				if manager != nil {
					_ = manager.Close()
				}
				_ = owner.Close()
			}
			owner, manager = nil, nil
		}
	}
	t.Cleanup(func() { closeAll(false) })
	open := func(create bool) {
		t.Helper()
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(backend)
		if create {
			_, err = manager.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
				Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
			}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "unrelated", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
				t.Fatal(err)
			}
		}
		col, err = manager.OpenCollection("events")
		if err != nil {
			t.Fatal(err)
		}
		noise, err = manager.OpenCollection("unrelated")
		if err != nil {
			t.Fatal(err)
		}
		read, err := col.nativeJSON.Capture("events")
		if err != nil {
			t.Fatal(err)
		}
		limitsErr := read.capture.NativeAllocator().Limits().Validate()
		if err = read.Close(); err != nil {
			t.Fatal(err)
		}
		if limitsErr != nil {
			t.Fatal(limitsErr)
		}
	}
	open(true)
	ids := [][]byte{[]byte("seed"), []byte("a"), []byte("b"), []byte("c"), []byte("d")}
	docs := [][]byte{
		[]byte(`{"embedding":[1,0],"content":"physical seed"}`),
		[]byte(`{"embedding":[1,0],"content":"first span a"}`),
		[]byte(`{"embedding":[1,0],"content":"first span b"}`),
		[]byte(`{"embedding":[1,0],"content":"second span c"}`),
		[]byte(`{"embedding":[1,0],"content":"second span d"}`),
	}
	insert := func(i int) uint64 {
		t.Helper()
		lsn := backend.CommandWALNextLSN()
		got, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids[i:i+1], docs[i:i+1], "embedding", VectorMetricCosine, [][]float32{{1, 0}})
		if err != nil || len(got) != 1 || !bytes.Equal(got[0], ids[i]) || stats.Documents != 1 || backend.CommandWALNextLSN() != lsn+1 {
			t.Fatalf("original insert %d: ids=%q stats=%+v error=%v", i, got, stats, err)
		}
		return lsn
	}
	seedLSN := insert(0)
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	assertLatest := func(count int, targetLSN uint64) ColumnManifestIdentity {
		t.Helper()
		for i := 0; i < count; i++ {
			got, err := col.Get(ids[i])
			if err != nil || !bytes.Equal(got, docs[i]) {
				t.Fatalf("physical basis/latest %q: %s: %v", ids[i], got, err)
			}
		}
		view, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		meta, err := view.Meta()
		if e := view.Close(); e != nil {
			t.Fatal(e)
		}
		cfg := meta.Options.ColumnStore
		if err != nil || cfg == nil || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || *cfg.ActiveManifest != *cfg.RecoveryAuthoritativeManifest || cfg.RecoveryAuthoritativeAppliedCommandLSN != targetLSN {
			t.Fatalf("nonempty physical basis/frontier target=%d: %+v: %v", targetLSN, cfg, err)
		}
		if col.registeredVectorIndex("vec") != nil {
			t.Fatal("graph-free repeated span installed HNSW")
		}
		return *cfg.ActiveManifest
	}
	basis := assertLatest(1, seedLSN)
	var counts [2]int64
	var finalLSN uint64
	noiseIDs := [][]byte{[]byte("later-first"), []byte("later-second")}
	noiseDoc := []byte(`{"content":"actual later unrelated namespace"}`)
	var heldManifest ColumnManifestIdentity
	var heldLSN uint64
	for span := 0; span < 2; span++ {
		first := 1 + span*2
		firstLSN := insert(first)
		finalLSN = insert(first + 1)
		noiseLSN := backend.CommandWALNextLSN()
		if got, err := noise.InsertBatch(noiseIDs[span:span+1], [][]byte{noiseDoc}); err != nil || len(got) != 1 || !bytes.Equal(got[0], noiseIDs[span]) || noiseLSN <= finalLSN || backend.CommandWALNextLSN() != noiseLSN+1 {
			t.Fatalf("span %d later unrelated original command: ids=%q error=%v", span, got, err)
		}
		read, err := col.nativeJSON.Capture("events")
		if err != nil {
			t.Fatal(err)
		}
		// Exactly this span's two new original deltas must follow its retained
		// physical basis; no pressure drain may make a one-member test pass.
		d := read.domain.domain.typed
		if d == nil || d.appliedLSN != finalLSN || d.memberOrdinal != 0 || d.operation != ColumnPublishOperationInsert || len(d.rows) != 1 || !bytes.Equal(d.rows[0].ID, ids[first+1]) || d.data == nil || d.data.graph != nil || d.data.source != nil || d.previous == nil || d.previous.appliedLSN != firstLSN || d.previous.memberOrdinal != 0 || d.previous.operation != ColumnPublishOperationInsert || len(d.previous.rows) != 1 || !bytes.Equal(d.previous.rows[0].ID, ids[first]) || d.previous.data == nil || d.previous.data.graph != nil || d.previous.data.source != nil || d.previous.previous != nil {
			t.Fatalf("span %d lost original two-delta insert cut: %+v", span, d)
		}
		if err = read.Close(); err != nil {
			t.Fatal(err)
		}
		var syncs atomic.Int64
		originalSync := syncColumnAssetSegmentFileForPublish
		defer func() { syncColumnAssetSegmentFileForPublish = originalSync }()
		syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
			syncs.Add(1)
			return originalSync(file)
		}
		next := backend.CommandWALNextLSN()
		err = owner.Checkpoint()
		counts[span] = syncs.Load()
		syncColumnAssetSegmentFileForPublish = originalSync
		if err != nil {
			t.Fatal(err)
		}
		if backend.CommandWALNextLSN() != next {
			t.Fatal("repeated checkpoint changed original command identities")
		}
		latest := assertLatest(first+2, finalLSN)
		if latest.Generation <= basis.Generation {
			t.Fatalf("span %d did not advance its nonempty physical basis", span)
		}
		basis = latest
		if span == 0 {
			held, err = col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			heldManifest, heldLSN = latest, finalLSN
		}
	}
	// The held first-span view has a physical basis, unlike the logical-only
	// held prefix in the original causal test above.
	got, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || len(got.Results) != len(ids) {
		t.Fatalf("held physical cut: %+v: %v", got, err)
	}
	for i, result := range got.Results {
		if result.Found != (i < 3) || (i < 3 && !bytes.Equal(result.Document, docs[i])) || (i >= 3 && result.Document != nil) {
			t.Fatalf("held physical row %d changed across second span: %+v", i, result)
		}
	}
	meta, err := held.Meta()
	if err != nil || meta.Options.ColumnStore == nil || meta.Options.ColumnStore.ActiveManifest == nil || *meta.Options.ColumnStore.ActiveManifest != heldManifest || meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN != heldLSN {
		t.Fatalf("held physical catalog adopted later span: %+v: %v", meta, err)
	}
	assertNoise := func() {
		t.Helper()
		for _, id := range noiseIDs {
			if doc, e := noise.Get(id); e != nil || !bytes.Equal(doc, noiseDoc) {
				t.Fatalf("unrelated original row %q: %s: %v", id, doc, e)
			}
		}
	}
	assertNoise()
	closeAll(true)
	if t.Failed() {
		return
	}
	open(false)
	if reopened := assertLatest(len(ids), finalLSN); reopened != basis {
		t.Fatalf("reopen changed final physical identity: %+v != %+v", reopened, basis)
	}
	assertNoise()
	closeAll(true)
	if t.Failed() {
		return
	}
	if counts != [2]int64{1, 1} {
		t.Fatalf("repeated nonempty physical insert-span content syncs=%v want=[1 1]", counts)
	}
}
