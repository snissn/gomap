package templatedb

import (
	"bytes"
	"context"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/template"
)

type memKV struct {
	data map[string][]byte
}

type memBatch struct {
	kv   *memKV
	sets map[string][]byte
	dels []string
}

func newMemKV() *memKV {
	return &memKV{data: make(map[string][]byte)}
}

func (m *memKV) Get(key []byte) ([]byte, error) {
	if v, ok := m.data[string(key)]; ok {
		cp := append([]byte(nil), v...)
		return cp, nil
	}
	return nil, nil
}

func (m *memKV) SetSync(key, value []byte) error {
	m.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (m *memKV) DeleteSync(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *memKV) NewBatch() Batch {
	return &memBatch{kv: m, sets: make(map[string][]byte)}
}

func (b *memBatch) Set(key, value []byte) error {
	b.sets[string(key)] = append([]byte(nil), value...)
	return nil
}

func (b *memBatch) Delete(key []byte) error {
	b.dels = append(b.dels, string(key))
	return nil
}

func (b *memBatch) WriteSync() error {
	for _, k := range b.dels {
		delete(b.kv.data, k)
	}
	for k, v := range b.sets {
		b.kv.data[k] = append([]byte(nil), v...)
	}
	return nil
}

func (b *memBatch) Close() error { return nil }

func TestCandidateListEvictionDeterminism(t *testing.T) {
	kv := newMemKV()
	store := New(kv, Config{MaxCandidatesPerFP: 2, MaxCandidateListBytes: 1024})
	cfg := template.Config{MinAnchorLen: 2, MaxAnchorLen: 64, MaxAnchorsPerTemplate: 4, MaxAnchorBytesTotal: 128}
	def1, err := template.EncodeTemplateDef(template.TemplateDef{Anchors: [][]byte{[]byte("aa"), []byte("bb")}}, cfg)
	if err != nil {
		t.Fatalf("encode def1: %v", err)
	}
	def2, err := template.EncodeTemplateDef(template.TemplateDef{Anchors: [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")}}, cfg)
	if err != nil {
		t.Fatalf("encode def2: %v", err)
	}
	def3, err := template.EncodeTemplateDef(template.TemplateDef{Anchors: [][]byte{[]byte("aa")}}, cfg)
	if err != nil {
		t.Fatalf("encode def3: %v", err)
	}
	if _, err := store.PutTemplateDef(context.Background(), def1, []uint64{1}); err != nil {
		t.Fatalf("put def1: %v", err)
	}
	if _, err := store.PutTemplateDef(context.Background(), def2, []uint64{1}); err != nil {
		t.Fatalf("put def2: %v", err)
	}
	if _, err := store.PutTemplateDef(context.Background(), def3, []uint64{1}); err != nil {
		t.Fatalf("put def3: %v", err)
	}
	cands, err := store.GetCandidates(context.Background(), 1, 0)
	if err != nil {
		t.Fatalf("get candidates: %v", err)
	}
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(cands))
	}
	if cands[0].Size > cands[1].Size {
		t.Fatalf("candidates not ordered by size")
	}
}

func TestBackendSystemRoot_InlineChunkedAndPointerRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	store := NewBackend(backend, Config{MaxCandidatesPerFP: 4, MaxCandidateListBytes: 4096})

	// 1) Small inline template definition (<= 2 KiB).
	inlineDef := bytes.Repeat([]byte("inline-template-anchor-"), 12)
	inlineID, err := store.PutTemplateDef(ctx, inlineDef, []uint64{101, 202})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutTemplateDef(inline): %v", err)
	}

	// 2) Multi-chunk inline template definition (> 2 KiB).
	chunkedDef := bytes.Repeat([]byte("chunked-template-anchor-payload-"), 256) // 8 KiB
	chunkedID, err := store.PutTemplateDef(ctx, chunkedDef, []uint64{101, 303})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutTemplateDef(chunked): %v", err)
	}

	// 3) Optional raw lane-254 value-log pointer template definition.
	ptrStore := NewBackend(backend, Config{MaxCandidatesPerFP: 4, MaxCandidateListBytes: 4096})
	ptrStore.SetPointerThreshold(1024)
	ptrDef := bytes.Repeat([]byte("pointer-template-anchor-payload-"), 256) // 8 KiB
	ptrID, err := ptrStore.PutTemplateDef(ctx, ptrDef, []uint64{404})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutTemplateDef(pointer): %v", err)
	}
	entry, ok, err := backend.GetSystemEntry(backenddb.SystemTemplateDefKey(ptrID))
	if err != nil || !ok {
		_ = backend.Close()
		t.Fatalf("GetSystemEntry(pointer): ok=%v err=%v", ok, err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		_ = backend.Close()
		t.Fatalf("expected pointer-backed template def to set FlagPointer, got flags=%#x", entry.Flags)
	}

	// Verify ValueLogGC retains the raw system pointer segment.
	if _, err := backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{}); err != nil {
		_ = backend.Close()
		t.Fatalf("ValueLogGC: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen read-only and verify all 3 encodings + FP routing lists survive.
	ro, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		ReadOnly:               true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open(ReadOnly): %v", err)
	}
	defer func() { _ = ro.Close() }()

	roStore := NewBackend(ro, Config{})
	for _, tc := range []struct {
		name string
		id   uint64
		want []byte
	}{
		{"inline", inlineID, inlineDef},
		{"chunked", chunkedID, chunkedDef},
		{"pointer", ptrID, ptrDef},
	} {
		gotStore, err := roStore.GetTemplateDef(ctx, tc.id)
		if err != nil || !bytes.Equal(gotStore, tc.want) {
			t.Fatalf("%s roStore.GetTemplateDef: len=%d want=%d err=%v", tc.name, len(gotStore), len(tc.want), err)
		}
		gotLookup, err := ro.LookupSystemTemplateDef(tc.id)
		if err != nil || !bytes.Equal(gotLookup, tc.want) {
			t.Fatalf("%s ro.LookupSystemTemplateDef: len=%d want=%d err=%v", tc.name, len(gotLookup), len(tc.want), err)
		}
	}

	cands, err := roStore.GetCandidates(ctx, 101, 0)
	if err != nil {
		t.Fatalf("roStore.GetCandidates(101): %v", err)
	}
	if len(cands) != 2 || cands[0].ID != inlineID || cands[1].ID != chunkedID {
		t.Fatalf("roStore.GetCandidates(101)=%+v want [%d %d]", cands, inlineID, chunkedID)
	}
	candsPtr, err := roStore.GetCandidates(ctx, 404, 0)
	if err != nil || len(candsPtr) != 1 || candsPtr[0].ID != ptrID {
		t.Fatalf("roStore.GetCandidates(404)=%+v err=%v want [%d]", candsPtr, err, ptrID)
	}
	defCount, err := ro.CountSystemPrefix([]byte("template/def/"))
	if err != nil || defCount != 3 {
		t.Fatalf("CountSystemPrefix(template/def/)=(%d, %v) want (3, nil)", defCount, err)
	}
	fpCount, err := ro.CountSystemPrefix([]byte("template/fp/"))
	if err != nil || fpCount != 4 {
		t.Fatalf("CountSystemPrefix(template/fp/)=(%d, %v) want (4, nil)", fpCount, err)
	}
}

func TestStore_StageTemplateDef_PiggybackAndIdempotentCallback(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = backend.Close() }()

	store := NewBackend(backend, Config{})
	ctx := context.Background()
	tcfg := template.NormalizeConfig(template.Config{})
	defBytes, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("stage-tpl-anchor-alpha-"), []byte("-stage-tpl-anchor-beta")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}

	inputFPs := []uint64{900, 300, 900}
	fired := false
	id, err := store.StageTemplateDef(ctx, defBytes, inputFPs, func() {
		fired = true
	})
	if err != nil || id == 0 {
		t.Fatalf("StageTemplateDef: id=%d err=%v", id, err)
	}
	// Verify caller's slice was not mutated.
	if inputFPs[0] != 900 || inputFPs[1] != 300 || inputFPs[2] != 900 {
		t.Fatalf("StageTemplateDef mutated caller routeFPs: %v", inputFPs)
	}
	if fired {
		t.Fatalf("onDurable should wait until piggybacked commit syncs")
	}
	if err := backend.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	if !fired {
		t.Fatalf("expected onDurable to fire after SetSync")
	}
	got, err := store.GetTemplateDef(ctx, id)
	if err != nil || !bytes.Equal(got, defBytes) {
		t.Fatalf("GetTemplateDef(%d): len=%d err=%v", id, len(got), err)
	}

	// Idempotent StageTemplateDef with no new routeFPs must invoke onDurable
	// after releasing store.mu (verified by reading from the store inside callback).
	idempotentFired := false
	id2, err := store.StageTemplateDef(ctx, defBytes, nil, func() {
		if _, cbErr := store.GetTemplateDef(ctx, id); cbErr != nil {
			t.Errorf("re-entrant GetTemplateDef inside onDurable failed: %v", cbErr)
		}
		idempotentFired = true
	})
	if err != nil || id2 != id {
		t.Fatalf("StageTemplateDef(idempotent): id=%d want %d err=%v", id2, id, err)
	}
	if !idempotentFired {
		t.Fatalf("expected idempotent StageTemplateDef onDurable to run")
	}
}

func TestStore_StageTemplateDef_PreservesDurabilityBarrierAndMergesStagedFP(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityDurable,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = backend.Close() }()

	store := NewBackend(backend, Config{})
	ctx := context.Background()
	tcfg := template.NormalizeConfig(template.Config{})
	def1, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("stage-merge-1-anchor-a-"), []byte("-stage-merge-1-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(1): %v", err)
	}
	def2, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("stage-merge-2-anchor-a-"), []byte("-stage-merge-2-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(2): %v", err)
	}

	const sharedFP uint64 = 0xfeedface01020304
	var cb1Fired, cb2Fired, cb3Fired bool
	id1, err := store.StageTemplateDef(ctx, def1, []uint64{sharedFP}, func() { cb1Fired = true })
	if err != nil || id1 == 0 {
		t.Fatalf("StageTemplateDef(def1): id=%d err=%v", id1, err)
	}
	// Stage a second distinct template onto the SAME fingerprint BEFORE any flush.
	id2, err := store.StageTemplateDef(ctx, def2, []uint64{sharedFP}, func() { cb2Fired = true })
	if err != nil || id2 == 0 || id2 == id1 {
		t.Fatalf("StageTemplateDef(def2): id=%d id1=%d err=%v", id2, id1, err)
	}

	// Piggyback both staged templates onto an UNSYNCED user batch in Durable mode.
	b := backend.NewBatch()
	if err := b.Set([]byte("u1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()
	if cb1Fired || cb2Fired {
		t.Fatalf("callbacks fired before durable sync barrier: cb1=%v cb2=%v", cb1Fired, cb2Fired)
	}
	// GetCandidates must hide unsynced template IDs until the durability sync barrier completes.
	preSyncCands, err := store.GetCandidates(ctx, sharedFP, 0)
	if err != nil {
		t.Fatalf("GetCandidates(pre-sync): %v", err)
	}
	if len(preSyncCands) != 0 {
		t.Fatalf("GetCandidates(pre-sync) exposed unsynced templates: %+v", preSyncCands)
	}

	// Re-staging def1 with no routeFPs while its original commit is still awaiting
	// the durability sync barrier must NOT invoke cb3 immediately.
	id1Again, err := store.StageTemplateDef(ctx, def1, nil, func() { cb3Fired = true })
	if err != nil || id1Again != id1 {
		t.Fatalf("StageTemplateDef(def1 idempotent): id=%d want %d err=%v", id1Again, id1, err)
	}
	if cb3Fired {
		t.Fatalf("idempotent StageTemplateDef fired callback before durability sync barrier")
	}

	// Checkpoint syncs the durable barrier and releases all three callbacks.
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !cb1Fired || !cb2Fired || !cb3Fired {
		t.Fatalf("expected all callbacks to fire after Checkpoint: cb1=%v cb2=%v cb3=%v", cb1Fired, cb2Fired, cb3Fired)
	}

	// Both staged templates must be present in the merged sharedFP candidate list.
	cands, err := store.GetCandidates(ctx, sharedFP, 0)
	if err != nil {
		t.Fatalf("GetCandidates: %v", err)
	}
	var saw1, saw2 bool
	for _, c := range cands {
		if c.ID == id1 {
			saw1 = true
		}
		if c.ID == id2 {
			saw2 = true
		}
	}
	if !saw1 || !saw2 {
		t.Fatalf("merged sharedFP candidates missing staged template: got=%+v want both %d and %d", cands, id1, id2)
	}

	// Stage a third template and immediately publish it synchronously via
	// PutTemplateDef BEFORE any flush; PutTemplateDef must commit it durably so
	// GetTemplateDef immediately succeeds.
	def3, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("stage-sync-3-anchor-a-"), []byte("-stage-sync-3-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(3): %v", err)
	}
	var cb4Fired bool
	stagedID3, err := store.StageTemplateDef(ctx, def3, []uint64{sharedFP}, func() { cb4Fired = true })
	if err != nil || stagedID3 == 0 {
		t.Fatalf("StageTemplateDef(def3): id=%d err=%v", stagedID3, err)
	}
	syncID3, err := store.PutTemplateDef(ctx, def3, nil)
	if err != nil || syncID3 != stagedID3 {
		t.Fatalf("PutTemplateDef(def3 after stage): id=%d want %d err=%v", syncID3, stagedID3, err)
	}
	if !cb4Fired {
		t.Fatalf("expected synchronous PutTemplateDef to flush staged def3 callback")
	}
	got3, err := store.GetTemplateDef(ctx, syncID3)
	if err != nil || !bytes.Equal(got3, def3) {
		t.Fatalf("GetTemplateDef(%d) after sync PutTemplateDef: len=%d err=%v", syncID3, len(got3), err)
	}

	// Verify re-entrant callback staging + unsynced piggyback durability barrier
	// on synchronous PutTemplateDef:
	// 1. Stage A with an onDurable callback that stages C on the same FP.
	// 2. Synchronously PutTemplateDef(B, [fp2]): must drain A, run callback (staging C),
	//    drain C, and commit B without losing any candidate on fp2.
	const reentrantFP uint64 = 0x11223344aabbccdd
	defA, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("reentrant-A-anchor-1-"), []byte("-reentrant-A-anchor-2")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(A): %v", err)
	}
	defB, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("reentrant-B-anchor-1-"), []byte("-reentrant-B-anchor-2")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(B): %v", err)
	}
	defC, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("reentrant-C-anchor-1-"), []byte("-reentrant-C-anchor-2")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(C): %v", err)
	}
	var idC uint64
	var stageCErr error
	idA, err := store.StageTemplateDef(ctx, defA, []uint64{reentrantFP}, func() {
		idC, stageCErr = store.StageTemplateDef(ctx, defC, []uint64{reentrantFP}, nil)
	})
	if err != nil || idA == 0 {
		t.Fatalf("StageTemplateDef(A): id=%d err=%v", idA, err)
	}
	idB, err := store.PutTemplateDef(ctx, defB, []uint64{reentrantFP})
	if err != nil || idB == 0 {
		t.Fatalf("PutTemplateDef(B): id=%d err=%v", idB, err)
	}
	if stageCErr != nil || idC == 0 {
		t.Fatalf("reentrant StageTemplateDef(C) failed: id=%d err=%v", idC, stageCErr)
	}
	if backend.HasPendingStagedSystemOps() || backend.HasUnsyncedSystemOps() {
		t.Fatalf("expected PutTemplateDef to quiesce reentrant staged ops and sync barriers")
	}
	cands2, err := store.GetCandidates(ctx, reentrantFP, 0)
	if err != nil {
		t.Fatalf("GetCandidates(reentrantFP): %v", err)
	}
	var sawA, sawB, sawC bool
	for _, c := range cands2 {
		switch c.ID {
		case idA:
			sawA = true
		case idB:
			sawB = true
		case idC:
			sawC = true
		}
	}
	if !sawA || !sawB || !sawC {
		t.Fatalf("reentrantFP candidates missing template: got=%+v want A=%d B=%d C=%d", cands2, idA, idB, idC)
	}

	// Verify synchronous PutTemplateDef syncs an already-piggybacked unsynced def.
	defD, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("unsynced-D-anchor-1-"), []byte("-unsynced-D-anchor-2")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(D): %v", err)
	}
	var cbDFired bool
	idD, err := store.StageTemplateDef(ctx, defD, nil, func() { cbDFired = true })
	if err != nil || idD == 0 {
		t.Fatalf("StageTemplateDef(D): id=%d err=%v", idD, err)
	}
	b2 := backend.NewBatch()
	if err := b2.Set([]byte("u2"), []byte("v2")); err != nil {
		t.Fatalf("Set(u2): %v", err)
	}
	if err := b2.Write(); err != nil {
		t.Fatalf("Write(u2): %v", err)
	}
	_ = b2.Close()
	if cbDFired {
		t.Fatalf("cbD fired before sync barrier")
	}
	idDSync, err := store.PutTemplateDef(ctx, defD, nil)
	if err != nil || idDSync != idD {
		t.Fatalf("PutTemplateDef(D): id=%d want %d err=%v", idDSync, idD, err)
	}
	if !cbDFired || backend.HasUnsyncedSystemOps() {
		t.Fatalf("expected synchronous PutTemplateDef(D) to sync outstanding barrier: cbDFired=%v unsynced=%v", cbDFired, backend.HasUnsyncedSystemOps())
	}
}
