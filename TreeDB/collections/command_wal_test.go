package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionCommandWALInsertBatchByIDPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		[]byte(`{"name":"Ada"}`),
		[]byte(`{"name":"Grace"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionDocument(t, reopened, "u1", `{"name":"Ada"}`)
	assertCollectionDocument(t, reopened, "u2", `{"name":"Grace"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertByIDPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"Ada"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCollectionInsertBatchByID {
		_ = d.Close()
		t.Fatalf("command WAL frames=%+v, want one collection insert frame", frames)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionDocument(t, reopened, "u1", `{"name":"Ada"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDReplayRecoversUnappliedFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u2"), Document: []byte(`{"name":"Grace"}`)},
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	assertCollectionDocument(t, col, "u2", `{"name":"Grace"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != 1 {
		t.Fatalf("command WAL frames after replay=%d, want original frame only", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDReplayTemplateV1StoredDocument(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection setup: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	encoder := &TemplateV1Encoder{}
	seed := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "HNL"})
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("seed")}, [][]byte{seed}, encoder); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatchWithTemplateV1Encoder setup: %v", err)
	}
	stored, err := encoder.EncodeDocument([]string{"name", "city"}, []any{"Grace", "NYC"})
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeDocument stored: %v", err)
	}
	if !bytes.HasPrefix(stored, []byte(templateV1StoredMagic)) {
		_ = d.Close()
		t.Fatalf("stored prefix=%q, want %q", stored[:min(len(stored), len(templateV1StoredMagic))], templateV1StoredMagic)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u1"), Document: stored},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Grace"`)) || !bytes.Contains(jsonDoc, []byte(`"NYC"`)) {
		t.Fatalf("materialized template doc=%s, want Grace/NYC", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDTemplateV1NewShapeReplay(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}
	doc := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "SEA"})

	updateDir := prepareCollectionCommandWALDir(t, meta)
	updateDB := openCollectionCommandWALDB(t, updateDir)
	updateCol, err := NewCollectionManager(updateDB).OpenCollection("users")
	if err != nil {
		_ = updateDB.Close()
		t.Fatalf("OpenCollection update: %v", err)
	}
	if _, err := updateCol.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		_ = updateDB.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := updateDB.Close(); err != nil {
		t.Fatalf("Close update DB: %v", err)
	}

	frames := collectionCommandWALFrames(t, updateDir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	frame := frames[0]
	if frame.Kind != commitlog.CommandKindCollectionInsertBatchByID {
		t.Fatalf("frame kind=%d, want insert batch", frame.Kind)
	}
	payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(frame.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
	if len(payload.Documents) != 1 {
		t.Fatalf("payload documents=%d, want 1", len(payload.Documents))
	}
	if bytes.HasPrefix(payload.Documents[0].Document, []byte(templateV1StoredMagic)) {
		t.Fatalf("TemplateV1 insert WAL payload used stored document bytes; replay needs deterministic input envelope")
	}

	replayDir := prepareCollectionCommandWALDir(t, meta)
	writeCollectionCommandWALFrame(t, replayDir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, frame.Payload)
	reopen := openCollectionCommandWALDB(t, replayDir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replay: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Ada"`)) || !bytes.Contains(jsonDoc, []byte(`"SEA"`)) {
		t.Fatalf("materialized template doc=%s, want Ada/SEA", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDReplayAdvancesEmptyFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", nil)
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDReplayIgnoresMissingIDs(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
			[]byte(`{"name":"Grace"}`),
		},
	})
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("u1"), []byte("missing")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if got, err := col.Get([]byte("u1")); err != nil || got != nil {
		t.Fatalf("u1=%q err=%v, want missing", got, err)
	}
	assertCollectionDocument(t, col, "u2", `{"name":"Grace"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDReplayAdvancesMissingOnlyFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
		},
	})
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDReplayAdvancesRootlessNoopFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDPublishesMissingOnlyNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	deleted, err := col.DeleteBatch([][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("DeleteBatch missing only: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted=%d, want 0", deleted)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteDocumentByIDPublishesMissingNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	deleted, err := col.DeleteDocument([]byte("missing"))
	if err != nil {
		t.Fatalf("DeleteDocument missing: %v", err)
	}
	if deleted {
		t.Fatal("deleted=true, want false")
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","active":false}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	callbacks := 0
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		callbacks++
		if !bytes.Contains(current, []byte(`"active":false`)) {
			t.Fatalf("callback current=%s, want inactive", current)
		}
		return []byte(`{"name":"Ada","active":true}`), true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified || callbacks != 1 {
		t.Fatalf("Update matched=%t modified=%t callbacks=%d, want true/true/1", matched, modified, callbacks)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada","active":true}`)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionDocument(t, reopened, "u1", `{"name":"Ada","active":true}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDReplayRecoversUnappliedFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","city":"hnl"}`),
			[]byte(`{"name":"Grace","city":"nyc"}`),
		},
	})
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada","city":"sea"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada","city":"sea"}`)
	assertCollectionDocument(t, col, "u2", `{"name":"Grace","city":"nyc"}`)
}

func TestCollectionCommandWALUpdateByIDReplayTemplateV1StoredDocument(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection setup: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	encoder := &TemplateV1Encoder{}
	seed := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "HNL"})
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{seed}, encoder); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatchWithTemplateV1Encoder setup: %v", err)
	}
	stored, err := encoder.EncodeDocument([]string{"name", "city"}, []any{"Ada", "SEA"})
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeDocument stored: %v", err)
	}
	if !bytes.HasPrefix(stored, []byte(templateV1StoredMagic)) {
		_ = d.Close()
		t.Fatalf("stored prefix=%q, want %q", stored[:min(len(stored), len(templateV1StoredMagic))], templateV1StoredMagic)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u1"), Document: stored},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Ada"`)) || !bytes.Contains(jsonDoc, []byte(`"SEA"`)) {
		t.Fatalf("materialized template doc=%s, want Ada/SEA", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDTemplateV1NewShapeReplay(t *testing.T) {
	baseMeta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}
	baseInsert := collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustTemplateV1Document(t, []string{"name"}, []any{"Ada"}),
		},
	}
	replacement := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "SEA"})

	updateDir := prepareCollectionCommandWALDir(t, baseMeta, baseInsert)
	updateDB := openCollectionCommandWALDB(t, updateDir)
	updateCol, err := NewCollectionManager(updateDB).OpenCollection("users")
	if err != nil {
		_ = updateDB.Close()
		t.Fatalf("OpenCollection update: %v", err)
	}
	matched, modified, err := updateCol.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		_ = updateDB.Close()
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		_ = updateDB.Close()
		t.Fatalf("Update matched=%t modified=%t, want true/true", matched, modified)
	}
	if err := updateDB.Close(); err != nil {
		t.Fatalf("Close update DB: %v", err)
	}

	frames := collectionCommandWALFrames(t, updateDir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	frame := frames[0]
	if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID {
		t.Fatalf("frame kind=%d, want update batch", frame.Kind)
	}
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	if len(payload.Documents) != 1 {
		t.Fatalf("payload documents=%d, want 1", len(payload.Documents))
	}
	if bytes.HasPrefix(payload.Documents[0].Document, []byte(templateV1StoredMagic)) {
		t.Fatalf("TemplateV1 update WAL payload used stored document bytes; replay needs deterministic input envelope")
	}

	replayDir := prepareCollectionCommandWALDir(t, baseMeta, baseInsert)
	writeCollectionCommandWALFrame(t, replayDir, 1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, frame.Payload)
	reopen := openCollectionCommandWALDB(t, replayDir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replay: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Ada"`)) || !bytes.Contains(jsonDoc, []byte(`"SEA"`)) {
		t.Fatalf("materialized template doc=%s, want Ada/SEA", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDIndexedPublishesSecondaryRoots(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","city":"hnl"}`),
			[]byte(`{"name":"Grace","city":"nyc"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"name":"Ada","city":"sea"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v, want one matched modified", results)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada","city":"sea"}`)
	assertCollectionIndexIDs(t, col, "city", "hnl")
	assertCollectionIndexIDs(t, col, "city", "sea", "u1")
	assertCollectionIndexIDs(t, col, "city", "nyc", "u2")
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDPublishesMissingNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.Update([]byte("missing"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"name":"Ada"}`), true, nil
	})
	if err != nil {
		t.Fatalf("Update missing: %v", err)
	}
	if matched || modified {
		t.Fatalf("Update missing matched=%t modified=%t, want false/false", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCollectionUpdateBatchByID {
		t.Fatalf("command WAL frames=%+v, want one collection update noop frame", frames)
	}
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frames[0].Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	if len(payload.Documents) != 0 {
		t.Fatalf("noop update payload documents=%d, want 0", len(payload.Documents))
	}
}

func TestCollectionCommandWALUpdateByIDPublishesCallbackNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids:  [][]byte{[]byte("u1")},
		docs: [][]byte{[]byte(`{"name":"Ada"}`)},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return nil, false, nil
	})
	if err != nil {
		t.Fatalf("Update callback noop: %v", err)
	}
	if !matched || modified {
		t.Fatalf("Update callback noop matched=%t modified=%t, want true/false", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
}

func TestCollectionCommandWALUpdateBSONSetUniqueIndexChangePublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString, Unique: true}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Grace"}, {Key: "city", Value: "nyc"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	assertCollectionIndexIDs(t, col, "city", "hnl")
	assertCollectionIndexIDs(t, col, "city", "sea", "u1")
	assertCollectionIndexIDs(t, col, "city", "nyc", "u2")
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetUniqueIndexReopenRecovery(t *testing.T) {
	doc1 := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}})
	doc2 := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Grace"}, {Key: "city", Value: "nyc"}})
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString, Unique: true}},
	}, collectionCommandWALSetupInsert{
		ids:  [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{doc1, doc2},
	})
	d := openCollectionCommandWALDB(t, dir)
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}}); err != nil {
		_ = d.Close()
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col2, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionIndexIDs(t, col2, "city", "hnl")
	assertCollectionIndexIDs(t, col2, "city", "sea", "u1")
	assertCollectionIndexIDs(t, col2, "city", "nyc", "u2")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDPreflightReplansStaleIndexedPlan(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","city":"hnl"}`),
			[]byte(`{"name":"Grace","city":"nyc"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	colA, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection A: %v", err)
	}
	colB, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection B: %v", err)
	}

	readyA := make(chan struct{}, 2)
	readyB := make(chan struct{}, 2)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	var callsA atomic.Int32
	var callsB atomic.Int32
	go func() {
		_, _, err := colA.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
			callsA.Add(1)
			readyA <- struct{}{}
			<-releaseA
			return []byte(`{"name":"Ada","city":"sea"}`), true, nil
		})
		doneA <- err
	}()
	go func() {
		_, _, err := colB.Update([]byte("u2"), func([]byte) ([]byte, bool, error) {
			call := callsB.Add(1)
			readyB <- struct{}{}
			<-releaseB
			if call == 1 {
				return []byte(`{"name":"Grace","city":"bos"}`), true, nil
			}
			return []byte(`{"name":"Grace","city":"lax"}`), true, nil
		})
		doneB <- err
	}()
	waitCollectionCommandWALSignal(t, readyA, "update A planned")
	waitCollectionCommandWALSignal(t, readyB, "update B planned")
	close(releaseA)
	if err := waitCollectionCommandWALErr(t, doneA, "update A"); err != nil {
		t.Fatalf("update A error: %v", err)
	}
	close(releaseB)
	if err := waitCollectionCommandWALErr(t, doneB, "update B"); err != nil {
		t.Fatalf("update B error: %v", err)
	}
	if got := callsB.Load(); got < 2 {
		t.Fatalf("update B callbacks=%d, want retry after stale command-WAL preflight", got)
	}
	assertCollectionDocument(t, colA, "u1", `{"name":"Ada","city":"sea"}`)
	assertCollectionDocument(t, colA, "u2", `{"name":"Grace","city":"lax"}`)
	assertCollectionIndexIDs(t, colA, "city", "sea", "u1")
	assertCollectionIndexIDs(t, colA, "city", "lax", "u2")
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
	if got := callsA.Load(); got != 1 {
		t.Fatalf("update A callbacks=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	var foundRetryPayload bool
	for _, frame := range frames {
		if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID {
			continue
		}
		payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
		if err != nil {
			t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
		}
		for _, doc := range payload.Documents {
			if bytes.Equal(doc.ID, []byte("u2")) {
				if bytes.Contains(doc.Document, []byte(`"city":"bos"`)) {
					t.Fatalf("stale retry command WAL payload recorded first callback result: %s", doc.Document)
				}
				if bytes.Contains(doc.Document, []byte(`"city":"lax"`)) {
					foundRetryPayload = true
				}
			}
		}
	}
	if !foundRetryPayload {
		t.Fatalf("command WAL frames did not record retry result for u2: %+v", frames)
	}
}

func TestCollectionCommandWALCreateCollectionPublishesAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCatalogCreateCollection || frames[0].Scope != commitlog.CommandScopeCatalog {
		_ = d.Close()
		t.Fatalf("catalog create frames=%+v, want one catalog create frame", frames)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if _, err := NewCollectionManager(reopen).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection after reopen: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("reopen AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALCreateCollectionReplayRecoversUnappliedFrame(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	payload := catalogCreateCollectionPayload(t, meta)
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if _, err := NewCollectionManager(reopen).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection after replay: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALCreateCollectionReplaySameMetadataIdempotent(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	dir := prepareCollectionCommandWALDir(t, meta)
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, meta))

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection after idempotent replay: %v", err)
	}
	if normalizedDocumentFormat(col.meta.Options.DocumentFormat) != DocumentFormatJSON {
		t.Fatalf("document format=%q, want json", col.meta.Options.DocumentFormat)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALCreateCollectionReplayIncompatibleMetadataFailsClosed(t *testing.T) {
	existing := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	dir := prepareCollectionCommandWALDir(t, existing)
	incompatible := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, incompatible))

	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err == nil {
		_ = d.Close()
		t.Fatalf("Open with incompatible catalog replay succeeded, want fail-closed error")
	}
	if !errors.Is(err, backenddb.ErrCommandWALUnsupported) && !bytes.Contains([]byte(err.Error()), []byte("incompatible")) {
		t.Fatalf("Open error=%v, want incompatible catalog replay failure", err)
	}

	if err := os.RemoveAll(backenddb.WALDirPath(dir)); err != nil {
		t.Fatalf("RemoveAll wal: %v", err)
	}
	inspect := openCollectionCommandWALDB(t, dir)
	defer func() { _ = inspect.Close() }()
	col, err := NewCollectionManager(inspect).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection after removing failed frame: %v", err)
	}
	if normalizedDocumentFormat(col.meta.Options.DocumentFormat) != DocumentFormatJSON {
		t.Fatalf("document format after failed replay=%q, want original json", col.meta.Options.DocumentFormat)
	}
	if got := inspect.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after failed replay=%d, want original 0", got)
	}
}

func TestCollectionCommandWALCreateCollectionDrainsRecoveredLowerLSN(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	rawPayload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindRawKVBatch, commitlog.PayloadFormatRawKVBatchV1, rawPayload)

	d := openCollectionCommandWALDB(t, dir)
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after lower LSN recovery=%d, want 1", got)
	}
	if _, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection after lower LSN recovery: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after catalog create=%d, want 2", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCollectionCommandWALCreateCollectionExistingNoop(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	dir := prepareCollectionCommandWALDir(t, meta)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	got, err := NewCollectionManager(d).CreateCollection(&meta)
	if err != nil {
		t.Fatalf("CreateCollection existing: %v", err)
	}
	if got.Name != meta.Name {
		t.Fatalf("existing metadata=%+v want collection name %q", got, meta.Name)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 for existing no-op", got)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != 0 {
		t.Fatalf("command WAL frames=%d, want 0 for existing no-op", got)
	}
}

func TestCollectionCommandWALRejectsCatalogIndexMutations(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString}); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("CreateIndex error=%v, want ErrCommandWALUnsupported", err)
	}
	if _, err := col.DropIndex("city"); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("DropIndex error=%v, want ErrCommandWALUnsupported", err)
	}
	if _, err := col.DropIndexes([]string{"city"}); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("DropIndexes error=%v, want ErrCommandWALUnsupported", err)
	}
	if _, err := col.DropAllIndexes(); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("DropAllIndexes error=%v, want ErrCommandWALUnsupported", err)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after rejected index DDL=%d, want 0", got)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != 0 {
		t.Fatalf("command WAL frames after rejected index DDL=%d, want 0", got)
	}
}

type collectionCommandWALSetupInsert struct {
	ids  [][]byte
	docs [][]byte
}

func prepareCollectionCommandWALDir(t *testing.T, meta CollectionMeta, inserts ...collectionCommandWALSetupInsert) string {
	t.Helper()
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&meta); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection setup: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	for _, insert := range inserts {
		if _, err := col.InsertBatch(insert.ids, insert.docs); err != nil {
			_ = d.Close()
			t.Fatalf("InsertBatch setup: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint setup DB: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	return dir
}

func openCollectionCommandWALDB(t *testing.T, dir string) *backenddb.DB {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL DB: %v", err)
	}
	return d
}

func writeCollectionCommandWALFrame(t *testing.T, dir string, lsn uint64, kind commitlog.CommandKind, format commitlog.PayloadFormat, payload []byte) {
	t.Helper()
	walDir := backenddb.WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(commitlog.CommandEnvelope{
		LSN:           lsn,
		Kind:          kind,
		Scope:         commandWALScopeForKind(kind),
		PayloadFormat: format,
		Payload:       payload,
	}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func commandWALScopeForKind(kind commitlog.CommandKind) commitlog.CommandScope {
	switch kind {
	case commitlog.CommandKindRawKVBatch:
		return commitlog.CommandScopeRawKV
	case commitlog.CommandKindCatalogCreateCollection:
		return commitlog.CommandScopeCatalog
	default:
		return commitlog.CommandScopeCollection
	}
}

func catalogCreateCollectionPayload(t *testing.T, meta CollectionMeta) []byte {
	t.Helper()
	encoded, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatalf("encodeCollectionMeta: %v", err)
	}
	payload, err := commitlog.EncodeCatalogCreateCollectionPayload(meta.Name, encoded)
	if err != nil {
		t.Fatalf("EncodeCatalogCreateCollectionPayload: %v", err)
	}
	return payload
}

func countCollectionCommandWALFrames(t *testing.T, dir string) int {
	t.Helper()
	return len(collectionCommandWALFrames(t, dir))
}

func collectionCommandWALFrames(t *testing.T, dir string) []commitlog.CommandEnvelope {
	t.Helper()
	walDir := backenddb.WALDirPath(dir)
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("ReadDir wal: %v", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		paths = append(paths, filepath.Join(walDir, entry.Name()))
	}
	frames, err := commitlog.ScanCommandFrameSegments(paths, commitlog.Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrameSegments: %v", err)
	}
	return frames
}

func assertCollectionDocument(t *testing.T, col *Collection, id string, want string) {
	t.Helper()
	got, err := col.Get([]byte(id))
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%q)=%q, want %q", id, got, want)
	}
}

func waitCollectionCommandWALSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
	}
}

func waitCollectionCommandWALErr(t *testing.T, ch <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
		return nil
	}
}

func assertCollectionIndexIDs(t *testing.T, col *Collection, indexName string, value any, want ...string) {
	t.Helper()
	got, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		t.Fatalf("FindByIndexValue(%q,%v): %v", indexName, value, err)
	}
	if len(got) != len(want) {
		t.Fatalf("FindByIndexValue(%q,%v)=%q, want %q", indexName, value, got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("FindByIndexValue(%q,%v)[%d]=%q, want %q (all got %q)", indexName, value, i, got[i], want[i], got)
		}
	}
}
