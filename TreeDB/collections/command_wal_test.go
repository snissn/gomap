package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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

func TestCollectionCommandWALDeleteBatchByIDReplayIgnoresMissingIDs(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
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

func TestCollectionCommandWALRejectsCatalogCreate(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	_, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{Name: "users"})
	if !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("CreateCollection error=%v, want ErrCommandWALUnsupported", err)
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
		Scope:         commitlog.CommandScopeCollection,
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
