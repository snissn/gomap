//go:build sqlite_bench && cgo

package collections_test

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	sqliteInsertIndexedDocumentSQL              = `INSERT INTO documents(id, document) VALUES (?, ?)`
	sqliteInsertNativeColumnsIndexedDocumentSQL = `INSERT INTO documents(id, name, email, city, pad) VALUES (?, ?, ?, ?, ?)`
)

func openBenchmarkSQLiteDB(tb testing.TB, name string) *sql.DB {
	tb.Helper()

	db := openBenchmarkSQLiteBaseDB(tb, name)
	for _, stmt := range []string{
		`CREATE TABLE documents (
			id TEXT PRIMARY KEY,
			document TEXT NOT NULL,
			email TEXT GENERATED ALWAYS AS (json_extract(document, '$.email')) STORED,
			city TEXT GENERATED ALWAYS AS (json_extract(document, '$.city')) STORED
		) WITHOUT ROWID`,
		`CREATE UNIQUE INDEX documents_email_idx ON documents(email)`,
		`CREATE INDEX documents_city_idx ON documents(city)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			tb.Fatalf("sqlite setup %q: %v", stmt, err)
		}
	}
	return db
}

func openBenchmarkSQLiteNativeColumnsDB(tb testing.TB, name string) *sql.DB {
	tb.Helper()

	db := openBenchmarkSQLiteBaseDB(tb, name)
	for _, stmt := range []string{
		`CREATE TABLE documents (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			city TEXT NOT NULL,
			pad TEXT NOT NULL
		) WITHOUT ROWID`,
		`CREATE UNIQUE INDEX documents_email_idx ON documents(email)`,
		`CREATE INDEX documents_city_idx ON documents(city)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			tb.Fatalf("sqlite native-columns setup %q: %v", stmt, err)
		}
	}
	return db
}

func openBenchmarkSQLiteBaseDB(tb testing.TB, name string) *sql.DB {
	tb.Helper()

	dbPath := filepath.Join(tb.TempDir(), name+".db")
	dsn := (&url.URL{
		Scheme: "file",
		Path:   dbPath,
	}).String() + "?_busy_timeout=5000&_foreign_keys=off&_journal_mode=WAL&_synchronous=NORMAL&_txlock=immediate"

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		tb.Fatalf("open sqlite: %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Errorf("close sqlite: %v", err)
		}
	})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, stmt := range []string{
		`PRAGMA page_size = 4096`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA cache_size = -262144`,
		`PRAGMA mmap_size = 268435456`,
		`PRAGMA wal_autocheckpoint = 0`,
		`PRAGMA foreign_keys = OFF`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			tb.Fatalf("sqlite base setup %q: %v", stmt, err)
		}
	}
	return db
}

func benchmarkSQLiteDocumentBatch(tb testing.TB, start, count int) ([]string, []string) {
	tb.Helper()

	ids := make([]string, count)
	docs := make([]string, count)
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = string(benchmarkDocumentID(docNum))
		docs[i] = string(benchmarkIndexedDocument(docNum))
	}
	return ids, docs
}

type benchmarkSQLiteNativeColumnsDocument struct {
	id    string
	name  string
	email string
	city  string
	pad   string
}

func benchmarkSQLiteNativeColumnsDocumentBatch(tb testing.TB, start, count int) []benchmarkSQLiteNativeColumnsDocument {
	tb.Helper()

	docs := make([]benchmarkSQLiteNativeColumnsDocument, count)
	for i := range docs {
		docNum := start + i
		docs[i] = benchmarkSQLiteNativeColumnsDocument{
			id:    string(benchmarkDocumentID(docNum)),
			name:  benchmarkSQLiteUserName(docNum),
			email: benchmarkSQLiteUserEmail(docNum),
			city:  benchmarkSQLiteCity(docNum),
			pad:   collectionBenchIndexedPad,
		}
	}
	return docs
}

func benchmarkSQLiteUserName(n int) string {
	out := make([]byte, 0, len("user-")+9)
	out = append(out, "user-"...)
	return string(appendZeroPaddedInt(out, n, 9))
}

func benchmarkSQLiteUserEmail(n int) string {
	out := make([]byte, 0, len("user-")+9+len("@example.com"))
	out = append(out, "user-"...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, "@example.com"...)
	return string(out)
}

func benchmarkSQLiteCity(n int) string {
	out := make([]byte, 0, len("city-")+2)
	out = append(out, "city-"...)
	return string(appendZeroPaddedInt(out, n%collectionBenchCities, 2))
}

func insertSQLiteDocumentBatch(tb testing.TB, db *sql.DB, ids, docs []string) {
	tb.Helper()

	if len(ids) != len(docs) {
		tb.Fatalf("sqlite batch length mismatch ids=%d docs=%d", len(ids), len(docs))
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatalf("sqlite begin: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, sqliteInsertIndexedDocumentSQL)
	if err != nil {
		tb.Fatalf("sqlite prepare insert: %v", err)
	}
	defer stmt.Close()

	for i := range ids {
		if _, err := stmt.ExecContext(ctx, ids[i], docs[i]); err != nil {
			tb.Fatalf("sqlite insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatalf("sqlite commit: %v", err)
	}
	committed = true
}

func insertSQLiteNativeColumnsDocumentBatch(tb testing.TB, db *sql.DB, docs []benchmarkSQLiteNativeColumnsDocument) {
	tb.Helper()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatalf("sqlite native-columns begin: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, sqliteInsertNativeColumnsIndexedDocumentSQL)
	if err != nil {
		tb.Fatalf("sqlite native-columns prepare insert: %v", err)
	}
	defer stmt.Close()

	for i := range docs {
		doc := docs[i]
		if _, err := stmt.ExecContext(ctx, doc.id, doc.name, doc.email, doc.city, doc.pad); err != nil {
			tb.Fatalf("sqlite native-columns insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatalf("sqlite native-columns commit: %v", err)
	}
	committed = true
}

func checkpointSQLiteWAL(tb testing.TB, db *sql.DB) {
	tb.Helper()

	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		tb.Fatalf("sqlite wal checkpoint: %v", err)
	}
}

func TestSQLiteBenchmarkSchemaExtractsIndexedFields(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_DOCUMENT_FORMAT", "template-v1")
	db := openBenchmarkSQLiteDB(t, "schema_extract")
	ids, docs := benchmarkSQLiteDocumentBatch(t, 0, 1)
	insertSQLiteDocumentBatch(t, db, ids, docs)

	var email, city string
	if err := db.QueryRow(`SELECT email, city FROM documents WHERE id = ?`, ids[0]).Scan(&email, &city); err != nil {
		t.Fatalf("query generated columns: %v", err)
	}
	if want := "user-000000000@example.com"; email != want {
		t.Fatalf("email=%q want %q", email, want)
	}
	if want := "city-00"; city != want {
		t.Fatalf("city=%q want %q", city, want)
	}
}

func TestSQLiteNativeColumnsBenchmarkSchemaStoresIndexedFields(t *testing.T) {
	db := openBenchmarkSQLiteNativeColumnsDB(t, "native_columns_schema")
	docs := benchmarkSQLiteNativeColumnsDocumentBatch(t, 0, 1)
	insertSQLiteNativeColumnsDocumentBatch(t, db, docs)

	var name, email, city, pad string
	if err := db.QueryRow(`SELECT name, email, city, pad FROM documents WHERE id = ?`, docs[0].id).Scan(&name, &email, &city, &pad); err != nil {
		t.Fatalf("query native columns: %v", err)
	}
	if want := "user-000000000"; name != want {
		t.Fatalf("name=%q want %q", name, want)
	}
	if want := "user-000000000@example.com"; email != want {
		t.Fatalf("email=%q want %q", email, want)
	}
	if want := "city-00"; city != want {
		t.Fatalf("city=%q want %q", city, want)
	}
	if pad != collectionBenchIndexedPad {
		t.Fatalf("pad=%q want %q", pad, collectionBenchIndexedPad)
	}

	var id string
	if err := db.QueryRow(`SELECT id FROM documents WHERE email = ?`, docs[0].email).Scan(&id); err != nil {
		t.Fatalf("query native email index: %v", err)
	}
	if id != docs[0].id {
		t.Fatalf("email index id=%q want %q", id, docs[0].id)
	}
}

func BenchmarkSQLiteInsertBatchWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteDB(b, "bench_insert_batch_secondary")
	targetBatchSize := benchmarkBatchSize(b)

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkSQLiteDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		insertSQLiteDocumentBatch(b, db, ids, docs)
		inserted += batchSize
	}
}

func BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteDB(b, "bench_insert_batch_checkpoint_secondary")
	targetBatchSize := benchmarkBatchSize(b)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/checkpoint")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkSQLiteDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		insertStart := time.Now()
		insertSQLiteDocumentBatch(b, db, ids, docs)
		insertElapsed += time.Since(insertStart)
		syncStart := time.Now()
		checkpointSQLiteWAL(b, db)
		syncElapsed += time.Since(syncStart)
		inserted += batchSize
	}
	b.StopTimer()
	benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
}

func BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteNativeColumnsDB(b, "bench_native_columns_insert_batch_secondary")
	targetBatchSize := benchmarkBatchSize(b)

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		docs := benchmarkSQLiteNativeColumnsDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		insertSQLiteNativeColumnsDocumentBatch(b, db, docs)
		inserted += batchSize
	}
}

func BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteNativeColumnsDB(b, "bench_native_columns_insert_batch_checkpoint_secondary")
	targetBatchSize := benchmarkBatchSize(b)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/checkpoint")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		docs := benchmarkSQLiteNativeColumnsDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		insertStart := time.Now()
		insertSQLiteNativeColumnsDocumentBatch(b, db, docs)
		insertElapsed += time.Since(insertStart)
		syncStart := time.Now()
		checkpointSQLiteWAL(b, db)
		syncElapsed += time.Since(syncStart)
		inserted += batchSize
	}
	b.StopTimer()
	benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
}
