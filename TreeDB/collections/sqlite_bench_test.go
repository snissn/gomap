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

const sqliteInsertIndexedDocumentSQL = `INSERT INTO documents(id, document) VALUES (?, ?)`

func openBenchmarkSQLiteDB(tb testing.TB, name string) *sql.DB {
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

func benchmarkSQLiteDocumentBatch(tb testing.TB, start, count int) ([]string, []string) {
	tb.Helper()

	rawIDs, rawDocs := benchmarkDocumentBatch(tb, start, count, true)
	ids := make([]string, count)
	docs := make([]string, count)
	for i := range rawIDs {
		ids[i] = string(rawIDs[i])
		docs[i] = string(rawDocs[i])
	}
	return ids, docs
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

func checkpointSQLiteWAL(tb testing.TB, db *sql.DB) {
	tb.Helper()

	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		tb.Fatalf("sqlite wal checkpoint: %v", err)
	}
}

func TestSQLiteBenchmarkSchemaExtractsIndexedFields(t *testing.T) {
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
