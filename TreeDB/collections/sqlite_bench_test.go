//go:build sqlite_bench && cgo

package collections_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
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

	return openBenchmarkSQLiteJSONShapeDB(tb, name, 2)
}

func openBenchmarkSQLiteNativeColumnsDB(tb testing.TB, name string) *sql.DB {
	tb.Helper()

	return openBenchmarkSQLiteNativeColumnsShapeDB(tb, name, 2)
}

func openBenchmarkSQLiteJSONShapeDB(tb testing.TB, name string, indexCount int) *sql.DB {
	tb.Helper()

	db := openBenchmarkSQLiteBaseDB(tb, name)
	columns := []string{
		"id TEXT PRIMARY KEY",
		"document TEXT NOT NULL",
	}
	if indexCount >= 1 {
		columns = append(columns, "email TEXT GENERATED ALWAYS AS (json_extract(document, '$.email')) STORED")
	}
	if indexCount >= 2 {
		columns = append(columns, "city TEXT GENERATED ALWAYS AS (json_extract(document, '$.city')) STORED")
	}
	if indexCount >= 3 {
		columns = append(columns, "name TEXT GENERATED ALWAYS AS (json_extract(document, '$.name')) STORED")
	}
	stmts := append([]string{
		"CREATE TABLE documents (" + strings.Join(columns, ", ") + ") WITHOUT ROWID",
	}, sqliteShapeIndexStatements(indexCount)...)
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			tb.Fatalf("sqlite setup %q: %v", stmt, err)
		}
	}
	return db
}

func openBenchmarkSQLiteNativeColumnsShapeDB(tb testing.TB, name string, indexCount int) *sql.DB {
	tb.Helper()

	db := openBenchmarkSQLiteBaseDB(tb, name)
	stmts := append([]string{
		`CREATE TABLE documents (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			city TEXT NOT NULL,
			pad TEXT NOT NULL
		) WITHOUT ROWID`,
	}, sqliteShapeIndexStatements(indexCount)...)
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			tb.Fatalf("sqlite native-columns setup %q: %v", stmt, err)
		}
	}
	return db
}

func sqliteShapeIndexStatements(indexCount int) []string {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []string{`CREATE UNIQUE INDEX documents_email_idx ON documents(email)`}
	case 2:
		return []string{
			`CREATE UNIQUE INDEX documents_email_idx ON documents(email)`,
			`CREATE INDEX documents_city_idx ON documents(city)`,
		}
	case 3:
		return []string{
			`CREATE UNIQUE INDEX documents_email_idx ON documents(email)`,
			`CREATE INDEX documents_city_idx ON documents(city)`,
			`CREATE INDEX documents_name_idx ON documents(name)`,
		}
	default:
		panic(fmt.Sprintf("unsupported sqlite benchmark index count %d", indexCount))
	}
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

func benchmarkReportSQLiteDiskUsage(b *testing.B, db *sql.DB, docs int) {
	b.Helper()

	if docs <= 0 {
		return
	}
	if !benchmarkBoolEnv(b, "TREEDB_COLLECTION_REPORT_DISK_USAGE", true) {
		return
	}
	checkpointSQLiteWAL(b, db)
	totalBytes := benchmarkSQLiteMainDiskBytes(b, db)
	benchmarkReportDiskUsage(b, docs, totalBytes)
	if collectionBytes, indexBytes, ok := benchmarkSQLiteObjectDiskUsage(db); ok {
		b.ReportMetric(float64(collectionBytes), "collection_disk_bytes")
		b.ReportMetric(float64(collectionBytes)/float64(docs), "collection_disk_bytes/doc")
		b.ReportMetric(float64(indexBytes), "index_disk_bytes")
		b.ReportMetric(float64(indexBytes)/float64(docs), "index_disk_bytes/doc")
	}
	benchmarkReportSQLiteVacuum(b, db, docs, totalBytes)
}

func benchmarkReportSQLiteVacuum(b *testing.B, db *sql.DB, docs int, beforeTotalBytes uint64) {
	b.Helper()

	if docs <= 0 || !benchmarkBoolEnv(b, "TREEDB_COLLECTION_REPORT_SQLITE_VACUUM", false) {
		return
	}
	vacuumStart := time.Now()
	if _, err := db.Exec(`VACUUM`); err != nil {
		b.Fatalf("sqlite vacuum: %v", err)
	}
	vacuumElapsed := time.Since(vacuumStart)
	checkpointSQLiteWAL(b, db)
	afterTotalBytes := benchmarkSQLiteMainDiskBytes(b, db)

	b.ReportMetric(float64(vacuumElapsed.Nanoseconds()), "sqlite_vacuum_ns/op")
	b.ReportMetric(float64(beforeTotalBytes), "sqlite_vacuum_disk_total_bytes_before")
	b.ReportMetric(float64(afterTotalBytes), "sqlite_vacuum_disk_total_bytes_after")
	b.ReportMetric(float64(int64(afterTotalBytes)-int64(beforeTotalBytes)), "sqlite_vacuum_disk_total_bytes_delta")
	b.ReportMetric(float64(afterTotalBytes)/float64(docs), "sqlite_vacuum_disk_bytes/doc_after")
	if collectionBytes, indexBytes, ok := benchmarkSQLiteObjectDiskUsage(db); ok {
		b.ReportMetric(float64(collectionBytes), "sqlite_vacuum_collection_disk_bytes_after")
		b.ReportMetric(float64(collectionBytes)/float64(docs), "sqlite_vacuum_collection_disk_bytes/doc_after")
		b.ReportMetric(float64(indexBytes), "sqlite_vacuum_index_disk_bytes_after")
		b.ReportMetric(float64(indexBytes)/float64(docs), "sqlite_vacuum_index_disk_bytes/doc_after")
	}
}

func benchmarkSQLiteMainDiskBytes(tb testing.TB, db *sql.DB) uint64 {
	tb.Helper()

	var pageSize uint64
	if err := db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		tb.Fatalf("sqlite page_size: %v", err)
	}
	var pageCount uint64
	if err := db.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		tb.Fatalf("sqlite page_count: %v", err)
	}
	return pageSize * pageCount
}

func benchmarkSQLiteObjectDiskUsage(db *sql.DB) (collectionBytes, indexBytes uint64, ok bool) {
	rows, err := db.Query(`SELECT name, SUM(pgsize) FROM dbstat GROUP BY name`)
	if err != nil {
		return 0, 0, false
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var bytes sql.NullInt64
		if err := rows.Scan(&name, &bytes); err != nil {
			return 0, 0, false
		}
		if !bytes.Valid || bytes.Int64 <= 0 {
			continue
		}
		switch {
		case name == "documents" || strings.HasPrefix(name, "sqlite_autoindex_documents_"):
			collectionBytes += uint64(bytes.Int64)
		case strings.HasPrefix(name, "documents_"):
			indexBytes += uint64(bytes.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, false
	}
	return collectionBytes, indexBytes, collectionBytes > 0 || indexBytes > 0
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
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ReportMetric(2, "indexes/doc")
	benchmarkReportSQLiteDiskUsage(b, db, b.N)
}

func BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteDB(b, "bench_insert_batch_checkpoint_secondary")
	targetBatchSize := benchmarkBatchSize(b)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	b.ReportAllocs()
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
	b.ReportMetric(float64(targetBatchSize), "target_docs/checkpoint")
	b.ReportMetric(2, "indexes/doc")
	benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	benchmarkReportSQLiteDiskUsage(b, db, b.N)
}

func BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteNativeColumnsDB(b, "bench_native_columns_insert_batch_secondary")
	targetBatchSize := benchmarkBatchSize(b)

	b.ReportAllocs()
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
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ReportMetric(2, "indexes/doc")
	benchmarkReportSQLiteDiskUsage(b, db, b.N)
}

func BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	db := openBenchmarkSQLiteNativeColumnsDB(b, "bench_native_columns_insert_batch_checkpoint_secondary")
	targetBatchSize := benchmarkBatchSize(b)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	b.ReportAllocs()
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
	b.ReportMetric(float64(targetBatchSize), "target_docs/checkpoint")
	b.ReportMetric(2, "indexes/doc")
	benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	benchmarkReportSQLiteDiskUsage(b, db, b.N)
}

func benchmarkSQLiteShapeInsertBatchJSON(b *testing.B, indexCount int, checkpoint bool) {
	db := openBenchmarkSQLiteJSONShapeDB(b, fmt.Sprintf("bench_shape_json_%d", indexCount), indexCount)
	targetBatchSize := benchmarkBatchSize(b)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkSQLiteDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		if checkpoint {
			insertStart := time.Now()
			insertSQLiteDocumentBatch(b, db, ids, docs)
			insertElapsed += time.Since(insertStart)
			syncStart := time.Now()
			checkpointSQLiteWAL(b, db)
			syncElapsed += time.Since(syncStart)
		} else {
			insertSQLiteDocumentBatch(b, db, ids, docs)
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ReportMetric(float64(targetBatchSize), metricName)
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportSQLiteDiskUsage(b, db, b.N)
}

func benchmarkSQLiteShapeInsertBatchNativeColumns(b *testing.B, indexCount int, checkpoint bool) {
	db := openBenchmarkSQLiteNativeColumnsShapeDB(b, fmt.Sprintf("bench_shape_native_%d", indexCount), indexCount)
	targetBatchSize := benchmarkBatchSize(b)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		docs := benchmarkSQLiteNativeColumnsDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		if checkpoint {
			insertStart := time.Now()
			insertSQLiteNativeColumnsDocumentBatch(b, db, docs)
			insertElapsed += time.Since(insertStart)
			syncStart := time.Now()
			checkpointSQLiteWAL(b, db)
			syncElapsed += time.Since(syncStart)
		} else {
			insertSQLiteNativeColumnsDocumentBatch(b, db, docs)
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ReportMetric(float64(targetBatchSize), metricName)
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportSQLiteDiskUsage(b, db, b.N)
}

func BenchmarkSQLiteShapeInsertBatchJSON(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkSQLiteShapeInsertBatchJSON(b, indexCount, false)
		})
	}
}

func BenchmarkSQLiteShapeInsertBatchCheckpointJSON(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkSQLiteShapeInsertBatchJSON(b, indexCount, true)
		})
	}
}

func BenchmarkSQLiteShapeInsertBatchNativeColumns(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkSQLiteShapeInsertBatchNativeColumns(b, indexCount, false)
		})
	}
}

func BenchmarkSQLiteShapeInsertBatchCheckpointNativeColumns(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkSQLiteShapeInsertBatchNativeColumns(b, indexCount, true)
		})
	}
}

func seedBenchmarkSQLiteJSON(b *testing.B, db *sql.DB, count int) []string {
	b.Helper()

	targetBatchSize := benchmarkBatchSize(b)
	allIDs := make([]string, 0, count)
	for inserted := 0; inserted < count; inserted += targetBatchSize {
		batchSize := targetBatchSize
		if remaining := count - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkSQLiteDocumentBatch(b, inserted, batchSize)
		insertSQLiteDocumentBatch(b, db, ids, docs)
		allIDs = append(allIDs, ids...)
	}
	return allIDs
}

func seedBenchmarkSQLiteNativeColumns(b *testing.B, db *sql.DB, count int) []benchmarkSQLiteNativeColumnsDocument {
	b.Helper()

	targetBatchSize := benchmarkBatchSize(b)
	allDocs := make([]benchmarkSQLiteNativeColumnsDocument, 0, count)
	for inserted := 0; inserted < count; inserted += targetBatchSize {
		batchSize := targetBatchSize
		if remaining := count - inserted; remaining < batchSize {
			batchSize = remaining
		}
		docs := benchmarkSQLiteNativeColumnsDocumentBatch(b, inserted, batchSize)
		insertSQLiteNativeColumnsDocumentBatch(b, db, docs)
		allDocs = append(allDocs, docs...)
	}
	return allDocs
}

func benchmarkSQLiteShapeReadPrimaryJSON(b *testing.B, indexCount int) {
	db := openBenchmarkSQLiteJSONShapeDB(b, fmt.Sprintf("bench_shape_read_json_%d", indexCount), indexCount)
	ids := seedBenchmarkSQLiteJSON(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	stmt, err := db.Prepare(`SELECT document FROM documents WHERE id = ?`)
	if err != nil {
		b.Fatalf("sqlite prepare primary JSON read: %v", err)
	}
	defer stmt.Close()

	var doc string
	b.ReportAllocs()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow(ids[i%len(ids)]).Scan(&doc); err != nil {
			b.Fatalf("sqlite primary JSON read: %v", err)
		}
	}
}

func benchmarkSQLiteShapeReadPrimaryNativeColumns(b *testing.B, indexCount int) {
	db := openBenchmarkSQLiteNativeColumnsShapeDB(b, fmt.Sprintf("bench_shape_read_native_%d", indexCount), indexCount)
	docs := seedBenchmarkSQLiteNativeColumns(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	stmt, err := db.Prepare(`SELECT name, email, city, pad FROM documents WHERE id = ?`)
	if err != nil {
		b.Fatalf("sqlite prepare primary native read: %v", err)
	}
	defer stmt.Close()

	var name, email, city, pad string
	b.ReportAllocs()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow(docs[i%len(docs)].id).Scan(&name, &email, &city, &pad); err != nil {
			b.Fatalf("sqlite primary native read: %v", err)
		}
	}
}

func BenchmarkSQLiteShapeReadPrimaryJSON(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkSQLiteShapeReadPrimaryJSON(b, indexCount)
		})
	}
}

func BenchmarkSQLiteShapeReadPrimaryNativeColumns(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkSQLiteShapeReadPrimaryNativeColumns(b, indexCount)
		})
	}
}

func benchmarkSQLiteShapeSecondaryLookupJSON(b *testing.B, unique bool) {
	db := openBenchmarkSQLiteJSONShapeDB(b, "bench_shape_secondary_json", 2)
	seedBenchmarkSQLiteJSON(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	query := `SELECT id FROM documents WHERE email = ?`
	if !unique {
		query = `SELECT id FROM documents WHERE city = ?`
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		b.Fatalf("sqlite prepare secondary JSON lookup: %v", err)
	}
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if unique {
			email := benchmarkSQLiteUserEmail(i % collectionBenchSeedDocs)
			var id string
			if err := stmt.QueryRow(email).Scan(&id); err != nil {
				b.Fatalf("sqlite unique JSON lookup: %v", err)
			}
			continue
		}
		city := benchmarkSQLiteCity(i)
		rows, err := stmt.Query(city)
		if err != nil {
			b.Fatalf("sqlite nonunique JSON lookup: %v", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				b.Fatalf("sqlite nonunique JSON scan: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatalf("sqlite nonunique JSON rows: %v", err)
		}
		if err := rows.Close(); err != nil {
			b.Fatalf("sqlite nonunique JSON close: %v", err)
		}
	}
}

func benchmarkSQLiteShapeSecondaryLookupNativeColumns(b *testing.B, unique bool) {
	db := openBenchmarkSQLiteNativeColumnsShapeDB(b, "bench_shape_secondary_native", 2)
	seedBenchmarkSQLiteNativeColumns(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	query := `SELECT id FROM documents WHERE email = ?`
	if !unique {
		query = `SELECT id FROM documents WHERE city = ?`
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		b.Fatalf("sqlite prepare secondary native lookup: %v", err)
	}
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if unique {
			email := benchmarkSQLiteUserEmail(i % collectionBenchSeedDocs)
			var id string
			if err := stmt.QueryRow(email).Scan(&id); err != nil {
				b.Fatalf("sqlite unique native lookup: %v", err)
			}
			continue
		}
		city := benchmarkSQLiteCity(i)
		rows, err := stmt.Query(city)
		if err != nil {
			b.Fatalf("sqlite nonunique native lookup: %v", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				b.Fatalf("sqlite nonunique native scan: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatalf("sqlite nonunique native rows: %v", err)
		}
		if err := rows.Close(); err != nil {
			b.Fatalf("sqlite nonunique native close: %v", err)
		}
	}
}

func benchmarkSQLiteShapeSecondaryRangeJSON(b *testing.B) {
	db := openBenchmarkSQLiteJSONShapeDB(b, "bench_shape_secondary_range_json", 2)
	seedBenchmarkSQLiteJSON(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	stmt, err := db.Prepare(`SELECT id FROM documents WHERE city >= ? AND city <= ?`)
	if err != nil {
		b.Fatalf("sqlite prepare secondary JSON range: %v", err)
	}
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		city := benchmarkSQLiteCity(i)
		rows, err := stmt.Query(city, city)
		if err != nil {
			b.Fatalf("sqlite secondary JSON range: %v", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				b.Fatalf("sqlite secondary JSON range scan: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatalf("sqlite secondary JSON range rows: %v", err)
		}
		if err := rows.Close(); err != nil {
			b.Fatalf("sqlite secondary JSON range close: %v", err)
		}
	}
}

func benchmarkSQLiteShapeSecondaryRangeNativeColumns(b *testing.B) {
	db := openBenchmarkSQLiteNativeColumnsShapeDB(b, "bench_shape_secondary_range_native", 2)
	seedBenchmarkSQLiteNativeColumns(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	stmt, err := db.Prepare(`SELECT id FROM documents WHERE city >= ? AND city <= ?`)
	if err != nil {
		b.Fatalf("sqlite prepare secondary native range: %v", err)
	}
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		city := benchmarkSQLiteCity(i)
		rows, err := stmt.Query(city, city)
		if err != nil {
			b.Fatalf("sqlite secondary native range: %v", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				b.Fatalf("sqlite secondary native range scan: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatalf("sqlite secondary native range rows: %v", err)
		}
		if err := rows.Close(); err != nil {
			b.Fatalf("sqlite secondary native range close: %v", err)
		}
	}
}

func BenchmarkSQLiteShapeSecondaryLookupJSON(b *testing.B) {
	b.Run("unique", func(b *testing.B) {
		benchmarkSQLiteShapeSecondaryLookupJSON(b, true)
	})
	b.Run("non_unique", func(b *testing.B) {
		benchmarkSQLiteShapeSecondaryLookupJSON(b, false)
	})
}

func BenchmarkSQLiteShapeSecondaryLookupNativeColumns(b *testing.B) {
	b.Run("unique", func(b *testing.B) {
		benchmarkSQLiteShapeSecondaryLookupNativeColumns(b, true)
	})
	b.Run("non_unique", func(b *testing.B) {
		benchmarkSQLiteShapeSecondaryLookupNativeColumns(b, false)
	})
}

func BenchmarkSQLiteShapeSecondaryRangeJSON(b *testing.B) {
	benchmarkSQLiteShapeSecondaryRangeJSON(b)
}

func BenchmarkSQLiteShapeSecondaryRangeNativeColumns(b *testing.B) {
	benchmarkSQLiteShapeSecondaryRangeNativeColumns(b)
}
