//go:build sqlite_bench && cgo

package collections_test

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sqliteConcurrencyConnCase struct {
	name  string
	conns int
}

func sqliteConcurrencyConnCases() []sqliteConcurrencyConnCase {
	gomax := runtime.GOMAXPROCS(0)
	if gomax <= 0 {
		gomax = 1
	}
	cases := []sqliteConcurrencyConnCase{{name: "conns_1", conns: 1}}
	if gomax != 1 {
		cases = append(cases, sqliteConcurrencyConnCase{name: "conns_gomaxprocs", conns: gomax})
	}
	return cases
}

func configureBenchmarkSQLiteConcurrency(db *sql.DB, conns int) {
	if conns <= 0 {
		conns = 1
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
}

func reportSQLiteConcurrency(b *testing.B, conns int) {
	b.Helper()
	b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
	b.ReportMetric(float64(conns), "sqlite_max_open_conns")
	b.ReportMetric(2, "indexes/doc")
}

func BenchmarkSQLiteConcurrencyReadPrimaryJSONParallel(b *testing.B) {
	for _, cc := range sqliteConcurrencyConnCases() {
		b.Run(cc.name, func(b *testing.B) {
			db := openBenchmarkSQLiteJSONShapeDB(b, fmt.Sprintf("bench_sqlite_concurrency_read_json_%s", cc.name), 2)
			configureBenchmarkSQLiteConcurrency(db, cc.conns)
			ids := seedBenchmarkSQLiteJSON(b, db, collectionBenchSeedDocs)
			checkpointSQLiteWAL(b, db)

			b.ReportAllocs()
			b.ResetTimer()
			reportSQLiteConcurrency(b, cc.conns)
			b.RunParallel(func(pb *testing.PB) {
				stmt, err := db.Prepare(`SELECT document FROM documents WHERE id = ?`)
				if err != nil {
					b.Errorf("sqlite prepare concurrent JSON primary read: %v", err)
					return
				}
				defer stmt.Close()
				i := 0
				stride := runtime.GOMAXPROCS(0)
				if stride <= 0 {
					stride = 1
				}
				var doc string
				for pb.Next() {
					if err := stmt.QueryRow(ids[i%len(ids)]).Scan(&doc); err != nil {
						b.Errorf("sqlite concurrent JSON primary read: %v", err)
					}
					i += stride
				}
			})
		})
	}
}

func BenchmarkSQLiteConcurrencyReadPrimaryNativeColumnsParallel(b *testing.B) {
	for _, cc := range sqliteConcurrencyConnCases() {
		b.Run(cc.name, func(b *testing.B) {
			db := openBenchmarkSQLiteNativeColumnsShapeDB(b, fmt.Sprintf("bench_sqlite_concurrency_read_native_%s", cc.name), 2)
			configureBenchmarkSQLiteConcurrency(db, cc.conns)
			docs := seedBenchmarkSQLiteNativeColumns(b, db, collectionBenchSeedDocs)
			checkpointSQLiteWAL(b, db)

			b.ReportAllocs()
			b.ResetTimer()
			reportSQLiteConcurrency(b, cc.conns)
			b.RunParallel(func(pb *testing.PB) {
				stmt, err := db.Prepare(`SELECT name, email, city, pad FROM documents WHERE id = ?`)
				if err != nil {
					b.Errorf("sqlite prepare concurrent native primary read: %v", err)
					return
				}
				defer stmt.Close()
				i := 0
				stride := runtime.GOMAXPROCS(0)
				if stride <= 0 {
					stride = 1
				}
				var name, email, city, pad string
				for pb.Next() {
					if err := stmt.QueryRow(docs[i%len(docs)].id).Scan(&name, &email, &city, &pad); err != nil {
						b.Errorf("sqlite concurrent native primary read: %v", err)
					}
					i += stride
				}
			})
		})
	}
}

func BenchmarkSQLiteConcurrencySecondaryLookupJSONParallel(b *testing.B) {
	for _, cc := range sqliteConcurrencyConnCases() {
		b.Run(cc.name, func(b *testing.B) {
			b.Run("unique", func(b *testing.B) {
				benchmarkSQLiteConcurrencySecondaryLookupJSON(b, cc.conns, true)
			})
			b.Run("non_unique", func(b *testing.B) {
				benchmarkSQLiteConcurrencySecondaryLookupJSON(b, cc.conns, false)
			})
		})
	}
}

func benchmarkSQLiteConcurrencySecondaryLookupJSON(b *testing.B, conns int, unique bool) {
	db := openBenchmarkSQLiteJSONShapeDB(b, fmt.Sprintf("bench_sqlite_concurrency_secondary_json_c%d", conns), 2)
	configureBenchmarkSQLiteConcurrency(db, conns)
	seedBenchmarkSQLiteJSON(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	query := `SELECT id FROM documents WHERE email = ?`
	if !unique {
		query = `SELECT id FROM documents WHERE city = ?`
	}

	b.ReportAllocs()
	b.ResetTimer()
	reportSQLiteConcurrency(b, conns)
	b.RunParallel(func(pb *testing.PB) {
		stmt, err := db.Prepare(query)
		if err != nil {
			b.Errorf("sqlite prepare concurrent JSON secondary lookup: %v", err)
			return
		}
		defer stmt.Close()
		i := 0
		stride := runtime.GOMAXPROCS(0)
		if stride <= 0 {
			stride = 1
		}
		for pb.Next() {
			if unique {
				email := benchmarkSQLiteUserEmail(i % collectionBenchSeedDocs)
				var id string
				if err := stmt.QueryRow(email).Scan(&id); err != nil {
					b.Errorf("sqlite concurrent unique JSON lookup: %v", err)
				}
			} else {
				city := benchmarkSQLiteCity(i)
				rows, err := stmt.Query(city)
				if err != nil {
					b.Errorf("sqlite concurrent nonunique JSON lookup: %v", err)
					return
				}
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						b.Errorf("sqlite concurrent nonunique JSON scan: %v", err)
						return
					}
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					b.Errorf("sqlite concurrent nonunique JSON rows: %v", err)
					return
				}
				if err := rows.Close(); err != nil {
					b.Errorf("sqlite concurrent nonunique JSON close: %v", err)
					return
				}
			}
			i += stride
		}
	})
}

func BenchmarkSQLiteConcurrencySecondaryLookupNativeColumnsParallel(b *testing.B) {
	for _, cc := range sqliteConcurrencyConnCases() {
		b.Run(cc.name, func(b *testing.B) {
			b.Run("unique", func(b *testing.B) {
				benchmarkSQLiteConcurrencySecondaryLookupNativeColumns(b, cc.conns, true)
			})
			b.Run("non_unique", func(b *testing.B) {
				benchmarkSQLiteConcurrencySecondaryLookupNativeColumns(b, cc.conns, false)
			})
		})
	}
}

func benchmarkSQLiteConcurrencySecondaryLookupNativeColumns(b *testing.B, conns int, unique bool) {
	db := openBenchmarkSQLiteNativeColumnsShapeDB(b, fmt.Sprintf("bench_sqlite_concurrency_secondary_native_c%d", conns), 2)
	configureBenchmarkSQLiteConcurrency(db, conns)
	seedBenchmarkSQLiteNativeColumns(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	query := `SELECT id FROM documents WHERE email = ?`
	if !unique {
		query = `SELECT id FROM documents WHERE city = ?`
	}

	b.ReportAllocs()
	b.ResetTimer()
	reportSQLiteConcurrency(b, conns)
	b.RunParallel(func(pb *testing.PB) {
		stmt, err := db.Prepare(query)
		if err != nil {
			b.Errorf("sqlite prepare concurrent native secondary lookup: %v", err)
			return
		}
		defer stmt.Close()
		i := 0
		stride := runtime.GOMAXPROCS(0)
		if stride <= 0 {
			stride = 1
		}
		for pb.Next() {
			if unique {
				email := benchmarkSQLiteUserEmail(i % collectionBenchSeedDocs)
				var id string
				if err := stmt.QueryRow(email).Scan(&id); err != nil {
					b.Errorf("sqlite concurrent unique native lookup: %v", err)
				}
			} else {
				city := benchmarkSQLiteCity(i)
				rows, err := stmt.Query(city)
				if err != nil {
					b.Errorf("sqlite concurrent nonunique native lookup: %v", err)
					return
				}
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						b.Errorf("sqlite concurrent nonunique native scan: %v", err)
						return
					}
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					b.Errorf("sqlite concurrent nonunique native rows: %v", err)
					return
				}
				if err := rows.Close(); err != nil {
					b.Errorf("sqlite concurrent nonunique native close: %v", err)
					return
				}
			}
			i += stride
		}
	})
}

func sqliteConcurrencyJSONDocumentBatch(start, count int) ([]string, []string) {
	ids := make([]string, count)
	docs := make([]string, count)
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = string(benchmarkDocumentID(docNum))
		docs[i] = string(benchmarkIndexedDocument(docNum))
	}
	return ids, docs
}

func sqliteConcurrencyNativeColumnsDocumentBatch(start, count int) []benchmarkSQLiteNativeColumnsDocument {
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

func insertSQLiteDocumentBatchErr(ctx context.Context, db *sql.DB, ids, docs []string) error {
	if len(ids) != len(docs) {
		return fmt.Errorf("sqlite batch length mismatch ids=%d docs=%d", len(ids), len(docs))
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, sqliteInsertIndexedDocumentSQL)
	if err != nil {
		return fmt.Errorf("sqlite prepare insert: %w", err)
	}
	defer stmt.Close()

	for i := range ids {
		if _, err := stmt.ExecContext(ctx, ids[i], docs[i]); err != nil {
			return fmt.Errorf("sqlite insert %d: %w", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite commit: %w", err)
	}
	committed = true
	return nil
}

func insertSQLiteNativeColumnsDocumentBatchErr(ctx context.Context, db *sql.DB, docs []benchmarkSQLiteNativeColumnsDocument) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite native-columns begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, sqliteInsertNativeColumnsIndexedDocumentSQL)
	if err != nil {
		return fmt.Errorf("sqlite native-columns prepare insert: %w", err)
	}
	defer stmt.Close()

	for i := range docs {
		doc := docs[i]
		if _, err := stmt.ExecContext(ctx, doc.id, doc.name, doc.email, doc.city, doc.pad); err != nil {
			return fmt.Errorf("sqlite native-columns insert %d: %w", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite native-columns commit: %w", err)
	}
	committed = true
	return nil
}

type sqliteMixedConcurrencyCase struct {
	readers int
	writers int
}

func BenchmarkSQLiteConcurrencyMixedReadWriteJSON(b *testing.B) {
	for _, tc := range []sqliteMixedConcurrencyCase{{readers: 1, writers: 1}, {readers: 4, writers: 1}, {readers: 8, writers: 2}} {
		b.Run(fmt.Sprintf("readers_%d/writers_%d", tc.readers, tc.writers), func(b *testing.B) {
			benchmarkSQLiteConcurrencyMixedReadWriteJSON(b, tc.readers, tc.writers)
		})
	}
}

func benchmarkSQLiteConcurrencyMixedReadWriteJSON(b *testing.B, readers, writers int) {
	if readers <= 0 {
		readers = 1
	}
	if writers <= 0 {
		writers = 1
	}
	db := openBenchmarkSQLiteJSONShapeDB(b, fmt.Sprintf("bench_sqlite_mixed_json_r%d_w%d", readers, writers), 2)
	conns := readers + writers + 1
	configureBenchmarkSQLiteConcurrency(db, conns)
	ids := seedBenchmarkSQLiteJSON(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	benchmarkSQLiteConcurrencyMixedReadWrite(b, db, readers, writers, conns, ids, nil)
}

func BenchmarkSQLiteConcurrencyMixedReadWriteNativeColumns(b *testing.B) {
	for _, tc := range []sqliteMixedConcurrencyCase{{readers: 1, writers: 1}, {readers: 4, writers: 1}, {readers: 8, writers: 2}} {
		b.Run(fmt.Sprintf("readers_%d/writers_%d", tc.readers, tc.writers), func(b *testing.B) {
			benchmarkSQLiteConcurrencyMixedReadWriteNativeColumns(b, tc.readers, tc.writers)
		})
	}
}

func benchmarkSQLiteConcurrencyMixedReadWriteNativeColumns(b *testing.B, readers, writers int) {
	if readers <= 0 {
		readers = 1
	}
	if writers <= 0 {
		writers = 1
	}
	db := openBenchmarkSQLiteNativeColumnsShapeDB(b, fmt.Sprintf("bench_sqlite_mixed_native_r%d_w%d", readers, writers), 2)
	conns := readers + writers + 1
	configureBenchmarkSQLiteConcurrency(db, conns)
	docs := seedBenchmarkSQLiteNativeColumns(b, db, collectionBenchSeedDocs)
	checkpointSQLiteWAL(b, db)
	ids := make([]string, len(docs))
	for i := range docs {
		ids[i] = docs[i].id
	}
	benchmarkSQLiteConcurrencyMixedReadWrite(b, db, readers, writers, conns, ids, docs)
}

func benchmarkSQLiteConcurrencyMixedReadWrite(b *testing.B, db *sql.DB, readers, writers, conns int, ids []string, nativeDocs []benchmarkSQLiteNativeColumnsDocument) {
	ctx := context.Background()
	writeBatchSize := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_WRITE_BATCH_SIZE", defaultCollectionMixedWriteBatchSize)
	if writeBatchSize <= 0 {
		writeBatchSize = defaultCollectionMixedWriteBatchSize
	}
	if maxBatch := benchmarkBatchSize(b); writeBatchSize > maxBatch {
		writeBatchSize = maxBatch
	}

	var stop atomic.Bool
	var writerDocs atomic.Uint64
	errCh := make(chan error, readers+writers)
	startCh := make(chan struct{})
	var readerWG sync.WaitGroup
	var writerWG sync.WaitGroup

	for writerID := 0; writerID < writers; writerID++ {
		writerID := writerID
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			<-startCh
			for next := 1_000_000 + writerID*100_000_000; !stop.Load(); next += writeBatchSize {
				var err error
				if nativeDocs == nil {
					batchIDs, docs := sqliteConcurrencyJSONDocumentBatch(next, writeBatchSize)
					err = insertSQLiteDocumentBatchErr(ctx, db, batchIDs, docs)
				} else {
					docs := sqliteConcurrencyNativeColumnsDocumentBatch(next, writeBatchSize)
					err = insertSQLiteNativeColumnsDocumentBatchErr(ctx, db, docs)
				}
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				writerDocs.Add(uint64(writeBatchSize))
			}
		}()
	}

	readBase := b.N / readers
	readRemainder := b.N % readers
	for readerID := 0; readerID < readers; readerID++ {
		readerID := readerID
		readOps := readBase
		if readerID < readRemainder {
			readOps++
		}
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			stmt, err := db.PrepareContext(ctx, `SELECT id FROM documents WHERE id = ?`)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			defer stmt.Close()
			<-startCh
			stride := runtime.GOMAXPROCS(0)
			if stride <= 0 {
				stride = 1
			}
			i := (readerID * max(1, len(ids)/readers)) % len(ids)
			for op := 0; op < readOps; op++ {
				var id string
				if err := stmt.QueryRowContext(ctx, ids[i%len(ids)]).Scan(&id); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				i += stride
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(readers), "readers")
	b.ReportMetric(float64(writers), "writers")
	b.ReportMetric(float64(conns), "sqlite_max_open_conns")
	b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
	b.ReportMetric(float64(writeBatchSize), "writer_docs/batch")
	start := time.Now()
	close(startCh)

	readerWG.Wait()
	readerElapsed := time.Since(start)
	b.StopTimer()
	stop.Store(true)
	writerWG.Wait()
	select {
	case err := <-errCh:
		b.Fatalf("sqlite mixed concurrency benchmark: %v", err)
	default:
	}
	if readerElapsed > 0 {
		b.ReportMetric(float64(b.N)/readerElapsed.Seconds(), "reader_ops/sec")
	}
	writerElapsed := time.Since(start)
	if writerElapsed > 0 {
		b.ReportMetric(float64(writerDocs.Load())/writerElapsed.Seconds(), "writer_docs/sec")
	}
}
