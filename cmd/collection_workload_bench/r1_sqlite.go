package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/snissn/gomap/TreeDB/collections"
)

type r1SQLite struct {
	dir, engine string
	db          *sql.DB
}

func openR1SQLite(c r1Config, engine string) (*r1SQLite, error) {
	dir, e := os.MkdirTemp("", "gomap-r1-sqlite-")
	if e != nil {
		return nil, e
	}
	s := &r1SQLite{dir: dir, engine: engine}
	syncMode := "FULL"
	if c.Durability == "relaxed" {
		syncMode = "NORMAL"
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.Join(dir, "rows.db")}).String() + "?_journal_mode=WAL&_synchronous=" + syncMode + "&_busy_timeout=5000"
	s.db, e = sql.Open("sqlite3", dsn)
	if e != nil {
		_ = os.RemoveAll(dir)
		return nil, e
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	for _, q := range []string{"PRAGMA wal_autocheckpoint=0", "PRAGMA cache_size=-65536", "PRAGMA mmap_size=0"} {
		if _, e = s.db.Exec(q); e != nil {
			_ = s.close()
			return nil, e
		}
	}
	var journal string
	var synchronous int
	if e = s.db.QueryRow("PRAGMA journal_mode").Scan(&journal); e == nil {
		e = s.db.QueryRow("PRAGMA synchronous").Scan(&synchronous)
	}
	want := 2
	if c.Durability == "relaxed" {
		want = 1
	}
	if e != nil || journal != "wal" || synchronous != want {
		_ = s.close()
		return nil, fmt.Errorf("SQLite acknowledgement mismatch: journal=%s synchronous=%d: %w", journal, synchronous, e)
	}
	q := `CREATE TABLE rows(id TEXT PRIMARY KEY, document TEXT NOT NULL, email TEXT GENERATED ALWAYS AS (json_extract(document,'$.email')) STORED, city TEXT GENERATED ALWAYS AS (json_extract(document,'$.city')) STORED) WITHOUT ROWID`
	if engine == "sqlite-row" {
		q = `CREATE TABLE rows(id TEXT PRIMARY KEY,email TEXT NOT NULL,city TEXT NOT NULL,name TEXT NOT NULL,bio TEXT NOT NULL,residual TEXT NOT NULL) WITHOUT ROWID`
	}
	for _, stmt := range []string{q, "CREATE UNIQUE INDEX email ON rows(email)", "CREATE INDEX city ON rows(city)"} {
		if _, e = s.db.Exec(stmt); e != nil {
			_ = s.close()
			return nil, e
		}
	}
	return s, nil
}
func (s *r1SQLite) write(rows []r1Document, mode string) (err error) {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	q := "INSERT INTO rows(id,document) VALUES (?,?)"
	if s.engine == "sqlite-row" {
		q = "INSERT INTO rows(id,email,city,name,bio,residual) VALUES (?,?,?,?,?,?)"
	}
	if mode == "replace" {
		q = "UPDATE rows SET document=? WHERE id=?"
		if s.engine == "sqlite-row" {
			q = "UPDATE rows SET email=?,city=?,name=?,bio=?,residual=? WHERE id=?"
		}
	}
	if mode == "upsert" {
		q += " ON CONFLICT(id) DO UPDATE SET document=excluded.document"
		if s.engine == "sqlite-row" {
			q = "INSERT INTO rows(id,email,city,name,bio,residual) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET email=excluded.email,city=excluded.city,name=excluded.name,bio=excluded.bio,residual=excluded.residual"
		}
	}
	stmt, e := tx.Prepare(q)
	if e != nil {
		return e
	}
	defer stmt.Close()
	for _, d := range rows {
		raw, e := json.Marshal(d)
		if e != nil {
			return e
		}
		args := []any{d.ID, string(raw)}
		if mode == "replace" {
			args = []any{string(raw), d.ID}
		}
		if s.engine == "sqlite-row" {
			var obj map[string]any
			if e = json.Unmarshal(raw, &obj); e != nil {
				return e
			}
			for _, key := range []string{"email", "city", "name", "bio"} {
				delete(obj, key)
			}
			raw, e = json.Marshal(obj)
			if e != nil {
				return e
			}
			args = []any{d.ID, d.Email, d.City, d.Name, d.Bio, string(raw)}
			if mode == "replace" {
				args = []any{d.Email, d.City, d.Name, d.Bio, string(raw), d.ID}
			}
		}
		result, e := stmt.Exec(args...)
		if e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e != nil || n != 1 {
			return fmt.Errorf("SQLite write matched %d: %w", n, e)
		}
	}
	return tx.Commit()
}
func (s *r1SQLite) insert(rows []r1Document) error  { return s.write(rows, "insert") }
func (s *r1SQLite) replace(rows []r1Document) error { return s.write(rows, "replace") }
func (s *r1SQLite) upsert(rows []r1Document) error  { return s.write(rows, "upsert") }
func (s *r1SQLite) supportsUpsert() bool            { return true }
func (s *r1SQLite) delete(ids [][]byte) (err error) {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, id := range ids {
		if _, e = tx.Exec("DELETE FROM rows WHERE id=?", string(id)); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *r1SQLite) transition(state string) error {
	if state != "checkpointed" {
		return nil
	}
	var busy, log, checkpointed int
	if e := s.db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &checkpointed); e != nil {
		return e
	}
	if busy != 0 {
		return errors.New("SQLite checkpoint busy")
	}
	return nil
}
func (s *r1SQLite) rangeIDs(city string, limit int) ([][]byte, error) {
	rows, e := s.db.Query("SELECT id FROM rows WHERE city=? ORDER BY city,id LIMIT ?", city, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			return nil, e
		}
		out = append(out, []byte(id))
	}
	return out, rows.Err()
}
func (s *r1SQLite) storage() (int64, int64, error) { return r1Storage(s.dir) }
func (s *r1SQLite) stats() map[string]string {
	var version string
	_ = s.db.QueryRow("SELECT sqlite_version()").Scan(&version)
	return map[string]string{"sqlite.version": version, "sqlite.journal_mode": "wal", "sqlite.cache_bytes": "67108864", "sqlite.mmap_bytes": "0"}
}
func (s *r1SQLite) close() error { e := s.db.Close(); return errors.Join(e, os.RemoveAll(s.dir)) }

// The comparison is single-writer/quiescent. Opening a SQLite reader prepares
// its complete-row query, matching the separately reported TreeDB view setup.
// It deliberately does not hold the sole connection while rangeIDs runs.
type r1SQLiteReader struct {
	db         *sql.DB
	engine     string
	statements map[int]*sql.Stmt
}

func (s *r1SQLite) openReader() (r1Reader, error) {
	r := &r1SQLiteReader{db: s.db, engine: s.engine, statements: map[int]*sql.Stmt{}}
	_, e := r.statement(1)
	return r, e
}
func (r *r1SQLiteReader) statement(n int) (*sql.Stmt, error) {
	if stmt := r.statements[n]; stmt != nil {
		return stmt, nil
	}
	projection := "id,document"
	if r.engine == "sqlite-row" {
		projection = "id,email,city,name,bio,residual"
	}
	q := "SELECT " + projection + " FROM rows WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?,", n), ",") + ")"
	stmt, e := r.db.Prepare(q)
	if e == nil {
		r.statements[n] = stmt
	}
	return stmt, e
}
func (r *r1SQLiteReader) fetch(ids [][]byte) ([][]byte, collections.DocumentMaterializationStats, error) {
	out := make([][]byte, len(ids))
	if len(ids) == 0 {
		return out, collections.DocumentMaterializationStats{}, nil
	}
	stmt, e := r.statement(len(ids))
	if e != nil {
		return nil, collections.DocumentMaterializationStats{}, e
	}
	args := make([]any, len(ids))
	positions := map[string][]int{}
	for i, id := range ids {
		args[i] = string(id)
		positions[string(id)] = append(positions[string(id)], i)
	}
	rows, e := stmt.Query(args...)
	if e != nil {
		return nil, collections.DocumentMaterializationStats{}, e
	}
	defer rows.Close()
	for rows.Next() {
		id, doc, e := r1ScanSQLRow(r.engine, rows)
		if e != nil {
			return nil, collections.DocumentMaterializationStats{}, e
		}
		for _, i := range positions[id] {
			out[i] = bytes.Clone(doc)
		}
	}
	if e = rows.Err(); e != nil {
		return nil, collections.DocumentMaterializationStats{}, e
	}

	return out, collections.DocumentMaterializationStats{}, nil
}
func (r *r1SQLiteReader) close() error {
	var err error
	for _, stmt := range r.statements {
		err = errors.Join(err, stmt.Close())
	}
	return err
}

func (s *r1SQLite) update(rows []r1Document) error { return s.replace(rows) }
func (s *r1SQLite) point(id []byte) ([]byte, error) {
	r, e := s.openReader()
	if e != nil {
		return nil, e
	}
	docs, _, e := r.fetch([][]byte{id})
	e = errors.Join(e, r.close())
	if e != nil {
		return nil, e
	}
	return docs[0], nil
}
func (s *r1SQLite) capabilities([]r1Document) (map[string]string, error) {
	return map[string]string{"range_decomposition": "quiescent_ids_then_prepared_full_fetch", "ordinary_point": "owned_complete_SQL_select", "ordinary_range": "complete_SQL_row"}, nil
}

func r1ScanSQLRow(engine string, rows *sql.Rows) (string, []byte, error) {
	var id, doc string
	var e error
	var d r1Document
	if engine == "sqlite-row" {
		var residual string
		e = rows.Scan(&id, &d.Email, &d.City, &d.Name, &d.Bio, &residual)
		if e == nil {
			var obj map[string]json.RawMessage
			e = json.Unmarshal([]byte(residual), &obj)
			if e == nil {
				raw, _ := json.Marshal(d)
				var typed map[string]json.RawMessage
				_ = json.Unmarshal(raw, &typed)
				for _, name := range []string{"email", "city", "name", "bio"} {
					obj[name] = typed[name]
				}
				raw, e = json.Marshal(obj)
				doc = string(raw)
			}
		}
	} else {
		e = rows.Scan(&id, &doc)
	}
	if e != nil {
		return "", nil, e
	}
	return id, []byte(doc), nil
}
func (s *r1SQLite) rangeDocuments(city string, limit int) ([][]byte, error) {
	projection := "id,document"
	if s.engine == "sqlite-row" {
		projection = "id,email,city,name,bio,residual"
	}
	rows, e := s.db.Query("SELECT "+projection+" FROM rows WHERE city=? ORDER BY city,id LIMIT ?", city, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		_, doc, e := r1ScanSQLRow(s.engine, rows)
		if e != nil {
			return nil, e
		}
		out = append(out, doc)
	}
	return out, rows.Err()
}
