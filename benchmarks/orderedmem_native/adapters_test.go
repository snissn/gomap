package orderedmemnative

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// These adapters deliberately use the maintained public MVCC/managed-DB calls
// from benchmarks/dgraph_durability. They do not call experimental helpers.
type record struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	State     mvcc.ReadState
}

type historyCursor interface {
	Valid() bool
	Entry() (record, error)
	Next()
	Error() error
	Close() error
}

type publicEngine interface {
	Commit(uint64, []mvcc.Mutation) error
	Get([]byte, uint64) (record, error)
	Range([]byte, []byte, uint64, int) ([]record, error)
	History(uint64) (historyCursor, error)
	Persist() error
	Stats() map[string]string
	Close() error
}

type route struct {
	Engine                string            `json:"engine"`
	ResolvedProfile       string            `json:"resolved_profile"`
	RequestedMemtableMode string            `json:"requested_memtable_mode"`
	MemtableMode          string            `json:"memtable_mode,omitempty"`
	MemtableModeConfig    string            `json:"memtable_mode_config,omitempty"`
	LogicalMVCC           bool              `json:"logical_mvcc"`
	Acknowledgment        string            `json:"acknowledgment"`
	PersistenceBarrier    string            `json:"persistence_barrier"`
	Options               map[string]string `json:"options"`
}

func openEngine(c config) (publicEngine, route, error) {
	if c.Engine == "badger" {
		if c.Profile == "no_wal_fast" {
			return nil, route{}, errors.New("disk-backed Badger has no matched no_wal_fast ordinary acknowledgment")
		}
		syncWrites := c.Profile == "command_wal_durable"
		opts := badger.DefaultOptions(c.Dir).WithLogger(nil).WithSyncWrites(syncWrites)
		if c.BadgerMemtableSize > 0 {
			opts.MemTableSize = c.BadgerMemtableSize
		}
		if c.BadgerValueThreshold > 0 {
			opts.ValueThreshold = c.BadgerValueThreshold
		}
		opts.NumVersionsToKeep, opts.DetectConflicts = math.MaxInt32, false
		db, err := badger.OpenManaged(opts)
		if err != nil {
			return nil, route{}, err
		}
		r := route{Engine: "badger", ResolvedProfile: "managed", Acknowledgment: c.Profile,
			PersistenceBarrier: "Badger.Sync (persistence, not a TreeDB root-checkpoint equivalent)",
			Options: map[string]string{"sync_writes": strconv.FormatBool(syncWrites), "detect_conflicts": "false",
				"versions_to_keep": strconv.Itoa(math.MaxInt32), "in_memory": "false", "iterator_prefetch_values": "true",
				"memtable_size": strconv.FormatInt(opts.MemTableSize, 10), "value_threshold": strconv.FormatInt(opts.ValueThreshold, 10),
				"base_table_size": strconv.FormatInt(opts.BaseTableSize, 10), "num_memtables": strconv.Itoa(opts.NumMemtables),
				"metrics_enabled": strconv.FormatBool(opts.MetricsEnabled)}}
		return &badgerEngine{db: db}, r, nil
	}
	profile := treedb.Profile(c.Profile)
	opts := treedb.OptionsFor(profile, c.Dir)
	opts.MemtableMode = c.MemtableMode
	if c.FlushThreshold > 0 {
		opts.FlushThreshold = c.FlushThreshold
	}
	if c.PointerThreshold > 0 {
		opts.ValueLog.PointerThreshold = c.PointerThreshold
	}
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, route{}, err
	}
	stats := db.Stats()
	r := route{Engine: "treedb", ResolvedProfile: string(db.ResolvedProfile()),
		RequestedMemtableMode: c.MemtableMode, MemtableMode: stats["treedb.cache.memtable_mode"],
		MemtableModeConfig: stats["treedb.cache.memtable_mode_config"], Acknowledgment: c.Profile,
		PersistenceBarrier: "TreeDB.Checkpoint (sealed complete root covering captured frontier)",
		Options: map[string]string{"flush_threshold": strconv.FormatInt(opts.FlushThreshold, 10),
			"pointer_threshold": strconv.Itoa(opts.ValueLog.PointerThreshold), "iterator_debug": "false",
			"memtable_shards": strconv.Itoa(opts.MemtableShards)}}
	if capability, ok := any(db).(interface{ SupportsLogicalMVCC() bool }); ok {
		r.LogicalMVCC = capability.SupportsLogicalMVCC()
	}
	if err := validateTreeRoute(c, r); err != nil {
		_ = db.Close()
		return nil, r, err
	}
	mode := mvcc.CommitRelaxed
	if c.Profile == "command_wal_durable" {
		mode = mvcc.CommitDurable
	}
	return &treeEngine{db: db, store: mvcc.New(db), mode: mode}, r, nil
}

func validateTreeRoute(c config, r route) error {
	validMode := r.MemtableMode == c.MemtableMode && r.MemtableModeConfig == "fixed"
	if c.MemtableMode == "adaptive" {
		// This exact selector starts append_only and can choose hash_sorted or
		// btree. Keep the public Stats value; adaptive is a selector, not a
		// physical representation. Explicit adaptive:skiplist is not this arm.
		validMode = r.MemtableModeConfig == "adaptive" &&
			(r.MemtableMode == "append_only" || r.MemtableMode == "hash_sorted" || r.MemtableMode == "btree")
	}
	if r.Engine != "treedb" || r.ResolvedProfile != c.Profile || r.RequestedMemtableMode != c.MemtableMode ||
		!validMode || (c.MemtableMode == "cow_ordered" && !r.LogicalMVCC) {
		return fmt.Errorf("wrong public route: requested %s/%s, observed %+v", c.Profile, c.MemtableMode, r)
	}
	return nil
}

type treeEngine struct {
	db    *treedb.DB
	store *mvcc.Store
	mode  mvcc.CommitMode
}

func (e *treeEngine) Commit(ts uint64, m []mvcc.Mutation) error {
	return e.store.CommitAt(ts, m, e.mode)
}
func (e *treeEngine) Get(key []byte, ts uint64) (record, error) {
	r, err := e.store.GetAt(key, ts)
	if r.State != mvcc.Present {
		// Badger's public Get reports tombstones and absence identically. The
		// retained-history cursor below preserves deletion state and timestamp.
		return record{State: mvcc.Absent}, err
	}
	return record{Key: key, Value: r.Value, Timestamp: r.Timestamp, State: r.State}, err
}
func (e *treeEngine) Range(lo, hi []byte, ts uint64, limit int) (rows []record, err error) {
	it, err := e.store.IterateVersions(mvcc.VersionIteratorOptions{LowerBound: lo, UpperBound: hi, ReadTimestamp: ts})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	rows = make([]record, 0, limit)
	next := make([]byte, 0, len(lo)+1)
	for it.Valid() && len(rows) < limit {
		v := it.EntryView()
		if v.State == mvcc.Present {
			// Borrow during selection, then own each retained output once.
			rows = append(rows, record{Key: append([]byte(nil), v.Key...), Value: append([]byte(nil), v.Value...), Timestamp: v.Timestamp, State: v.State})
		}
		// Select the first version before filtering a tombstone. Seeking to
		// key+NUL skips every timestamp of that exact key, including old values.
		// It does not skip a distinct key with the original key as its prefix.
		next = append(next[:0], v.Key...)
		next = append(next, 0)
		it.Seek(next, ts)
	}
	return rows, it.Error()
}
func (e *treeEngine) History(ts uint64) (historyCursor, error) {
	it, err := e.store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: ts})
	return &treeHistory{it: it}, err
}
func (e *treeEngine) Persist() error           { return e.db.Checkpoint() }
func (e *treeEngine) Stats() map[string]string { return e.db.Stats() }
func (e *treeEngine) Close() error             { return e.db.Close() }

type treeHistory struct{ it *mvcc.VersionIterator }

func (c *treeHistory) Valid() bool { return c.it.Valid() }
func (c *treeHistory) Entry() (record, error) {
	v := c.it.Entry()
	return record{Key: v.Key, Value: v.Value, Timestamp: v.Timestamp, State: v.State}, c.it.Error()
}
func (c *treeHistory) Next()        { c.it.Next() }
func (c *treeHistory) Error() error { return c.it.Error() }
func (c *treeHistory) Close() error { return c.it.Close() }

type badgerEngine struct{ db *badger.DB }

func (e *badgerEngine) Commit(ts uint64, mutations []mvcc.Mutation) error {
	tx := e.db.NewTransactionAt(math.MaxUint64, true)
	defer tx.Discard()
	for _, m := range mutations {
		var err error
		if m.Delete {
			err = tx.Delete(m.Key)
		} else {
			err = tx.Set(m.Key, m.Value)
		}
		if err != nil {
			return err
		}
	}
	return tx.CommitAt(ts, nil)
}
func (e *badgerEngine) Get(key []byte, ts uint64) (record, error) {
	tx := e.db.NewTransactionAt(ts, false)
	defer tx.Discard()
	item, err := tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return record{State: mvcc.Absent}, nil
	}
	if err != nil {
		return record{}, err
	}
	v, err := item.ValueCopy(nil)
	return record{Key: key, Value: v, Timestamp: item.Version(), State: mvcc.Present}, err
}
func (e *badgerEngine) Range(lo, hi []byte, ts uint64, limit int) ([]record, error) {
	tx := e.db.NewTransactionAt(ts, false)
	defer tx.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues, opts.AllVersions = true, false
	it := tx.NewIterator(opts)
	defer it.Close()
	rows := make([]record, 0, limit)
	for it.Seek(lo); it.Valid() && len(rows) < limit; it.Next() {
		item := it.Item()
		if hi != nil && bytes.Compare(item.Key(), hi) >= 0 {
			break
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		rows = append(rows, record{Key: item.KeyCopy(nil), Value: value, Timestamp: item.Version(), State: mvcc.Present})
	}
	return rows, nil
}
func (e *badgerEngine) History(ts uint64) (historyCursor, error) {
	tx := e.db.NewTransactionAt(ts, false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues, opts.AllVersions = true, true
	it := tx.NewIterator(opts)
	it.Rewind()
	return &badgerHistory{tx: tx, it: it}, nil
}
func (e *badgerEngine) Persist() error { return e.db.Sync() }
func (e *badgerEngine) Stats() map[string]string {
	lsm, vlog := e.db.Size()
	tables := e.db.Tables()
	ids := make([]string, 0, len(tables))
	for _, table := range tables {
		ids = append(ids, strconv.FormatUint(table.ID, 10))
	}
	stats := map[string]string{"badger.lsm_bytes": strconv.FormatInt(lsm, 10), "badger.vlog_bytes": strconv.FormatInt(vlog, 10),
		"badger.table_count": strconv.Itoa(len(tables)), "badger.table_ids": strings.Join(ids, ",")}
	for _, level := range e.db.Levels() {
		prefix := "badger.level." + strconv.Itoa(level.Level) + "."
		stats[prefix+"table_count"] = strconv.Itoa(level.NumTables)
		stats[prefix+"size_bytes"] = strconv.FormatInt(level.Size, 10)
		stats[prefix+"target_size_bytes"] = strconv.FormatInt(level.TargetSize, 10)
		stats[prefix+"target_file_size_bytes"] = strconv.FormatInt(level.TargetFileSize, 10)
	}
	return stats
}
func (e *badgerEngine) Close() error { return e.db.Close() }

type badgerHistory struct {
	tx *badger.Txn
	it *badger.Iterator
}

func (c *badgerHistory) Valid() bool { return c.it.Valid() }
func (c *badgerHistory) Entry() (record, error) {
	item := c.it.Item()
	r := record{Key: item.KeyCopy(nil), Timestamp: item.Version(), State: mvcc.Tombstone}
	if !item.IsDeletedOrExpired() {
		r.State = mvcc.Present
		var err error
		r.Value, err = item.ValueCopy(nil)
		return r, err
	}
	return r, nil
}
func (c *badgerHistory) Next()        { c.it.Next() }
func (c *badgerHistory) Error() error { return nil }
func (c *badgerHistory) Close() error { c.it.Close(); c.tx.Discard(); return nil }
