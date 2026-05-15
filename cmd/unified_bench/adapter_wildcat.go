package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/snissn/gomap/kvstore"
	"github.com/wildcatdb/wildcat"
)

var (
	wildcatSyncMode       = flag.String("wildcat-sync", "none", "Wildcat: sync option (none|partial|full)")
	wildcatSyncInterval   = flag.Duration("wildcat-sync-interval", 16*time.Nanosecond, "Wildcat: sync interval when -wildcat-sync=partial")
	wildcatWriteBufferMB  = flag.Int("wildcat-write-buffer-mb", 64, "Wildcat: write buffer size in MiB")
	wildcatBloomFilter    = flag.Bool("wildcat-bloom-filter", false, "Wildcat: enable SSTable Bloom filters")
	wildcatCompactionJobs = flag.Int("wildcat-compaction-jobs", 4, "Wildcat: max compaction concurrency")
)

func init() {
	RegisterDB("wildcat", NewWildcat)
}

type WildcatWrapper struct {
	db  *wildcat.DB
	dir string
}

type WildcatBatch struct {
	db        *WildcatWrapper
	txn       *wildcat.Txn
	committed bool
}

type WildcatIterator struct {
	keys [][]byte
	vals [][]byte
	idx  int
}

func NewWildcat(dir string) (kvstore.DB, error) {
	db, err := openWildcat(dir)
	if err != nil {
		return nil, err
	}
	return &WildcatWrapper{db: db, dir: dir}, nil
}

func openWildcat(dir string) (*wildcat.DB, error) {
	syncOpt, err := parseWildcatSyncOption(*wildcatSyncMode)
	if err != nil {
		return nil, err
	}
	writeBufferSize := int64(*wildcatWriteBufferMB) << 20
	if writeBufferSize <= 0 {
		writeBufferSize = wildcat.DefaultWriteBufferSize
	}
	compactionJobs := *wildcatCompactionJobs
	if compactionJobs <= 0 {
		compactionJobs = wildcat.DefaultMaxCompactionConcurrency
	}
	return wildcat.Open(&wildcat.Options{
		Directory:                dir,
		WriteBufferSize:          writeBufferSize,
		SyncOption:               syncOpt,
		SyncInterval:             *wildcatSyncInterval,
		BloomFilter:              *wildcatBloomFilter,
		MaxCompactionConcurrency: compactionJobs,
	})
}

func parseWildcatSyncOption(mode string) (wildcat.SyncOption, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "none", "off", "false", "0":
		return wildcat.SyncNone, nil
	case "partial", "interval", "1":
		return wildcat.SyncPartial, nil
	case "full", "always", "true", "2":
		return wildcat.SyncFull, nil
	default:
		return wildcat.SyncNone, fmt.Errorf("unknown Wildcat sync option %q (want none, partial, or full)", mode)
	}
}

func isWildcatNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "key not found")
}

func encodeWildcatKey(key []byte) []byte {
	out := make([]byte, hex.EncodedLen(len(key)))
	hex.Encode(out, key)
	return out
}

func decodeWildcatKey(key []byte) []byte {
	out := make([]byte, hex.DecodedLen(len(key)))
	if _, err := hex.Decode(out, key); err != nil {
		return append([]byte(nil), key...)
	}
	return out
}

func encodeWildcatBound(key []byte) []byte {
	if key == nil {
		return nil
	}
	return encodeWildcatKey(key)
}

func (w *WildcatWrapper) Name() string { return "Wildcat" }

func (w *WildcatWrapper) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	err := w.db.Close()
	w.db = nil
	return err
}

func (w *WildcatWrapper) Get(key []byte) ([]byte, error) {
	var out []byte
	encodedKey := encodeWildcatKey(key)
	err := w.db.View(func(txn *wildcat.Txn) error {
		val, err := txn.Get(encodedKey)
		if isWildcatNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		out = append(out[:0], val...)
		return nil
	})
	return out, err
}

func (w *WildcatWrapper) GetMany(keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	encodedKeys := make([][]byte, len(keys))
	for i, key := range keys {
		encodedKeys[i] = encodeWildcatKey(key)
	}
	err := w.db.View(func(txn *wildcat.Txn) error {
		for i, key := range encodedKeys {
			val, err := txn.Get(key)
			if isWildcatNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			out[i] = append([]byte(nil), val...)
		}
		return nil
	})
	return out, err
}

func (w *WildcatWrapper) Set(key, value []byte) error {
	if len(value) == 0 {
		return errors.New("wildcat: empty values are not supported")
	}
	encodedKey := encodeWildcatKey(key)
	valueCopy := append([]byte(nil), value...)
	return w.db.Update(func(txn *wildcat.Txn) error {
		return txn.Put(encodedKey, valueCopy)
	})
}

func (w *WildcatWrapper) SetSync(key, value []byte) error {
	if err := w.Set(key, value); err != nil {
		return err
	}
	return w.syncIfEscalatable()
}

func (w *WildcatWrapper) Delete(key []byte) error {
	encodedKey := encodeWildcatKey(key)
	return w.db.Update(func(txn *wildcat.Txn) error {
		return txn.Delete(encodedKey)
	})
}

func (w *WildcatWrapper) DeleteSync(key []byte) error {
	if err := w.Delete(key); err != nil {
		return err
	}
	return w.syncIfEscalatable()
}

func (w *WildcatWrapper) Checkpoint() error {
	if w == nil || w.db == nil {
		return nil
	}
	if err := w.syncIfEscalatable(); err != nil {
		return err
	}
	if err := w.db.Close(); err != nil {
		return err
	}
	db, err := openWildcat(w.dir)
	if err != nil {
		w.db = nil
		return err
	}
	w.db = db
	return nil
}

func (w *WildcatWrapper) syncIfEscalatable() error {
	syncOpt, err := parseWildcatSyncOption(*wildcatSyncMode)
	if err != nil {
		return err
	}
	if syncOpt != wildcat.SyncNone {
		return nil
	}
	err = w.db.Sync()
	if err == nil || strings.Contains(err.Error(), "no block manager found for WAL") {
		return nil
	}
	return err
}

func (w *WildcatWrapper) NewBatch() (kvstore.Batch, error) {
	return &WildcatBatch{db: w, txn: w.db.Begin()}, nil
}

func (b *WildcatBatch) Set(key, value []byte) error {
	if b == nil || b.txn == nil {
		return errors.New("wildcat batch is closed")
	}
	if len(value) == 0 {
		return errors.New("wildcat: empty values are not supported")
	}
	return b.txn.Put(encodeWildcatKey(key), append([]byte(nil), value...))
}

func (b *WildcatBatch) Delete(key []byte) error {
	if b == nil || b.txn == nil {
		return errors.New("wildcat batch is closed")
	}
	return b.txn.Delete(encodeWildcatKey(key))
}

func (b *WildcatBatch) Commit() error {
	if b == nil || b.txn == nil {
		return errors.New("wildcat batch is closed")
	}
	err := b.txn.Commit()
	if err == nil {
		b.committed = true
		b.txn = nil
	}
	return err
}

func (b *WildcatBatch) CommitSync() error {
	if err := b.Commit(); err != nil {
		return err
	}
	return b.db.syncIfEscalatable()
}

func (b *WildcatBatch) Close() error {
	if b == nil || b.txn == nil || b.committed {
		return nil
	}
	err := b.txn.Rollback()
	b.txn = nil
	return err
}

func (w *WildcatWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	return w.materializeIterator(start, end, true)
}

func (w *WildcatWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	return w.materializeIterator(start, end, false)
}

func (w *WildcatWrapper) materializeIterator(start, end []byte, asc bool) (kvstore.Iterator, error) {
	it := &WildcatIterator{}
	encodedStart := encodeWildcatBound(start)
	encodedEnd := encodeWildcatBound(end)
	err := w.db.View(func(txn *wildcat.Txn) error {
		var merge *wildcat.MergeIterator
		var err error
		if encodedStart != nil || encodedEnd != nil {
			merge, err = txn.NewRangeIterator(encodedStart, encodedEnd, asc)
		} else {
			merge, err = txn.NewIterator(asc)
		}
		if err != nil {
			return err
		}
		for {
			key, val, _, ok := merge.Next()
			if !ok {
				break
			}
			if val == nil {
				continue
			}
			it.keys = append(it.keys, decodeWildcatKey(key))
			it.vals = append(it.vals, append([]byte(nil), val...))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (it *WildcatIterator) Valid() bool { return it != nil && it.idx < len(it.keys) }
func (it *WildcatIterator) Next()       { it.idx++ }
func (it *WildcatIterator) Key() []byte { return it.keys[it.idx] }
func (it *WildcatIterator) Value() []byte {
	return it.vals[it.idx]
}
func (it *WildcatIterator) KeyCopy(dst []byte) []byte {
	return append(dst, it.keys[it.idx]...)
}
func (it *WildcatIterator) ValueCopy(dst []byte) []byte {
	return append(dst, it.vals[it.idx]...)
}
func (it *WildcatIterator) Error() error { return nil }
func (it *WildcatIterator) Close() error { return nil }
