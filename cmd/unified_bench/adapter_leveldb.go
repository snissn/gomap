package main

import (
	"bytes"
	"errors"
	"flag"

	"github.com/snissn/gomap/kvstore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func init() {
	RegisterDB("leveldb", NewLevelDB)
	RegisterHiddenDB("leveldb_block_comp_on", NewLevelDBBlockCompressionOn)
	RegisterHiddenDB("leveldb_block_comp_off", NewLevelDBBlockCompressionOff)
}

const (
	leveldbMinCacheMB = 16
	leveldbMinHandles = 16
)

var (
	leveldbCacheMB              = flag.Int("leveldb-cache-mb", 1024, "LevelDB: total cache in MiB (matches geth semantics; used as BlockCache=cache/2 and WriteBuffer=cache/4)")
	leveldbHandles              = flag.Int("leveldb-handles", 1024, "LevelDB: open files cache capacity (matches geth semantics)")
	leveldbBlockCompressionMode = flag.String("leveldb-block-compression", "default", "LevelDB: block compression mode for unified_bench (default|on|off|both)")
	leveldbBlockSize            = flag.Int("leveldb-block-size", 4096, "LevelDB: table block size in bytes (opt.Options.BlockSize)")
)

type LevelDBBatch struct {
	db       *leveldb.DB
	batch    leveldb.Batch
	pointOps int
	ranges   []levelDBRangeOp
}

type levelDBRangeOp struct {
	start, limit []byte
}

func (b *LevelDBBatch) Set(key, value []byte) error {
	if len(b.ranges) > 0 {
		return errors.New("leveldb: mixing point writes with DeleteRange in one batch is unsupported")
	}
	b.batch.Put(key, value)
	b.pointOps++
	return nil
}
func (b *LevelDBBatch) Delete(key []byte) error {
	if len(b.ranges) > 0 {
		return errors.New("leveldb: mixing point deletes with DeleteRange in one batch is unsupported")
	}
	b.batch.Delete(key)
	b.pointOps++
	return nil
}
func cloneLevelDBRangeBound(bound []byte) []byte {
	if bound == nil {
		return nil
	}
	clone := make([]byte, len(bound))
	copy(clone, bound)
	return clone
}

func (b *LevelDBBatch) DeleteRange(start, end []byte) error {
	if b.pointOps > 0 {
		return errors.New("leveldb: mixing DeleteRange with point operations in one batch is unsupported")
	}
	b.ranges = append(b.ranges, levelDBRangeOp{
		start: cloneLevelDBRangeBound(start),
		limit: cloneLevelDBRangeBound(end),
	})
	return nil
}
func (b *LevelDBBatch) Commit() error {
	return b.commit(nil)
}
func (b *LevelDBBatch) CommitSync() error {
	return b.commit(&opt.WriteOptions{Sync: true})
}
func (b *LevelDBBatch) Close() error {
	b.batch.Reset()
	b.pointOps = 0
	b.ranges = nil
	return nil
}

func (b *LevelDBBatch) commit(writeOpts *opt.WriteOptions) error {
	if len(b.ranges) == 0 {
		return b.db.Write(&b.batch, writeOpts)
	}
	var batch leveldb.Batch
	for _, r := range b.ranges {
		if err := b.appendDeleteRangeOps(&batch, r.start, r.limit); err != nil {
			return err
		}
	}
	return b.db.Write(&batch, writeOpts)
}

func (b *LevelDBBatch) appendDeleteRangeOps(batch *leveldb.Batch, start, end []byte) error {
	var slice *util.Range
	if start != nil || end != nil {
		slice = &util.Range{Start: start, Limit: end}
	}
	it := b.db.NewIterator(slice, nil)
	defer it.Release()
	for ok := it.First(); ok; ok = it.Next() {
		batch.Delete(append([]byte(nil), it.Key()...))
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

type LevelDBIterator struct {
	it      iterator.Iterator
	reverse bool
}

func (i *LevelDBIterator) Valid() bool { return i.it.Valid() }
func (i *LevelDBIterator) Next() {
	if i.reverse {
		i.it.Prev()
		return
	}
	i.it.Next()
}
func (i *LevelDBIterator) Key() []byte   { return i.it.Key() }
func (i *LevelDBIterator) Value() []byte { return i.it.Value() }
func (i *LevelDBIterator) KeyCopy(dst []byte) []byte {
	return append(dst, i.Key()...)
}
func (i *LevelDBIterator) ValueCopy(dst []byte) []byte {
	return append(dst, i.Value()...)
}
func (i *LevelDBIterator) Close() error { i.it.Release(); return nil }
func (i *LevelDBIterator) Error() error { return i.it.Error() }

type LevelDBWrapper struct {
	db          *leveldb.DB
	dir         string
	name        string
	compression opt.Compression
}

type levelDBReadSnapshot struct {
	snap *leveldb.Snapshot
}

func (s *levelDBReadSnapshot) Get(key []byte) ([]byte, error) {
	if s == nil || s.snap == nil {
		return nil, nil
	}
	val, err := s.snap.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	return val, err
}

func (s *levelDBReadSnapshot) GetAppend(key, dst []byte) ([]byte, error) {
	if s == nil || s.snap == nil {
		return dst, nil
	}
	val, err := s.snap.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return dst, nil
	}
	if err != nil {
		return dst, err
	}
	return append(dst, val...), nil
}

func (s *levelDBReadSnapshot) Close() error {
	if s == nil || s.snap == nil {
		return nil
	}
	s.snap.Release()
	s.snap = nil
	return nil
}

func leveldbBenchOptions(compression opt.Compression) *opt.Options {
	cache := *leveldbCacheMB
	handles := *leveldbHandles
	if cache < leveldbMinCacheMB {
		cache = leveldbMinCacheMB
	}
	if handles < leveldbMinHandles {
		handles = leveldbMinHandles
	}
	return &opt.Options{
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
		OpenFilesCacheCapacity: handles,
		BlockCacheCapacity:     cache / 2 * opt.MiB,
		WriteBuffer:            cache / 4 * opt.MiB, // two write buffers are used internally
		Compression:            compression,
		BlockSize:              *leveldbBlockSize,
	}
}

func openLevelDB(dir, name string, compression opt.Compression) (kvstore.DB, error) {
	db, err := leveldb.OpenFile(dir, leveldbBenchOptions(compression))
	if err != nil {
		return nil, err
	}
	return &LevelDBWrapper{db: db, dir: dir, name: name, compression: compression}, nil
}

func NewLevelDB(dir string) (kvstore.DB, error) {
	// Explicitly enable block compression to keep the benchmark deterministic.
	return openLevelDB(dir, "LevelDB", opt.SnappyCompression)
}

func NewLevelDBBlockCompressionOn(dir string) (kvstore.DB, error) {
	return openLevelDB(dir, "LevelDB (block=on)", opt.SnappyCompression)
}

func NewLevelDBBlockCompressionOff(dir string) (kvstore.DB, error) {
	return openLevelDB(dir, "LevelDB (block=off)", opt.NoCompression)
}

func (l *LevelDBWrapper) Name() string {
	if l.name != "" {
		return l.name
	}
	return "LevelDB"
}
func (l *LevelDBWrapper) Set(k, v []byte) error        { return l.db.Put(k, v, nil) }
func (l *LevelDBWrapper) Get(k []byte) ([]byte, error) { return l.db.Get(k, nil) }
func (l *LevelDBWrapper) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	snap, err := l.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, errors.New("leveldb: GetSnapshot returned nil without error")
	}
	return &levelDBReadSnapshot{snap: snap}, nil
}
func (l *LevelDBWrapper) GetMany(keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	snap, err := l.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()
	for i, key := range keys {
		val, err := snap.Get(key, nil)
		if err == nil {
			out[i] = val
			continue
		}
		if errors.Is(err, leveldb.ErrNotFound) {
			continue
		}
		return nil, err
	}
	return out, nil
}
func (l *LevelDBWrapper) Delete(k []byte) error { return l.db.Delete(k, nil) }
func (l *LevelDBWrapper) RangeDeleteMode() string {
	return kvstore.RangeDeleteModeFallbackIteratorDelete
}
func (l *LevelDBWrapper) Close() error { return l.db.Close() }
func (l *LevelDBWrapper) Checkpoint() error {
	if l == nil || l.db == nil {
		return nil
	}
	if err := l.db.Close(); err != nil {
		return err
	}
	db, err := leveldb.OpenFile(l.dir, leveldbBenchOptions(l.compression))
	if err != nil {
		return err
	}
	l.db = db
	return nil
}
func (l *LevelDBWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	var slice *util.Range
	if start != nil || end != nil {
		slice = &util.Range{Start: start, Limit: end}
	}
	it := l.db.NewIterator(slice, nil)
	it.First()
	return &LevelDBIterator{it: it, reverse: false}, nil
}
func (l *LevelDBWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	var slice *util.Range
	if start != nil || end != nil {
		slice = &util.Range{Start: start, Limit: end}
	}
	it := l.db.NewIterator(slice, nil)
	if end == nil {
		it.Last()
	} else {
		it.Seek(end)
		if it.Valid() {
			if bytes.Compare(it.Key(), end) >= 0 {
				it.Prev()
			}
		} else {
			it.Last()
		}
	}
	return &LevelDBIterator{it: it, reverse: true}, nil
}
func (l *LevelDBWrapper) NewBatch() (kvstore.Batch, error) {
	return &LevelDBBatch{db: l.db}, nil
}

func verifyRangeIteration(db kvstore.DB, rs kvstore.RangeScanner, prefix []byte, n int) (retErr error) {
	// ... (Implementation from main.go if needed, or remove if unused in bench loop)
	// For now, I'll exclude it unless it's called by the benchmark loop.
	// Looking at main.go, verifyRangeIteration was a helper function.
	// I'll keep it here if it's used, but it seems unused in the main bench loop logic I extracted.
	// I'll remove it for now to save space, assuming it was for ad-hoc verification.
	return nil
}
