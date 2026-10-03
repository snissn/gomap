//go:build rocksdb && cgo

package main

/*
#cgo LDFLAGS: -lrocksdb
#include <rocksdb/c.h>
#include <rocksdb/version.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/snissn/gomap/kvstore"
)

func init() { RegisterDB("rocksdb", NewRocksDB) }

// The C API copies writes and allocates reads; Go callers receive owned bytes.
type RocksDBWrapper struct {
	db *C.rocksdb_t
	ro *C.rocksdb_readoptions_t
	wo *C.rocksdb_writeoptions_t
}

var rocksDBEmptyByte byte

func rocksDBBytes(b []byte) *C.char {
	if len(b) == 0 {
		return (*C.char)(unsafe.Pointer(&rocksDBEmptyByte))
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}
func rocksDBError(p *C.char) error {
	if p == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(p))
	return errors.New(C.GoString(p))
}

func NewRocksDB(dir string) (kvstore.DB, error) {
	opts := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(opts)
	C.rocksdb_options_set_create_if_missing(opts, 1)
	C.rocksdb_options_set_write_buffer_size(opts, 64<<20)
	C.rocksdb_options_set_compression(opts, C.rocksdb_snappy_compression)
	C.rocksdb_options_set_level_compaction_dynamic_level_bytes(opts, 1)
	table := C.rocksdb_block_based_options_create()
	defer C.rocksdb_block_based_options_destroy(table)
	cache := C.rocksdb_cache_create_lru(64 << 20)
	defer C.rocksdb_cache_destroy(cache)
	C.rocksdb_block_based_options_set_block_cache(table, cache)
	// set_filter_policy transfers ownership to the table's shared_ptr.
	C.rocksdb_block_based_options_set_filter_policy(table, C.rocksdb_filterpolicy_create_bloom_full(10))
	C.rocksdb_block_based_options_set_whole_key_filtering(table, 1)
	C.rocksdb_block_based_options_set_cache_index_and_filter_blocks(table, 1)
	C.rocksdb_options_set_block_based_table_factory(opts, table)
	name := C.CString(dir)
	defer C.free(unsafe.Pointer(name))
	var ep *C.char
	db := C.rocksdb_open(opts, name, &ep)
	if err := rocksDBError(ep); err != nil {
		return nil, err
	}
	ro := C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_verify_checksums(ro, 1)
	wo := C.rocksdb_writeoptions_create()
	C.rocksdb_writeoptions_set_sync(wo, 1)
	return &RocksDBWrapper{db: db, ro: ro, wo: wo}, nil
}
func (r *RocksDBWrapper) Name() string {
	return fmt.Sprintf("RocksDB %d.%d.%d", C.ROCKSDB_MAJOR, C.ROCKSDB_MINOR, C.ROCKSDB_PATCH)
}
func (r *RocksDBWrapper) Close() error {
	if r.db != nil {
		C.rocksdb_readoptions_destroy(r.ro)
		C.rocksdb_writeoptions_destroy(r.wo)
		C.rocksdb_close(r.db)
		r.db = nil
	}
	return nil
}
func (r *RocksDBWrapper) getAppend(ro *C.rocksdb_readoptions_t, key, dst []byte) ([]byte, error) {
	if r.db == nil || ro == nil {
		return nil, kvstore.ErrUnsupported
	}
	var n C.size_t
	var ep *C.char
	p := C.rocksdb_get(r.db, ro, rocksDBBytes(key), C.size_t(len(key)), &n, &ep)
	if p != nil {
		defer C.rocksdb_free(unsafe.Pointer(p))
	}
	if err := rocksDBError(ep); err != nil {
		return nil, err
	}
	if p == nil {
		return nil, nil
	}
	if uint64(n) > uint64(^uint(0)>>1)-uint64(len(dst)) {
		return nil, errors.New("rocksdb: value exceeds Go slice limit")
	}
	if n == 0 && dst == nil {
		return []byte{}, nil
	}
	return append(dst, unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))...), nil
}
func (r *RocksDBWrapper) Get(key []byte) ([]byte, error) { return r.getAppend(r.ro, key, nil) }
func (r *RocksDBWrapper) Set(key, value []byte) error {
	if r.db == nil {
		return kvstore.ErrUnsupported
	}
	var ep *C.char
	C.rocksdb_put(r.db, r.wo, rocksDBBytes(key), C.size_t(len(key)), rocksDBBytes(value), C.size_t(len(value)), &ep)
	return rocksDBError(ep)
}
func (r *RocksDBWrapper) Delete(key []byte) error {
	if r.db == nil {
		return kvstore.ErrUnsupported
	}
	var ep *C.char
	C.rocksdb_delete(r.db, r.wo, rocksDBBytes(key), C.size_t(len(key)), &ep)
	return rocksDBError(ep)
}
func (r *RocksDBWrapper) SetSync(key, value []byte) error { return r.Set(key, value) }
func (r *RocksDBWrapper) DeleteSync(key []byte) error     { return r.Delete(key) }
func (r *RocksDBWrapper) Checkpoint() error {
	if r.db == nil {
		return kvstore.ErrUnsupported
	}
	o := C.rocksdb_flushoptions_create()
	defer C.rocksdb_flushoptions_destroy(o)
	C.rocksdb_flushoptions_set_wait(o, 1)
	var ep *C.char
	C.rocksdb_flush(r.db, o, &ep)
	return rocksDBError(ep)
}
func (r *RocksDBWrapper) Stats() map[string]string {
	m := map[string]string{"rocksdb.version": r.Name(), "rocksdb.compression": "snappy", "rocksdb.bloom_bits_per_key": "10", "rocksdb.write_buffer_bytes": strconv.Itoa(64 << 20), "rocksdb.block_cache_capacity_bytes": strconv.Itoa(64 << 20), "rocksdb.sync_writes": "true", "rocksdb.verify_checksums": "true"}
	if r.db == nil {
		return m
	}
	for _, key := range []string{"rocksdb.block-cache-usage", "rocksdb.estimate-table-readers-mem", "rocksdb.cur-size-all-mem-tables"} {
		name := C.CString(key)
		p := C.rocksdb_property_value(r.db, name)
		C.free(unsafe.Pointer(name))
		if p != nil {
			m[key] = C.GoString(p)
			C.rocksdb_free(unsafe.Pointer(p))
		}
	}
	return m
}

type rocksDBSnapshot struct {
	owner    *RocksDBWrapper
	snapshot *C.rocksdb_snapshot_t
	ro       *C.rocksdb_readoptions_t
}

func (r *RocksDBWrapper) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	if r.db == nil {
		return nil, kvstore.ErrUnsupported
	}
	s := &rocksDBSnapshot{owner: r, snapshot: C.rocksdb_create_snapshot(r.db), ro: C.rocksdb_readoptions_create()}
	C.rocksdb_readoptions_set_verify_checksums(s.ro, 1)
	C.rocksdb_readoptions_set_snapshot(s.ro, s.snapshot)
	return s, nil
}
func (s *rocksDBSnapshot) Get(key []byte) ([]byte, error) { return s.GetAppend(key, nil) }
func (s *rocksDBSnapshot) GetAppend(key, dst []byte) ([]byte, error) {
	return s.owner.getAppend(s.ro, key, dst)
}
func (s *rocksDBSnapshot) Close() error {
	if s.ro != nil {
		C.rocksdb_readoptions_destroy(s.ro)
		if s.owner.db != nil {
			C.rocksdb_release_snapshot(s.owner.db, s.snapshot)
		}
		s.ro = nil
	}
	return nil
}

type rocksDBBatch struct {
	owner *RocksDBWrapper
	batch *C.rocksdb_writebatch_t
}

func (r *RocksDBWrapper) NewBatch() (kvstore.Batch, error) {
	if r.db == nil {
		return nil, kvstore.ErrUnsupported
	}
	return &rocksDBBatch{owner: r, batch: C.rocksdb_writebatch_create()}, nil
}
func (b *rocksDBBatch) Set(key, value []byte) error {
	if b.batch == nil {
		return kvstore.ErrUnsupported
	}
	C.rocksdb_writebatch_put(b.batch, rocksDBBytes(key), C.size_t(len(key)), rocksDBBytes(value), C.size_t(len(value)))
	return nil
}
func (b *rocksDBBatch) Delete(key []byte) error {
	if b.batch == nil {
		return kvstore.ErrUnsupported
	}
	C.rocksdb_writebatch_delete(b.batch, rocksDBBytes(key), C.size_t(len(key)))
	return nil
}
func (b *rocksDBBatch) Commit() error {
	if b.batch == nil || b.owner.db == nil {
		return kvstore.ErrUnsupported
	}
	var ep *C.char
	C.rocksdb_write(b.owner.db, b.owner.wo, b.batch, &ep)
	return rocksDBError(ep)
}
func (b *rocksDBBatch) CommitSync() error { return b.Commit() }
func (b *rocksDBBatch) Close() error {
	if b.batch != nil {
		C.rocksdb_writebatch_destroy(b.batch)
		b.batch = nil
	}
	return nil
}
