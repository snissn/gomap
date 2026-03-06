package caching

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type systemOverlayValue struct {
	value   []byte
	deleted bool
}

type systemOverlayBatch struct {
	db      *DB
	entries []batch.Entry
	closed  bool
}

type systemOverlayIterEntry struct {
	key     []byte
	value   []byte
	deleted bool
}

type systemOverlayIterator struct {
	entries []systemOverlayIterEntry
	index   int
	start   []byte
	end     []byte
	err     error
}

func (db *DB) PendingSystemOverlay() bool {
	if db == nil {
		return false
	}
	db.systemMu.RLock()
	defer db.systemMu.RUnlock()
	return db.systemDomain != nil && db.systemDomain.pending()
}

func (db *DB) ApplySystemOverlayEntries(entries []batch.Entry) error {
	if db == nil {
		return errDBClosing
	}
	cloned, err := cloneSystemOverlayEntries(entries)
	if err != nil {
		return err
	}
	if len(cloned) == 0 {
		return nil
	}
	db.applySystemOverlayEntriesOwned(cloned)
	return nil
}

func (db *DB) ApplySystemOverlayEntriesOwned(entries []batch.Entry) error {
	if db == nil {
		return errDBClosing
	}
	if len(entries) == 0 {
		return nil
	}
	db.applySystemOverlayEntriesOwned(entries)
	return nil
}

func (db *DB) GetSystem(key []byte) ([]byte, error) {
	if db == nil {
		return nil, errDBClosing
	}
	db.systemMu.RLock()
	if domain := db.systemDomain; domain != nil {
		if value, _, flags, found := domain.getEntry(key); found {
			db.systemMu.RUnlock()
			if flags&node.FlagTombstone != 0 {
				return nil, nil
			}
			return append([]byte(nil), value...), nil
		}
	}
	db.systemMu.RUnlock()
	bridge, err := db.systemBridge(true)
	if err != nil {
		return nil, err
	}
	return bridge.GetSystem(key)
}

func (db *DB) SetSystem(key, value []byte) error {
	if db == nil {
		return errDBClosing
	}
	db.waitForCheckpoint()
	return db.ApplySystemOverlayEntries([]batch.Entry{{Type: batch.OpPut, Key: key, Value: value}})
}

func (db *DB) NewSystemBatch() batch.Interface {
	if db == nil {
		return nil
	}
	return &systemOverlayBatch{
		db:      db,
		entries: make([]batch.Entry, 0, 16),
	}
}

func (db *DB) SystemIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	if db == nil {
		return nil, errDBClosing
	}
	bridge, err := db.systemBridge(true)
	if err != nil {
		return nil, err
	}
	db.systemMu.RLock()
	domain := db.systemDomain
	pending := domain != nil && domain.pending()
	db.systemMu.RUnlock()
	if !pending {
		return bridge.SystemIterator(start, end)
	}
	overlayIter, err := db.systemDomainIterator(start, end)
	if err != nil {
		return nil, err
	}
	baseIter, err := bridge.SystemIterator(start, end)
	if err != nil {
		_ = overlayIter.Close()
		return nil, err
	}
	return newNamedRootMergeIterator(overlayIter, baseIter, start, end), nil
}

func (db *DB) SystemRootVersion() uint64 {
	if db == nil {
		return 0
	}
	bridge, err := db.systemBridge(true)
	if err != nil {
		return 0
	}
	base := bridge.SystemRootVersion()
	db.systemMu.RLock()
	pending := db.systemDomain != nil && db.systemDomain.pending()
	overlayVersion := uint64(0)
	if db.systemDomain != nil {
		overlayVersion = db.systemDomain.version.Load()
	}
	db.systemMu.RUnlock()
	if !pending {
		return base
	}
	var payload [16]byte
	binary.LittleEndian.PutUint64(payload[:8], base)
	binary.LittleEndian.PutUint64(payload[8:], overlayVersion)
	return xxhash.Sum64(payload[:])
}

func (db *DB) flushSystemOverlay(sync bool) error {
	if db == nil {
		return errDBClosing
	}
	db.waitForCheckpoint()
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	bridge, err := db.directBridge()
	if err != nil {
		return err
	}
	return db.flushSystemOverlayLocked(bridge, sync)
}

func (db *DB) flushSystemOverlayLocked(bridge BackendDirectBridge, sync bool) error {
	if db == nil {
		return errDBClosing
	}
	db.systemMu.Lock()
	defer db.systemMu.Unlock()
	if db.systemDomain == nil || !db.systemDomain.pending() {
		return nil
	}
	flushIter, err := db.systemDomain.newIterator(nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = flushIter.Close() }()
	target := bridge.NewSystemBatch()
	if target == nil {
		return errDBClosing
	}
	if err := applySystemIterator(target, flushIter); err != nil {
		_ = target.Close()
		return err
	}
	var writeErr error
	if sync {
		writeErr = target.WriteSync()
	} else {
		writeErr = target.Write()
	}
	closeErr := target.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	nextDomain, err := newRootDomain()
	if err != nil {
		return err
	}
	db.systemDomain = nextDomain
	db.systemDomain.version.Add(1)
	return nil
}

func (db *DB) systemBridge(wait bool) (BackendDirectBridge, error) {
	if db == nil {
		return nil, errDBClosing
	}
	if wait {
		db.waitForCheckpoint()
	}
	return db.directBridge()
}

func (db *DB) ensureSystemDomainLocked() (*rootDomain, error) {
	if db.systemDomain != nil && db.systemDomain.table != nil {
		return db.systemDomain, nil
	}
	domain, err := newRootDomain()
	if err != nil {
		return nil, err
	}
	db.systemDomain = domain
	return domain, nil
}

func (db *DB) systemDomainIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	db.systemMu.RLock()
	defer db.systemMu.RUnlock()
	if db.systemDomain == nil || !db.systemDomain.pending() {
		return nil, errDBClosing
	}
	return db.systemDomain.newIterator(start, end)
}

func (db *DB) DebugSystemRootState() SystemRootDebugState {
	if db == nil {
		return SystemRootDebugState{}
	}
	db.systemMu.RLock()
	defer db.systemMu.RUnlock()
	state := SystemRootDebugState{
		LegacyEntryCount: len(db.systemOverlay),
	}
	if db.systemDomain != nil && db.systemDomain.table != nil && db.systemDomain.table.Len() > 0 {
		state.HasMutable = true
	}
	return state
}

func applySystemIterator(target batch.Interface, iter iterator.UnsafeIterator) error {
	if iter == nil {
		return nil
	}
	const chunkCap = 256
	entries := make([]batch.Entry, 0, chunkCap)
	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		if err := target.SetOps(entries); err != nil {
			return err
		}
		entries = entries[:0]
		return nil
	}
	for iter.Valid() {
		key := iter.KeyCopy(nil)
		entry := batch.Entry{Key: key}
		if iter.IsDeleted() {
			entry.Type = batch.OpDelete
		} else {
			entry.Type = batch.OpPut
			entry.Value = iter.ValueCopy(nil)
		}
		entries = append(entries, entry)
		iter.Next()
		if len(entries) == cap(entries) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return flush()
}

func (db *DB) applySystemOverlayEntriesOwned(entries []batch.Entry) {
	if len(entries) == 0 {
		return
	}
	db.systemMu.Lock()
	defer db.systemMu.Unlock()
	if _, err := db.ensureSystemDomainLocked(); err != nil {
		panic(err)
	}
	if err := db.systemDomain.applyEntriesOwned(entries, false); err != nil {
		panic(err)
	}
}

func (db *DB) snapshotSystemOverlayEntries() []batch.Entry {
	db.systemMu.RLock()
	defer db.systemMu.RUnlock()
	if len(db.systemOverlay) == 0 {
		return nil
	}
	keys := make([]string, 0, len(db.systemOverlay))
	for key := range db.systemOverlay {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]batch.Entry, 0, len(keys))
	for _, key := range keys {
		value := db.systemOverlay[key]
		entry := batch.Entry{Key: []byte(key)}
		if value.deleted {
			entry.Type = batch.OpDelete
		} else {
			entry.Type = batch.OpPut
			entry.Value = append([]byte(nil), value.value...)
		}
		entries = append(entries, entry)
	}
	return entries
}

func (db *DB) clearFlushedSystemOverlayEntries(entries []batch.Entry) {
	if len(entries) == 0 {
		return
	}
	db.systemMu.Lock()
	defer db.systemMu.Unlock()
	if len(db.systemOverlay) == 0 {
		return
	}
	changed := false
	for _, entry := range entries {
		key := string(entry.Key)
		current, ok := db.systemOverlay[key]
		if !ok || !systemOverlayMatchesEntry(current, entry) {
			continue
		}
		delete(db.systemOverlay, key)
		changed = true
	}
	if changed {
		db.systemOverlayVersion.Add(1)
	}
}

func (db *DB) systemOverlayGet(key []byte) (systemOverlayValue, bool) {
	db.systemMu.RLock()
	defer db.systemMu.RUnlock()
	if len(db.systemOverlay) == 0 {
		return systemOverlayValue{}, false
	}
	value, ok := db.systemOverlay[string(key)]
	if !ok {
		return systemOverlayValue{}, false
	}
	if value.deleted {
		return systemOverlayValue{deleted: true}, true
	}
	return systemOverlayValue{value: append([]byte(nil), value.value...)}, true
}

func (db *DB) buildMergedSystemEntries(bridge BackendDirectBridge, start, end []byte) ([]systemOverlayIterEntry, error) {
	merged := make(map[string]systemOverlayIterEntry)

	it, err := bridge.SystemIterator(start, end)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.Valid() {
		key := it.KeyCopy(nil)
		entry := systemOverlayIterEntry{key: key}
		if it.IsDeleted() {
			entry.deleted = true
		} else {
			entry.value = it.ValueCopy(nil)
		}
		merged[string(key)] = entry
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}

	db.systemMu.RLock()
	if len(db.systemOverlay) > 0 {
		for key, value := range db.systemOverlay {
			keyBytes := []byte(key)
			if !systemOverlayKeyInRange(keyBytes, start, end) {
				continue
			}
			entry := systemOverlayIterEntry{key: keyBytes, deleted: value.deleted}
			if !value.deleted {
				entry.value = append([]byte(nil), value.value...)
			}
			merged[key] = entry
		}
	}
	db.systemMu.RUnlock()

	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})

	entries := make([]systemOverlayIterEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, merged[key])
	}
	return entries, nil
}

func cloneSystemOverlayEntries(entries []batch.Entry) ([]batch.Entry, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	cloned := make([]batch.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsPtr {
			return nil, batch.ErrValueTooLarge
		}
		next := batch.Entry{
			Type: entry.Type,
			Key:  append([]byte(nil), entry.Key...),
		}
		if entry.Type == batch.OpPut {
			next.Value = append([]byte(nil), entry.Value...)
		}
		cloned = append(cloned, next)
	}
	return cloned, nil
}

func systemOverlayMatchesEntry(value systemOverlayValue, entry batch.Entry) bool {
	switch entry.Type {
	case batch.OpDelete:
		return value.deleted
	case batch.OpPut:
		return !value.deleted && bytes.Equal(value.value, entry.Value)
	default:
		return false
	}
}

func systemOverlayKeyInRange(key, start, end []byte) bool {
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

func (b *systemOverlayBatch) Set(key, value []byte) error {
	if b == nil || b.db == nil {
		return errDBClosing
	}
	if b.closed {
		return batch.ErrBatchClosed
	}
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
	return nil
}

func (b *systemOverlayBatch) Delete(key []byte) error {
	if b == nil || b.db == nil {
		return errDBClosing
	}
	if b.closed {
		return batch.ErrBatchClosed
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  append([]byte(nil), key...),
	})
	return nil
}

func (b *systemOverlayBatch) SetOps(ops []batch.Entry) error {
	if b == nil || b.db == nil {
		return errDBClosing
	}
	if b.closed {
		return batch.ErrBatchClosed
	}
	cloned, err := cloneSystemOverlayEntries(ops)
	if err != nil {
		return err
	}
	b.entries = append(b.entries, cloned...)
	return nil
}

func (b *systemOverlayBatch) Write() error {
	if b == nil || b.db == nil {
		return errDBClosing
	}
	if b.closed {
		return batch.ErrBatchClosed
	}
	b.db.waitForCheckpoint()
	b.closed = true
	return b.db.ApplySystemOverlayEntriesOwned(b.entries)
}

func (b *systemOverlayBatch) WriteSync() error {
	if err := b.Write(); err != nil {
		return err
	}
	return b.db.flushSystemOverlay(true)
}

func (b *systemOverlayBatch) Close() error {
	if b == nil {
		return nil
	}
	b.closed = true
	b.entries = nil
	return nil
}

func (b *systemOverlayBatch) Replay(fn func(batch.Entry) error) error {
	if b == nil {
		return nil
	}
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *systemOverlayBatch) GetByteSize() (int, error) {
	if b == nil {
		return 0, nil
	}
	size := 0
	for _, entry := range b.entries {
		size += len(entry.Key) + len(entry.Value)
	}
	return size, nil
}

func (it *systemOverlayIterator) Valid() bool {
	return it != nil && it.index < len(it.entries)
}

func (it *systemOverlayIterator) Next() {
	if it == nil || !it.Valid() {
		return
	}
	it.index++
}

func (it *systemOverlayIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.index = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
}

func (it *systemOverlayIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.index].key
}

func (it *systemOverlayIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.index].value
}

func (it *systemOverlayIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	entry := it.entries[it.index]
	if entry.deleted {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return entry.value, page.ValuePtr{}, node.FlagInline
}

func (it *systemOverlayIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *systemOverlayIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *systemOverlayIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.entries[it.index].key...)
}

func (it *systemOverlayIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.entries[it.index].value...)
}

func (it *systemOverlayIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.index].deleted
}

func (it *systemOverlayIterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *systemOverlayIterator) Close() error {
	if it == nil {
		return nil
	}
	it.entries = nil
	return nil
}

func (it *systemOverlayIterator) Domain() ([]byte, []byte) {
	if it == nil {
		return nil, nil
	}
	return it.start, it.end
}
