package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var (
	rawKVRevisionKeyPrefix    = []byte{0x00, 't', 'r', 'e', 'e', 'd', 'b', 0x00, 'r', 'e', 'v', 0x00}
	rawKVRevisionKeyPrefixEnd = []byte{0x00, 't', 'r', 'e', 'e', 'd', 'b', 0x00, 'r', 'e', 'v', 0x01}
)

const rawKVRevisionValueSize = 8

func appendRawKVRevisionKey(dst, key []byte) []byte {
	key = normalizeRawKVPointKey(key)
	dst = append(dst, rawKVRevisionKeyPrefix...)
	dst = append(dst, key...)
	return dst
}

func rawKVRevisionKey(key []byte) []byte {
	return appendRawKVRevisionKey(nil, key)
}

func isRawKVRevisionMetadataKey(key []byte) bool {
	return bytes.HasPrefix(key, rawKVRevisionKeyPrefix) && bytes.Compare(key, rawKVRevisionKeyPrefixEnd) < 0
}

func appendRawKVRevisionRangeStart(dst, start []byte) []byte {
	if start == nil {
		return append(dst, rawKVRevisionKeyPrefix...)
	}
	return appendRawKVRevisionKey(dst, start)
}

func appendRawKVRevisionRangeEnd(dst, end []byte) []byte {
	if end == nil {
		return append(dst, rawKVRevisionKeyPrefixEnd...)
	}
	return appendRawKVRevisionKey(dst, end)
}

func encodeRawKVRevision(revision uint64) []byte {
	var buf [rawKVRevisionValueSize]byte
	binary.LittleEndian.PutUint64(buf[:], revision)
	return buf[:]
}

func rawKVRevisionDeltaArenaBytes(points []batch.Entry, ranges []batch.DeleteRange) int {
	total := 0
	for _, r := range ranges {
		if r.Start == nil {
			total += len(rawKVRevisionKeyPrefix)
		} else {
			total += len(rawKVRevisionKeyPrefix) + len(normalizeRawKVPointKey(r.Start))
		}
		if r.End == nil {
			total += len(rawKVRevisionKeyPrefixEnd)
		} else {
			total += len(rawKVRevisionKeyPrefix) + len(normalizeRawKVPointKey(r.End))
		}
	}
	for _, entry := range points {
		switch entry.Type {
		case batch.OpPut, batch.OpDelete:
			total += len(rawKVRevisionKeyPrefix) + len(normalizeRawKVPointKey(entry.Key))
		}
	}
	return total
}

func appendRawKVRevisionDeltaKey(arena []byte, key []byte) ([]byte, []byte) {
	start := len(arena)
	arena = appendRawKVRevisionKey(arena, key)
	return arena, arena[start:]
}

func appendRawKVRevisionDeltaRangeStart(arena []byte, key []byte) ([]byte, []byte) {
	start := len(arena)
	arena = appendRawKVRevisionRangeStart(arena, key)
	return arena, arena[start:]
}

func appendRawKVRevisionDeltaRangeEnd(arena []byte, key []byte) ([]byte, []byte) {
	start := len(arena)
	arena = appendRawKVRevisionRangeEnd(arena, key)
	return arena, arena[start:]
}

func decodeRawKVRevision(value []byte) (uint64, error) {
	if len(value) != rawKVRevisionValueSize {
		return 0, fmt.Errorf("treedb: corrupt raw kv revision metadata: got %d bytes", len(value))
	}
	return binary.LittleEndian.Uint64(value), nil
}

func newRawKVRevisionDelta(points []batch.Entry, ranges []batch.DeleteRange, revision uint64) (*batch.Batch, bool, error) {
	if revision == 0 || (len(points) == 0 && len(ranges) == 0) {
		return nil, false, nil
	}
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	delta.Reserve(len(points) + len(ranges))
	keyArena := make([]byte, 0, rawKVRevisionDeltaArenaBytes(points, ranges))
	for _, r := range ranges {
		var start, end []byte
		keyArena, start = appendRawKVRevisionDeltaRangeStart(keyArena, r.Start)
		keyArena, end = appendRawKVRevisionDeltaRangeEnd(keyArena, r.End)
		if err := delta.DeleteRangeView(start, end); err != nil {
			_ = delta.Close()
			return nil, false, err
		}
	}
	encodedRevision := encodeRawKVRevision(revision)
	for _, entry := range points {
		switch entry.Type {
		case batch.OpPut:
			var key []byte
			keyArena, key = appendRawKVRevisionDeltaKey(keyArena, entry.Key)
			if len(ranges) == 0 {
				if err := delta.AppendViewTrustedSortedUnique(key, encodedRevision); err != nil {
					_ = delta.Close()
					return nil, false, err
				}
				continue
			}
			if err := delta.SetView(key, encodedRevision); err != nil {
				_ = delta.Close()
				return nil, false, err
			}
		case batch.OpDelete:
			var key []byte
			keyArena, key = appendRawKVRevisionDeltaKey(keyArena, entry.Key)
			if len(ranges) == 0 {
				if err := delta.AppendDeleteViewTrustedSortedUnique(key); err != nil {
					_ = delta.Close()
					return nil, false, err
				}
				continue
			}
			if err := delta.DeleteView(key); err != nil {
				_ = delta.Close()
				return nil, false, err
			}
		}
	}
	if delta.IsEmpty() {
		_ = delta.Close()
		return nil, false, nil
	}
	return delta, true, nil
}

func (db *DB) applyRawKVRevisionDelta(idx *indexGen, baseSystemRoot uint64, points []batch.Entry, ranges []batch.DeleteRange, revision uint64, alloc zipper.PageAllocator) (uint64, []uint64, adaptive.Metrics, error) {
	var metrics adaptive.Metrics
	delta, ok, err := newRawKVRevisionDelta(points, ranges, revision)
	if err != nil || !ok {
		return baseSystemRoot, nil, metrics, err
	}
	defer delta.Close()
	opts := systemRootOrderedPublishOptions(db).withSpanNativeRoute(
		OrderedRootSpanNativeRouteSystemDeltaBuilderPublish,
		"raw kv revision metadata delta apply",
	)
	return db.publishOrderedRootDeltaBatchWithAllocator(idx, baseSystemRoot, delta, opts, alloc, alloc, false)
}

func (s *Snapshot) rawKVRevision(key []byte) (uint64, error) {
	if s == nil || s.state == nil || s.state.SystemRootPageID == 0 {
		return 0, nil
	}
	value, err := s.GetAppendAtRoot(s.state.SystemRootPageID, rawKVRevisionKey(key), nil)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return decodeRawKVRevision(value)
}

func stripRawKVRevisionMetadata(target memtable.Table) error {
	if target == nil {
		return nil
	}
	it := target.NewIterator(rawKVRevisionKeyPrefix, rawKVRevisionKeyPrefixEnd)
	var keys [][]byte
	for it.Valid() {
		keys = append(keys, it.KeyCopy(nil))
		it.Next()
	}
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, key := range keys {
		target.Delete(key)
	}
	return nil
}

func (db *DB) preserveRawKVRevisionMetadata(baseSystemRoot uint64, target memtable.Table) error {
	if baseSystemRoot == 0 || target == nil {
		return stripRawKVRevisionMetadata(target)
	}
	if err := stripRawKVRevisionMetadata(target); err != nil {
		return err
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return ErrClosed
	}
	defer snap.Close()
	if snap.state == nil || snap.state.SystemRootPageID != baseSystemRoot {
		return errors.New("concurrent modification detected during raw kv revision metadata preservation")
	}
	it, err := snap.IteratorAtRoot(baseSystemRoot, rawKVRevisionKeyPrefix, rawKVRevisionKeyPrefixEnd)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() {
		val, ptr, flags := it.UnsafeEntry()
		target.SetEntry(it.UnsafeKey(), val, ptr, flags)
		it.Next()
	}
	return it.Error()
}
