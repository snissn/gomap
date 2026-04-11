package db

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	valueLogLocatorCatalogFileName = "vlog_locator_catalog.meta"
	valueLogLocatorCatalogVersion  = 1
	envEnableVlogLocatorCatalog    = "TREEDB_VLOG_LOCATOR_CATALOG"
)

type valueLogLocatorCatalogSegment struct {
	FileID uint32   `json:"file_id"`
	Keys   [][]byte `json:"keys,omitempty"`
}

type valueLogLocatorCatalogDisk struct {
	Version   uint32                          `json:"version"`
	CommitSeq uint64                          `json:"commit_seq"`
	Segments  []valueLogLocatorCatalogSegment `json:"segments,omitempty"`
}

type valueLogLocatorCatalog struct {
	mu        sync.RWMutex
	commitSeq uint64
	segments  map[uint32]map[string][]byte
	valid     bool
	dirty     bool
}

func newValueLogLocatorCatalog() *valueLogLocatorCatalog {
	return &valueLogLocatorCatalog{segments: make(map[uint32]map[string][]byte)}
}

func valueLogLocatorCatalogEnabled() bool {
	return envDebtLedgerBool(envEnableVlogLocatorCatalog)
}

func normalizeLocatorSegments(segments []valueLogLocatorCatalogSegment) []valueLogLocatorCatalogSegment {
	if len(segments) == 0 {
		return nil
	}
	out := make([]valueLogLocatorCatalogSegment, 0, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		seen := make(map[string]struct{}, len(seg.Keys))
		keys := make([][]byte, 0, len(seg.Keys))
		for _, key := range seg.Keys {
			if len(key) == 0 {
				continue
			}
			s := string(key)
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			keys = append(keys, append([]byte(nil), key...))
		}
		sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
		out = append(out, valueLogLocatorCatalogSegment{FileID: seg.FileID, Keys: keys})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FileID < out[j].FileID })
	return out
}

func locatorCatalogSegmentsToMap(segments []valueLogLocatorCatalogSegment) map[uint32]map[string][]byte {
	out := make(map[uint32]map[string][]byte, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		keys := make(map[string][]byte, len(seg.Keys))
		for _, key := range seg.Keys {
			if len(key) == 0 {
				continue
			}
			keys[string(key)] = append([]byte(nil), key...)
		}
		if len(keys) > 0 {
			out[seg.FileID] = keys
		}
	}
	return out
}

func locatorCatalogMapToSegments(segments map[uint32]map[string][]byte) []valueLogLocatorCatalogSegment {
	if len(segments) == 0 {
		return nil
	}
	out := make([]valueLogLocatorCatalogSegment, 0, len(segments))
	for fileID, keysMap := range segments {
		keys := make([][]byte, 0, len(keysMap))
		for _, key := range keysMap {
			keys = append(keys, append([]byte(nil), key...))
		}
		sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
		out = append(out, valueLogLocatorCatalogSegment{FileID: fileID, Keys: keys})
	}
	return normalizeLocatorSegments(out)
}

func (c *valueLogLocatorCatalog) replace(commitSeq uint64, segments map[uint32]map[string][]byte, dirty bool) {
	if c == nil {
		return
	}
	next := make(map[uint32]map[string][]byte, len(segments))
	for fileID, keys := range segments {
		if fileID == 0 || len(keys) == 0 {
			continue
		}
		cloned := make(map[string][]byte, len(keys))
		for s, key := range keys {
			cloned[s] = append([]byte(nil), key...)
		}
		next[fileID] = cloned
	}
	c.mu.Lock()
	c.commitSeq = commitSeq
	c.segments = next
	c.valid = true
	c.dirty = dirty
	c.mu.Unlock()
}

func (c *valueLogLocatorCatalog) invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.valid = false
	c.mu.Unlock()
}

func (c *valueLogLocatorCatalog) keysForSegments(commitSeq uint64, fileIDs []uint32) ([][]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.valid || c.commitSeq != commitSeq {
		return nil, false
	}
	seen := make(map[string]struct{})
	out := make([][]byte, 0)
	for _, fileID := range fileIDs {
		keys := c.segments[fileID]
		for s, key := range keys {
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, append([]byte(nil), key...))
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out, true
}

func (c *valueLogLocatorCatalog) dirtySnapshot() (uint64, []valueLogLocatorCatalogSegment, bool) {
	if c == nil {
		return 0, nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.valid || !c.dirty {
		return 0, nil, false
	}
	return c.commitSeq, locatorCatalogMapToSegments(c.segments), true
}

func (c *valueLogLocatorCatalog) markClean(commitSeq uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.valid && c.commitSeq == commitSeq {
		c.dirty = false
	}
	c.mu.Unlock()
}

func (db *DB) valueLogLocatorCatalogPath() string {
	if db == nil || db.dir == "" {
		return ""
	}
	return filepath.Join(db.dir, valueLogLocatorCatalogFileName)
}

func loadValueLogLocatorCatalogFromPath(path string, commitSeq uint64) (valueLogLocatorCatalogDisk, bool, error) {
	if path == "" {
		return valueLogLocatorCatalogDisk{}, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return valueLogLocatorCatalogDisk{}, false, nil
		}
		return valueLogLocatorCatalogDisk{}, false, err
	}
	var disk valueLogLocatorCatalogDisk
	if len(data) > 0 {
		if err := json.Unmarshal(data, &disk); err != nil {
			return valueLogLocatorCatalogDisk{}, false, nil
		}
	}
	if disk.Version != valueLogLocatorCatalogVersion || disk.CommitSeq != commitSeq {
		return valueLogLocatorCatalogDisk{}, false, nil
	}
	disk.Segments = normalizeLocatorSegments(disk.Segments)
	return disk, true, nil
}

func (db *DB) initValueLogLocatorCatalog() error {
	if db == nil || db.valueLogLocatorCatalog == nil || !valueLogLocatorCatalogEnabled() {
		return nil
	}
	_, err := db.loadValueLogLocatorCatalog(db.currentCommitSeq())
	return err
}

func (db *DB) loadValueLogLocatorCatalog(commitSeq uint64) (bool, error) {
	if db == nil || db.valueLogLocatorCatalog == nil || !valueLogLocatorCatalogEnabled() {
		return false, nil
	}
	disk, ok, err := loadValueLogLocatorCatalogFromPath(db.valueLogLocatorCatalogPath(), commitSeq)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	db.valueLogLocatorCatalog.replace(disk.CommitSeq, locatorCatalogSegmentsToMap(disk.Segments), false)
	return true, nil
}

func (db *DB) persistValueLogLocatorCatalog() error {
	if db == nil || db.valueLogLocatorCatalog == nil || !valueLogLocatorCatalogEnabled() {
		return nil
	}
	path := db.valueLogLocatorCatalogPath()
	if path == "" {
		return nil
	}
	commitSeq, segments, ok := db.valueLogLocatorCatalog.dirtySnapshot()
	if !ok {
		return nil
	}
	blob, err := json.Marshal(valueLogLocatorCatalogDisk{
		Version:   valueLogLocatorCatalogVersion,
		CommitSeq: commitSeq,
		Segments:  segments,
	})
	if err != nil {
		return err
	}
	if err := atomicfile.Write(path, blob, 0o600); err != nil {
		return err
	}
	db.valueLogLocatorCatalog.markClean(commitSeq)
	return nil
}

func (db *DB) persistValueLogLocatorCatalogBestEffort() {
	if err := db.persistValueLogLocatorCatalog(); err != nil {
		db.reportError(err)
	}
}

func (db *DB) rebuildValueLogLocatorCatalog(ctx context.Context) error {
	if db == nil || db.valueLogLocatorCatalog == nil || !valueLogLocatorCatalogEnabled() {
		return nil
	}
	segments, commitSeq, err := db.scanValueLogLocatorCatalog(ctx)
	if err != nil {
		return err
	}
	db.valueLogLocatorCatalog.replace(commitSeq, segments, true)
	return db.persistValueLogLocatorCatalog()
}

func (db *DB) scanValueLogLocatorCatalog(ctx context.Context) (map[uint32]map[string][]byte, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, 0, errors.New("missing snapshot state")
	}
	defer func() { _ = snap.Close() }()
	segments := make(map[uint32]map[string][]byte)
	it := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := collectValueLogLocatorCatalog(ctx, it, segments); err != nil {
		_ = it.Close()
		return nil, 0, err
	}
	_ = it.Close()
	return segments, snap.state.CommitSeq, nil
}

func collectValueLogLocatorCatalog(ctx context.Context, it iterator.UnsafeIterator, segments map[uint32]map[string][]byte) error {
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			seg := segments[ptr.FileID]
			if seg == nil {
				seg = make(map[string][]byte)
				segments[ptr.FileID] = seg
			}
			key := append([]byte(nil), it.UnsafeKey()...)
			seg[string(key)] = key
		}
		it.Next()
	}
	return it.Error()
}

func (db *DB) locatorKeysForSegments(ctx context.Context, fileIDs []uint32) ([][]byte, bool, error) {
	if db == nil || db.valueLogLocatorCatalog == nil || !valueLogLocatorCatalogEnabled() {
		return nil, false, nil
	}
	if keys, ok := db.valueLogLocatorCatalog.keysForSegments(db.currentCommitSeq(), fileIDs); ok {
		return keys, true, nil
	}
	if err := db.rebuildValueLogLocatorCatalog(ctx); err != nil {
		return nil, false, err
	}
	keys, ok := db.valueLogLocatorCatalog.keysForSegments(db.currentCommitSeq(), fileIDs)
	return keys, ok, nil
}
