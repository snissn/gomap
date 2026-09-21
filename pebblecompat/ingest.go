package pebblecompat

import (
	"bytes"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	pebblerangekey "github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

const sharedMetaLocalPathPrefix = "pebblecompat-local-path:"

func keyInUserBounds(key []byte, bounds *pebble.KeyRange) bool {
	if bounds == nil {
		return true
	}
	if bounds.Start != nil && bytes.Compare(key, bounds.Start) < 0 {
		return false
	}
	if bounds.End != nil && bytes.Compare(key, bounds.End) >= 0 {
		return false
	}
	return true
}

func clipToBounds(start, end []byte, bounds *pebble.KeyRange) ([]byte, []byte, bool) {
	if bounds == nil {
		if bytes.Compare(start, end) >= 0 {
			return nil, nil, false
		}
		return append([]byte(nil), start...), append([]byte(nil), end...), true
	}
	return clipRange(start, end, bounds.Start, bounds.End)
}

func buildBatchFromSST(
	path string,
	bounds *pebble.KeyRange,
	allowPoint bool,
	allowRange bool,
) (*pebble.Batch, uint64, error) {
	batch := &pebble.Batch{}

	stat, statErr := os.Stat(path)
	var sizeBytes uint64
	if statErr == nil {
		sizeBytes = uint64(stat.Size())
	}

	f, err := vfs.Default.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return nil, 0, err
	}
	defer readable.Close()

	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		return nil, 0, err
	}
	defer reader.Close()

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()

	for ik, lv := iter.First(); ik != nil; ik, lv = iter.Next() {
		value, _, err := lv.Value(nil)
		if err != nil {
			return nil, 0, err
		}

		switch ik.Kind() {
		case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
			if !allowPoint || !keyInUserBounds(ik.UserKey, bounds) {
				continue
			}
			if err := batch.Set(ik.UserKey, value, nil); err != nil {
				return nil, 0, err
			}

		case pebble.InternalKeyKindMerge:
			if !allowPoint || !keyInUserBounds(ik.UserKey, bounds) {
				continue
			}
			if err := batch.Merge(ik.UserKey, value, nil); err != nil {
				return nil, 0, err
			}

		case pebble.InternalKeyKindDelete, pebble.InternalKeyKindSingleDelete, pebble.InternalKeyKindDeleteSized:
			if !allowPoint || !keyInUserBounds(ik.UserKey, bounds) {
				continue
			}
			if err := batch.Delete(ik.UserKey, nil); err != nil {
				return nil, 0, err
			}

		case pebble.InternalKeyKindRangeDelete:
			if !allowPoint {
				continue
			}
			start, end, ok := clipToBounds(ik.UserKey, value, bounds)
			if !ok {
				continue
			}
			if err := batch.DeleteRange(start, end, nil); err != nil {
				return nil, 0, err
			}

		case pebble.InternalKeyKindRangeKeySet,
			pebble.InternalKeyKindRangeKeyUnset,
			pebble.InternalKeyKindRangeKeyDelete:
			if !allowRange {
				continue
			}
			span, err := pebblerangekey.Decode(*ik, value, nil)
			if err != nil {
				return nil, 0, err
			}
			start, end, ok := clipToBounds(span.Start, span.End, bounds)
			if !ok {
				continue
			}
			for i := range span.Keys {
				rk := span.Keys[i]
				switch pebble.InternalKeyKind(rk.Trailer & 0xff) {
				case pebble.InternalKeyKindRangeKeySet:
					if err := batch.RangeKeySet(start, end, rk.Suffix, rk.Value, nil); err != nil {
						return nil, 0, err
					}
				case pebble.InternalKeyKindRangeKeyUnset:
					if err := batch.RangeKeyUnset(start, end, rk.Suffix, nil); err != nil {
						return nil, 0, err
					}
				case pebble.InternalKeyKindRangeKeyDelete:
					if err := batch.RangeKeyDelete(start, end, nil); err != nil {
						return nil, 0, err
					}
				}
			}
		}
	}

	if err := iter.Error(); err != nil {
		return nil, 0, err
	}
	if statErr != nil {
		sizeBytes = uint64(batch.Len())
	}
	return batch, sizeBytes, nil
}

// Ingest ingests local sstable paths.
func (d *DB) Ingest(paths []string) error {
	_, err := d.IngestWithStats(paths)
	return err
}

// IngestWithStats ingests local sstable/object paths and returns coarse stats.
func (d *DB) IngestWithStats(paths []string) (pebble.IngestOperationStats, error) {
	var stats pebble.IngestOperationStats
	for _, path := range paths {
		if isExportObjectPath(path) {
			objStats, err := d.IngestSharedObject(path, pebble.NoSync)
			if err != nil {
				return stats, err
			}
			stats.Bytes += objStats.Bytes
			stats.ApproxIngestedIntoL0Bytes += objStats.ApproxIngestedIntoL0Bytes
			stats.MemtableOverlappingFiles += objStats.MemtableOverlappingFiles
			continue
		}

		batch, sizeBytes, err := buildBatchFromSST(path, nil, true, true)
		if err != nil {
			return stats, err
		}
		if !batch.Empty() {
			if err := d.ApplyBatchRepr(batch.Repr(), pebble.NoSync); err != nil {
				_ = batch.Close()
				return stats, err
			}
			stats.MemtableOverlappingFiles++
		}
		stats.Bytes += sizeBytes
		stats.ApproxIngestedIntoL0Bytes += sizeBytes
		_ = batch.Close()
	}
	return stats, nil
}

// IngestExternalFiles ingests external descriptors when ObjName is a local path.
func (d *DB) IngestExternalFiles(external []pebble.ExternalFile) (pebble.IngestOperationStats, error) {
	resolved := make([]pebble.ExternalFile, 0, len(external))
	for i := range external {
		ef := external[i]
		if ef.Locator != "" {
			if d.externalFileResolver == nil {
				return pebble.IngestOperationStats{}, ErrExternalFileUnsupported
			}
			path, err := d.externalFileResolver(ef)
			if err != nil {
				return pebble.IngestOperationStats{}, fmt.Errorf("%w: %v", ErrExternalFileUnsupported, err)
			}
			if path == "" {
				return pebble.IngestOperationStats{}, ErrExternalFileUnsupported
			}
			ef.ObjName = path
			ef.Locator = ""
		} else if ef.ObjName == "" {
			if d.externalFileResolver == nil {
				return pebble.IngestOperationStats{}, ErrExternalFileUnsupported
			}
			path, err := d.externalFileResolver(ef)
			if err != nil {
				return pebble.IngestOperationStats{}, fmt.Errorf("%w: %v", ErrExternalFileUnsupported, err)
			}
			if path == "" {
				return pebble.IngestOperationStats{}, ErrExternalFileUnsupported
			}
			ef.ObjName = path
		}
		if !ef.HasPointKey && !ef.HasRangeKey {
			return pebble.IngestOperationStats{}, fmt.Errorf("pebblecompat: external file has neither point nor range keys")
		}
		resolved = append(resolved, ef)
	}

	var stats pebble.IngestOperationStats
	for i := range resolved {
		ef := resolved[i]
		if isExportObjectPath(ef.ObjName) {
			objStats, err := d.IngestSharedObject(ef.ObjName, pebble.NoSync)
			if err != nil {
				return stats, err
			}
			stats.Bytes += objStats.Bytes
			stats.ApproxIngestedIntoL0Bytes += objStats.ApproxIngestedIntoL0Bytes
			stats.MemtableOverlappingFiles += objStats.MemtableOverlappingFiles
			continue
		}

		var bounds *pebble.KeyRange
		if ef.SmallestUserKey != nil || ef.LargestUserKey != nil {
			bounds = &pebble.KeyRange{
				Start: append([]byte(nil), ef.SmallestUserKey...),
				End:   append([]byte(nil), ef.LargestUserKey...),
			}
		}
		batch, sizeBytes, err := buildBatchFromSST(ef.ObjName, bounds, ef.HasPointKey, ef.HasRangeKey)
		if err != nil {
			return stats, err
		}
		if !batch.Empty() {
			if err := d.ApplyBatchRepr(batch.Repr(), pebble.NoSync); err != nil {
				_ = batch.Close()
				return stats, err
			}
			stats.MemtableOverlappingFiles++
		}
		stats.Bytes += sizeBytes
		stats.ApproxIngestedIntoL0Bytes += sizeBytes
		_ = batch.Close()
	}
	return stats, nil
}

func encodeSharedMetaLocalPathBacking(path string) objstorage.RemoteObjectBacking {
	return append([]byte(sharedMetaLocalPathPrefix), []byte(path)...)
}

func decodeSharedMetaLocalPathBacking(backing objstorage.RemoteObjectBacking) (string, bool) {
	prefix := []byte(sharedMetaLocalPathPrefix)
	if len(backing) <= len(prefix) || !bytes.HasPrefix(backing, prefix) {
		return "", false
	}
	return string(backing[len(prefix):]), true
}

func resolveSharedMetaCompatLocalPath(meta pebble.SharedSSTMeta) (string, error) {
	if meta.Backing == nil {
		return "", ErrSharedSSTUnsupported
	}
	backing, err := meta.Backing.Get()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSharedSSTUnsupported, err)
	}
	path, ok := decodeSharedMetaLocalPathBacking(backing)
	if !ok || !isExportObjectPath(path) {
		return "", ErrSharedSSTUnsupported
	}
	return path, nil
}

func (d *DB) resolveSharedMetaPath(meta pebble.SharedSSTMeta) (string, error) {
	if meta.Backing != nil {
		defer meta.Backing.Close()
	}
	path, err := resolveSharedMetaCompatLocalPath(meta)
	if err == nil {
		return path, nil
	}
	if d.sharedMetaResolver == nil {
		return "", err
	}
	path, err = d.sharedMetaResolver(meta)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSharedSSTUnsupported, err)
	}
	if path == "" {
		return "", ErrSharedSSTUnsupported
	}
	return path, nil
}

func (d *DB) resolveSharedMetaPaths(shared []pebble.SharedSSTMeta) ([]string, error) {
	paths := make([]string, 0, len(shared))
	for i := range shared {
		path, err := d.resolveSharedMetaPath(shared[i])
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func addIngestStats(dst *pebble.IngestOperationStats, add pebble.IngestOperationStats) {
	dst.Bytes += add.Bytes
	dst.ApproxIngestedIntoL0Bytes += add.ApproxIngestedIntoL0Bytes
	dst.MemtableOverlappingFiles += add.MemtableOverlappingFiles
}

func (d *DB) ingestObjectPathsWithExcise(paths []string, exciseSpan pebble.KeyRange) (pebble.IngestOperationStats, error) {
	if err := validateSpan(exciseSpan); err != nil {
		return pebble.IngestOperationStats{}, err
	}
	var stats pebble.IngestOperationStats
	for i := range paths {
		var excise *pebble.KeyRange
		if i == 0 && spanDefined(exciseSpan) {
			spanCopy := pebble.KeyRange{
				Start: append([]byte(nil), exciseSpan.Start...),
				End:   append([]byte(nil), exciseSpan.End...),
			}
			excise = &spanCopy
		}
		objStats, err := d.ingestSharedObjectWithExcise(paths[i], pebble.NoSync, excise)
		if err != nil {
			return stats, err
		}
		addIngestStats(&stats, objStats)
	}
	return stats, nil
}

// IngestAndExcise applies a best-effort local ingest + excise flow.
func (d *DB) IngestAndExcise(paths []string, shared []pebble.SharedSSTMeta, exciseSpan pebble.KeyRange) (pebble.IngestOperationStats, error) {
	sharedPaths, err := d.resolveSharedMetaPaths(shared)
	if err != nil {
		return pebble.IngestOperationStats{}, err
	}

	allPaths := make([]string, 0, len(paths)+len(sharedPaths))
	allPaths = append(allPaths, paths...)
	allPaths = append(allPaths, sharedPaths...)

	objectPaths := make([]string, 0, len(allPaths))
	sstPaths := make([]string, 0, len(allPaths))
	for _, p := range allPaths {
		if isExportObjectPath(p) {
			objectPaths = append(objectPaths, p)
			continue
		}
		sstPaths = append(sstPaths, p)
	}

	var stats pebble.IngestOperationStats
	if len(objectPaths) > 0 {
		objStats, err := d.ingestObjectPathsWithExcise(objectPaths, exciseSpan)
		if err != nil {
			return stats, err
		}
		addIngestStats(&stats, objStats)
	} else if spanDefined(exciseSpan) {
		if bytes.Compare(exciseSpan.Start, exciseSpan.End) >= 0 {
			return pebble.IngestOperationStats{}, ErrInvalidRange
		}
		if err := d.DeleteRange(exciseSpan.Start, exciseSpan.End, pebble.NoSync); err != nil {
			return pebble.IngestOperationStats{}, err
		}
	}

	if len(sstPaths) > 0 {
		sstStats, err := d.IngestWithStats(sstPaths)
		if err != nil {
			return stats, err
		}
		addIngestStats(&stats, sstStats)
	}
	return stats, nil
}
