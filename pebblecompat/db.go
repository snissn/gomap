package pebblecompat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"
	treedb "github.com/snissn/gomap/TreeDB"
)

// DB exposes a Pebble-like API backed by TreeDB.
type DB struct {
	tree *treedb.DB
	// shadow maintains a Pebble-native mirror for iterator/snapshot/indexed-batch APIs.
	shadow *pebble.DB

	mu sync.Mutex

	internalPrefix       []byte
	seqKey               []byte
	pointPrefix          []byte
	rangePrefix          []byte
	dataDir              string
	merger               *pebble.Merger
	sharedMetaResolver   func(meta pebble.SharedSSTMeta) (localPath string, err error)
	externalFileResolver func(file pebble.ExternalFile) (localPath string, err error)

	lastSeq uint64
}

type noopCloser struct{}

func (noopCloser) Close() error { return nil }

// Open opens a TreeDB directory using the Pebble-compat wrapper.
func Open(dirname string, opts *Options) (*DB, error) {
	cfg := Options{}
	if opts != nil {
		cfg = *opts
	}
	if len(cfg.InternalPrefix) == 0 {
		cfg.InternalPrefix = []byte(defaultInternalPrefix)
	} else {
		cfg.InternalPrefix = append([]byte(nil), cfg.InternalPrefix...)
	}
	if cfg.Merger == nil {
		cfg.Merger = pebble.DefaultMerger
	}
	if cfg.Merger.Merge == nil {
		return nil, errors.New("pebblecompat: merger missing Merge function")
	}
	to := cfg.TreeDB
	if to.Dir == "" {
		to.Dir = dirname
	}

	tdb, err := treedb.Open(to)
	if err != nil {
		return nil, err
	}
	d := &DB{
		tree:                 tdb,
		internalPrefix:       cfg.InternalPrefix,
		dataDir:              to.Dir,
		merger:               cfg.Merger,
		sharedMetaResolver:   cfg.SharedMetaResolver,
		externalFileResolver: cfg.ExternalFileResolver,
		lastSeq:              initialSeqNum,
	}
	d.seqKey = append(append([]byte(nil), d.internalPrefix...), []byte("seq")...)
	d.pointPrefix = append(append([]byte(nil), d.internalPrefix...), []byte("pt/")...)
	d.rangePrefix = append(append([]byte(nil), d.internalPrefix...), []byte("rg/")...)

	seqBytes, err := d.tree.Get(d.seqKey)
	if err != nil {
		_ = d.tree.Close()
		return nil, err
	}
	if len(seqBytes) > 0 {
		if seq, ok := decodeSeq(seqBytes); ok {
			d.lastSeq = seq
		} else {
			_ = d.tree.Close()
			return nil, fmt.Errorf("pebblecompat: invalid stored sequence")
		}
	}
	if err := d.initShadowLocked(); err != nil {
		_ = d.tree.Close()
		d.tree = nil
		return nil, err
	}
	return d, nil
}

func (d *DB) ensureOpenLocked() error {
	if d == nil || d.tree == nil {
		return ErrClosed
	}
	return nil
}

func (d *DB) ensureOpen() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ensureOpenLocked()
}

func (d *DB) isInternalKey(key []byte) bool {
	return bytes.HasPrefix(key, d.internalPrefix)
}

func (d *DB) validateUserKey(key []byte) error {
	if d.isInternalKey(key) {
		return ErrReservedKeyPrefix
	}
	return nil
}

func (d *DB) validateSpan(start, end []byte) error {
	if bytes.Compare(start, end) >= 0 {
		return ErrInvalidRange
	}
	if err := d.validateUserKey(start); err != nil {
		return err
	}
	if err := d.validateUserKey(end); err != nil {
		return err
	}
	return nil
}

func (d *DB) pointMetaKey(userKey []byte) []byte {
	out := make([]byte, len(d.pointPrefix)+len(userKey))
	copy(out, d.pointPrefix)
	copy(out[len(d.pointPrefix):], userKey)
	return out
}

func (d *DB) rangeLogKey(seq uint64, order uint32) []byte {
	out := make([]byte, len(d.rangePrefix)+12)
	copy(out, d.rangePrefix)
	binary.BigEndian.PutUint64(out[len(d.rangePrefix):], seq)
	binary.BigEndian.PutUint32(out[len(d.rangePrefix)+8:], order)
	return out
}

func (d *DB) parseRangeLogKey(key []byte) (seq uint64, order uint32, ok bool) {
	if len(key) != len(d.rangePrefix)+12 {
		return 0, 0, false
	}
	if !bytes.HasPrefix(key, d.rangePrefix) {
		return 0, 0, false
	}
	seq = binary.BigEndian.Uint64(key[len(d.rangePrefix):])
	order = binary.BigEndian.Uint32(key[len(d.rangePrefix)+8:])
	return seq, order, true
}

// Close closes the underlying TreeDB handle.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d == nil {
		return nil
	}
	var shadowErr error
	if d.shadow != nil {
		shadowErr = d.shadow.Close()
		d.shadow = nil
	}
	if d.tree == nil {
		return shadowErr
	}
	err := d.tree.Close()
	d.tree = nil
	if err == nil {
		return shadowErr
	}
	return err
}

// Get fetches a value, returning pebble.ErrNotFound when absent.
func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return nil, nil, err
	}
	if err := d.validateUserKey(key); err != nil {
		return nil, nil, err
	}
	val, err := d.tree.Get(key)
	if err != nil {
		return nil, nil, err
	}
	if val == nil {
		return nil, nil, pebble.ErrNotFound
	}
	return val, noopCloser{}, nil
}

// Set writes a point key.
func (d *DB) Set(key, value []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.Set(key, value, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// Merge merges with the configured Pebble merger semantics.
func (d *DB) Merge(key, value []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.Merge(key, value, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// Delete writes a tombstone.
func (d *DB) Delete(key []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.Delete(key, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// DeleteSized writes a sized tombstone.
func (d *DB) DeleteSized(key []byte, valueSize uint32, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.DeleteSized(key, valueSize, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// SingleDelete writes a single-delete tombstone.
func (d *DB) SingleDelete(key []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.SingleDelete(key, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// DeleteRange writes a range tombstone.
func (d *DB) DeleteRange(start, end []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(start, end, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// RangeKeySet writes a range key set.
func (d *DB) RangeKeySet(start, end, suffix, value []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.RangeKeySet(start, end, suffix, value, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// RangeKeyUnset writes a range key unset.
func (d *DB) RangeKeyUnset(start, end, suffix []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.RangeKeyUnset(start, end, suffix, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// RangeKeyDelete writes a range key delete.
func (d *DB) RangeKeyDelete(start, end []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.RangeKeyDelete(start, end, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

// LogData persists ignorable log data as a sequence-reserving no-op.
func (d *DB) LogData(data []byte, opts *pebble.WriteOptions) error {
	b := d.NewBatch()
	defer b.Close()
	if err := b.LogData(data, nil); err != nil {
		return err
	}
	return d.Apply(b, opts)
}

func syncFromWriteOptions(opts *pebble.WriteOptions) bool {
	return opts != nil && opts.Sync
}

// Apply applies a compatibility batch atomically.
func (d *DB) Apply(batch *Batch, opts *pebble.WriteOptions) error {
	if batch == nil {
		return nil
	}
	return d.ApplyBatchRepr(batch.Repr(), opts)
}

// ApplyNoSyncWait is equivalent to Apply for this wrapper.
func (d *DB) ApplyNoSyncWait(batch *Batch, opts *pebble.WriteOptions) error {
	return d.Apply(batch, opts)
}

// ApplyBatchRepr parses and applies a Pebble batch representation.
func (d *DB) ApplyBatchRepr(repr []byte, opts *pebble.WriteOptions) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return err
	}
	return d.applyBatchReprLocked(repr, syncFromWriteOptions(opts))
}

func isDeleteKind(kind pebble.InternalKeyKind) bool {
	switch kind {
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindSingleDelete, pebble.InternalKeyKindDeleteSized:
		return true
	default:
		return false
	}
}

func normalizePointKind(kind pebble.InternalKeyKind) pebble.InternalKeyKind {
	if kind == pebble.InternalKeyKindMerge {
		return pebble.InternalKeyKindSet
	}
	return kind
}

func finishValueMergerCompat(
	valueMerger pebble.ValueMerger, includesBase bool,
) (value []byte, needDelete bool, err error) {
	var closer io.Closer
	if deletable, ok := valueMerger.(pebble.DeletableValueMerger); ok {
		value, needDelete, closer, err = deletable.DeletableFinish(includesBase)
	} else {
		value, closer, err = valueMerger.Finish(includesBase)
	}
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, false, err
	}
	if value != nil {
		value = append([]byte(nil), value...)
	}
	if closer != nil {
		if closeErr := closer.Close(); closeErr != nil {
			return nil, false, closeErr
		}
	}
	return value, needDelete, nil
}

func (d *DB) mergeValueLocked(
	key, operand []byte,
	pending map[string]pointWrite,
) (pointWrite, error) {
	valueMerger, err := d.merger.Merge(key, operand)
	if err != nil {
		return pointWrite{}, err
	}
	if valueMerger == nil {
		return pointWrite{}, errors.New("pebblecompat: merger returned nil ValueMerger")
	}
	if cur, ok := pending[string(key)]; ok {
		if !isDeleteKind(cur.Kind) {
			if err := valueMerger.MergeOlder(cur.Value); err != nil {
				return pointWrite{}, err
			}
		}
	} else {
		base, err := d.tree.Get(key)
		if err != nil {
			return pointWrite{}, err
		}
		if base != nil {
			if err := valueMerger.MergeOlder(base); err != nil {
				return pointWrite{}, err
			}
		}
	}

	merged, needDelete, err := finishValueMergerCompat(valueMerger, true /* includesBase */)
	if err != nil {
		return pointWrite{}, err
	}
	if needDelete {
		return pointWrite{Key: key, Kind: pebble.InternalKeyKindDelete}, nil
	}
	if merged == nil {
		merged = []byte{}
	}
	return pointWrite{Key: key, Value: merged, Kind: pebble.InternalKeyKindSet}, nil
}

type pointWrite struct {
	Key   []byte
	Value []byte
	Kind  pebble.InternalKeyKind
	Seq   uint64
}

const (
	// Internal key kind values reserved by RocksDB/Pebble as historical no-op and
	// separator markers. Batch replay should accept and sequence-reserve these
	// kinds without mutating user-visible state.
	internalKeyKindNoop      pebble.InternalKeyKind = 13
	internalKeyKindSeparator pebble.InternalKeyKind = 17
)

func (d *DB) applyBatchReprLocked(repr []byte, sync bool) error {
	if len(repr) == 0 {
		return nil
	}
	reader, count := pebble.ReadBatch(repr)

	pending := make(map[string]pointWrite)
	rangeRecords := make([]rangeLogRecord, 0, 8)
	nextSeq := d.lastSeq
	seen := uint32(0)
	order := uint32(0)
	requiresShadowRebuild := false

	for seen < count {
		kind, key, value, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("pebblecompat: batch count mismatch header=%d parsed=%d", count, seen)
		}
		seen++
		nextSeq++

		key = append([]byte(nil), key...)
		value = append([]byte(nil), value...)

		switch kind {
		case pebble.InternalKeyKindSet,
			pebble.InternalKeyKindSetWithDelete:
			if err := d.validateUserKey(key); err != nil {
				return err
			}
			if value == nil {
				value = []byte{}
			}
			pending[string(key)] = pointWrite{Key: key, Value: value, Kind: kind, Seq: nextSeq}
			if kind == pebble.InternalKeyKindSetWithDelete {
				requiresShadowRebuild = true
			}

		case pebble.InternalKeyKindMerge:
			if err := d.validateUserKey(key); err != nil {
				return err
			}
			merged, err := d.mergeValueLocked(key, value, pending)
			if err != nil {
				return err
			}
			merged.Seq = nextSeq
			pending[string(key)] = merged

		case pebble.InternalKeyKindDelete,
			pebble.InternalKeyKindSingleDelete,
			pebble.InternalKeyKindDeleteSized:
			if err := d.validateUserKey(key); err != nil {
				return err
			}
			pending[string(key)] = pointWrite{Key: key, Kind: kind, Seq: nextSeq}

		case pebble.InternalKeyKindRangeDelete:
			if err := d.validateSpan(key, value); err != nil {
				return err
			}
			rangeRecords = append(rangeRecords, rangeLogRecord{
				Seq:   nextSeq,
				Order: order,
				Kind:  kind,
				Start: key,
				End:   value,
			})
			order++
			for k := range pending {
				if keyInRange([]byte(k), key, value) {
					delete(pending, k)
				}
			}

		case pebble.InternalKeyKindRangeKeySet,
			pebble.InternalKeyKindRangeKeyUnset,
			pebble.InternalKeyKindRangeKeyDelete:
			recs, err := decodeRangeKeyBatchRecord(kind, nextSeq, key, value, order)
			if err != nil {
				return err
			}
			for i := range recs {
				if err := d.validateSpan(recs[i].Start, recs[i].End); err != nil {
					return err
				}
			}
			rangeRecords = append(rangeRecords, recs...)
			order += uint32(len(recs))

		case pebble.InternalKeyKindLogData, pebble.InternalKeyKindIngestSST:
			// No-op data record. Sequence reservation is enough.

		case internalKeyKindNoop, internalKeyKindSeparator:
			// Historical no-op and separator kinds are accepted for replay parity.
			// The shadow Pebble Apply path cannot decode these kinds, so force a
			// shadow rebuild after applying to TreeDB.
			requiresShadowRebuild = true

		default:
			return fmt.Errorf("pebblecompat: unsupported batch kind %d", kind)
		}
	}

	if seen == 0 {
		return nil
	}

	batch := d.tree.NewBatch()
	if batch == nil {
		return ErrClosed
	}
	defer batch.Close()

	for i := range rangeRecords {
		rec := rangeRecords[i]
		if rec.Kind == pebble.InternalKeyKindRangeDelete {
			if err := d.addDeleteRangeExistingKeysLocked(batch, rec.Start, rec.End); err != nil {
				return err
			}
		}
		if err := batch.Set(d.rangeLogKey(rec.Seq, rec.Order), encodeRangeLogValue(rec)); err != nil {
			return err
		}
	}

	for _, write := range pending {
		if isDeleteKind(write.Kind) {
			if err := batch.Delete(write.Key); err != nil {
				return err
			}
		} else {
			if err := batch.Set(write.Key, write.Value); err != nil {
				return err
			}
		}
		if err := batch.Set(d.pointMetaKey(write.Key), encodePointMeta(write.Seq, normalizePointKind(write.Kind))); err != nil {
			return err
		}
	}

	if err := batch.Set(d.seqKey, encodeSeq(nextSeq)); err != nil {
		return err
	}

	var writeErr error
	if sync {
		writeErr = batch.WriteSync()
	} else {
		writeErr = batch.Write()
	}
	if writeErr != nil {
		return writeErr
	}
	if requiresShadowRebuild {
		if err := d.rebuildShadowFromTreeLocked(); err != nil {
			return err
		}
	} else {
		if err := d.applyBatchReprToShadowLocked(repr, sync); err != nil {
			return err
		}
	}
	d.lastSeq = nextSeq
	return nil
}

func (d *DB) rebuildShadowFromTreeLocked() error {
	if d.shadow != nil {
		if err := d.shadow.Close(); err != nil {
			return err
		}
		d.shadow = nil
	}
	return d.initShadowLocked()
}

func (d *DB) addDeleteRangeExistingKeysLocked(batch treedb.Batch, start, end []byte) error {
	iter, err := d.tree.Iterator(start, end)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Valid() {
		k := iter.KeyCopy(nil)
		iter.Next()
		if d.isInternalKey(k) {
			continue
		}
		if err := batch.Delete(k); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return nil
}

// Flush provides a durable boundary, similar to pebble.DB.Flush.
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return err
	}
	if err := d.tree.Checkpoint(); err != nil {
		return err
	}
	return d.shadow.Flush()
}

// Checkpoint creates a filesystem checkpoint when destDir is provided.
func (d *DB) Checkpoint(destDir string, opts ...pebble.CheckpointOption) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return err
	}
	decodedOpts, err := decodeCheckpointOptions(opts)
	if err != nil {
		return err
	}
	if decodedOpts.restrictToSpan {
		return ErrCheckpointOptionUnsupported
	}
	if err := d.tree.Checkpoint(); err != nil {
		return err
	}
	if destDir == "" {
		return nil
	}
	if d.dataDir == "" {
		return errors.New("pebblecompat: source data dir unavailable")
	}
	return copyTreeDBCheckpoint(d.dataDir, destDir)
}
