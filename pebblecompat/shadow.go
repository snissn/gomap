package pebblecompat

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func newShadowDB(merger *pebble.Merger) (*pebble.DB, error) {
	cmp := *pebble.DefaultComparer
	cmp.Split = func(key []byte) int { return len(key) }
	if merger == nil {
		merger = pebble.DefaultMerger
	}
	return pebble.Open("shadow", &pebble.Options{
		FS:                 vfs.NewMem(),
		Comparer:           &cmp,
		FormatMajorVersion: pebble.FormatRangeKeys,
		Merger:             merger,
	})
}

func writeOptionsFromSync(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}
	return pebble.NoSync
}

func (d *DB) initShadowLocked() error {
	if err := d.ensureOpenLocked(); err != nil {
		return err
	}
	shadow, err := newShadowDB(d.merger)
	if err != nil {
		return err
	}
	d.shadow = shadow

	iter, err := d.tree.Iterator(nil, nil)
	if err != nil {
		_ = d.shadow.Close()
		d.shadow = nil
		return err
	}
	for iter.Valid() {
		k := iter.KeyCopy(nil)
		v := iter.ValueCopy(nil)
		iter.Next()
		if d.isInternalKey(k) {
			continue
		}
		if err := d.shadow.Set(k, v, pebble.NoSync); err != nil {
			_ = iter.Close()
			_ = d.shadow.Close()
			d.shadow = nil
			return err
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		_ = d.shadow.Close()
		d.shadow = nil
		return err
	}
	if err := iter.Close(); err != nil {
		_ = d.shadow.Close()
		d.shadow = nil
		return err
	}

	records, err := d.loadRangeLogRecordsLocked()
	if err != nil {
		_ = d.shadow.Close()
		d.shadow = nil
		return err
	}
	for i := range records {
		if err := d.applyRangeRecordToShadowLocked(records[i], pebble.NoSync); err != nil {
			_ = d.shadow.Close()
			d.shadow = nil
			return err
		}
	}
	return nil
}

func (d *DB) applyBatchReprToShadowLocked(repr []byte, sync bool) error {
	if d.shadow == nil || len(repr) == 0 {
		return nil
	}
	b := &pebble.Batch{}
	if err := b.SetRepr(repr); err != nil {
		return err
	}
	defer b.Close()
	return d.shadow.Apply(b, writeOptionsFromSync(sync))
}

func (d *DB) applyRangeRecordToShadowLocked(rec rangeLogRecord, opts *pebble.WriteOptions) error {
	if d.shadow == nil {
		return ErrClosed
	}
	switch rec.Kind {
	case pebble.InternalKeyKindRangeDelete:
		return d.shadow.DeleteRange(rec.Start, rec.End, opts)
	case pebble.InternalKeyKindRangeKeySet:
		return d.shadow.RangeKeySet(rec.Start, rec.End, rec.Suffix, rec.Value, opts)
	case pebble.InternalKeyKindRangeKeyUnset:
		return d.shadow.RangeKeyUnset(rec.Start, rec.End, rec.Suffix, opts)
	case pebble.InternalKeyKindRangeKeyDelete:
		return d.shadow.RangeKeyDelete(rec.Start, rec.End, opts)
	default:
		return fmt.Errorf("pebblecompat: unsupported range record kind %d", rec.Kind)
	}
}

func (d *DB) mirrorExciseToShadowLocked(span pebble.KeyRange) error {
	if d.shadow == nil || !spanDefined(span) {
		return nil
	}
	if err := d.shadow.DeleteRange(span.Start, span.End, pebble.NoSync); err != nil {
		return err
	}
	return d.shadow.RangeKeyDelete(span.Start, span.End, pebble.NoSync)
}

func (d *DB) applySharedObjectRecordToShadowLocked(key, value []byte) error {
	if d.shadow == nil {
		return ErrClosed
	}
	if !d.isInternalKey(key) {
		return d.shadow.Set(key, value, pebble.NoSync)
	}
	if bytes.Equal(key, d.seqKey) {
		return nil
	}
	if bytes.HasPrefix(key, d.pointPrefix) {
		return nil
	}
	if bytes.HasPrefix(key, d.rangePrefix) {
		rec, err := decodeRangeLogValue(value)
		if err != nil {
			return err
		}
		return d.applyRangeRecordToShadowLocked(rec, pebble.NoSync)
	}
	return nil
}

// NewIter returns a Pebble iterator backed by the shadow engine.
func (d *DB) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return nil, err
	}
	if d.shadow == nil {
		return nil, ErrClosed
	}
	return d.shadow.NewIter(o)
}

// NewIterWithContext returns a Pebble iterator backed by the shadow engine.
func (d *DB) NewIterWithContext(ctx context.Context, o *pebble.IterOptions) (*pebble.Iterator, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return nil, err
	}
	if d.shadow == nil {
		return nil, ErrClosed
	}
	return d.shadow.NewIterWithContext(ctx, o)
}

// NewSnapshot returns a Pebble snapshot from the shadow engine.
func (d *DB) NewSnapshot() *pebble.Snapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return nil
	}
	if d.shadow == nil {
		return nil
	}
	return d.shadow.NewSnapshot()
}

// NewIndexedBatch returns an indexed compatibility batch.
func (d *DB) NewIndexedBatch() *Batch {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil || d.shadow == nil {
		return &Batch{db: d, batch: &pebble.Batch{}, indexed: true, closed: true}
	}
	return &Batch{db: d, batch: d.shadow.NewIndexedBatch(), indexed: true}
}

// NewIndexedBatchWithSize returns an indexed compatibility batch with a size hint.
func (d *DB) NewIndexedBatchWithSize(size int) *Batch {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil || d.shadow == nil {
		return &Batch{db: d, batch: &pebble.Batch{}, indexed: true, closed: true}
	}
	return &Batch{db: d, batch: d.shadow.NewIndexedBatchWithSize(size), indexed: true}
}
