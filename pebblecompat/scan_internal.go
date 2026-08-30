package pebblecompat

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	pebblerangekey "github.com/cockroachdb/pebble/rangekey"
)

func (d *DB) loadRangeLogRecordsLocked() ([]rangeLogRecord, error) {
	start := append([]byte(nil), d.rangePrefix...)
	end := prefixUpperBound(d.rangePrefix)

	iter, err := d.tree.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var out []rangeLogRecord
	for iter.Valid() {
		k := iter.KeyCopy(nil)
		v := iter.ValueCopy(nil)
		iter.Next()

		seq, order, ok := d.parseRangeLogKey(k)
		if !ok {
			continue
		}
		rec, err := decodeRangeLogValue(v)
		if err != nil {
			return nil, err
		}
		rec.Seq = seq
		rec.Order = order
		out = append(out, rec)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func maskedByRangeDelete(key []byte, seq uint64, rangeDeletes []rangeLogRecord) bool {
	for i := range rangeDeletes {
		if rangeDeletes[i].Seq > seq && keyInRange(key, rangeDeletes[i].Start, rangeDeletes[i].End) {
			return true
		}
	}
	return false
}

func pointMetaIterationBounds(prefix, lower, upper []byte) ([]byte, []byte) {
	start := append([]byte(nil), prefix...)
	if lower != nil {
		start = append(start, lower...)
	}
	if upper != nil {
		end := append([]byte(nil), prefix...)
		end = append(end, upper...)
		return start, end
	}
	return start, prefixUpperBound(prefix)
}

// ScanInternal scans current internal state and invokes callbacks in Pebble form.
func (d *DB) ScanInternal(
	ctx context.Context,
	lower, upper []byte,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, iterInfo pebble.IteratorLevel) error,
	visitRangeDel func(start, end []byte, seqNum uint64) error,
	visitRangeKey func(start, end []byte, keys []pebblerangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureOpenLocked(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	records, err := d.loadRangeLogRecordsLocked()
	if err != nil {
		return err
	}

	rangeDeletes := make([]rangeLogRecord, 0, len(records))
	for i := range records {
		rec := records[i]
		clippedStart, clippedEnd, ok := clipRange(rec.Start, rec.End, lower, upper)
		if !ok {
			continue
		}
		switch rec.Kind {
		case pebble.InternalKeyKindRangeDelete:
			rangeDeletes = append(rangeDeletes, rec)
			if visitRangeDel != nil {
				if err := visitRangeDel(clippedStart, clippedEnd, rec.Seq); err != nil {
					return err
				}
			}
		case pebble.InternalKeyKindRangeKeySet,
			pebble.InternalKeyKindRangeKeyUnset,
			pebble.InternalKeyKindRangeKeyDelete:
			if visitRangeKey != nil {
				trailer := (rec.Seq << 8) | uint64(rec.Kind)
				keys := []pebblerangekey.Key{{
					Trailer: trailer,
					Suffix:  append([]byte(nil), rec.Suffix...),
					Value:   append([]byte(nil), rec.Value...),
				}}
				if err := visitRangeKey(clippedStart, clippedEnd, keys); err != nil {
					return err
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	if visitPointKey != nil {
		pointStart, pointEnd := pointMetaIterationBounds(d.pointPrefix, lower, upper)
		iter, err := d.tree.Iterator(pointStart, pointEnd)
		if err != nil {
			return err
		}
		defer iter.Close()

		for iter.Valid() {
			metaKey := iter.KeyCopy(nil)
			metaValue := iter.ValueCopy(nil)
			iter.Next()

			if !bytes.HasPrefix(metaKey, d.pointPrefix) {
				continue
			}
			userKey := append([]byte(nil), metaKey[len(d.pointPrefix):]...)
			if !keyInRange(userKey, lower, upper) {
				continue
			}
			pm, ok := decodePointMeta(metaValue)
			if !ok {
				return fmt.Errorf("pebblecompat: invalid point meta for key %x", userKey)
			}
			if maskedByRangeDelete(userKey, pm.Seq, rangeDeletes) {
				continue
			}

			ik := pebble.InternalKey{
				UserKey: userKey,
				Trailer: (pm.Seq << 8) | uint64(pm.Kind),
			}
			var lv pebble.LazyValue
			if isDeleteKind(pm.Kind) {
				lv = pebble.LazyValue{ValueOrHandle: nil}
			} else {
				val, err := d.tree.Get(userKey)
				if err != nil {
					return err
				}
				if val == nil {
					continue
				}
				lv = pebble.LazyValue{ValueOrHandle: val}
			}
			if err := visitPointKey(&ik, lv, pebble.IteratorLevel{Kind: pebble.IteratorLevelLSM, Level: 0}); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
	}

	if visitSharedFile != nil {
		// This wrapper does not currently maintain shared-object sstables.
		return nil
	}
	return nil
}
