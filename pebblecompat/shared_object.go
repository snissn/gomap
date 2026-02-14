package pebblecompat

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
	treedb "github.com/snissn/gomap/TreeDB"
)

const (
	sharedObjectExt        = ".pcobj"
	sharedObjectVersion    = uint64(1)
	sharedObjectIOBuffer   = 1 << 20
	sharedIngestBatchOps   = 8192
	sharedIngestBatchBytes = 4 << 20
	maxSharedFieldBytes    = 1 << 30
)

var sharedObjectMagic = []byte{'P', 'C', 'O', 'B', 'J', 'V', '1', 0}

// ExportStats captures export volume stats for shared object files.
type ExportStats struct {
	Records          int
	UserRecords      int
	PointMetaRecords int
	RangeRecords     int
	Bytes            uint64
	SourceSeq        uint64
}

type sharedObjectHeader struct {
	InternalPrefix []byte
	SpanStart      []byte
	SpanEnd        []byte
	SourceSeq      uint64
}

func isExportObjectPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), sharedObjectExt)
}

func writeUvarint(w *bufio.Writer, x uint64) error {
	var scratch [10]byte
	n := binary.PutUvarint(scratch[:], x)
	_, err := w.Write(scratch[:n])
	return err
}

func writeVarBytes(w *bufio.Writer, b []byte) error {
	if err := writeUvarint(w, uint64(len(b))); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err := w.Write(b)
	return err
}

func readVarBytes(r *bufio.Reader) ([]byte, bool, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true, nil
		}
		return nil, false, err
	}
	if n > maxSharedFieldBytes {
		return nil, false, fmt.Errorf("pebblecompat: shared object field too large: %d", n)
	}
	if n == 0 {
		return nil, false, nil
	}
	buf := make([]byte, int(n))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, false, err
	}
	return buf, false, nil
}

func writeSharedObjectHeader(w *bufio.Writer, h sharedObjectHeader) error {
	if _, err := w.Write(sharedObjectMagic); err != nil {
		return err
	}
	if err := writeUvarint(w, sharedObjectVersion); err != nil {
		return err
	}
	if err := writeVarBytes(w, h.InternalPrefix); err != nil {
		return err
	}
	if err := writeVarBytes(w, h.SpanStart); err != nil {
		return err
	}
	if err := writeVarBytes(w, h.SpanEnd); err != nil {
		return err
	}
	return writeUvarint(w, h.SourceSeq)
}

func readSharedObjectHeader(r *bufio.Reader) (sharedObjectHeader, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return sharedObjectHeader{}, err
	}
	if !bytes.Equal(magic[:], sharedObjectMagic) {
		return sharedObjectHeader{}, fmt.Errorf("pebblecompat: invalid shared object magic")
	}
	version, err := binary.ReadUvarint(r)
	if err != nil {
		return sharedObjectHeader{}, err
	}
	if version != sharedObjectVersion {
		return sharedObjectHeader{}, fmt.Errorf("pebblecompat: unsupported shared object version %d", version)
	}
	prefix, eof, err := readVarBytes(r)
	if err != nil {
		return sharedObjectHeader{}, err
	}
	if eof {
		return sharedObjectHeader{}, io.ErrUnexpectedEOF
	}
	start, eof, err := readVarBytes(r)
	if err != nil {
		return sharedObjectHeader{}, err
	}
	if eof {
		return sharedObjectHeader{}, io.ErrUnexpectedEOF
	}
	end, eof, err := readVarBytes(r)
	if err != nil {
		return sharedObjectHeader{}, err
	}
	if eof {
		return sharedObjectHeader{}, io.ErrUnexpectedEOF
	}
	seq, err := binary.ReadUvarint(r)
	if err != nil {
		return sharedObjectHeader{}, err
	}
	return sharedObjectHeader{
		InternalPrefix: prefix,
		SpanStart:      start,
		SpanEnd:        end,
		SourceSeq:      seq,
	}, nil
}

func spanDefined(span pebble.KeyRange) bool {
	return span.Start != nil || span.End != nil
}

func validateSpan(span pebble.KeyRange) error {
	if span.Start != nil && span.End != nil && bytes.Compare(span.Start, span.End) >= 0 {
		return ErrInvalidRange
	}
	return nil
}

func (d *DB) writeSharedObjectRecord(
	w *bufio.Writer,
	stats *ExportStats,
	key []byte,
	value []byte,
	recordKind int,
) error {
	if err := writeVarBytes(w, key); err != nil {
		return err
	}
	if err := writeVarBytes(w, value); err != nil {
		return err
	}
	stats.Records++
	stats.Bytes += uint64(len(key) + len(value))
	switch recordKind {
	case 1:
		stats.UserRecords++
	case 2:
		stats.PointMetaRecords++
	case 3:
		stats.RangeRecords++
	}
	return nil
}

func (d *DB) exportSharedObjectLocked(path string, span pebble.KeyRange) (ExportStats, error) {
	if err := d.ensureOpenLocked(); err != nil {
		return ExportStats{}, err
	}
	if err := validateSpan(span); err != nil {
		return ExportStats{}, err
	}

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return ExportStats{}, err
	}

	cleanup := func(closeErr error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return closeErr
	}

	w := bufio.NewWriterSize(f, sharedObjectIOBuffer)
	header := sharedObjectHeader{
		InternalPrefix: append([]byte(nil), d.internalPrefix...),
		SpanStart:      append([]byte(nil), span.Start...),
		SpanEnd:        append([]byte(nil), span.End...),
		SourceSeq:      d.lastSeq,
	}
	if err := writeSharedObjectHeader(w, header); err != nil {
		return ExportStats{}, cleanup(err)
	}

	stats := ExportStats{SourceSeq: d.lastSeq}

	userIter, err := d.tree.Iterator(span.Start, span.End)
	if err != nil {
		return ExportStats{}, cleanup(err)
	}
	for userIter.Valid() {
		k := userIter.KeyCopy(nil)
		v := userIter.ValueCopy(nil)
		userIter.Next()
		if d.isInternalKey(k) {
			continue
		}
		if err := d.writeSharedObjectRecord(w, &stats, k, v, 1); err != nil {
			_ = userIter.Close()
			return ExportStats{}, cleanup(err)
		}
	}
	if err := userIter.Error(); err != nil {
		_ = userIter.Close()
		return ExportStats{}, cleanup(err)
	}
	if err := userIter.Close(); err != nil {
		return ExportStats{}, cleanup(err)
	}

	metaStart, metaEnd := pointMetaIterationBounds(d.pointPrefix, span.Start, span.End)
	metaIter, err := d.tree.Iterator(metaStart, metaEnd)
	if err != nil {
		return ExportStats{}, cleanup(err)
	}
	for metaIter.Valid() {
		k := metaIter.KeyCopy(nil)
		v := metaIter.ValueCopy(nil)
		metaIter.Next()
		if err := d.writeSharedObjectRecord(w, &stats, k, v, 2); err != nil {
			_ = metaIter.Close()
			return ExportStats{}, cleanup(err)
		}
	}
	if err := metaIter.Error(); err != nil {
		_ = metaIter.Close()
		return ExportStats{}, cleanup(err)
	}
	if err := metaIter.Close(); err != nil {
		return ExportStats{}, cleanup(err)
	}

	rangeIter, err := d.tree.Iterator(d.rangePrefix, prefixUpperBound(d.rangePrefix))
	if err != nil {
		return ExportStats{}, cleanup(err)
	}
	for rangeIter.Valid() {
		k := rangeIter.KeyCopy(nil)
		v := rangeIter.ValueCopy(nil)
		rangeIter.Next()

		rec, err := decodeRangeLogValue(v)
		if err != nil {
			_ = rangeIter.Close()
			return ExportStats{}, cleanup(err)
		}
		clippedStart, clippedEnd, ok := clipRange(rec.Start, rec.End, span.Start, span.End)
		if !ok {
			continue
		}
		if !bytes.Equal(clippedStart, rec.Start) || !bytes.Equal(clippedEnd, rec.End) {
			rec.Start = clippedStart
			rec.End = clippedEnd
			v = encodeRangeLogValue(rec)
		}
		if err := d.writeSharedObjectRecord(w, &stats, k, v, 3); err != nil {
			_ = rangeIter.Close()
			return ExportStats{}, cleanup(err)
		}
	}
	if err := rangeIter.Error(); err != nil {
		_ = rangeIter.Close()
		return ExportStats{}, cleanup(err)
	}
	if err := rangeIter.Close(); err != nil {
		return ExportStats{}, cleanup(err)
	}

	if err := w.Flush(); err != nil {
		return ExportStats{}, cleanup(err)
	}
	if err := f.Sync(); err != nil {
		return ExportStats{}, cleanup(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return ExportStats{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return ExportStats{}, err
	}
	return stats, nil
}

// ExportSharedObject exports a point-in-time TreeDB-compatible object file.
func (d *DB) ExportSharedObject(path string, span pebble.KeyRange) (ExportStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.exportSharedObjectLocked(path, span)
}

func (d *DB) maybeFlushIngestBatch(
	batch *treedbBatchState,
	forceSync bool,
) error {
	if batch.ops == 0 {
		return nil
	}
	if forceSync {
		if err := batch.batch.WriteSync(); err != nil {
			return err
		}
	} else {
		if err := batch.batch.Write(); err != nil {
			return err
		}
	}
	if err := batch.batch.Close(); err != nil {
		return err
	}
	next := d.tree.NewBatchWithSize(sharedIngestBatchOps)
	if next == nil {
		return ErrClosed
	}
	batch.batch = next
	batch.ops = 0
	batch.bytes = 0
	return nil
}

type treedbBatchState struct {
	batch treedb.Batch
	ops   int
	bytes int
}

func (d *DB) queueSet(batch *treedbBatchState, key, value []byte) error {
	if err := batch.batch.Set(key, value); err != nil {
		return err
	}
	batch.ops++
	batch.bytes += len(key) + len(value)
	if batch.ops >= sharedIngestBatchOps || batch.bytes >= sharedIngestBatchBytes {
		return d.maybeFlushIngestBatch(batch, false)
	}
	return nil
}

func (d *DB) queueDelete(batch *treedbBatchState, key []byte) error {
	if err := batch.batch.Delete(key); err != nil {
		return err
	}
	batch.ops++
	batch.bytes += len(key)
	if batch.ops >= sharedIngestBatchOps || batch.bytes >= sharedIngestBatchBytes {
		return d.maybeFlushIngestBatch(batch, false)
	}
	return nil
}

func splitRangeRecordForExcise(rec rangeLogRecord, span pebble.KeyRange) []rangeLogRecord {
	if _, _, ok := clipRange(rec.Start, rec.End, span.Start, span.End); !ok {
		return []rangeLogRecord{rec}
	}
	leftStart, leftEnd, leftOK := clipRange(rec.Start, rec.End, nil, span.Start)
	rightStart, rightEnd, rightOK := clipRange(rec.Start, rec.End, span.End, nil)
	out := make([]rangeLogRecord, 0, 2)
	if leftOK {
		left := rec
		left.Start = leftStart
		left.End = leftEnd
		out = append(out, left)
	}
	if rightOK {
		right := rec
		right.Start = rightStart
		right.End = rightEnd
		out = append(out, right)
	}
	return out
}

func (d *DB) exciseSpanLocked(span pebble.KeyRange, sync bool) error {
	if err := validateSpan(span); err != nil {
		return err
	}
	if !spanDefined(span) {
		return nil
	}
	if err := d.mirrorExciseToShadowLocked(span); err != nil {
		return err
	}
	batch := d.tree.NewBatchWithSize(sharedIngestBatchOps)
	if batch == nil {
		return ErrClosed
	}
	state := &treedbBatchState{batch: batch}
	defer state.batch.Close()

	userIter, err := d.tree.Iterator(span.Start, span.End)
	if err != nil {
		return err
	}
	for userIter.Valid() {
		k := userIter.KeyCopy(nil)
		userIter.Next()
		if d.isInternalKey(k) {
			continue
		}
		if err := d.queueDelete(state, k); err != nil {
			_ = userIter.Close()
			return err
		}
	}
	if err := userIter.Error(); err != nil {
		_ = userIter.Close()
		return err
	}
	if err := userIter.Close(); err != nil {
		return err
	}

	metaStart, metaEnd := pointMetaIterationBounds(d.pointPrefix, span.Start, span.End)
	metaIter, err := d.tree.Iterator(metaStart, metaEnd)
	if err != nil {
		return err
	}
	for metaIter.Valid() {
		k := metaIter.KeyCopy(nil)
		metaIter.Next()
		if err := d.queueDelete(state, k); err != nil {
			_ = metaIter.Close()
			return err
		}
	}
	if err := metaIter.Error(); err != nil {
		_ = metaIter.Close()
		return err
	}
	if err := metaIter.Close(); err != nil {
		return err
	}

	maxOrderBySeq := make(map[uint64]uint32)
	rangeIter, err := d.tree.Iterator(d.rangePrefix, prefixUpperBound(d.rangePrefix))
	if err != nil {
		return err
	}
	for rangeIter.Valid() {
		k := rangeIter.KeyCopy(nil)
		rangeIter.Next()
		seq, order, ok := d.parseRangeLogKey(k)
		if !ok {
			_ = rangeIter.Close()
			return fmt.Errorf("pebblecompat: invalid range log key during excise")
		}
		if cur, exists := maxOrderBySeq[seq]; !exists || order > cur {
			maxOrderBySeq[seq] = order
		}
	}
	if err := rangeIter.Error(); err != nil {
		_ = rangeIter.Close()
		return err
	}
	if err := rangeIter.Close(); err != nil {
		return err
	}

	rangeIter, err = d.tree.Iterator(d.rangePrefix, prefixUpperBound(d.rangePrefix))
	if err != nil {
		return err
	}
	for rangeIter.Valid() {
		k := rangeIter.KeyCopy(nil)
		v := rangeIter.ValueCopy(nil)
		rangeIter.Next()

		seq, _, ok := d.parseRangeLogKey(k)
		if !ok {
			_ = rangeIter.Close()
			return fmt.Errorf("pebblecompat: invalid range log key during excise")
		}
		rec, err := decodeRangeLogValue(v)
		if err != nil {
			_ = rangeIter.Close()
			return err
		}
		if _, _, overlap := clipRange(rec.Start, rec.End, span.Start, span.End); !overlap {
			continue
		}

		if err := d.queueDelete(state, k); err != nil {
			_ = rangeIter.Close()
			return err
		}
		fragments := splitRangeRecordForExcise(rec, span)
		for i := range fragments {
			maxOrderBySeq[seq]++
			fragments[i].Seq = seq
			fragments[i].Order = maxOrderBySeq[seq]
			if err := d.queueSet(state, d.rangeLogKey(seq, fragments[i].Order), encodeRangeLogValue(fragments[i])); err != nil {
				_ = rangeIter.Close()
				return err
			}
		}
	}
	if err := rangeIter.Error(); err != nil {
		_ = rangeIter.Close()
		return err
	}
	if err := rangeIter.Close(); err != nil {
		return err
	}

	return d.maybeFlushIngestBatch(state, sync)
}
func (d *DB) ingestSharedObjectLocked(
	path string,
	sync bool,
	excise *pebble.KeyRange,
) (pebble.IngestOperationStats, error) {
	if err := d.ensureOpenLocked(); err != nil {
		return pebble.IngestOperationStats{}, err
	}

	f, err := os.Open(path)
	if err != nil {
		return pebble.IngestOperationStats{}, err
	}
	defer f.Close()

	stat, statErr := f.Stat()
	r := bufio.NewReaderSize(f, sharedObjectIOBuffer)
	header, err := readSharedObjectHeader(r)
	if err != nil {
		return pebble.IngestOperationStats{}, err
	}
	if !bytes.Equal(header.InternalPrefix, d.internalPrefix) {
		return pebble.IngestOperationStats{}, fmt.Errorf("pebblecompat: internal prefix mismatch in shared object")
	}

	if excise != nil {
		if err := d.exciseSpanLocked(*excise, false); err != nil {
			return pebble.IngestOperationStats{}, err
		}
	}

	batch := d.tree.NewBatchWithSize(sharedIngestBatchOps)
	if batch == nil {
		return pebble.IngestOperationStats{}, ErrClosed
	}
	state := &treedbBatchState{batch: batch}
	defer state.batch.Close()

	for {
		key, eof, err := readVarBytes(r)
		if err != nil {
			return pebble.IngestOperationStats{}, err
		}
		if eof {
			break
		}
		if len(key) == 0 {
			return pebble.IngestOperationStats{}, fmt.Errorf("pebblecompat: invalid zero-length key in shared object")
		}
		value, eof, err := readVarBytes(r)
		if err != nil {
			return pebble.IngestOperationStats{}, err
		}
		if eof {
			return pebble.IngestOperationStats{}, io.ErrUnexpectedEOF
		}
		if err := d.queueSet(state, key, value); err != nil {
			return pebble.IngestOperationStats{}, err
		}
		if err := d.applySharedObjectRecordToShadowLocked(key, value); err != nil {
			return pebble.IngestOperationStats{}, err
		}
	}

	targetSeq := d.lastSeq
	if header.SourceSeq > targetSeq {
		targetSeq = header.SourceSeq
	}
	if err := d.queueSet(state, d.seqKey, encodeSeq(targetSeq)); err != nil {
		return pebble.IngestOperationStats{}, err
	}
	if err := d.maybeFlushIngestBatch(state, sync); err != nil {
		return pebble.IngestOperationStats{}, err
	}
	d.lastSeq = targetSeq

	stats := pebble.IngestOperationStats{MemtableOverlappingFiles: 1}
	if statErr == nil {
		stats.Bytes = uint64(stat.Size())
	} else {
		stats.Bytes = uint64(state.bytes)
	}
	stats.ApproxIngestedIntoL0Bytes = stats.Bytes
	return stats, nil
}

// IngestSharedObject ingests a .pcobj shared object into the current DB.
func (d *DB) IngestSharedObject(path string, opts *pebble.WriteOptions) (pebble.IngestOperationStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ingestSharedObjectLocked(path, syncFromWriteOptions(opts), nil)
}

func (d *DB) ingestSharedObjectWithExcise(
	path string,
	opts *pebble.WriteOptions,
	excise *pebble.KeyRange,
) (pebble.IngestOperationStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ingestSharedObjectLocked(path, syncFromWriteOptions(opts), excise)
}
