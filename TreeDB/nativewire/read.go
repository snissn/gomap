package nativewire

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type Scalar struct {
	Type  collections.IndexValueType
	Value any
}

type IndexRange struct {
	Lower          Scalar
	LowerInclusive bool
	LowerUnbounded bool
	Upper          Scalar
	UpperInclusive bool
	UpperUnbounded bool
	Limit          int
	MaxBytes       int
}

type CursorLimits struct {
	MaxItems int
	MaxBytes int
}

type CursorMeta struct {
	CursorID uint64
	Items    int
	Bytes    int
	HasMore  bool
}

func encodeScalar(value any) ([]byte, error) {
	dst := make([]byte, 0, 16)
	switch v := value.(type) {
	case string:
		dst = binary.AppendUvarint(dst, 1)
		return appendString(dst, v), nil
	case bool:
		dst = binary.AppendUvarint(dst, 2)
		return appendBool(dst, v), nil
	case int:
		dst = binary.AppendUvarint(dst, 3)
		return binary.AppendVarint(dst, int64(v)), nil
	case int64:
		dst = binary.AppendUvarint(dst, 3)
		return binary.AppendVarint(dst, v), nil
	case float64:
		dst = binary.AppendUvarint(dst, 4)
		return binary.LittleEndian.AppendUint64(dst, math.Float64bits(v)), nil
	default:
		return nil, protocolError(iwire.ErrInvalidCommand, "unsupported scalar type %T", value)
	}
}

func decodeScalar(src []byte) (any, error) {
	value, off, err := decodeScalarAt(src, 0)
	if err != nil {
		return nil, err
	}
	if off != len(src) {
		return nil, protocolError(iwire.ErrMalformedFrame, "scalar has %d trailing bytes", len(src)-off)
	}
	return value, nil
}

func decodeScalarAt(src []byte, off int) (any, int, error) {
	code, n, err := readUvarint(src[off:])
	if err != nil {
		return nil, 0, err
	}
	off += n
	switch code {
	case 1:
		value, err := readString(src, &off)
		return value, off, err
	case 2:
		value, err := readBool(src, &off)
		return value, off, err
	case 3:
		value, err := readVarint(src, &off)
		return value, off, err
	case 4:
		if len(src)-off < 8 {
			return nil, 0, protocolError(iwire.ErrMalformedFrame, "short float64 scalar")
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(src[off : off+8]))
		return value, off + 8, nil
	default:
		return nil, 0, protocolError(iwire.ErrInvalidCommand, "unsupported scalar code %d", code)
	}
}

func encodeIndexBound(value any, inclusive, unbounded bool) ([]byte, error) {
	dst := appendBool(nil, unbounded)
	dst = appendBool(dst, inclusive)
	if unbounded {
		return dst, nil
	}
	scalar, err := encodeScalar(value)
	if err != nil {
		return nil, err
	}
	return append(dst, scalar...), nil
}

func decodeIndexBound(src []byte) (collections.IndexRangeBound, error) {
	off := 0
	unbounded, err := readBool(src, &off)
	if err != nil {
		return collections.IndexRangeBound{}, err
	}
	inclusive, err := readBool(src, &off)
	if err != nil {
		return collections.IndexRangeBound{}, err
	}
	if unbounded {
		if off != len(src) {
			return collections.IndexRangeBound{}, protocolError(iwire.ErrMalformedFrame, "unbounded index bound has trailing bytes")
		}
		return collections.IndexRangeBound{Unbounded: true, Inclusive: inclusive}, nil
	}
	value, next, err := decodeScalarAt(src, off)
	if err != nil {
		return collections.IndexRangeBound{}, err
	}
	if next != len(src) {
		return collections.IndexRangeBound{}, protocolError(iwire.ErrMalformedFrame, "index bound has trailing bytes")
	}
	return collections.IndexRangeBound{Value: value, Inclusive: inclusive}, nil
}

func encodeCursorLimits(limits CursorLimits) []byte {
	dst := binary.AppendUvarint(nil, uint64(max(0, limits.MaxItems)))
	return binary.AppendUvarint(dst, uint64(max(0, limits.MaxBytes)))
}

func decodeCursorLimits(src []byte) (CursorLimits, error) {
	maxItems, off, err := readUvarint(src)
	if err != nil {
		return CursorLimits{}, err
	}
	maxBytes, n, err := readUvarint(src[off:])
	if err != nil {
		return CursorLimits{}, err
	}
	off += n
	if off != len(src) {
		return CursorLimits{}, protocolError(iwire.ErrMalformedFrame, "cursor_limits has trailing bytes")
	}
	if maxItems > uint64(maxInt) || maxBytes > uint64(maxInt) {
		return CursorLimits{}, protocolError(iwire.ErrResourceExhausted, "cursor limit exceeds int capacity")
	}
	return CursorLimits{MaxItems: int(maxItems), MaxBytes: int(maxBytes)}, nil
}

func encodeCursorRef(cursorID uint64) []byte {
	return binary.AppendUvarint(nil, cursorID)
}

func decodeCursorRef(src []byte) (uint64, error) {
	cursorID, n, err := readUvarint(src)
	if err != nil {
		return 0, err
	}
	if n != len(src) {
		return 0, protocolError(iwire.ErrMalformedFrame, "cursor_ref has trailing bytes")
	}
	return cursorID, nil
}

func encodeCursorMeta(meta CursorMeta) []byte {
	dst := binary.AppendUvarint(nil, meta.CursorID)
	dst = binary.AppendUvarint(dst, uint64(max(0, meta.Items)))
	dst = binary.AppendUvarint(dst, uint64(max(0, meta.Bytes)))
	return appendBool(dst, meta.HasMore)
}

func decodeCursorMeta(src []byte) (CursorMeta, error) {
	cursorID, off, err := readUvarint(src)
	if err != nil {
		return CursorMeta{}, err
	}
	items, n, err := readUvarint(src[off:])
	if err != nil {
		return CursorMeta{}, err
	}
	off += n
	bytes, n, err := readUvarint(src[off:])
	if err != nil {
		return CursorMeta{}, err
	}
	off += n
	hasMore, err := readBool(src, &off)
	if err != nil {
		return CursorMeta{}, err
	}
	if off != len(src) {
		return CursorMeta{}, protocolError(iwire.ErrMalformedFrame, "cursor_meta has trailing bytes")
	}
	if items > uint64(maxInt) || bytes > uint64(maxInt) {
		return CursorMeta{}, protocolError(iwire.ErrResourceExhausted, "cursor_meta exceeds int capacity")
	}
	return CursorMeta{CursorID: cursorID, Items: int(items), Bytes: int(bytes), HasMore: hasMore}, nil
}

func encodePresenceBitmap(present []bool) []byte {
	out := make([]byte, (len(present)+7)/8)
	for i, ok := range present {
		if ok {
			out[i/8] |= 1 << uint(i%8)
		}
	}
	return out
}

func appendPresenceBitmap(dst []byte, present []bool) []byte {
	n := (len(present) + 7) / 8
	start := len(dst)
	for i := 0; i < n; i++ {
		dst = append(dst, 0)
	}
	for i, ok := range present {
		if ok {
			dst[start+i/8] |= 1 << uint(i%8)
		}
	}
	return dst
}

func decodePresenceBitmap(src []byte, count int) ([]bool, error) {
	if len(src) != (count+7)/8 {
		return nil, protocolError(iwire.ErrMalformedFrame, "presence bitmap length %d want %d", len(src), (count+7)/8)
	}
	out := make([]bool, count)
	for i := range out {
		out[i] = src[i/8]&(1<<uint(i%8)) != 0
	}
	return out, nil
}

func decodeByteVectorCloned(src []byte, limits iwire.Limits) ([][]byte, error) {
	vec, err := iwire.DecodeByteVector(src, limits)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, vec.Len())
	for i := 0; i < vec.Len(); i++ {
		item, _ := vec.Item(i)
		out[i] = append([]byte(nil), item...)
	}
	return out, nil
}

func decodeByteVectorBorrowed(src []byte, limits iwire.Limits) ([][]byte, error) {
	return iwire.DecodeByteVectorItems(src, limits)
}

func decodeByteVectorBorrowedInto(dst [][]byte, src []byte, limits iwire.Limits) ([][]byte, error) {
	return iwire.DecodeByteVectorItemsInto(dst, src, limits)
}

func documentRecordsBytes(records []collections.DocumentRecord) int {
	total := 0
	for _, record := range records {
		total += len(record.ID) + len(record.Document)
	}
	return total
}

func splitCursorBatch(records []collections.DocumentRecord, start int, limits CursorLimits, defaultItems int) (end, bytes int) {
	maxItems := limits.MaxItems
	if maxItems <= 0 {
		maxItems = defaultItems
	}
	maxBytes := limits.MaxBytes
	if maxBytes <= 0 {
		maxBytes = maxInt
	}
	end = start
	for end < len(records) && end-start < maxItems {
		nextBytes := len(records[end].ID) + len(records[end].Document)
		if end > start && bytes+nextBytes > maxBytes {
			break
		}
		bytes += nextBytes
		end++
		if bytes >= maxBytes {
			break
		}
	}
	return end, bytes
}

func (s *Server) splitCursorBatchForWire(records []collections.DocumentRecord, start int, limits CursorLimits) (end, bytes int, err error) {
	end, bytes = splitCursorBatch(records, start, limits, s.defaultCursorBatchSize)
	if end <= start {
		return end, bytes, nil
	}
	if err := s.checkCursorResponseBounds(records[start:end], true); err == nil {
		return end, bytes, nil
	}
	lo, hi := start, end
	bestEnd, bestBytes := start, 0
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		midBytes := documentRecordsBytes(records[start:mid])
		err := s.checkCursorResponseBounds(records[start:mid], true)
		if err == nil {
			bestEnd, bestBytes = mid, midBytes
			lo = mid
			continue
		}
		hi = mid - 1
	}
	if bestEnd == start {
		return start, 0, protocolError(iwire.ErrResourceExhausted, "cursor response record exceeds frame limits")
	}
	return bestEnd, bestBytes, nil
}

func (s *Server) checkCursorResponseBounds(records []collections.DocumentRecord, includeTruncated bool) error {
	if s == nil {
		return nil
	}
	if s.limits.MaxSections > 0 {
		sections := 3
		if includeTruncated {
			sections++
		}
		if sections > s.limits.MaxSections {
			return protocolError(iwire.ErrResourceExhausted, "cursor response sections %d exceeds limit %d", sections, s.limits.MaxSections)
		}
	}
	idsLen, err := recordByteVectorEncodedLen(records, true)
	if err != nil {
		return err
	}
	docsLen, err := recordByteVectorEncodedLen(records, false)
	if err != nil {
		return err
	}
	if err := s.checkCursorSectionLen("document_ids", idsLen); err != nil {
		return err
	}
	if err := s.checkCursorSectionLen("documents", docsLen); err != nil {
		return err
	}
	bodyLen := sectionEnvelopeLen(iwire.SectionDocumentIDs, idsLen)
	bodyLen, err = addResponseLen(bodyLen, sectionEnvelopeLen(iwire.SectionDocuments, docsLen))
	if err != nil {
		return err
	}
	bodyLen, err = addResponseLen(bodyLen, sectionEnvelopeLen(iwire.SectionCursorMeta, maxCursorMetaEncodedLen()))
	if err != nil {
		return err
	}
	if includeTruncated {
		bodyLen, err = addResponseLen(bodyLen, sectionEnvelopeLen(iwire.SectionTruncated, 1))
		if err != nil {
			return err
		}
	}
	frameLen, err := addResponseLen(uint64(iwire.FrameHeaderLenV1), bodyLen)
	if err != nil {
		return err
	}
	if s.limits.MaxFrameSize > 0 && frameLen > s.limits.MaxFrameSize {
		return protocolError(iwire.ErrResourceExhausted, "cursor response frame length %d exceeds limit %d", frameLen, s.limits.MaxFrameSize)
	}
	return nil
}

func recordByteVectorEncodedLen(records []collections.DocumentRecord, ids bool) (int, error) {
	total := uvarintEncodedLen(uint64(len(records)))
	payload := 0
	for _, record := range records {
		itemLen := len(record.Document)
		if ids {
			itemLen = len(record.ID)
		}
		var err error
		total, err = addInt(total, uvarintEncodedLen(uint64(itemLen)))
		if err != nil {
			return 0, err
		}
		payload, err = addInt(payload, itemLen)
		if err != nil {
			return 0, err
		}
	}
	return addInt(total, payload)
}

func sectionEnvelopeLen(id iwire.SectionID, payloadLen int) uint64 {
	return uint64(uvarintEncodedLen(uint64(id)) + uvarintEncodedLen(0) + uvarintEncodedLen(uint64(payloadLen)) + payloadLen)
}

func maxCursorMetaEncodedLen() int {
	return 3*uvarintEncodedLen(uint64(maxInt)) + 1
}

func uvarintEncodedLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func addInt(a, b int) (int, error) {
	if b < 0 || a > maxInt-b {
		return 0, protocolError(iwire.ErrResourceExhausted, "response length exceeds int capacity")
	}
	return a + b, nil
}

func addResponseLen(a, b uint64) (uint64, error) {
	if a+b < a {
		return 0, protocolError(iwire.ErrResourceExhausted, "response length overflow")
	}
	return a + b, nil
}

func (s *Server) checkCursorSectionLen(name string, length int) error {
	if length < 0 {
		return protocolError(iwire.ErrMalformedFrame, "%s section length is negative", name)
	}
	if s.limits.MaxSectionLen > 0 && uint64(length) > s.limits.MaxSectionLen {
		return protocolError(iwire.ErrResourceExhausted, "%s section length %d exceeds limit %d", name, length, s.limits.MaxSectionLen)
	}
	return nil
}

func responseForRecords(records []collections.DocumentRecord, meta CursorMeta, truncated bool) ([]iwire.Section, error) {
	ids := make([][]byte, len(records))
	docs := make([][]byte, len(records))
	for i := range records {
		ids[i] = records[i].ID
		docs[i] = records[i].Document
	}
	sections := []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
		{ID: iwire.SectionCursorMeta, Bytes: encodeCursorMeta(meta)},
	}
	if truncated {
		sections = append(sections, iwire.Section{ID: iwire.SectionTruncated, Bytes: appendBool(nil, true)})
	}
	return sections, nil
}

func (s *Server) reapExpiredCursors() {
	if s == nil || s.cursorIdleTimeout == 0 || s.cursorCount.Load() == 0 {
		return
	}
	now := time.Now()
	interval := cursorReapInterval(s.cursorIdleTimeout)
	s.reapMu.Lock()
	if !s.nextReap.IsZero() && now.Before(s.nextReap) {
		s.reapMu.Unlock()
		return
	}
	s.nextReap = now.Add(interval)
	s.reapMu.Unlock()
	s.cursorMu.Lock()
	expired := 0
	for id, cursor := range s.cursors {
		if now.Sub(cursor.lastUsed) > s.cursorIdleTimeout {
			delete(s.cursors, id)
			expired++
			s.counters.inc("cursors.timeouts_total")
			s.counters.inc("cursors.closed_total")
		}
	}
	s.cursorMu.Unlock()
	if expired > 0 {
		s.cursorCount.Add(-int64(expired))
	}
}

func (s *Server) startCursorReaper() {
	if s == nil || s.cursorIdleTimeout == 0 {
		return
	}
	s.cursorReaperOnce.Do(func() {
		done := s.cursorReaperDone
		if done == nil {
			done = make(chan struct{})
			s.cursorReaperDone = done
		}
		go s.reapExpiredCursorsUntilDone(done)
	})
}

func (s *Server) stopCursorReaper() {
	if s == nil {
		return
	}
	s.cursorReaperStopOnce.Do(func() {
		if s.cursorReaperDone != nil {
			close(s.cursorReaperDone)
		}
	})
}

func (s *Server) reapExpiredCursorsUntilDone(done <-chan struct{}) {
	if s == nil || s.cursorIdleTimeout == 0 {
		return
	}
	interval := cursorReapInterval(s.cursorIdleTimeout)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reapExpiredCursors()
		case <-done:
			return
		}
	}
}

func cursorReapInterval(idleTimeout time.Duration) time.Duration {
	if idleTimeout <= time.Second {
		return idleTimeout
	}
	interval := idleTimeout / 4
	if interval < time.Second {
		return time.Second
	}
	return interval
}

func (s *Server) openCursorCount() int {
	if s == nil {
		return 0
	}
	return int(s.cursorCount.Load())
}

func (s *Server) killCursorsForOwner(owner uint64) {
	if s == nil {
		return
	}
	closed := 0
	s.cursorMu.Lock()
	for id, cursor := range s.cursors {
		if cursor.owner == owner {
			delete(s.cursors, id)
			closed++
		}
	}
	s.cursorMu.Unlock()
	if closed > 0 {
		s.cursorCount.Add(-int64(closed))
	}
	for i := 0; i < closed; i++ {
		s.counters.inc("cursors.closed_total")
	}
}

func (s *Server) storeCursor(owner uint64, records []collections.DocumentRecord, pos int, truncated bool) (uint64, error) {
	if len(records) <= pos {
		return 0, nil
	}
	tail := append([]collections.DocumentRecord(nil), records[pos:]...)
	clear(records[:pos])
	bytes := documentRecordsBytes(tail)
	if bytes > s.maxCursorRetainedBytes {
		return 0, protocolError(iwire.ErrResourceExhausted, "cursor retained bytes %d exceeds limit %d", bytes, s.maxCursorRetainedBytes)
	}
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	if len(s.cursors) >= s.maxOpenCursors {
		return 0, protocolError(iwire.ErrResourceExhausted, "open cursor limit reached")
	}
	id := s.nextCursor.Add(1)
	if s.cursors == nil {
		s.cursors = make(map[uint64]*serverCursor)
	}
	s.cursors[id] = &serverCursor{owner: owner, records: tail, lastUsed: time.Now(), bytes: bytes, truncated: truncated}
	s.cursorCount.Add(1)
	s.counters.inc("cursors.opened_total")
	return id, nil
}
