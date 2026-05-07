package nativewire

import (
	"errors"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (s *Server) handleGetManyBody(state *connState, sections []iwire.Section, dst []byte) ([]byte, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	var ids [][]byte
	if state != nil {
		ids, err = decodeByteVectorBorrowedInto(state.idsScratch, rawIDs, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeByteVectorBorrowed(rawIDs, s.limits)
	}
	if err != nil {
		return nil, err
	}

	lengths := make([]int, len(ids))
	presence := make([]byte, (len(ids)+7)/8)
	payload := make([]byte, 0, getManyPayloadCapacityHint(len(ids), s.limits))
	for i, id := range ids {
		start := len(payload)
		doc, found, err := collection.GetInto(id, payload[start:start])
		if err != nil {
			return nil, metadataWrap(err)
		}
		if !found {
			continue
		}
		presence[i/8] |= 1 << uint(i%8)
		lengths[i] = len(doc)
		if len(doc) == 0 {
			continue
		}
		if cap(payload) >= start+len(doc) {
			next := payload[:start+len(doc)]
			if &doc[0] == &next[start] {
				payload = next
				continue
			}
		}
		payload = append(payload, doc...)
	}

	docSectionLen, err := iwire.ByteVectorPayloadEncodedLen(lengths, len(payload))
	if err != nil {
		return nil, err
	}
	if err := s.checkResponseSectionLen("presence_bitmap", len(presence)); err != nil {
		return nil, err
	}
	if err := s.checkResponseSectionLen("documents", docSectionLen); err != nil {
		return nil, err
	}
	presenceBodyLen, err := responseSectionBodyLen(iwire.SectionPresenceBitmap, len(presence))
	if err != nil {
		return nil, err
	}
	docBodyLen, err := responseSectionBodyLen(iwire.SectionDocuments, docSectionLen)
	if err != nil {
		return nil, err
	}
	bodyLen := presenceBodyLen + docBodyLen
	if bodyLen < presenceBodyLen {
		return nil, protocolError(iwire.ErrResourceExhausted, "response body length overflow")
	}
	if err := s.checkResponseBodyLen(bodyLen); err != nil {
		return nil, err
	}
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionPresenceBitmap, 0, len(presence))
	if err != nil {
		return nil, err
	}
	body = append(body, presence...)
	body, err = iwire.AppendSectionHeader(body, iwire.SectionDocuments, 0, docSectionLen)
	if err != nil {
		return nil, err
	}
	return iwire.AppendByteVectorPayload(body, lengths, payload)
}

func (s *Server) checkResponseSectionLen(name string, sectionLen int) error {
	if sectionLen < 0 {
		return protocolError(iwire.ErrMalformedFrame, "%s section length is negative", name)
	}
	if uint64(sectionLen) > s.limits.MaxSectionLen {
		return protocolError(iwire.ErrResourceExhausted, "%s section length %d exceeds limit %d", name, sectionLen, s.limits.MaxSectionLen)
	}
	return nil
}

func responseSectionBodyLen(id iwire.SectionID, sectionLen int) (uint64, error) {
	if sectionLen < 0 {
		return 0, protocolError(iwire.ErrMalformedFrame, "response section length is negative")
	}
	headerLen := iwire.SectionHeaderEncodedLen(id, 0, sectionLen)
	return uint64(headerLen) + uint64(sectionLen), nil
}

func (s *Server) checkResponseBodyLen(bodyLen uint64) error {
	frameLen := uint64(iwire.FrameHeaderLenV1) + bodyLen
	if frameLen < bodyLen {
		return protocolError(iwire.ErrResourceExhausted, "response frame length overflow")
	}
	if frameLen > s.limits.MaxFrameSize {
		return protocolError(iwire.ErrResourceExhausted, "response frame length %d exceeds limit %d", frameLen, s.limits.MaxFrameSize)
	}
	return nil
}

func getManyPayloadCapacityHint(count int, limits iwire.Limits) int {
	if count <= 0 || count > maxInt/64 {
		return 0
	}
	hint := count * 64
	maxBytes := limits.MaxByteVectorBytes
	if maxBytes > 0 && uint64(hint) > maxBytes {
		return int(maxBytes)
	}
	return hint
}

func (s *Server) handleIndexLookup(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	indexName, value, err := indexLookupRequest(sections)
	if err != nil {
		return nil, err
	}
	limits, err := optionalCursorLimits(sections)
	if err != nil {
		return nil, err
	}
	var ids [][]byte
	truncated := false
	if limits.MaxItems > 0 {
		ids, truncated, err = collection.FindByIndexValueLimit(indexName, value, limits.MaxItems)
		if err != nil {
			return nil, metadataWrap(err)
		}
	} else {
		ids, err = collection.FindByIndexValue(indexName, value)
		if err != nil {
			return nil, metadataWrap(err)
		}
	}
	ids, truncated = applyIDByteLimit(ids, limits.MaxBytes, truncated)
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		{ID: iwire.SectionTruncated, Bytes: appendBool(nil, truncated)},
	}, nil
}

func applyIDByteLimit(ids [][]byte, maxBytes int, truncated bool) ([][]byte, bool) {
	if maxBytes <= 0 || len(ids) == 0 {
		return ids, truncated
	}
	end := 0
	bytes := 0
	for end < len(ids) {
		nextBytes := len(ids[end])
		if end > 0 && bytes+nextBytes > maxBytes {
			break
		}
		bytes += nextBytes
		end++
		if bytes >= maxBytes {
			break
		}
	}
	if end < len(ids) {
		return ids[:end], true
	}
	return ids, truncated
}

func (s *Server) handleIndexRange(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	indexName, opts, limits, err := s.indexRangeRequest(sections)
	if err != nil {
		return nil, err
	}
	ids, truncated, err := collection.FindByIndexRange(indexName, opts)
	if err != nil {
		return nil, metadataWrap(err)
	}
	ids, truncated = applyIDByteLimit(ids, limits.MaxBytes, truncated)
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		{ID: iwire.SectionTruncated, Bytes: appendBool(nil, truncated)},
	}, nil
}

func (s *Server) handleOpenScan(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	limits, err := optionalCursorLimits(sections)
	if err != nil {
		return nil, err
	}
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	records, truncated, err := collection.ScanDocuments(s.maxScanDocuments)
	if err != nil {
		return nil, metadataWrap(err)
	}
	end, bytes := splitCursorBatch(records, 0, limits, s.defaultCursorBatchSize)
	if end < len(records) && documentRecordsBytes(records[end:]) > s.maxCursorRetainedBytes {
		return responseForRecords(records[:end], CursorMeta{Items: end, Bytes: bytes}, true)
	}
	cursorID, err := s.storeCursor(state.id, records, end, truncated)
	if err != nil {
		return nil, err
	}
	hasMore := cursorID != 0
	return responseForRecords(records[:end], CursorMeta{CursorID: cursorID, Items: end, Bytes: bytes, HasMore: hasMore}, truncated)
}

func (s *Server) handleCursorNext(state *connState, cursorID uint64, sections []iwire.Section) ([]iwire.Section, error) {
	if cursorID == 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_next requires stream_id")
	}
	if state == nil {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_next requires connection state")
	}
	limits, err := requiredCursorLimits(sections)
	if err != nil {
		return nil, err
	}
	s.cursorMu.Lock()
	cursor := s.cursors[cursorID]
	if cursor == nil || cursor.owner != state.id {
		s.cursorMu.Unlock()
		return nil, protocolError(iwire.ErrCursorNotFound, "cursor %d not found", cursorID)
	}
	start := cursor.pos
	end, bytes := splitCursorBatch(cursor.records, start, limits, s.defaultCursorBatchSize)
	batch := append([]collections.DocumentRecord(nil), cursor.records[start:end]...)
	cursor.pos = end
	cursor.lastUsed = time.Now()
	hasMore := cursor.pos < len(cursor.records)
	truncated := cursor.truncated
	if !hasMore {
		delete(s.cursors, cursorID)
		s.cursorCount.Add(-1)
		s.counters.inc("cursors.closed_total")
	}
	s.cursorMu.Unlock()
	meta := CursorMeta{CursorID: cursorID, Items: len(batch), Bytes: bytes, HasMore: hasMore}
	if !hasMore {
		meta.CursorID = 0
	}
	return responseForRecords(batch, meta, truncated)
}

func (s *Server) handleCursorClose(state *connState, cursorID uint64, sections []iwire.Section) ([]iwire.Section, error) {
	if cursorID == 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_close requires stream_id")
	}
	if state == nil {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_close requires connection state")
	}
	s.cursorMu.Lock()
	cursor := s.cursors[cursorID]
	if cursor == nil || cursor.owner != state.id {
		s.cursorMu.Unlock()
		return nil, protocolError(iwire.ErrCursorNotFound, "cursor %d not found", cursorID)
	}
	delete(s.cursors, cursorID)
	s.cursorMu.Unlock()
	s.cursorCount.Add(-1)
	s.counters.inc("cursors.closed_total")
	return nil, nil
}

func optionalCursorLimits(sections []iwire.Section) (CursorLimits, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCursorLimits)
	if err != nil || !ok {
		return CursorLimits{}, err
	}
	return decodeCursorLimits(raw)
}

func requiredCursorLimits(sections []iwire.Section) (CursorLimits, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCursorLimits)
	if err != nil {
		return CursorLimits{}, err
	}
	if !ok {
		return CursorLimits{}, protocolError(iwire.ErrInvalidCommand, "missing cursor_limits")
	}
	return decodeCursorLimits(raw)
}

func indexLookupRequest(sections []iwire.Section) (string, any, error) {
	raw, err := metadataSection(sections, iwire.SectionIndexName)
	if err != nil {
		return "", nil, err
	}
	indexName, err := decodeIndexName(raw)
	if err != nil {
		return "", nil, err
	}
	raw, err = metadataSection(sections, iwire.SectionIndexValue)
	if err != nil {
		return "", nil, err
	}
	value, err := decodeScalar(raw)
	return indexName, value, err
}

func (s *Server) indexRangeRequest(sections []iwire.Section) (string, collections.IndexRangeOptions, CursorLimits, error) {
	raw, err := metadataSection(sections, iwire.SectionIndexName)
	if err != nil {
		return "", collections.IndexRangeOptions{}, CursorLimits{}, err
	}
	indexName, err := decodeIndexName(raw)
	if err != nil {
		return "", collections.IndexRangeOptions{}, CursorLimits{}, err
	}
	opts := collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Unbounded: true},
		Upper: collections.IndexRangeBound{Unbounded: true},
	}
	if raw, ok, err := singletonSection(sections, iwire.SectionIndexLowerBound); err != nil {
		return "", collections.IndexRangeOptions{}, CursorLimits{}, err
	} else if ok {
		opts.Lower, err = decodeIndexBound(raw)
		if err != nil {
			return "", collections.IndexRangeOptions{}, CursorLimits{}, err
		}
	}
	if raw, ok, err := singletonSection(sections, iwire.SectionIndexUpperBound); err != nil {
		return "", collections.IndexRangeOptions{}, CursorLimits{}, err
	} else if ok {
		opts.Upper, err = decodeIndexBound(raw)
		if err != nil {
			return "", collections.IndexRangeOptions{}, CursorLimits{}, err
		}
	}
	limits, err := optionalCursorLimits(sections)
	if err != nil {
		return "", collections.IndexRangeOptions{}, CursorLimits{}, err
	}
	opts.Limit = limits.MaxItems
	if opts.Limit <= 0 && limits.MaxBytes > 0 && limits.MaxBytes < maxInt {
		opts.Limit = limits.MaxBytes + 1
		if s != nil && s.maxScanDocuments > 0 && opts.Limit > s.maxScanDocuments {
			opts.Limit = s.maxScanDocuments
		}
	}
	if opts.Limit < 0 {
		return "", collections.IndexRangeOptions{}, CursorLimits{}, errors.New("negative range limit")
	}
	return indexName, opts, limits, nil
}
