package nativewire

import (
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (s *Server) handleGetMany(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, docs, present, err := s.getManyDocuments(state, sections)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{
		{ID: iwire.SectionPresenceBitmap, Bytes: encodePresenceBitmap(present)},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
	}, nil
}

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

func (s *Server) getManyDocuments(state *connState, sections []iwire.Section) ([][]byte, [][]byte, []bool, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, nil, nil, err
	}
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, nil, nil, err
	}
	var ids [][]byte
	if state != nil {
		ids, err = decodeByteVectorBorrowedInto(state.idsScratch, rawIDs, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeByteVectorBorrowed(rawIDs, s.limits)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	docs := make([][]byte, len(ids))
	present := make([]bool, len(ids))
	for i, id := range ids {
		doc, err := collection.Get(id)
		if err != nil {
			return nil, nil, nil, metadataWrap(err)
		}
		if doc != nil {
			docs[i] = doc
			present[i] = true
		} else {
			docs[i] = []byte{}
		}
	}
	return ids, docs, present, nil
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
	queryLimit := s.indexLookupResultLimit(limits)
	ids, truncated, err = collection.FindByIndexValueLimit(indexName, value, queryLimit)
	if err != nil {
		return nil, metadataWrap(err)
	}
	ids, truncated = applyIDByteLimit(ids, limits.MaxBytes, truncated)
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		{ID: iwire.SectionTruncated, Bytes: appendBool(nil, truncated)},
	}, nil
}

func (s *Server) indexLookupResultLimit(limits CursorLimits) int {
	limit := s.limits.MaxByteVectorItems
	if limit <= 0 {
		limit = iwire.DefaultLimits().MaxByteVectorItems
	}
	if limits.MaxItems > 0 && limits.MaxItems < limit {
		limit = limits.MaxItems
	}
	if limits.MaxBytes > 0 && limits.MaxBytes < maxInt {
		byteBound := limits.MaxBytes + 1
		if byteBound < limit {
			limit = byteBound
		}
	}
	if limit <= 0 {
		return 1
	}
	return limit
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
	indexName, opts, limits, err := indexRangeRequest(sections)
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
	end, bytes, err := s.splitCursorBatchForWire(records, 0, limits)
	if err != nil {
		return nil, err
	}
	if end < len(records) && documentRecordsBytes(records[end:]) > s.maxCursorRetainedBytes {
		return responseForRecords(records[:end], CursorMeta{Items: end, Bytes: bytes, Truncated: true})
	}
	cursorID, err := s.storeCursor(state.id, records, end, truncated)
	if err != nil {
		return nil, err
	}
	hasMore := cursorID != 0
	return responseForRecords(records[:end], CursorMeta{CursorID: cursorID, Items: end, Bytes: bytes, HasMore: hasMore, Truncated: truncated})
}

func (s *Server) handleCursorNext(state *connState, cursorID uint64, sections []iwire.Section) ([]iwire.Section, error) {
	if state == nil {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_next requires connection state")
	}
	sectionCursorID, err := cursorRefFromSections(sections)
	if err != nil {
		return nil, err
	}
	if cursorID != 0 && cursorID != sectionCursorID {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_ref %d does not match stream_id %d", sectionCursorID, cursorID)
	}
	cursorID = sectionCursorID
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
	end, bytes, err := s.splitCursorBatchForWire(cursor.records, start, limits)
	if err != nil {
		s.cursorMu.Unlock()
		return nil, err
	}
	batch := append([]collections.DocumentRecord(nil), cursor.records[start:end]...)
	cursor.lastUsed = time.Now()
	hasMore := end < len(cursor.records)
	truncated := cursor.truncated
	if !hasMore {
		delete(s.cursors, cursorID)
		s.cursorCount.Add(-1)
		s.counters.inc("cursors.closed_total")
	} else {
		clear(cursor.records[:end])
		cursor.records = cursor.records[end:]
		cursor.pos = 0
		cursor.bytes = documentRecordsBytes(cursor.records)
	}
	s.cursorMu.Unlock()
	meta := CursorMeta{CursorID: cursorID, Items: len(batch), Bytes: bytes, HasMore: hasMore, Truncated: truncated}
	if !hasMore {
		meta.CursorID = 0
	}
	return responseForRecords(batch, meta)
}

func (s *Server) handleCursorClose(state *connState, cursorID uint64, sections []iwire.Section) ([]iwire.Section, error) {
	if state == nil {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_close requires connection state")
	}
	sectionCursorID, err := cursorRefFromSections(sections)
	if err != nil {
		return nil, err
	}
	if cursorID != 0 && cursorID != sectionCursorID {
		return nil, protocolError(iwire.ErrInvalidCommand, "cursor_ref %d does not match stream_id %d", sectionCursorID, cursorID)
	}
	cursorID = sectionCursorID
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

func cursorRefFromSections(sections []iwire.Section) (uint64, error) {
	raw, err := metadataSection(sections, iwire.SectionCursorRef)
	if err != nil {
		return 0, err
	}
	cursorID, err := decodeCursorRef(raw)
	if err != nil {
		return 0, err
	}
	if cursorID == 0 {
		return 0, protocolError(iwire.ErrInvalidCommand, "cursor_ref cannot be zero")
	}
	return cursorID, nil
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

func indexRangeRequest(sections []iwire.Section) (string, collections.IndexRangeOptions, CursorLimits, error) {
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
	return indexName, opts, limits, nil
}
