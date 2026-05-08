package nativewire

import (
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (s *Server) handleGetMany(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	ids, err := decodeByteVectorCloned(rawIDs, s.limits)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	docs := make([][]byte, len(ids))
	present := make([]bool, len(ids))
	for i, id := range ids {
		doc, err := collection.Get(id)
		if err != nil {
			return nil, metadataWrap(err)
		}
		if doc != nil {
			docs[i] = doc
			present[i] = true
		} else {
			docs[i] = []byte{}
		}
	}
	return []iwire.Section{
		{ID: iwire.SectionPresenceBitmap, Bytes: encodePresenceBitmap(present)},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
	}, nil
}

func (s *Server) handleIndexLookup(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
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
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
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

func (s *Server) indexRangeResultLimit(limits CursorLimits) int {
	limit := s.limits.MaxByteVectorItems
	if limit <= 0 {
		limit = iwire.DefaultLimits().MaxByteVectorItems
	}
	if limits.MaxItems > 0 && limits.MaxItems < limit {
		limit = limits.MaxItems
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
		if bytes+nextBytes > maxBytes {
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
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	indexName, opts, limits, err := indexRangeRequest(sections)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	opts.Limit = s.indexRangeResultLimit(limits)
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
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	limits, err := optionalCursorLimits(sections)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
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
	batch := append([]collections.DocumentRecord(nil), records[:end]...)
	cursorID, err := s.storeCursor(state.id, records, end, truncated)
	if err != nil {
		return nil, err
	}
	hasMore := cursorID != 0
	return responseForRecords(batch, CursorMeta{CursorID: cursorID, Items: end, Bytes: bytes, HasMore: hasMore, Truncated: truncated})
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
