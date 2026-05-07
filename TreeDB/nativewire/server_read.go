package nativewire

import (
	"errors"
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
	ids, truncated, err := collection.FindByIndexValueLimit(indexName, value, max(1, limits.MaxItems))
	if err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		{ID: iwire.SectionTruncated, Bytes: appendBool(nil, truncated)},
	}, nil
}

func (s *Server) handleIndexRange(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	indexName, opts, err := indexRangeRequest(sections)
	if err != nil {
		return nil, err
	}
	ids, truncated, err := collection.FindByIndexRange(indexName, opts)
	if err != nil {
		return nil, metadataWrap(err)
	}
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
	cursorID, err := s.storeCursor(state.id, records, end)
	if err != nil {
		return nil, err
	}
	hasMore := cursorID != 0
	if truncated && !hasMore {
		hasMore = false
	}
	return responseForRecords(records[:end], CursorMeta{CursorID: cursorID, Items: end, Bytes: bytes, HasMore: hasMore})
}

func (s *Server) handleCursorNext(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	cursorID, err := cursorRefFromSections(sections)
	if err != nil {
		return nil, err
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
	return responseForRecords(batch, meta)
}

func (s *Server) handleCursorClose(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	cursorID, err := cursorRefFromSections(sections)
	if err != nil {
		return nil, err
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

func cursorRefFromSections(sections []iwire.Section) (uint64, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCursorRef)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, protocolError(iwire.ErrInvalidCommand, "missing cursor_ref")
	}
	return decodeCursorRef(raw)
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

func indexRangeRequest(sections []iwire.Section) (string, collections.IndexRangeOptions, error) {
	raw, err := metadataSection(sections, iwire.SectionIndexName)
	if err != nil {
		return "", collections.IndexRangeOptions{}, err
	}
	indexName, err := decodeIndexName(raw)
	if err != nil {
		return "", collections.IndexRangeOptions{}, err
	}
	var opts collections.IndexRangeOptions
	if raw, ok, err := singletonSection(sections, iwire.SectionIndexLowerBound); err != nil {
		return "", collections.IndexRangeOptions{}, err
	} else if ok {
		opts.Lower, err = decodeIndexBound(raw)
		if err != nil {
			return "", collections.IndexRangeOptions{}, err
		}
	}
	if raw, ok, err := singletonSection(sections, iwire.SectionIndexUpperBound); err != nil {
		return "", collections.IndexRangeOptions{}, err
	} else if ok {
		opts.Upper, err = decodeIndexBound(raw)
		if err != nil {
			return "", collections.IndexRangeOptions{}, err
		}
	}
	limits, err := optionalCursorLimits(sections)
	if err != nil {
		return "", collections.IndexRangeOptions{}, err
	}
	opts.Limit = limits.MaxItems
	if opts.Limit < 0 {
		return "", collections.IndexRangeOptions{}, errors.New("negative range limit")
	}
	return indexName, opts, nil
}
