package nativewire

import (
	"errors"
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
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	indexName, opts, err := indexRangeRequest(sections)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
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
