package nativewire

import (
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type insertBatchFastRequest struct {
	collection       *collections.Collection
	format           collections.DocumentFormat
	ids              [][]byte
	docs             [][]byte
	ack              AckPolicy
	includeResultIDs bool
	includeMeta      bool
}

func (s *Server) handleInsertBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	resultIDs, actualAck, err := s.insertBatch(state, sections)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, resultIDs...)},
		ackMetaCounts(actualAck, responseMetaCount{key: "inserted_count", value: len(resultIDs)}),
	}, nil
}

func (s *Server) handleInsertBatchBody(state *connState, sections []iwire.Section, dst []byte, includeResultIDs, includeMeta bool) ([]byte, error) {
	resultIDs, actualAck, err := s.insertBatch(state, sections)
	if err != nil {
		return nil, err
	}
	return appendInsertBatchResponseBody(dst, resultIDs, actualAck, includeResultIDs, includeMeta)
}

func (s *Server) handleInsertBatchFastBody(req insertBatchFastRequest, dst []byte) ([]byte, error) {
	resultIDs, actualAck, err := s.insertBatchDecoded(req.collection, req.format, req.ids, req.docs, req.ack)
	if err != nil {
		return nil, err
	}
	return appendInsertBatchResponseBody(dst, resultIDs, actualAck, req.includeResultIDs, req.includeMeta)
}

func appendInsertBatchResponseBody(dst []byte, resultIDs [][]byte, actualAck iwire.AckPolicy, includeResultIDs, includeMeta bool) ([]byte, error) {
	if !includeResultIDs {
		if !includeMeta {
			return dst[:0], nil
		}
		return appendAckMetaSection(dst, actualAck, responseMetaCount{key: "inserted_count", value: len(resultIDs)})
	}
	idsLen := iwire.ByteVectorEncodedLen(resultIDs)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionDocumentIDs, 0, idsLen)
	if err != nil {
		return nil, err
	}
	body = iwire.AppendByteVectorWithEncodedLen(body, idsLen, resultIDs...)
	if !includeMeta {
		return body, nil
	}
	return appendAckMetaSection(body, actualAck, responseMetaCount{key: "inserted_count", value: len(resultIDs)})
}

func (s *Server) insertBatch(state *connState, sections []iwire.Section) ([][]byte, iwire.AckPolicy, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, 0, err
	}
	format, err := decodeDocumentFormatSection(sections)
	if err != nil {
		return nil, 0, err
	}
	var ids, docs [][]byte
	if state != nil {
		ids, docs, err = decodeIDsAndDocumentsInto(state.idsScratch, state.docsScratch, sections, s.limits)
		state.idsScratch = ids
		state.docsScratch = docs
	} else {
		ids, docs, err = decodeIDsAndDocuments(sections, s.limits)
	}
	if err != nil {
		return nil, 0, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, 0, err
	}
	return s.insertBatchDecoded(collection, format, ids, docs, ack)
}

func (s *Server) insertBatchDecoded(collection *collections.Collection, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([][]byte, iwire.AckPolicy, error) {
	if err := s.admitMutationAck(ack); err != nil {
		return nil, 0, err
	}
	var err error
	var resultIDs [][]byte
	if format == collections.DocumentFormatBSON {
		if err := validateBSONDocuments(docs); err != nil {
			return nil, 0, err
		}
		resultIDs, err = collection.InsertBatchValidatedBSON(ids, docs)
	} else {
		resultIDs, err = collection.InsertBatch(ids, docs)
	}
	if err != nil {
		return nil, 0, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, 0, err
	}
	return resultIDs, actualAck, nil
}

func (s *Server) decodeInsertBatchFastRequest(state *connState, sections []iwire.Section) (insertBatchFastRequest, bool, error) {
	if len(sections) == 0 || sections[0].ID != iwire.SectionCommandHeader {
		return insertBatchFastRequest{}, false, nil
	}
	header, err := iwire.DecodeCommandHeader(sections[0].Bytes)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	if header.ID != iwire.CommandInsertBatch {
		return insertBatchFastRequest{}, false, nil
	}
	if header.Version != 1 {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrUnsupportedVersion, "unsupported command %d version %d", header.ID, header.Version)
	}

	var seen [128]bool
	var rawCollection, rawFormat, rawIDs, rawDocs, rawAck []byte
	for _, section := range sections {
		switch section.ID {
		case iwire.SectionCommandHeader,
			iwire.SectionDeadline,
			iwire.SectionTraceContext,
			iwire.SectionAckPolicy,
			iwire.SectionConsistencyPolicy,
			iwire.SectionIdempotencyKey,
			iwire.SectionChecksum,
			iwire.SectionCompression,
			iwire.SectionCollectionRef,
			iwire.SectionDocumentFormat,
			iwire.SectionDocumentIDs,
			iwire.SectionDocuments,
			iwire.SectionTemplateRecords,
			iwire.SectionExpectedCatalogVersion:
			if err := markInsertBatchFastSection(&seen, section.ID); err != nil {
				return insertBatchFastRequest{}, true, err
			}
		default:
			if section.Critical() {
				return insertBatchFastRequest{}, true, protocolError(iwire.ErrUnsupportedFeature, "unknown critical section %d", section.ID)
			}
			continue
		}

		switch section.ID {
		case iwire.SectionCollectionRef:
			rawCollection = section.Bytes
		case iwire.SectionDocumentFormat:
			rawFormat = section.Bytes
		case iwire.SectionDocumentIDs:
			rawIDs = section.Bytes
		case iwire.SectionDocuments:
			rawDocs = section.Bytes
		case iwire.SectionAckPolicy:
			rawAck = section.Bytes
		}
	}
	if !seen[iwire.SectionCommandHeader] {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionCommandHeader)
	}
	if !seen[iwire.SectionCollectionRef] {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionCollectionRef)
	}
	if !seen[iwire.SectionDocumentFormat] {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionDocumentFormat)
	}
	if !seen[iwire.SectionDocumentIDs] {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionDocumentIDs)
	}
	if !seen[iwire.SectionDocuments] {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionDocuments)
	}
	_, collection, err := s.openCollectionRawRef(state, rawCollection)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	format, err := decodeDocumentFormatPayload(rawFormat)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	var ids, docs [][]byte
	if state != nil {
		ids, err = decodeByteVectorBorrowedInto(state.idsScratch, rawIDs, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeByteVectorBorrowed(rawIDs, s.limits)
	}
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	if state != nil {
		docs, err = decodeByteVectorBorrowedInto(state.docsScratch, rawDocs, s.limits)
		state.docsScratch = docs
	} else {
		docs, err = decodeByteVectorBorrowed(rawDocs, s.limits)
	}
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	if len(ids) != len(docs) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "document_ids length %d does not match documents length %d", len(ids), len(docs))
	}
	if err := rejectDuplicateIDs(ids); err != nil {
		return insertBatchFastRequest{}, true, err
	}
	ack := s.defaultAckPolicy
	if seen[iwire.SectionAckPolicy] {
		decodedAck, err := ackPolicyFromPayload(rawAck)
		if err != nil {
			return insertBatchFastRequest{}, true, err
		}
		if decodedAck != 0 {
			ack = decodedAck
		}
	}
	return insertBatchFastRequest{
		collection:       collection,
		format:           format,
		ids:              ids,
		docs:             docs,
		ack:              ack,
		includeResultIDs: header.Flags&iwire.CommandFlagOmitResultIDs == 0,
		includeMeta:      header.Flags&iwire.CommandFlagOmitResponseMeta == 0,
	}, true, nil
}

func markInsertBatchFastSection(seen *[128]bool, id iwire.SectionID) error {
	if id >= iwire.SectionID(len(seen)) {
		return nil
	}
	if seen[id] {
		return protocolError(iwire.ErrInvalidCommand, "duplicate singleton section %d", id)
	}
	seen[id] = true
	return nil
}

func (s *Server) handleReplaceBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	format, err := decodeDocumentFormatSection(sections)
	if err != nil {
		return nil, err
	}
	if err := validateReplacementMode(sections); err != nil {
		return nil, err
	}
	var ids, docs [][]byte
	if state != nil {
		ids, docs, err = decodeIDsAndDocumentsInto(state.idsScratch, state.docsScratch, sections, s.limits)
		state.idsScratch = ids
		state.docsScratch = docs
	} else {
		ids, docs, err = decodeIDsAndDocuments(sections, s.limits)
	}
	if err != nil {
		return nil, err
	}
	if format == collections.DocumentFormatBSON {
		if err := validateBSONDocuments(docs); err != nil {
			return nil, err
		}
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.admitMutationAck(ack); err != nil {
		return nil, err
	}
	results, err := collection.UpdateBatch(updateBatchItems(ids, docs))
	if err != nil {
		return nil, metadataWrap(err)
	}
	matched, modified := 0, 0
	for _, result := range results {
		if result.Matched {
			matched++
		}
		if result.Modified {
			modified++
		}
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{ackMetaCounts(actualAck,
		responseMetaCount{key: "matched_count", value: matched},
		responseMetaCount{key: "modified_count", value: modified},
	)}, nil
}

func (s *Server) handleDeleteBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	var ids [][]byte
	if state != nil {
		ids, err = decodeIDVectorInto(state.idsScratch, sections, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeIDVector(sections, s.limits)
	}
	if err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.admitMutationAck(ack); err != nil {
		return nil, err
	}
	deleted, err := collection.DeleteBatch(ids)
	if err != nil {
		return nil, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{ackMetaCounts(actualAck, responseMetaCount{key: "deleted_count", value: deleted})}, nil
}

func (s *Server) handleFlushCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, iwire.AckFlushed)
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(iwire.AckFlushed, ack); err != nil {
		return nil, err
	}
	if err := collection.Flush(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{ackMeta(iwire.AckFlushed)}, nil
}

func (s *Server) handleFlushAll(sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, iwire.AckFlushed)
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(iwire.AckFlushed, ack); err != nil {
		return nil, err
	}
	if err := s.collections.FlushAll(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{ackMeta(iwire.AckFlushed)}, nil
}

func (s *Server) handleCheckpoint(sections []iwire.Section) ([]iwire.Section, error) {
	if s.backend == nil {
		return nil, protocolError(iwire.ErrDurabilityUnavailable, "checkpoint requires a backend DB")
	}
	ack, err := ackPolicyFromSections(sections, iwire.AckSynced)
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(iwire.AckSynced, ack); err != nil {
		return nil, err
	}
	if s.collections != nil {
		if err := s.collections.FlushAll(); err != nil {
			return nil, metadataWrap(err)
		}
	}
	if err := s.backend.Checkpoint(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{ackMeta(iwire.AckSynced)}, nil
}

func (s *Server) admitBarrierAck(actual, requested iwire.AckPolicy) error {
	switch requested {
	case 0, iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced:
	case iwire.AckRaftCommitted:
		return protocolError(iwire.ErrDurabilityUnavailable, "raft_committed ack is unavailable in single-node nativewire R1")
	default:
		return protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", requested)
	}
	if requested == 0 || requested <= actual {
		return nil
	}
	return protocolError(iwire.ErrDurabilityUnavailable, "requested ack policy %d cannot be satisfied by barrier ack policy %d", requested, actual)
}

func (s *Server) admitMutationAck(requested iwire.AckPolicy) error {
	switch requested {
	case 0, iwire.AckVisible, iwire.AckFlushed:
		return nil
	case iwire.AckSynced:
		if s.backend == nil {
			return protocolError(iwire.ErrDurabilityUnavailable, "synced ack requires a backend DB")
		}
		return nil
	case iwire.AckRaftCommitted:
		return protocolError(iwire.ErrDurabilityUnavailable, "raft_committed ack is unavailable in single-node nativewire R1")
	default:
		return protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", requested)
	}
}

func validateBSONDocuments(docs [][]byte) error {
	for i, doc := range docs {
		if err := bson.Raw(doc).Validate(); err != nil {
			return protocolError(iwire.ErrInvalidCommand, "invalid BSON document at index %d: %v", i, err)
		}
	}
	return nil
}

func (s *Server) satisfyAck(collection interface{ Flush() error }, requested iwire.AckPolicy) (iwire.AckPolicy, error) {
	switch requested {
	case 0, iwire.AckVisible:
		return iwire.AckVisible, nil
	case iwire.AckFlushed:
		if collection != nil {
			if err := collection.Flush(); err != nil {
				return 0, metadataWrap(err)
			}
		}
		return iwire.AckFlushed, nil
	case iwire.AckSynced:
		if collection != nil {
			if err := collection.Flush(); err != nil {
				return 0, metadataWrap(err)
			}
		}
		if s.backend == nil {
			return 0, protocolError(iwire.ErrDurabilityUnavailable, "synced ack requires a backend DB")
		}
		if err := s.backend.Checkpoint(); err != nil {
			return 0, metadataWrap(err)
		}
		return iwire.AckSynced, nil
	case iwire.AckRaftCommitted:
		return 0, protocolError(iwire.ErrDurabilityUnavailable, "raft_committed ack is unavailable in single-node nativewire R1")
	default:
		return 0, protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", requested)
	}
}
