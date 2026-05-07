package nativewire

import (
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

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

func (s *Server) handleInsertBatchBody(state *connState, sections []iwire.Section, dst []byte) ([]byte, error) {
	resultIDs, actualAck, err := s.insertBatch(state, sections)
	if err != nil {
		return nil, err
	}
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionDocumentIDs, 0, iwire.ByteVectorEncodedLen(resultIDs))
	if err != nil {
		return nil, err
	}
	body = iwire.AppendByteVector(body, resultIDs...)
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
	var resultIDs [][]byte
	if format == collections.DocumentFormatBSON {
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

func (s *Server) handleReplaceBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	if _, err := decodeDocumentFormatSection(sections); err != nil {
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
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
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
	if err := collection.Flush(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{ackMeta(iwire.AckFlushed)}, nil
}

func (s *Server) handleFlushAll() ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.collections.FlushAll(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{ackMeta(iwire.AckFlushed)}, nil
}

func (s *Server) handleCheckpoint() ([]iwire.Section, error) {
	if s.backend == nil {
		return nil, protocolError(iwire.ErrDurabilityUnavailable, "checkpoint requires a backend DB")
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
