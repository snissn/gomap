package nativewire

import (
	"strconv"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (s *Server) handleInsertBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	format, err := decodeDocumentFormatSection(sections)
	if err != nil {
		return nil, err
	}
	ids, docs, err := decodeIDsAndDocuments(sections, s.limits)
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
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	var resultIDs [][]byte
	if format == collections.DocumentFormatBSON {
		if err := validateBSONDocuments(docs); err != nil {
			return nil, err
		}
		resultIDs, err = collection.InsertBatchValidatedBSON(ids, docs)
	} else {
		resultIDs, err = collection.InsertBatch(ids, docs)
	}
	if err != nil {
		return nil, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, resultIDs...)},
		ackMeta(actualAck, "inserted_count", strconv.Itoa(len(resultIDs))),
	}, nil
}

func (s *Server) handleReplaceBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
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
	ids, docs, err := decodeIDsAndDocuments(sections, s.limits)
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
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
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
	return []iwire.Section{ackMeta(actualAck,
		"matched_count", strconv.Itoa(matched),
		"modified_count", strconv.Itoa(modified),
	)}, nil
}

func (s *Server) handleDeleteBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	ids, err := decodeIDVector(sections, s.limits)
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
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	deleted, err := collection.DeleteBatch(ids)
	if err != nil {
		return nil, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{ackMeta(actualAck, "deleted_count", strconv.Itoa(deleted))}, nil
}

func (s *Server) handleFlushCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultBarrierAck(iwire.AckFlushed))
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(iwire.AckFlushed, ack); err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
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
	ack, err := ackPolicyFromSections(sections, s.defaultBarrierAck(iwire.AckFlushed))
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
	ack, err := ackPolicyFromSections(sections, s.defaultBarrierAck(iwire.AckSynced))
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

func (s *Server) defaultBarrierAck(minimum iwire.AckPolicy) iwire.AckPolicy {
	if s != nil && s.defaultAckPolicy > minimum {
		return s.defaultAckPolicy
	}
	return minimum
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
