package mongogateway

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const mongoFilterWriteMaxAttempts = 4

func validateMongoWritePlan(plan findPlan) error {
	for _, predicates := range append([][]findPredicate{plan.predicates}, plan.orBranches...) {
		for _, predicate := range predicates {
			for _, value := range predicate.values {
				if value.Type == bson.TypeRegex {
					return errors.New("Mongo gateway filter writes require an indexed _id equality predicate or supported scalar predicate")
				}
			}
		}
	}
	return nil
}

func (s *Server) selectMongoFilterWriteKey(col *collections.Collection, plan findPlan) ([]byte, bool, error) {
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return nil, false, err
	}
	defer materializer.Close()
	var key []byte
	truncated, err := col.ScanDocumentsFunc(s.maxFindScanDocuments(), func(record collections.DocumentRecord) (bool, error) {
		doc, err := storedDocumentToBSON(col, materializer, record.Document)
		if err != nil {
			return false, err
		}
		match, err := documentMatchesPlan(doc, plan)
		if err != nil || !match {
			return err == nil, err
		}
		id := bson.Raw(doc).Lookup("_id")
		if id.IsZero() {
			return false, errors.New("Mongo gateway selected document has no _id")
		}
		key, err = encodePrimaryKey(id)
		return false, err
	})
	if err != nil {
		return nil, false, err
	}
	if truncated {
		return nil, false, fmt.Errorf("Mongo gateway filter write requires a bounded scan and exceeded %d documents", s.maxFindScanDocuments())
	}
	return key, len(key) != 0, nil
}

func mongoStoredDocumentMatchesPlan(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, stored []byte, plan findPlan) (bool, error) {
	raw, err := storedDocumentToBSON(col, materializer, stored)
	if err != nil {
		return false, err
	}
	return documentMatchesPlan(raw, plan)
}

func (s *Server) runMongoFilterUpdateOne(col *collections.Collection, update mongoUpdateItem) (bool, bool, error) {
	for attempt := 0; attempt < mongoFilterWriteMaxAttempts; attempt++ {
		key, found, err := s.selectMongoFilterWriteKey(col, update.plan)
		if err != nil || !found {
			return false, false, err
		}
		if s.filterWriteSelectedHook != nil {
			s.filterWriteSelectedHook()
		}
		materializer, err := storedDocumentMaterializerForCollection(col)
		if err != nil {
			return false, false, err
		}
		predicateMatched := false
		matched, modified, err := col.Update(key, func(stored []byte) ([]byte, bool, error) {
			match, err := mongoStoredDocumentMatchesPlan(col, materializer, stored, update.plan)
			if err != nil || !match {
				return nil, false, err
			}
			predicateMatched = true
			update.key = key
			return applyMongoUpdateToStoredDocument(col, materializer, update, stored)
		})
		_ = materializer.Close()
		if err != nil || predicateMatched {
			return matched, modified, err
		}
	}
	return false, false, fmt.Errorf("Mongo gateway filter write exceeded %d predicate-drift retries", mongoFilterWriteMaxAttempts)
}

func (s *Server) deleteMongoFilterOne(col *collections.Collection, plan findPlan) (bool, error) {
	for attempt := 0; attempt < mongoFilterWriteMaxAttempts; attempt++ {
		key, found, err := s.selectMongoFilterWriteKey(col, plan)
		if err != nil || !found {
			return false, err
		}
		if s.filterWriteSelectedHook != nil {
			s.filterWriteSelectedHook()
		}
		materializer, err := storedDocumentMaterializerForCollection(col)
		if err != nil {
			return false, err
		}
		predicateMatched := false
		deleted, err := col.DeleteDocumentIf(key, func(stored []byte) (bool, error) {
			match, err := mongoStoredDocumentMatchesPlan(col, materializer, stored, plan)
			predicateMatched = match
			return match, err
		})
		_ = materializer.Close()
		if err != nil || predicateMatched {
			return deleted, err
		}
	}
	return false, fmt.Errorf("Mongo gateway filter delete exceeded %d predicate-drift retries", mongoFilterWriteMaxAttempts)
}

func (s *Server) findAndModifyFilterExisting(col *collections.Collection, item mongoUpdateItem) (before, after wire.Document, matched bool, err error) {
	for attempt := 0; attempt < mongoFilterWriteMaxAttempts; attempt++ {
		key, found, err := s.selectMongoFilterWriteKey(col, item.plan)
		if err != nil || !found {
			return nil, nil, false, err
		}
		if s.filterWriteSelectedHook != nil {
			s.filterWriteSelectedHook()
		}
		item.key = key
		materializer, err := storedDocumentMaterializerForCollection(col)
		if err != nil {
			return nil, nil, false, err
		}
		predicateMatched := false
		matched, _, err = col.Update(key, func(stored []byte) ([]byte, bool, error) {
			raw, err := storedDocumentToBSON(col, materializer, stored)
			if err != nil {
				return nil, false, err
			}
			match, err := documentMatchesPlan(raw, item.plan)
			if err != nil || !match {
				return nil, false, err
			}
			predicateMatched, before = true, append(before[:0], raw...)
			var updated wire.Document
			var changed bool
			if item.pureSet && normalizedMongoUpdateDocumentFormat(col) == collections.DocumentFormatBSON && item.bsonSetFieldsOK {
				updated, changed, err = applyBSONSetUpdate(raw, item.bsonSetFields)
			} else if item.pureSet {
				updated, changed, err = applySetUpdate(raw, item.updateDoc)
			} else {
				updated, changed, err = applyMongoMutation(raw, item.mutation)
			}
			if err != nil {
				return nil, false, err
			}
			updatedKey, encoded, err := prepareInsertDocument(updated, col.MetaView().Options.DocumentFormat)
			if err != nil {
				return nil, false, err
			}
			if !bytes.Equal(updatedKey, key) {
				return nil, false, errors.New("Mongo gateway update cannot modify _id")
			}
			after = append(after[:0], updated...)
			if !changed {
				return nil, false, nil
			}
			return encoded, true, nil
		})
		_ = materializer.Close()
		if err != nil || predicateMatched {
			return finalizeFindAndModifyImages(before, after, matched && predicateMatched, err)
		}
	}
	return nil, nil, false, fmt.Errorf("Mongo gateway findAndModify exceeded %d predicate-drift retries", mongoFilterWriteMaxAttempts)
}
