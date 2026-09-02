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

func resetMongoFilterWriteAttempt(predicateMatched *bool) {
	*predicateMatched = false
}

func reconcileMongoFilterWriteOutcome(predicateMatched *bool, matched bool) {
	if !matched {
		resetMongoFilterWriteAttempt(predicateMatched)
	}
}

func resetMongoFindAndModifyAttempt(predicateMatched *bool, before, after *wire.Document) {
	*predicateMatched = false
	*before = nil
	*after = nil
}

func reconcileMongoFindAndModifyOutcome(predicateMatched *bool, before, after *wire.Document, matched bool) {
	if !matched {
		resetMongoFindAndModifyAttempt(predicateMatched, before, after)
	}
}

func validateMongoWritePlan(plan findPlan) error {
	branches := append([][]findPredicate{plan.predicates}, plan.orBranches...)
	branches = append(branches, plan.norBranches...)
	for _, predicates := range branches {
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
	return s.selectMongoFilterWriteKeyWithBudget(col, plan, nil)
}

// mongoWriteScanLookahead asks the collection scan for one additional record
// so a command can distinguish its own work cap from a naturally truncated
// scan. Saturate at maxInt: MaxFindScanDocuments=maxInt is a supported way to
// make the scan cap effectively unlimited.
func mongoWriteScanLookahead(remaining int) int {
	if remaining >= maxInt {
		return maxInt
	}
	return remaining + 1
}

func (s *Server) selectMongoFilterWriteKeyWithBudget(col *collections.Collection, plan findPlan, budget *mongoWriteBudget) ([]byte, bool, error) {
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return nil, false, err
	}
	defer materializer.Close()
	var key []byte
	limit := s.maxFindScanDocuments()
	if budget != nil && mongoWriteScanLookahead(budget.examinedRemaining) < limit {
		limit = mongoWriteScanLookahead(budget.examinedRemaining)
	}
	truncated, err := col.ScanDocumentsFunc(limit, func(record collections.DocumentRecord) (bool, error) {
		if err := budget.charge(); err != nil {
			return false, err
		}
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
		if err == nil {
			err = budget.reserveTargetKey(len(key))
		}
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
		key, found, err := s.selectMongoFilterWriteKeyWithBudget(col, update.plan, update.budget)
		if err != nil || !found {
			return false, false, err
		}
		if s.filterWriteSelectedHook != nil {
			s.filterWriteSelectedHook()
		}
		// Selection can block in a hook or concurrent writer. Check again at
		// the boundary immediately before the atomic mutation.
		if err := update.budget.checkDeadline(); err != nil {
			return false, false, err
		}
		materializer, err := storedDocumentMaterializerForCollection(col)
		if err != nil {
			return false, false, err
		}
		if s.filterWriteAfterMaterializerHook != nil {
			s.filterWriteAfterMaterializerHook()
		}
		// Template materializer setup can acquire or refresh a snapshot. Recheck
		// after that work and immediately before the atomic conditional update.
		if err := update.budget.checkDeadline(); err != nil {
			_ = materializer.Close()
			return false, false, err
		}
		predicateMatched := false
		matched, modified, err := col.Update(key, func(stored []byte) ([]byte, bool, error) {
			resetMongoFilterWriteAttempt(&predicateMatched)
			match, err := mongoStoredDocumentMatchesPlan(col, materializer, stored, update.plan)
			if err != nil || !match {
				return nil, false, err
			}
			predicateMatched = true
			update.key = key
			return applyMongoUpdateToStoredDocument(col, materializer, update, stored)
		})
		_ = materializer.Close()
		reconcileMongoFilterWriteOutcome(&predicateMatched, matched)
		if err != nil || predicateMatched {
			return matched, modified, err
		}
	}
	return false, false, fmt.Errorf("Mongo gateway filter write exceeded %d predicate-drift retries", mongoFilterWriteMaxAttempts)
}

// runMongoFilterUpdateMany selects in collection natural order, then rechecks
// the original predicate inside each selected key's atomic Update callback.
// This keeps concurrent predicate drift from mutating a document that no
// longer matches without rerunning selection (which would change natural-order
// and target-budget accounting). The shared scan cap is the command's explicit
// work bound.
func (s *Server) runMongoFilterUpdateMany(col *collections.Collection, update mongoUpdateItem) (int32, int32, bool, error) {
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return 0, 0, false, err
	}
	defer materializer.Close()
	keys := make([][]byte, 0)
	limit := s.maxFindScanDocuments()
	if update.budget != nil && mongoWriteScanLookahead(update.budget.examinedRemaining) < limit {
		limit = mongoWriteScanLookahead(update.budget.examinedRemaining)
	}
	truncated, err := col.ScanDocumentsFunc(limit, func(record collections.DocumentRecord) (bool, error) {
		if err := update.budget.charge(); err != nil {
			return false, err
		}
		doc, err := storedDocumentToBSON(col, materializer, record.Document)
		if err != nil {
			return false, err
		}
		match, err := documentMatchesPlan(doc, update.plan)
		if err != nil || !match {
			return err == nil, err
		}
		id := bson.Raw(doc).Lookup("_id")
		if id.IsZero() {
			return false, errors.New("Mongo gateway selected document has no _id")
		}
		key, err := encodePrimaryKey(id)
		if err != nil {
			return false, err
		}
		if err := update.budget.reserveTargetKey(len(key)); err != nil {
			return false, err
		}
		keys = append(keys, key)
		return true, nil
	})
	if err != nil {
		return 0, 0, false, err
	}
	if truncated {
		return 0, 0, false, fmt.Errorf("Mongo gateway multi update requires a bounded scan and exceeded %d documents", s.maxFindScanDocuments())
	}
	var matched, modified int32
	if s.filterWriteSelectedHook != nil && len(keys) != 0 {
		s.filterWriteSelectedHook()
	}
	for _, key := range keys {
		if err := update.budget.checkDeadline(); err != nil {
			return matched, modified, false, err
		}
		item := update
		item.multi, item.exactID, item.key = false, false, key
		predicateMatched := false
		matchedOne, modifiedOne, err := col.Update(key, func(stored []byte) ([]byte, bool, error) {
			resetMongoFilterWriteAttempt(&predicateMatched)
			match, matchErr := mongoStoredDocumentMatchesPlan(col, materializer, stored, update.plan)
			if matchErr != nil || !match {
				return nil, false, matchErr
			}
			predicateMatched = true
			return applyMongoUpdateToStoredDocument(col, materializer, item, stored)
		})
		if err != nil {
			return matched, modified, false, err
		}
		reconcileMongoFilterWriteOutcome(&predicateMatched, matchedOne)
		if predicateMatched {
			matched++
		}
		if modifiedOne {
			modified++
		}
	}
	return matched, modified, false, nil
}

func (s *Server) deleteMongoFilterMany(col *collections.Collection, plan findPlan, budget *mongoWriteBudget) (int32, error) {
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return 0, err
	}
	defer materializer.Close()
	keys := make([][]byte, 0)
	limit := s.maxFindScanDocuments()
	if budget != nil && mongoWriteScanLookahead(budget.examinedRemaining) < limit {
		limit = mongoWriteScanLookahead(budget.examinedRemaining)
	}
	truncated, err := col.ScanDocumentsFunc(limit, func(record collections.DocumentRecord) (bool, error) {
		if err := budget.charge(); err != nil {
			return false, err
		}
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
		key, err := encodePrimaryKey(id)
		if err != nil {
			return false, err
		}
		if err := budget.reserveTargetKey(len(key)); err != nil {
			return false, err
		}
		keys = append(keys, key)
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	if truncated {
		return 0, fmt.Errorf("Mongo gateway multi delete requires a bounded scan and exceeded %d documents", s.maxFindScanDocuments())
	}
	var deleted int32
	if s.filterWriteSelectedHook != nil && len(keys) != 0 {
		s.filterWriteSelectedHook()
	}
	for _, key := range keys {
		if err := budget.checkDeadline(); err != nil {
			return deleted, err
		}
		matched, err := col.DeleteDocumentIf(key, func(stored []byte) (bool, error) {
			return mongoStoredDocumentMatchesPlan(col, materializer, stored, plan)
		})
		if err != nil {
			return deleted, err
		}
		if matched {
			deleted++
		}
	}
	return deleted, nil
}

func (s *Server) deleteMongoFilterOne(col *collections.Collection, plan findPlan) (bool, error) {
	return s.deleteMongoFilterOneWithBudget(col, plan, nil)
}

func (s *Server) deleteMongoFilterOneWithBudget(col *collections.Collection, plan findPlan, budget *mongoWriteBudget) (bool, error) {
	for attempt := 0; attempt < mongoFilterWriteMaxAttempts; attempt++ {
		key, found, err := s.selectMongoFilterWriteKeyWithBudget(col, plan, budget)
		if err != nil || !found {
			return false, err
		}
		if s.filterWriteSelectedHook != nil {
			s.filterWriteSelectedHook()
		}
		// Do not let a selected-key retry mutate after the command deadline.
		if err := budget.checkDeadline(); err != nil {
			return false, err
		}
		materializer, err := storedDocumentMaterializerForCollection(col)
		if err != nil {
			return false, err
		}
		if s.filterWriteAfterMaterializerHook != nil {
			s.filterWriteAfterMaterializerHook()
		}
		// See update-one: snapshot/materializer setup itself is interruptible
		// work and must not let the following DeleteDocumentIf enter expired.
		if err := budget.checkDeadline(); err != nil {
			_ = materializer.Close()
			return false, err
		}
		predicateMatched := false
		deleted, err := col.DeleteDocumentIf(key, func(stored []byte) (bool, error) {
			resetMongoFilterWriteAttempt(&predicateMatched)
			match, err := mongoStoredDocumentMatchesPlan(col, materializer, stored, plan)
			predicateMatched = match
			return match, err
		})
		_ = materializer.Close()
		reconcileMongoFilterWriteOutcome(&predicateMatched, deleted)
		if err != nil || predicateMatched {
			return deleted, err
		}
	}
	return false, fmt.Errorf("Mongo gateway filter delete exceeded %d predicate-drift retries", mongoFilterWriteMaxAttempts)
}

func (s *Server) findAndModifyFilterExisting(col *collections.Collection, item mongoUpdateItem, newImage bool, projection compiledProjection) (before, after wire.Document, matched bool, err error) {
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
			resetMongoFindAndModifyAttempt(&predicateMatched, &before, &after)
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
			value := before
			if newImage {
				value = after
			}
			// Keep response admission in this conditional callback. In particular,
			// dotted projection can reject array traversal only after inspecting the
			// selected document, and that rejection must precede publication.
			if err := validateFindAndModifyResponse(value, true, false, bson.RawValue{}, projection, s.maxMessageLength()); err != nil {
				return nil, false, err
			}
			if !changed {
				return nil, false, nil
			}
			return encoded, true, nil
		})
		_ = materializer.Close()
		reconcileMongoFindAndModifyOutcome(&predicateMatched, &before, &after, matched)
		if err != nil || predicateMatched {
			return finalizeFindAndModifyImages(before, after, matched && predicateMatched, err)
		}
	}
	return nil, nil, false, fmt.Errorf("Mongo gateway findAndModify exceeded %d predicate-drift retries", mongoFilterWriteMaxAttempts)
}
