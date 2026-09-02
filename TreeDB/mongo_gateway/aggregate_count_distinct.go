package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	mongoAggregateMaxStages             = 256
	mongoDistinctMaxEqualityComparisons = 65_536
)

var errMongoAggregateStageLimit = errors.New("Mongo gateway aggregate pipeline stage limit exceeded")

func (s *Server) countResponse(ctx context.Context, command wire.Document) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if err := validateMongoReadCommandFields(command, "count", map[string]struct{}{
		"count": {}, "query": {}, "skip": {}, "limit": {}, "readConcern": {},
	}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	_, _, name, doc, err := s.mongoCollectionReadCommand(command, "count")
	if err != nil || doc != nil {
		return doc, err
	}
	filter, err := commandOptionalDocument(command, "query")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	predicates, orBranches, norBranches, err := parseFindFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	skip, limit, err := parseFindPagination(command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	col, err := s.openCollectionCached(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return marshalDocument(bson.D{{Key: "n", Value: int64(0)}, {Key: "ok", Value: 1.0}})
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	result, err := s.executeFind(col, finalizeFindPlan(findPlan{predicates: predicates, orBranches: orBranches, norBranches: norBranches, skip: skip, limit: limit}))
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return marshalDocument(bson.D{{Key: "n", Value: int64(len(result.docs))}, {Key: "ok", Value: 1.0}})
}

func (s *Server) distinctResponse(ctx context.Context, command wire.Document) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if err := validateMongoReadCommandFields(command, "distinct", map[string]struct{}{
		"distinct": {}, "key": {}, "query": {}, "readConcern": {},
	}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	_, _, name, doc, err := s.mongoCollectionReadCommand(command, "distinct")
	if err != nil || doc != nil {
		return doc, err
	}
	field, err := commandString(command, "key")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if strings.Contains(field, ".") || strings.HasPrefix(field, "$") {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway distinct supports top-level fields only")
	}
	filter, err := commandOptionalDocument(command, "query")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	predicates, orBranches, norBranches, err := parseFindFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	col, err := s.openCollectionCached(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return marshalDocument(bson.D{{Key: "values", Value: bson.A{}}, {Key: "ok", Value: 1.0}})
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	result, err := s.executeFind(col, finalizeFindPlan(findPlan{predicates: predicates, orBranches: orBranches, norBranches: norBranches}))
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	values, err := s.distinctValues(result.docs, field)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return marshalDocument(bson.D{{Key: "values", Value: values}, {Key: "ok", Value: 1.0}})
}

func (s *Server) distinctValues(docs []wire.Document, field string) (bson.A, error) {
	limit := s.maxFindScanDocuments()
	values := make(bson.A, 0)
	rawValues := make([]bson.RawValue, 0)
	exact := make(map[string]struct{})
	remainingDecimal128Normalizations := mongoQueryMaxDecimal128Normalizations
	seenValues := 0
	equalityComparisons := 0
	outputBytes := findBatchOverheadBytes
	maxOutputBytes := s.maxFindBatchBytes()
	for _, doc := range docs {
		value := bson.Raw(doc).Lookup(field)
		if value.IsZero() {
			continue
		}
		visit := func(candidate bson.RawValue) error {
			seenValues++
			if seenValues > limit {
				return fmt.Errorf("Mongo gateway distinct requires a bounded value scan and exceeded %d values", limit)
			}
			key := string(append([]byte{byte(candidate.Type)}, candidate.Value...))
			if _, ok := exact[key]; ok {
				return nil
			}
			duplicate := false
			for _, existing := range rawValues {
				if !rawValuesMayNeedSemanticEquality(candidate, existing) {
					continue
				}
				equalityComparisons++
				if equalityComparisons > mongoDistinctMaxEqualityComparisons {
					return fmt.Errorf("Mongo gateway distinct equality exceeds %d comparisons", mongoDistinctMaxEqualityComparisons)
				}
				equal, err := rawValuesEqualModeBudget(candidate, existing, true, &remainingDecimal128Normalizations)
				if err != nil {
					return fmt.Errorf("Mongo gateway distinct equality exceeds %d Decimal128 normalizations", mongoQueryMaxDecimal128Normalizations)
				}
				if equal {
					duplicate = true
					break
				}
			}
			exact[key] = struct{}{}
			if duplicate {
				return nil
			}
			rawValues = append(rawValues, candidate)
			valueBytes := len(candidate.Value) + bsonArrayElementOverhead(len(values))
			if valueBytes > maxOutputBytes-outputBytes {
				return fmt.Errorf("Mongo gateway distinct result exceeds %d bytes", maxOutputBytes)
			}
			outputBytes += valueBytes
			values = append(values, candidate)
			return nil
		}
		if value.Type == bson.TypeArray {
			if err := forEachRawArrayValue(value, visit); err != nil {
				return nil, err
			}
		} else if err := visit(value); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func forEachRawArrayValue(value bson.RawValue, visit func(bson.RawValue) error) error {
	if value.Type != bson.TypeArray {
		return errors.New("Mongo gateway internal value is not a BSON array")
	}
	contents, ok := rawBSONContainerContents(value)
	if !ok {
		return errors.New("Mongo gateway distinct field contains malformed BSON array")
	}
	for len(contents) > 0 {
		element, remaining, ok := bsoncore.ReadElement(contents)
		if !ok || element.Validate() != nil {
			return errors.New("Mongo gateway distinct field contains malformed BSON array")
		}
		value, err := element.ValueErr()
		if err != nil {
			return err
		}
		if err := visit(bson.RawValue{Type: bson.Type(value.Type), Value: value.Data}); err != nil {
			return err
		}
		contents = remaining
	}
	return nil
}

func rawValuesMayNeedSemanticEquality(left, right bson.RawValue) bool {
	if left.IsNumber() && right.IsNumber() {
		if left.Type == right.Type && (left.Type == bson.TypeInt32 || left.Type == bson.TypeInt64) {
			return false
		}
		return true
	}
	return left.Type == bson.TypeEmbeddedDocument || left.Type == bson.TypeArray || left.Type == bson.TypeCodeWithScope ||
		right.Type == bson.TypeEmbeddedDocument || right.Type == bson.TypeArray || right.Type == bson.TypeCodeWithScope
}

func (s *Server) aggregateResponse(ctx context.Context, command wire.Document, cursorOwner int64) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if err := validateMongoReadCommandFields(command, "aggregate", map[string]struct{}{
		"aggregate": {}, "pipeline": {}, "cursor": {}, "readConcern": {},
	}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	db, collection, name, doc, err := s.mongoCollectionReadCommand(command, "aggregate")
	if err != nil || doc != nil {
		return doc, err
	}
	pipeline, err := commandBoundedDocumentArray(command, "pipeline", mongoAggregateMaxStages)
	if err != nil {
		if errors.Is(err, errMongoAggregateStageLimit) {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	cursor, err := requiredDocumentField(command, "cursor")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if err := validateAggregateCursor(cursor); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	batchSize, batchSizeSet, err := optionalInt32FieldWithPresence(cursor, "batchSize")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if _, err := normalizeBatchSize(int(batchSize), batchSizeSet, defaultCursorBatchSize); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	stages, err := parseAggregateStages(pipeline)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	col, err := s.openCollectionCached(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return marshalCursorResponse(db, collection, bson.A{})
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	plan := findPlan{}
	consumed := 0
	if len(stages) > 0 && stages[0].name == "$match" {
		plan = stages[0].plan
		consumed++
	}
	if len(stages) > consumed && stages[consumed].name == "$sort" {
		plan.sort = stages[consumed].plan.sort
		consumed++
	}
	if len(stages) > consumed && stages[consumed].name == "$skip" && stages[consumed].amount <= math.MaxInt32 {
		plan.skip = int32(stages[consumed].amount)
		consumed++
	}
	if len(stages) > consumed && stages[consumed].name == "$limit" && stages[consumed].amount <= math.MaxInt32 {
		plan.limit = int32(stages[consumed].amount)
		consumed++
	}
	plan = finalizeFindPlan(plan)
	stages = stages[consumed:]
	result, err := s.executeFind(col, plan)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	docs, err := executeAggregateStages(result.docs, stages)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	ns := db + "." + collection
	cursorID, firstBatch, err := s.openCursor(ns, docs, compiledProjection{}, int(batchSize), batchSizeSet, defaultCursorBatchSize, cursorOwner)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return marshalCursorResponseWithID(ns, cursorID, "firstBatch", firstBatch)
}

func commandBoundedDocumentArray(doc wire.Document, key string, limit int) ([]wire.Document, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return nil, fmt.Errorf("Mongo command missing %q", key)
	}
	if value.Type != bson.TypeArray {
		return nil, fmt.Errorf("Mongo command field %q must be an array", key)
	}
	contents, ok := rawBSONContainerContents(value)
	if !ok {
		return nil, fmt.Errorf("Mongo command field %q must be a valid BSON array", key)
	}
	out := make([]wire.Document, 0, min(limit, 8))
	for len(contents) > 0 {
		if len(out) == limit {
			return nil, fmt.Errorf("%w: maximum is %d stages", errMongoAggregateStageLimit, limit)
		}
		element, remaining, ok := bsoncore.ReadElement(contents)
		if !ok || element.Validate() != nil {
			return nil, fmt.Errorf("Mongo command field %q must be a valid BSON array", key)
		}
		rawValue, err := element.ValueErr()
		if err != nil {
			return nil, err
		}
		stage, ok := bson.RawValue{Type: bson.Type(rawValue.Type), Value: rawValue.Data}.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("Mongo command field %q[%d] must be a document", key, len(out))
		}
		out = append(out, wire.Document(stage))
		contents = remaining
	}
	return out, nil
}

type aggregateStage struct {
	name       string
	plan       findPlan
	projection compiledProjection
	amount     int
	countField string
}

func parseAggregateStages(pipeline []wire.Document) ([]aggregateStage, error) {
	stages := make([]aggregateStage, 0, len(pipeline))
	for i, stageDoc := range pipeline {
		elements, err := bson.Raw(stageDoc).Elements()
		if err != nil {
			return nil, err
		}
		if len(elements) != 1 {
			return nil, fmt.Errorf("Mongo gateway aggregate pipeline stage %d must contain exactly one field", i)
		}
		name, err := elements[0].KeyErr()
		if err != nil {
			return nil, err
		}
		value := elements[0].Value()
		stage := aggregateStage{name: name}
		switch name {
		case "$match":
			doc, ok := value.DocumentOK()
			if !ok {
				return nil, fmt.Errorf("Mongo gateway aggregate %s must be a document", name)
			}
			stage.plan.predicates, stage.plan.orBranches, stage.plan.norBranches, err = parseFindFilter(wire.Document(doc))
			stage.plan = finalizeFindPlan(stage.plan)
		case "$project":
			doc, ok := value.DocumentOK()
			if !ok {
				return nil, fmt.Errorf("Mongo gateway aggregate %s must be a document", name)
			}
			stage.projection, err = compileProjection(wire.Document(doc))
		case "$sort":
			doc, ok := value.DocumentOK()
			if !ok {
				return nil, fmt.Errorf("Mongo gateway aggregate %s must be a document", name)
			}
			stage.plan.sort, err = parseAggregateSort(wire.Document(doc))
		case "$skip":
			stage.amount, err = aggregateNonnegativeInteger(value, name)
		case "$limit":
			stage.amount, err = aggregatePositiveInteger(value, name)
		case "$count":
			stage.countField, err = aggregateCountField(value)
		case "$group":
			err = validateCountDocumentsGroup(value)
			stage.countField = "n"
			if i != len(pipeline)-1 {
				err = errors.New("Mongo gateway aggregate count $group must be the final stage")
			}
		default:
			return nil, fmt.Errorf("Mongo gateway aggregate does not support stage %q", name)
		}
		if err != nil {
			return nil, err
		}
		stages = append(stages, stage)
	}
	return stages, nil
}

func parseAggregateSort(doc wire.Document) (findSort, error) {
	return parseFindSortDocument(doc)
}

func aggregateNonnegativeInteger(value bson.RawValue, stage string) (int, error) {
	v, ok := strictBSONInt64(value)
	if !ok || v < 0 || uint64(v) > uint64(maxInt) {
		return 0, fmt.Errorf("Mongo gateway aggregate %s must be a non-negative supported integer", stage)
	}
	return int(v), nil
}

func aggregatePositiveInteger(value bson.RawValue, stage string) (int, error) {
	v, err := aggregateNonnegativeInteger(value, stage)
	if err != nil {
		return 0, err
	}
	if v == 0 {
		return 0, fmt.Errorf("Mongo gateway aggregate %s must be a positive supported integer", stage)
	}
	return v, nil
}

func aggregateCountField(value bson.RawValue) (string, error) {
	field, ok := value.StringValueOK()
	if !ok || field == "" || strings.HasPrefix(field, "$") || strings.Contains(field, ".") {
		return "", errors.New("Mongo gateway aggregate $count requires a supported output field name")
	}
	return field, nil
}

func validateCountDocumentsGroup(value bson.RawValue) error {
	doc, ok := value.DocumentOK()
	if !ok {
		return errors.New("Mongo gateway aggregate $group must be a document")
	}
	elements, err := doc.Elements()
	if err != nil || len(elements) != 2 || elements[0].Key() != "_id" || elements[1].Key() != "n" {
		return errors.New("Mongo gateway aggregate supports only the CountDocuments $group shape")
	}
	if id, ok := strictBSONInt64(elements[0].Value()); !ok || id != 1 {
		return errors.New("Mongo gateway aggregate supports only the CountDocuments $group shape")
	}
	sumDoc, ok := elements[1].Value().DocumentOK()
	if !ok {
		return errors.New("Mongo gateway aggregate supports only the CountDocuments $group shape")
	}
	sumElements, err := sumDoc.Elements()
	if err != nil || len(sumElements) != 1 || sumElements[0].Key() != "$sum" {
		return errors.New("Mongo gateway aggregate supports only the CountDocuments $group shape")
	}
	if sum, ok := strictBSONInt64(sumElements[0].Value()); !ok || sum != 1 {
		return errors.New("Mongo gateway aggregate supports only the CountDocuments $group shape")
	}
	return nil
}

func executeAggregateStages(docs []wire.Document, stages []aggregateStage) ([]wire.Document, error) {
	for _, stage := range stages {
		switch stage.name {
		case "$match":
			filtered := make([]wire.Document, 0, len(docs))
			for _, doc := range docs {
				match, err := documentMatchesPlan(doc, stage.plan)
				if err != nil {
					return nil, err
				}
				if match {
					filtered = append(filtered, doc)
				}
			}
			docs = filtered
		case "$project":
			projected := make([]wire.Document, 0, len(docs))
			for _, doc := range docs {
				out, err := projectDocumentWithProjection(doc, stage.projection)
				if err != nil {
					return nil, err
				}
				projected = append(projected, out)
			}
			docs = projected
		case "$sort":
			if err := validateFindSortDocuments(docs, stage.plan.sort); err != nil {
				return nil, err
			}
			sort.SliceStable(docs, func(i, j int) bool {
				return compareDocumentsForFindSort(docs[i], docs[j], stage.plan.sort) < 0
			})
		case "$skip":
			if stage.amount >= len(docs) {
				docs = nil
			} else {
				docs = docs[stage.amount:]
			}
		case "$limit":
			if stage.amount < len(docs) {
				docs = docs[:stage.amount]
			}
		case "$count", "$group":
			if len(docs) == 0 {
				docs = nil
				continue
			}
			countDoc, err := marshalDocument(bson.D{{Key: stage.countField, Value: int64(len(docs))}})
			if err != nil {
				return nil, err
			}
			docs = []wire.Document{countDoc}
		}
	}
	return docs, nil
}

func validateAggregateCursor(cursor wire.Document) error {
	elements, err := bson.Raw(cursor).Elements()
	if err != nil {
		return err
	}
	for _, element := range elements {
		if element.Key() != "batchSize" {
			return fmt.Errorf("Mongo gateway aggregate does not support cursor option %q", element.Key())
		}
	}
	return nil
}

func validateMongoReadCommandFields(command wire.Document, commandName string, commandFields map[string]struct{}) error {
	common := map[string]struct{}{
		"$db": {}, "lsid": {}, "$readPreference": {},
	}
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return err
	}
	for _, element := range elements {
		key := element.Key()
		if _, ok := commandFields[key]; ok {
			continue
		}
		if _, ok := common[key]; ok {
			continue
		}
		return fmt.Errorf("Mongo gateway %s does not support option %q", commandName, key)
	}
	return nil
}

func (s *Server) mongoCollectionReadCommand(command wire.Document, commandName string) (db, collection, name string, response wire.Document, err error) {
	if s.clusterSubmitterConfigured() {
		response, err = commandError(commandCodeBadValue, "BadValue", "Mongo gateway "+commandName+" is disabled in cluster mode until routed reads are supported")
		return
	}
	if s.Collections == nil {
		response, err = commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
		return
	}
	collection, err = commandString(command, commandName)
	if err != nil {
		response, err = commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return
	}
	db, err = commandString(command, "$db")
	if err != nil {
		response, err = commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return
	}
	name, err = gatewayCollectionName(db, collection)
	if err != nil {
		response, err = commandError(commandCodeBadValue, "BadValue", err.Error())
		return
	}
	return
}
