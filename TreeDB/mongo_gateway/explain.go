package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type findExecutionStats struct {
	candidatesExamined     int64
	candidatesMaterialized int64
	documentsReturned      int64
	stage                  string
	indexName              string
}

func (p findPlan) recordWinner(stage, indexName string) {
	if p.stats != nil {
		p.stats.stage = stage
		p.stats.indexName = indexName
	}
}

func (p findPlan) recordCandidate() {
	if p.stats != nil {
		p.stats.candidatesExamined++
	}
}

func (p findPlan) recordCandidates(n int) {
	if p.stats != nil {
		p.stats.candidatesExamined += int64(n)
		p.stats.candidatesMaterialized += int64(n)
	}
}

func (p findPlan) recordMaterialized() {
	if p.stats != nil {
		p.stats.candidatesMaterialized++
	}
}

func (p findPlan) recordReturned(n int) {
	if p.stats != nil {
		p.stats.documentsReturned = int64(n)
	}
}

// explainResponse implements the deliberately small standalone explain surface.
// It reports gateway-owned planner terms only: it never serializes pages,
// roots, addresses, or ValueLog pointers.
func (s *Server) explainResponse(ctx context.Context, command wire.Document, cursorOwner int64) (wire.Document, error) {
	if err := validateMongoReadCommandFields(command, "explain", map[string]struct{}{
		"explain": {}, "verbosity": {},
	}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	inner, err := requiredDocumentField(command, "explain")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	verbosity, err := explainVerbosity(command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	name, err := mongoCommandName(inner)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	outerDB, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	switch name {
	case "find":
		return s.explainFindResponse(ctx, inner, outerDB, verbosity, cursorOwner)
	case "count":
		return s.explainCountResponse(ctx, inner, outerDB, verbosity)
	case "distinct":
		return s.explainDistinctResponse(ctx, inner, outerDB, verbosity)
	case "aggregate":
		return s.explainAggregateResponse(ctx, inner, outerDB, verbosity)
	default:
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway explain supports bounded standalone find, count, distinct, and aggregate reads only")
	}
}

func explainVerbosity(command wire.Document) (string, error) {
	value, present, err := optionalStringFieldWithPresence(command, "verbosity")
	if err != nil {
		return "", err
	}
	if !present || value == "" {
		return "queryPlanner", nil
	}
	if value != "queryPlanner" && value != "executionStats" {
		return "", fmt.Errorf("Mongo gateway explain does not support verbosity %q", value)
	}
	return value, nil
}

func mongoCommandName(command wire.Document) (string, error) {
	elements, err := bson.Raw(command).Elements()
	if err != nil || len(elements) == 0 {
		return "", errors.New("Mongo gateway explain requires a non-empty command document")
	}
	return elements[0].Key(), nil
}

func explainInnerDatabase(command wire.Document, outerDB string) (string, error) {
	value := bson.Raw(command).Lookup("$db")
	if value.IsZero() {
		return outerDB, nil
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return "", err
	}
	if db != outerDB {
		return "", errors.New("Mongo gateway explain inner $db must match outer $db")
	}
	return db, nil
}

func (s *Server) explainFindResponse(ctx context.Context, command wire.Document, outerDB, verbosity string, _ int64) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if s.clusterSubmitterConfigured() {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway explain is disabled in cluster mode until routed reads are supported")
	}
	if err := validateMongoReadCommandFields(command, "find", map[string]struct{}{
		"find": {}, "filter": {}, "projection": {}, "sort": {}, "skip": {}, "limit": {}, "batchSize": {}, "singleBatch": {}, "readConcern": {},
	}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "find")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := explainInnerDatabase(command, outerDB)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	filter, err := commandOptionalDocument(command, "filter")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	plan, err := parseFindPlan(command, filter)
	if err != nil {
		return explainPlannerRejected(db, collection, "unsupported_query", err.Error())
	}
	// This is deliberately before collection opening, matching find's routed
	// preflight boundary: cluster mode must not observe a local collection.
	if err := s.preflightClusterFindRoute(ctx, db, collection, plan); err != nil {
		return mongoClusterRouteCommandError(err)
	}
	col, err := s.openCollectionCached(name)
	missing := errors.Is(err, collections.ErrCollectionNotFound)
	if err != nil && !missing {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return s.explainPlannedRead(col, missing, db, collection, plan, verbosity, func(plan findPlan) (int64, error) {
		result, err := s.executeFind(col, plan)
		return int64(len(result.docs)), err
	})
}

func explainPlannerRejected(db, collection, reason, message string) (wire.Document, error) {
	return commandErrorWithFields(commandCodeBadValue, "BadValue", message, bson.D{{Key: "queryPlanner", Value: bson.D{
		{Key: "namespace", Value: db + "." + collection},
		{Key: "winningPlan", Value: bson.D{{Key: "stage", Value: "unsupported_route"}}},
		{Key: "rejectionReason", Value: reason},
		{Key: "cursorWork", Value: "none"},
	}}})
}

func (s *Server) explainCountResponse(ctx context.Context, command wire.Document, outerDB, verbosity string) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if err := validateMongoReadCommandFields(command, "count", map[string]struct{}{"count": {}, "query": {}, "skip": {}, "limit": {}, "readConcern": {}}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "count")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := explainInnerDatabase(command, outerDB)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	filter, err := commandOptionalDocument(command, "query")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	predicates, branches, err := parseFindFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	skip, limit, err := parseFindPagination(command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return s.explainCollectionRead(ctx, db, collection, findPlan{predicates: predicates, orBranches: branches, skip: skip, limit: limit}, verbosity, func(col *collections.Collection, plan findPlan) (int64, error) {
		result, err := s.executeFind(col, plan)
		return int64(len(result.docs)), err
	})
}

func (s *Server) explainDistinctResponse(ctx context.Context, command wire.Document, outerDB, verbosity string) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if err := validateMongoReadCommandFields(command, "distinct", map[string]struct{}{"distinct": {}, "key": {}, "query": {}, "readConcern": {}}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "distinct")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := explainInnerDatabase(command, outerDB)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
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
	predicates, branches, err := parseFindFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return s.explainCollectionRead(ctx, db, collection, findPlan{predicates: predicates, orBranches: branches}, verbosity, func(col *collections.Collection, plan findPlan) (int64, error) {
		result, err := s.executeFind(col, plan)
		if err != nil {
			return 0, err
		}
		values, err := s.distinctValues(result.docs, field)
		return int64(len(values)), err
	})
}

func (s *Server) explainAggregateResponse(ctx context.Context, command wire.Document, outerDB, verbosity string) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if err := validateMongoReadCommandFields(command, "aggregate", map[string]struct{}{"aggregate": {}, "pipeline": {}, "cursor": {}, "readConcern": {}}); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "aggregate")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := explainInnerDatabase(command, outerDB)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	pipeline, err := commandBoundedDocumentArray(command, "pipeline", mongoAggregateMaxStages)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if cursor, err := requiredDocumentField(command, "cursor"); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	} else if err := validateAggregateCursor(cursor); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	stages, err := parseAggregateStages(pipeline)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	plan, consumed := findPlan{}, 0
	if len(stages) > 0 && stages[0].name == "$match" {
		plan = stages[0].plan
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
	remaining := append([]aggregateStage(nil), stages[consumed:]...)
	// The shared find vocabulary can truthfully describe only the initial
	// match/skip/limit prefix. A later match or sort would add pipeline work
	// that is not a find-plan property, so fail closed instead of claiming the
	// find sort is satisfied or hiding in-memory work.
	for _, stage := range remaining {
		if stage.name == "$match" || stage.name == "$sort" {
			return explainPlannerRejected(db, collection, "unsupported_aggregate_pipeline", "Mongo gateway explain requires aggregate $match and $sort stages in the initial planner prefix")
		}
	}
	return s.explainCollectionRead(ctx, db, collection, plan, verbosity, func(col *collections.Collection, plan findPlan) (int64, error) {
		result, err := s.executeFind(col, plan)
		if err != nil {
			return 0, err
		}
		docs, err := executeAggregateStages(result.docs, remaining)
		return int64(len(docs)), err
	})
}

func (s *Server) explainCollectionRead(ctx context.Context, db, collection string, plan findPlan, verbosity string, execute func(*collections.Collection, findPlan) (int64, error)) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway explain is disabled in cluster mode until routed reads are supported")
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if err := s.preflightClusterFindRoute(ctx, db, collection, plan); err != nil {
		return mongoClusterRouteCommandError(err)
	}
	col, err := s.openCollectionCached(name)
	missing := errors.Is(err, collections.ErrCollectionNotFound)
	if err != nil && !missing {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return s.explainPlannedRead(col, missing, db, collection, plan, verbosity, func(plan findPlan) (int64, error) { return execute(col, plan) })
}

func (s *Server) explainPlannedRead(col *collections.Collection, missing bool, db, collection string, plan findPlan, verbosity string, execute func(findPlan) (int64, error)) (wire.Document, error) {
	selection := findPlannerSelection{stage: "bounded_scan"}
	if !missing {
		selection = explainPlannerSelection(col, plan)
	}
	winning := bson.D{{Key: "stage", Value: selection.stage}, {Key: "residualFilter", Value: len(plan.predicates) > 1 || len(plan.orBranches) != 0}}
	if selection.indexName != "" {
		winning = append(winning, bson.E{Key: "indexName", Value: selection.indexName})
	}
	if plan.sort.field != "" {
		winning = append(winning, bson.E{Key: "inMemorySort", Value: true})
	}
	planner := bson.D{
		{Key: "namespace", Value: db + "." + collection},
		{Key: "winningPlan", Value: winning},
		{Key: "usableIndexes", Value: explainUsableIndexes(col, plan)},
		{Key: "rejectedIndexes", Value: explainRejectedIndexes(col, plan)},
		{Key: "scanBounds", Value: explainScanBounds(plan)},
		{Key: "sort", Value: explainSort(plan)},
		{Key: "maxScanDocuments", Value: int64(s.maxFindScanDocuments())},
		{Key: "cursorWork", Value: "none"},
	}
	if selection.stage == "adaptive_candidate_selection" {
		planner = append(planner, bson.E{Key: "candidatePlans", Value: explainCandidatePlans(col, plan)})
	}
	response := bson.D{{Key: "queryPlanner", Value: planner}}
	if verbosity == "executionStats" {
		start := time.Now()
		stats := &findExecutionStats{stage: selection.stage, indexName: selection.indexName}
		plan.stats = stats
		if !missing {
			if returned, execErr := execute(plan); execErr != nil {
				reason := explainExecutionRejectionReason(execErr)
				return commandErrorWithFields(commandCodeBadValue, "BadValue", execErr.Error(), bson.D{
					{Key: "queryPlanner", Value: planner},
					{Key: "executionStats", Value: bson.D{
						{Key: "truncated", Value: reason == "scan_cap_exceeded"},
						{Key: "rejectionReason", Value: reason},
						{Key: "candidateDocumentsMaterialized", Value: stats.candidatesMaterialized},
						{Key: "scanCap", Value: int64(s.maxFindScanDocuments())},
					}},
				})
			} else {
				stats.documentsReturned = returned
			}
		}
		// Execution is free to reject a stale candidate before materialization,
		// but its final winner is reported from that shared execution path.
		winning[0].Value = stats.stage
		if stats.stage != selection.stage || stats.indexName != selection.indexName {
			winning = bson.D{{Key: "stage", Value: stats.stage}, {Key: "residualFilter", Value: len(plan.predicates) > 1 || len(plan.orBranches) != 0}}
			if stats.indexName != "" {
				winning = append(winning, bson.E{Key: "indexName", Value: stats.indexName})
			}
			if plan.sort.field != "" {
				winning = append(winning, bson.E{Key: "inMemorySort", Value: true})
			}
			planner[1].Value = winning
		}
		response = append(response, bson.E{Key: "executionStats", Value: bson.D{
			{Key: "nReturned", Value: stats.documentsReturned},
			{Key: "totalDocsExamined", Value: stats.candidatesExamined},
			{Key: "candidateDocumentsMaterialized", Value: stats.candidatesMaterialized},
			{Key: "cursorDocumentsMaterialized", Value: int64(0)},
			{Key: "scanCap", Value: int64(s.maxFindScanDocuments())},
			{Key: "executionTimeMillis", Value: time.Since(start).Milliseconds()},
		}})
	}
	response = append(response, bson.E{Key: "ok", Value: 1.0})
	return marshalDocument(response)
}

func explainPlannerSelection(col *collections.Collection, plan findPlan) findPlannerSelection {
	candidates := 0
	if _, ok := primaryCandidatePredicate(plan.predicates); ok {
		candidates++
	}
	candidates += len(explainUsableIndexes(col, plan))
	if candidates > 1 {
		return findPlannerSelection{stage: "adaptive_candidate_selection"}
	}
	if candidates == 0 {
		return findPlannerSelection{stage: "bounded_scan"}
	}
	return selectFindPlannerSelection(col.MetaView(), plan)
}

func explainCandidatePlans(col *collections.Collection, plan findPlan) bson.A {
	if col == nil {
		return bson.A{}
	}
	out := bson.A{}
	if _, ok := primaryCandidatePredicate(plan.predicates); ok {
		out = append(out, bson.D{{Key: "stage", Value: "primary_lookup"}})
	}
	for _, item := range explainUsableIndexes(col, plan) {
		out = append(out, item)
	}
	return out
}

func explainExecutionRejectionReason(err error) string {
	message := err.Error()
	switch {
	case strings.Contains(message, "bounded scan"), strings.Contains(message, "candidate set exceeded"):
		return "scan_cap_exceeded"
	default:
		return "execution_rejected"
	}
}

func explainUsableIndexes(col *collections.Collection, plan findPlan) bson.A {
	if col == nil {
		return bson.A{}
	}
	out := bson.A{}
	for _, idx := range col.MetaView().Indexes {
		for _, pred := range plan.predicates {
			if idx.Field != pred.field || (pred.op != findPredicateEq && pred.op != findPredicateIn && !isRangePredicate(pred.op)) {
				continue
			}
			if predicateContainsNull(pred) {
				continue
			}
			if isRangePredicate(pred.op) {
				_, ok, _, err := indexRangeOptionsForPredicates(plan.predicates, idx)
				if err != nil || !ok {
					continue
				}
			} else {
				compatible := true
				for _, value := range pred.values {
					if _, ok := indexScalarForBSONValue(value, idx.ValueType); !ok {
						compatible = false
						break
					}
				}
				if !compatible {
					continue
				}
			}
			out = append(out, bson.D{{Key: "name", Value: idx.Name}, {Key: "field", Value: idx.Field}, {Key: "kind", Value: indexedFindStage(plan, idx)}})
			break
		}
	}
	return out
}

func explainRejectedIndexes(col *collections.Collection, plan findPlan) bson.A {
	if col == nil {
		return bson.A{}
	}
	usable := explainUsableIndexes(col, plan)
	usableNames := make(map[string]struct{}, len(usable))
	for _, item := range usable {
		if definition, ok := item.(bson.D); ok {
			for _, field := range definition {
				if field.Key == "name" {
					if name, ok := field.Value.(string); ok {
						usableNames[name] = struct{}{}
					}
				}
			}
		}
	}
	out := bson.A{}
	for _, idx := range col.MetaView().Indexes {
		if _, ok := usableNames[idx.Name]; !ok {
			out = append(out, bson.D{{Key: "name", Value: idx.Name}, {Key: "reason", Value: "filter_not_covered"}})
		}
	}
	return out
}

func explainScanBounds(plan findPlan) bson.A {
	out := bson.A{}
	for _, pred := range plan.predicates {
		out = append(out, bson.D{{Key: "field", Value: pred.field}, {Key: "operator", Value: explainPredicateOperator(pred.op)}})
	}
	return out
}

func explainPredicateOperator(op findPredicateOp) string {
	switch op {
	case findPredicateEq:
		return "eq"
	case findPredicateIn:
		return "in"
	case findPredicateGT:
		return "gt"
	case findPredicateGTE:
		return "gte"
	case findPredicateLT:
		return "lt"
	case findPredicateLTE:
		return "lte"
	default:
		return "unknown"
	}
}

func explainSort(plan findPlan) bson.D {
	if plan.sort.field == "" {
		return bson.D{{Key: "satisfied", Value: true}}
	}
	return bson.D{{Key: "field", Value: plan.sort.field}, {Key: "direction", Value: map[bool]int{true: -1, false: 1}[plan.sort.desc]}, {Key: "satisfied", Value: false}}
}
