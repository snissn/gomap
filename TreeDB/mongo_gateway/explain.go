package mongogateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	materializedBytes      int64
	documentsReturned      int64
	stage                  string
	indexName              string
	selection              findPlannerSelection
}

func (p findPlan) recordWinner(stage, indexName string) {
	if p.stats != nil {
		p.stats.stage = stage
		p.stats.indexName = indexName
		p.stats.selection.stage = stage
		p.stats.selection.indexName = indexName
	}
}

func (p findPlan) recordCompoundWinner(candidate compoundIndexPlan) {
	if p.stats == nil {
		return
	}
	p.stats.stage = "compound_index_scan"
	p.stats.indexName = candidate.idx.Name
	p.stats.selection = findPlannerSelection{
		stage: "compound_index_scan", indexName: candidate.idx.Name,
		indexField: candidate.idx.Field, residualFilters: candidate.residualFilters,
		sortSatisfied: candidate.sortSatisfied, equalityPrefix: candidate.equalityPrefix,
		hasRange: candidate.hasRange, reverse: candidate.reverse,
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

func (p findPlan) recordMaterializedBytes(n int) {
	if p.stats != nil && n > 0 {
		p.stats.materializedBytes += int64(n)
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
	if !supportedExplainReadCommand(name) {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway explain supports bounded standalone find, count, distinct, and aggregate reads only")
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
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway explain command is not dispatched")
	}
}

func supportedExplainReadCommand(name string) bool {
	switch name {
	case "find", "count", "distinct", "aggregate":
		return true
	}
	return false
}

func explainVerbosity(command wire.Document) (string, error) {
	value, present, err := optionalStringFieldWithPresence(command, "verbosity")
	if err != nil {
		return "", err
	}
	if !present {
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
		"find": {}, "filter": {}, "projection": {}, "sort": {}, "skip": {}, "limit": {}, "batchSize": {}, "singleBatch": {}, "hint": {}, "readConcern": {},
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
	if err := validateFindCommandOptions(command, filter); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
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
	predicates, branches, norBranches, err := parseFindFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	skip, limit, err := parseFindPagination(command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return s.explainCollectionRead(ctx, db, collection, finalizeFindPlan(findPlan{predicates: predicates, orBranches: branches, norBranches: norBranches, skip: skip, limit: limit}), verbosity, func(col *collections.Collection, plan findPlan) (int64, error) {
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
	predicates, branches, norBranches, err := parseFindFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return s.explainCollectionRead(ctx, db, collection, finalizeFindPlan(findPlan{predicates: predicates, orBranches: branches, norBranches: norBranches}), verbosity, func(col *collections.Collection, plan findPlan) (int64, error) {
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
	} else if batchSize, set, err := optionalInt32FieldWithPresence(cursor, "batchSize"); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	} else if _, err := normalizeBatchSize(int(batchSize), set, defaultCursorBatchSize); err != nil {
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
	plan = finalizeFindPlan(plan)
	if missing && plan.hint.present {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway find hint does not name an existing index")
	}
	if !missing {
		if err := validateCompoundHint(col.MetaView(), plan); err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
	}
	selection := findPlannerSelection{stage: "bounded_scan"}
	if !missing {
		selection = explainPlannerSelection(col, plan)
	}
	sortSatisfied := selection.sortSatisfied
	winning := bson.D{{Key: "stage", Value: selection.stage}, {Key: "residualFilter", Value: findPlanHasResidualFilter(plan, selection)}}
	if selection.indexName != "" {
		winning = append(winning, bson.E{Key: "indexName", Value: selection.indexName})
	}
	if selection.stage == "compound_index_scan" {
		winning = append(winning,
			bson.E{Key: "equalityPrefix", Value: int32(selection.equalityPrefix)},
			bson.E{Key: "range", Value: selection.hasRange},
			bson.E{Key: "reverse", Value: selection.reverse},
		)
	}
	if plan.sort.field != "" {
		winning = append(winning, bson.E{Key: "inMemorySort", Value: !sortSatisfied})
	}
	sortDescription := explainSort(plan, sortSatisfied)
	planner := bson.D{
		{Key: "namespace", Value: db + "." + collection},
		{Key: "winningPlan", Value: winning},
		{Key: "usableIndexes", Value: explainUsableIndexes(col, plan)},
		{Key: "rejectedIndexes", Value: explainRejectedIndexes(col, plan)},
		{Key: "scanBounds", Value: explainScanBounds(plan)},
		{Key: "sort", Value: sortDescription},
		{Key: "maxScanDocuments", Value: int64(s.maxFindScanDocuments())},
		{Key: "cursorWork", Value: "none"},
		{Key: "hint", Value: bson.D{{Key: "present", Value: plan.hint.present}, {Key: "status", Value: explainHintStatus(plan)}}},
	}
	if selection.stage == "adaptive_candidate_selection" {
		planner = append(planner, bson.E{Key: "candidatePlans", Value: explainCandidatePlans(col, plan)})
	}
	response := bson.D{{Key: "queryPlanner", Value: planner}}
	if verbosity == "executionStats" {
		start := time.Now()
		stats := &findExecutionStats{stage: selection.stage, indexName: selection.indexName, selection: selection}
		plan.stats = stats
		if !missing {
			if returned, execErr := execute(plan); execErr != nil {
				reason := explainExecutionRejectionReason(execErr)
				return commandErrorWithFields(commandCodeBadValue, "BadValue", execErr.Error(), bson.D{
					{Key: "queryPlanner", Value: planner},
					{Key: "executionStats", Value: bson.D{
						{Key: "nReturned", Value: int64(0)},
						{Key: "candidateDocumentsExamined", Value: stats.candidatesExamined},
						{Key: "truncated", Value: reason == "scan_cap_exceeded"},
						{Key: "rejectionReason", Value: reason},
						{Key: "candidateDocumentsMaterialized", Value: stats.candidatesMaterialized},
						{Key: "candidateMaterializedBytes", Value: stats.materializedBytes},
						{Key: "cursorDocumentsMaterialized", Value: int64(0)},
						{Key: "scanCap", Value: int64(s.maxFindScanDocuments())},
						{Key: "executionTimeMillis", Value: time.Since(start).Milliseconds()},
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
			actual := stats.selection
			if actual.stage == "" {
				actual = findPlannerSelection{stage: stats.stage, indexName: stats.indexName, indexField: selection.indexField}
			}
			for _, idx := range col.MetaView().Indexes {
				if idx.Name == stats.indexName {
					actual.indexField = idx.Field
					break
				}
			}
			winning = bson.D{{Key: "stage", Value: stats.stage}, {Key: "residualFilter", Value: findPlanHasResidualFilter(plan, actual)}}
			if stats.indexName != "" {
				winning = append(winning, bson.E{Key: "indexName", Value: stats.indexName})
			}
			if actual.stage == "compound_index_scan" {
				winning = append(winning,
					bson.E{Key: "equalityPrefix", Value: int32(actual.equalityPrefix)},
					bson.E{Key: "range", Value: actual.hasRange},
					bson.E{Key: "reverse", Value: actual.reverse},
				)
			}
			if plan.sort.field != "" {
				winning = append(winning, bson.E{Key: "inMemorySort", Value: !actual.sortSatisfied})
			}
			for i := range planner {
				switch planner[i].Key {
				case "winningPlan":
					planner[i].Value = winning
				case "sort":
					// The adaptive placeholder may not satisfy the requested order,
					// while the executor's selected compound plan can. Keep the
					// planner-wide sort descriptor consistent with inMemorySort.
					planner[i].Value = explainSort(plan, actual.sortSatisfied)
				}
			}
		}
		if stats.stage != "adaptive_candidate_selection" {
			planner = explainPlannerWithoutCandidatePlans(planner)
		}
		response[0].Value = planner
		response = append(response, bson.E{Key: "executionStats", Value: bson.D{
			{Key: "nReturned", Value: stats.documentsReturned},
			// This includes adaptive planner probes; it is deliberately a
			// gateway-owned counter, not MongoDB's winner-plan-only metric.
			{Key: "candidateDocumentsExamined", Value: stats.candidatesExamined},
			{Key: "candidateDocumentsMaterialized", Value: stats.candidatesMaterialized},
			{Key: "candidateMaterializedBytes", Value: stats.materializedBytes},
			{Key: "cursorDocumentsMaterialized", Value: int64(0)},
			{Key: "scanCap", Value: int64(s.maxFindScanDocuments())},
			{Key: "executionTimeMillis", Value: time.Since(start).Milliseconds()},
		}})
	}
	response = append(response, bson.E{Key: "ok", Value: 1.0})
	return marshalDocument(response)
}

func explainHintStatus(plan findPlan) string {
	if !plan.hint.present {
		return "not_requested"
	}
	return "honored"
}

func explainPlannerWithoutCandidatePlans(planner bson.D) bson.D {
	for i := range planner {
		if planner[i].Key == "candidatePlans" {
			return append(planner[:i:i], planner[i+1:]...)
		}
	}
	return planner
}

func explainPlannerSelection(col *collections.Collection, plan findPlan) findPlannerSelection {
	// $or needs a union of branch candidate sets, which this metadata-only
	// selector cannot represent. $nor is different: its sibling positive
	// predicates remain safe index probes and the negative branches are
	// rechecked as residual filters by the executor.
	if len(plan.orBranches) != 0 {
		return findPlannerSelection{stage: "bounded_scan"}
	}
	candidatePlans := explainCandidatePlans(col, plan)
	if len(candidatePlans) > 1 {
		return findPlannerSelection{stage: "adaptive_candidate_selection"}
	}
	if len(candidatePlans) == 0 {
		return findPlannerSelection{stage: "bounded_scan"}
	}
	return selectFindPlannerSelection(col.MetaView(), plan)
}

func findPlanHasResidualFilter(plan findPlan, selection findPlannerSelection) bool {
	if len(plan.orBranches) != 0 || len(plan.norBranches) != 0 || selection.stage == "bounded_scan" || selection.stage == "adaptive_candidate_selection" {
		return len(plan.predicates) != 0 || len(plan.orBranches) != 0 || len(plan.norBranches) != 0
	}
	if selection.stage == "primary_lookup" {
		return len(plan.predicates) != 1
	}
	if selection.stage == "compound_index_scan" {
		return selection.residualFilters != 0
	}
	equalityPredicates := 0
	for _, pred := range plan.predicates {
		if pred.field != selection.indexField {
			return true
		}
		// A concrete probe only covers predicates of its own kind. For
		// example, choosing a range probe still leaves an equality/$in
		// predicate on that field to be filtered after materialization.
		if selection.stage == "secondary_equality_lookup" && isRangePredicate(pred.op) {
			return true
		}
		if selection.stage == "secondary_range_lookup" && (pred.op == findPredicateEq || pred.op == findPredicateIn) {
			return true
		}
		if selection.stage == "secondary_equality_lookup" && (pred.op == findPredicateEq || pred.op == findPredicateIn) {
			equalityPredicates++
		}
	}
	// The executor probes same-kind equality/$in predicates independently and
	// chooses one candidate set. Every additional predicate is therefore
	// filtered after materialization rather than covered by the winning probe.
	if selection.stage == "secondary_equality_lookup" && equalityPredicates != 1 {
		return true
	}
	return false
}

func explainCandidatePlans(col *collections.Collection, plan findPlan) bson.A {
	if col == nil {
		return bson.A{}
	}
	out := bson.A{}
	if !plan.hint.present {
		if _, ok := primaryCandidatePredicate(plan.predicates); ok {
			out = append(out, bson.D{{Key: "stage", Value: "primary_lookup"}})
		}
	}
	for _, probe := range findIndexProbes(col.MetaView(), plan) {
		out = append(out, bson.D{{Key: "stage", Value: probe.stage}, {Key: "indexName", Value: probe.idx.Name}, {Key: "field", Value: probe.idx.Field}})
	}
	// documentsForCompoundIndexPlan deliberately preserves a direct unhinted
	// primary lookup as the winner. Do not present a compound plan that the
	// executor will decline before walking it: queryPlanner candidates describe
	// executable alternatives, not merely syntactically coverable indexes.
	if !compoundPlanDeferredToPrimaryLookup(plan) && !compoundPlanDeferredToLegacyLookup(col.MetaView(), plan) {
		for _, compound := range compoundIndexPlans(col.MetaView(), plan) {
			out = append(out, bson.D{{Key: "stage", Value: "compound_index_scan"}, {Key: "indexName", Value: compound.idx.Name}, {Key: "field", Value: compound.idx.Field}, {Key: "equalityPrefix", Value: int32(compound.equalityPrefix)}, {Key: "range", Value: compound.hasRange}, {Key: "reverse", Value: compound.reverse}, {Key: "sortSatisfied", Value: compound.sortSatisfied}})
		}
	}
	return out
}

func explainExecutionRejectionReason(err error) string {
	switch {
	case errors.Is(err, errMongoFindScanCapExceeded):
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
	seen := make(map[string]struct{})
	for _, probe := range findIndexProbes(col.MetaView(), plan) {
		if _, ok := seen[probe.idx.Name]; ok {
			continue
		}
		seen[probe.idx.Name] = struct{}{}
		out = append(out, bson.D{{Key: "name", Value: probe.idx.Name}, {Key: "field", Value: probe.idx.Field}, {Key: "kind", Value: probe.stage}})
	}
	if !compoundPlanDeferredToPrimaryLookup(plan) && !compoundPlanDeferredToLegacyLookup(col.MetaView(), plan) {
		for _, compound := range compoundIndexPlans(col.MetaView(), plan) {
			if _, ok := seen[compound.idx.Name]; ok {
				continue
			}
			seen[compound.idx.Name] = struct{}{}
			out = append(out, bson.D{{Key: "name", Value: compound.idx.Name}, {Key: "field", Value: compound.idx.Field}, {Key: "kind", Value: "compound_index_scan"}, {Key: "equalityPrefix", Value: int32(compound.equalityPrefix)}, {Key: "range", Value: compound.hasRange}, {Key: "reverse", Value: compound.reverse}, {Key: "sortSatisfied", Value: compound.sortSatisfied}})
		}
	}
	return out
}

func compoundPlanDeferredToPrimaryLookup(plan findPlan) bool {
	if plan.hint.present {
		return false
	}
	_, primary := primaryCandidatePredicate(plan.predicates)
	return primary
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
		if plan.hint.present && !findHintMatchesIndex(plan.hint, idx) {
			continue
		}
		if _, ok := usableNames[idx.Name]; !ok {
			out = append(out, bson.D{{Key: "name", Value: idx.Name}, {Key: "reason", Value: "filter_or_sort_not_covered"}})
		}
	}
	return out
}

func explainScanBounds(plan findPlan) bson.A {
	out := bson.A{}
	for _, pred := range plan.predicates {
		types := bson.A{}
		seen := map[bson.Type]struct{}{}
		for _, value := range pred.values {
			if _, ok := seen[value.Type]; ok {
				continue
			}
			seen[value.Type] = struct{}{}
			types = append(types, explainBSONType(value.Type))
		}
		fingerprints := bson.A{}
		for _, value := range pred.values {
			fingerprints = append(fingerprints, explainBoundFingerprint(value))
		}
		bound := bson.D{{Key: "field", Value: pred.field}, {Key: "operator", Value: explainPredicateOperator(pred.op)}, {Key: "valueTypes", Value: types}, {Key: "valueCount", Value: int32(len(pred.values))}, {Key: "valueFingerprints", Value: fingerprints}}
		switch pred.op {
		case findPredicateGT:
			bound = append(bound, bson.E{Key: "lowerInclusive", Value: false})
		case findPredicateGTE:
			bound = append(bound, bson.E{Key: "lowerInclusive", Value: true})
		case findPredicateLT:
			bound = append(bound, bson.E{Key: "upperInclusive", Value: false})
		case findPredicateLTE:
			bound = append(bound, bson.E{Key: "upperInclusive", Value: true})
		}
		out = append(out, bound)
	}
	return out
}

func explainBoundFingerprint(value bson.RawValue) string {
	input := append([]byte{byte(value.Type)}, value.Value...)
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:12])
}

func explainBSONType(typ bson.Type) string {
	switch typ {
	case bson.TypeString:
		return "string"
	case bson.TypeInt32:
		return "int32"
	case bson.TypeInt64:
		return "int64"
	case bson.TypeDouble:
		return "double"
	case bson.TypeBoolean:
		return "bool"
	case bson.TypeNull:
		return "null"
	default:
		return "other"
	}
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

func explainSort(plan findPlan, satisfied bool) bson.D {
	if plan.sort.field == "" {
		return bson.D{{Key: "satisfied", Value: true}}
	}
	terms := bson.A{}
	for _, term := range findSortTerms(plan.sort) {
		terms = append(terms, bson.D{{Key: "field", Value: term.field}, {Key: "direction", Value: map[bool]int{true: -1, false: 1}[term.desc]}})
	}
	return bson.D{{Key: "field", Value: plan.sort.field}, {Key: "direction", Value: map[bool]int{true: -1, false: 1}[plan.sort.desc]}, {Key: "terms", Value: terms}, {Key: "satisfied", Value: satisfied}}
}
