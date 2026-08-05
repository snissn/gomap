package compatdiff

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type executorFunc func(context.Context, Fixture) (Observation, error)

func (f executorFunc) Execute(ctx context.Context, fixture Fixture) (Observation, error) {
	return f(ctx, fixture)
}

func raw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func fixture() Fixture {
	return Fixture{Schema: FixtureSchema, Version: FixtureVersion, ID: "case", CapabilityID: "crud.find-by-id-equality", Expectation: ExpectedSupported, Database: "db", Collection: "c", Command: bson.Raw{5, 0, 0, 0, 0}}
}

func TestNormalizePreservesBSONTypeAndOrder(t *testing.T) {
	left := raw(t, bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}})
	wrongType := raw(t, bson.D{{Key: "a", Value: int64(1)}, {Key: "b", Value: "x"}})
	wrongOrder := raw(t, bson.D{{Key: "b", Value: "x"}, {Key: "a", Value: int32(1)}})
	if sameRaw(left, wrongType, nil) || sameRaw(left, wrongOrder, nil) {
		t.Fatal("normalization hid BSON type or order difference")
	}
}

func TestResponseEnvelopeOrderNormalizationDoesNotHideNestedOrder(t *testing.T) {
	left := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "n", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}}}}}})
	topLevelReordered := raw(t, bson.D{{Key: "n", Value: int32(1)}, {Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}}}}}})
	if !sameResponseWithNormalization(left, topLevelReordered, nil, nil, true, false) {
		t.Fatal("explicit reply-envelope normalization did not accept top-level transport order")
	}
	nestedReordered := raw(t, bson.D{{Key: "n", Value: int32(1)}, {Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "b", Value: int32(2)}, {Key: "a", Value: int32(1)}}}}}}})
	if sameResponseWithNormalization(left, nestedReordered, nil, nil, true, false) {
		t.Fatal("reply-envelope normalization hid nested cursor document order")
	}
}

func TestCursorEnvelopeOrderNormalizationDoesNotHideBatchDocumentOrder(t *testing.T) {
	left := raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}}}}}, {Key: "ok", Value: int32(1)}})
	cursorEnvelopeReordered := raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}}}, {Key: "ns", Value: "db.c"}, {Key: "id", Value: int64(0)}}}, {Key: "ok", Value: int32(1)}})
	if !sameResponseWithNormalization(left, cursorEnvelopeReordered, nil, nil, false, true) {
		t.Fatal("explicit cursor-envelope normalization did not accept cursor transport field order")
	}
	batchDocumentReordered := raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "b", Value: int32(2)}, {Key: "a", Value: int32(1)}}}}, {Key: "ns", Value: "db.c"}, {Key: "id", Value: int64(0)}}}, {Key: "ok", Value: int32(1)}})
	if sameResponseWithNormalization(left, batchDocumentReordered, nil, nil, false, true) {
		t.Fatal("cursor-envelope normalization hid batch-document order")
	}
}

func TestIgnoredFieldsAreFixtureScoped(t *testing.T) {
	left := raw(t, bson.D{{Key: "ok", Value: 1}, {Key: "operationTime", Value: int64(1)}})
	right := raw(t, bson.D{{Key: "ok", Value: 1}, {Key: "operationTime", Value: int64(2)}})
	if sameRaw(left, right, nil) {
		t.Fatal("unexpected global ignore")
	}
	if !sameRaw(left, right, []string{"operationTime"}) {
		t.Fatal("declared field was not ignored")
	}
	nestedLeft := raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(1)}, {Key: "ns", Value: "db.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "ts", Value: int64(1)}, {Key: "keep", Value: "same"}}}}}}})
	nestedRight := raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(2)}, {Key: "ns", Value: "db.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "ts", Value: int64(2)}, {Key: "keep", Value: "same"}}}}}}})
	if !sameRaw(nestedLeft, nestedRight, []string{"cursor.id", "cursor.firstBatch.0.ts"}) {
		t.Fatal("nested and array ignored paths were not applied")
	}
	nestedSibling := raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(2)}, {Key: "ns", Value: "db.other"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "ts", Value: int64(2)}, {Key: "keep", Value: "same"}}}}}}})
	if sameRaw(nestedLeft, nestedSibling, []string{"cursor.id", "cursor.firstBatch.0.ts"}) {
		t.Fatal("nested ignore hid sibling difference")
	}
}

func TestNormalizationAppliesToResponseAndStateWithoutHidingType(t *testing.T) {
	f := fixture()
	f.NormalizeFields = []string{"generatedAt", "clusterTime", "_id"}
	f.IgnoreStateFields = []string{"trace"}
	left := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "generatedAt", Value: bson.NewObjectID()}, {Key: "clusterTime", Value: bson.Timestamp{T: 1, I: 2}}, {Key: "ok", Value: int32(1)}}), Baseline: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "clusterTime", Value: bson.Timestamp{T: 3, I: 4}}})}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "clusterTime", Value: bson.Timestamp{T: 5, I: 6}}, {Key: "trace", Value: "left"}})}}, nil
	})
	right := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "generatedAt", Value: bson.NewObjectID()}, {Key: "clusterTime", Value: bson.Timestamp{T: 7, I: 8}}, {Key: "ok", Value: int32(1)}}), Baseline: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "clusterTime", Value: bson.Timestamp{T: 9, I: 10}}})}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "clusterTime", Value: bson.Timestamp{T: 11, I: 12}}, {Key: "trace", Value: "right"}})}}, nil
	})
	row := Run(context.Background(), "identity", []Fixture{f}, left, right).Fixtures[0]
	if row.Status != "pass" || row.TreeDB.Response == nil || len(row.TreeDB.Baseline) != 1 || len(row.TreeDB.State) != 1 {
		t.Fatalf("normalization did not apply consistently: %+v", row)
	}
	wrongType := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "generatedAt", Value: "not-an-object-id"}, {Key: "clusterTime", Value: bson.Timestamp{T: 7, I: 8}}, {Key: "ok", Value: int32(1)}})}, nil
	})
	if got := Run(context.Background(), "identity", []Fixture{f}, left, wrongType).Fixtures[0].Status; got != "mismatch" {
		t.Fatalf("normalized field hid type difference: %s", got)
	}
}

func TestNormalizationTokensPreserveEqualityRelationshipsAcrossObservation(t *testing.T) {
	f := fixture()
	f.NormalizeFields = []string{"generated", "_id"}
	shared := bson.NewObjectID()
	observation := Observation{Response: raw(t, bson.D{{Key: "generated", Value: shared}}), Baseline: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: shared}})}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}})}}
	normalized := normalizedObservation(observation, nil, nil, f.NormalizeFields, false, false, false)
	if fmt.Sprint(normalized.Response) == fmt.Sprint(normalized.State) {
		t.Fatal("distinct generated values collapsed to one token")
	}
	if !strings.Contains(fmt.Sprint(normalized.Response), "1") || !strings.Contains(fmt.Sprint(normalized.Baseline), "1") {
		t.Fatalf("shared response/state value did not retain one token: %+v", normalized)
	}
}

func TestComparisonNormalizationPreservesCrossResponseStateIdentity(t *testing.T) {
	f := fixture()
	f.NormalizeFields = []string{"generated", "_id"}
	shared := bson.NewObjectID()
	left := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "generated", Value: shared}}), State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: shared}})}}, nil
	})
	right := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "generated", Value: bson.NewObjectID()}}), State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}})}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, left, right).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "post-command") {
		t.Fatalf("cross-observation identity was hidden: %+v", row)
	}
}

func TestCursorNamespaceNormalizationPreservesSuffixAndCursorError(t *testing.T) {
	f := fixture()
	f.NormalizeFields = []string{"cursor.id"}
	f.NormalizeCursorNamespace = true
	left := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(3)}, {Key: "ns", Value: "tree.$cmd.listIndexes"}}}}), CursorError: &Error{Code: 43, Labels: []string{"CursorNotFound"}}}, nil
	})
	right := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(4)}, {Key: "ns", Value: "reference.$cmd.listIndexes"}}}}), CursorError: &Error{Code: 43, Labels: []string{"CursorNotFound"}}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, left, right).Fixtures[0]; row.Status != "pass" {
		t.Fatalf("db prefix normalization should preserve matching suffix: %+v", row)
	}
	wrongSuffix := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(4)}, {Key: "ns", Value: "reference.$cmd.listCollections"}}}}), CursorError: &Error{Code: 44}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, left, wrongSuffix).Fixtures[0]; row.Status != "mismatch" {
		t.Fatalf("cursor suffix/error semantic mismatch was hidden: %+v", row)
	}
}

func TestCursorNamespaceNormalizationDoesNotHidePersistedState(t *testing.T) {
	f := fixture()
	f.NormalizeCursorNamespace = true
	response := raw(t, bson.D{{Key: "ok", Value: int32(1)}})
	left := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: response, State: []bson.Raw{raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "ns", Value: "treedb.records"}}}})}}, nil
	})
	right := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: response, State: []bson.Raw{raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "ns", Value: "mongo.records"}}}})}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, left, right).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "post-command") {
		t.Fatalf("cursor namespace normalization hid persisted state: %+v", row)
	}
}

func TestExpectedRejectionMutationFails(t *testing.T) {
	f := fixture()
	f.Expectation = ExpectedRejected
	f.ExpectedErrorCode = 1
	seed := raw(t, bson.D{{Key: "_id", Value: "a"}})
	f.Seed = []bson.Raw{seed}
	good := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 1, CommandRejection: true}, Baseline: []bson.Raw{seed}, State: []bson.Raw{seed}}, nil
	})
	mutated := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 1, CommandRejection: true}, Baseline: []bson.Raw{seed}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "a"}, {Key: "bad", Value: true}})}}, nil
	})
	reference := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "ok", Value: int32(1)}}), Baseline: []bson.Raw{seed}, State: []bson.Raw{seed}}, nil
	})
	result := Run(context.Background(), "identity", []Fixture{f}, mutated, reference)
	if result.Fixtures[0].Status != "mismatch" || !strings.Contains(result.Fixtures[0].Reason, "mutated") {
		t.Fatalf("got %+v", result.Fixtures[0])
	}
	if clean := Run(context.Background(), "identity", []Fixture{f}, good, reference); clean.Fixtures[0].Status != "pass" {
		t.Fatalf("unmutated rejection fixture did not pass: %+v", clean.Fixtures[0])
	}
}

func TestExpectedRejectionRequiresSuccessfulReferenceAndExactErrorCode(t *testing.T) {
	f := fixture()
	f.Expectation = ExpectedRejected
	f.ExpectedErrorCode = 2
	state := []bson.Raw{
		raw(t, bson.D{{Key: "_compatdiff_metadata", Value: bson.D{{Key: "collection", Value: "c"}}}}),
		raw(t, bson.D{{Key: "_id", Value: "a"}}),
	}
	tree := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 2, CommandRejection: true}, Baseline: state, State: state}, nil
	})
	referenceOK := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "ok", Value: int32(1)}})}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, tree, referenceOK).Fixtures[0]; row.Status != "pass" {
		t.Fatalf("expected known rejection with reference success to pass: %+v", row)
	}
	wrongCode := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 9, CommandRejection: true}, Baseline: state, State: state}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, wrongCode, referenceOK).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "code=9") {
		t.Fatalf("duplicate maxTimeMS code was accepted: %+v", row)
	}
	badReference := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "ok", Value: int32(0)}})}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, tree, badReference).Fixtures[0]; row.Status != "harness-error" {
		t.Fatalf("reference non-success was accepted: %+v", row)
	}
}

func TestDifferencesInCountsErrorsAndCursorContentFail(t *testing.T) {
	f := fixture()
	base := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "n", Value: int32(1)}})}, nil
	})
	count := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "n", Value: int32(2)}})}, nil
	})
	if got := Run(context.Background(), "identity", []Fixture{f}, base, count).Fixtures[0].Status; got != "mismatch" {
		t.Fatalf("count status=%s", got)
	}
	errExec := executorFunc(func(context.Context, Fixture) (Observation, error) { return Observation{Error: &Error{Code: 99}}, nil })
	if got := Run(context.Background(), "identity", []Fixture{f}, base, errExec).Fixtures[0].Status; got != "mismatch" {
		t.Fatalf("error status=%s", got)
	}
	cursorBase := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(99)}, {Key: "ns", Value: "db.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "same"}}}}}}}), CursorReplies: []bson.Raw{raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.c"}, {Key: "nextBatch", Value: bson.A{bson.D{{Key: "_id", Value: "later"}}}}}}})}}, nil
	})
	cursor := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(99)}, {Key: "ns", Value: "db.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "same"}}}}}}}), CursorReplies: []bson.Raw{raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.c"}, {Key: "nextBatch", Value: bson.A{bson.D{{Key: "_id", Value: "different"}}}}}}})}}, nil
	})
	if got := Run(context.Background(), "identity", []Fixture{f}, cursorBase, cursor).Fixtures[0].Status; got != "mismatch" {
		t.Fatalf("cursor status=%s", got)
	}
}

func TestMatchingErrorsStillRequireMatchingPostState(t *testing.T) {
	f := fixture()
	errState := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 2, CommandRejection: true}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "a"}})}}, nil
	})
	mutated := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 2, CommandRejection: true}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "a"}, {Key: "mutated", Value: true}})}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, errState, mutated).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "post-command") {
		t.Fatalf("matching errors hid state mutation: %+v", row)
	}
}

func TestCursorReplyNormalizationRetainsInitialAndGetMoreStructure(t *testing.T) {
	f := fixture()
	f.NormalizeFields = []string{"cursor.id"}
	f.NormalizeCursorNamespace = true
	initial := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(71)}, {Key: "ns", Value: "db.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "first"}}}}}}})
	next := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.c"}, {Key: "nextBatch", Value: bson.A{bson.D{{Key: "_id", Value: "next"}}}}}}})
	otherInitial := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(999)}, {Key: "ns", Value: "other.c"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "first"}}}}}}})
	otherNext := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "other.c"}, {Key: "nextBatch", Value: bson.A{bson.D{{Key: "_id", Value: "next"}}}}}}})
	left := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: initial, CursorReplies: []bson.Raw{next}}, nil
	})
	right := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: otherInitial, CursorReplies: []bson.Raw{otherNext}}, nil
	})
	row := Run(context.Background(), "identity", []Fixture{f}, left, right).Fixtures[0]
	if row.Status != "pass" || row.TreeDB.Response == nil || len(row.TreeDB.CursorReplies) != 1 {
		t.Fatalf("cursor observations were not retained/normalized: %+v", row)
	}
	droppedGetMore := executorFunc(func(context.Context, Fixture) (Observation, error) { return Observation{Response: otherInitial}, nil })
	if row := Run(context.Background(), "identity", []Fixture{f}, left, droppedGetMore).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "cursor reply") {
		t.Fatalf("cursor closure sequence difference was hidden: %+v", row)
	}
	openAfterGetMore := executorFunc(func(context.Context, Fixture) (Observation, error) {
		stillOpen := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(999)}, {Key: "ns", Value: "other.c"}, {Key: "nextBatch", Value: bson.A{bson.D{{Key: "_id", Value: "next"}}}}}}})
		return Observation{Response: otherInitial, CursorReplies: []bson.Raw{stillOpen}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, left, openAfterGetMore).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "cursor reply") {
		t.Fatalf("cursor closure relationship was hidden: %+v", row)
	}
}

func TestNormalizedCursorIDsRetainClosedSemantics(t *testing.T) {
	f := fixture()
	f.NormalizeFields = []string{"cursor.id"}
	initial := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(10)}, {Key: "ns", Value: "db.c"}}}})
	closed := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.c"}}}})
	reallocatedOpen := raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(777)}, {Key: "ns", Value: "db.c"}}}})
	tree := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: initial, CursorReplies: []bson.Raw{closed}}, nil
	})
	reference := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(99)}, {Key: "ns", Value: "db.c"}}}}), CursorReplies: []bson.Raw{reallocatedOpen}}, nil
	})
	if row := Run(context.Background(), "identity", []Fixture{f}, tree, reference).Fixtures[0]; row.Status != "mismatch" || !strings.Contains(row.Reason, "cursor reply") {
		t.Fatalf("cursor closure was hidden by ID normalization: %+v", row)
	}
}

func TestUnavailableReferenceIsNotCompatibilityFailure(t *testing.T) {
	f := fixture()
	ok := executorFunc(func(context.Context, Fixture) (Observation, error) { return Observation{}, nil })
	missing := executorFunc(func(context.Context, Fixture) (Observation, error) {
		time.Sleep(time.Millisecond)
		return Observation{}, ReferenceUnavailable{Err: errors.New("dial refused")}
	})
	result := Run(context.Background(), "identity", []Fixture{f}, ok, missing)
	if result.Fixtures[0].Status != "reference-unavailable" {
		t.Fatalf("got %+v", result.Fixtures[0])
	}
	if result.Fixtures[0].Duration <= 0 {
		t.Fatalf("unavailable result has no duration: %+v", result.Fixtures[0])
	}
	if result.Status != "reference-unavailable" {
		t.Fatalf("result status=%s", result.Status)
	}
	other := fixture()
	other.ID = "mismatch"
	result = Run(context.Background(), "identity", []Fixture{f, other}, ok, executorFunc(func(_ context.Context, got Fixture) (Observation, error) {
		if got.ID == f.ID {
			return Observation{}, ReferenceUnavailable{Err: errors.New("dial refused")}
		}
		return Observation{Response: raw(t, bson.D{{Key: "n", Value: int32(2)}})}, nil
	}))
	if result.Status != "mismatch" {
		t.Fatalf("status precedence=%s", result.Status)
	}
}

func TestReferenceUnavailableWithoutCauseDoesNotPanic(t *testing.T) {
	if got := (ReferenceUnavailable{}).Error(); got != "reference MongoDB unavailable" {
		t.Fatalf("error=%q", got)
	}
}

func TestExpectedRejectionDoesNotAcceptExecutionFailure(t *testing.T) {
	f := fixture()
	f.Expectation = ExpectedRejected
	f.ExpectedErrorCode = 1
	exec := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Message: "connection reset"}}, nil
	})
	result := Run(context.Background(), "identity", []Fixture{f}, exec, exec)
	if result.Fixtures[0].Status != "harness-error" {
		t.Fatalf("status=%s reason=%q", result.Fixtures[0].Status, result.Fixtures[0].Reason)
	}
}

func TestExpectedRejectionRequiresReferenceExecution(t *testing.T) {
	f := fixture()
	f.Expectation = ExpectedRejected
	f.ExpectedErrorCode = 9
	rejected := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 9, CommandRejection: true}}, nil
	})
	badReference := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 9, CommandRejection: true}}, nil
	})
	result := Run(context.Background(), "identity", []Fixture{f}, rejected, badReference)
	if result.Fixtures[0].Status != "harness-error" {
		t.Fatalf("status=%s reason=%q", result.Fixtures[0].Status, result.Fixtures[0].Reason)
	}
}

func TestResultRetainsNormalizedAttributableObservations(t *testing.T) {
	f := fixture()
	exec := executorFunc(func(context.Context, Fixture) (Observation, error) {
		time.Sleep(time.Millisecond)
		return Observation{Response: raw(t, bson.D{{Key: "n", Value: int32(1)}}), State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "a"}})}}, nil
	})
	row := Run(context.Background(), "identity", []Fixture{f}, exec, exec).Fixtures[0]
	if row.TreeDB.Response == nil || row.Reference.Response == nil || len(row.TreeDB.State) != 1 || row.Duration <= 0 {
		t.Fatalf("result lost comparable observations: %+v", row)
	}
}
