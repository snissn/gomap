package compatdiff

import (
	"context"
	"errors"
	"strings"
	"testing"

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

func TestExpectedRejectionMutationFails(t *testing.T) {
	f := fixture()
	f.Expectation = ExpectedRejected
	seed := raw(t, bson.D{{Key: "_id", Value: "a"}})
	f.Seed = []bson.Raw{seed}
	good := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 1, CommandRejection: true}, State: []bson.Raw{seed}}, nil
	})
	mutated := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Code: 1, CommandRejection: true}, State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "a"}, {Key: "bad", Value: true}})}}, nil
	})
	result := Run(context.Background(), "identity", []Fixture{f}, mutated, good)
	if result.Fixtures[0].Status != "mismatch" || !strings.Contains(result.Fixtures[0].Reason, "mutated") {
		t.Fatalf("got %+v", result.Fixtures[0])
	}
	if clean := Run(context.Background(), "identity", []Fixture{f}, good, good); clean.Fixtures[0].Status != "pass" {
		t.Fatalf("unmutated rejection fixture did not pass: %+v", clean.Fixtures[0])
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
		return Observation{Response: raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "same"}}}}}}})}, nil
	})
	cursor := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "different"}}}}}}})}, nil
	})
	if got := Run(context.Background(), "identity", []Fixture{f}, cursorBase, cursor).Fixtures[0].Status; got != "mismatch" {
		t.Fatalf("cursor status=%s", got)
	}
}

func TestUnavailableReferenceIsNotCompatibilityFailure(t *testing.T) {
	f := fixture()
	ok := executorFunc(func(context.Context, Fixture) (Observation, error) { return Observation{}, nil })
	missing := executorFunc(func(context.Context, Fixture) (Observation, error) {
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
	exec := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Error: &Error{Message: "connection reset"}}, nil
	})
	result := Run(context.Background(), "identity", []Fixture{f}, exec, exec)
	if result.Fixtures[0].Status != "harness-error" {
		t.Fatalf("status=%s reason=%q", result.Fixtures[0].Status, result.Fixtures[0].Reason)
	}
}

func TestResultRetainsNormalizedAttributableObservations(t *testing.T) {
	f := fixture()
	exec := executorFunc(func(context.Context, Fixture) (Observation, error) {
		return Observation{Response: raw(t, bson.D{{Key: "n", Value: int32(1)}}), State: []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "a"}})}}, nil
	})
	row := Run(context.Background(), "identity", []Fixture{f}, exec, exec).Fixtures[0]
	if row.TreeDB.Response == nil || row.Reference.Response == nil || len(row.TreeDB.State) != 1 || row.Duration <= 0 {
		t.Fatalf("result lost comparable observations: %+v", row)
	}
}
