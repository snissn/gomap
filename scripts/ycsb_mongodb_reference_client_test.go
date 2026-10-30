//go:build ignore

package main

import (
	"errors"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// These are synthetic command-construction/checked-error controls. They do not
// connect to MongoDB or establish support for a particular image/build.
func TestReferencePublicCommandOrderAndFailures(t *testing.T) {
	observations := []string{"before.hello", "before.buildInfo", "before.serverStatus", "before.getCmdLineOpts", "before.getParameter", "after.hello", "after.buildInfo", "after.serverStatus", "after.getCmdLineOpts", "after.getParameter"}
	for _, mode := range []string{"observe", "checkpoint"} {
		want := append([]string(nil), observations...)
		if mode == "checkpoint" {
			want = append(append(append([]string(nil), observations[:5]...), "checkpoint"), observations[5:]...)
		}
		for failure := -1; failure < len(want); failure++ {
			var got []string
			injected := errors.New("injected public command failure")
			err := referenceControls(mode, func(key, name string, request bson.D) error {
				got = append(got, key)
				if key == "checkpoint" {
					if name != "fsync" || !reflect.DeepEqual(request, bson.D{{Key: "fsync", Value: int32(1)}, {Key: "lock", Value: false}}) {
						t.Fatalf("wrong explicit fsync request: %s %#v", name, request)
					}
				} else if name == "getParameter" {
					if !reflect.DeepEqual(request, bson.D{{Key: name, Value: int32(1)}, {Key: "journalCommitInterval", Value: int32(1)}, {Key: "syncdelay", Value: int32(1)}}) {
						t.Fatalf("wrong journal parameter observation request: %#v", request)
					}
				} else if !reflect.DeepEqual(request, bson.D{{Key: name, Value: int32(1)}}) {
					t.Fatalf("wrong observation request: %#v", request)
				}
				if len(got)-1 == failure {
					return injected
				}
				return nil
			})
			end := len(want)
			if failure >= 0 {
				end = failure + 1
				if !errors.Is(err, injected) {
					t.Fatalf("%s failure %d lost: %v", mode, failure, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want[:end]) {
				t.Fatalf("%s failure %d order %v, want %v", mode, failure, got, want[:end])
			}
		}
	}
}

func TestReferenceResultRetainsActualError(t *testing.T) {
	for _, injected := range []error{nil, errors.New("injected actual fsync error")} {
		r := referenceCall(func() error { return injected })
		if !r.Attempted || r.Success != (injected == nil) || r.ElapsedNS < 0 || r.EndUTC.Before(r.StartUTC) {
			t.Fatalf("invalid checked result: %#v", r)
		}
		if injected != nil && r.Error != injected.Error() {
			t.Fatalf("actual error lost: %#v", r)
		}
	}
}
