package ycsbevidence

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
)

func checkpointOwner(t *testing.T) (*Observer, func() error, func() error, string) {
	t.Helper()
	data, evidence := t.TempDir(), t.TempDir()
	t.Setenv("YCSB_SERVER_RECEIPT_DIR", evidence)
	server, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: data, Profile: treedb.ProfileCommandWALRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	observer, err := OpenFromEnv("command_wal_relaxed", data, server.Backend, server.Stats)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := observer.Close(server.Close); err != nil {
			t.Error(err)
		}
	})
	return observer, server.Collections.FlushAll, server.Backend.Checkpoint, evidence
}
func TestCheckpointDisabledHasNoControlOrCallbacks(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "")
	var observer *Observer
	control, err := observer.StartCheckpointControl(func() error { t.Fatal("disabled callback"); return nil })
	if err != nil || control != nil {
		t.Fatal(control, err)
	}
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	if _, err = observer.StartCheckpointControl(func() error { return nil }); err == nil {
		t.Fatal("missing owner accepted")
	}
}
func TestPrivateCheckpointActualOwnerAndInjectedFailures(t *testing.T) {
	for _, failure := range []string{"", "flush", "checkpoint"} {
		t.Run(failure, func(t *testing.T) {
			observer, actualFlush, actualCheckpoint, dir := checkpointOwner(t)
			calls := []string{}
			flush := func() error {
				calls = append(calls, "flush")
				if failure == "flush" {
					return errors.New("injected flush")
				}
				return actualFlush()
			}
			checkpoint := func() error {
				calls = append(calls, "checkpoint")
				if failure == "checkpoint" {
					return errors.New("injected checkpoint")
				}
				return actualCheckpoint()
			}
			control, err := observer.startCheckpointControl(flush, checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := filepath.Join(dir, "snapshot.json")
			if err = os.WriteFile(snapshot, []byte(`{"actual":"snapshot"}`), 0600); err != nil {
				t.Fatal(err)
			}
			receipt, err := RequestCheckpoint(context.Background(), dir, strings.Repeat("a", 40), "bounded-control", snapshot)
			if (err == nil) != (failure == "") {
				t.Fatal(receipt, err)
			}
			if receipt.Owner.OwnerID != observer.ownerID || receipt.Owner.Process.PID != os.Getpid() {
				t.Fatal("wrong actual owner", receipt)
			}
			expected := []string{"flush", "checkpoint"}
			if failure == "flush" {
				expected = []string{"flush"}
				if receipt.Checkpoint.Attempted {
					t.Fatal("checkpoint after failed flush")
				}
			}
			if !reflect.DeepEqual(calls, expected) {
				t.Fatal(calls, expected)
			}
			if receipt.Flush.StartUTC.Before(receipt.Result.StartUTC) || receipt.Flush.EndUTC.After(receipt.Result.EndUTC) {
				t.Fatal("flush outside actual result")
			}
			if receipt.Checkpoint.Attempted && receipt.Checkpoint.StartUTC.Before(receipt.Flush.EndUTC) {
				t.Fatal("checkpoint before flush settled")
			}
			var retained CheckpointReceipt
			raw, readErr := os.ReadFile(filepath.Join(dir, "owner.checkpoint.json"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if readErr = json.Unmarshal(raw, &retained); readErr != nil || !reflect.DeepEqual(receipt, retained) {
				t.Fatal("raw receipt mismatch", readErr)
			}
			if _, err = RequestCheckpoint(context.Background(), dir, strings.Repeat("a", 40), "duplicate", snapshot); err == nil {
				t.Fatal("duplicate mutation accepted")
			}
			if !reflect.DeepEqual(calls, expected) {
				t.Fatal("duplicate invoked callbacks")
			}
			closeErr := control.Close()
			if (closeErr == nil) != (failure == "") {
				t.Fatal("lost actual callback error", closeErr)
			}
		})
	}
}
func TestCheckpointStaleEndpointCannotInvokeOwner(t *testing.T) {
	observer, _, _, dir := checkpointOwner(t)
	calls := 0
	control, err := observer.startCheckpointControl(func() error { calls++; return nil }, func() error { calls++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	path := filepath.Join(dir, "owner.checkpoint-control.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var endpoint checkpointEndpoint
	if err = json.Unmarshal(raw, &endpoint); err != nil {
		t.Fatal(err)
	}
	endpoint.OwnerID = strings.Repeat("f", 32)
	raw, _ = json.Marshal(endpoint)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = RequestCheckpoint(context.Background(), dir, "source", "fixture", "unused"); err == nil || calls != 0 {
		t.Fatal(calls, err)
	}
}
