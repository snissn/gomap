package ycsbevidence

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
)

// Kept separate so the persistence regression is identical across the callback
// signature repair; this helper is the public gateway wiring under test.
func startActualOwnerCheckpoint(observer *Observer, server *mongogateway.StandaloneServer) (*CheckpointControl, error) {
	return observer.StartCheckpointControl(server.Collections.FlushAll, server.Collections.CheckpointStorage)
}

func checkpointOwner(t *testing.T) (*Observer, func() error, func() error, string) {
	t.Helper()
	requireLinuxProcessEvidence(t)
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
	return observer, server.Collections.FlushAll, server.Collections.CheckpointStorage, evidence
}
func TestCheckpointDisabledHasNoControlOrCallbacks(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "")
	var observer *Observer
	unexpected := func() error { t.Fatal("disabled or invalid callback"); return nil }
	control, err := observer.StartCheckpointControl(unexpected, unexpected)
	if err != nil || control != nil {
		t.Fatal(control, err)
	}
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	if _, err = observer.StartCheckpointControl(unexpected, unexpected); err == nil {
		t.Fatal("missing owner accepted")
	}
	observer = &Observer{}
	for _, callbacks := range [][2]func() error{{nil, unexpected}, {unexpected, nil}} {
		if control, err = observer.StartCheckpointControl(callbacks[0], callbacks[1]); err == nil || control != nil {
			t.Fatal("missing owner callback accepted", control, err)
		}
	}
}
func TestPrivateCheckpointActualOwnerAndInjectedFailures(t *testing.T) {
	for _, failure := range []string{"", "flush", "checkpoint"} {
		t.Run(failure, func(t *testing.T) {
			observer, actualFlush, actualCheckpoint, dir := checkpointOwner(t)
			t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
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
			control, err := observer.StartCheckpointControl(flush, checkpoint)
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
			if receipt.SchemaVersion != 2 {
				t.Fatal("not owner-complete receipt schema", receipt.SchemaVersion)
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

func TestRequestCheckpointRejectsLegacyReceiptSchema(t *testing.T) {
	observer, _, _, dir := checkpointOwner(t)
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err = writeNew(filepath.Join(dir, "owner.checkpoint-control.json"), checkpointEndpoint{1, "private_owner_checkpoint_control", observer.ownerID, ln.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot.json")
	if err = os.WriteFile(snapshot, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		var request CheckpointRequest
		if err = json.NewDecoder(conn).Decode(&request); err == nil {
			err = json.NewEncoder(conn).Encode(checkpointResponse{Receipt: CheckpointReceipt{SchemaVersion: 1, Kind: "explicit_live_owner_checkpoint", Request: request, Result: Result{Success: true}}})
		}
		done <- err
	}()
	if _, err = RequestCheckpoint(context.Background(), dir, "source", "fixture", snapshot); err == nil || !strings.Contains(err.Error(), "checkpoint response mismatch") {
		t.Fatal("legacy receipt accepted", err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointIdleConnectionBoundsClose(t *testing.T) {
	observer, _, _, _ := checkpointOwner(t)
	var calls atomic.Int32
	unrequested := func() error { calls.Add(1); return nil }
	control, err := observer.startCheckpointControl(unrequested, unrequested)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp4", control.listener.Addr().String())
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		conn.Close()
		if !closed {
			control.Close()
		}
	})
	// An incomplete JSON request exercises the actual listener's idle read.
	if _, err = conn.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	var one [1]byte
	if _, err = conn.Read(one[:]); err == nil {
		t.Fatal("incomplete request unexpectedly produced a response")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal("incomplete request did not remain idle", err)
	}
	conn.SetReadDeadline(time.Time{})
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- control.Close() }()
	select {
	case err = <-done:
		closed = true
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("idle connection Close completed in %s without callbacks", time.Since(started))
	case <-time.After(7 * time.Second):
		// Unblock and join the old two-minute reader before reporting RED.
		conn.Close()
		select {
		case <-done:
			closed = true
		case <-time.After(2 * time.Second):
			t.Fatal("idle reader did not settle after connection cleanup")
		}
		t.Fatal("idle connection held Close beyond seven seconds")
	}
	if calls.Load() != 0 {
		t.Fatal("incomplete request invoked callbacks", calls.Load())
	}
}

func TestCheckpointCloseWaitsForActualCallback(t *testing.T) {
	observer, flush, checkpoint, dir := checkpointOwner(t)
	entered, release := make(chan struct{}), make(chan struct{})
	control, err := observer.startCheckpointControl(func() error {
		close(entered)
		<-release
		return flush()
	}, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot.json")
	if err = os.WriteFile(snapshot, []byte(`{}`), 0600); err != nil {
		control.Close()
		t.Fatal(err)
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := RequestCheckpoint(context.Background(), dir, "source", "fixture", snapshot)
		requestDone <- err
	}()
	select {
	case <-entered:
	case err = <-requestDone:
		control.Close()
		t.Fatal("request failed before callback", err)
	case <-time.After(5 * time.Second):
		close(release)
		control.Close()
		t.Fatal("callback did not begin")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- control.Close() }()
	select {
	case err = <-closeDone:
		close(release)
		<-requestDone
		t.Fatal("Close returned while actual callback was active", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err = <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err = <-closeDone; err != nil {
		t.Fatal(err)
	}
}
