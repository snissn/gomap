package ycsbevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func phaseStatsRequest(owner Observation, sequence int) CheckpointRequest {
	stem, boundary := "load", "before"
	pair := (sequence - 1) / 2
	if pair != 0 {
		stem = fmt.Sprintf("run_%d", pair)
	}
	if sequence%2 == 0 {
		boundary = "after"
	}
	campaign := strings.Repeat("c", 32)
	return CheckpointRequest{
		SchemaVersion: 1, OwnerID: owner.OwnerID, RequestID: fmt.Sprintf("%032x", sequence), Client: owner.Process,
		SourceID: "71639b0fc5c9598e9715d797072e50cd8aaf27ab", Operation: "phase_stats",
		CampaignID: campaign, PhaseID: campaign + ":treedb_mongo:" + stem + ":" + fmt.Sprintf("%032x", pair+1),
		Stem: stem, Boundary: boundary, Sequence: sequence,
	}
}

func exchangePhaseStats(t *testing.T, control *CheckpointControl, request CheckpointRequest) phaseStatsResponse {
	t.Helper()
	response, err := phaseStatsExchange(control, request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func phaseStatsExchange(control *CheckpointControl, request CheckpointRequest) (response phaseStatsResponse, resultErr error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return response, err
	}
	return phaseStatsRawExchange(control, append(raw, '\n'), false)
}

func phaseStatsRawExchange(control *CheckpointControl, frame []byte, closeWrite bool) (response phaseStatsResponse, resultErr error) {
	conn, err := net.DialTimeout("tcp4", control.listener.Addr().String(), 5*time.Second)
	if err != nil {
		return response, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, conn.Close())
	}()
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return response, err
	}
	n, err := conn.Write(frame)
	if err != nil {
		return response, err
	}
	if n != len(frame) {
		return response, io.ErrShortWrite
	}
	if closeWrite {
		if err = conn.(*net.TCPConn).CloseWrite(); err != nil {
			return response, err
		}
	}
	err = json.NewDecoder(io.LimitReader(conn, 1<<20)).Decode(&response)
	return response, err
}

func startPhaseStatsTestControl(t *testing.T, observer *Observer, flush, checkpoint func() error) (*CheckpointControl, func() error) {
	t.Helper()
	control, err := observer.StartCheckpointControl(flush, checkpoint)
	if err != nil || control == nil {
		t.Fatalf("control=%v err=%v", control, err)
	}
	var closed atomic.Bool
	closeControl := func() error {
		if closed.Swap(true) {
			return errors.New("test tried to close control twice")
		}
		return control.Close()
	}
	t.Cleanup(func() {
		if !closed.Load() {
			if err := closeControl(); err != nil {
				t.Error(err)
			}
		}
	})
	return control, closeControl
}

func phaseStatsSequence(t *testing.T, owner Observation) uint64 {
	t.Helper()
	seq, err := strconv.ParseUint(owner.Stats["treedb.cache.cow.txn.logical_seq"], 10, 64)
	if err != nil || owner.Backend != "cow" || owner.Profile != "command_wal_relaxed" || owner.CaptureError != "" {
		t.Fatalf("actual native owner: %+v err=%v", owner, err)
	}
	return seq
}

func TestPhaseStatsPairPreservesActualOwnerAndFreshReadback(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "1")
	observer, server, closeOwner, dir := checkpointOwnerWithServer(t)
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(7)}, {Key: "binary", Value: bson.Binary{Subtype: 0, Data: []byte{0, 255, 1, 128}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.Collections.CreateCollection(&collections.CollectionMeta{Name: "db.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	c, err := server.Collections.OpenCollection("db.rows")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InsertBatchValidatedBSON([][]byte{[]byte("row")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	var observed, storageCalls atomic.Int32
	stats := observer.stats
	observer.stats = func() map[string]string { observed.Add(1); return stats() }
	before, err := observer.observe()
	if err != nil {
		t.Fatal(err)
	}
	seq, ack := phaseStatsSequence(t, before), server.Server.StandaloneWriteConcernStats()
	control, closeControl := startPhaseStatsTestControl(t, observer, func() error { storageCalls.Add(1); return server.Collections.FlushAll() }, func() error { storageCalls.Add(1); return server.Collections.CheckpointStorage() })
	for sequence := 1; sequence <= 2; sequence++ {
		request := phaseStatsRequest(before, sequence)
		count := observed.Load()
		response := exchangePhaseStats(t, control, request)
		if response.Error != "" || !response.Receipt.Result.Success || response.Receipt.Request != request || observed.Load()-count != 1 || phaseStatsSequence(t, response.Receipt.Owner) != seq {
			t.Fatalf("actual phase response=%+v observations=%d", response, observed.Load()-count)
		}
		var retained phaseStatsReceipt
		raw, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("owner.phase-stats-%02d.json", sequence)))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(raw, &retained); err != nil || !reflect.DeepEqual(retained, response.Receipt) {
			t.Fatal("server retained phase differs", err)
		}
	}
	if storageCalls.Load() != 0 || server.Server.StandaloneWriteConcernStats() != ack {
		t.Fatal("read-only observation changed storage or gateway acknowledgements")
	}
	// The phase arm must not reserve or consume the old one-shot checkpoint.
	snapshot := filepath.Join(dir, "row.bson")
	if err = os.WriteFile(snapshot, doc, 0600); err != nil {
		t.Fatal(err)
	}
	receipt, err := RequestCheckpoint(context.Background(), dir, "source", "phase-pair", snapshot)
	if err != nil || receipt.SchemaVersion != 2 || !receipt.Flush.Success || !receipt.Checkpoint.Success || storageCalls.Load() != 2 {
		t.Fatalf("unaffected physical checkpoint receipt=%+v calls=%d err=%v", receipt, storageCalls.Load(), err)
	}
	if err = closeControl(); err != nil {
		t.Fatal(err)
	}
	if err = closeOwner(); err != nil {
		t.Fatal("checked original owner Close:", err)
	}
	fresh, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: observer.dataDir, Profile: treedb.ProfileCommandWALRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fresh.Close(); err != nil {
			t.Error("fresh owner Close:", err)
		}
	}()
	if !fresh.Backend.HasNativeSemanticBridge() || fresh.Stats()["treedb.cache.memtable_mode"] != "cow_btree" {
		t.Fatal("fresh owner is not native")
	}
	c, err = fresh.Collections.OpenCollection("db.rows")
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := c.GetInto([]byte("row"), nil)
	if err != nil || !found || !bytes.Equal(got, doc) {
		t.Fatalf("fresh typed readback found=%v got=%x want=%x err=%v", found, got, doc, err)
	}
}

func TestPhaseStatsDisabledAndPartialEnable(t *testing.T) {
	for _, config := range []struct{ checkpoint, phase string }{{"", "1"}, {"1", "invalid"}, {"invalid", "1"}} {
		t.Run(config.checkpoint+"/"+config.phase, func(t *testing.T) {
			t.Setenv("YCSB_OWNER_CHECKPOINT", config.checkpoint)
			t.Setenv("YCSB_OWNER_PHASE_STATS", config.phase)
			observer, _, _, dir := checkpointOwner(t)
			if control, err := observer.StartCheckpointControl(func() error { t.Error("invalid callback"); return nil }, func() error { t.Error("invalid callback"); return nil }); err == nil || control != nil {
				if control != nil {
					if closeErr := control.Close(); closeErr != nil {
						t.Error(closeErr)
					}
				}
				t.Fatal("partial enable accepted", control, err)
			}
			if _, err := os.Stat(filepath.Join(dir, "owner.checkpoint-control.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial enable published an endpoint", err)
			}
		})
	}
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "")
	observer, flush, checkpoint, _ := checkpointOwner(t)
	before, err := observer.observe()
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	control, _ := startPhaseStatsTestControl(t, observer, func() error { calls.Add(1); return flush() }, func() error { calls.Add(1); return checkpoint() })
	response := exchangePhaseStats(t, control, phaseStatsRequest(before, 1))
	if !strings.Contains(response.Error, "disabled") || calls.Load() != 0 {
		t.Fatal("disabled phase callback", response, calls.Load())
	}
	after, err := observer.observe()
	if err != nil || phaseStatsSequence(t, after) != phaseStatsSequence(t, before) {
		t.Fatal("disabled phase changed logical state", err)
	}
}

func TestPhaseStatsRejectsInvalidBeforeReservation(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "1")
	observer, flush, checkpoint, dir := checkpointOwner(t)
	before, err := observer.observe()
	if err != nil {
		t.Fatal(err)
	}
	var observations, calls atomic.Int32
	stats := observer.stats
	observer.stats = func() map[string]string { observations.Add(1); return stats() }
	control, _ := startPhaseStatsTestControl(t, observer, func() error { calls.Add(1); return flush() }, func() error { calls.Add(1); return checkpoint() })
	mutants := []struct {
		name string
		edit func(*CheckpointRequest)
	}{
		{"schema", func(r *CheckpointRequest) { r.SchemaVersion = 2 }},
		{"owner", func(r *CheckpointRequest) { r.OwnerID = strings.Repeat("f", 32) }},
		{"request", func(r *CheckpointRequest) { r.RequestID = strings.Repeat("x", 32) }},
		{"source", func(r *CheckpointRequest) { r.SourceID = "" }},
		{"campaign", func(r *CheckpointRequest) { r.CampaignID = "" }},
		{"phase", func(r *CheckpointRequest) { r.PhaseID = "wrong" }},
		{"stem", func(r *CheckpointRequest) { r.Stem = "run_1" }},
		{"boundary", func(r *CheckpointRequest) { r.Boundary = "after" }},
		{"zero", func(r *CheckpointRequest) { r.Sequence = 0 }},
		{"skip", func(r *CheckpointRequest) { r.Sequence = 3 }},
		{"excess", func(r *CheckpointRequest) { r.Sequence = 65 }},
		{"snapshot", func(r *CheckpointRequest) { r.SnapshotSHA256 = strings.Repeat("a", 64) }},
		{"fixture", func(r *CheckpointRequest) { r.FixtureID = "wrong" }},
		{"stale_process", func(r *CheckpointRequest) { r.Client.StartTicks++ }},
		{"wrong_executable", func(r *CheckpointRequest) { r.Client.EXESHA256 = strings.Repeat("a", 64) }},
		{"invalid_process", func(r *CheckpointRequest) { r.Client.PID = 0 }},
		{"unknown_operation", func(r *CheckpointRequest) { r.Operation = "unknown" }},
	}
	for _, mutant := range mutants {
		t.Run(mutant.name, func(t *testing.T) {
			request := phaseStatsRequest(before, 1)
			mutant.edit(&request)
			if response := exchangePhaseStats(t, control, request); response.Error == "" {
				t.Fatal("invalid request accepted", response)
			}
		})
	}
	if observations.Load() != 0 || calls.Load() != 0 {
		t.Fatal("invalid request observed or touched storage", observations.Load(), calls.Load())
	}
	if _, err = os.Stat(filepath.Join(dir, "owner.phase-stats-01-request.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid request reserved phase", err)
	}
	first := phaseStatsRequest(before, 1)
	if response := exchangePhaseStats(t, control, first); response.Error != "" {
		t.Fatal("invalid request poisoned later valid request", response)
	}
	if response := exchangePhaseStats(t, control, first); response.Error == "" {
		t.Fatal("duplicate sequence accepted")
	}
	for _, edit := range []func(*CheckpointRequest){
		func(r *CheckpointRequest) { r.CampaignID = "other" },
		func(r *CheckpointRequest) { r.SourceID = "other" },
		func(r *CheckpointRequest) { r.PhaseID = r.CampaignID + ":treedb_mongo:load:" + strings.Repeat("f", 32) },
		func(r *CheckpointRequest) { r.RequestID = first.RequestID },
	} {
		r := phaseStatsRequest(before, 2)
		edit(&r)
		if response := exchangePhaseStats(t, control, r); response.Error == "" {
			t.Fatal("mismatched pair accepted")
		}
	}
	if observations.Load() != 1 || calls.Load() != 0 {
		t.Fatal("rejected reuse/pair touched owner", observations.Load(), calls.Load())
	}
	after, err := observer.observe()
	if err != nil || phaseStatsSequence(t, after) != phaseStatsSequence(t, before) {
		t.Fatal("rejected request changed logical state", err)
	}
}

func TestPhaseStatsRejectsMalformedRawFramesBeforeAdmission(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "1")
	observer, flush, checkpoint, dir := checkpointOwner(t)
	before, err := observer.observe()
	if err != nil {
		t.Fatal(err)
	}
	request := phaseStatsRequest(before, 1)
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	prefix := func(member string) []byte {
		return append(append([]byte("{"+member+","), raw[1:]...), '\n')
	}
	replace := func(old, new string) []byte {
		return append(bytes.Replace(raw, []byte(old), []byte(new), 1), '\n')
	}
	// This would be a valid legacy mutation after ordinary last-value-wins
	// decoding. The earlier phase operation must still select strict rejection.
	legacy, err := json.Marshal(CheckpointRequest{SchemaVersion: 1, OwnerID: before.OwnerID,
		RequestID: strings.Repeat("a", 32), Client: before.Process, SourceID: "source",
		FixtureID: "fixture", SnapshotSHA256: strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	hiddenPhase := append(append([]byte(`{"operation":"phase_stats","operation":"",`), legacy[1:]...), '\n')
	cases := []struct {
		name       string
		frame      []byte
		closeWrite bool
	}{
		{"duplicate_operation", prefix(`"operation":"unknown"`), false},
		{"duplicate_owner", prefix(`"owner_id":"wrong"`), false},
		{"duplicate_sequence", prefix(`"sequence":0`), false},
		{"escaped_duplicate", prefix(`"sequ\u0065nce":0`), false},
		{"case_duplicate", prefix(`"Sequence":0`), false},
		{"nested_client_duplicate", replace(`"client":{`, `"client":{"pid":0,`), false},
		{"nested_escaped_duplicate", replace(`"client":{`, `"client":{"p\u0069d":0,`), false},
		{"hidden_earlier_phase", hiddenPhase, false},
		{"unique_case_alias", replace(`"sequence":`, `"Sequence":`), false},
		{"unique_client_alias", replace(`"client":`, `"CLIENT":`), false},
		{"unique_pid_alias", replace(`"pid":`, `"PID":`), false},
		{"null_client_environment", replace(`"gomaxprocs_unset":`+strconv.FormatBool(before.Process.GOMAXPROCSUnset), `"gomaxprocs_unset":null`), false},
		{"missing_client_environment", replace(`,"gomaxprocs_unset":`+strconv.FormatBool(before.Process.GOMAXPROCSUnset), ""), false},
		{"trailing_second_json", append(append([]byte{}, raw...), []byte(" {}\n")...), false},
		{"trailing_garbage", append(append([]byte{}, raw...), []byte(" garbage\n")...), false},
		{"missing_newline", raw, true},
		{"frame_limit_without_newline", append(append([]byte{}, raw...), bytes.Repeat([]byte(" "), 8192-len(raw))...), false},
	}
	var observations, calls atomic.Int32
	stats := observer.stats
	observer.stats = func() map[string]string { observations.Add(1); return stats() }
	control, _ := startPhaseStatsTestControl(t, observer,
		func() error { calls.Add(1); return flush() },
		func() error { calls.Add(1); return checkpoint() })
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, err := phaseStatsRawExchange(control, tc.frame, tc.closeWrite)
			if err != nil || response.Error == "" {
				t.Fatalf("malformed frame response=%+v err=%v", response, err)
			}
			if observations.Load() != 0 || calls.Load() != 0 {
				t.Fatal("malformed frame observed or mutated owner", observations.Load(), calls.Load())
			}
			for _, name := range []string{"owner.phase-stats-01-request.json", "owner.phase-stats-01.json", "owner.checkpoint-request.json", "owner.checkpoint.json"} {
				if _, err = os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("malformed frame reserved evidence", name, err)
				}
			}
		})
	}
	// Internal pretty-print newlines must work; no write-side EOF is sent.
	pretty, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	response, err := phaseStatsRawExchange(control, append(pretty, '\n'), false)
	if err != nil || response.Error != "" || !response.Receipt.Result.Success || response.Receipt.Request != request || observations.Load() != 1 || calls.Load() != 0 {
		t.Fatalf("rejections prevented later valid newline frame: response=%+v observations=%d calls=%d err=%v", response, observations.Load(), calls.Load(), err)
	}
	if phaseStatsSequence(t, response.Receipt.Owner) != phaseStatsSequence(t, before) {
		t.Fatal("raw frame controls changed logical sequence")
	}
	// The existing legacy decoder still accepts its original first-JSON,
	// last-value-wins envelope. Phase-only strictness must not change it.
	legacyFrame := append(append([]byte(`{"owner_id":"wrong",`), legacy[1:]...), []byte(" garbage\n")...)
	response, err = phaseStatsRawExchange(control, legacyFrame, false)
	if err != nil || response.Error != "" || response.Receipt.SchemaVersion != 2 || response.Receipt.Kind != "explicit_live_owner_checkpoint" || !response.Receipt.Result.Success || observations.Load() != 2 || calls.Load() != 2 {
		t.Fatalf("legacy decoding changed: response=%+v observations=%d calls=%d err=%v", response, observations.Load(), calls.Load(), err)
	}
}

func TestPhaseStatsBoundedSequenceAndRetainedFiles(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "1")
	observer, _, _, dir := checkpointOwner(t)
	before, err := observer.observe()
	if err != nil {
		t.Fatal(err)
	}
	var observations atomic.Int32
	stats := observer.stats
	observer.stats = func() map[string]string { observations.Add(1); return stats() }
	unexpected := func() error { t.Error("phase invoked checkpoint callback"); return nil }
	control, _ := startPhaseStatsTestControl(t, observer, unexpected, unexpected)
	for sequence := 1; sequence <= 64; sequence++ {
		request := phaseStatsRequest(before, sequence)
		if response := exchangePhaseStats(t, control, request); response.Error != "" || !response.Receipt.Result.Success || response.Receipt.Request != request {
			t.Fatalf("sequence=%d response=%+v", sequence, response)
		}
		for _, name := range []string{fmt.Sprintf("owner.phase-stats-%02d-request.json", sequence), fmt.Sprintf("owner.phase-stats-%02d.json", sequence)} {
			if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if response := exchangePhaseStats(t, control, phaseStatsRequest(before, 65)); response.Error == "" {
		t.Fatal("more than64 observations accepted")
	}
	if observations.Load() != 64 {
		t.Fatalf("want exactly64 observations, got%d", observations.Load())
	}
}

func TestPhaseStatsObservationAndReceiptFailuresAreTerminal(t *testing.T) {
	for _, failure := range []string{"observation", "request_write", "receipt_write"} {
		t.Run(failure, func(t *testing.T) {
			t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
			t.Setenv("YCSB_OWNER_PHASE_STATS", "1")
			observer, _, _, dir := checkpointOwner(t)
			before, err := observer.observe()
			if err != nil {
				t.Fatal(err)
			}
			var observations, calls atomic.Int32
			stats := observer.stats
			observer.stats = func() map[string]string {
				observations.Add(1)
				result := stats()
				if failure == "observation" && observations.Load() == 1 {
					result["treedb.profile.resolved"] = "wrong"
				}
				return result
			}
			if failure != "observation" {
				name := "owner.phase-stats-01.json"
				if failure == "request_write" {
					name = "owner.phase-stats-01-request.json"
				}
				if err = os.WriteFile(filepath.Join(dir, name), []byte("retained collision"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			unrequested := func() error { calls.Add(1); return nil }
			control, closeControl := startPhaseStatsTestControl(t, observer, unrequested, unrequested)
			if response := exchangePhaseStats(t, control, phaseStatsRequest(before, 1)); response.Error == "" {
				t.Fatal("failure accepted", response)
			}
			if failure == "observation" {
				raw, err := os.ReadFile(filepath.Join(dir, "owner.phase-stats-01.json"))
				if err != nil {
					t.Fatal("failed observation receipt not retained", err)
				}
				var receipt phaseStatsReceipt
				if err = json.Unmarshal(raw, &receipt); err != nil || !receipt.Result.Attempted || receipt.Result.Success || receipt.Result.Error == "" {
					t.Fatal("lost actual failed observation", receipt, err)
				}
			} else {
				name := "owner.phase-stats-01.json"
				if failure == "request_write" {
					name = "owner.phase-stats-01-request.json"
				}
				raw, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || string(raw) != "retained collision" {
					t.Fatal("exclusive evidence collision was overwritten", err)
				}
			}
			count := observations.Load()
			expected := int32(1)
			if failure == "request_write" {
				expected = 0
			}
			if count != expected {
				t.Fatalf("failure=%s observations=%d want=%d", failure, count, expected)
			}
			if response := exchangePhaseStats(t, control, phaseStatsRequest(before, 1)); !strings.Contains(response.Error, "terminal") {
				t.Fatal("admitted failure retried", response)
			}
			if response := exchangePhaseStats(t, control, phaseStatsRequest(before, 2)); !strings.Contains(response.Error, "terminal") {
				t.Fatal("later operation admitted after failure", response)
			}
			if observations.Load() != count || calls.Load() != 0 {
				t.Fatal("terminal control observed or touched storage")
			}
			if err = closeControl(); err == nil {
				t.Fatal("Close lost admitted phase failure")
			}
		})
	}
}

func TestPhaseStatsCloseWaitsForObservation(t *testing.T) {
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "1")
	observer, _, _, _ := checkpointOwner(t)
	before, err := observer.observe()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	stats := observer.stats
	var calls atomic.Int32
	observer.stats = func() map[string]string {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return stats()
	}
	unrequested := func() error { t.Error("observation invoked storage callback"); return nil }
	control, closeControl := startPhaseStatsTestControl(t, observer, unrequested, unrequested)
	responseDone := make(chan error, 1)
	go func() {
		response, err := phaseStatsExchange(control, phaseStatsRequest(before, 1))
		if err == nil && response.Error != "" {
			err = errors.New(response.Error)
		}
		responseDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("observation did not begin")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- closeControl() }()
	select {
	case err := <-closeDone:
		close(release)
		<-responseDone
		t.Fatal("control Close returned during observation", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err = <-responseDone; err != nil {
		t.Fatal(err)
	}
	if err = <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestPhaseStatsPreservesPopulatedLegacyRequestJSON(t *testing.T) {
	r := CheckpointRequest{SchemaVersion: 1, OwnerID: "owner", RequestID: "request", Client: Process{1, 2, "exe", true}, SourceID: "source", FixtureID: "fixture", SnapshotSHA256: "snapshot"}
	raw, err := json.Marshal(r)
	const legacy = `{"schema_version":1,"owner_id":"owner","request_id":"request","client":{"pid":1,"start_ticks":2,"exe_sha256":"exe","gomaxprocs_unset":true},"source_id":"source","fixture_id":"fixture","snapshot_sha256":"snapshot"}`
	if err != nil || string(raw) != legacy {
		t.Fatalf("populated legacy bytes changed: %s err=%v", raw, err)
	}
	r = phaseStatsRequest(Observation{}, 1)
	raw, err = json.Marshal(r)
	if err != nil || bytes.Contains(raw, []byte("fixture_id")) || bytes.Contains(raw, []byte("snapshot_sha256")) {
		t.Fatalf("phase request claims a checkpoint snapshot: %s err=%v", raw, err)
	}
}
