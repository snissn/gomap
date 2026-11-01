package ycsbevidence

import (
	"encoding/json"
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
)

// This first characterization uses only the existing actual-owner fixture and
// raw private TCP protocol. It must compile before phase_stats is implemented;
// on that source the unknown operation is rejected before any storage callback.
func TestPhaseStatsOriginalOwnerReadOnly(t *testing.T) {
	observer, actualFlush, actualCheckpoint, dir := checkpointOwner(t)
	t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
	t.Setenv("YCSB_OWNER_PHASE_STATS", "1")

	var observations, flushes, checkpoints atomic.Int32
	actualStats := observer.stats
	observer.stats = func() map[string]string {
		observations.Add(1)
		return actualStats()
	}
	before, err := observer.observe()
	if err != nil {
		t.Fatal("actual owner setup observation:", err)
	}
	sequence := func(observation Observation) uint64 {
		t.Helper()
		value, err := strconv.ParseUint(observation.Stats["treedb.cache.cow.txn.logical_seq"], 10, 64)
		if err != nil || !observer.backend.HasNativeSemanticBridge() || observation.Backend != "cow" ||
			observation.Profile != "command_wal_relaxed" || observation.CaptureError != "" ||
			observation.Stats["treedb.profile.resolved"] != "command_wal_relaxed" ||
			observation.Stats["treedb.cache.memtable_mode"] != "cow_btree" {
			t.Fatalf("not actual relaxed native owner: observation=%+v sequence_error=%v", observation, err)
		}
		return value
	}
	beforeSequence := sequence(before)
	control, err := observer.StartCheckpointControl(func() error {
		flushes.Add(1)
		return actualFlush()
	}, func() error {
		checkpoints.Add(1)
		return actualCheckpoint()
	})
	if err != nil || control == nil {
		t.Fatalf("actual private control setup: control=%v err=%v", control, err)
	}
	// Registered after checkpointOwner's cleanup, so the actual control is joined
	// before its existing checked original observer/server Close runs, even on RED.
	t.Cleanup(func() {
		if err := control.Close(); err != nil {
			t.Error("actual phase control Close:", err)
		}
		t.Logf("actual control joined before checked original owner Close; flushes=%d checkpoints=%d", flushes.Load(), checkpoints.Load())
	})
	var endpoint checkpointEndpoint
	raw, err := os.ReadFile(filepath.Join(dir, "owner.checkpoint-control.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &endpoint); err != nil || endpoint.OwnerID != before.OwnerID || endpoint.SchemaVersion != 1 || endpoint.Kind != "private_owner_checkpoint_control" {
		t.Fatalf("actual control endpoint=%+v err=%v", endpoint, err)
	}
	address, err := net.ResolveTCPAddr("tcp4", endpoint.Address)
	if err != nil || address == nil || !address.IP.IsLoopback() {
		t.Fatalf("not the private loopback endpoint: %v %v", address, err)
	}
	client, _, _, err := processIdentity(observer.dataDir)
	if err != nil {
		t.Fatal("actual client identity:", err)
	}
	// Test-local wire types deliberately avoid any future production API. Put
	// operation first so baseline strict decoding identifies the intended cause.
	type phaseRequest struct {
		Operation     string  `json:"operation"`
		SchemaVersion int     `json:"schema_version"`
		OwnerID       string  `json:"owner_id"`
		RequestID     string  `json:"request_id"`
		Client        Process `json:"client"`
		SourceID      string  `json:"source_id"`
		CampaignID    string  `json:"campaign_id"`
		PhaseID       string  `json:"phase_id"`
		Stem          string  `json:"stem"`
		Boundary      string  `json:"boundary"`
		Sequence      int     `json:"sequence"`
	}
	request := phaseRequest{
		Operation: "phase_stats", SchemaVersion: 1, OwnerID: before.OwnerID,
		RequestID: strings.Repeat("d", 32), Client: client,
		SourceID:   "71639b0fc5c9598e9715d797072e50cd8aaf27ab",
		CampaignID: strings.Repeat("c", 32),
		PhaseID:    strings.Repeat("c", 32) + ":treedb_mongo:load:" + strings.Repeat("e", 32),
		Stem:       "load", Boundary: "before", Sequence: 1,
	}
	conn, err := net.DialTimeout("tcp4", endpoint.Address, 5*time.Second)
	if err != nil {
		t.Fatal("actual private TCP dial:", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error("phase client connection Close:", err)
		}
	}()
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	beforeObservations := observations.Load()
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal("actual phase request write:", err)
	}
	type phaseReceipt struct {
		SchemaVersion int          `json:"schema_version"`
		Kind          string       `json:"kind"`
		Request       phaseRequest `json:"request"`
		Owner         Observation  `json:"owner"`
		Result        Result       `json:"result"`
	}
	var response struct {
		Receipt phaseReceipt `json:"receipt"`
		Error   string       `json:"error"`
	}
	if err = json.NewDecoder(io.LimitReader(conn, 1<<20)).Decode(&response); err != nil {
		t.Fatal("actual phase response decode:", err)
	}
	phaseObservations := observations.Load() - beforeObservations
	after, err := observer.observe()
	if err != nil {
		t.Fatal("post-request actual owner observation:", err)
	}
	if afterSequence := sequence(after); afterSequence != beforeSequence || flushes.Load() != 0 || checkpoints.Load() != 0 {
		t.Fatalf("phase request changed storage: sequence=%d->%d flushes=%d checkpoints=%d", beforeSequence, afterSequence, flushes.Load(), checkpoints.Load())
	}
	t.Logf("actual phase response error=%q; native profile=%s backend=%s logical_sequence=%d unchanged; FlushAll=0 CheckpointStorage=0", response.Error, after.Profile, after.Backend, beforeSequence)
	if response.Error != "" {
		t.Fatalf("missing read-only phase_stats capability on original owner: %s", response.Error)
	}
	if phaseObservations != 1 {
		t.Fatalf("phase operation must observe exactly once, got %d", phaseObservations)
	}
	receipt := response.Receipt
	if receipt.SchemaVersion != 1 || receipt.Kind != "read_only_live_owner_phase_stats" || receipt.Request != request ||
		!receipt.Result.Attempted || !receipt.Result.Success || receipt.Result.Error != "" || receipt.Result.ElapsedNS < 0 ||
		receipt.Result.StartUTC.IsZero() || receipt.Result.EndUTC.Before(receipt.Result.StartUTC) ||
		receipt.Owner.ObservedUTC.Before(receipt.Result.StartUTC) || receipt.Owner.ObservedUTC.After(receipt.Result.EndUTC) ||
		receipt.Owner.OwnerID != before.OwnerID || receipt.Owner.Process != before.Process || receipt.Owner.DataDir != before.DataDir ||
		receipt.Owner.DataDevice != before.DataDevice || receipt.Owner.DataInode != before.DataInode || sequence(receipt.Owner) != beforeSequence {
		t.Fatalf("invalid actual read-only phase receipt: %+v", receipt)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	retained := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var candidate phaseReceipt
		if err = json.Unmarshal(raw, &candidate); err != nil {
			t.Fatal("retained JSON:", err)
		}
		if candidate.Kind == receipt.Kind {
			if !reflect.DeepEqual(candidate, receipt) {
				t.Fatalf("server phase receipt differs from response: %s", entry.Name())
			}
			retained++
		}
	}
	if retained != 1 {
		t.Fatalf("want one actual retained phase receipt, got %d", retained)
	}
}
