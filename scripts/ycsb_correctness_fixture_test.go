//go:build ignore

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type crashDriverProbe struct {
	calls  int
	failAt int
	cause  error
}

func (p *crashDriverProbe) insert(_ context.Context, r row) error {
	if r.ID != fmt.Sprintf("user%04d", p.calls) {
		return errors.New("non-sequential insert")
	}
	p.calls++
	if p.calls == p.failAt {
		return p.cause
	}
	return nil
}
func (*crashDriverProbe) update(context.Context, update) error {
	panic("crash fixture must not update")
}
func (*crashDriverProbe) get(context.Context, string) (row, error) { panic("unexpected readback") }
func (*crashDriverProbe) scan(context.Context) ([]row, error)      { panic("unexpected scan") }
func (*crashDriverProbe) close(context.Context) error {
	panic("writes must not close/flush/checkpoint")
}

func TestCrashWritesRetainActualOutcomeAndStopOnAmbiguousFailure(t *testing.T) {
	cause := errors.New("transport failure; ACK ambiguous")
	for _, failAt := range []int{0, 3} {
		path := filepath.Join(t.TempDir(), "outcomes.jsonl")
		probe := &crashDriverProbe{failAt: failAt, cause: cause}
		m := crashManifest(identity{1, "source", "crash", "command_wal_durable", "native", "json"})
		err := crashWrites(t.Context(), probe, m, path)
		if failAt == 0 && err != nil || failAt != 0 && !errors.Is(err, cause) {
			t.Fatalf("lost actual result: %v", err)
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		wanted := 64
		if failAt != 0 {
			wanted = failAt
		}
		if len(lines) != wanted || probe.calls != wanted {
			t.Fatal("ambiguous write retried or unrecorded")
		}
		for n, line := range lines {
			var result crashOutcome
			if err := json.Unmarshal([]byte(line), &result); err != nil {
				t.Fatal(err)
			}
			success := failAt == 0 || n != failAt-1
			if result.Ordinal != n || result.Acknowledged != success || result.Write.Success != success || !result.Write.Attempted || result.RequestedACK != "AckVisible" {
				t.Fatalf("lost ACK/result: %+v", result)
			}
			if !success && result.Write.Error != cause.Error() {
				t.Fatal("lost original error")
			}
		}
		if err := crashWrites(t.Context(), probe, m, path); err == nil {
			t.Fatal("outcome journal overwritten")
		}
	}
}

func TestCrashRowsProfileGuaranteesAndTypedWholeRows(t *testing.T) {
	for _, profile := range []string{"command_wal_durable", "command_wal_relaxed", "no_wal_fast"} {
		m := crashManifest(identity{1, "source", "crash", profile, "native", "json"})
		if !crashRowsValid(m, m.Load) {
			t.Fatal("intact state rejected")
		}
		if crashRowsValid(m, m.Load[:12]) != (profile != "command_wal_durable") {
			t.Fatal("wrong loss guarantee")
		}
		if crashRowsValid(m, m.Load[12:13]) != (profile == "no_wal_fast") {
			t.Fatal("invented global no-WAL ACK prefix")
		}
		bad := []row{{ID: m.Load[0].ID, Fields: map[string]value{"field0": {Type: "string", Value: "bad"}}}}
		if crashRowsValid(m, bad) || crashRowsValid(m, []row{m.Load[0], m.Load[0]}) {
			t.Fatal("torn/duplicate row accepted")
		}
	}
}

type checkpointProbe struct {
	calls int
	err   error
}

func (p *checkpointProbe) Checkpoint(context.Context) error { p.calls++; return p.err }

func TestCheckpointResultObservesOneOriginalCallAndError(t *testing.T) {
	cause := errors.New("original public checkpoint failed")
	for _, failure := range []error{nil, cause} {
		probe := &checkpointProbe{err: failure}
		result, err := checkedCheckpoint(t.Context(), probe)
		if probe.calls != 1 || !errors.Is(err, failure) || !result.Attempted || result.Success != (failure == nil) || result.StartUTC.IsZero() || result.EndUTC.Before(result.StartUTC) || result.ElapsedNS < 0 {
			t.Fatalf("lost original call/result: calls=%d result=%+v err=%v", probe.calls, result, err)
		}
		if failure != nil && result.Error != failure.Error() {
			t.Fatal("lost actual checkpoint error")
		}
	}
}

func TestFailedOutcomesRetainedWithoutOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempt.json")
	cause := errors.New("readback failed after ACK")
	observed := []outcome{{ID: "user0001", Sequence: 1, Acknowledged: true, ReadbackOK: false, Error: cause.Error()}}
	id := identity{1, "source", "fixture", "command_wal_durable", "native", "json"}
	if err := persistOutcomes(path, id, "update_operations", observed, cause); !errors.Is(err, cause) {
		t.Fatalf("wanted original failure, got %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved snapshot
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Stage != "update_operations" || len(saved.Operations) != 1 || !saved.Operations[0].Acknowledged || saved.Operations[0].ReadbackOK || saved.Operations[0].Error != cause.Error() {
		t.Fatalf("lost failure/ACK: %+v", saved)
	}
	if err := persistOutcomes(path, id, "load_operations", nil, nil); err == nil {
		t.Fatal("reused attempt path accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(data) {
		t.Fatal("attempt was overwritten")
	}
}

func TestSnapshotDecodeRejectsDuplicateJSONAndWrongPhysicalTypes(t *testing.T) {
	for _, raw := range []string{
		`{"_ycsb_key":"id","field0":"YQ==","field0":"Yg=="}`,
		`{"_ycsb_key":"id","field0":100}`,
		`{"_ycsb_key":"id","field0":"YQ=="} {}`,
		`[]`,
	} {
		if _, err := decodeJSON("id", []byte(raw)); err == nil {
			t.Fatalf("accepted malformed document %s", raw)
		}
	}
	if _, err := decodeJSON("id", []byte(`{"_ycsb_key":"id","field0":"YQ=="}`)); err != nil {
		t.Fatal(err)
	}
}
