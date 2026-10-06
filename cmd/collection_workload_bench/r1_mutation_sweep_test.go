package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The real 16-cell packet exercises both public admission paths, full-row and
// complete historical-posting oracles, reopen, and required counter availability.
// Corrupt copies keep all unrelated original fields, including source identities.
func TestR1MutationSweepSmokeAndCorruption(t *testing.T) {
	// These expected source pins are selected before creating the real packet.
	expectedSource := r1TestSource()
	p, err := runR1Sweep(r1SweepConfig{Documents: 32, Operations: 2, Repetitions: 1, Qualification: "rehearsal"}, expectedSource)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateR1Sweep(p); err != nil {
		t.Fatal(err)
	}
	// Replay the original through the public validator and require its separate
	// expected-source input to reject a well-formed different commit.
	dir := t.TempDir()
	packetPath, sourcePath := filepath.Join(dir, "packet.json"), filepath.Join(dir, "source.json")
	writeJSON := func(path string, value any) {
		t.Helper()
		raw, e := json.Marshal(value)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	writeJSON(packetPath, p)
	writeJSON(sourcePath, p.Source)
	args := []string{"-semantic-only", "-source-manifest", sourcePath, packetPath}
	var semanticOutput bytes.Buffer
	if err = validateR1SweepCommand(&semanticOutput, args); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(semanticOutput.String(), "UNQUALIFIED ") {
		t.Fatal("producer semantic check claimed acceptance")
	}
	otherSource := p.Source
	otherSource.Commit = strings.Repeat("f", 40)
	writeJSON(sourcePath, otherSource)
	if validateR1SweepCommand(io.Discard, args) == nil {
		t.Fatal("independent source mismatch accepted")
	}
	writeJSON(sourcePath, expectedSource)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(packetPath)
	if err != nil {
		t.Fatal(err)
	}
	// The separate completed-run receipt hashes the original bytes, never a
	// decoded/re-encoded packet. Receipt verification also works for a diagnostic
	// but must continue to label that diagnostic UNQUALIFIED.
	flags := []string{"-expected-commit", "-expected-runtime", "-expected-harness", "-expected-landed-tooling-commit", "-expected-binary-sha256", "-expected-packet-sha256"}
	values := []string{expectedSource.Commit, expectedSource.RuntimeSHA256, expectedSource.HarnessSHA256, expectedSource.Commit, r1SweepSHA256(binary), r1SweepSHA256(original)}
	receiptArgs := func(missing int, wrong int) []string {
		result := []string{"-source-manifest", sourcePath}
		for i, flag := range flags {
			if i == missing {
				continue
			}
			value := values[i]
			if i == wrong {
				value = strings.Repeat("0", len(value))
			}
			result = append(result, flag, value)
		}
		return append(result, packetPath)
	}
	var receiptOutput bytes.Buffer
	if err = validateR1SweepCommand(&receiptOutput, receiptArgs(-1, -1)); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(receiptOutput.String(), "UNQUALIFIED R1 mutation sweep rehearsal receipt verified:") {
		t.Fatal("rehearsal receipt claimed retained acceptance")
	}
	for i, flag := range flags {
		t.Run("missing_"+flag, func(t *testing.T) {
			if validateR1SweepCommand(io.Discard, receiptArgs(i, -1)) == nil {
				t.Fatal("missing independent pin accepted")
			}
		})
		t.Run("wrong_"+flag, func(t *testing.T) {
			if validateR1SweepCommand(io.Discard, receiptArgs(-1, i)) == nil {
				t.Fatal("wrong independent pin accepted")
			}
		})
	}
	// Whitespace preserves every decoded value but changes the original packet
	// byte identity; producer-local semantic validation may not waive the receipt.
	if err = os.WriteFile(packetPath, append(original, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if validateR1SweepCommand(io.Discard, receiptArgs(-1, -1)) == nil {
		t.Fatal("changed original bytes accepted")
	}
	if err = os.WriteFile(packetPath, original, 0600); err != nil {
		t.Fatal(err)
	}
	if validateR1SweepCommand(io.Discard, append([]string{"-semantic-only"}, receiptArgs(-1, -1)...)) == nil {
		t.Fatal("semantic-only silently ignored receipt pins")
	}
	// Even a relabeled retained packet cannot bypass the independent-pin gate.
	relabelled := p
	relabelled.Config.Qualification = "retained"
	writeJSON(packetPath, relabelled)
	if e := validateR1SweepCommand(io.Discard, []string{"-source-manifest", sourcePath, packetPath}); e == nil || !strings.Contains(e.Error(), "all six independent") {
		t.Fatalf("retained receipt gate: %v", e)
	}
	writeJSON(packetPath, p)
	for name, corrupt := range map[string]func(*r1SweepPacket){
		"missing_cell":        func(p *r1SweepPacket) { p.Cells = p.Cells[1:] },
		"duplicate_cell":      func(p *r1SweepPacket) { p.Cells[1] = p.Cells[0] },
		"actual_request_rows": func(p *r1SweepPacket) { p.Cells[0].RequestRows = 32 },
		"wrong_fixture":       func(p *r1SweepPacket) { p.Cells[0].FixtureSHA256 = p.Cells[8].FixtureSHA256 },
		"missing_counter":     func(p *r1SweepPacket) { delete(p.Cells[0].CountersBefore, r1SweepCounterKeys[0]) },
		"rebound_delta":       func(p *r1SweepPacket) { p.Cells[0].ACKCounterDelta[r1SweepCounterKeys[0]]++ },
		"counter_decreased": func(p *r1SweepPacket) {
			p.Cells[0].CountersBefore[r1SweepCounterKeys[0]] = p.Cells[0].CountersAfterACK[r1SweepCounterKeys[0]] + 1
		},
		"reopen_unverified":             func(p *r1SweepPacket) { p.Cells[0].ReopenOracle = false },
		"flush_timer":                   func(p *r1SweepPacket) { p.Cells[0].FlushNS = 0 },
		"negative_allocation":           func(p *r1SweepPacket) { p.Cells[0].Acknowledgement.BytesPerOp = -1 },
		"retained_small_fixture":        func(p *r1SweepPacket) { p.Config.Qualification = "retained"; p.Source.Clean = true },
		"scope":                         func(p *r1SweepPacket) { p.ExecutionScope = "native direct backend" },
		"runtime_source":                func(p *r1SweepPacket) { p.Source.RuntimeBlobs["go.mod"] = p.Source.Commit },
		"consistent_zero_physical_sync": func(p *r1SweepPacket) { r1SweepCorruptPhysical(p, "treedb.command_wal.file_sync.calls_total", 0) },
		"consistent_short_physical_sync": func(p *r1SweepPacket) {
			r1SweepCorruptPhysical(p, "treedb.command_wal.file_sync.calls_total", uint64(p.Config.Operations-1))
		},
		"consistent_zero_written_bytes": func(p *r1SweepPacket) { r1SweepCorruptPhysical(p, "treedb.command_wal.write.bytes_total", 0) },
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			var copy r1SweepPacket
			if err = json.Unmarshal(raw, &copy); err != nil {
				t.Fatal(err)
			}
			corrupt(&copy)
			if validateR1Sweep(copy) == nil {
				t.Fatal("corrupted actual packet accepted")
			}
		})
	}
}

// Rebind both snapshots and both deltas so rejection proves the physical-work
// bound, rather than merely finding inconsistent derived counters.
func r1SweepCorruptPhysical(p *r1SweepPacket, key string, ackDelta uint64) {
	c := &p.Cells[0]
	c.CountersAfterACK[key] = c.CountersBefore[key] + ackDelta
	c.CountersAfterFlush[key] = c.CountersAfterACK[key] + c.FlushCounterDelta[key]
	c.ACKCounterDelta[key] = ackDelta
}
