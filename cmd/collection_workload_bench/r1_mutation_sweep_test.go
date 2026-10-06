package main

import (
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
	p, err := runR1Sweep(r1SweepConfig{Documents: 32, Operations: 2, Repetitions: 1, Qualification: "rehearsal"}, r1TestSource())
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
	args := []string{"-source-manifest", sourcePath, packetPath}
	if err = validateR1SweepCommand(io.Discard, args); err != nil {
		t.Fatal(err)
	}
	otherSource := p.Source
	otherSource.Commit = strings.Repeat("f", 40)
	writeJSON(sourcePath, otherSource)
	if validateR1SweepCommand(io.Discard, args) == nil {
		t.Fatal("independent source mismatch accepted")
	}
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
		"reopen_unverified":      func(p *r1SweepPacket) { p.Cells[0].ReopenOracle = false },
		"flush_timer":            func(p *r1SweepPacket) { p.Cells[0].FlushNS = 0 },
		"negative_allocation":    func(p *r1SweepPacket) { p.Cells[0].Acknowledgement.BytesPerOp = -1 },
		"retained_small_fixture": func(p *r1SweepPacket) { p.Config.Qualification = "retained"; p.Source.Clean = true },
		"scope":                  func(p *r1SweepPacket) { p.ExecutionScope = "native direct backend" },
		"runtime_source":         func(p *r1SweepPacket) { p.Source.RuntimeBlobs["go.mod"] = p.Source.Commit },
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
