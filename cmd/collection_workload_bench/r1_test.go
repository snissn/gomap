package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/snissn/gomap/TreeDB/collections"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func r1TestSource() r1Source {
	blob := strings.Repeat("b", 40)
	sum := sha256.Sum256([]byte("go.mod\x00" + blob + "\x00"))
	return r1Source{Commit: strings.Repeat("a", 40), RuntimeSHA256: hex.EncodeToString(sum[:]), RuntimeBlobs: map[string]string{"go.mod": blob}, HarnessSHA256: strings.Repeat("c", 64)}
}
func TestR1PublicPathRehearsalAndRejectPackets(t *testing.T) {
	c := r1Config{Documents: 16, Batch: 4, Operations: 3, Repetitions: 1, Durability: "durable", State: "buffered", Engines: r1Engines, Qualification: "rehearsal"}
	p, e := runR1(c, r1TestSource())
	if e != nil {
		t.Fatal(e)
	}
	if e = validateR1Packet(p); e != nil {
		t.Fatal(e)
	}
	path := os.Getenv("GOMAP_R1_TEST_PACKET")
	if path != "" {
		raw, e := json.MarshalIndent(p, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}

	dir := t.TempDir()
	packetPath := filepath.Join(dir, "packet.json")
	sourcePath := filepath.Join(dir, "source.json")
	raw, e := json.Marshal(p)
	if e != nil {
		t.Fatal(e)
	}
	sourceRaw, _ := json.Marshal(p.Source)
	if e = os.WriteFile(packetPath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(sourcePath, sourceRaw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = validateR1Command(&bytes.Buffer{}, []string{"-source-manifest", sourcePath, packetPath}); e != nil {
		t.Fatal(e)
	}
	different := p.Source
	different.HarnessSHA256 = strings.Repeat("d", 64)
	wrong, _ := json.Marshal(different)
	if e = os.WriteFile(sourcePath, wrong, 0600); e != nil {
		t.Fatal(e)
	}
	if e = validateR1Command(&bytes.Buffer{}, []string{"-source-manifest", sourcePath, packetPath}); e == nil {
		t.Fatal("accepted stale frozen manifest")
	}
	if e = os.WriteFile(sourcePath, sourceRaw, 0600); e != nil {
		t.Fatal(e)
	}
	for _, bad := range [][]byte{append(append([]byte{}, raw...), []byte("{}")...), []byte(`{"schema":"gomap-r1-row-v1","unexpected":1}`), []byte(`{"broken":`)} {
		if e = os.WriteFile(packetPath, bad, 0600); e != nil {
			t.Fatal(e)
		}
		if e = validateR1Command(&bytes.Buffer{}, []string{"-source-manifest", sourcePath, packetPath}); e == nil {
			t.Fatal("accepted malformed/trailing/unknown JSON")
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*r1Packet)
	}{
		{"missing setup denominator", func(p *r1Packet) { p.Cells[0].Setup.Operations = 2 }},
		{"wrong full batch denominator", func(p *r1Packet) { p.Cells[0].Phases[3].Rows++ }},
		{"fixture mismatch", func(p *r1Packet) { p.FixtureSHA256 = strings.Repeat("d", 64) }},
		{"ID only phase", func(p *r1Packet) { p.Cells[0].Phases[1].Rows = 0 }},
		{"durability mismatch", func(p *r1Packet) { p.Cells[0].Ack = "process_visible_no_fsync" }},
		{"missing raw cell", func(p *r1Packet) { p.Cells = p.Cells[:len(p.Cells)-1] }},
		{"duplicate raw cell", func(p *r1Packet) { p.Cells[1] = p.Cells[0] }},
		{"missing phase", func(p *r1Packet) { p.Cells[0].Phases = p.Cells[0].Phases[1:] }},
		{"malformed latency", func(p *r1Packet) { p.Cells[0].Phases[0].P95NS = -1 }},
		{"unverified oracle", func(p *r1Packet) { p.Cells[0].OracleVerified = false }},
		{"typed retained-only fallback", func(p *r1Packet) { p.Cells[3].Phases[2].Counters.FieldsReconstructed = 0 }},
		{"unsupported skipped read", func(p *r1Packet) { p.Cells[0].Phases[1].Skipped = "unsupported" }},
		{"dirty retained label", func(p *r1Packet) { p.Config.Qualification = "retained" }},
		{"runtime digest corruption", func(p *r1Packet) { p.Source.RuntimeBlobs["go.mod"] = strings.Repeat("d", 40) }},
		{"wrong storage boundary", func(p *r1Packet) { p.Cells[0].StorageBoundary = "after_unsupported_upsert" }},
		{"source missing", func(p *r1Packet) { p.Source.RuntimeSHA256 = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(p)
			var bad r1Packet
			if e := json.Unmarshal(raw, &bad); e != nil {
				t.Fatal(e)
			}
			tc.mutate(&bad)
			if e := validateR1Packet(bad); e == nil {
				t.Fatal("accepted invalid packet")
			}
		})
	}
}
func TestR1RelaxedSettledRehearsal(t *testing.T) {
	for _, state := range []string{"flushed", "checkpointed"} {
		t.Run(state, func(t *testing.T) {
			c := r1Config{Documents: 16, Batch: 4, Operations: 1, Repetitions: 1, Durability: "relaxed", State: state, Engines: r1Engines, Qualification: "rehearsal"}
			p, e := runR1(c, r1TestSource())
			if e != nil {
				t.Fatal(e)
			}
			if e = validateR1Packet(p); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestR1FixtureOwnershipAndNullMissing(t *testing.T) {
	rows := r1Fixture(16)
	ids, retained, cols, e := r1TypedInput(rows)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(ids, r1IDs(rows)) {
		t.Fatal("ID mismatch")
	}
	for i, raw := range retained {
		var obj map[string]any
		if e = json.Unmarshal(raw, &obj); e != nil {
			t.Fatal(e)
		}
		for _, col := range cols {
			if _, exists := obj[col.Name]; exists {
				t.Fatal("duplicate typed authority")
			}
		}
		_, optional := obj["optional"]
		if optional != (i%2 == 0) || (optional && obj["optional"] != nil) {
			t.Fatal("null/missing residual changed")
		}
	}
}
func TestR1RejectConfiguration(t *testing.T) {
	for _, args := range [][]string{{"-durability", "NORMAL"}, {"-engines", "json,json"}, {"-engines", "IDs"}, {"-read-state", "cold"}, {"-batch-size", "0"}, {"-qualification", "qualified"}} {
		if _, e := parseR1Config(args); e == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestR1CompleteOracleAndGauge(t *testing.T) {
	expected := r1Fixture(2)
	for _, row := range expected {
		raw, e := json.Marshal(row)
		if e != nil {
			t.Fatal(e)
		}
		if e = r1VerifyDocument(raw, row); e != nil {
			t.Fatal(e)
		}
		var partial map[string]any
		_ = json.Unmarshal(raw, &partial)
		delete(partial, "bio")
		bad, _ := json.Marshal(partial)
		if r1VerifyDocument(bad, row) == nil {
			t.Fatal("accepted partial full row")
		}
	}
	var counters collections.DocumentMaterializationStats
	r1AddCounters(&counters, collections.DocumentMaterializationStats{AssetActiveHandles: 3, FieldsReconstructed: 2})
	r1AddCounters(&counters, collections.DocumentMaterializationStats{AssetActiveHandles: 2, FieldsReconstructed: 4})
	if counters.AssetActiveHandles != 3 || counters.FieldsReconstructed != 6 {
		t.Fatal("gauge/counter attribution mismatch", counters)
	}
}
