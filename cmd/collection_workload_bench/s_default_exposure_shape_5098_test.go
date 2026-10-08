package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Source-only exposure candidate. This helper is separate from the unchanged
// latency fixture and is not invoked by discovery or fixed-work campaigns.
// Payload size, entropy, compression, resource fit and all-producer rollover
// remain subject to ROOT's encoding/resource preflight and runtime gates.
const (
	r1SExposurePopulation5098    = 4096
	r1SExposureHot5098           = 64
	r1SExposureBatchSize5098     = 32
	r1SExposureOptionalBytes5098 = 131072
	r1SExposureAlphabet5098      = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)

// SHA256 counter expansion gives deterministic printable candidate bytes, with
// no quote, backslash or control characters. This is not a compression proof.
func r1SExposurePayload5098(row int, revision int64, call int) string {
	seed := sha256.Sum256([]byte(fmt.Sprintf("s5098-exposure-v1/row%d/revision%d/call%d", row, revision, call)))
	var input [40]byte
	copy(input[:32], seed[:])
	out := make([]byte, r1SExposureOptionalBytes5098)
	for offset := 0; offset < len(out); offset += sha256.Size {
		binary.LittleEndian.PutUint64(input[32:], uint64(offset/sha256.Size))
		block := sha256.Sum256(input[:])
		for i, b := range block {
			out[offset+i] = r1SExposureAlphabet5098[b&63]
		}
	}
	return string(out)
}

func r1SExposureFixture5098() []r1Document {
	rows := r1Fixture(r1SExposurePopulation5098)
	for i := range rows {
		rows[i].Optional = nil
		if i < r1SExposureHot5098 {
			rows[i].Optional = r1SExposurePayload5098(i, rows[i].Revision, 0)
		}
	}
	return rows
}

// Even calls alternate the two contiguous hot batches. Across 128 odd calls,
// round+128*j covers each of the 4096 IDs exactly once, without assuming any
// physical worker assignment or certifying that a worker receives equal bytes.
func r1SExposureIndices5098(call int) ([r1SExposureBatchSize5098]int, error) {
	var indices [r1SExposureBatchSize5098]int
	if call < 0 {
		return indices, fmt.Errorf("negative exposure call")
	}
	round := call / 2
	for j := range indices {
		if call%2 == 0 {
			indices[j] = (round%2)*r1SExposureBatchSize5098 + j
		} else {
			indices[j] = round%128 + 128*j
		}
	}
	return indices, nil
}

// The caller owns applying the returned rows through the existing r1Tree
// replace path and updating its expected rows/index histories. No input row or
// held alias is mutated here; immutable old Optional strings stay complete.
func r1SExposureBatch5098(rows []r1Document, call int) ([r1SExposureBatchSize5098]int, []r1Document, error) {
	indices, err := r1SExposureIndices5098(call)
	if err != nil {
		return indices, nil, err
	}
	if len(rows) != r1SExposurePopulation5098 {
		return indices, nil, fmt.Errorf("exposure requires exactly 4096 rows")
	}
	for _, index := range indices {
		if rows[index].Revision == int64(1<<63-1) {
			return indices, nil, fmt.Errorf("exposure revision overflow at row %d", index)
		}
	}
	update := make([]r1Document, len(indices))
	for j, index := range indices {
		update[j] = rows[index]
		update[j].Revision++
		update[j].Email = fmt.Sprintf("s5098-exposure-c%d-r%d@example.test", call%8, index)
		update[j].City = fmt.Sprintf("city-%02d", (index+call%8+1)%8)
		update[j].Bio = strings.Repeat(fmt.Sprintf("%02d", call%100), 48)
		update[j].Optional = nil
		if index < r1SExposureHot5098 {
			update[j].Optional = r1SExposurePayload5098(index, update[j].Revision, call)
		}
	}
	return indices, update, nil
}

func TestR1SExposureFixtureShape5098(t *testing.T) {
	original := r1Fixture(4096)
	const originalHash = "fd5d377dceaa2e53dd37bb3c2b56504f202e411988e17843ed3be1f257ee205c"
	if got := r1FixtureHash(original); got != originalHash {
		t.Fatalf("original latency fixture changed: %s", got)
	}
	rows := r1SExposureFixture5098()
	if len(rows) != 4096 {
		t.Fatal("population changed")
	}
	for i, row := range rows {
		withoutOptional := row
		withoutOptional.Optional = original[i].Optional
		if !reflect.DeepEqual(withoutOptional, original[i]) || len(row.Bio) != 96 {
			t.Fatalf("original row fields changed at %d", i)
		}
		if i < 64 {
			payload, ok := row.Optional.(string)
			if !ok || len(payload) != 131072 {
				t.Fatalf("hot Optional shape at %d", i)
			}
		} else if row.Optional != nil {
			t.Fatalf("nonhot Optional at %d", i)
		}
	}
	if r1FixtureHash(original) != originalHash || r1FixtureHash(r1Fixture(4096)) != originalHash {
		t.Fatal("original latency fixture mutated")
	}
}

func TestR1SExposureScheduleCoverage5098(t *testing.T) {
	var broad [4096]int
	var hot [64]int
	for call := 0; call < 256; call++ {
		indices, err := r1SExposureIndices5098(call)
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[int]bool, 32)
		for j, index := range indices {
			if index < 0 || index >= 4096 || seen[index] {
				t.Fatalf("call %d invalid/repeated row %d", call, index)
			}
			seen[index] = true
			if call%2 == 0 {
				if index != ((call/2)%2)*32+j {
					t.Fatal("hot batch order changed")
				}
				hot[index]++
			} else {
				if index != (call/2)%128+128*j {
					t.Fatal("broad batch order changed")
				}
				broad[index]++
			}
		}
	}
	for i, count := range broad {
		if count != 1 {
			t.Fatalf("broad row %d seen %d times", i, count)
		}
	}
	for i, count := range hot {
		if count != 64 {
			t.Fatalf("hot row %d seen %d times", i, count)
		}
	}
}

func TestR1SExposurePayloadDeterminismAndHeldAlias5098(t *testing.T) {
	old := r1SExposurePayload5098(0, 0, 0)
	if len(old) != 131072 || old != r1SExposurePayload5098(0, 0, 0) {
		t.Fatal("payload size/determinism")
	}
	sum := sha256.Sum256([]byte(old))
	if hex.EncodeToString(sum[:]) != "3038935b9e7030e9b8c601eb753c682595051e449bb5c115e96f54836f7d92f5" {
		t.Fatal("payload algorithm/seed/alphabet changed")
	}
	for _, b := range []byte(old) {
		if !strings.ContainsRune(r1SExposureAlphabet5098, rune(b)) {
			t.Fatal("payload includes JSON escape/control byte")
		}
	}
	for _, next := range []string{r1SExposurePayload5098(1, 0, 0), r1SExposurePayload5098(0, 1, 0), r1SExposurePayload5098(0, 0, 1)} {
		if next == old {
			t.Fatal("row/revision/call did not change payload")
		}
	}
	rows := r1SExposureFixture5098()
	held := rows[0]
	before := r1FixtureHash(rows)
	_, update, err := r1SExposureBatch5098(rows, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r1FixtureHash(rows) != before || held.Optional != rows[0].Optional || held.Optional == update[0].Optional {
		t.Fatal("held/input alias mutated or replacement payload unchanged")
	}
}

func TestR1SExposureTypedRetainedJSONAndOracle5098(t *testing.T) {
	rows := r1SExposureFixture5098()
	for _, call := range []int{0, 1, 129} {
		indices, update, err := r1SExposureBatch5098(rows, call)
		if err != nil {
			t.Fatal(err)
		}
		ids, retained, columns, err := r1TypedInput(update)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != 32 || len(retained) != 32 || len(columns) != 4 {
			t.Fatal("typed batch shape changed")
		}
		for j, row := range update {
			if string(ids[j]) != rows[indices[j]].ID || row.Revision != rows[indices[j]].Revision+1 || len(row.Bio) != 96 {
				t.Fatal("row identity/revision/Bio changed")
			}
			var obj map[string]any
			if err := json.Unmarshal(retained[j], &obj); err != nil {
				t.Fatal(err)
			}
			for k, name := range []string{"email", "city", "name", "bio"} {
				if _, exists := obj[name]; exists || columns[k].Name != name || len(columns[k].Strings) != 32 {
					t.Fatalf("typed column %s retained or incomplete", name)
				}
				obj[name] = columns[k].Strings[j]
			}
			if indices[j] < 64 {
				if obj["optional"] != row.Optional {
					t.Fatal("hot payload lost in retained JSON")
				}
			} else if _, exists := obj["optional"]; exists || row.Optional != nil {
				t.Fatal("nonhot retained Optional present")
			}
			raw, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			if err := r1VerifyDocument(raw, row); err != nil {
				t.Fatal(err)
			}
			obj["optional"] = "corrupted"
			raw, err = json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			if r1VerifyDocument(raw, row) == nil {
				t.Fatal("full-row oracle ignored Optional corruption")
			}
		}
	}
}

func TestR1SExposureInvalidInputNoMutation5098(t *testing.T) {
	rows := r1Fixture(4096)
	before := r1FixtureHash(rows)
	if _, update, err := r1SExposureBatch5098(rows, -1); err == nil || update != nil {
		t.Fatal("negative call accepted")
	}
	if _, update, err := r1SExposureBatch5098(rows[:4095], 0); err == nil || update != nil {
		t.Fatal("wrong population accepted")
	}
	if r1FixtureHash(rows) != before {
		t.Fatal("refusal mutated input")
	}
	rows[0].Revision = int64(1<<63 - 1)
	before = r1FixtureHash(rows)
	if _, update, err := r1SExposureBatch5098(rows, 0); err == nil || update != nil || r1FixtureHash(rows) != before {
		t.Fatal("revision overflow accepted or mutated input")
	}
}
