package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestGenerateFixtureFreshQueryOrdinalsV1(t *testing.T) {
	dir := t.TempDir()
	if err := run([]string{"generate-fixture", "-out", dir, "-vectors", "16", "-queries", "4", "-dimensions", "8", "-seed", "4016", "-generator", qualificationEmbeddingGeneratorV1, "-query-ordinal-offset", "1000"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	fixture, err := loadFixture(dir)
	if err != nil || fixture.QueryOrdinalOffset != 1000 {
		t.Fatalf("fresh manifest=%+v err=%v", fixture, err)
	}
	vectors, queries := fixtureData(fixture)
	if fixtureChecksumFromData(vectors, queries) != fixture.Checksum {
		t.Fatal("fresh checksum does not bind generated queries")
	}
}

func TestFixtureQueryOrdinalOffsetCompatibilityV1(t *testing.T) {
	for _, generator := range []string{fixtureGenerator, qualificationSyntheticGeneratorV1, qualificationEmbeddingGeneratorV1} {
		t.Run(generator, func(t *testing.T) {
			generate := func(offset string) (fixtureManifest, []byte) {
				t.Helper()
				dir := t.TempDir()
				args := []string{"generate-fixture", "-out", dir, "-vectors", "16", "-queries", "4", "-dimensions", "8", "-seed", "4016", "-generator", generator}
				if offset != "" {
					args = append(args, "-query-ordinal-offset", offset)
				}
				if err := run(args, io.Discard); err != nil {
					t.Fatal(err)
				}
				fixture, err := loadFixture(dir)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(filepath.Join(dir, "fixture_manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				return fixture, raw
			}
			old, oldRaw := generate("")
			zero, zeroRaw := generate("0")
			if old != zero || !bytes.Equal(oldRaw, zeroRaw) || bytes.Contains(zeroRaw, []byte("query_ordinal_offset")) {
				t.Fatal("explicit zero changed legacy manifest bytes")
			}
			// Pin the pre-offset field order and bytes, not only two new-code outputs.
			wantJSON := fmt.Sprintf("{\n  \"schema_version\": 1,\n  \"fixture\": \"deterministic_16\",\n  \"generator\": %q,\n  \"arithmetic\": %q,\n  \"vectors\": 16,\n  \"queries\": 4,\n  \"dimensions\": 8,\n  \"metric\": \"cosine\",\n  \"seed\": 4016,\n  \"checksum\": %q\n}\n", generator, fixtureArithmetic, old.Checksum)
			if string(oldRaw) != wantJSON {
				t.Fatalf("offset-zero manifest bytes changed: %s", oldRaw)
			}
			var oldVectors, oldQueries [][]float64
			if generator == fixtureGenerator {
				oldVectors, oldQueries = deterministicFixture(old)
			} else {
				oldVectors = qualificationVectorsV1(old, 0x9e3779b97f4a7c15)
				oldQueries = contiguousFloat64Matrix(old.Queries, old.Dimensions)
				for i := range oldQueries {
					qualificationVectorV1(oldQueries[i], old, uint64(i), 0xd1b54a32d192ed03)
				}
			}
			if fixtureChecksumFromData(oldVectors, oldQueries) != old.Checksum || m8TruthCacheIdentityV1(old, 10) != m8TruthCacheIdentityV1(zero, 10) {
				t.Fatal("offset zero changed checksum or cache identity")
			}
			if generator == fixtureGenerator {
				return
			}
			fresh, _ := generate("1000")
			freshVectors, freshQueries := fixtureData(fresh)
			if !reflect.DeepEqual(oldVectors, freshVectors) {
				t.Fatal("query offset changed corpus")
			}
			for i, query := range freshQueries {
				want := make([]float64, fresh.Dimensions)
				qualificationVectorV1(want, old, uint64(1000+i), 0xd1b54a32d192ed03)
				if !reflect.DeepEqual(query, want) {
					t.Fatalf("query %d did not use its absolute ordinal", i)
				}
				for _, prior := range oldQueries {
					if reflect.DeepEqual(query, prior) {
						t.Fatalf("fresh query %d repeats an old query", i)
					}
				}
			}
			if fresh.Checksum == old.Checksum || m8TruthCacheIdentityV1(fresh, 10) == m8TruthCacheIdentityV1(old, 10) {
				t.Fatal("fresh queries retained old checksum/cache identity")
			}
		})
	}
}

func TestFixtureQueryOrdinalOffsetBoundsV1(t *testing.T) {
	for _, tc := range []struct {
		name, generator, offset, want string
	}{
		{"negative", qualificationEmbeddingGeneratorV1, "-1", "query ordinal range"},
		{"overflow", qualificationEmbeddingGeneratorV1, strconv.FormatInt(math.MaxInt64-3, 10), "query ordinal range"},
		{"legacy", fixtureGenerator, "1", "legacy fixture generator"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "must-not-exist")
			err := run([]string{"generate-fixture", "-out", out, "-vectors", "16", "-queries", "4", "-dimensions", "8", "-generator", tc.generator, "-query-ordinal-offset", tc.offset}, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected rejection: %v", err)
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("invalid query range created output")
			}
			offset, _ := strconv.ParseInt(tc.offset, 10, 64)
			fixture := fixtureManifest{SchemaVersion: schemaVersion, Fixture: "bounded", Generator: tc.generator, Arithmetic: fixtureArithmetic, Vectors: 16, Queries: 4, QueryOrdinalOffset: offset, Dimensions: 8, Metric: "cosine", Checksum: strings.Repeat("0", 64)}
			raw, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			var decoded fixtureManifest
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := validateM3FixtureWithCaps(decoded, maxVectors, maxFixtureBytes); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("manifest accepted invalid range: %v", err)
			}
			dataset := t.TempDir()
			if err := os.WriteFile(filepath.Join(dataset, "fixture_manifest.json"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadFixture(dataset); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("manifest decoder accepted invalid range: %v", err)
			}
		})
	}
	fixture := fixtureManifest{Generator: qualificationEmbeddingGeneratorV1, Queries: 4, QueryOrdinalOffset: math.MaxInt64 - 4, Dimensions: 8}
	if err := validateFixtureQueryOrdinalsV1(fixture); err != nil {
		t.Fatal("valid upper boundary rejected", err)
	}
	queries := qualificationQueriesV1(fixture)
	want := make([]float64, fixture.Dimensions)
	qualificationVectorV1(want, fixture, uint64(math.MaxInt64-1), 0xd1b54a32d192ed03)
	if !reflect.DeepEqual(queries[3], want) {
		t.Fatal("upper-bound query ordinal wrapped")
	}
}
