package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOrderForOrdinal(t *testing.T) {
	if got, want := orderForOrdinal(0), [4]string{"A1", "B1", "B2", "A2"}; got != want {
		t.Fatalf("even order = %v, want %v", got, want)
	}
	if got, want := orderForOrdinal(1), [4]string{"A2", "B2", "B1", "A1"}; got != want {
		t.Fatalf("odd order = %v, want %v", got, want)
	}
	if got, want := orderForOrdinal(382), orderForOrdinal(0); got != want {
		t.Fatalf("later even order = %v, want %v", got, want)
	}
	if got, want := orderForOrdinal(383), orderForOrdinal(1); got != want {
		t.Fatalf("later odd order = %v, want %v", got, want)
	}
}

func TestValidateResponses(t *testing.T) {
	row := json.RawMessage(`{"cell":{"route":"hybrid","projection":"chunks","filter":"none","collapse":"disabled","surface":"direct_collection","embedding":"fixture","vector_route":"declared_column_graph_exact","clients":1},"status":"supported","qps_mean":12.5}`)
	responses := func() []namedResponse {
		result := make([]namedResponse, len(legOrder))
		for i, leg := range legOrder {
			result[i] = namedResponse{Leg: leg, Response: workerResponse{Ordinal: 7, Row: append(json.RawMessage(nil), row...)}}
		}
		return result
	}

	t.Run("matching", func(t *testing.T) {
		if err := validateResponses(7, responses()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("mismatched cell", func(t *testing.T) {
		got := responses()
		got[2].Response.Row = json.RawMessage(strings.Replace(string(row), `"route":"hybrid"`, `"route":"text_only"`, 1))
		if err := validateResponses(7, got); err == nil || !strings.Contains(err.Error(), "cell identity") {
			t.Fatalf("error = %v, want cell identity mismatch", err)
		}
	})
	t.Run("capability status transition", func(t *testing.T) {
		got := responses()
		got[3].Response.Row = json.RawMessage(strings.Replace(string(row), `"status":"supported"`, `"status":"unsupported"`, 1))
		if err := validateResponses(7, got); err != nil {
			t.Fatalf("status transition rejected: %v", err)
		}
	})
	t.Run("worker error", func(t *testing.T) {
		got := responses()
		got[1].Response.Row = nil
		got[1].Response.Error = "benchmark failed"
		if err := validateResponses(7, got); err == nil || !strings.Contains(err.Error(), "B1: worker error: benchmark failed") {
			t.Fatalf("error = %v, want worker error", err)
		}
	})
}

func TestHashFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "benchmark")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Fatalf("hash = %s, want %s", got, want)
	}
}
