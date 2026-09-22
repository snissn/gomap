package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestBatchSetOps_StreamEligible_PreservesSorted(t *testing.T) {
	b := &Batch{streamEligible: true}
	ops := []batch.Entry{
		{Type: batch.OpPut, Key: []byte("a"), Value: []byte("1")},
		{Type: batch.OpPut, Key: []byte("b"), Value: []byte("2")},
		{Type: batch.OpDelete, Key: []byte("c")},
	}
	if err := b.SetOps(ops); err != nil {
		t.Fatalf("SetOps: %v", err)
	}
	if !b.streamEligible {
		t.Fatalf("streamEligible=false for strictly increasing SetOps")
	}
	if got := string(b.firstKey); got != "a" {
		t.Fatalf("firstKey=%q, want %q", got, "a")
	}
	if got := string(b.lastKey); got != "c" {
		t.Fatalf("lastKey=%q, want %q", got, "c")
	}
}

func TestBatchSetOps_StreamEligible_DisablesOnNonMonotonic(t *testing.T) {
	b := &Batch{streamEligible: true}
	ops := []batch.Entry{
		{Type: batch.OpPut, Key: []byte("b"), Value: []byte("1")},
		{Type: batch.OpPut, Key: []byte("a"), Value: []byte("2")},
	}
	if err := b.SetOps(ops); err != nil {
		t.Fatalf("SetOps: %v", err)
	}
	if b.streamEligible {
		t.Fatalf("streamEligible=true for non-monotonic SetOps")
	}
}

func TestBatchSetOps_StreamEligible_TracksAcrossCalls(t *testing.T) {
	b := &Batch{streamEligible: true}
	if err := b.SetOps([]batch.Entry{
		{Type: batch.OpPut, Key: []byte("a"), Value: []byte("1")},
		{Type: batch.OpPut, Key: []byte("b"), Value: []byte("2")},
	}); err != nil {
		t.Fatalf("SetOps (first): %v", err)
	}
	if !b.streamEligible {
		t.Fatalf("streamEligible=false after sorted first SetOps")
	}
	if err := b.SetOps([]batch.Entry{
		{Type: batch.OpPut, Key: []byte("b"), Value: []byte("3")},
		{Type: batch.OpPut, Key: []byte("c"), Value: []byte("4")},
	}); err != nil {
		t.Fatalf("SetOps (second): %v", err)
	}
	if b.streamEligible {
		t.Fatalf("streamEligible=true after second SetOps starts at previous max key")
	}
}
