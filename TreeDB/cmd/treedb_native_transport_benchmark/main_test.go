package main

import "testing"

func TestPercentile(t *testing.T) {
	values := []float64{4, 1, 3, 2}
	if got := percentile(values, 0.50); got != 2 {
		t.Fatalf("p50=%v", got)
	}
	if got := percentile(values, 0.99); got != 4 {
		t.Fatalf("p99=%v", got)
	}
}
