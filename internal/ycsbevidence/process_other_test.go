//go:build !linux

package ycsbevidence

import (
	"reflect"
	"testing"
)

func TestNonLinuxObservationRejectsWithoutFabricatingEvidence(t *testing.T) {
	observer := &Observer{stats: func() map[string]string {
		t.Fatal("unsupported process evidence must fail before owner inspection")
		return nil
	}}
	observation, err := observer.observe()
	if err == nil || err.Error() != "YCSB process evidence requires Linux procfs" {
		t.Fatalf("expected unsupported process evidence error, got %v", err)
	}
	if !reflect.DeepEqual(observation, Observation{}) {
		t.Fatalf("unsupported observation fabricated evidence: %+v", observation)
	}
}
