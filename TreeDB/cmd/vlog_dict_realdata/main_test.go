package main

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func TestMode3ShowsDictActivity(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	// Generate test data (enough for training + eval)
	sampleCount := 200
	values := generateSyntheticData(sampleCount)

	// Mock the train samples count for testing
	oldTrain := *trainSamples
	*trainSamples = 100
	defer func() { *trainSamples = oldTrain }()

	// Run mode3 benchmark
	if err := runBenchmark("mode3", true, values); err != nil {
		t.Fatalf("mode3 benchmark failed: %v", err)
	}

	output := buf.String()

	// Verify dict training happens before steady
	if !strings.Contains(output, "trained dict (BEFORE steady") {
		t.Error("mode3: dict training should occur BEFORE steady state")
	}

	// Verify headline shows non-zero dict_id
	if !strings.Contains(output, "dict_id=1") {
		t.Error("mode3: headline should show dict_id=1")
	}

	// Verify headline shows non-zero attempted_frac
	if strings.Contains(output, "attempted_frac=0.000000") {
		t.Error("mode3: headline should show attempted_frac > 0")
	}
}

func TestMode4ShowsDictActivity(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	// Generate test data (enough for training + eval)
	sampleCount := 200
	values := generateSyntheticData(sampleCount)

	// Mock the train samples count for testing
	oldTrain := *trainSamples
	*trainSamples = 100
	defer func() { *trainSamples = oldTrain }()

	// Run mode4 benchmark
	if err := runBenchmark("mode4", true, values); err != nil {
		t.Fatalf("mode4 benchmark failed: %v", err)
	}

	output := buf.String()

	// Verify dict training happens before steady
	if !strings.Contains(output, "trained dict (BEFORE steady") {
		t.Error("mode4: dict training should occur BEFORE steady state")
	}

	// Verify headline shows non-zero dict_id
	if !strings.Contains(output, "dict_id=1") {
		t.Error("mode4: headline should show dict_id=1")
	}

	// Verify headline shows non-zero attempted_frac
	if strings.Contains(output, "attempted_frac=0.000000") {
		t.Error("mode4: headline should show attempted_frac > 0")
	}

	// Verify dict training does NOT happen after steady (the old bug)
	if strings.Contains(output, "training occurs AFTER steady") {
		t.Error("mode4: dict training should NOT occur after steady (old bug)")
	}
}

func TestMode4MatchesMode3Timing(t *testing.T) {
	// This test verifies that mode4 and mode3 both train dictionaries
	// at the same time (before steady state)

	sampleCount := 200
	values := generateSyntheticData(sampleCount)

	// Mock the train samples count for testing
	oldTrain := *trainSamples
	*trainSamples = 100
	defer func() { *trainSamples = oldTrain }()

	// Capture mode3 output
	var buf3 bytes.Buffer
	log.SetOutput(&buf3)
	if err := runBenchmark("mode3", true, values); err != nil {
		t.Fatalf("mode3 failed: %v", err)
	}

	// Capture mode4 output
	var buf4 bytes.Buffer
	log.SetOutput(&buf4)
	if err := runBenchmark("mode4", true, values); err != nil {
		t.Fatalf("mode4 failed: %v", err)
	}

	log.SetOutput(os.Stderr)

	out3 := buf3.String()
	out4 := buf4.String()

	// Both should train dict before steady
	if !strings.Contains(out3, "trained dict (BEFORE steady") {
		t.Error("mode3 should train dict before steady")
	}
	if !strings.Contains(out4, "trained dict (BEFORE steady") {
		t.Error("mode4 should train dict before steady")
	}

	// Both should show dict_id=1 in headline
	if !strings.Contains(out3, "dict_id=1") {
		t.Error("mode3 should show dict_id=1")
	}
	if !strings.Contains(out4, "dict_id=1") {
		t.Error("mode4 should show dict_id=1")
	}

	// Both should show attempted_frac > 0
	if strings.Contains(out3, "attempted_frac=0.000000") {
		t.Error("mode3 should show attempted_frac > 0")
	}
	if strings.Contains(out4, "attempted_frac=0.000000") {
		t.Error("mode4 should show attempted_frac > 0")
	}
}
