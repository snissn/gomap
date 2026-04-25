package valuelog

import "testing"

func TestNormalizeAutotuneOptions_DefaultDictCandidates(t *testing.T) {
	opts := NormalizeAutotuneOptions(AutotuneOptions{}, true)
	wantHistory := []int{64 << 10, 96 << 10, 128 << 10, 192 << 10}
	wantDict := []int{40 << 10, 64 << 10, 96 << 10, 128 << 10}
	if len(opts.CandidateHistoryBytes) != len(wantHistory) {
		t.Fatalf("history len=%d want=%d", len(opts.CandidateHistoryBytes), len(wantHistory))
	}
	for i := range wantHistory {
		if opts.CandidateHistoryBytes[i] != wantHistory[i] {
			t.Fatalf("history[%d]=%d want=%d", i, opts.CandidateHistoryBytes[i], wantHistory[i])
		}
	}
	if len(opts.CandidateDictBytes) != len(wantDict) {
		t.Fatalf("dict len=%d want=%d", len(opts.CandidateDictBytes), len(wantDict))
	}
	for i := range wantDict {
		if opts.CandidateDictBytes[i] != wantDict[i] {
			t.Fatalf("dict[%d]=%d want=%d", i, opts.CandidateDictBytes[i], wantDict[i])
		}
	}
}

func TestNormalizeAutotuneOptions_AggressiveDefaultDictCandidates(t *testing.T) {
	opts := NormalizeAutotuneOptions(AutotuneOptions{Mode: AutotuneAggressive}, true)
	wantHistory := []int{64 << 10, 96 << 10, 128 << 10, 192 << 10, 256 << 10, 512 << 10}
	wantDict := []int{40 << 10, 64 << 10, 96 << 10, 128 << 10, 192 << 10, 256 << 10}
	if len(opts.CandidateHistoryBytes) != len(wantHistory) {
		t.Fatalf("history len=%d want=%d", len(opts.CandidateHistoryBytes), len(wantHistory))
	}
	for i := range wantHistory {
		if opts.CandidateHistoryBytes[i] != wantHistory[i] {
			t.Fatalf("history[%d]=%d want=%d", i, opts.CandidateHistoryBytes[i], wantHistory[i])
		}
	}
	if len(opts.CandidateDictBytes) != len(wantDict) {
		t.Fatalf("dict len=%d want=%d", len(opts.CandidateDictBytes), len(wantDict))
	}
	for i := range wantDict {
		if opts.CandidateDictBytes[i] != wantDict[i] {
			t.Fatalf("dict[%d]=%d want=%d", i, opts.CandidateDictBytes[i], wantDict[i])
		}
	}
}
