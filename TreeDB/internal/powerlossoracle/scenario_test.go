package powerlossoracle

import "testing"

func completeGeneration(sequence, applied uint64) Generation {
	resources := make([]Resource, 0, len(requiredRootResourceKinds))
	for _, kind := range requiredRootResourceKinds {
		resources = append(resources, Resource{Kind: kind, ID: string(kind), Stable: true, Live: true})
	}
	return Generation{Sequence: sequence, Recoverable: true, Resources: resources, AppliedLSN: applied}
}

func TestValidateChecksEveryRecoverableGenerationClosure(t *testing.T) {
	older := completeGeneration(1, 0)
	older.Resources[0].Stable = false
	scenario := Scenario{
		Cut:                  AfterMetaSync,
		ReopenAttempted:      true,
		LatestSealedSequence: 2,
		Generations:          []Generation{older, completeGeneration(2, 0)},
	}
	if err := RequireViolation(scenario.Validate(), InvariantIncompleteRecoverableRoot); err != nil {
		t.Fatal(err)
	}
}

func TestValidateReplayStartsAfterSelectedAppliedLSN(t *testing.T) {
	selected := completeGeneration(2, 10)
	scenario := Scenario{
		Cut:                  AfterAppliedLSNAdvance,
		ReopenAttempted:      true,
		Generations:          []Generation{selected},
		LatestSealedSequence: 2,
		SelectedSequence:     2,
		OpenedSequence:       2,
		OpenedAppliedLSN:     10,
		CommandFrames: []CommandFrame{
			{LSN: 12, ChecksumValid: true, Applied: true},
		},
	}
	if err := RequireViolation(scenario.Validate(), InvariantCommandReplayHole); err != nil {
		t.Fatal(err)
	}
}

func TestValidateUnappliedFrameBeforeLaterAppliedIsHole(t *testing.T) {
	selected := completeGeneration(2, 10)
	scenario := Scenario{
		Cut:                  AfterAppliedLSNAdvance,
		ReopenAttempted:      true,
		Generations:          []Generation{selected},
		LatestSealedSequence: 2,
		SelectedSequence:     2,
		OpenedSequence:       2,
		OpenedAppliedLSN:     10,
		CommandFrames: []CommandFrame{
			{LSN: 11, ChecksumValid: true},
			{LSN: 12, ChecksumValid: true, Applied: true},
		},
	}
	if err := RequireViolation(scenario.Validate(), InvariantCommandReplayHole); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSkippedCompleteNextFrameIsReplayHole(t *testing.T) {
	selected := completeGeneration(2, 10)
	scenario := Scenario{
		Cut:                  AfterAppliedLSNAdvance,
		ReopenAttempted:      true,
		Generations:          []Generation{selected},
		LatestSealedSequence: 2,
		SelectedSequence:     2,
		OpenedSequence:       2,
		OpenedAppliedLSN:     10,
		CommandFrames: []CommandFrame{
			{LSN: 11, ChecksumValid: true},
		},
	}
	if err := RequireViolation(scenario.Validate(), InvariantCommandReplayHole); err != nil {
		t.Fatal(err)
	}
}

func TestValidateDurableAckMayBeRecoveredFromWAL(t *testing.T) {
	scenario := Scenario{
		Cut:                       AfterMetaSync,
		ReopenAttempted:           true,
		Generations:               []Generation{completeGeneration(4, 4)},
		LatestSealedSequence:      4,
		SelectedSequence:          4,
		OpenedSequence:            4,
		OpenedAppliedLSN:          4,
		Acknowledged:              []Acknowledgement{{Sequence: 5, Durable: true}},
		RecoveredAcknowledgements: []uint64{5},
	}
	if err := scenario.Validate(); err != nil {
		t.Fatalf("WAL-recovered durable ack rejected: %v", err)
	}
}

func TestValidateAllowsPublicOpenToAdvanceBeyondSelectedRootByCommandReplay(t *testing.T) {
	scenario := Scenario{
		Name:                 "command-replay-after-root-selection",
		Cut:                  AfterDependencyFileSync,
		Generations:          []Generation{completeGeneration(1, 1)},
		LatestSealedSequence: 1,
		SelectedSequence:     1,
		OpenedSequence:       3,
		OpenedAppliedLSN:     3,
		ReopenAttempted:      true,
		CommandFrames: []CommandFrame{
			{LSN: 2, ChecksumValid: true, Applied: true},
			{LSN: 3, ChecksumValid: true, Applied: true},
		},
	}
	if err := scenario.Validate(); err != nil {
		t.Fatalf("command replay after root selection should validate: %v", err)
	}
}

func TestValidateAllowsLossOfAppendedUnstableCommandSuffix(t *testing.T) {
	scenario := Scenario{
		Name:                 "unstable-command-suffix",
		Cut:                  AfterDependencyAppend,
		Generations:          []Generation{completeGeneration(1, 1)},
		LatestSealedSequence: 1,
		SelectedSequence:     1,
		OpenedSequence:       2,
		OpenedAppliedLSN:     2,
		ReopenAttempted:      true,
		CommandFrames: []CommandFrame{
			{LSN: 2, ChecksumValid: true, Applied: true},
			{LSN: 3, ChecksumValid: false},
		},
	}
	if err := scenario.Validate(); err != nil {
		t.Fatalf("loss of appended unstable suffix should validate: %v", err)
	}
}

func TestValidateRejectsUnexplainedPublicReplayAdvance(t *testing.T) {
	for _, tt := range []struct {
		name   string
		frames []CommandFrame
	}{
		{name: "no-frames"},
		{name: "partial-frontier", frames: []CommandFrame{{LSN: 2, ChecksumValid: true, Applied: true}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scenario := Scenario{
				Name:                 "unexplained-command-replay-advance",
				Cut:                  AfterDependencyFileSync,
				Generations:          []Generation{completeGeneration(1, 1)},
				LatestSealedSequence: 1,
				SelectedSequence:     1,
				OpenedSequence:       3,
				OpenedAppliedLSN:     3,
				ReopenAttempted:      true,
				CommandFrames:        tt.frames,
			}
			if err := RequireViolation(scenario.Validate(), InvariantCommandReplayHole); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateRejectsPublicOpenBehindSelectedRoot(t *testing.T) {
	for _, tt := range []struct {
		name             string
		openedSequence   uint64
		openedAppliedLSN uint64
	}{
		{name: "commit-sequence", openedSequence: 2, openedAppliedLSN: 3},
		{name: "applied-lsn", openedSequence: 3, openedAppliedLSN: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scenario := Scenario{
				Name:                 "public-open-behind-selected-root",
				Cut:                  AfterDependencyFileSync,
				Generations:          []Generation{completeGeneration(3, 3)},
				LatestSealedSequence: 3,
				SelectedSequence:     3,
				OpenedSequence:       tt.openedSequence,
				OpenedAppliedLSN:     tt.openedAppliedLSN,
				ReopenAttempted:      true,
			}
			if err := RequireViolation(scenario.Validate(), InvariantSelectedRootInvalid); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateDurableAckWithoutRootUsesRecoveredAcknowledgements(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		scenario := Scenario{
			Cut:             AfterMetaSync,
			ReopenAttempted: true,
			Acknowledged:    []Acknowledgement{{Sequence: 5, Durable: true}},
		}
		if err := RequireViolation(scenario.Validate(), InvariantDurableAckLost); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("recovered-by-wal", func(t *testing.T) {
		scenario := Scenario{
			Cut:                       AfterMetaSync,
			ReopenAttempted:           true,
			Acknowledged:              []Acknowledgement{{Sequence: 5, Durable: true}},
			RecoveredAcknowledgements: []uint64{5},
		}
		if err := scenario.Validate(); err != nil {
			t.Fatalf("WAL-recovered durable ack without root rejected: %v", err)
		}
	})
}

func TestValidateDurableAckUsesRecoveredRootAppliedLSN(t *testing.T) {
	t.Run("commit generation does not cover later acknowledgement", func(t *testing.T) {
		scenario := Scenario{
			Cut:                  AfterMetaSync,
			ReopenAttempted:      true,
			Generations:          []Generation{completeGeneration(10, 4)},
			LatestSealedSequence: 10,
			SelectedSequence:     10,
			OpenedSequence:       10,
			OpenedAppliedLSN:     4,
			Acknowledged:         []Acknowledgement{{Sequence: 5, Durable: true}},
		}
		if err := RequireViolation(scenario.Validate(), InvariantDurableAckLost); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("applied lsn covers acknowledgement beyond commit generation", func(t *testing.T) {
		scenario := Scenario{
			Cut:                  AfterMetaSync,
			ReopenAttempted:      true,
			Generations:          []Generation{completeGeneration(1, 5)},
			LatestSealedSequence: 1,
			SelectedSequence:     1,
			OpenedSequence:       1,
			OpenedAppliedLSN:     5,
			Acknowledged:         []Acknowledgement{{Sequence: 5, Durable: true}},
		}
		if err := scenario.Validate(); err != nil {
			t.Fatalf("AppliedLSN-covered durable ack rejected: %v", err)
		}
	})
}

func TestValidateRecoveredDurableAckAfterGapIsNonSuffixLoss(t *testing.T) {
	scenario := Scenario{
		Cut:                       AfterMetaSync,
		ReopenAttempted:           true,
		Generations:               []Generation{completeGeneration(2, 0)},
		LatestSealedSequence:      2,
		SelectedSequence:          2,
		OpenedSequence:            2,
		Acknowledged:              []Acknowledgement{{Sequence: 3}, {Sequence: 4, Durable: true}},
		RecoveredAcknowledgements: []uint64{4},
	}
	if err := RequireViolation(scenario.Validate(), InvariantRelaxedNonSuffixLoss); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSelectedRootMustBeNewestCompleteCandidate(t *testing.T) {
	scenario := Scenario{
		Cut:                  AfterMetaSync,
		ReopenAttempted:      true,
		Generations:          []Generation{completeGeneration(5, 0), completeGeneration(6, 0)},
		LatestSealedSequence: 6,
		SelectedSequence:     5,
		OpenedSequence:       5,
	}
	if err := RequireViolation(scenario.Validate(), InvariantSelectedRootInvalid); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsPublicOpenRejectionWhenCompleteCandidateExists(t *testing.T) {
	scenario := Scenario{
		Cut:                  AfterMetaSync,
		ReopenAttempted:      true,
		ReopenRejected:       true,
		Generations:          []Generation{completeGeneration(5, 0)},
		LatestSealedSequence: 5,
	}
	if err := RequireViolation(scenario.Validate(), InvariantSelectedRootInvalid); err != nil {
		t.Fatal(err)
	}
}

func TestValidateKeyStateDiagnosisIsDeterministic(t *testing.T) {
	scenario := Scenario{
		Cut:                  AfterMetaSync,
		ReopenAttempted:      true,
		Generations:          []Generation{completeGeneration(5, 0)},
		LatestSealedSequence: 5,
		SelectedSequence:     5,
		OpenedSequence:       5,
		ExpectedKeyValuesByPrefix: map[string]map[string]string{
			"z/": {},
			"a/": {},
		},
		ObservedKeyValuesByPrefix: map[string]map[string]string{
			"z/": {"z/2": "two", "z/1": "one"},
			"a/": {"a/2": "two", "a/1": "one"},
		},
	}
	err := scenario.Validate()
	if err == nil || err.Error() != `key-state-mismatch: generation=5 prefix="a/" unexpected-key="a/1" cut=after-meta-sync` {
		t.Fatalf("diagnosis=%v", err)
	}
}

func TestValidateAcknowledgementAndSelectedRootViolations(t *testing.T) {
	tests := []struct {
		name      string
		scenario  Scenario
		invariant string
	}{
		{
			name: "durable-ack-survival",
			scenario: Scenario{
				Generations:          []Generation{completeGeneration(4, 0)},
				LatestSealedSequence: 4,
				SelectedSequence:     4,
				OpenedSequence:       4,
				Acknowledged:         []Acknowledgement{{Sequence: 5, Durable: true}},
			},
			invariant: InvariantDurableAckLost,
		},
		{
			name: "relaxed-loss-is-suffix",
			scenario: Scenario{
				Generations:               []Generation{completeGeneration(6, 0)},
				LatestSealedSequence:      6,
				SelectedSequence:          6,
				OpenedSequence:            6,
				Acknowledged:              []Acknowledgement{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}},
				RecoveredAcknowledgements: []uint64{1, 3},
			},
			invariant: InvariantRelaxedNonSuffixLoss,
		},
		{
			name: "selected-root-is-complete-candidate",
			scenario: Scenario{
				Generations:          []Generation{completeGeneration(6, 0)},
				LatestSealedSequence: 6,
				SelectedSequence:     7,
			},
			invariant: InvariantSelectedRootInvalid,
		},
		{
			name: "selected-root-key-prefix-state",
			scenario: Scenario{
				Generations:               []Generation{completeGeneration(6, 0)},
				LatestSealedSequence:      6,
				SelectedSequence:          6,
				OpenedSequence:            6,
				ExpectedKeyValuesByPrefix: map[string]map[string]string{"user/": {"user/a": "old"}},
				ObservedKeyValuesByPrefix: map[string]map[string]string{"user/": {"user/a": "new"}},
			},
			invariant: InvariantKeyStateMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.scenario.Name = "TestValidateAcknowledgementAndSelectedRootViolations/" + test.name
			test.scenario.Cut = AfterMetaSync
			test.scenario.ReopenAttempted = true
			if err := RequireViolation(test.scenario.Validate(), test.invariant); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateSelectionIsBoundedByLatestSealedSequence(t *testing.T) {
	scenario := Scenario{
		Cut:                  AfterPublicationSealWrite,
		ReopenAttempted:      true,
		Generations:          []Generation{completeGeneration(5, 0), completeGeneration(6, 0)},
		LatestSealedSequence: 5,
		SelectedSequence:     5,
		OpenedSequence:       5,
	}
	if err := scenario.Validate(); err != nil {
		t.Fatalf("newest complete generation at-or-below seal rejected: %v", err)
	}

	scenario.SelectedSequence = 6
	scenario.OpenedSequence = 6
	if err := RequireViolation(scenario.Validate(), InvariantSelectedRootInvalid); err != nil {
		t.Fatal(err)
	}
}

func TestValidateIgnoresIncompleteVolatileCandidateAboveLatestSeal(t *testing.T) {
	volatile := completeGeneration(6, 0)
	volatile.Resources[0].Stable = false
	scenario := Scenario{
		Cut:                  AfterPublicationSealWrite,
		ReopenAttempted:      true,
		Generations:          []Generation{completeGeneration(5, 0), volatile},
		LatestSealedSequence: 5,
		SelectedSequence:     5,
		OpenedSequence:       5,
	}
	if err := scenario.Validate(); err != nil {
		t.Fatalf("incomplete volatile candidate above latest seal invalidated sealed selection: %v", err)
	}
}

func TestValidateAcceptsInitialGenerationZeroSelection(t *testing.T) {
	scenario := Scenario{
		Cut:                  AfterMetaSync,
		ReopenAttempted:      true,
		Generations:          []Generation{completeGeneration(0, 0)},
		LatestSealedSequence: 0,
		SelectedSequence:     0,
		OpenedSequence:       0,
	}
	if err := scenario.Validate(); err != nil {
		t.Fatalf("initial generation zero selection rejected: %v", err)
	}
}

func TestRequireViolationRejectsMissingCounterexample(t *testing.T) {
	if err := RequireViolation(nil, InvariantCommandReplayHole); err == nil {
		t.Fatal("RequireViolation must reject a missing counterexample")
	}
}
