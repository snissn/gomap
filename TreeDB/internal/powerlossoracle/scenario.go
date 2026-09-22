package powerlossoracle

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ResourceKind classifies every dependency in a recoverable root closure.
type ResourceKind string

const (
	ResourceIndex      ResourceKind = "index"
	ResourceFreelist   ResourceKind = "freelist"
	ResourceValueLog   ResourceKind = "value-log"
	ResourceOuterLeaf  ResourceKind = "outer-leaf"
	ResourceAuxiliary  ResourceKind = "auxiliary"
	ResourceDirectory  ResourceKind = "directory-entry"
	ResourceSeal       ResourceKind = "publication-seal"
	ResourceCommandWAL ResourceKind = "command-wal"
)

// Resource identifies a generation dependency and its modeled stability.
type Resource struct {
	Kind   ResourceKind
	ID     string
	Stable bool
	Live   bool
}

// Generation is one candidate recoverable meta generation.
type Generation struct {
	Sequence    uint64
	Recoverable bool
	// Complete is retained for fixture readability but is never trusted by the
	// validator; completeness is derived from the resource-kind closure below.
	Complete   bool
	Resources  []Resource
	KeyValues  map[string]string
	AppliedLSN uint64
	LivePages  []uint64
}

// Acknowledgement records the state promised when a public call returned.
type Acknowledgement struct {
	Sequence uint64
	Durable  bool
}

// CommandFrame records replay closure relevant to the oracle.
type CommandFrame struct {
	LSN           uint64
	ChecksumValid bool
	Dependencies  []Resource
	Applied       bool
}

// Scenario is the small durability DSL shared by later graph children.
type Scenario struct {
	Name                      string
	Cut                       CutPoint
	Generations               []Generation
	Acknowledged              []Acknowledgement
	RecoveredAcknowledgements []uint64
	// LatestSealedSequence is the newest generation whose publication seal is
	// stable in the crash image. Public recovery may only select complete
	// candidates at or below this boundary.
	LatestSealedSequence uint64
	// SelectedSequence identifies the complete sealed root chosen before any
	// command-WAL replay. OpenedSequence and OpenedAppliedLSN describe the
	// public handle after replay and may therefore be newer.
	SelectedSequence          uint64
	OpenedSequence            uint64
	OpenedAppliedLSN          uint64
	ExpectedKeyValuesByPrefix map[string]map[string]string
	ObservedKeyValuesByPrefix map[string]map[string]string
	CommandFrames             []CommandFrame
	ReusedPages               []uint64
	RemovedResources          []Resource
	ReopenAttempted           bool
	ReopenRejected            bool
}

// Violation is a stable, test-assertable invariant diagnosis.
type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) Error() string { return v.Invariant + ": " + v.Detail }

const (
	InvariantIncompleteRecoverableRoot = "incomplete-recoverable-root"
	InvariantRecoverablePageReused     = "recoverable-page-reused"
	InvariantCommandReplayHole         = "command-replay-hole"
	InvariantDurableAckLost            = "durable-ack-lost"
	InvariantRelaxedNonSuffixLoss      = "relaxed-non-suffix-loss"
	InvariantEarlySourceDeletion       = "early-source-deletion"
	InvariantPublicReopenMissing       = "public-reopen-missing"
	InvariantSelectedRootInvalid       = "selected-root-invalid"
	InvariantKeyStateMismatch          = "key-state-mismatch"
)

// Validate applies the fixed #3674 contract after a public reopen attempt.
func (s Scenario) Validate() error {
	if !s.ReopenAttempted {
		return Violation{Invariant: InvariantPublicReopenMissing, Detail: "stable-only image was not passed through public Open"}
	}
	newest := s.newestRecoverable()
	for _, generation := range s.Generations {
		if generation.Recoverable && generation.Sequence <= s.LatestSealedSequence && !generationComplete(generation) {
			missing := missingGenerationResources(generation)
			return Violation{Invariant: InvariantIncompleteRecoverableRoot, Detail: fmt.Sprintf("generation=%d missing=%s cut=%s", generation.Sequence, strings.Join(missing, ","), s.Cut)}
		}
	}
	recovered := make(map[uint64]struct{}, len(s.RecoveredAcknowledgements))
	for _, sequence := range s.RecoveredAcknowledgements {
		recovered[sequence] = struct{}{}
	}
	for _, ack := range s.Acknowledged {
		_, recoveredByWAL := recovered[ack.Sequence]
		recoveredByRoot := newest != nil && ack.Sequence <= newest.AppliedLSN
		if ack.Durable && !recoveredByRoot && !recoveredByWAL {
			recoveredAppliedLSN := uint64(0)
			if newest != nil {
				recoveredAppliedLSN = newest.AppliedLSN
			}
			return Violation{Invariant: InvariantDurableAckLost, Detail: fmt.Sprintf("ack=%d recovered-applied-lsn=%d cut=%s", ack.Sequence, recoveredAppliedLSN, s.Cut)}
		}
	}
	if err := s.validateSelectedState(); err != nil {
		return err
	}
	protected := make(map[uint64]uint64)
	for _, generation := range s.Generations {
		if !generation.Recoverable {
			continue
		}
		for _, page := range generation.LivePages {
			protected[page] = generation.Sequence
		}
	}
	for _, page := range s.ReusedPages {
		if sequence, ok := protected[page]; ok {
			return Violation{Invariant: InvariantRecoverablePageReused, Detail: fmt.Sprintf("page=%d generation=%d cut=%s", page, sequence, s.Cut)}
		}
	}
	frames := append([]CommandFrame(nil), s.CommandFrames...)
	sort.Slice(frames, func(i, j int) bool { return frames[i].LSN < frames[j].LSN })
	baseAppliedLSN := uint64(0)
	if selected := s.selectedGeneration(); selected != nil {
		baseAppliedLSN = selected.AppliedLSN
	} else if newest != nil {
		baseAppliedLSN = newest.AppliedLSN
	}
	expectedLSN := baseAppliedLSN + 1
	contiguousAppliedLSN := baseAppliedLSN
	holeSeen := false
	for _, frame := range frames {
		if frame.LSN <= baseAppliedLSN {
			continue
		}
		if frame.LSN != expectedLSN {
			holeSeen = true
		}
		complete := frame.ChecksumValid && len(missingResources(frame.Dependencies)) == 0
		if frame.Applied {
			if !complete || holeSeen {
				return Violation{Invariant: InvariantCommandReplayHole, Detail: fmt.Sprintf("base=%d lsn=%d complete=%t prior-hole=%t cut=%s", baseAppliedLSN, frame.LSN, complete, holeSeen, s.Cut)}
			}
			contiguousAppliedLSN = frame.LSN
		} else {
			if complete && !holeSeen {
				return Violation{Invariant: InvariantCommandReplayHole, Detail: fmt.Sprintf("base=%d skipped-complete-lsn=%d cut=%s", baseAppliedLSN, frame.LSN, s.Cut)}
			}
			holeSeen = true
		}
		expectedLSN = frame.LSN + 1
	}
	if s.OpenedAppliedLSN != contiguousAppliedLSN {
		return Violation{Invariant: InvariantCommandReplayHole, Detail: fmt.Sprintf("base=%d contiguous-applied=%d public-open-applied=%d cut=%s", baseAppliedLSN, contiguousAppliedLSN, s.OpenedAppliedLSN, s.Cut)}
	}
	if selected := s.selectedGeneration(); selected != nil && s.OpenedSequence > selected.Sequence && contiguousAppliedLSN == baseAppliedLSN {
		return Violation{Invariant: InvariantCommandReplayHole, Detail: fmt.Sprintf("selected-generation=%d public-open-generation=%d without replay cut=%s", selected.Sequence, s.OpenedSequence, s.Cut)}
	}
	if err := s.validateRelaxedSuffix(); err != nil {
		return err
	}
	for _, removed := range s.RemovedResources {
		for _, generation := range s.Generations {
			if !generation.Recoverable {
				continue
			}
			for _, required := range generation.Resources {
				if required.Kind == removed.Kind && required.ID == removed.ID {
					return Violation{Invariant: InvariantEarlySourceDeletion, Detail: fmt.Sprintf("%s/%s still required by generation=%d cut=%s", removed.Kind, removed.ID, generation.Sequence, s.Cut)}
				}
			}
		}
	}
	return nil
}

func (s Scenario) validateSelectedState() error {
	newest := s.newestRecoverable()
	if s.ReopenRejected {
		if s.SelectedSequence != 0 || s.OpenedSequence != 0 {
			return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("public Open rejected but selected=(scenario=%d opened=%d) cut=%s", s.SelectedSequence, s.OpenedSequence, s.Cut)}
		}
		if newest != nil {
			return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("public Open rejected despite complete generation=%d at-or-below seal=%d cut=%s", newest.Sequence, s.LatestSealedSequence, s.Cut)}
		}
		return nil
	}
	if newest == nil {
		if s.SelectedSequence != 0 || s.OpenedSequence != 0 {
			return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("selected generation without complete candidate at-or-below seal=%d scenario=%d opened=%d cut=%s", s.LatestSealedSequence, s.SelectedSequence, s.OpenedSequence, s.Cut)}
		}
		return nil
	}
	selected := s.selectedGeneration()
	if selected == nil || !selected.Recoverable || !generationComplete(*selected) {
		return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("selected generation=%d is not a complete recovery candidate cut=%s", s.SelectedSequence, s.Cut)}
	}
	if selected.Sequence > s.LatestSealedSequence {
		return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("selected generation=%d exceeds latest sealed generation=%d cut=%s", selected.Sequence, s.LatestSealedSequence, s.Cut)}
	}
	if selected.Sequence != newest.Sequence {
		return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("selected generation=%d is not newest complete generation=%d at-or-below seal=%d cut=%s", selected.Sequence, newest.Sequence, s.LatestSealedSequence, s.Cut)}
	}
	if s.ReopenRejected || s.OpenedSequence < s.SelectedSequence || s.OpenedAppliedLSN < selected.AppliedLSN {
		return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("selected=(generation=%d applied=%d) public-open=(rejected=%t generation=%d applied=%d) cut=%s", s.SelectedSequence, selected.AppliedLSN, s.ReopenRejected, s.OpenedSequence, s.OpenedAppliedLSN, s.Cut)}
	}
	for _, prefix := range sortedPrefixKeys(s.ExpectedKeyValuesByPrefix) {
		expected := s.ExpectedKeyValuesByPrefix[prefix]
		observed, ok := s.ObservedKeyValuesByPrefix[prefix]
		if !ok {
			return Violation{Invariant: InvariantKeyStateMismatch, Detail: fmt.Sprintf("generation=%d prefix=%q was not observed cut=%s", s.SelectedSequence, prefix, s.Cut)}
		}
		for _, key := range sortedStringKeys(expected) {
			value := expected[key]
			got, ok := observed[key]
			if !ok || got != value {
				return Violation{Invariant: InvariantKeyStateMismatch, Detail: fmt.Sprintf("generation=%d prefix=%q key=%q got=%q want=%q cut=%s", s.SelectedSequence, prefix, key, got, value, s.Cut)}
			}
		}
		for _, key := range sortedStringKeys(observed) {
			if _, ok := expected[key]; !ok {
				return Violation{Invariant: InvariantKeyStateMismatch, Detail: fmt.Sprintf("generation=%d prefix=%q unexpected-key=%q cut=%s", s.SelectedSequence, prefix, key, s.Cut)}
			}
		}
	}
	return nil
}

func sortedStringKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedPrefixKeys(values map[string]map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s Scenario) validateRelaxedSuffix() error {
	if len(s.RecoveredAcknowledgements) == 0 {
		return nil
	}
	recovered := make(map[uint64]struct{}, len(s.RecoveredAcknowledgements))
	for _, sequence := range s.RecoveredAcknowledgements {
		recovered[sequence] = struct{}{}
	}
	acks := append([]Acknowledgement(nil), s.Acknowledged...)
	sort.Slice(acks, func(i, j int) bool { return acks[i].Sequence < acks[j].Sequence })
	lost := false
	for _, ack := range acks {
		_, ok := recovered[ack.Sequence]
		if !ok {
			lost = true
			continue
		}
		if lost {
			return Violation{Invariant: InvariantRelaxedNonSuffixLoss, Detail: fmt.Sprintf("recovered sequence=%d after earlier acknowledged loss cut=%s", ack.Sequence, s.Cut)}
		}
	}
	return nil
}

// RequireViolation makes counterexamples pass only for their named diagnosis.
func RequireViolation(err error, invariant string) error {
	if err == nil {
		return fmt.Errorf("powerlossoracle: expected %s violation", invariant)
	}
	var violation Violation
	if !errors.As(err, &violation) {
		return fmt.Errorf("powerlossoracle: expected %s violation, got %v", invariant, err)
	}
	if violation.Invariant != invariant {
		return fmt.Errorf("powerlossoracle: expected %s violation, got %s (%s)", invariant, violation.Invariant, violation.Detail)
	}
	return nil
}

func (s Scenario) newestRecoverable() *Generation {
	var newest *Generation
	for i := range s.Generations {
		candidate := &s.Generations[i]
		if candidate.Recoverable && candidate.Sequence <= s.LatestSealedSequence && generationComplete(*candidate) && (newest == nil || candidate.Sequence > newest.Sequence) {
			newest = candidate
		}
	}
	return newest
}

func (s Scenario) selectedGeneration() *Generation {
	for index := range s.Generations {
		if s.Generations[index].Sequence == s.SelectedSequence {
			return &s.Generations[index]
		}
	}
	return nil
}

func missingResources(resources []Resource) []string {
	var missing []string
	for _, resource := range resources {
		if !resource.Stable || !resource.Live {
			missing = append(missing, string(resource.Kind)+"/"+resource.ID)
		}
	}
	sort.Strings(missing)
	return missing
}

func generationComplete(generation Generation) bool {
	return len(missingGenerationResources(generation)) == 0
}

var requiredRootResourceKinds = []ResourceKind{
	ResourceIndex,
	ResourceFreelist,
	ResourceValueLog,
	ResourceOuterLeaf,
	ResourceAuxiliary,
	ResourceDirectory,
	ResourceSeal,
	ResourceCommandWAL,
}

func missingGenerationResources(generation Generation) []string {
	missing := missingResources(generation.Resources)
	present := make(map[ResourceKind]bool, len(generation.Resources))
	for _, resource := range generation.Resources {
		if resource.Stable && resource.Live {
			present[resource.Kind] = true
		}
	}
	for _, kind := range requiredRootResourceKinds {
		if !present[kind] {
			missing = append(missing, string(kind)+"/<closure>")
		}
	}
	sort.Strings(missing)
	return missing
}
