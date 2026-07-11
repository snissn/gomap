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
	Complete    bool
	Resources   []Resource
	KeyValues   map[string]string
	AppliedLSN  uint64
	LivePages   []uint64
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
	SelectedSequence          uint64
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
	if newest != nil && !generationComplete(*newest) {
		missing := missingResources(newest.Resources)
		if !s.ReopenRejected {
			return Violation{Invariant: InvariantIncompleteRecoverableRoot, Detail: fmt.Sprintf("generation=%d missing=%s cut=%s", newest.Sequence, strings.Join(missing, ","), s.Cut)}
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
	var lastApplied uint64
	missingSeen := false
	for _, frame := range frames {
		complete := frame.ChecksumValid && len(missingResources(frame.Dependencies)) == 0
		if frame.Applied {
			if !complete || missingSeen || (lastApplied != 0 && frame.LSN != lastApplied+1) {
				return Violation{Invariant: InvariantCommandReplayHole, Detail: fmt.Sprintf("lsn=%d complete=%t prior-hole=%t cut=%s", frame.LSN, complete, missingSeen, s.Cut)}
			}
			lastApplied = frame.LSN
		} else if !complete {
			missingSeen = true
		}
	}
	if newest != nil {
		for _, ack := range s.Acknowledged {
			if ack.Durable && ack.Sequence > newest.Sequence {
				return Violation{Invariant: InvariantDurableAckLost, Detail: fmt.Sprintf("ack=%d recovered=%d cut=%s", ack.Sequence, newest.Sequence, s.Cut)}
			}
		}
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
	if s.SelectedSequence == 0 {
		return nil
	}
	var selected *Generation
	for index := range s.Generations {
		if s.Generations[index].Sequence == s.SelectedSequence {
			selected = &s.Generations[index]
			break
		}
	}
	if selected == nil || !selected.Recoverable || !generationComplete(*selected) {
		return Violation{Invariant: InvariantSelectedRootInvalid, Detail: fmt.Sprintf("selected generation=%d is not a complete recovery candidate cut=%s", s.SelectedSequence, s.Cut)}
	}
	for prefix, expected := range s.ExpectedKeyValuesByPrefix {
		observed, ok := s.ObservedKeyValuesByPrefix[prefix]
		if !ok {
			return Violation{Invariant: InvariantKeyStateMismatch, Detail: fmt.Sprintf("generation=%d prefix=%q was not observed cut=%s", s.SelectedSequence, prefix, s.Cut)}
		}
		for key, value := range expected {
			got, ok := observed[key]
			if !ok || got != value {
				return Violation{Invariant: InvariantKeyStateMismatch, Detail: fmt.Sprintf("generation=%d prefix=%q key=%q got=%q want=%q cut=%s", s.SelectedSequence, prefix, key, got, value, s.Cut)}
			}
		}
		for key := range observed {
			if _, ok := expected[key]; !ok {
				return Violation{Invariant: InvariantKeyStateMismatch, Detail: fmt.Sprintf("generation=%d prefix=%q unexpected-key=%q cut=%s", s.SelectedSequence, prefix, key, s.Cut)}
			}
		}
	}
	return nil
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
		if lost && !ack.Durable {
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
		if candidate.Recoverable && (newest == nil || candidate.Sequence > newest.Sequence) {
			newest = candidate
		}
	}
	return newest
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
	return generation.Complete && len(missingResources(generation.Resources)) == 0
}
