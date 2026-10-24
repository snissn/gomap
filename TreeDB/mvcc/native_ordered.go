package mvcc

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// logicalMVCCWriter is optional: ordinary profiles keep the physical codec
// path. Only New's concrete DB admission may select it; internal decorators
// must retain their overridden Set/Batch behavior and errors.
type logicalMVCCWriter interface {
	SupportsLogicalMVCC() bool
	CommitLogicalMVCC([]mvccadmission.LogicalMutation, mvccadmission.LogicalInput, bool) error
}

// commitLogicalGroups preserves CommitGroupAt's validation order while passing
// typed operands to the existing DB producer. All key/duplicate validation
// precedes discard-floor I/O. No physical keys or value envelopes are allocated.
func (s *Store) commitLogicalGroups(writer logicalMVCCWriter, groups []CommitGroup, total int, mode CommitMode) error {
	mutations := make([]mvccadmission.LogicalMutation, 0, total)
	type identity struct {
		key       string
		timestamp uint64
	}
	var seen map[identity]struct{}
	if total > 1 {
		seen = make(map[identity]struct{}, total)
	}
	for groupIndex := range groups {
		group := &groups[groupIndex]
		for keyIndex := range group.Mutations {
			m := &group.Mutations[keyIndex]
			if _, err := mvcckey.EncodedLen(m.Key); err != nil {
				return fmt.Errorf("%w at group index %d key index %d: %w", ErrInvalidKey, groupIndex, keyIndex, err)
			}
			if seen != nil {
				id := identity{key: string(m.Key), timestamp: group.Timestamp}
				if _, exists := seen[id]; exists {
					return fmt.Errorf("%w at group index %d key index %d", ErrDuplicateKey, groupIndex, keyIndex)
				}
				seen[id] = struct{}{}
			}
			mutations = append(mutations, mvccadmission.LogicalMutation{
				Key: m.Key, Value: m.Value, Timestamp: group.Timestamp, Delete: m.Delete,
			})
		}
	}
	// The selected producer must provide atomic group publication. Store keeps
	// its existing admitted floor exclusion through that synchronous call.
	if err := s.lockDiscardFloorRead(); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	floor := s.discardFloor
	for groupIndex := range groups {
		group := &groups[groupIndex]
		if len(group.Mutations) != 0 && group.Timestamp <= floor {
			return fmt.Errorf("%w: group index %d timestamp %d is not above floor %d", ErrVersionBelowDiscardFloor, groupIndex, group.Timestamp, floor)
		}
	}
	if err := writer.CommitLogicalMVCC(mutations, s.admission.LogicalInput(mutations, floor), mode == CommitDurable); err != nil {
		if total == 1 {
			return storageError("commit point", err)
		}
		return storageError("commit batch", err)
	}
	return nil
}
