package rootpublication

import (
	"fmt"
	"sort"
)

// pendingResourceIndex is coordinator-owned and guarded by Coordinator.mu. It
// validates and accounts for only the tokens in the candidate being added or
// removed. A deterministic borrowed set is materialized once per publication.
type pendingResourceIndex struct {
	logical      map[stableResourceLogicalKey]pendingResourceRecord
	logicalRange map[stableResourceLogicalKey]pendingResourceRecord
	physical     map[stableResourcePhysicalKey]pendingResourceRecord
}

type pendingResourceRecord struct {
	first     *StableResourceToken
	firstRefs uint64
	more      map[*StableResourceToken]uint64
	namespace *StableNamespaceToken
}

func (i *pendingResourceIndex) add(set *StableResourceSet) error {
	if set == nil || len(set.entries) == 0 {
		return nil
	}
	if err := i.validateAdd(set); err != nil {
		return err
	}
	if i.logical == nil {
		i.logical = make(map[stableResourceLogicalKey]pendingResourceRecord)
		i.logicalRange = make(map[stableResourceLogicalKey]pendingResourceRecord)
		i.physical = make(map[stableResourcePhysicalKey]pendingResourceRecord)
	}
	for _, entry := range set.entries {
		token := entry.token
		logicalKey := stableResourceLogicalKeyOf(token)
		logical := i.logical[logicalKey]
		logical.add(token)
		i.logical[logicalKey] = logical

		physical, _ := i.entryRecord(token)
		physical.add(token)
		if physical.namespace == nil && token.namespace != nil {
			physical.namespace = token.namespace
		}
		i.storeEntryRecord(token, physical)
	}
	return nil
}

func (i *pendingResourceIndex) validateAdd(set *StableResourceSet) error {
	for _, entry := range set.entries {
		token := entry.token
		if logical, ok := i.logical[stableResourceLogicalKeyOf(token)]; ok {
			existing := logical.anyToken()
			if existing.identity != token.identity ||
				(resourceKindClass(token.kind) == resourceClassLogicalRange && !stableResourceMetadataCompatible(existing, token)) {
				return fmt.Errorf("%w: logical resource %s/%s changed contract", ErrResourceConflict, token.logicalNamespace, token.resourceID)
			}
		}
		if physical, ok := i.entryRecord(token); ok {
			existing := physical.anyToken()
			if !stableResourcePhysicalContractCompatible(existing, token) {
				return fmt.Errorf("%w: kind=%s identity=%+v", ErrResourceConflict, token.kind, token.identity)
			}
			if _, err := mergeNamespaceToken(physical.namespace, token.namespace); err != nil {
				return fmt.Errorf("%w: kind=%s identity=%+v: %v", ErrResourceConflict, token.kind, token.identity, err)
			}
		}
	}
	return nil
}

func (i *pendingResourceIndex) remove(set *StableResourceSet) {
	if set == nil {
		return
	}
	for _, entry := range set.entries {
		token := entry.token
		logicalKey := stableResourceLogicalKeyOf(token)
		logical, ok := i.logical[logicalKey]
		if !ok || !logical.remove(token) {
			panic("rootpublication: pending logical resource index underflow")
		}
		if logical.empty() {
			delete(i.logical, logicalKey)
		} else {
			i.logical[logicalKey] = logical
		}

		physicalKey := stableResourcePhysicalKeyOf(token)
		physical, ok := i.entryRecord(token)
		if !ok || !physical.remove(token) {
			panic("rootpublication: pending resource token underflow")
		}
		if physical.empty() {
			if resourceKindClass(token.kind) == resourceClassLogicalRange {
				delete(i.logicalRange, logicalKey)
			} else {
				delete(i.physical, physicalKey)
			}
			continue
		}
		physical.namespace = nil
		physical.each(func(candidate *StableResourceToken) {
			if candidate.namespace != nil {
				physical.namespace = candidate.namespace
			}
		})
		i.storeEntryRecord(token, physical)
	}
	if len(i.logical) == 0 {
		i.logical = nil
		i.logicalRange = nil
		i.physical = nil
	}
}

func (r *pendingResourceRecord) add(token *StableResourceToken) {
	if r.first == nil {
		r.first = token
		r.firstRefs = 1
		return
	}
	if r.first == token {
		r.firstRefs++
		return
	}
	if r.more == nil {
		r.more = make(map[*StableResourceToken]uint64)
	}
	r.more[token]++
}

func (r *pendingResourceRecord) remove(token *StableResourceToken) bool {
	if r.first == token {
		if r.firstRefs > 1 {
			r.firstRefs--
			return true
		}
		for next, refs := range r.more {
			r.first = next
			r.firstRefs = refs
			delete(r.more, next)
			return true
		}
		r.first = nil
		r.firstRefs = 0
		return true
	}
	refs := r.more[token]
	if refs == 0 {
		return false
	}
	if refs == 1 {
		delete(r.more, token)
	} else {
		r.more[token] = refs - 1
	}
	return true
}

func (r pendingResourceRecord) empty() bool { return r.first == nil }

func (r pendingResourceRecord) anyToken() *StableResourceToken { return r.first }

func (r pendingResourceRecord) each(fn func(*StableResourceToken)) {
	if r.first != nil {
		fn(r.first)
	}
	for token := range r.more {
		fn(token)
	}
}

func (i *pendingResourceIndex) entryRecord(token *StableResourceToken) (pendingResourceRecord, bool) {
	if resourceKindClass(token.kind) == resourceClassLogicalRange {
		record, ok := i.logicalRange[stableResourceLogicalKeyOf(token)]
		return record, ok
	}
	record, ok := i.physical[stableResourcePhysicalKeyOf(token)]
	return record, ok
}

func (i *pendingResourceIndex) storeEntryRecord(token *StableResourceToken, record pendingResourceRecord) {
	if resourceKindClass(token.kind) == resourceClassLogicalRange {
		i.logicalRange[stableResourceLogicalKeyOf(token)] = record
		return
	}
	i.physical[stableResourcePhysicalKeyOf(token)] = record
}

func (i *pendingResourceIndex) borrowedSnapshot() *StableResourceSet {
	if len(i.physical) == 0 && len(i.logicalRange) == 0 {
		return nil
	}
	entries := make([]stableResourceEntry, 0, len(i.physical)+len(i.logicalRange))
	appendRecord := func(record pendingResourceRecord) {
		var selected *StableResourceToken
		record.each(func(token *StableResourceToken) {
			if selected == nil || token.requiredFrontier > selected.requiredFrontier ||
				(token.requiredFrontier == selected.requiredFrontier && stableResourceLess(token, selected)) {
				selected = token
			}
		})
		if selected.namespace != record.namespace {
			clone := *selected
			clone.namespace = record.namespace
			clone.owner = nil
			selected = &clone
		}
		entries = append(entries, stableResourceEntry{token: selected})
	}
	for _, record := range i.physical {
		appendRecord(record)
	}
	for _, record := range i.logicalRange {
		appendRecord(record)
	}
	sort.Slice(entries, func(a, b int) bool { return stableResourceLess(entries[a].token, entries[b].token) })
	return &StableResourceSet{entries: entries}
}

func (i *pendingResourceIndex) reset() {
	i.logical = nil
	i.logicalRange = nil
	i.physical = nil
}
