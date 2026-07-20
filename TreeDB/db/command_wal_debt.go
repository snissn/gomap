package db

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// CommandWALDependencyDebt retains the exact physical resources on which a
// relaxed command frame depends until a later durable command/barrier covers
// its LSN. Entries are ordered by the command-journal serialization boundary.
type CommandWALDependencyDebt struct {
	mu      sync.Mutex
	entries []commandWALDependencyDebtEntry
}

func (debt *CommandWALDependencyDebt) entryCountThrough(lsn uint64) uint64 {
	if debt == nil || lsn == 0 {
		return 0
	}
	debt.mu.Lock()
	var count uint64
	for _, entry := range debt.entries {
		if entry.firstLSN > lsn {
			break
		}
		count++
	}
	debt.mu.Unlock()
	return count
}

type commandWALDependencyDebtEntry struct {
	firstLSN      uint64
	lastLSN       uint64
	resources     []*rootpublication.StableResourceSet
	rotationFiles []*rootpublication.StableResourceToken
	createdAt     time.Time
	retries       uint64
}

func (debt *CommandWALDependencyDebt) add(lsn uint64, rotationFiles []*rootpublication.StableResourceToken, resources ...*rootpublication.StableResourceSet) error {
	if debt == nil || lsn == 0 {
		return fmt.Errorf("treedb: command WAL dependency debt requires a non-zero LSN")
	}
	owned := resources[:0]
	for _, resource := range resources {
		if resource != nil {
			owned = append(owned, resource)
		}
	}
	debt.mu.Lock()
	defer debt.mu.Unlock()
	if n := len(debt.entries); n != 0 && lsn <= debt.entries[n-1].lastLSN {
		return fmt.Errorf("treedb: command WAL dependency debt LSN %d is not after %d", lsn, debt.entries[n-1].lastLSN)
	}
	debt.entries = append(debt.entries, commandWALDependencyDebtEntry{
		firstLSN: lsn, lastLSN: lsn, resources: append([]*rootpublication.StableResourceSet(nil), owned...),
		rotationFiles: append([]*rootpublication.StableResourceToken(nil), rotationFiles...), createdAt: time.Now(),
	})
	return nil
}

func (debt *CommandWALDependencyDebt) resourceViewThrough(lsn uint64, extra ...*rootpublication.StableResourceSet) (*rootpublication.StableResourceSet, error) {
	sets := append([]*rootpublication.StableResourceSet(nil), extra...)
	if debt != nil {
		debt.mu.Lock()
		for _, entry := range debt.entries {
			if entry.firstLSN > lsn {
				break
			}
			sets = append(sets, entry.resources...)
		}
		debt.mu.Unlock()
	}
	return rootpublication.UnionStableResourceSets(sets...)
}

// hasPhysicalDependenciesThrough reports whether closing the prefix requires
// any external-resource or rotated-command-WAL work before the final WAL sync.
// Empty debt entries still retain their LSN identity for retry diagnostics, but
// do not require allocating union/sort views on an otherwise inline durable
// append.
func (debt *CommandWALDependencyDebt) hasPhysicalDependenciesThrough(lsn uint64) bool {
	if debt == nil {
		return false
	}
	debt.mu.Lock()
	defer debt.mu.Unlock()
	for _, entry := range debt.entries {
		if entry.firstLSN > lsn {
			break
		}
		if len(entry.resources) != 0 || len(entry.rotationFiles) != 0 {
			return true
		}
	}
	return false
}

// rotationFileViewThrough returns a deterministic, exact-handle view of the
// command-WAL files retained by relaxed rotations. These tokens deliberately
// remain outside StableResourceSet until their namespace-create obligations
// have crossed a directory barrier: normal root-publication resource sets are
// only valid after namespaces are stable.
func (debt *CommandWALDependencyDebt) rotationFileViewThrough(lsn uint64) ([]*rootpublication.StableResourceToken, error) {
	if debt == nil {
		return nil, nil
	}
	debt.mu.Lock()
	var tokens []*rootpublication.StableResourceToken
	for _, entry := range debt.entries {
		if entry.firstLSN > lsn {
			break
		}
		tokens = append(tokens, entry.rotationFiles...)
	}
	debt.mu.Unlock()

	// One segment may first be retained as the active successor of a rotation
	// and later as the closed predecessor of another rotation. Coalesce that
	// exact physical identity and select the token with the greatest byte/LSN
	// frontier while retaining every namespace token separately below.
	byIdentity := make(map[rootpublication.StableIdentity]*rootpublication.StableResourceToken, len(tokens))
	for _, token := range tokens {
		if token == nil {
			continue
		}
		identity := token.Identity()
		if existing := byIdentity[identity]; existing != nil {
			if existing.Kind() != token.Kind() || existing.ResourceID() != token.ResourceID() ||
				existing.Generation() != token.Generation() || existing.Digest() != token.Digest() {
				return nil, fmt.Errorf("%w: command-WAL identity has conflicting immutable registration", rootpublication.ErrResourceConflict)
			}
			left, right := existing.Frontier(), token.Frontier()
			if right.Bytes > left.Bytes || (right.Bytes == left.Bytes && right.MaxLSN > left.MaxLSN) {
				byIdentity[identity] = token
			}
			continue
		}
		byIdentity[identity] = token
	}
	view := make([]*rootpublication.StableResourceToken, 0, len(byIdentity))
	for _, token := range byIdentity {
		view = append(view, token)
	}
	sort.Slice(view, func(i, j int) bool {
		if view[i].Kind() != view[j].Kind() {
			return view[i].Kind() < view[j].Kind()
		}
		if view[i].ResourceID() != view[j].ResourceID() {
			return view[i].ResourceID() < view[j].ResourceID()
		}
		return view[i].Generation() < view[j].Generation()
	})
	return view, nil
}

func (debt *CommandWALDependencyDebt) syncRotationFilesThrough(root, walDir string, lsn uint64) error {
	tokens, err := debt.rotationFileViewThrough(lsn)
	if err != nil {
		return err
	}
	for _, token := range tokens {
		path := filepath.Join(walDir, filepath.Base(token.DiagnosticPath()))
		if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, root, path); err != nil {
			return err
		}
		if err := token.SyncThrough(); err != nil {
			return err
		}
		if err := durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, root, path); err != nil {
			return err
		}
	}
	return nil
}

func (debt *CommandWALDependencyDebt) stabilizeRotationNamespacesThrough(root, walDir string, lsn uint64) error {
	if debt == nil {
		return nil
	}
	debt.mu.Lock()
	var tokens []*rootpublication.StableResourceToken
	for _, entry := range debt.entries {
		if entry.firstLSN > lsn {
			break
		}
		tokens = append(tokens, entry.rotationFiles...)
	}
	debt.mu.Unlock()
	compact := tokens[:0]
	for _, token := range tokens {
		if token != nil {
			compact = append(compact, token)
		}
	}
	tokens = compact
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].ResourceID() != tokens[j].ResourceID() {
			return tokens[i].ResourceID() < tokens[j].ResourceID()
		}
		return tokens[i].Generation() < tokens[j].Generation()
	})
	seen := make(map[*rootpublication.StableNamespaceToken]struct{})
	byGeneration := make(map[rootpublication.StableIdentity][]*rootpublication.StableNamespaceToken)
	var generationOrder []rootpublication.StableIdentity
	for _, token := range tokens {
		namespace := token.Namespace()
		if namespace == nil {
			continue
		}
		if _, ok := seen[namespace]; ok {
			continue
		}
		seen[namespace] = struct{}{}
		identity := namespace.ParentIdentity()
		if _, ok := byGeneration[identity]; !ok {
			generationOrder = append(generationOrder, identity)
		}
		byGeneration[identity] = append(byGeneration[identity], namespace)
	}
	for _, identity := range generationOrder {
		if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceCommandWAL, root, walDir); err != nil {
			return err
		}
		if err := rootpublication.StabilizeStableNamespaceTokens(byGeneration[identity]...); err != nil {
			return err
		}
		if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceCommandWAL, root, walDir); err != nil {
			return err
		}
	}
	return nil
}

func (debt *CommandWALDependencyDebt) noteRetryThrough(lsn uint64) {
	if debt == nil {
		return
	}
	debt.mu.Lock()
	defer debt.mu.Unlock()
	for i := range debt.entries {
		if debt.entries[i].firstLSN > lsn {
			break
		}
		debt.entries[i].retries++
	}
}

func (debt *CommandWALDependencyDebt) releaseThrough(lsn uint64) {
	if debt == nil || lsn == 0 {
		return
	}
	debt.mu.Lock()
	cut := 0
	for cut < len(debt.entries) && debt.entries[cut].lastLSN <= lsn {
		cut++
	}
	if cut == 0 {
		debt.mu.Unlock()
		return
	}
	physical := false
	for i := 0; i < cut; i++ {
		if len(debt.entries[i].resources) != 0 || len(debt.entries[i].rotationFiles) != 0 {
			physical = true
			break
		}
	}
	if !physical {
		copy(debt.entries, debt.entries[cut:])
		clear(debt.entries[len(debt.entries)-cut:])
		debt.entries = debt.entries[:len(debt.entries)-cut]
		debt.mu.Unlock()
		return
	}
	released := append([]commandWALDependencyDebtEntry(nil), debt.entries[:cut]...)
	copy(debt.entries, debt.entries[cut:])
	clear(debt.entries[len(debt.entries)-cut:])
	debt.entries = debt.entries[:len(debt.entries)-cut]
	debt.mu.Unlock()
	for _, entry := range released {
		for _, resource := range entry.resources {
			resource.Release()
		}
		for _, token := range entry.rotationFiles {
			token.Release()
		}
	}
}

func (debt *CommandWALDependencyDebt) releaseAll() {
	if debt == nil {
		return
	}
	debt.mu.Lock()
	entries := debt.entries
	debt.entries = nil
	debt.mu.Unlock()
	for _, entry := range entries {
		for _, resource := range entry.resources {
			resource.Release()
		}
		for _, token := range entry.rotationFiles {
			token.Release()
		}
	}
}

type commandWALDependencyDebtStats struct {
	entries uint64
	oldest  time.Duration
	retries uint64
	byKind  []rootpublication.ResourceKindStats
}

func (debt *CommandWALDependencyDebt) stats(now time.Time) commandWALDependencyDebtStats {
	if debt == nil {
		return commandWALDependencyDebtStats{}
	}
	debt.mu.Lock()
	sets := make([]*rootpublication.StableResourceSet, 0)
	stats := commandWALDependencyDebtStats{entries: uint64(len(debt.entries))}
	for _, entry := range debt.entries {
		sets = append(sets, entry.resources...)
		stats.retries += entry.retries
		if age := now.Sub(entry.createdAt); age > stats.oldest {
			stats.oldest = age
		}
	}
	debt.mu.Unlock()
	view, err := rootpublication.UnionStableResourceSets(sets...)
	if err == nil && view != nil {
		stats.byKind = view.Stats(now)
	}
	rotationFiles, rotationErr := debt.rotationFileViewThrough(^uint64(0))
	if rotationErr == nil {
		byKind := make(map[rootpublication.ResourceKind]*rootpublication.ResourceKindStats, len(stats.byKind)+1)
		for i := range stats.byKind {
			kindStats := new(rootpublication.ResourceKindStats)
			*kindStats = stats.byKind[i]
			byKind[kindStats.Kind] = kindStats
		}
		for _, token := range rotationFiles {
			kindStats := byKind[token.Kind()]
			if kindStats == nil {
				kindStats = &rootpublication.ResourceKindStats{Kind: token.Kind()}
				byKind[token.Kind()] = kindStats
			}
			kindStats.PendingCount++
			kindStats.PendingBytes += token.Frontier().Bytes
			kindStats.PendingAge = stats.oldest
			kindStats.ActivePins++
			kindStats.PinHighWater++
		}
		stats.byKind = stats.byKind[:0]
		for _, kindStats := range byKind {
			stats.byKind = append(stats.byKind, *kindStats)
		}
		sort.Slice(stats.byKind, func(i, j int) bool { return stats.byKind[i].Kind < stats.byKind[j].Kind })
	}
	return stats
}

func stabilizeCommandWALResourceNamespaces(resources *rootpublication.StableResourceSet) error {
	if resources == nil {
		return nil
	}
	tokens := resources.Tokens()
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].Kind() != tokens[j].Kind() {
			return tokens[i].Kind() < tokens[j].Kind()
		}
		if tokens[i].ResourceID() != tokens[j].ResourceID() {
			return tokens[i].ResourceID() < tokens[j].ResourceID()
		}
		return tokens[i].Generation() < tokens[j].Generation()
	})
	seen := make(map[*rootpublication.StableNamespaceToken]struct{})
	byGeneration := make(map[rootpublication.StableIdentity][]*rootpublication.StableNamespaceToken)
	var generationOrder []rootpublication.StableIdentity
	for _, token := range tokens {
		namespace := token.Namespace()
		if namespace == nil {
			continue
		}
		if _, ok := seen[namespace]; ok {
			continue
		}
		seen[namespace] = struct{}{}
		identity := namespace.ParentIdentity()
		if _, ok := byGeneration[identity]; !ok {
			generationOrder = append(generationOrder, identity)
		}
		byGeneration[identity] = append(byGeneration[identity], namespace)
	}
	for _, identity := range generationOrder {
		if err := rootpublication.StabilizeStableNamespaceTokens(byGeneration[identity]...); err != nil {
			return err
		}
	}
	return nil
}
