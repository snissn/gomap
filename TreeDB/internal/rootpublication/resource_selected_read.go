package rootpublication

import "os"

// WithSelectedResourceFile scopes access to the original pinned file for one
// exact selected logical obligation. The caller must own source and its
// metadata enrollment, and exclude source.Release until fn returns. This is a
// borrowed read capability: it does not acquire a token or synchronize a
// concurrent Release. The callback must neither retain nor close the file.
//
// Lookup holds source.mu; the callback runs after that mutex is released and
// must run outside publication and cut gates. Generic inventories and physical
// projections fail closed. No admission, clone or current-path lookup occurs.
func (source *StableResourceSet) WithSelectedResourceFile(selector StableResourceSelector, fn func(*os.File) error) error {
	return source.withSelectedResourceFile(selector, fn, nil)
}

type selectedResourceReadWork struct {
	LogicalIndexNodes, EntriesInspected uint64
}

func (source *StableResourceSet) withSelectedResourceFile(selector StableResourceSelector, fn func(*os.File) error, work *selectedResourceReadWork) error {
	if source == nil || fn == nil {
		return ErrResourceOwnership
	}
	if selector.Kind == "" || selector.LogicalLane == "" || selector.ResourceID == "" || selector.PhysicalGeneration == 0 {
		return ErrUnresolvedResource
	}
	token, err := source.selectedResourceReadToken(selector, work)
	if err != nil {
		return err
	}
	return token.WithPinnedFile(fn)
}

// The returned token is borrowed from the caller-owned source. No token,
// entry, tree or allocation alias escapes to the callback except the existing
// pinned file's scoped argument.
func (source *StableResourceSet) selectedResourceReadToken(selector StableResourceSelector, work *selectedResourceReadWork) (*StableResourceToken, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if !source.selected || source.metadata == nil || source.physicalOnly ||
		!source.selectedKindViewsAccessibleLocked() || source.kindViews == nil {
		return nil, ErrResourceOwnership
	}
	view, found := source.kindViews.find(selector.Kind)
	if !found {
		return nil, ErrUnresolvedResource
	}
	key := stableLogicalResourceKey{kind: selector.Kind, lane: selector.LogicalLane, resourceID: selector.ResourceID, generation: selector.PhysicalGeneration}
	var entry *stableResourceEntry
	for root := view.logical; root != nil; {
		if work != nil {
			work.LogicalIndexNodes++
		}
		if key == root.key {
			entry = root.entry
			break
		}
		if stableLogicalResourceKeyLess(key, root.key) {
			root = root.left
		} else {
			root = root.right
		}
	}
	if entry == nil {
		return nil, ErrUnresolvedResource
	}
	if work != nil {
		work.EntriesInspected++
	}
	obligation, found, err := entry.logicalObligations.lookup(selector.Obligation, nil)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrUnresolvedResource
	}
	if obligation != selector.Obligation {
		return nil, ErrResourceConflict
	}
	token := activeEntryToken(*entry)
	if token == nil || token.metadata == nil || token.metadata.owner != source.metadata.owner {
		return nil, ErrResourceOwnership
	}
	return token, nil
}
