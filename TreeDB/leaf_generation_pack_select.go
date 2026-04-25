package treedb

import treedbdb "github.com/snissn/gomap/TreeDB/db"

// LeafGenerationPackSelectOptions bounds a selected prefix of plan candidates.
type LeafGenerationPackSelectOptions = treedbdb.LeafGenerationPackSelectOptions

// LeafGenerationPackSelection summarizes a bounded prefix of pack candidates.
type LeafGenerationPackSelection = treedbdb.LeafGenerationPackSelection

// SelectLeafGenerationPackCandidates selects a bounded prefix from an eligible
// leaf-generation plan. This is a pure helper for operator tooling and future
// bounded online maintenance runners.
func SelectLeafGenerationPackCandidates(plan LeafGenerationPlan, opts LeafGenerationPackSelectOptions) (LeafGenerationPackSelection, error) {
	selection, err := treedbdb.SelectLeafGenerationPackCandidates(treedbdb.LeafGenerationPlan(plan), treedbdb.LeafGenerationPackSelectOptions(opts))
	if err != nil {
		return LeafGenerationPackSelection{}, err
	}
	return LeafGenerationPackSelection(selection), nil
}
