package zipper

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// ApplyMaterializedPrimaryDeltaV6 applies the canonical physical delta through
// the original materialized DATA path. It creates no PRIMARY constructor and
// does not publish or seal: its caller retains the SAME native constructor,
// allocator tracker, retired pages and original publication transaction.
// The synchronous diagnostic accounts its whole elapsed/allocation cost; this
// method does not claim an ordinal work bound.
func (z *Zipper) ApplyMaterializedPrimaryDeltaV6(rootID uint64, delta *batch.Batch) (base node.PrimaryOperand, sequence uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	image, err := z.pager.Get(rootID)
	if err != nil {
		return base, 0, nil, metrics, err
	}
	base = node.PrimaryOperand{Ref: page.PageChildRef(rootID), Digest: sha256.Sum256(image)}
	sequence = 1
	cfg := applyRunConfig{materializedBase: true}
	if node.NewNode(image).Type() == page.PageTypePrimaryDirectory {
		directory, e := node.DecodePrimaryDirectory(image)
		if e != nil {
			return base, 0, nil, metrics, e
		}
		base, sequence = directory.Base()
		entries, e := makePrimaryScratch[node.PrimaryDirectoryEntry](z.primaryArena, directory.Count(), directory.Count())
		if e != nil {
			return base, 0, nil, metrics, e
		}
		defer releasePrimaryScratch(z.primaryArena, &entries)
		for i := range entries {
			entries[i], _ = directory.Entry(i)
		}
		scratch := z.acquireApplyScratch()
		defer z.releaseApplyScratch(scratch)
		if _, err = z.loadPrimaryOperand(base, nil, &metrics, scratch); err != nil {
			return base, 0, nil, metrics, err
		}
		var old []uint64
		base, sequence, old, err = z.primaryMaterialize(base, sequence, entries, len(entries), cfg, &metrics, scratch)
		if err != nil {
			return base, 0, nil, metrics, err
		}
		retired = append(retired, old...)
		if !primaryarena.IsPage(rootID) {
			retired = append(retired, rootID)
		}
	}
	root, old, work, err := z.applyWithConfig(base.Ref.Page, delta, cfg)
	mergeMetrics(&metrics, &work)
	if err != nil {
		return base, 0, nil, metrics, err
	}
	retired = append(retired, old...)
	image, err = z.pager.Get(root)
	if err != nil {
		return base, 0, nil, metrics, err
	}
	base = node.PrimaryOperand{Ref: page.PageChildRef(root), Digest: sha256.Sum256(image)}
	return base, sequence + 1, retired, metrics, nil
}
