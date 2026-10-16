package db

import (
	"context"
	"errors"
	"sort"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// vacuumSharedBase belongs only to one unpublished replacement construction.
// Source generations are pinned by its snapshots/recoverable set while read;
// the key retains exact incarnation identity, not a path or commit sequence.
// Outputs are fresh COW trees in the replacement pager, never old-arena refs.
// The fixed cache covers the four required records. Additional distinct bases
// use the existing full rebuild, without changing their lifetime requirements.
type vacuumSharedBase struct {
	zipper  *zipper.Zipper
	pager   *pager.Pager
	bases   [4]vacuumBaseImage
	count   int
	retired map[uint64]struct{}
}
type vacuumBaseImage struct {
	source   *indexGen
	operand  node.PrimaryOperand
	sequence uint64
	root     uint64
}

func (s *vacuumSharedBase) release(alloc vacuumCollectionAllocator, ids []uint64) error {
	if s == nil {
		if freer, ok := alloc.(vacuumRetiredPageFreer); ok {
			return freeVacuumRetired(freer, ids)
		}
		var first error
		for _, id := range ids {
			if err := alloc.Free(id); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	if len(ids) != 0 && s.retired == nil {
		s.retired = make(map[uint64]struct{}, len(ids))
	}
	for _, id := range ids {
		s.retired[id] = struct{}{}
	}
	return nil
}

// finish is called only after all current, fallback and direct-parent outputs
// exist. A page shared by any accepted output cannot enter the replacement
// freelist during tail replay or a recoverable-set recapture. Returned dead
// IDs become free in the actual adopted canonical generation, not a temporary
// legacy freelist that is discarded by adoption.
func (s *vacuumSharedBase) finish(ctx context.Context, db *DB, metas []page.MetaPageBody) ([]uint64, error) {
	if s == nil {
		return nil, nil
	}
	live := make(map[uint64]struct{})
	for _, meta := range metas {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		roots := []uint64{meta.UserRootPageID, meta.SystemRootPageID}
		entries, err := vacuumCollectCollectionEntriesFromRoot(ctx, s.pager, db.valueLogManager, meta.SystemRootPageID)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			roots = append(roots, entry.sourceRootIDs...)
		}
		for _, root := range roots {
			if err := tree.New(s.pager, nil, root).WalkPages(func(id uint64, _ node.Node) error {
				live[id] = struct{}{}
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}
	dead := make([]uint64, 0, len(s.retired))
	for id := range s.retired {
		if _, ok := live[id]; !ok {
			dead = append(dead, id)
		}
	}
	sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })
	// No construction aliases survive adoption of the replacement generation.
	*s = vacuumSharedBase{}
	return dead, nil
}

func (s *vacuumSharedBase) build(db *DB, snapshot *Snapshot, root uint64, alloc vacuumCollectionAllocator) (result uint64, handled bool, leavesAppended bool, err error) {
	if s == nil {
		return 0, false, false, nil
	}
	image, err := snapshot.idx.pager.Get(root)
	if err != nil {
		return 0, true, false, err
	}
	if node.NewNode(image).Type() != page.PageTypePrimaryDirectory {
		return 0, false, false, nil
	}
	directory, err := node.DecodePrimaryDirectory(image)
	if err != nil {
		return 0, true, false, err
	}
	base, sequence := directory.Base()
	baseImage, err := snapshot.idx.pager.Get(base.Ref.Page)
	if err != nil {
		return 0, true, false, err
	}
	if kind := node.NewNode(baseImage).Type(); (kind != page.PageTypeLeaf && kind != page.PageTypeInternal) || !node.VerifyPrimaryOperand(base, baseImage) {
		return 0, true, false, node.ErrPrimaryDirectory
	}
	found := -1
	for i := 0; i < s.count; i++ {
		prior := &s.bases[i]
		if prior.source == snapshot.idx && prior.operand == base && prior.sequence == sequence {
			found = i
			break
		}
	}
	if found < 0 {
		if s.count == len(s.bases) {
			return 0, false, false, nil
		}
		var leafLog bulk.LeafPageAppender
		if db.indexOuterLeavesInValueLog {
			if db.leafPageLog == nil {
				return 0, true, false, errors.New("vacuum: leaf page log not configured")
			}
			leafLog = db.leafPageLog
		}
		source := tree.New(snapshot.idx.pager, &snapshot.reader, base.Ref.Page)
		iter := source.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		recording := newVacuumRecordingAllocator(alloc)
		result, err = bulk.BuildWithOptions(iter, recording, s.pager, bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression, LeafColumnar: db.indexColumnarLeaves,
			PackedValuePtr: db.indexPackedValuePtr, InternalBaseDelta: db.indexInternalBaseDelta && !db.indexOuterLeavesInValueLog,
			LeafPageLog: leafLog,
		})
		err = errors.Join(err, iter.Close())
		if err == nil && leafLog != nil {
			err = db.vacuumFinishOuterLeafBuild(s.pager, result)
			leavesAppended = err == nil
		}
		if err != nil {
			return 0, true, leavesAppended, err
		}
		if err = s.release(alloc, recording.pages); err != nil {
			return 0, true, leavesAppended, err
		}
		found = s.count
		s.count++
		s.bases[found] = vacuumBaseImage{source: snapshot.idx, operand: base, sequence: sequence, root: result}
	}
	result = s.bases[found].root
	if directory.Count() == 0 {
		return result, true, leavesAppended, nil
	}
	// SetOps borrows entry bytes. The immutable directory and component pages
	// remain pinned by snapshot until the synchronous Apply completes; the batch
	// and all borrowed aliases expire before that source snapshot is closed.
	delta := batch.New(db.valueLogManager, vacuumInlineThresholdMax)
	defer func() { err = errors.Join(err, delta.Close()) }()
	for i := 0; i < directory.Count(); i++ {
		entry, _ := directory.Entry(i)
		op := batch.Entry{Type: batch.OpDelete, Key: entry.Key, Revision: entry.Revision}
		if !entry.InlineAbsence() {
			value, readErr := snapshot.tree.GetEntry(entry.Key)
			if readErr != nil {
				return 0, true, leavesAppended, readErr
			}
			op.Value, op.ValuePtr, op.IsPtr, op.Revision = value.Value, value.ValuePtr, value.Flags&node.FlagPointer != 0, value.Revision
			if value.Flags&node.FlagTombstone == 0 {
				op.Type = batch.OpPut
			}
		}
		if err = delta.SetOps([]batch.Entry{op}); err != nil {
			return 0, true, leavesAppended, err
		}
	}
	z := s.zipper.CloneWithAllocator(alloc)
	var retired []uint64
	result, retired, _, err = z.Apply(result, delta)
	if err == nil {
		err = s.release(alloc, retired)
	}
	if err == nil && db.indexOuterLeavesInValueLog {
		err = db.vacuumFinishOuterLeafBuild(s.pager, result)
	}
	// A component delta can append outer leaves even when its shared base is a hit.
	leavesAppended = leavesAppended || (err == nil && db.indexOuterLeavesInValueLog)
	return result, true, leavesAppended, err
}
