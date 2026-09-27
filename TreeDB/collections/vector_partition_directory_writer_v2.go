package collections

import (
	"context"
	"encoding/json"
	"fmt"
)

// writeVectorPartitionDirectoryV2 consumes canonical records using one bounded
// leaf and one bounded child buffer per level. emit must durably retain each
// returned local asset until the caller installs or abandons the root. Records
// are copied before the producer callback returns, including pointer payloads.
func writeVectorPartitionDirectoryV2(ctx context.Context, kind string, generation uint64, walk func(func(VectorPartitionDirectoryRecordV2) error) error, emit func(VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error)) (VectorPartitionAssetV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if walk == nil || emit == nil {
		return VectorPartitionAssetV1{}, fmt.Errorf("%w: directory writer inputs", ErrVectorPartitionManifestInvalid)
	}
	var levels [maxVectorPartitionDirectoryDepthV2][]VectorPartitionDirectoryChildV2
	var levelBytes [maxVectorPartitionDirectoryDepthV2]int
	var levelCounts [maxVectorPartitionDirectoryDepthV2]uint64
	var leaf []VectorPartitionDirectoryRecordV2
	var leafBytes int
	// Size the bounded header separately from individually encoded records. This
	// keeps sizing linear without repeatedly encoding the accumulated page.
	fits := func(level uint32, first, last string, count uint64, itemCount, itemBytes int) (bool, error) {
		header, err := json.Marshal(VectorPartitionDirectoryPageV2{Version: 2, Kind: kind, Generation: generation, Level: level, First: first, Last: last, Count: count})
		field := `,"Records":[]`
		if level != 0 {
			field = `,"Children":[]`
		}
		return itemCount <= MaxVectorPartitionDirectoryPageEntriesV2 && 8+len(header)+len(field)+itemBytes+itemCount-1 <= MaxVectorPartitionDirectoryPageBytesV2, err
	}
	var previous string
	var push func(uint32, VectorPartitionDirectoryChildV2) error
	emitChildren := func(level uint32) (VectorPartitionDirectoryChildV2, error) {
		children := levels[level]
		p := VectorPartitionDirectoryPageV2{Version: 2, Kind: kind, Generation: generation, Level: level, First: children[0].First, Last: children[len(children)-1].Last, Children: children}
		for _, child := range children {
			if child.Count > ^uint64(0)-p.Count {
				return VectorPartitionDirectoryChildV2{}, fmt.Errorf("%w: directory count overflow", ErrVectorPartitionManifestInvalid)
			}
			p.Count += child.Count
		}
		a, err := emit(p)
		if err != nil {
			return VectorPartitionDirectoryChildV2{}, err
		}
		levels[level] = nil
		levelBytes[level], levelCounts[level] = 0, 0
		return VectorPartitionDirectoryChildV2{First: p.First, Last: p.Last, Count: p.Count, Asset: a}, nil
	}
	push = func(level uint32, child VectorPartitionDirectoryChildV2) error {
		if level >= maxVectorPartitionDirectoryDepthV2 {
			return fmt.Errorf("%w: directory depth", ErrVectorPartitionManifestInvalid)
		}
		raw, err := json.Marshal(child)
		if err != nil {
			return err
		}
		first := child.First
		if len(levels[level]) != 0 {
			first = levels[level][0].First
		}
		if child.Count > ^uint64(0)-levelCounts[level] {
			return fmt.Errorf("%w: directory count overflow", ErrVectorPartitionManifestInvalid)
		}
		ok, err := fits(level, first, child.Last, levelCounts[level]+child.Count, len(levels[level])+1, levelBytes[level]+len(raw))
		if err != nil {
			return err
		}
		if !ok && len(levels[level]) != 0 {
			parent, err := emitChildren(level)
			if err != nil {
				return err
			}
			if err := push(level+1, parent); err != nil {
				return err
			}
			ok, err = fits(level, child.First, child.Last, child.Count, 1, len(raw))
		}
		if err != nil || !ok {
			return fmt.Errorf("%w: directory child byte cap", ErrVectorPartitionManifestInvalid)
		}
		levels[level] = append(levels[level], child)
		levelBytes[level] += len(raw)
		levelCounts[level] += child.Count
		return nil
	}
	flushLeaf := func() error {
		if len(leaf) == 0 {
			return nil
		}
		first, _ := leaf[0].keyV2(kind)
		last, _ := leaf[len(leaf)-1].keyV2(kind)
		p := VectorPartitionDirectoryPageV2{Version: 2, Kind: kind, Generation: generation, First: first, Last: last, Count: uint64(len(leaf)), Records: leaf}
		a, err := emit(p)
		if err != nil {
			return err
		}
		leaf = nil
		leafBytes = 0
		return push(1, VectorPartitionDirectoryChildV2{First: first, Last: last, Count: p.Count, Asset: a})
	}
	err := walk(func(record VectorPartitionDirectoryRecordV2) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, err := record.keyV2(kind)
		if err != nil {
			return err
		}
		if previous != "" && key <= previous {
			return fmt.Errorf("%w: unsorted or duplicate directory record", ErrVectorPartitionManifestInvalid)
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		first := key
		if len(leaf) != 0 {
			first, _ = leaf[0].keyV2(kind)
		}
		ok, err := fits(0, first, key, uint64(len(leaf)+1), len(leaf)+1, leafBytes+len(raw))
		if err != nil {
			return err
		}
		if !ok && len(leaf) != 0 {
			if err := flushLeaf(); err != nil {
				return err
			}
			ok, err = fits(0, key, key, 1, 1, len(raw))
		}
		if err != nil || !ok {
			return fmt.Errorf("%w: directory record byte cap", ErrVectorPartitionManifestInvalid)
		}
		if record.Snapshot != nil {
			v := *record.Snapshot
			record.Snapshot = &v
		}
		if record.Member != nil {
			v := *record.Member
			record.Member = &v
		}
		if record.Asset != nil {
			v := *record.Asset
			record.Asset = &v
		}
		leaf = append(leaf, record)
		leafBytes += len(raw)
		previous = key
		return nil
	})
	if err != nil {
		return VectorPartitionAssetV1{}, err
	}
	if err := flushLeaf(); err != nil {
		return VectorPartitionAssetV1{}, err
	}
	for level := uint32(1); level < maxVectorPartitionDirectoryDepthV2; level++ {
		if len(levels[level]) == 0 {
			continue
		}
		higher := false
		for i := level + 1; i < maxVectorPartitionDirectoryDepthV2; i++ {
			higher = higher || len(levels[i]) != 0
		}
		if !higher && len(levels[level]) == 1 {
			return levels[level][0].Asset, ctx.Err()
		}
		parent, err := emitChildren(level)
		if err != nil {
			return VectorPartitionAssetV1{}, err
		}
		if err := push(level+1, parent); err != nil {
			return VectorPartitionAssetV1{}, err
		}
	}
	return VectorPartitionAssetV1{}, fmt.Errorf("%w: empty or excessive directory", ErrVectorPartitionManifestInvalid)
}
