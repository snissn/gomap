package raftcluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	hraft "github.com/hashicorp/raft"
)

// readIndexSnapshotGapV1 caches only a verified, installed native snapshot's
// command-free interval. Native Open verifies the complete file CRC before the
// archive header is read; the cache avoids that full pass on every read fence.
type readIndexSnapshotGapV1 struct {
	mu         sync.Mutex
	generation uint64
	proof      readIndexSnapshotGapProofV1
	loading    chan struct{}
}

type readIndexSnapshotGapProofV1 struct {
	commandTerm  uint64
	commandIndex uint64
	nativeTerm   uint64
	nativeIndex  uint64
}

func (c *readIndexSnapshotGapV1) invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.generation++
	c.proof = readIndexSnapshotGapProofV1{}
	c.mu.Unlock()
}

func (p readIndexSnapshotGapProofV1) covers(progress AppliedProgress, index uint64) bool {
	return progress.HasApplied && progress.Term == p.commandTerm && progress.Index == p.commandIndex &&
		p.commandIndex < index && index <= p.nativeIndex
}

func (c *readIndexSnapshotGapV1) get(ctx context.Context, progress AppliedProgress, index uint64, current func() (uint64, uint64), verify func(context.Context) (readIndexSnapshotGapProofV1, error)) (readIndexSnapshotGapProofV1, error) {
	if c == nil || current == nil || verify == nil {
		return readIndexSnapshotGapProofV1{}, ErrReadBarrierNotSatisfied
	}
	for {
		if err := ctx.Err(); err != nil {
			return readIndexSnapshotGapProofV1{}, err
		}
		c.mu.Lock()
		if c.proof.covers(progress, index) {
			proof := c.proof
			c.mu.Unlock()
			if nativeIndex, nativeTerm := current(); nativeIndex == proof.nativeIndex && nativeTerm == proof.nativeTerm {
				return proof, nil
			}
			c.invalidate()
			continue
		}
		if c.loading != nil {
			loading := c.loading
			c.mu.Unlock()
			select {
			case <-loading:
				continue
			case <-ctx.Done():
				return readIndexSnapshotGapProofV1{}, ctx.Err()
			}
		}
		generation := c.generation
		loading := make(chan struct{})
		c.loading = loading
		c.mu.Unlock()

		proof, err := verify(ctx)
		c.mu.Lock()
		if c.generation != generation {
			err = fmt.Errorf("%w: installed snapshot changed during gap verification", ErrReadBarrierNotSatisfied)
		} else if err == nil && proof.covers(progress, index) {
			c.proof = proof
		} else if err == nil {
			err = fmt.Errorf("%w: snapshot does not cover missing raft log %d", ErrReadBarrierNotSatisfied, index)
		}
		c.loading = nil
		close(loading)
		c.mu.Unlock()
		if err == nil {
			if nativeIndex, nativeTerm := current(); nativeIndex != proof.nativeIndex || nativeTerm != proof.nativeTerm {
				return readIndexSnapshotGapProofV1{}, fmt.Errorf("%w: installed snapshot changed after gap verification", ErrReadBarrierNotSatisfied)
			}
		}
		return proof, err
	}
}

func (p *HashicorpRaftProvider) verifyInstalledReadIndexSnapshotGapV1(ctx context.Context, progress AppliedProgress) (readIndexSnapshotGapProofV1, error) {
	if p == nil || p.raft == nil || p.snapshotStore == nil || !progress.HasApplied {
		return readIndexSnapshotGapProofV1{}, ErrReadBarrierNotSatisfied
	}
	if err := ctx.Err(); err != nil {
		return readIndexSnapshotGapProofV1{}, err
	}
	nativeIndex, nativeTerm := p.raft.InstalledSnapshotBoundary()
	if nativeIndex <= progress.Index || nativeTerm == 0 {
		return readIndexSnapshotGapProofV1{}, fmt.Errorf("%w: no installed snapshot covers command index %d", ErrReadBarrierNotSatisfied, progress.Index)
	}
	metas, err := p.snapshotStore.List()
	if err != nil {
		return readIndexSnapshotGapProofV1{}, err
	}
	var selected *hraft.SnapshotMeta
	for _, meta := range metas {
		if meta != nil && meta.Index == nativeIndex && meta.Term == nativeTerm {
			selected = meta
			break
		}
	}
	if selected == nil || selected.ID == "" || selected.Size <= 0 {
		return readIndexSnapshotGapProofV1{}, fmt.Errorf("%w: installed snapshot archive is unavailable", ErrReadBarrierNotSatisfied)
	}
	// FileSnapshotStore.Open performs its whole-file CRC pass before returning.
	// That pass may not be interruptible; it is performed once per installed
	// boundary, and concurrent readers wait on the cache's completion channel.
	meta, src, err := p.snapshotStore.Open(selected.ID)
	if err != nil {
		return readIndexSnapshotGapProofV1{}, err
	}
	manifest, decodeErr := DecodeSnapshotManifestV1FromArchiveReader(src)
	closeErr := src.Close()
	if decodeErr != nil || closeErr != nil {
		return readIndexSnapshotGapProofV1{}, fmt.Errorf("%w: read installed snapshot manifest: %v", ErrReadBarrierNotSatisfied, errors.Join(decodeErr, closeErr))
	}
	commandTerm, commandIndex := manifest.CommandBoundaryV1()
	if meta == nil || meta.ID != selected.ID || meta.Size != selected.Size ||
		meta.Index != nativeIndex || meta.Term != nativeTerm ||
		manifest.Version != SnapshotManifestVersion2 || manifest.GroupID != p.cluster.GroupID ||
		manifest.LastIncludedIndex != nativeIndex || manifest.LastIncludedTerm != nativeTerm ||
		commandIndex != progress.Index || commandTerm != progress.Term {
		return readIndexSnapshotGapProofV1{}, fmt.Errorf("%w: installed snapshot and durable command boundary differ", ErrReadBarrierNotSatisfied)
	}
	if afterIndex, afterTerm := p.raft.InstalledSnapshotBoundary(); afterIndex != nativeIndex || afterTerm != nativeTerm {
		return readIndexSnapshotGapProofV1{}, fmt.Errorf("%w: installed snapshot changed during gap verification", ErrReadBarrierNotSatisfied)
	}
	return readIndexSnapshotGapProofV1{commandTerm: commandTerm, commandIndex: commandIndex, nativeTerm: nativeTerm, nativeIndex: nativeIndex}, nil
}
