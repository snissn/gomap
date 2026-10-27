package caching

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func privateCOWActiveLeafSources(t *testing.T, cache *DB) ([]*lane, []backenddb.LeafPageLogPackSource) {
	t.Helper()
	var lanes []*lane
	var sources []backenddb.LeafPageLogPackSource
	for _, lane := range cache.leafLogAppendLanesSnapshot() {
		lane.vlogMu.Lock()
		if lane.vlog == nil {
			lane.vlogMu.Unlock()
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(lane.id), uint32(lane.vlogSeq))
		if err != nil {
			t.Fatal(err)
		}
		identity, err := lane.vlog.(interface {
			StableIdentity() (rootpublication.StableIdentity, error)
		}).StableIdentity()
		lane.vlogMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		lanes = append(lanes, lane)
		sources = append(sources, backenddb.LeafPageLogPackSource{FileID: id, Identity: identity})
	}
	return lanes, sources
}

func TestPrivateCOWLeafSourceHandoffValidatesAllBeforeRotation(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	lanes, sources := privateCOWActiveLeafSources(t, cache)
	if len(lanes) < 2 {
		t.Fatal("need two real active source writers")
	}
	cache.flushMu.Lock()
	defer cache.flushMu.Unlock()
	paths := make([]string, len(lanes))
	rotations := make([]uint64, len(lanes))
	for i, lane := range lanes {
		paths[i], rotations[i] = lane.vlogPath, lane.vlogRotateTotal.Load()
	}
	for _, owner := range []*transactionowner.Owner{nil, transactionowner.New()} {
		if err := cache.handoffPrivateCOWLeafPackSources(context.Background(), owner, sources); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("wrong owner: %v", err)
		}
	}
	wrong := append([]backenddb.LeafPageLogPackSource(nil), sources...)
	wrong[len(wrong)-1].Identity.ObjectID[0] ^= 1
	if err := cache.handoffPrivateCOWLeafPackSources(context.Background(), cache.cow.txnOwner, wrong); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("wrong later identity: %v", err)
	}
	for i, lane := range lanes {
		if lane.vlogPath != paths[i] || lane.vlogRotateTotal.Load() != rotations[i] {
			t.Fatal("rotated before all exact source identities were validated")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cache.handoffPrivateCOWLeafPackSources(ctx, cache.cow.txnOwner, sources); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled owner handoff: %v", err)
	}
}

type leafHandoffFailureWriter struct {
	valueWriter
	identity  rootpublication.StableIdentity
	failure   error
	installed bool
}

func (w *leafHandoffFailureWriter) StableIdentity() (rootpublication.StableIdentity, error) {
	return w.identity, nil
}
func (w *leafHandoffFailureWriter) RotateToWithSync(path string, fileID uint32, syncCurrent bool) error {
	if w.installed {
		if err := w.valueWriter.RotateToWithSync(path, fileID, syncCurrent); err != nil {
			return err
		}
		return installedValueLogRotationError{err: w.failure}
	}
	return w.failure
}

func TestPrivateCOWLeafSourceHandoffPartialRotationFailureKeepsInstalledOwner(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-installed", true: "installed"}[installed], func(t *testing.T) {
			cache, backend := privateCOWPackFixture(t)
			preparePrivateCOWPack(t, cache, backend.DB)
			lanes, sources := privateCOWActiveLeafSources(t, cache)
			if len(lanes) < 2 {
				t.Fatal("need two real active source writers")
			}
			cache.flushMu.Lock()
			before, _ := backend.DB.StateToken()
			first, last := lanes[0], lanes[1]
			firstPath, lastPath := first.vlogPath, last.vlogPath
			original := last.vlog
			failure := errors.New("selected source rotation failed")
			last.vlog = &leafHandoffFailureWriter{valueWriter: original, identity: sources[1].Identity, failure: failure, installed: installed}
			err := cache.handoffPrivateCOWLeafPackSources(context.Background(), cache.cow.txnOwner, sources[:2])
			last.vlog = original
			cache.flushMu.Unlock()
			if !errors.Is(err, failure) {
				t.Fatalf("rotation error lost: %v", err)
			}
			if first.vlogPath == firstPath || (last.vlogPath != lastPath) != installed {
				t.Fatalf("installed rotation ownership not reconciled: first=%s last=%s", first.vlogPath, last.vlogPath)
			}
			after, _ := backend.DB.StateToken()
			if !samePrivateCOWLeafLogicalBasis(before, after) || backend.gcCalls != 0 {
				t.Fatalf("partial handoff published packed root or collected: before=%+v after=%+v GC=%d", before, after, backend.gcCalls)
			}
			// The original writers remain usable after either failure outcome;
			// ordinary writes and a later checkpoint resolve the selected suffix.
			privateCOWPackWrite(t, cache, backend.DB, 5, 100)
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if got, err := cache.Get([]byte("pack-00000")); err != nil || len(got) != 128 || got[0] != 5 {
				t.Fatalf("post-failure writer unusable: %x err=%v", got, err)
			}
		})
	}
}
