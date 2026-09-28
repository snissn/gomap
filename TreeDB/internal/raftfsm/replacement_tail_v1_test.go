package raftfsm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestReplacementTailProgressRequiresExactDurableSemanticBoundaryV1(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db := openFSMTestDB(t, dir)
	defer db.Close()
	f := openFSMForTest(t, db, dir)
	defer f.Close()
	users := deterministicCreateCollectionEntry(t, "users", "tail:users")
	for i, raw := range [][]byte{users, deterministicCreateCollectionEntry(t, "orders", "tail:orders"), users} {
		if _, err := f.ApplyCommittedEntryV1(committedCommand(9, uint64(i+1), raw)); err != nil {
			t.Fatal(err)
		}
	}
	id := raftentry.ApplyEntryID{Term: 9, Index: 3}
	proof, err := f.ReplacementTailProgressV1(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if proof.ProgressDigest == proof.Result.ResultDigest {
		t.Fatal("duplicate fixture lost distinct progress digest")
	}
	if _, err := f.ApplyCommittedEntryV1(committedCommand(9, 4, deterministicCreateCollectionEntry(t, "audit", "tail:audit"))); err != nil {
		t.Fatal(err)
	}
	advanced, err := f.ReplacementTailProgressV1(context.Background(), id)
	if err != nil || advanced != proof {
		t.Fatalf("later command changed immutable tail: %+v %v", advanced, err)
	}
	for _, missing := range []raftentry.ApplyEntryID{{Term: 8, Index: 3}, {Term: 9, Index: 5}, {}} {
		if _, err := f.ReplacementTailProgressV1(context.Background(), missing); !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
			t.Fatalf("unproved boundary %+v: %v", missing, err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.ReplacementTailProgressV1(canceled, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	original := f.results
	record, ok, err := original.LookupApplyResult(id)
	if err != nil || !ok {
		t.Fatal(err)
	}
	for _, kind := range []string{"missing", "command", "progress", "lsn"} {
		t.Run(kind, func(t *testing.T) {
			resultStore, err := raftapply.OpenDurableApplyResultStore(t.TempDir(), raftapply.DurableApplyStoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer resultStore.Close()
			if kind != "missing" {
				changed := record
				switch kind {
				case "command":
					changed.CommandDigest[0] ^= 1
					changed.Result.CommandDigest = changed.CommandDigest
				case "progress":
					changed.ProgressLogicalDigestV1[0] ^= 1
				case "lsn":
					changed.AppliedCommandLSN++
				}
				if err := resultStore.RecordApplyResult(changed); err != nil {
					t.Fatal(err)
				}
			}
			f.results = resultStore
			defer func() { f.results = original }()
			if _, err := f.ReplacementTailProgressV1(context.Background(), id); !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
				t.Fatalf("unproved result: %v", err)
			}
		})
	}
	// A local mutation beyond progress is not a durable Raft tail even when an
	// earlier result and progress pair still match each other.
	entry := committedCommand(9, 5, deterministicCreateCollectionEntry(t, "uncovered", "tail:uncovered"))
	meta, err := f.applyMetadata(entry, raftentry.ApplyEntryID{Term: 9, Index: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raftapply.ApplyCommittedEntryV1(db, entry.Bytes, meta, raftapply.Options{DecodeLimits: f.decodeLimits}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReplacementTailProgressV1(context.Background(), id); !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("uncovered local WAL accepted: %v", err)
	}
}
