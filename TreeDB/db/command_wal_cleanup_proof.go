package db

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	errDurableWALCleanupProofUnavailable = errors.New("durable WAL cleanup proof unavailable")
	// ErrDurableWALCleanupProofStale reports that command-WAL cleanup authority
	// changed before deletion. Callers may retry from a fresh checkpoint cut.
	ErrDurableWALCleanupProofStale = errors.New("durable WAL cleanup proof stale")
	errDurableWALCleanupProofStale = ErrDurableWALCleanupProofStale
)

type durableWALCleanupRootV1 struct {
	slot   uint64
	meta   page.DurableMetaV1
	record rootpublication.DurableRootRecordV1
}

// durableWALCleanupProofV1 is backend-private deletion authority. It freezes
// every independently recovery-selectable root and the dependency-closed WAL
// frontier observed at the same durable-root publication cut. Callers must
// revalidate it immediately before unlinking any segment.
type durableWALCleanupProofV1 struct {
	selectedSlot   uint64
	roots          [2]durableWALCleanupRootV1
	rootCount      int
	cleanupThrough uint64
	durableWALLSN  uint64
	journalOwner   *commitlog.CommandJournal
	journal        commitlog.CommandJournalCleanupSnapshot
	segments       []commandWALSegmentCleanupDecision
}

func (db *DB) captureDurableWALCleanupProofV1() (durableWALCleanupProofV1, error) {
	if db == nil {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: nil DB", errDurableWALCleanupProofUnavailable)
	}
	journal := db.commandJournal
	if journal == nil {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: command journal owner is not live", errDurableWALCleanupProofUnavailable)
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	proof, err := captureDurableWALCleanupProofFromRuntimeV1(db.durableRoot, db.commandWALDurableLSN.Load())
	if err != nil {
		return durableWALCleanupProofV1{}, err
	}
	if db.commandJournal != journal {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: command journal owner changed during capture", errDurableWALCleanupProofUnavailable)
	}
	proof.journalOwner = journal
	proof.journal, err = journal.CaptureCleanupSnapshot()
	if err != nil {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: capture journal namespace: %v", errDurableWALCleanupProofUnavailable, err)
	}
	if err := validateCommandJournalCleanupSnapshotV1(proof.journal); err != nil {
		return durableWALCleanupProofV1{}, err
	}
	return proof, nil
}

func validateCommandJournalCleanupSnapshotV1(snapshot commitlog.CommandJournalCleanupSnapshot) error {
	if snapshot.PendingStableRotation != 0 || snapshot.PendingSuccessor {
		return fmt.Errorf("%w: command WAL rotation/retry ownership is pending", errDurableWALCleanupProofUnavailable)
	}
	return nil
}

func (proof durableWALCleanupProofV1) selectedCommitSeq() uint64 {
	for i := 0; i < proof.rootCount; i++ {
		if proof.roots[i].slot == proof.selectedSlot {
			return proof.roots[i].record.CommitSeq
		}
	}
	return 0
}

func (proof durableWALCleanupProofV1) olderCommitSeq() uint64 {
	for i := 0; i < proof.rootCount; i++ {
		if proof.roots[i].slot != proof.selectedSlot {
			return proof.roots[i].record.CommitSeq
		}
	}
	return 0
}

func captureDurableWALCleanupProofFromRuntimeV1(runtime durableRootRuntimeV1, durableWALLSN uint64) (durableWALCleanupProofV1, error) {
	proof := durableWALCleanupProofV1{selectedSlot: runtime.slot, durableWALLSN: durableWALLSN}
	if runtime.slot > 1 {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: selected slot %d", errDurableWALCleanupProofUnavailable, runtime.slot)
	}
	if runtime.pending != nil {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: durable-root publication retry is pending", errDurableWALCleanupProofUnavailable)
	}
	if len(runtime.ambiguous) != 0 {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: durable-root publication outcome is ambiguous", errDurableWALCleanupProofUnavailable)
	}
	for slot := uint64(0); slot < 2; slot++ {
		commitSeq := runtime.slotCommit[slot]
		if commitSeq == 0 {
			continue
		}
		record := runtime.slotRecord[slot]
		meta := runtime.slotMeta[slot]
		if record.CommitSeq != commitSeq || meta.CommitSeq != commitSeq {
			return durableWALCleanupProofV1{}, fmt.Errorf("%w: slot %d commit identity mismatch", errDurableWALCleanupProofUnavailable, slot)
		}
		if durableWALLSN < record.AppliedCommandLSN {
			return durableWALCleanupProofV1{}, fmt.Errorf("%w: durable WAL LSN %d is behind slot %d applied LSN %d", errDurableWALCleanupProofUnavailable, durableWALLSN, slot, record.AppliedCommandLSN)
		}
		proof.roots[proof.rootCount] = durableWALCleanupRootV1{slot: slot, meta: meta, record: record}
		proof.rootCount++
		if proof.rootCount == 1 || record.AppliedCommandLSN < proof.cleanupThrough {
			proof.cleanupThrough = record.AppliedCommandLSN
		}
	}
	if proof.rootCount == 0 {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: no recovery-selectable roots", errDurableWALCleanupProofUnavailable)
	}
	selectedCommit := runtime.slotCommit[runtime.slot]
	if selectedCommit == 0 || runtime.record.CommitSeq != selectedCommit || runtime.slotRecord[runtime.slot] != runtime.record || runtime.slotMeta[runtime.slot] != runtime.meta {
		return durableWALCleanupProofV1{}, fmt.Errorf("%w: selected root identity mismatch", errDurableWALCleanupProofUnavailable)
	}
	return proof, nil
}

func (db *DB) revalidateDurableWALCleanupProofV1(proof durableWALCleanupProofV1) error {
	if db == nil {
		return fmt.Errorf("%w: nil DB", errDurableWALCleanupProofStale)
	}
	if proof.journalOwner == nil || db.commandJournal != proof.journalOwner {
		return fmt.Errorf("%w: command journal owner changed", errDurableWALCleanupProofStale)
	}
	current, err := captureDurableWALCleanupProofFromRuntimeV1(db.durableRoot, db.commandWALDurableLSN.Load())
	if err != nil {
		return errors.Join(errDurableWALCleanupProofStale, err)
	}
	if current.cleanupThrough < proof.cleanupThrough {
		return fmt.Errorf("%w: cleanup frontier regressed from %d to %d", errDurableWALCleanupProofStale, proof.cleanupThrough, current.cleanupThrough)
	}
	if current.durableWALLSN < proof.durableWALLSN {
		return fmt.Errorf("%w: durable WAL LSN regressed from %d to %d", errDurableWALCleanupProofStale, proof.durableWALLSN, current.durableWALLSN)
	}
	if db.commandJournal != proof.journalOwner {
		return fmt.Errorf("%w: command journal owner changed during revalidation", errDurableWALCleanupProofStale)
	}
	return nil
}
