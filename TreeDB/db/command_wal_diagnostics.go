package db

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/snissn/gomap/TreeDB/page"
)

// CommandWALClosedDiagnosticsV1 describes a completed handle and its remaining
// files. It is observation only, never authority to delete or truncate a file.
// Counters are cumulative for this handle; segment classifications describe the
// post-close directory, not the decisions of any particular cleanup attempt.
type CommandWALClosedDiagnosticsV1 struct {
	LastCleanupError string                          `json:"last_cleanup_error,omitempty"`
	SelectedSlot     uint64                          `json:"selected_slot"`
	Roots            [2]CommandWALDiagnosticRootV1   `json:"roots"`
	DurableWALLSN    uint64                          `json:"durable_wal_lsn"`
	CleanupThrough   uint64                          `json:"cleanup_through"`
	ProofError       string                          `json:"proof_error,omitempty"`
	ScanError        string                          `json:"scan_error,omitempty"`
	CleanupCounters  map[string]string               `json:"cleanup_counters"`
	Segments         []CommandWALDiagnosticSegmentV1 `json:"segments"`
}

type CommandWALDiagnosticRootV1 struct {
	Slot              uint64             `json:"slot"`
	CommitSeq         uint64             `json:"commit_seq"`
	AppliedCommandLSN uint64             `json:"applied_command_lsn"`
	Meta              page.DurableMetaV1 `json:"meta"`
}

type CommandWALDiagnosticSegmentV1 struct {
	Path           string `json:"path"`
	PhysicalBytes  int64  `json:"physical_bytes"`
	LogicalBytes   int64  `json:"logical_bytes"`
	Frames         uint64 `json:"frames"`
	MinLSN         uint64 `json:"min_lsn"`
	MaxLSN         uint64 `json:"max_lsn"`
	ActiveByLane   bool   `json:"active_by_lane"`
	CoveredByRoots bool   `json:"covered_by_roots"`
	Error          string `json:"error,omitempty"`
}

// InspectClosedCommandWALV1 scans without opening a new DB owner or changing
// recovery/cleanup state. Call after Close has returned, and before any other
// owner opens or modifies this directory. A failed Close may still have finished
// teardown. ProofError records legitimate conservative retention; ScanError and
// the returned error indicate an incomplete diagnostic, with partial data kept.
func (db *DB) InspectClosedCommandWALV1() (CommandWALClosedDiagnosticsV1, error) {
	var out CommandWALClosedDiagnosticsV1
	if db == nil || !db.closeDiagnosticsReady.Load() {
		return out, errors.New("command WAL diagnostics require completed Close")
	}
	if message := db.commandWALCleanupLastError.Load(); message != nil {
		out.LastCleanupError = *message
	}
	runtime := db.durableRoot
	out.SelectedSlot = runtime.slot
	out.DurableWALLSN = db.commandWALDurableLSN.Load()
	for slot := range out.Roots {
		out.Roots[slot] = CommandWALDiagnosticRootV1{Slot: uint64(slot), CommitSeq: runtime.slotCommit[slot], AppliedCommandLSN: runtime.slotRecord[slot].AppliedCommandLSN, Meta: runtime.slotMeta[slot]}
	}
	proof, proofErr := captureDurableWALCleanupProofFromRuntimeV1(runtime, out.DurableWALLSN)
	if proofErr != nil {
		out.ProofError = proofErr.Error()
	} else {
		out.CleanupThrough = proof.cleanupThrough
	}
	stats := make(map[string]string)
	// This helper reads atomics and terminal handle state; it never scans files.
	writeCommandWALLifecycleStats(stats, db)
	out.CleanupCounters = make(map[string]string)
	for key, value := range stats {
		if strings.HasPrefix(key, "treedb.command_wal.cleanup.") {
			out.CleanupCounters[key] = value
		}
	}
	decisions, scanErr := scanCommandWALSegmentsForCleanupProof(db.dir, out.CleanupThrough, out.DurableWALLSN, db.walMaxSegmentBytes)
	defer closeCommandWALCleanupDecisions(decisions)
	for _, decision := range decisions {
		rel, err := filepath.Rel(db.dir, decision.Path)
		if err != nil {
			return out, err
		}
		out.Segments = append(out.Segments, CommandWALDiagnosticSegmentV1{Path: filepath.ToSlash(rel), PhysicalBytes: decision.Size, LogicalBytes: decision.LogicalSize, Frames: decision.Frames, MinLSN: decision.MinLSN, MaxLSN: decision.MaxLSN, ActiveByLane: decision.Active, CoveredByRoots: proofErr == nil && decision.Covered, Error: decision.Error})
	}
	if scanErr != nil {
		out.ScanError = scanErr.Error()
	}
	return out, scanErr
}
