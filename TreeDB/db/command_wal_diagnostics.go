package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/snissn/gomap/TreeDB/page"
)

// CommandWALClosedDiagnosticsV1 describes a completed handle and its remaining
// files. It is observation only, never authority to delete or truncate a file.
// Counters are cumulative for this handle; segment classifications describe the
// post-close directory, not the decisions of any particular cleanup attempt.
type CommandWALClosedDiagnosticsV1 struct {
	LastCleanupError       string                          `json:"last_cleanup_error,omitempty"`
	SelectedSlot           uint64                          `json:"selected_slot"`
	Roots                  [2]CommandWALDiagnosticRootV1   `json:"roots"`
	DurableWALLSN          uint64                          `json:"durable_wal_lsn"`
	CleanupThrough         uint64                          `json:"cleanup_through"`
	ProofError             string                          `json:"proof_error,omitempty"`
	ReplayLineageValidated bool                            `json:"replay_lineage_validated"`
	ScanError              string                          `json:"scan_error,omitempty"`
	CleanupCounters        map[string]string               `json:"cleanup_counters"`
	Segments               []CommandWALDiagnosticSegmentV1 `json:"segments"`
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

	// An unavailable proof supplies no replay lower bound. Inventoring the
	// remaining frames must not invent a zero lower bound: earlier legitimate
	// cleanup may already have removed that historical prefix.
	var validProof *durableWALCleanupProofV1
	if proofErr == nil {
		validProof = &proof
	}
	var scanErr error
	out.Segments, scanErr = scanCommandWALDiagnosticInventoryV1(db.dir, db.walMaxSegmentBytes, validProof)
	if scanErr != nil {
		out.ScanError = scanErr.Error()
	}
	out.ReplayLineageValidated = validProof != nil && scanErr == nil
	return out, scanErr
}

// scanCommandWALDiagnosticInventoryV1 owns read-only handles only. Cross-file
// replay validation is permitted solely when a captured proof defines the lower
// and upper bounds; physical/frame inventory does not require that authority.
func scanCommandWALDiagnosticInventoryV1(dir string, maxSegmentBytes int64, proof *durableWALCleanupProofV1) ([]CommandWALDiagnosticSegmentV1, error) {
	segments, err := listWALSegments(dir)
	if err != nil {
		return nil, err
	}
	activeByLane := commandWALActiveSeqByLane(segments)
	var out []CommandWALDiagnosticSegmentV1
	var scanErr error
	options := commandWALSegmentScanOptions{}
	if proof != nil {
		options.seenLSNs = make(map[uint64]struct{})
		options.seenLSNAppliedLSN = proof.cleanupThrough
		options.seenLSNMax = proof.durableWALLSN
	}
	for _, segment := range segments {
		if segment.valueLog || !isCommandWALLaneSegment(segment) {
			continue
		}
		rel, err := filepath.Rel(dir, segment.path)
		if err != nil {
			scanErr = errors.Join(scanErr, err)
			continue
		}
		item := CommandWALDiagnosticSegmentV1{Path: filepath.ToSlash(rel), LogicalBytes: segment.size, ActiveByLane: segment.seq == activeByLane[segment.lane]}
		file, openErr := os.Open(segment.path)
		if openErr != nil {
			item.Error = openErr.Error()
		} else {
			info, statErr := file.Stat()
			if statErr == nil {
				item.PhysicalBytes = info.Size()
			}
			scan, frameErr := scanCommandWALSegmentFileWithOptions(file, maxSegmentBytes, item.ActiveByLane, options)
			item.Frames, item.MinLSN, item.MaxLSN = scan.frames, scan.minLSN, scan.maxLSN
			item.CoveredByRoots = proof != nil && frameErr == nil && (scan.typed || scan.emptyFixed) && scan.maxLSN <= proof.cleanupThrough
			if itemErr := errors.Join(statErr, frameErr, file.Close()); itemErr != nil {
				item.Error = itemErr.Error()
			}
		}
		if item.Error != "" {
			scanErr = errors.Join(scanErr, fmt.Errorf("inventory command WAL segment %s: %s", item.Path, item.Error))
		}
		out = append(out, item)
	}
	if proof != nil {
		scanErr = errors.Join(scanErr, validateCommandWALCleanupReplayLineage(options.seenLSNs, proof.cleanupThrough, proof.durableWALLSN))
	}
	return out, scanErr
}
