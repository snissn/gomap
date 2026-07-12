// Package powerlossreopen materializes power-loss oracle images and reopens
// them through the normal public TreeDB API.
package powerlossreopen

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

// Result records the public-open evidence for a stable-only crash image.
type Result struct {
	Dir        string
	ReadOnly   bool
	Rejected   bool
	Err        error
	CommitSeq  uint64
	AppliedLSN uint64
	// RecoveryDiagnostic is populated for both writable repair and typed
	// read-only recovery-required errors.
	RecoveryDiagnostic treedb.CommandWALRecoveryDiagnostic
}

// Stable materializes the model's stable image and passes it through the
// normal public TreeDB Open path. The returned close callback is a no-op when
// Open rejects the image.
func Stable(model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	dir, err := os.MkdirTemp("", "treedb-powerloss-stable-")
	if err != nil {
		return Result{}, nil, nil, err
	}
	cleanup := func() error { return os.RemoveAll(dir) }
	if err := model.MaterializeStable(dir); err != nil {
		_ = cleanup()
		return Result{}, nil, nil, err
	}
	opts.Dir = dir
	opts.ReadOnly = readOnly
	db, openErr := treedb.Open(opts)
	result := Result{Dir: dir, ReadOnly: readOnly, Rejected: openErr != nil, Err: openErr}
	if db != nil {
		stats := db.Stats()
		result.CommitSeq, _ = strconv.ParseUint(stats["treedb.commit_seq"], 10, 64)
		result.AppliedLSN, _ = strconv.ParseUint(stats["treedb.applied_command_lsn"], 10, 64)
		result.RecoveryDiagnostic.FirstDiscardedLSN, _ = strconv.ParseUint(stats["treedb.command_wal.recovery.first_discarded_lsn"], 10, 64)
		result.RecoveryDiagnostic.DiscardedFrameCount, _ = strconv.ParseUint(stats["treedb.command_wal.recovery.discarded_frames"], 10, 64)
		result.RecoveryDiagnostic.DiscardedBytes, _ = strconv.ParseUint(stats["treedb.command_wal.recovery.discarded_bytes"], 10, 64)
		result.RecoveryDiagnostic.MissingRIDCount, _ = strconv.ParseUint(stats["treedb.command_wal.recovery.missing_rids"], 10, 64)
		result.RecoveryDiagnostic.SourceSegment = stats["treedb.command_wal.recovery.source_segment"]
		result.RecoveryDiagnostic.DurabilityClass = uint16(parseUintOrZero(stats["treedb.command_wal.recovery.durability_class"]))
		result.RecoveryDiagnostic.TruncationCompleted, _ = strconv.ParseBool(stats["treedb.command_wal.recovery.truncation_completed"])
		result.RecoveryDiagnostic.DirectorySyncCompleted, _ = strconv.ParseBool(stats["treedb.command_wal.recovery.directory_sync_completed"])
	} else {
		var recoveryErr *treedb.CommandWALRecoveryRequiredError
		if errors.As(openErr, &recoveryErr) {
			result.RecoveryDiagnostic = recoveryErr.Diagnostic
		}
	}
	closeFn := func() error {
		var closeErr error
		if db != nil {
			closeErr = db.Close()
		}
		cleanupErr := cleanup()
		if closeErr != nil {
			return closeErr
		}
		return cleanupErr
	}
	if db == nil && openErr == nil {
		_ = cleanup()
		return result, nil, nil, fmt.Errorf("powerlossreopen: public Open returned nil DB and nil error")
	}
	return result, db, closeFn, nil
}

func parseUintOrZero(value string) uint64 {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}
