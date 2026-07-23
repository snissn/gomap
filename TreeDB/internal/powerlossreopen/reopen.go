// Package powerlossreopen materializes power-loss oracle images and reopens
// them through the normal public TreeDB API.
package powerlossreopen

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	Stats      map[string]string
}

var recoveryStatKeys = []string{
	"treedb.profile.resolved",
	"treedb.commit_seq",
	"treedb.applied_command_lsn",
	"treedb.durable_root.selected_slot",
	"treedb.durable_root.commit_seq",
	"treedb.durable_root.durable_seq",
	"treedb.durable_root.freelist.generation",
	"treedb.durable_root.manifest.entries",
	"treedb.durable_root.slot0.commit_seq",
	"treedb.durable_root.slot1.commit_seq",
	"treedb.command_wal.durable_wal_lsn",
}

// Stable materializes the model's stable image and passes it through the
// normal public TreeDB Open path. The returned close callback releases the
// modeled identity scope and removes the materialized image whether Open
// accepts or rejects it.
func Stable(model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	return stableRelative(model, ".", opts, readOnly)
}

// StableChild materializes the model's stable parent image and reopens a
// relative child through the normal public TreeDB API. This is required for a
// fresh database root whose directory entry itself is part of the modeled
// namespace rather than an already-existing materialization root.
func StableChild(model *powerlossoracle.Model, relativeDir string, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	relativeDir, err := cleanRelativeDir(relativeDir)
	if err != nil {
		return Result{}, nil, nil, err
	}
	return stableRelative(model, relativeDir, opts, readOnly)
}

func stableRelative(model *powerlossoracle.Model, relativeDir string, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	evidence, err := powerlossoracle.BeginEvidenceFromEnv(model, readOnly)
	if err != nil {
		return Result{}, nil, nil, err
	}
	if evidence != nil {
		materializationRoot := evidence.RecoveryInputDir()
		openDir := filepath.Join(materializationRoot, relativeDir)
		result, db, closeFn, err := openAt(materializationRoot, openDir, model, opts, readOnly, false)
		if err != nil {
			return Result{}, nil, nil, err
		}
		recovery := struct {
			SchemaVersion      string            `json:"schema_version"`
			PublicAPI          string            `json:"public_api"`
			Dir                string            `json:"dir"`
			PreOpenSnapshotDir string            `json:"pre_open_snapshot_dir"`
			InputTreeSHA256    string            `json:"input_image_tree_sha256"`
			StableFingerprint  string            `json:"stable_fingerprint"`
			ReadOnly           bool              `json:"read_only"`
			Rejected           bool              `json:"rejected"`
			ErrorType          string            `json:"error_type"`
			Error              string            `json:"error"`
			CommitSeq          uint64            `json:"commit_seq"`
			AppliedLSN         uint64            `json:"applied_lsn"`
			Stats              map[string]string `json:"stats"`
		}{
			SchemaVersion: "treedb-power-loss-recovery-trace/v2",
			PublicAPI:     "treedb.Open",
			Dir:           filepath.ToSlash(filepath.Join("recovery-input", relativeDir)),
			// The immutable snapshot and its tree hash cover the whole modeled
			// parent. Dir may name an absent child within that parent.
			PreOpenSnapshotDir: "recovery-preopen",
			InputTreeSHA256:    evidence.StableImageTreeSHA256(),
			StableFingerprint:  evidence.StableFingerprint(),
			ReadOnly:           result.ReadOnly,
			Rejected:           result.Rejected,
			CommitSeq:          result.CommitSeq,
			AppliedLSN:         result.AppliedLSN,
			Stats:              result.Stats,
		}
		if result.Err != nil {
			recovery.ErrorType = fmt.Sprintf("%T", result.Err)
			recovery.Error = result.Err.Error()
		}
		if err := evidence.RecordRecovery(recovery); err != nil {
			_ = closeFn()
			return Result{}, nil, nil, err
		}
		return result, db, closeFn, nil
	}
	dir, err := os.MkdirTemp("", "treedb-powerloss-stable-")
	if err != nil {
		return Result{}, nil, nil, err
	}
	return stableRelativeAt(dir, relativeDir, model, opts, readOnly, true)
}

// StableAt materializes the stable-only crash image at a caller-owned path and
// reopens it through the normal public API. The close callback releases the DB
// and modeled identity scope but deliberately preserves dir for evidence.
func StableAt(dir string, model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	if err := requireEmptyDestination(dir); err != nil {
		return Result{}, nil, nil, err
	}
	return stableRelativeAt(dir, ".", model, opts, readOnly, false)
}

// StableChildAt is StableChild with a caller-owned materialization root. The
// close callback preserves root for inspection.
func StableChildAt(root, relativeDir string, model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	if err := requireEmptyDestination(root); err != nil {
		return Result{}, nil, nil, err
	}
	relativeDir, err := cleanRelativeDir(relativeDir)
	if err != nil {
		return Result{}, nil, nil, err
	}
	return stableRelativeAt(root, relativeDir, model, opts, readOnly, false)
}

func stableRelativeAt(root, relativeDir string, model *powerlossoracle.Model, opts treedb.Options, readOnly, removeOnClose bool) (Result, *treedb.DB, func() error, error) {
	if err := model.MaterializeStable(root); err != nil {
		if removeOnClose {
			_ = os.RemoveAll(root)
		}
		return Result{}, nil, nil, err
	}
	return openAt(root, filepath.Join(root, relativeDir), model, opts, readOnly, removeOnClose)
}

func openAt(materializationRoot, dir string, model *powerlossoracle.Model, opts treedb.Options, readOnly, removeOnClose bool) (Result, *treedb.DB, func() error, error) {
	cleanup := func() error {
		if removeOnClose {
			return os.RemoveAll(materializationRoot)
		}
		return nil
	}
	releaseIdentities, err := model.InstallStableIdentityOverrides(materializationRoot)
	if err != nil {
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
		result.Stats = make(map[string]string, len(recoveryStatKeys))
		for _, key := range recoveryStatKeys {
			result.Stats[key] = stats[key]
		}
	}
	closeFn := func() error {
		var closeErr error
		if db != nil {
			closeErr = db.Close()
		}
		releaseIdentities()
		cleanupErr := cleanup()
		if closeErr != nil {
			return closeErr
		}
		return cleanupErr
	}
	if db == nil && openErr == nil {
		releaseIdentities()
		_ = cleanup()
		return result, nil, nil, fmt.Errorf("powerlossreopen: public Open returned nil DB and nil error")
	}
	return result, db, closeFn, nil
}

func cleanRelativeDir(relativeDir string) (string, error) {
	if relativeDir == "" || filepath.IsAbs(relativeDir) || filepath.VolumeName(relativeDir) != "" {
		return "", fmt.Errorf("powerlossreopen: child directory %q must be a non-empty relative path", relativeDir)
	}
	clean := filepath.Clean(relativeDir)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("powerlossreopen: child directory %q escapes the materialized root", relativeDir)
	}
	return clean, nil
}

func requireEmptyDestination(dir string) error {
	if !filepath.IsAbs(dir) {
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return fmt.Errorf("powerlossreopen: resolve destination %q: %w", dir, err)
		}
		dir = absolute
	}
	if err := powerlossoracle.EnsureNoSymlinkComponents(dir); err != nil {
		return fmt.Errorf("powerlossreopen: inspect destination %q: %w", dir, err)
	}
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("powerlossreopen: inspect destination %q: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("powerlossreopen: destination %q is not a directory", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("powerlossreopen: inspect destination %q: %w", dir, err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("powerlossreopen: destination %q is not empty", dir)
	}
	return nil
}
