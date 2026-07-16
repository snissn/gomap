// Package powerlossreopen materializes power-loss oracle images and reopens
// them through the normal public TreeDB API.
package powerlossreopen

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
}

// Stable materializes the model's stable image and passes it through the
// normal public TreeDB Open path. The returned close callback releases the
// modeled identity scope and removes the materialized image whether Open
// accepts or rejects it.
func Stable(model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	dir, err := os.MkdirTemp("", "treedb-powerloss-stable-")
	if err != nil {
		return Result{}, nil, nil, err
	}
	return stableAt(dir, model, opts, readOnly, true)
}

// StableAt materializes the stable-only crash image at a caller-owned path and
// reopens it through the normal public API. The close callback releases the DB
// and modeled identity scope but deliberately preserves dir for evidence.
func StableAt(dir string, model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (Result, *treedb.DB, func() error, error) {
	if err := requireEmptyDestination(dir); err != nil {
		return Result{}, nil, nil, err
	}
	return stableAt(dir, model, opts, readOnly, false)
}

func stableAt(dir string, model *powerlossoracle.Model, opts treedb.Options, readOnly, removeOnClose bool) (Result, *treedb.DB, func() error, error) {
	cleanup := func() error {
		if removeOnClose {
			return os.RemoveAll(dir)
		}
		return nil
	}
	if err := model.MaterializeStable(dir); err != nil {
		_ = cleanup()
		return Result{}, nil, nil, err
	}
	releaseIdentities, err := model.InstallStableIdentityOverrides(dir)
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

func requireEmptyDestination(dir string) error {
	if !filepath.IsAbs(dir) {
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return fmt.Errorf("powerlossreopen: resolve destination %q: %w", dir, err)
		}
		dir = absolute
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("powerlossreopen: inspect destination %q: %w", dir, err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("powerlossreopen: destination %q is not empty", dir)
	}
	return nil
}
