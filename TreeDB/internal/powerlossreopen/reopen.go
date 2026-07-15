// Package powerlossreopen materializes power-loss oracle images and reopens
// them through the normal public TreeDB API.
package powerlossreopen

import (
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
	cleanup := func() error { return os.RemoveAll(dir) }
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
