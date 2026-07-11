package powerlossoracle

import (
	"fmt"
	"os"

	treedb "github.com/snissn/gomap/TreeDB"
)

// ReopenResult records the public-open evidence for a stable-only crash image.
type ReopenResult struct {
	Dir      string
	ReadOnly bool
	Rejected bool
	Err      error
}

// ReopenStable materializes the model's stable image and passes it through the
// normal public TreeDB Open path. The returned close callback is a no-op when
// Open rejects the image.
func ReopenStable(model *Model, opts treedb.Options, readOnly bool) (ReopenResult, *treedb.DB, func() error, error) {
	dir, err := os.MkdirTemp("", "treedb-powerloss-stable-")
	if err != nil {
		return ReopenResult{}, nil, nil, err
	}
	cleanup := func() error { return os.RemoveAll(dir) }
	if err := model.MaterializeStable(dir); err != nil {
		_ = cleanup()
		return ReopenResult{}, nil, nil, err
	}
	opts.Dir = dir
	opts.ReadOnly = readOnly
	db, openErr := treedb.Open(opts)
	result := ReopenResult{Dir: dir, ReadOnly: readOnly, Rejected: openErr != nil, Err: openErr}
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
		return result, nil, nil, fmt.Errorf("powerlossoracle: public Open returned nil DB and nil error")
	}
	return result, db, closeFn, nil
}
