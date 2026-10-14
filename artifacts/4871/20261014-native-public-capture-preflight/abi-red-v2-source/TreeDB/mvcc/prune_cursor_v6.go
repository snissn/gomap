package mvcc

import (
 backenddb "github.com/snissn/gomap/TreeDB/db"
 "github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

// PruneCursor retains the original request and its physical preparation across
// bounded public calls. A caller must close a returned cursor before discarding it.
type PruneCursor struct {
 requestCaptured bool
 request mvccadmission.Request
 preparation *backenddb.NativeMVCCCurrentDeleteV6
 loadingFloor, integrityDone bool
}

func (c *PruneCursor) Close() error { return nil }
