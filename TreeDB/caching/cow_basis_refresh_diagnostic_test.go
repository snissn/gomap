package caching

import (
	"errors"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCOWBasisRefreshDiagnosticLabelsPreserveCause(t *testing.T) {
	for _, side := range []string{"live", "frozen"} {
		t.Run(side, func(t *testing.T) {
			db, c := cowCutFixture(t)
			db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
			original := newTestSemanticState(nil)
			cause := &os.PathError{Op: "diagnostic-control", Path: "no-io", Err: backenddb.ErrRootPublicationBasisMismatch}
			failing := &testBasisRefreshState{testSemanticState: original, failure: cause}
			if side == "live" {
				c.cut.semanticRootSet = failing
			} else {
				c.cut.frozenSemanticRootSet = failing
			}
			opening := c.cut
			basis, err := c.captureBackendBasis(db.backend.(*backenddb.DB))
			if err != nil {
				t.Fatal(err)
			}
			old, err := db.installCOWBasis(basis, nil)
			var typed *os.PathError
			if old != nil || c.cut != opening || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) || !errors.As(err, &typed) || typed != cause || errors.Unwrap(err) != cause || !strings.Contains(err.Error(), "COW basis-refresh state="+side+":") {
				t.Errorf("original outcome/side/cause changed: old=%p err=%v", old, err)
			}
			if failing.before != opening.basis.snapshot || failing.after != basis.snapshot || original.refs.Load() != 1 {
				t.Error("annotation changed original snapshot pair/opening custody")
			}
			failing.before, failing.after = nil, nil
			if err := basis.release(); err != nil {
				t.Error(err)
			}
			if err := db.backend.(*backenddb.DB).CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
				t.Errorf("original poison fence lost: %v", err)
			}
			if err := c.close(); err != nil {
				t.Error(err)
			}
			db.drainSemanticCleanup()
			if original.calls != 1 || original.resource != nil {
				t.Error("original opening cleanup incomplete")
			}
		})
	}
}
