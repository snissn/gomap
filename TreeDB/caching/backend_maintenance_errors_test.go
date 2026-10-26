package caching

import (
	"errors"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type maintenanceFailureBackend struct {
	*backenddb.DB
	failure                  error
	refreshes, failRefreshAt int
	failCapture              bool
}

func (backend *maintenanceFailureBackend) RefreshValueLogSet() error {
	backend.refreshes++
	if backend.refreshes == backend.failRefreshAt {
		return backend.failure
	}
	return backend.DB.RefreshValueLogSet()
}

func (backend *maintenanceFailureBackend) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	if backend.failCapture {
		return nil, backend.failure
	}
	return backend.DB.AcquireSnapshotWithAllocationAdmission(admit)
}

func TestCOWBackendMaintenanceErrorsIdentifyOwnershipPhase(t *testing.T) {
	for _, phase := range []string{"refresh backend value-log set before callback", "callback", "refresh backend value-log set after callback", "capture COW basis"} {
		t.Run(phase, func(t *testing.T) {
			cache := cowPointReadFixture(t)
			original := cache.backend.(*backenddb.DB)
			failure := &os.PathError{Op: "maintenance probe", Path: "original resource", Err: errors.New("refused")}
			backend := &maintenanceFailureBackend{DB: original, failure: failure}
			switch phase {
			case "refresh backend value-log set before callback":
				backend.failRefreshAt = 1
			case "refresh backend value-log set after callback":
				backend.failRefreshAt = 2
			case "capture COW basis":
				backend.failCapture = true
			}
			cache.backend = backend
			t.Cleanup(func() { cache.backend = original })
			called := false
			err := cache.RunBackendMaintenance(func() error {
				called = true
				if phase == "callback" {
					return failure
				}
				return nil
			})
			var physical *os.PathError
			if !errors.Is(err, failure) || !errors.As(err, &physical) || physical != failure || !strings.Contains(err.Error(), "backend maintenance "+phase+":") {
				t.Fatalf("maintenance lost physical cause or phase: %v", err)
			}
			if called != (phase != "refresh backend value-log set before callback") {
				t.Fatalf("callback ran across failed precondition: %v", called)
			}
		})
	}
}
