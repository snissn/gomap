package collections

import (
	"context"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedGraphColdManifestScanCancellation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// This preflight counts every inline record, even unknown keys. More than
	// two scan intervals distinguish periodic checks from the final Err check.
	records := make([]columnManifestRecord, 2*columnAssetReachabilityContextCheckInterval+1)
	for i := range records {
		records[i] = columnManifestRecord{key: []byte(fmt.Sprintf("z%06d", i)), value: []byte("inline")}
	}
	root := publishColumnManifestRecordsForScanTestM13A(t, db, ColumnManifestIdentity{Generation: 1}, records)
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer snap.Close()
	limits := typedGraphColdLimits{ManifestRecords: len(records) + 1, ManifestBytes: 1 << 20}
	if err := validateTypedGraphColdManifestBudget(context.Background(), snap, root, limits); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := &columnAssetDiscoveryCheckContext{Context: ctx, at: 2, check: cancel}
	if err := validateTypedGraphColdManifestBudget(checks, snap, root, limits); !errors.Is(err, context.Canceled) || checks.calls != 2 {
		t.Fatalf("mid-scan cancellation: err=%v checks=%d want2", err, checks.calls)
	}
	// Preserve the established admission error translation.
	limits.ManifestRecords = 1
	if err := validateTypedGraphColdManifestBudget(context.Background(), snap, root, limits); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("manifest limit translation: %v", err)
	}
}
