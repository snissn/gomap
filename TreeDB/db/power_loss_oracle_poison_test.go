package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPowerLossOraclePostMetaFailurePoisonsPublicHandle(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}

	cutErr := errors.New("power-loss-oracle: stop after meta dirty")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterMetaWrite {
			return cutErr
		}
		return nil
	})
	err = d.SetSync([]byte("dirty/new"), []byte("new-value"))
	restore()
	if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("post-meta write error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if err := d.SetSync([]byte("after/poison"), []byte("must-reopen")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("public SetSync after post-meta failure error=%v, want ErrRecoveryRequired", err)
	}
}
