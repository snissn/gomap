package db

import (
	"context"
	"errors"
	"testing"
)

func TestRecoverableRootSetPreparedMutationRetainsEveryValidation(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("live"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	database.publishPrepareMu.Lock()
	defer database.publishPrepareMu.Unlock()
	if err := set.revalidateWithPublishPrepareLockHeld(); err != nil {
		t.Fatal(err)
	}
	// Same-goroutine ownership makes any blocking mutex acquisition fail the
	// test deterministically, without relying on a scheduling delay.
	database.durablePublishMu.Lock()
	err = set.revalidateWithPublishPrepareLockHeld()
	database.durablePublishMu.Unlock()
	if !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatalf("busy durable authority=%v", err)
	}
	for _, field := range []string{"visible", "durable", "coordinator", "system"} {
		visible, durable, epoch, system := set.visible, set.durable, set.coordinatorEpoch, set.systemRootEpoch
		switch field {
		case "visible":
			set.visible.CommitSeq++
		case "durable":
			set.durable.slotRecord[0].CommitSeq++
		case "coordinator":
			set.coordinatorEpoch++
		case "system":
			set.systemRootEpoch++
		}
		err := set.revalidateWithPublishPrepareLockHeld()
		set.visible, set.durable, set.coordinatorEpoch, set.systemRootEpoch = visible, durable, epoch, system
		if !errors.Is(err, ErrRecoverableRootSetStale) {
			t.Fatalf("lost %s validation: %v", field, err)
		}
		if err := set.revalidateWithPublishPrepareLockHeld(); err != nil {
			t.Fatalf("restored %s: %v", field, err)
		}
	}
}
