package db

import (
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"testing"
)

func TestNativeInstallObserverFiltersAndNeverBlocks(t *testing.T) {
	database := &DB{}
	o, err := NewNativeInstallObserverForTest(database, commitlog.CommandKindCollectionRebuildVectorIndex, 17)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if _, err := NewNativeInstallObserverForTest(database, commitlog.CommandKindCollectionRebuildVectorIndex, 17); err == nil {
		t.Fatal("overlapping registration accepted")
	}
	intent := &CommandWALIntent{}
	intent.inner.kind = commitlog.CommandKindCollectionRebuildVectorIndex
	good := NativePublicationResult{Consumed: true, Assigned: true, LogicalInstalled: true, AssignedLSN: 17, LogicalSequence: 23, WALCompleted: true, WALStable: true}
	bad := []NativePublicationResult{good, good, good, good}
	bad[0].AssignedLSN++
	bad[1].LogicalInstalled = false
	bad[2].Uncertain = true
	bad[3].LogicalSequence = 0
	for _, r := range bad {
		database.ObserveNativeInstalledForTest(intent, r)
	}
	(&DB{}).ObserveNativeInstalledForTest(intent, good)
	wrong := &CommandWALIntent{}
	wrong.inner.kind = commitlog.CommandKindRawKVBatch
	database.ObserveNativeInstalledForTest(wrong, good)
	select {
	case <-o.Events():
		t.Fatal("unqualified installation observed")
	default:
	}
	database.ObserveNativeInstalledForTest(intent, good)
	database.ObserveNativeInstalledForTest(intent, good) // full bounded mailbox must drop, never wait
	if !o.Dropped() {
		t.Fatal("duplicate/full observation did not invalidate witness")
	}
	got := <-o.Events()
	if got.AssignedLSN != 17 || got.LogicalSequence != 23 || !got.WALStable || !got.WALCompleted {
		t.Fatalf("receipt changed: %+v", got)
	}
	o.Close()
	database.ObserveNativeInstalledForTest(intent, good)
	select {
	case <-o.Events():
		t.Fatal("closed registration rediscovered")
	default:
	}
}
