package commitlog

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestJournalDistinctSealedSyncKeepsFirstPhysicalFailure(t *testing.T) {
	var notifications atomic.Int32
	owner := journalFileSync{onError: func() { notifications.Add(1) }}
	releaseSealed, releaseActive := make(chan struct{}), make(chan struct{})
	unblockSealed, unblockActive := sync.OnceFunc(func() { close(releaseSealed) }), sync.OnceFunc(func() { close(releaseActive) })
	defer unblockSealed()
	defer unblockActive()
	sealedEntered, activeEntered := make(chan struct{}), make(chan struct{})
	sealedErr, activeErr := make(chan error, 1), make(chan error, 1)

	go func() {
		sealedErr <- owner.syncDistinctFile(func() error {
			close(sealedEntered)
			<-releaseSealed
			return syscall.EIO
		})
	}()
	<-sealedEntered
	go func() {
		activeErr <- owner.run(func() error {
			close(activeEntered)
			<-releaseActive
			return syscall.ENOSPC
		})
	}()
	select {
	case <-activeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("sealed fsync serialized the unrelated active sync")
	}
	unblockSealed()
	if err := <-sealedErr; !errors.Is(err, syscall.EIO) {
		t.Fatalf("first physical failure=%v, want EIO", err)
	}
	unblockActive()
	if err := <-activeErr; !errors.Is(err, syscall.EIO) {
		t.Fatalf("later physical failure replaced the first cause: %v", err)
	}
	if err := owner.run(func() error {
		t.Error("sticky error allowed a second physical attempt")
		return nil
	}); !errors.Is(err, syscall.EIO) || notifications.Load() != 1 {
		t.Fatalf("after failure: err=%v notifications=%d", err, notifications.Load())
	}
}
