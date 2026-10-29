//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
)

// The optional capability is asserted only after the original APIs have read a
// real packed dictionary leaf. This test compiles on the causal predecessor.
func TestCOWNativeIdleDictionaryDecoderLifetime(t *testing.T) {
	db, backend, ptr, dictID, _ := cowNativePackedDictionaryLeafFixture(t)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Close()
	reader := capture.(*NativeSemanticCapture)
	read := func() []byte {
		t.Helper()
		var dst [page.PageSize]byte
		got, err := reader.NativeLeafPage(ptr, dst[:])
		if err != nil {
			t.Fatal(err)
		}
		return append([]byte(nil), got...)
	}
	owned := read()
	budget := db.cow.budget
	db.cow.physicalMu.Lock()
	shared := db.cow.physicalReader
	var codec *cowReadCodec
	for _, c := range shared.codecs {
		if c != nil && c.id == dictID {
			codec = c
		}
	}
	db.cow.physicalMu.Unlock()
	if codec == nil || codec.definition == nil || codec.lease == nil {
		t.Fatal("real dictionary decoder was not retained")
	}
	receipt := codec.lease
	trim, ok := capture.(interface{ TryReleaseIdleNativeReadScratch() bool })
	if !ok {
		t.Fatal("original capture has no idle scratch capability")
	}
	// A busy borrower must be left intact without waiting on its read lock.
	db.cow.physicalMu.Lock()
	done := make(chan bool, 1)
	go func() { done <- trim.TryReleaseIdleNativeReadScratch() }()
	select {
	case released := <-done:
		if released {
			db.cow.physicalMu.Unlock()
			t.Fatal("busy decoder was reclaimed")
		}
	case <-time.After(2 * time.Second):
		db.cow.physicalMu.Unlock()
		<-done
		t.Fatal("idle scratch operation waited for busy borrower")
	}
	if codec.decoder == nil || codec.lease != receipt {
		db.cow.physicalMu.Unlock()
		t.Fatal("busy decoder custody changed")
	}
	db.cow.physicalMu.Unlock()
	before := budget.Stats()
	// This is the original finite budget and an actual lease request, including
	// its original header. The live decoder makes this exact request refuse.
	target := budget.Limits().MaxInFlightBytes - before.ReservedBytes
	if lease, err := budget.AcquireExternal(target); !errors.Is(err, memtable.ErrCOWCapacity) {
		lease.Close()
		t.Fatalf("expected actual retained-owner refusal: %v", err)
	}
	if !trim.TryReleaseIdleNativeReadScratch() {
		t.Fatal("idle dictionary decoder was not released")
	}
	if codec.decoder != nil || codec.definition != nil || codec.input != nil || codec.raw != nil || codec.lease != nil {
		t.Fatal("refund preceded alias scrubbing")
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("original decoder receipt is still live")
	}
	lease, err := budget.AcquireExternal(target)
	if err != nil {
		t.Fatalf("same request after actual scratch release: %v", err)
	}
	lease.Close()
	if trim.TryReleaseIdleNativeReadScratch() {
		t.Fatal("empty scratch reported a second refund")
	}
	if got := read(); !bytes.Equal(got, owned) {
		t.Fatal("reconstructed dictionary decoder changed captured bytes")
	}
	if db.cow.physicalReader != shared {
		t.Fatal("trim replaced the original workspace owner")
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	if trim.TryReleaseIdleNativeReadScratch() {
		t.Fatal("closed capture retained a cache capability")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if s := budget.Stats(); s.TotalBytes != 0 || s.ReservedBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("Close retained ownership: %+v", s)
	}
}
