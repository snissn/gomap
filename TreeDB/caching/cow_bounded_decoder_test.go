//go:build !windows

package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWNativeDictionaryUsesBoundedOriginalReceipt(t *testing.T) {
	db, backend, ptr, dictID, _ := cowNativePackedDictionaryLeafFixture(t)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	reader := capture.(*NativeSemanticCapture)
	var first [page.PageSize]byte
	got, err := reader.NativeLeafPage(ptr, first[:])
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), got...)
	db.cow.physicalMu.Lock()
	var found *cowReadCodec
	for _, c := range db.cow.physicalReader.codecs {
		if c != nil && c.id == dictID {
			found = c
		}
	}
	if found == nil {
		db.cow.physicalMu.Unlock()
		t.Fatal("actual dictionary route did not construct codec")
	}
	bounded, ok := found.decoder.(*valuelog.COWBoundedDecoder)
	original := found.lease
	db.cow.physicalMu.Unlock()
	if !ok || bounded.Closed() || original == nil {
		t.Fatal("generic fallback or missing original receipt")
	}
	var second [page.PageSize]byte
	got, err = reader.NativeLeafPage(ptr, second[:])
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("repeat original captured dictionary leaf: %v", err)
	}
	db.cow.physicalMu.Lock()
	same := found.lease == original
	db.cow.physicalMu.Unlock()
	if !same {
		t.Fatal("read replaced original retention owner")
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !bounded.Closed() {
		t.Fatal("cache close retained bounded continuation")
	}
}
