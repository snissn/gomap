//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCOWPointerWorkspaceDictionarySanitizationAndReuse(t *testing.T) {
	for _, captured := range []bool{false, true} {
		t.Run(fmt.Sprintf("captured=%t", captured), func(t *testing.T) {
			db, want, ptr := cowSuccessorDictionaryLeafFixture(t)
			cowCapturePendingLeafDictionary(t, db, ptr, want)
			if captured {
				original := db.dictStore
				db.dictStore = cowCapturedDictionaryOnlyStore{original}
				defer func() { db.dictStore = original }()
			}
			anchor, e := db.AcquireMVCCReadCut()
			if e != nil {
				t.Fatal(e)
			}
			defer anchor.Close()
			if !captured {
				if v, e := anchor.GetAppend([]byte("a000"), nil); e != nil || !bytes.Equal(v, want) {
					t.Fatalf("warm dictionary: %v", e)
				}
			}
			release := cowBatchOccupyShared(t, anchor)
			defer release()
			s, e := db.AcquireMVCCReadCut()
			if e != nil {
				t.Fatal(e)
			}
			owned, e := s.GetAppend([]byte("a000"), nil)
			if e != nil || !bytes.Equal(owned, want) {
				t.Fatalf("dictionary copy-out: %v", e)
			}
			w := s.cowReader
			if w == nil || w.point == nil {
				t.Fatal("dictionary fallback did not admit point workspace")
			}
			codecs := append([]*cowReadCodec(nil), w.codecs...)
			if captured && len(codecs) == 0 {
				t.Fatal("captured-only provider did not exercise cut-local codec")
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if w.budget == nil || w.snapshot != nil || w.codecs != nil || w.allocations != nil || w.input != nil || w.output != nil {
				t.Fatal("dictionary owner was not sanitized before park")
			}
			for _, c := range codecs {
				if c != nil && (c.decoder != nil || c.definition != nil || c.input != nil || c.raw != nil || c.lease != nil) {
					t.Fatal("dictionary decoder authority survived park")
				}
			}
			next, e := db.AcquireMVCCReadCut()
			if e != nil {
				t.Fatal(e)
			}
			out, e := next.GetAppend([]byte("a000"), nil)
			if e != nil || !bytes.Equal(out, want) {
				t.Fatalf("dictionary reborrow: %v", e)
			}
			if next.cowReader != w {
				t.Fatal("dictionary read did not reuse sanitized workspace")
			}
			if e = next.Close(); e != nil {
				t.Fatal(e)
			}

			if !bytes.Equal(owned, want) {
				t.Fatal("corruption/cleanup changed earlier owned result")
			}
		})
	}
}

func TestCOWPointerWorkspaceCorruptReuseRefund(t *testing.T) {
	db, want := cowPointerWorkspaceFixture(t, false)
	anchor, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer anchor.Close()
	entry, e := anchor.GetEntryExact([]byte("a000"))
	if e != nil {
		t.Fatal(e)
	}
	release := cowBatchOccupyShared(t, anchor)
	defer release()
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	owned, e := s.GetAppend([]byte("a000"), nil)
	if e != nil || !bytes.Equal(owned, want) {
		t.Fatalf("first owned read: %v", e)
	}
	w := s.cowReader
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	f, _, e := anchor.cowCut.basis.snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
	if e != nil {
		t.Fatal(e)
	}
	writer, e := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	offset := int64(entry.ValuePtr.Offset - 4)
	var saved [1]byte
	if _, e = writer.ReadAt(saved[:], offset); e != nil {
		t.Fatal(e)
	}
	if _, e = writer.WriteAt([]byte{saved[0] ^ 1}, offset); e != nil {
		t.Fatal(e)
	}
	defer writer.WriteAt(saved[:], offset)
	out, e := next.GetAppend([]byte("a000"), nil)
	if !errors.Is(e, valuelog.ErrCorrupt) || out != nil {
		t.Fatalf("reused workspace suppressed corrupt error: %v", e)
	}
	if next.cowReader != w {
		t.Fatal("corruption did not exercise parked workspace")
	}
	if e = next.Close(); e != nil {
		t.Fatal(e)
	}
	if w.budget != nil || w.point != nil || w.lease != nil || w.codecs != nil {
		t.Fatal("failed reused owner did not fully refund")
	}
	if !bytes.Equal(owned, want) {
		t.Fatal("corruption/cleanup changed earlier owned result")
	}
}
