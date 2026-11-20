package caching

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestNativeBSONProjectionPreservesConcurrentBulkStripes(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previous)
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	}()
	db, err := Open(dir, backend, Options{
		JournalLanes: 3, ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	group, err := backend.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := group.Close(); err != nil {
			t.Error(err)
		}
	}()

	values := make([][]byte, 32)
	for i := range values {
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 8192+i)
	}
	appendSerial := func(values [][]byte) []page.ValuePtr {
		t.Helper()
		ptrs := make([]page.ValuePtr, len(values))
		for i, value := range values {
			var err error
			ptrs[i], err = group.AppendNativeBSONValue(value)
			if err != nil {
				t.Fatal(err)
			}
		}
		return ptrs
	}
	verify := func(values [][]byte, ptrs []page.ValuePtr) map[uint32]bool {
		t.Helper()
		if len(ptrs) != len(values) {
			t.Fatalf("pointers=%d, values=%d", len(ptrs), len(values))
		}
		var lastRID uint64
		files := make(map[uint32]bool)
		for i, ptr := range ptrs {
			lane, _ := valuelog.DecodeFileID(ptr.FileID)
			got, err := backend.ReadValueLogRecordAppend(ptr, nil)
			if err != nil || !bytes.Equal(got, values[i]) || lane != 0 {
				t.Fatalf("pointer %d has lane=%d, bytes=%d, error=%v", i, lane, len(got), err)
			}
			f, err := os.Open(valuelog.SegmentPath(db.valueLogDir, ptr.FileID))
			if err != nil {
				t.Fatal(err)
			}
			rid, err := valuelog.ReadRIDAtUnverified(f, ptr.FileID, ptr)
			if err := errors.Join(err, f.Close()); err != nil || rid <= lastRID {
				t.Fatalf("pointer %d RID=%d previous=%d error=%v", i, rid, lastRID, err)
			}
			lastRID = rid
			files[ptr.FileID] = true
		}
		return files
	}
	initial := appendSerial(values[:8])
	if files := verify(values[:8], initial); len(files) != 1 {
		t.Fatalf("initial serial appends have %d physical files, want one", len(files))
	}
	var bulk []page.ValuePtr
	var bulkErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		bulk, bulkErr = backend.AppendValueLogValues(values[8:24])
	}()
	serial := appendSerial(values[24:])
	<-done
	if bulkErr != nil {
		t.Fatal(bulkErr)
	}
	defer backend.ReleaseValueLogValues(bulk)
	if files := verify(values[8:24], bulk); len(files) != 8 {
		t.Fatalf("bulk appends have %d physical files, want all eight", len(files))
	}
	verify(values[24:], serial)
	if files := verify(values[:8], appendSerial(values[:8])); len(files) != 8 {
		t.Fatalf("serial appends after the bulk writer have %d physical files, want all eight", len(files))
	}
	// Starting other writers must leave the first file readable by its actual ID.
	verify(values[:8], initial)
}
