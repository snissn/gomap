package caching

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func newPreparedAppendAllocTestLog(tb testing.TB) (*DB, *cachingLeafPageLog) {
	tb.Helper()
	clock := valuelog.NewVirtualClock(time.Unix(0, 0))
	sink := &valuelog.VirtualSink{Clock: clock}
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		tb.Fatal(err)
	}
	writer := valuelog.NewWriterWithSink(sink, fileID)
	writer.SetEncodeSampleStride(0)
	db := &DB{
		closeCh:                    make(chan struct{}),
		indexOuterLeavesInValueLog: true,
		valueLogCompressionMode:    uint8(vlogCompressionBlock),
		valueLogBlockCodec:         valuelog.BlockCodecLZ4,
		flushApplyConcurrency:      2,
		valueLogAutotuneOptions:    valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
	}
	db.leafLog = lane{id: leafLogLaneID, vlog: writer, vlogSeq: 1}
	return db, &cachingLeafPageLog{db: db, lane: &db.leafLog}
}

func BenchmarkAppendPreparedLeafPageAllocs(b *testing.B) {
	_, log := newPreparedAppendAllocTestLog(b)
	leafPage := make([]byte, page.PageSize)
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	if _, err := log.AppendPreparedLeafPage(leafPage, payload); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.AppendPreparedLeafPage(leafPage, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func preparedLeafBatchFixture(n int) ([][]byte, [][]byte) {
	leafPages := make([][]byte, n)
	payloads := make([][]byte, n)
	for i := 0; i < n; i++ {
		leafPages[i] = make([]byte, page.PageSize)
		payloads[i] = make([]byte, 256)
		payloads[i][0] = byte(i)
	}
	return leafPages, payloads
}

func TestAppendPreparedLeafPageAllocsBudget(t *testing.T) {
	if testRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	_, log := newPreparedAppendAllocTestLog(t)
	leafPage := make([]byte, page.PageSize)
	payload := make([]byte, 256)
	if _, err := log.AppendPreparedLeafPage(leafPage, payload); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := log.AppendPreparedLeafPage(leafPage, payload); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 0 {
		t.Fatalf("AppendPreparedLeafPage steady-state allocs = %.2f, want 0", allocs)
	}
}

func TestAppendPreparedLeafPageChildRefsAllocsBudget(t *testing.T) {
	if testRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	_, log := newPreparedAppendAllocTestLog(t)
	leafPages, payloads := preparedLeafBatchFixture(128)
	refs := make([]page.ChildRef, 0, len(leafPages))
	if _, err := log.AppendPreparedLeafPageChildRefs(leafPages, payloads, refs); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		out, err := log.AppendPreparedLeafPageChildRefs(leafPages, payloads, refs)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(leafPages) {
			t.Fatalf("refs len=%d want %d", len(out), len(leafPages))
		}
	})
	if allocs > 0 {
		t.Fatalf("AppendPreparedLeafPageChildRefs steady-state allocs = %.2f, want 0", allocs)
	}
}

func BenchmarkAppendPreparedLeafPagesAllocs(b *testing.B) {
	_, log := newPreparedAppendAllocTestLog(b)
	leafPages, payloads := preparedLeafBatchFixture(128)
	if _, err := log.AppendPreparedLeafPages(leafPages, payloads); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptrs, err := log.AppendPreparedLeafPages(leafPages, payloads)
		if err != nil {
			b.Fatal(err)
		}
		_ = ptrs
	}
}

func BenchmarkAppendPreparedLeafPageChildRefsAllocs(b *testing.B) {
	_, log := newPreparedAppendAllocTestLog(b)
	leafPages, payloads := preparedLeafBatchFixture(128)
	refs := make([]page.ChildRef, 0, len(leafPages))
	if _, err := log.AppendPreparedLeafPageChildRefs(leafPages, payloads, refs); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := log.AppendPreparedLeafPageChildRefs(leafPages, payloads, refs)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}
