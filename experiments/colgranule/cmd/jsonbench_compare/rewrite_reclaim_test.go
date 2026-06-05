package main

import (
	"context"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type recordingObservedRewriteSourceReclaimer struct {
	calls int
	opts  backenddb.ValueLogGCOptions
	stats backenddb.ValueLogGCStats
	err   error
}

func (r *recordingObservedRewriteSourceReclaimer) ValueLogGC(_ context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
	r.calls++
	r.opts = opts
	return r.stats, r.err
}

func TestReclaimUnreferencedRewriteSourcesUsesObservedActiveReclaim(t *testing.T) {
	recorder := &recordingObservedRewriteSourceReclaimer{
		stats: backenddb.ValueLogGCStats{
			ObservedSourceSegmentsDeleted: 2,
			ObservedSourceBytesDeleted:    1234,
		},
	}

	stats, err := reclaimUnreferencedRewriteSources(context.Background(), recorder, []uint32{22, 33})
	if err != nil {
		t.Fatalf("reclaimUnreferencedRewriteSources: %v", err)
	}
	if recorder.calls != 1 {
		t.Fatalf("ValueLogGC calls=%d want 1", recorder.calls)
	}
	if !reflect.DeepEqual(recorder.opts.ObservedSourceFileIDs, []uint32{22, 33}) {
		t.Fatalf("observed source ids=%v want [22 33]", recorder.opts.ObservedSourceFileIDs)
	}
	if !recorder.opts.ObservedSourceAssumeUnreferenced {
		t.Fatalf("ObservedSourceAssumeUnreferenced=false want true")
	}
	if !recorder.opts.ObservedSourceReclaimActive {
		t.Fatalf("ObservedSourceReclaimActive=false want true")
	}
	if recorder.opts.DryRun {
		t.Fatalf("DryRun=true want false")
	}
	if stats.ObservedSourceSegmentsDeleted != 2 || stats.ObservedSourceBytesDeleted != 1234 {
		t.Fatalf("stats=%+v want deleted=2 bytes=1234", stats)
	}
}

func TestReclaimUnreferencedRewriteSourcesNoopsWithoutIDs(t *testing.T) {
	recorder := &recordingObservedRewriteSourceReclaimer{}

	stats, err := reclaimUnreferencedRewriteSources(context.Background(), recorder, nil)
	if err != nil {
		t.Fatalf("reclaimUnreferencedRewriteSources: %v", err)
	}
	if recorder.calls != 0 {
		t.Fatalf("ValueLogGC calls=%d want 0", recorder.calls)
	}
	if stats != (backenddb.ValueLogGCStats{}) {
		t.Fatalf("stats=%+v want zero", stats)
	}
}
