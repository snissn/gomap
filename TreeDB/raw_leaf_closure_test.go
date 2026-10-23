package treedb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestOrdinaryFirstCheckpointRotatedLeafDependenciesReopen(t *testing.T) {
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 8 << 10
	opts.ValueLog.Generational.HotSegmentTargetBytes = 64 << 10
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	key := func(i int) []byte { return []byte(fmt.Sprintf("leaf-external-%08d", i)) }
	value := func(i int) []byte {
		out := make([]byte, 512)
		for j := range out {
			out[j] = byte((i*19 + j*13) % 251)
		}
		return out
	}
	b := d.NewBatch()
	defer b.Close()
	for i := 0; i < 1024; i++ {
		if err := b.Set(key(i), value(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	stats := d.Stats()
	for _, key := range []string{"treedb.raw.span_native.route.point_put.used_ops_total", "treedb.flush_apply.span_native.used_ops_total"} {
		t.Logf("activation %s=%s", key, stats[key])
	}
	t.Logf("first checkpoint backend state=%+v", d.backend.State())
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen first ordinary checkpoint: %v", err)
	}
	for i := 0; i < 1024; i++ {
		got, err := d.Get(key(i))
		if err != nil || !bytes.Equal(got, value(i)) {
			t.Fatalf("key%d len=%d err=%v", i, len(got), err)
		}
	}
}
