package treedb_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func requirePublicStatUint64(t *testing.T, db *treedb.DB, key string) uint64 {
	t.Helper()
	stats := db.Stats()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return v
}

func TestSpanNativeCheckpointDrainUsesSpanNativeReopenRewriteAndGC(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                   dir,
		FlushThreshold:        64 << 20,
		FlushAdmissionPolicy:  treedb.FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
		FlushApplySpanNative:  true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	values := make(map[string][]byte, 96)
	for i := 0; i < 96; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := bytes.Repeat([]byte{byte(1 + i%251)}, 2048)
		values[key] = value
		if err := db.Set([]byte(key), value); err != nil {
			_ = db.Close()
			t.Fatalf("set %s: %v", key, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint base: %v", err)
	}
	usedBefore := requirePublicStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total")
	closeFallbackBefore := requirePublicStatUint64(t, db, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total")
	for i := 0; i < 96; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := bytes.Repeat([]byte{byte(251 - i%251)}, 2048)
		values[key] = value
		if err := db.Set([]byte(key), value); err != nil {
			_ = db.Close()
			t.Fatalf("update %s: %v", key, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint update: %v", err)
	}
	usedAfter := requirePublicStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total")
	if usedAfter <= usedBefore {
		_ = db.Close()
		t.Fatalf("span-native used ops did not increase across checkpoint point drain: before=%d after=%d", usedBefore, usedAfter)
	}
	if got := requirePublicStatUint64(t, db, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"); got != closeFallbackBefore {
		_ = db.Close()
		t.Fatalf("close_or_checkpoint fallback ops changed during checkpoint point drain: before=%d after=%d", closeFallbackBefore, got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	for k, want := range values {
		got, err := reopen.Get([]byte(k))
		if err != nil {
			_ = reopen.Close()
			t.Fatalf("reopen get %s: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			_ = reopen.Close()
			t.Fatalf("reopen value mismatch for %s: got %dB want %dB", k, len(got), len(want))
		}
	}
	rewriteStats, err := reopen.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{BatchSize: 16})
	if err != nil {
		_ = reopen.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if rewriteStats.RecordsCopied == 0 {
		_ = reopen.Close()
		t.Fatalf("ValueLogRewriteOnline copied no records: %+v", rewriteStats)
	}
	for i := 0; i < 48; i++ {
		if err := reopen.Delete([]byte(fmt.Sprintf("key-%04d", i))); err != nil {
			_ = reopen.Close()
			t.Fatalf("delete key-%04d: %v", i, err)
		}
	}
	if err := reopen.Checkpoint(); err != nil {
		_ = reopen.Close()
		t.Fatalf("checkpoint after deletes: %v", err)
	}
	if _, err := reopen.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{}); err != nil {
		_ = reopen.Close()
		t.Fatalf("ValueLogGC after span-native checkpoint/rewrite: %v", err)
	}
	for i := 48; i < 96; i++ {
		key := fmt.Sprintf("key-%04d", i)
		got, err := reopen.Get([]byte(key))
		if err != nil {
			_ = reopen.Close()
			t.Fatalf("post-GC get %s: %v", key, err)
		}
		if !bytes.Equal(got, values[key]) {
			_ = reopen.Close()
			t.Fatalf("post-GC value mismatch for %s", key)
		}
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close reopen: %v", err)
	}
}
