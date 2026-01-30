package caching

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Diagnostic test: capture allocator + fragmentation snapshots at key lifecycle
// boundaries for the bloat workload:
//
//  1. after churn, before Close()
//  2. after Close()+reopen
//  3. after VacuumIndexOffline + reopen
//
// This test is intended to stay green and provide structured evidence about
// where "bloat" lives (freelist vs underfilled pages vs span).
func TestCachedBenchBloat_StatsAcrossLifecycle(t *testing.T) {
	keys := 20000
	if v := os.Getenv("TREEDB_TEST_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keys = n
		}
	}

	dir := t.TempDir()

	openBackend := func() *db.DB {
		b, err := db.Open(db.Options{
			Dir:               dir,
			PreferAppendAlloc: false,
			KeepRecent:        1,
		})
		if err != nil {
			t.Fatalf("backend open: %v", err)
		}
		return b
	}

	backend := openBackend()
	cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cached open: %v", err)
	}

	val := bytes.Repeat([]byte("a"), 128)

	// Mirror the bloat regression workload.
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after batch write: %v", err)
	}
	applyRandomUpdates(t, cached, keys, val, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random write: %v", err)
	}
	{
		b := cached.NewBatch()
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Delete(k); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("delete write: %v", err)
		}
		_ = b.Close()
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after delete: %v", err)
	}
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rewrite: %v", err)
	}

	type snap struct {
		label string
		stats map[string]string
		frag  map[string]string
	}

	capture := func(label string, b *db.DB) snap {
		s := b.Stats()
		f, ferr := b.FragmentationReport()
		if ferr != nil {
			// FragmentationReport is best-effort; keep the test green but surface the issue.
			t.Logf("%s: fragmentation error: %v", label, ferr)
			f = map[string]string{"treedb.fragmentation.error": ferr.Error()}
		}
		return snap{label: label, stats: s, frag: f}
	}

	beforeClose := capture("before_close", backend)

	_ = cached.Close()
	_ = backend.Close()

	backend2 := openBackend()
	afterReopen := capture("after_reopen", backend2)
	_ = backend2.Close()

	if err := db.VacuumIndexOffline(db.Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum offline: %v", err)
	}

	backend3 := openBackend()
	afterVacuum := capture("after_vacuum_reopen", backend3)
	_ = backend3.Close()

	// Emit concise diffs to help focus investigation. These are t.Logf so they
	// won't spam CI unless -v or the test fails.
	t.Logf("stats diff (before_close -> after_reopen):\n%s", diffMaps(beforeClose.stats, afterReopen.stats))
	t.Logf("frag  diff (before_close -> after_reopen):\n%s", diffMaps(beforeClose.frag, afterReopen.frag))
	t.Logf("stats diff (after_reopen -> after_vacuum_reopen):\n%s", diffMaps(afterReopen.stats, afterVacuum.stats))
	t.Logf("frag  diff (after_reopen -> after_vacuum_reopen):\n%s", diffMaps(afterReopen.frag, afterVacuum.frag))
}

func diffMaps(a, b map[string]string) string {
	if a == nil {
		a = map[string]string{}
	}
	if b == nil {
		b = map[string]string{}
	}

	keys := make([]string, 0, len(a)+len(b))
	seen := map[string]struct{}{}
	for k := range a {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	for k := range b {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		av, aok := a[k]
		bv, bok := b[k]
		if aok && bok && av == bv {
			continue
		}
		if !aok {
			av = "<missing>"
		}
		if !bok {
			bv = "<missing>"
		}
		sb.WriteString(fmt.Sprintf("%s: %s -> %s\n", k, av, bv))
	}
	if sb.Len() == 0 {
		return "<no changes>"
	}
	return sb.String()
}
