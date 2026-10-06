//go:build !windows

package caching

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func openApplyHandoffCache(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := Open(dir, backend, Options{IndexOuterLeavesInValueLog: true, FlushApplyConcurrency: 4, ValueLogCompression: uint8(vlogCompressionBlock), ValueLogMaxSegmentBytes: 512, RelaxedSync: true, AllowUnsafe: true})
	if err != nil {
		backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cached.Close() })
	return cached
}

func TestCachingLeafPageLogApplyTokenHandoffConcurrentPreparedLanes(t *testing.T) {
	cached := openApplyHandoffCache(t)
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer)
	defer builder.Abandon()
	var mu sync.Mutex
	var calls atomic.Int64
	var generations sync.Map
	raw := func(ptrs []page.ValuePtr, tokens []*rootpublication.StableResourceToken) error {
		// The real producer must supply current, complete exact append authority.
		for _, ptr := range ptrs {
			covered := false
			for _, token := range tokens {
				if token.Generation() == uint64(ptr.FileID) && token.Frontier().Bytes >= ptr.Offset+uint64(page.ValuePtrRecordLength(ptr)) {
					covered = true
				}
			}
			if !covered {
				for _, token := range tokens {
					token.Release()
				}
				return fmt.Errorf("missing raw frontier")
			}
			generations.Store(ptr.FileID, true)
		}
		mu.Lock()
		defer mu.Unlock()
		for i, token := range tokens {
			if err := token.ValidateStableNamespace(); err != nil {
				for _, pending := range tokens[i:] {
					pending.Release()
				}
				return err
			}
			if err := builder.Add(token); err != nil {
				for _, pending := range tokens[i:] {
					pending.Release()
				}
				return err
			}
		}
		calls.Add(1)
		return nil
	}
	log, supported, err := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPageLogApplyTokenProvider).LeafPageLogForApply(raw, builder.Merge)
	if err != nil || !supported {
		t.Fatalf("factory supported=%v err=%v", supported, err)
	}
	const count = valuelog.MaxFrameK*2 + 8
	pages, payloads := make([][]byte, count), make([][]byte, count)
	for i := range pages {
		pages[i] = buildSparseLeafPageForLeafLogTestWithTag(t, byte(i))
		payloads[i], _, err = valuelog.MaybeCompactLeafLogPayloadTo(nil, pages[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	failures := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		lane, ok := log.(backenddb.LeafPageLogLaneProvider).LeafPageLogLane(worker)
		if !ok {
			t.Fatalf("missing lane %d", worker)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			refs, err := lane.(backenddb.LeafPagePreparedChildRefBatchLog).AppendPreparedLeafPageChildRefs(pages, payloads, nil)
			if err == nil && len(refs) != count {
				err = fmt.Errorf("refs=%d want %d", len(refs), count)
			}
			failures <- err
		}()
	}
	wg.Wait() // The Apply owner freezes only after every lane is joined.
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 {
		t.Fatalf("handoff calls=%d want 4", calls.Load())
	}
	n := 0
	generations.Range(func(_, _ any) bool { n++; return true })
	if n < 2 || set.Len() != n {
		t.Fatalf("rotation/current membership resources=%d generations=%d", set.Len(), n)
	}
	tokens := set.Tokens()
	identity := tokens[0].Identity()
	frontier := tokens[0].Frontier().Bytes
	if _, err := cached.valueLogIdentityPins.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("live output pin=%v", err)
	}
	// Even on an attempt-bound appender, the PUBLIC stable API returns full owned
	// resources and cannot pass through the nil-set private handoff.
	_, public, err := log.(backenddb.LeafPageStableLog).AppendLeafPageWithStableResources(pages[0])
	if err != nil {
		t.Fatal(err)
	}
	if public == nil || public.Len() == 0 || public.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("public append bypassed owned closure")
	}
	public.Release()
	if calls.Load() != 4 {
		t.Fatal("public append used private callback")
	}
	if tokens[0].Frontier().Bytes != frontier {
		t.Fatal("later append changed immutable captured frontier")
	}
	set.Release()
	if got := cached.valueLogIdentityPins.Stats().ActivePins; got != 0 {
		t.Fatalf("final raw pins=%d want 0", got)
	}
}

func TestCachingLeafPageLogApplyTokenHandoffFailureConsumesAndRetries(t *testing.T) {
	cached := openApplyHandoffCache(t)
	producer := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPageLogApplyTokenProvider)
	injected := errors.New("Apply raw handoff failed")
	calls := 0
	log, _, err := producer.LeafPageLogForApply(func(_ []page.ValuePtr, tokens []*rootpublication.StableResourceToken) error {
		calls++
		for _, token := range tokens {
			token.Release()
		}
		return injected
	}, func(*rootpublication.StableResourceSet) error { return injected })
	if err != nil {
		t.Fatal(err)
	}
	leaf := buildSparseLeafPageForLeafLogTestWithTag(t, 'f')
	if _, err := log.AppendLeafPage(leaf); !errors.Is(err, injected) {
		t.Fatalf("append failure=%v", err)
	}
	if calls != 1 || cached.valueLogIdentityPins.Stats().ActivePins != 0 {
		t.Fatal("failed handoff leaked inventory or pins")
	}
	// A fresh attempt captures the post-failure physical frontier independently.
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer)
	defer builder.Abandon()
	retry, _, err := producer.LeafPageLogForApply(func(_ []page.ValuePtr, tokens []*rootpublication.StableResourceToken) error {
		for i, token := range tokens {
			if err := builder.Add(token); err != nil {
				for _, pending := range tokens[i:] {
					pending.Release()
				}
				return err
			}
		}
		return nil
	}, builder.Merge)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retry.AppendLeafPage(leaf); err != nil {
		t.Fatal(err)
	}
	builder.Abandon() // Simulate an optimistic conflict/abort after successful append.
	if cached.valueLogIdentityPins.Stats().ActivePins != 0 {
		t.Fatal("retry abort leaked pins")
	}
}
