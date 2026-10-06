package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// These tests exercise the real pure dictionary provider and private COW
// ownership carriers. They do not exercise compressed producer dispatch or the
// public Write path, which require a separate registered pointer/RID fixture.
type cowDictionaryCleanupProbe struct {
	t       *testing.T
	db      *DB
	c       *cowCache
	refuse  int
	stages  int
	payload uint64
	pin     uint64
	release [3]int // owner, temporary snapshot, immutable bytes
}

var cowDictionaryCleanupRefusal = errors.New("dictionary admission refused")

func (p *cowDictionaryCleanupProbe) admit(sizes dictdb.DictionaryReadAllocationSizes) (func(), error) {
	p.stages++
	p.payload += sizes.Payload
	p.pin += sizes.Pin
	stage := p.stages
	if p.c.writerMu.TryLock() {
		p.c.writerMu.Unlock()
		p.t.Error("dictionary preparation ran without COW writer ownership")
	}
	if p.db.writeMu.TryLock() {
		p.db.writeMu.Unlock()
		p.t.Error("dictionary preparation ran without write admission")
	}
	if stage == p.refuse {
		return nil, cowDictionaryCleanupRefusal
	}
	release, err := p.c.admitDictionaryRead(sizes)
	if err != nil {
		return nil, err
	}
	return func() {
		if p.c.writerMu.TryLock() {
			p.c.writerMu.Unlock()
		} else {
			p.t.Error("dictionary cleanup ran under COW writer ownership")
		}
		// An exclusive try proves the admission read lock is absent as well.
		if p.db.writeMu.TryLock() {
			p.db.writeMu.Unlock()
		} else {
			p.t.Error("dictionary cleanup ran under write admission")
		}
		if p.c.cutMu.TryLock() {
			p.c.cutMu.Unlock()
		} else {
			p.t.Error("dictionary cleanup ran under COW publication latch")
		}
		p.release[stage-1]++
		release()
	}, nil
}

func cowDictionaryCleanupStore(t *testing.T, pointer bool) (*dictdb.Store, uint64, []byte) {
	t.Helper()
	store, err := dictdb.Open(t.TempDir(), backenddb.Options{ChunkSize: 65536, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	want := []byte("dictionary")
	if pointer {
		want = bytes.Repeat([]byte("dictionary-payload|"), 100)
	}
	id, err := store.PutDictBytes(context.Background(), want)
	if err != nil {
		t.Fatal(err)
	}
	return store, id, want
}

func TestCOWDictionaryCleanupPrepareRefusalTransfersOwner(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		for stage := 1; stage <= 3; stage++ {
			t.Run(fmt.Sprintf("pointer=%t/stage=%d", pointer, stage), func(t *testing.T) {
				db, c := cowCutFixture(t)
				store, id, _ := cowDictionaryCleanupStore(t, pointer)
				probe := &cowDictionaryCleanupProbe{t: t, db: db, c: c, refuse: stage}
				before := c.budget.Stats()
				db.writeMu.RLock()
				c.writerMu.Lock()
				owner, err := store.PrepareDictionaryReadDefinition(id, c.readLimits(), c.budget.Limits().MaxResources, probe.admit)
				// The same carrier used by partial batch cancellation must preserve
				// transferred ownership without executing callbacks in cancel.
				b := &Batch{db: db, cowPrepared: &cowBatchPreparation{resources: []*cowLiveResource{{definition: owner}}}}
				cancelled := b.cancelCOWPublication()
				held := c.budget.Stats()
				c.writerMu.Unlock()
				db.writeMu.RUnlock()
				if !errors.Is(err, cowDictionaryCleanupRefusal) || probe.stages != stage {
					t.Fatalf("refusal err=%v stages=%d want=%d", err, probe.stages, stage)
				}
				if (owner == nil) != (stage == 1) {
					t.Fatal("partial owner was lost or allocated before owner admission")
				}
				if probe.release != [3]int{} || held.ExternalLeases != before.ExternalLeases+stage-1 {
					t.Fatalf("cleanup occurred before unlocked drain: releases=%v held=%+v before=%+v", probe.release, held, before)
				}
				cancelled.drainCancelled()
				cancelled.drainCancelled()
				owner.Close()
				var expected [3]int
				for i := 0; i < stage-1; i++ {
					expected[i] = 1
				}
				if probe.release != expected {
					t.Fatalf("release counts=%v want=%v", probe.release, expected)
				}
				if after := c.budget.Stats(); after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases {
					t.Fatalf("partial owner did not refund exactly: before=%+v after=%+v", before, after)
				}
			})
		}
	}
}

func TestCOWDictionaryCleanupPrivateCutOwnership(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		for _, publish := range []bool{false, true} {
			t.Run(fmt.Sprintf("pointer=%t/publish=%t", pointer, publish), func(t *testing.T) {
				db, c := cowCutFixture(t)
				store, id, want := cowDictionaryCleanupStore(t, pointer)
				probe := &cowDictionaryCleanupProbe{t: t, db: db, c: c}
				before := c.budget.Stats()
				db.writeMu.RLock()
				c.writerMu.Lock()
				owner, prepareErr := store.PrepareDictionaryReadDefinition(id, c.readLimits(), c.budget.Limits().MaxResources, probe.admit)
				var p *cowPreparedCut
				var err error
				shard := db.shardIndex([]byte("a"))
				resource := &cowLiveResource{id: memtable.COWResourceID{Kind: cowResourceDictionary, ID: id}, definition: owner, shard: shard}
				if prepareErr == nil {
					groups := make([][]memtable.COWMutation, len(c.writers))
					groups[shard] = []memtable.COWMutation{{Key: []byte("a"), Value: []byte("published"), Flags: node.FlagInline}}
					opts := make([]memtable.COWPrepareOptions, len(c.writers))
					opts[shard].ResourceSlots = 1
					p, err = c.prepare(groups, opts)
					if err == nil {
						err = p.next.shards[shard].resources.add(resource)
					}
					if err == nil {
						err = p.prepared[shard].AttachResources([]memtable.COWResourceID{resource.id}, resource.close)
						resource.attached = err == nil
					}
				}
				var old *cowReadCut
				if prepareErr == nil && err == nil {
					if publish {
						old = p.publish()
					} else {
						p.cancel()
					}
				} else if p != nil {
					p.cancel()
				}
				c.writerMu.Unlock()
				db.writeMu.RUnlock()
				if prepareErr != nil || err != nil {
					if p != nil {
						p.drainCancelled()
					}
					if !resource.attached {
						resource.close()
					}
					t.Fatalf("prepare=%v attach=%v", prepareErr, err)
				}
				if probe.release != [3]int{} || !bytes.Equal(owner.Bytes, want) || probe.stages != 3 {
					t.Fatal("prepared capture or definition retired before explicit unlocked drain")
				}
				if (probe.payload != 0 && probe.pin != 0) != pointer {
					t.Fatalf("dictionary source shape: pointer=%t payload=%d pin=%d", pointer, probe.payload, probe.pin)
				}
				if !publish {
					p.drainCancelled()
					owner.Close()
					if probe.release != [3]int{1, 1, 1} {
						t.Fatalf("cancel release counts=%v", probe.release)
					}
					if after := c.budget.Stats(); after.ExternalLeases != before.ExternalLeases || after.ExternalBytes != before.ExternalBytes {
						t.Fatalf("cancelled dictionary leaked: before=%+v after=%+v", before, after)
					}
					return
				}
				if old != nil {
					old.drain()
				}
				owner.ReleaseCapture()
				owner.ReleaseCapture()
				if probe.release != [3]int{0, 1, 0} || !bytes.Equal(owner.Bytes, want) {
					t.Fatal("temporary capture drain released the live definition")
				}
				if err := store.SetCurrent(context.Background(), id); err != nil {
					t.Fatal(err)
				}
				if err := store.SetCurrent(context.Background(), 0); err != nil {
					t.Fatal(err)
				}
				// A retained cut keeps generation resources alive after writer
				// teardown. Its final release transfers the real retirement callback.
				snapshot := db.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("published cut capture failed")
				}
				defer snapshot.Close()
				c.close()
				if probe.release != [3]int{0, 1, 0} || !bytes.Equal(owner.Bytes, want) {
					t.Fatal("writer teardown or marker change released retained cut definition")
				}
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				owner.Close()
				if probe.release != [3]int{1, 1, 1} || owner.Bytes != nil {
					t.Fatalf("retirement release counts=%v bytes=%d", probe.release, len(owner.Bytes))
				}
				if stats := c.budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
					t.Fatalf("retirement charge leaked: %+v", stats)
				}
			})
		}
	}
}
