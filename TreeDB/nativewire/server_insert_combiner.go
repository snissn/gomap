package nativewire

import (
	"errors"
	"runtime"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type nativewireInsertBatchCombiner struct {
	mu    sync.Mutex
	lanes map[string]*nativewireInsertBatchCombineLane
}

type nativewireInsertBatchCombineLane struct {
	draining bool
	queue    []*nativewireInsertBatchCombineItem
}

type nativewireInsertBatchCombineItem struct {
	req  insertBatchFastRequest
	done chan nativewireInsertBatchCombineResult
}

type nativewireInsertBatchCombineResult struct {
	actualAck         iwire.AckPolicy
	catalogVersion    uint64
	hasCatalogVersion bool
	err               error
}

// run keeps each request handler blocked until its item is applied, so decoded
// ids/docs may safely borrow from the request frame buffer.
func (c *nativewireInsertBatchCombiner) run(s *Server, req insertBatchFastRequest) (nativewireInsertBatchCombineResult, bool) {
	if !canCombineInsertBatchFastRequest(s, req) {
		return nativewireInsertBatchCombineResult{}, false
	}
	item := acquireInsertBatchCombineItem(req)

	c.mu.Lock()
	if c.lanes == nil {
		c.lanes = make(map[string]*nativewireInsertBatchCombineLane)
	}
	lane := c.lanes[req.collectionName]
	if lane == nil {
		lane = &nativewireInsertBatchCombineLane{}
		c.lanes[req.collectionName] = lane
	}
	lane.queue = append(lane.queue, item)
	leader := !lane.draining
	if leader {
		lane.draining = true
	}
	c.mu.Unlock()

	if leader {
		go c.drain(s, req.collectionName, lane)
	}
	result := <-item.done
	releaseInsertBatchCombineItem(item)
	return result, true
}

func canCombineInsertBatchFastRequest(s *Server, req insertBatchFastRequest) bool {
	if s == nil || s.insertBatchCombineMaxBatch <= 1 {
		return false
	}
	if s.clusterSubmitter != nil {
		return false
	}
	if req.collection == nil || req.collectionName == "" {
		return false
	}
	if req.format != collections.DocumentFormatBSON {
		return false
	}
	if req.includeResultIDs {
		return false
	}
	if req.ack != 0 && req.ack != iwire.AckVisible {
		return false
	}
	if len(req.ids) != 1 || len(req.docs) != 1 {
		return false
	}
	return validateBSONDocuments(req.docs) == nil
}

func (c *nativewireInsertBatchCombiner) drain(s *Server, collectionName string, lane *nativewireInsertBatchCombineLane) {
	yieldInsertBatchCombiner(s)
	var batchScratch []*nativewireInsertBatchCombineItem
	for {
		c.mu.Lock()
		batchScratch = popInsertBatchCombineItems(lane, s.insertBatchCombineMaxBatch, batchScratch[:0])
		batch := batchScratch
		if len(batch) == 0 {
			lane.draining = false
			delete(c.lanes, collectionName)
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		applyInsertBatchCombineItems(s, batch)
		clear(batch)
		batchScratch = batch[:0]
		yieldInsertBatchCombiner(s)
	}
}

func popInsertBatchCombineItems(lane *nativewireInsertBatchCombineLane, max int, dst []*nativewireInsertBatchCombineItem) []*nativewireInsertBatchCombineItem {
	if lane == nil || len(lane.queue) == 0 {
		return nil
	}
	if max <= 0 || max > len(lane.queue) {
		max = len(lane.queue)
	}
	out := append(dst[:0], lane.queue[:max]...)
	copy(lane.queue, lane.queue[max:])
	clear(lane.queue[len(lane.queue)-max:])
	lane.queue = lane.queue[:len(lane.queue)-max]
	return out
}

func yieldInsertBatchCombiner(s *Server) {
	yields := defaultInsertBatchCombineDrainYields
	if s != nil {
		yields = s.insertBatchCombineDrainYields
	}
	for i := 0; i < yields; i++ {
		runtime.Gosched()
	}
}

func applyInsertBatchCombineItems(s *Server, batch []*nativewireInsertBatchCombineItem) {
	if len(batch) == 0 {
		return
	}
	if err := s.rejectClusterLocalMutation("insert_batch combiner"); err != nil {
		for _, item := range batch {
			finishInsertBatchCombineItem(item, nativewireInsertBatchCombineResult{err: err})
		}
		return
	}
	if len(batch) == 1 {
		applySingleInsertBatchCombineItem(s, batch[0])
		if s != nil {
			s.counters.add("insert_batch_combiner.single_requests_total", 1)
		}
		return
	}

	var idScratch [64][]byte
	var docScratch [64][]byte
	ids := idScratch[:]
	docs := docScratch[:]
	if len(batch) > len(idScratch) {
		ids = make([][]byte, len(batch))
		docs = make([][]byte, len(batch))
	} else {
		ids = ids[:len(batch)]
		docs = docs[:len(batch)]
	}
	for i, item := range batch {
		ids[i] = item.req.ids[0]
		docs[i] = item.req.docs[0]
	}
	err := batch[0].req.collection.NativewireInsertBatchNoResultIDs(ids, docs, true)
	if err != nil && !errors.Is(err, collections.ErrCommitAmbiguous) {
		for _, item := range batch {
			applySingleInsertBatchCombineItem(s, item)
		}
		if s != nil {
			s.counters.add("insert_batch_combiner.fallback_requests_total", uint64(len(batch)))
		}
		return
	}

	result := nativewireInsertBatchCombineResult{err: metadataWrap(err)}
	if err == nil {
		result.actualAck = iwire.AckVisible
		result.catalogVersion, result.hasCatalogVersion = s.mutationCatalogVersion()
	}
	for _, item := range batch {
		finishInsertBatchCombineItem(item, result)
	}
	if s != nil {
		s.counters.add("insert_batch_combiner.batches_total", 1)
		s.counters.add("insert_batch_combiner.requests_total", uint64(len(batch)))
	}
}

func applySingleInsertBatchCombineItem(s *Server, item *nativewireInsertBatchCombineItem) {
	if item == nil {
		return
	}
	err := item.req.collection.NativewireInsertBatchNoResultIDs(item.req.ids, item.req.docs, true)
	result := nativewireInsertBatchCombineResult{err: metadataWrap(err)}
	if err == nil {
		result.actualAck = iwire.AckVisible
		result.catalogVersion, result.hasCatalogVersion = s.mutationCatalogVersion()
	}
	finishInsertBatchCombineItem(item, result)
}

func finishInsertBatchCombineItem(item *nativewireInsertBatchCombineItem, result nativewireInsertBatchCombineResult) {
	item.done <- result
}

var nativewireInsertBatchCombineItemPool sync.Pool

func acquireInsertBatchCombineItem(req insertBatchFastRequest) *nativewireInsertBatchCombineItem {
	if pooled := nativewireInsertBatchCombineItemPool.Get(); pooled != nil {
		item := pooled.(*nativewireInsertBatchCombineItem)
		item.req = req
		if item.done == nil {
			item.done = make(chan nativewireInsertBatchCombineResult, 1)
		}
		return item
	}
	return &nativewireInsertBatchCombineItem{
		req:  req,
		done: make(chan nativewireInsertBatchCombineResult, 1),
	}
}

func releaseInsertBatchCombineItem(item *nativewireInsertBatchCombineItem) {
	if item == nil {
		return
	}
	item.req = insertBatchFastRequest{}
	nativewireInsertBatchCombineItemPool.Put(item)
}
