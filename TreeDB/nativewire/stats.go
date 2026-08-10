package nativewire

import (
	"strconv"
	"sync"
	"sync/atomic"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const nativeStatsPrefix = "treedb.native_wire."
const maxTrackedErrorCode = int(iwire.MaxErrorCode)

// Stats is a string-valued snapshot of native-wire and TreeDB counters.
type Stats map[string]string

type counters struct {
	connectionsOpened                  atomic.Uint64
	connectionsClosed                  atomic.Uint64
	framesIn                           atomic.Uint64
	framesOut                          atomic.Uint64
	bytesIn                            atomic.Uint64
	bytesOut                           atomic.Uint64
	malformedFrames                    atomic.Uint64
	requestsStarted                    atomic.Uint64
	requestsCompleted                  atomic.Uint64
	requestsFailed                     atomic.Uint64
	requestsCanceled                   atomic.Uint64
	errorsTotal                        atomic.Uint64
	dispatchNanos                      atomic.Uint64
	clusterSubmitRequests              atomic.Uint64
	clusterSubmitSuccess               atomic.Uint64
	clusterSubmitErrors                atomic.Uint64
	clusterSubmitReadOnly              atomic.Uint64
	clusterSubmitDurabilityUnavailable atomic.Uint64
	clusterSubmitCommitAmbiguous       atomic.Uint64
	clusterSubmitAckVisible            atomic.Uint64
	clusterSubmitAckFlushed            atomic.Uint64
	clusterSubmitAckSynced             atomic.Uint64
	clusterSubmitAckRaftCommitted      atomic.Uint64
	clusterSubmitNanos                 atomic.Uint64

	errorCodes [maxTrackedErrorCode + 1]atomic.Uint64
	commands   [64]commandCounters
	mu         sync.Mutex
	values     map[string]uint64
}

type commandCounters struct {
	requests atomic.Uint64
	errors   atomic.Uint64
}

func (c *counters) add(key string, delta uint64) {
	if c == nil || key == "" || delta == 0 {
		return
	}
	if c.addHot(key, delta) {
		return
	}
	c.mu.Lock()
	if c.values == nil {
		c.values = make(map[string]uint64)
	}
	c.values[key] += delta
	c.mu.Unlock()
}

func (c *counters) inc(key string) {
	c.add(key, 1)
}

func (c *counters) incFramesIn() {
	if c == nil {
		return
	}
	c.framesIn.Add(1)
}

func (c *counters) incFramesOut() {
	if c == nil {
		return
	}
	c.framesOut.Add(1)
}

func (c *counters) addBytesIn(delta uint64) {
	if c == nil || delta == 0 {
		return
	}
	c.bytesIn.Add(delta)
}

func (c *counters) addBytesOut(delta uint64) {
	if c == nil || delta == 0 {
		return
	}
	c.bytesOut.Add(delta)
}

func (c *counters) incRequestsStarted() {
	if c == nil {
		return
	}
	c.requestsStarted.Add(1)
}

func (c *counters) incRequestsCompleted() {
	if c == nil {
		return
	}
	c.requestsCompleted.Add(1)
}

func (c *counters) incRequestsFailed() {
	if c == nil {
		return
	}
	c.requestsFailed.Add(1)
}

func (c *counters) incRequestsCanceled() {
	if c == nil {
		return
	}
	c.requestsCanceled.Add(1)
}

func (c *counters) addDispatchNanos(delta uint64) {
	if c == nil || delta == 0 {
		return
	}
	c.dispatchNanos.Add(delta)
}

func (c *counters) incErrorsTotal() {
	if c == nil {
		return
	}
	c.errorsTotal.Add(1)
}

func (c *counters) incErrorCode(code iwire.ErrorCode) {
	if c == nil {
		return
	}
	if code > 0 && code <= iwire.ErrorCode(maxTrackedErrorCode) {
		c.errorCodes[code].Add(1)
		return
	}
	c.add("errors.code."+strconv.FormatUint(uint64(code), 10), 1)
}

func (c *counters) addHot(key string, delta uint64) bool {
	switch key {
	case "connections.opened_total":
		c.connectionsOpened.Add(delta)
	case "connections.closed_total":
		c.connectionsClosed.Add(delta)
	case "frames.in_total":
		c.framesIn.Add(delta)
	case "frames.out_total":
		c.framesOut.Add(delta)
	case "bytes.in_total":
		c.bytesIn.Add(delta)
	case "bytes.out_total":
		c.bytesOut.Add(delta)
	case "malformed_frames_total":
		c.malformedFrames.Add(delta)
	case "requests.started_total":
		c.requestsStarted.Add(delta)
	case "requests.completed_total":
		c.requestsCompleted.Add(delta)
	case "requests.failed_total":
		c.requestsFailed.Add(delta)
	case "requests.canceled_total":
		c.requestsCanceled.Add(delta)
	case "errors.total":
		c.errorsTotal.Add(delta)
	case "dispatch_nanos_total":
		c.dispatchNanos.Add(delta)
	case "cluster_submit.requests_total":
		c.clusterSubmitRequests.Add(delta)
	case "cluster_submit.success_total":
		c.clusterSubmitSuccess.Add(delta)
	case "cluster_submit.errors_total":
		c.clusterSubmitErrors.Add(delta)
	case "cluster_submit.read_only_total":
		c.clusterSubmitReadOnly.Add(delta)
	case "cluster_submit.durability_unavailable_total":
		c.clusterSubmitDurabilityUnavailable.Add(delta)
	case "cluster_submit.commit_ambiguous_total":
		c.clusterSubmitCommitAmbiguous.Add(delta)
	case "cluster_submit.ack_visible_total":
		c.clusterSubmitAckVisible.Add(delta)
	case "cluster_submit.ack_flushed_total":
		c.clusterSubmitAckFlushed.Add(delta)
	case "cluster_submit.ack_synced_total":
		c.clusterSubmitAckSynced.Add(delta)
	case "cluster_submit.ack_raft_committed_total":
		c.clusterSubmitAckRaftCommitted.Add(delta)
	case "cluster_submit.nanos_total":
		c.clusterSubmitNanos.Add(delta)
	default:
		return false
	}
	return true
}

func (c *counters) incCommandRequest(id iwire.CommandID, fallbackName string) {
	c.incCommand(id, fallbackName, "requests_total")
}

func (c *counters) incCommandError(id iwire.CommandID, fallbackName string) {
	c.incCommand(id, fallbackName, "errors_total")
}

func (c *counters) incCommand(id iwire.CommandID, fallbackName, suffix string) {
	if c == nil {
		return
	}
	if id < iwire.CommandID(len(c.commands)) && commandCounterName(id) != "" {
		if suffix == "requests_total" {
			c.commands[id].requests.Add(1)
		} else {
			c.commands[id].errors.Add(1)
		}
		return
	}
	if fallbackName == "" {
		return
	}
	c.add("commands."+fallbackName+"."+suffix, 1)
}

func (c *counters) snapshot() map[string]uint64 {
	out := make(map[string]uint64)
	if c == nil {
		return out
	}
	addIfNonZero := func(key string, value uint64) {
		if value != 0 {
			out[key] = value
		}
	}
	addIfNonZero("connections.opened_total", c.connectionsOpened.Load())
	addIfNonZero("connections.closed_total", c.connectionsClosed.Load())
	addIfNonZero("frames.in_total", c.framesIn.Load())
	addIfNonZero("frames.out_total", c.framesOut.Load())
	addIfNonZero("bytes.in_total", c.bytesIn.Load())
	addIfNonZero("bytes.out_total", c.bytesOut.Load())
	addIfNonZero("malformed_frames_total", c.malformedFrames.Load())
	addIfNonZero("requests.started_total", c.requestsStarted.Load())
	addIfNonZero("requests.completed_total", c.requestsCompleted.Load())
	addIfNonZero("requests.failed_total", c.requestsFailed.Load())
	addIfNonZero("requests.canceled_total", c.requestsCanceled.Load())
	addIfNonZero("errors.total", c.errorsTotal.Load())
	addIfNonZero("dispatch_nanos_total", c.dispatchNanos.Load())
	addIfNonZero("cluster_submit.requests_total", c.clusterSubmitRequests.Load())
	addIfNonZero("cluster_submit.success_total", c.clusterSubmitSuccess.Load())
	addIfNonZero("cluster_submit.errors_total", c.clusterSubmitErrors.Load())
	addIfNonZero("cluster_submit.read_only_total", c.clusterSubmitReadOnly.Load())
	addIfNonZero("cluster_submit.durability_unavailable_total", c.clusterSubmitDurabilityUnavailable.Load())
	addIfNonZero("cluster_submit.commit_ambiguous_total", c.clusterSubmitCommitAmbiguous.Load())
	addIfNonZero("cluster_submit.ack_visible_total", c.clusterSubmitAckVisible.Load())
	addIfNonZero("cluster_submit.ack_flushed_total", c.clusterSubmitAckFlushed.Load())
	addIfNonZero("cluster_submit.ack_synced_total", c.clusterSubmitAckSynced.Load())
	addIfNonZero("cluster_submit.ack_raft_committed_total", c.clusterSubmitAckRaftCommitted.Load())
	addIfNonZero("cluster_submit.nanos_total", c.clusterSubmitNanos.Load())
	for code := iwire.ErrorCode(1); code <= iwire.ErrorCode(maxTrackedErrorCode); code++ {
		value := c.errorCodes[code].Load()
		if value != 0 {
			out["errors.code."+strconv.FormatUint(uint64(code), 10)] = value
		}
	}
	for id := range c.commands {
		name := commandCounterName(iwire.CommandID(id))
		if name == "" {
			continue
		}
		addIfNonZero("commands."+name+".requests_total", c.commands[id].requests.Load())
		addIfNonZero("commands."+name+".errors_total", c.commands[id].errors.Load())
	}
	c.mu.Lock()
	for key, value := range c.values {
		out[key] = value
	}
	c.mu.Unlock()
	return out
}
func commandCounterName(id iwire.CommandID) string {
	switch id {
	case iwire.CommandCreateCollection:
		return "create_collection"
	case iwire.CommandListCollections:
		return "list_collections"
	case iwire.CommandCreateIndex:
		return "create_index"
	case iwire.CommandListIndexes:
		return "list_indexes"
	case iwire.CommandDropIndex:
		return "drop_index"
	case iwire.CommandOpenCollection:
		return "open_collection"
	case iwire.CommandCloseCollection:
		return "close_collection"
	case iwire.CommandDropCollection:
		return "drop_collection"
	case iwire.CommandInsertBatch:
		return "insert_batch"
	case iwire.CommandReplaceBatch:
		return "replace_batch"
	case iwire.CommandDeleteBatch:
		return "delete_batch"
	case iwire.CommandUpdateBSONSet:
		return "update_bson_set"
	case iwire.CommandFlushCollection:
		return "flush_collection"
	case iwire.CommandFlushAll:
		return "flush_all"
	case iwire.CommandCheckpoint:
		return "checkpoint"
	case iwire.CommandGetMany:
		return "get_many"
	case iwire.CommandIndexLookup:
		return "index_lookup"
	case iwire.CommandIndexRange:
		return "index_range"
	case iwire.CommandOpenScan:
		return "open_scan"
	case iwire.CommandCursorNext:
		return "cursor_next"
	case iwire.CommandCursorClose:
		return "cursor_close"
	case iwire.CommandExplain:
		return "explain"
	case iwire.CommandStats:
		return "stats"
	case iwire.CommandVectorStatus:
		return "vector_status"
	case iwire.CommandVectorSearchStrict:
		return "vector_search_strict"
	case iwire.CommandVectorSearchFast:
		return "vector_search_fast"
	case iwire.CommandVectorPinSearchSnapshot:
		return "vector_pin_search_snapshot"
	case iwire.CommandVectorSearchPinned:
		return "vector_search_pinned"
	case iwire.CommandVectorClosePinnedSnapshot:
		return "vector_close_pinned_snapshot"
	default:
		return ""
	}
}
