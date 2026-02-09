package caching

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type lane struct {
	id int

	wal            commitWriter
	walPath        string
	walSeq         int
	walMu          sync.Mutex
	walCh          chan walWriteRequest
	walFastMu      sync.Mutex
	walFastCond    *sync.Cond
	walFastQueue   []walFastItem
	walFastHead    int
	walFastClosed  bool
	walLiveBytes   atomic.Int64
	walClosedBytes atomic.Int64
	walClosedSizes map[string]int64

	vlog                    valueWriter
	vlogPath                string
	vlogSeq                 int
	vlogRetainedPath        string
	vlogModeWriter          valueWriter
	vlogModeSet             bool
	vlogMode                vlogCompressionWriteMode
	vlogBlockCodec          valuelog.BlockCodec
	vlogCaps                vlogWriterCaps
	vlogMu                  sync.Mutex
	vlogCh                  chan vlogWriteRequest
	vlogWorkers             int
	vlogPrepCh              chan vlogDictPrepareTask
	vlogPrepWorkers         int
	vlogPrepMaxWorkers      int
	vlogPrepMu              sync.Mutex
	vlogDictBytesMu         sync.RWMutex
	vlogDictBytes           map[uint64][]byte
	vlogCompressionSelector *vlogCompressionSelector
	vlogBlockRatioBits      [vlogBlockCodecCount]atomic.Uint64
	vlogBlockRatioSamples   [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKCount         [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKSum           [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKMax           [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKBuckets       [vlogBlockCodecCount][vlogBlockKBucketCount]atomic.Uint64
	vlogQueueing            atomic.Bool
	vlogQueueEnqueued       atomic.Uint64
	vlogQueueLagCount       atomic.Uint64
	vlogQueueLagTotalNs     atomic.Uint64
	vlogQueueLagMaxNs       atomic.Uint64
	vlogQueueLagBuckets     [vlogQueueLagBucketCount]atomic.Uint64
	vlogQueueDepthSamples   atomic.Uint64
	vlogQueueDepthSum       atomic.Uint64
	vlogQueueDepthMax       atomic.Uint64
	vlogQueueDepthLast      atomic.Uint64
	vlogQueuePositiveRunMax atomic.Uint64
	vlogQueueDriftLastDepth int
	vlogQueueDriftLastAtNs  int64
	vlogQueueDriftCurrentNs uint64
	vlogDirty               atomic.Bool
	vlogLiveBytes           atomic.Int64
	vlogClosedBytes         atomic.Int64
	vlogClosedSizes         map[string]int64

	syncing atomic.Bool
}

func commitLogName(laneID, seq int) string {
	return fmt.Sprintf("commit-l%d-%06d.log", laneID, seq)
}

func valueLogName(laneID, seq int) string {
	return fmt.Sprintf("value-l%d-%06d.log", laneID, seq)
}
