package caching

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type lane struct {
	id int
	// vlogGenerationClass tags lane role for generational placement.
	// 0=hot, 1=warm, 2=cold.
	vlogGenerationClass uint8

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
	// walCoalesceSeq is protected by walMu and groups adjacent unsynced
	// inline point records into one recovery commit-fence sequence.
	walCoalesceSeq           uint64
	walZeroInlineKeyProvider zeroInlineBatchKeyProvider

	vlog                           valueWriter
	vlogPath                       string
	vlogSeq                        int
	vlogRetainedPath               string
	vlogModeWriter                 valueWriter
	vlogModeSet                    bool
	vlogMode                       vlogCompressionWriteMode
	vlogBlockCodec                 valuelog.BlockCodec
	vlogCaps                       vlogWriterCaps
	vlogMu                         sync.Mutex
	vlogCh                         chan vlogWriteRequest
	vlogWorkers                    int
	vlogPrepCh                     chan vlogDictPrepareTask
	vlogPrepWorkers                int
	vlogPrepMaxWorkers             int
	vlogPrepMu                     sync.Mutex
	vlogDictBytesMu                sync.RWMutex
	vlogDictBytes                  map[uint64][]byte
	vlogCompressionSelector        *vlogCompressionSelector
	vlogBlockRatioBits             [vlogBlockCodecCount]atomic.Uint64
	vlogBlockRatioSamples          [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKCount                [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKSum                  [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKMax                  [vlogBlockCodecCount]atomic.Uint64
	vlogBlockKBuckets              [vlogBlockCodecCount][vlogBlockKBucketCount]atomic.Uint64
	vlogWriteModeRawBytes          [vlogCompressionWriteModeCount]atomic.Uint64
	vlogWriteModeStoredBytes       [vlogCompressionWriteModeCount]atomic.Uint64
	vlogWriteModeFrames            [vlogCompressionWriteModeCount]atomic.Uint64
	vlogWriteModeBucketRawBytes    [vlogCompressionWriteModeCount][vlogPayloadBucketCount]atomic.Uint64
	vlogWriteModeBucketStoredBytes [vlogCompressionWriteModeCount][vlogPayloadBucketCount]atomic.Uint64
	vlogWriteModeBucketFrames      [vlogCompressionWriteModeCount][vlogPayloadBucketCount]atomic.Uint64
	vlogPayloadKindRawBytes        [vlogPayloadKindCount]atomic.Uint64
	vlogPayloadKindStoredBytes     [vlogPayloadKindCount]atomic.Uint64
	vlogPayloadKindFrames          [vlogPayloadKindCount]atomic.Uint64
	vlogPayloadSplitRawBytes       [vlogPayloadSplitKindCount]atomic.Uint64
	vlogPayloadSplitStoredBytes    [vlogPayloadSplitKindCount]atomic.Uint64
	vlogPayloadSplitRecords        [vlogPayloadSplitKindCount]atomic.Uint64
	vlogOuterLeafCodecRawBytes     [vlogOuterLeafCodecKindCount]atomic.Uint64
	vlogOuterLeafCodecStoredBytes  [vlogOuterLeafCodecKindCount]atomic.Uint64
	vlogOuterLeafCodecFrames       [vlogOuterLeafCodecKindCount]atomic.Uint64
	vlogQueueing                   atomic.Bool
	vlogQueueEnqueued              atomic.Uint64
	vlogQueueLagCount              atomic.Uint64
	vlogQueueLagTotalNs            atomic.Uint64
	vlogQueueLagMaxNs              atomic.Uint64
	vlogQueueLagBuckets            [vlogQueueLagBucketCount]atomic.Uint64
	vlogQueueDepthSamples          atomic.Uint64
	vlogQueueDepthSum              atomic.Uint64
	vlogQueueDepthMax              atomic.Uint64
	vlogQueueDepthLast             atomic.Uint64
	vlogQueuePositiveRunMax        atomic.Uint64
	vlogQueueDriftLastDepth        int
	vlogQueueDriftLastAtNs         int64
	vlogQueueDriftCurrentNs        uint64
	vlogDirty                      atomic.Bool
	vlogSyncPending                atomic.Bool
	// The materialization-sync certificate is protected by vlogMu. It is only
	// reusable while this lane's sync reservation remains held and the active
	// append-only writer still has the same file and size.
	vlogMaterializationSyncValid  bool
	vlogMaterializationSyncFileID uint32
	vlogMaterializationSyncSeq    int
	vlogMaterializationSyncSize   int64
	backendReadDirtySeq           atomic.Uint64
	backendReadFlushedSeq         atomic.Uint64
	vlogLiveBytes                 atomic.Int64
	vlogClosedBytes               atomic.Int64
	vlogClosedSizes               map[string]int64
	vlogCreatedSegments           []laneValueLogSegment

	// Observability counters for diagnosing pathological lane shapes (e.g. huge l0).
	// These are incremented on segment rotation and allow distinguishing useful
	// rotations from "idle" rotations that produced no bytes.
	vlogRotateTotal         atomic.Uint64
	vlogRotateIdleTotal     atomic.Uint64
	nativeRootAppendCalls   atomic.Uint64
	nativeRootAppendRecords atomic.Uint64
	nativeRootAppendBytes   atomic.Uint64
	nativeRootAppendWallNs  atomic.Uint64
	nativeRootAppendErrors  atomic.Uint64

	leafLogAppendCalls      atomic.Uint64
	leafLogAppendPages      atomic.Uint64
	leafLogAppendBytes      atomic.Uint64
	leafLogAppendLockWaitNs atomic.Uint64
	leafLogAppendLockHoldNs atomic.Uint64
	leafLogAppendErrors     atomic.Uint64

	syncing atomic.Bool
}

type laneValueLogSegment struct {
	path   string
	fileID uint32
}

const (
	vlogGenerationClassHot uint8 = iota
	vlogGenerationClassWarm
	vlogGenerationClassCold
)

const leafLogLaneID = valuelog.ReservedLeafLogLaneID

func commitLogName(laneID, seq int) string {
	return fmt.Sprintf("commit-l%d-%06d.log", laneID, seq)
}

func valueLogName(laneID, seq int) string {
	return fmt.Sprintf("value-l%d-%06d.log", laneID, seq)
}
