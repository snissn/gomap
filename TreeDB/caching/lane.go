package caching

import (
	"fmt"
	"sync"
	"sync/atomic"
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

	vlog               valueWriter
	vlogPath           string
	vlogSeq            int
	vlogRetainedPath   string
	vlogCaps           vlogWriterCaps
	vlogMu             sync.Mutex
	vlogCh             chan vlogWriteRequest
	vlogWorkers        int
	vlogPrepCh         chan vlogDictPrepareTask
	vlogPrepWorkers    int
	vlogPrepMaxWorkers int
	vlogPrepMu         sync.Mutex
	vlogDictBytesMu    sync.RWMutex
	vlogDictBytes      map[uint64][]byte
	vlogQueueing       atomic.Bool
	vlogDirty          atomic.Bool
	vlogLiveBytes      atomic.Int64
	vlogClosedBytes    atomic.Int64
	vlogClosedSizes    map[string]int64

	syncing atomic.Bool
}

func commitLogName(laneID, seq int) string {
	return fmt.Sprintf("commit-l%d-%06d.log", laneID, seq)
}

func valueLogName(laneID, seq int) string {
	return fmt.Sprintf("value-l%d-%06d.log", laneID, seq)
}
