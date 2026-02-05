package caching

import (
	"errors"
	"runtime"
	"sync"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

var vlogDictPipelineEncPool sync.Pool // stores []byte
var vlogDictPipelineJobPool sync.Pool // stores *vlogDictPipelineJob

func getVlogDictPipelineBuf(p *sync.Pool, capacity int) []byte {
	if capacity <= 0 {
		return nil
	}
	if v := p.Get(); v != nil {
		if b, ok := v.([]byte); ok && cap(b) >= capacity {
			return b[:capacity]
		}
	}
	return make([]byte, capacity)
}

func putVlogDictPipelineBuf(p *sync.Pool, b []byte, maxCap int) {
	if b == nil {
		return
	}
	if maxCap > 0 && cap(b) > maxCap {
		return
	}
	p.Put(b[:0])
}

type vlogDictPipelineCall struct {
	results chan *vlogDictPipelineJob
	pending []*vlogDictPipelineJob
}

var vlogDictPipelineCallPool sync.Pool // stores *vlogDictPipelineCall

func getVlogDictPipelineCall(workers, frameCount int) *vlogDictPipelineCall {
	callAny := vlogDictPipelineCallPool.Get()
	var call *vlogDictPipelineCall
	if callAny != nil {
		call, _ = callAny.(*vlogDictPipelineCall)
	}
	if call == nil {
		call = &vlogDictPipelineCall{}
	}

	// Ensure result channel is big enough to avoid blocking workers.
	buf := workers * 8
	if buf < 16 {
		buf = 16
	}
	if buf < frameCount {
		buf = frameCount
	}
	if call.results == nil || cap(call.results) < buf {
		call.results = make(chan *vlogDictPipelineJob, buf)
	} else {
		// Drain any leftover items (should be empty if callers are correct).
		for {
			select {
			case <-call.results:
			default:
				goto drained
			}
		}
	drained:
	}

	if cap(call.pending) < frameCount {
		call.pending = make([]*vlogDictPipelineJob, frameCount)
	} else {
		call.pending = call.pending[:frameCount]
		for i := range call.pending {
			call.pending[i] = nil
		}
	}

	return call
}

func putVlogDictPipelineCall(call *vlogDictPipelineCall) {
	if call == nil {
		return
	}
	// Best-effort drain to avoid retaining jobs if a caller exits early.
	if call.results != nil {
		for {
			select {
			case <-call.results:
			default:
				goto drained
			}
		}
	drained:
	}
	if call.pending != nil {
		for i := range call.pending {
			call.pending[i] = nil
		}
		call.pending = call.pending[:0]
	}
	vlogDictPipelineCallPool.Put(call)
}

type vlogDictPipelineJob struct {
	// immutable input
	dictID        uint64
	dict          []byte
	encodeLevel   zstd.EncoderLevel
	enableEntropy bool
	results       chan<- *vlogDictPipelineJob

	frameIdx int
	start    int
	end      int
	k        int

	rawBytes int

	rids    [valuelog.MaxFrameK]uint64
	offsets [valuelog.MaxFrameK + 1]uint32

	records []valuelog.Record

	// output
	encoded  []byte
	kept     bool
	encodeNs int64
	err      error
}

type vlogDictFramePipeline struct {
	jobs chan *vlogDictPipelineJob
}

func (db *DB) ensureVlogDictFramePipeline() *vlogDictFramePipeline {
	if db == nil || db.valueLogDictFramePipelineWorkers <= 1 {
		return nil
	}
	// If db.closeCh is nil, the DB wasn't created via Open; don't start
	// persistent goroutines that the caller can't shut down.
	if db.closeCh == nil {
		return nil
	}

	db.valueLogDictFramePipelineMu.Lock()
	defer db.valueLogDictFramePipelineMu.Unlock()
	if db.valueLogDictFramePipeline != nil {
		return db.valueLogDictFramePipeline
	}
	workers := db.valueLogDictFramePipelineWorkers
	if workers <= 1 {
		return nil
	}
	p := &vlogDictFramePipeline{
		// A small buffer avoids sender contention when compressing many tiny
		// frames (common with small batch sizes).
		jobs: make(chan *vlogDictPipelineJob, workers*4),
	}
	for i := 0; i < workers; i++ {
		db.wg.Add(1)
		go func() {
			defer db.wg.Done()
			db.runVlogDictFramePipelineWorker(p.jobs)
		}()
	}
	db.valueLogDictFramePipeline = p
	return p
}

func (db *DB) runVlogDictFramePipelineWorker(jobs <-chan *vlogDictPipelineJob) {
	if db == nil {
		return
	}
	// Per-worker scratch to avoid per-frame allocations.
	var rawScratch []byte
	const maxKeepScratch = 16 << 20 // 16 MiB

	for {
		select {
		case <-db.closeCh:
			return
		case job := <-jobs:
			if job == nil {
				continue
			}

			if job.rawBytes > 0 {
				if cap(rawScratch) < job.rawBytes && job.rawBytes <= maxKeepScratch {
					rawScratch = make([]byte, job.rawBytes)
				}
				var payload []byte
				if job.rawBytes <= maxKeepScratch {
					payload = rawScratch[:job.rawBytes]
				} else {
					payload = make([]byte, job.rawBytes)
				}
				off := 0
				for i := 0; i < job.k; i++ {
					off += copy(payload[off:], job.records[i].Value)
				}

				// Preallocate destination close to raw size (compressed payloads
				// should be smaller; allow a small overhead to avoid reallocs on
				// incompressible inputs).
				dst := getVlogDictPipelineBuf(&vlogDictPipelineEncPool, job.rawBytes+64)
				encoded, encErr := valuelog.CompressPayloadWithDictInto(job.dictID, job.dict, payload, job.encodeLevel, job.enableEntropy, dst[:0])
				if encErr != nil {
					job.err = encErr
					putVlogDictPipelineBuf(&vlogDictPipelineEncPool, dst, 4<<20)
				} else {
					if len(encoded) < job.rawBytes {
						job.kept = true
						job.encoded = encoded
					} else {
						job.kept = false
						putVlogDictPipelineBuf(&vlogDictPipelineEncPool, dst, 4<<20)
						job.encoded = nil
					}
				}
			}

			// Return control to the writer goroutine.
			if job.results != nil {
				job.results <- job
			}
		}
	}
}

func (db *DB) appendValueLogDictFramesPipeline(w framePayloadWriterInto, dictID uint64, dict []byte, records []valuelog.Record, k int, ptrs []page.ValuePtr) (rawFrameBytes, storedPayloadBytes, frameRecords, framesTotal, framesAttempted, framesKept int, encodeNsTotal int64, encodeRawBytes int, err error) {
	if db == nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: nil db")
	}
	if w == nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: nil writer")
	}
	if dictID == 0 || len(dict) == 0 || len(records) == 0 || k <= 0 {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: invalid pipeline args")
	}
	if len(ptrs) < len(records) {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: ptrs too small")
	}

	workers := db.valueLogDictFramePipelineWorkers
	if workers <= 1 {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: pipeline disabled")
	}
	maxInFlight := db.valueLogDictFramePipelineMaxInFlightBytes
	if maxInFlight <= 0 {
		maxInFlight = int64(workers) * (16 << 20)
	}

	encodeLevel := db.valueLogDictFrameEncodeLevel
	enableEntropy := db.valueLogDictFrameEnableEntropy

	frameCount := (len(records) + k - 1) / k
	if frameCount <= 1 {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: too few frames for pipeline")
	}

	pipeline := db.ensureVlogDictFramePipeline()
	if pipeline == nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: pipeline unavailable")
	}
	call := getVlogDictPipelineCall(workers, frameCount)
	defer putVlogDictPipelineCall(call)
	results := call.results
	pending := call.pending
	nextWrite := 0
	sent := 0
	received := 0
	inFlightBytes := int64(0)

	abort := error(nil)

	scheduleFrame := func(frameIdx int) *vlogDictPipelineJob {
		start := frameIdx * k
		end := start + k
		if end > len(records) {
			end = len(records)
		}
		frameK := end - start
		jobAny := vlogDictPipelineJobPool.Get()
		var job *vlogDictPipelineJob
		if jobAny != nil {
			job, _ = jobAny.(*vlogDictPipelineJob)
		}
		if job == nil {
			job = &vlogDictPipelineJob{}
		}
		*job = vlogDictPipelineJob{
			dictID:        dictID,
			dict:          dict,
			encodeLevel:   encodeLevel,
			enableEntropy: enableEntropy,
			results:       results,
			frameIdx:      frameIdx,
			start:         start,
			end:           end,
			k:             frameK,
			records:       records[start:end],
		}
		off := 0
		job.offsets[0] = 0
		for i := 0; i < frameK; i++ {
			rec := records[start+i]
			job.rids[i] = rec.RID
			off += len(rec.Value)
			job.offsets[i+1] = uint32(off)
		}
		job.rawBytes = off
		return job
	}

	freeJob := func(job *vlogDictPipelineJob) {
		if job == nil {
			return
		}
		inFlightBytes -= int64(job.rawBytes)
		if job.encoded != nil {
			putVlogDictPipelineBuf(&vlogDictPipelineEncPool, job.encoded, 4<<20)
			job.encoded = nil
		}
		job.records = nil
		job.dict = nil
		job.results = nil
		*job = vlogDictPipelineJob{}
		vlogDictPipelineJobPool.Put(job)
	}

	var nextJob *vlogDictPipelineJob

	writeReady := func() {
		for nextWrite < frameCount && pending[nextWrite] != nil {
			job := pending[nextWrite]
			pending[nextWrite] = nil

			if abort == nil && job != nil && job.err != nil {
				abort = job.err
			}
			if abort != nil {
				freeJob(job)
				nextWrite++
				continue
			}

			payload := []byte(nil)
			kept := job.kept
			if kept && len(job.encoded) > 0 {
				payload = job.encoded
			} else {
				kept = false
			}
			if !kept && job.rawBytes > 0 {
				// Rare path: we didn't keep compression (size inflation); build a
				// contiguous payload for the writer API.
				payload = make([]byte, job.rawBytes)
				off := 0
				for i := 0; i < job.k; i++ {
					off += copy(payload[off:], job.records[i].Value)
				}
			}
			attempted := dictID != 0 && len(dict) > 0 && job.rawBytes > 0
			dst := ptrs[job.start:job.end]
			_, stats, wErr := w.AppendFrameFromPayloadWithStatsInto(dictID, job.rids[:job.k], job.offsets[:job.k+1], job.rawBytes, payload, attempted, kept, job.encodeNs, dst)
			if wErr != nil {
				abort = wErr
				freeJob(job)
				nextWrite++
				continue
			}

			rawFrameBytes += stats.RawPayloadBytes
			storedPayloadBytes += stats.StoredPayloadBytes
			frameRecords += stats.Records
			framesTotal++
			if stats.Attempted {
				framesAttempted++
			}
			if stats.Kept {
				framesKept++
			}
			if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
				encodeNsTotal += stats.EncodeNs
				encodeRawBytes += stats.RawPayloadBytes
			}

			freeJob(job)
			nextWrite++
		}
	}

	for nextWrite < frameCount {
		// Yield periodically so other goroutines can make progress, matching the
		// sequential append loop behavior.
		if nextWrite > 0 && nextWrite%256 == 0 {
			runtime.Gosched()
		}
		writeReady()
		if abort != nil {
			break
		}
		if received >= sent && sent >= frameCount {
			break
		}

		canSend := abort == nil && sent < frameCount
		if canSend && nextJob == nil {
			nextJob = scheduleFrame(sent)
			inFlightBytes += int64(nextJob.rawBytes)
		}
		if canSend && nextJob != nil {
			oversize := int64(nextJob.rawBytes) > maxInFlight
			switch {
			case oversize && inFlightBytes-int64(nextJob.rawBytes) > 0:
				canSend = false
			case !oversize && inFlightBytes > maxInFlight:
				canSend = false
			}
		}

		if canSend && nextJob != nil {
			select {
			case pipeline.jobs <- nextJob:
				nextJob = nil
				sent++
			case job := <-results:
				received++
				if job != nil {
					pending[job.frameIdx] = job
					if job.err != nil && abort == nil {
						abort = job.err
					}
				}
			}
			continue
		}

		// Can't schedule more; wait for at least one result.
		if received < sent {
			job := <-results
			received++
			if job != nil {
				pending[job.frameIdx] = job
				if job.err != nil && abort == nil {
					abort = job.err
				}
			}
			continue
		}

		// No work in flight and nothing left to send.
		break
	}

	if nextJob != nil {
		freeJob(nextJob)
		nextJob = nil
	}
	for received < sent {
		job := <-results
		received++
		pending[job.frameIdx] = job
	}

	for nextWrite < frameCount {
		writeReady()
		if pending[nextWrite] == nil {
			break
		}
	}
	for i := range pending {
		freeJob(pending[i])
		pending[i] = nil
	}

	if abort != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, abort
	}
	if inFlightBytes != 0 {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: vlog pipeline in-flight bytes mismatch")
	}
	return rawFrameBytes, storedPayloadBytes, frameRecords, framesTotal, framesAttempted, framesKept, encodeNsTotal, encodeRawBytes, nil
}
