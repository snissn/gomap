package caching

import (
	"errors"
	"runtime"
	"sync"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

var vlogDictPipelineRecordPool sync.Pool // stores []byte
var vlogDictPipelineJobPool sync.Pool    // stores *vlogDictPipelineJob

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

	jobIdx     int
	frameStart int
	frameEnd   int
	k          int

	// rawBytes is the sum of raw payload bytes for all frames in this job (used
	// only for in-flight accounting).
	rawBytes int

	records []valuelog.Record

	// output
	preps []valuelog.PreparedGroupedRecord
	err   error
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
	var encScratch []byte
	const (
		maxKeepRawScratch     = 16 << 20 // 16 MiB
		maxKeepEncScratch     = 4 << 20  // 4 MiB
		maxKeepPreparedRecord = 8 << 20  // 8 MiB
	)

	var (
		rids    [valuelog.MaxFrameK]uint64
		offsets [valuelog.MaxFrameK + 1]uint32
	)

	for {
		select {
		case <-db.closeCh:
			return
		case job := <-jobs:
			if job == nil {
				continue
			}

			frames := job.frameEnd - job.frameStart
			if frames <= 0 || job.k <= 0 {
				job.err = errors.New("cachingdb: invalid pipeline job")
			} else {
				if cap(job.preps) < frames {
					job.preps = make([]valuelog.PreparedGroupedRecord, frames)
				} else {
					job.preps = job.preps[:frames]
					for i := range job.preps {
						job.preps[i] = valuelog.PreparedGroupedRecord{}
					}
				}

				for frameIdx := 0; frameIdx < frames; frameIdx++ {
					start := frameIdx * job.k
					end := start + job.k
					if end > len(job.records) {
						end = len(job.records)
					}
					if start >= end {
						job.err = errors.New("cachingdb: pipeline frame bounds out of range")
						break
					}
					recs := job.records[start:end]
					frameK := len(recs)

					rawBytes := 0
					offsets[0] = 0
					for i := 0; i < frameK; i++ {
						rid := recs[i].RID
						if rid == 0 {
							job.err = errors.New("cachingdb: missing rid")
							break
						}
						rids[i] = rid
						rawBytes += len(recs[i].Value)
						offsets[i+1] = uint32(rawBytes)
					}
					if job.err != nil {
						break
					}

					var rawPayload []byte
					if rawBytes > 0 {
						if rawBytes <= maxKeepRawScratch {
							if cap(rawScratch) < rawBytes {
								rawScratch = make([]byte, rawBytes)
							}
							rawPayload = rawScratch[:rawBytes]
						} else {
							rawPayload = make([]byte, rawBytes)
						}
						off := 0
						for i := 0; i < frameK; i++ {
							off += copy(rawPayload[off:], recs[i].Value)
						}
					}

					attempted := job.dictID != 0 && len(job.dict) > 0 && rawBytes > 0
					kept := false
					payload := rawPayload

					if attempted {
						targetCap := rawBytes + (rawBytes / 16) + 64
						if targetCap < rawBytes+64 {
							targetCap = rawBytes + 64
						}
						if rawBytes <= maxKeepEncScratch && cap(encScratch) < targetCap {
							encScratch = make([]byte, 0, targetCap)
						}
						encoded, encErr := valuelog.CompressPayloadWithDictInto(job.dictID, job.dict, rawPayload, job.encodeLevel, job.enableEntropy, encScratch[:0])
						if encErr != nil {
							job.err = encErr
							break
						}
						if len(encoded) < rawBytes {
							kept = true
							payload = encoded
						}
					}

					prefixLen := valuelog.FrameHeaderSize + (frameK * 8) + ((frameK + 1) * 4)
					recordLen := valuelog.HeaderSize + prefixLen + len(payload)
					recordBuf := getVlogDictPipelineBuf(&vlogDictPipelineRecordPool, recordLen)
					prep, prepErr := valuelog.BuildPreparedGroupedRecordFromPayloadInto(recordBuf[:0], job.dictID, rids[:frameK], offsets[:frameK+1], rawBytes, payload, attempted, kept, 0)
					if prepErr != nil {
						putVlogDictPipelineBuf(&vlogDictPipelineRecordPool, recordBuf, maxKeepPreparedRecord)
						job.err = prepErr
						break
					}
					job.preps[frameIdx] = prep
				}
			}

			// Return control to the writer goroutine.
			if job.results != nil {
				job.results <- job
			}
		}
	}
}

func (db *DB) appendValueLogDictFramesPipeline(w framePreparedWriterInto, dictID uint64, dict []byte, records []valuelog.Record, rawPayloadBytes int, k int, ptrs []page.ValuePtr) (rawFrameBytes, storedPayloadBytes, frameRecords, framesTotal, framesAttempted, framesKept int, encodeNsTotal int64, encodeRawBytes int, err error) {
	if db == nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: nil db")
	}
	if w == nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: nil writer")
	}
	if dictID == 0 || len(dict) == 0 || len(records) == 0 || rawPayloadBytes < 0 || k <= 0 {
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

	const targetRawBytesPerJob = 256 << 10
	framesPerJob := 1
	if rawPayloadBytes > 0 && frameCount > 0 {
		avgFrameBytes := rawPayloadBytes / frameCount
		if avgFrameBytes > 0 {
			framesPerJob = targetRawBytesPerJob / avgFrameBytes
		}
	}
	if framesPerJob < 1 {
		framesPerJob = 1
	}
	if framesPerJob > 64 {
		framesPerJob = 64
	}

	minJobs := workers * 2
	if minJobs < 1 {
		minJobs = 1
	}
	maxFramesPerJob := (frameCount + minJobs - 1) / minJobs
	if maxFramesPerJob < 1 {
		maxFramesPerJob = 1
	}
	if framesPerJob > maxFramesPerJob {
		framesPerJob = maxFramesPerJob
	}
	jobCount := (frameCount + framesPerJob - 1) / framesPerJob

	pipeline := db.ensureVlogDictFramePipeline()
	if pipeline == nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, errors.New("cachingdb: pipeline unavailable")
	}
	call := getVlogDictPipelineCall(workers, jobCount)
	defer putVlogDictPipelineCall(call)
	results := call.results
	pending := call.pending
	nextWrite := 0
	sent := 0
	received := 0
	inFlightBytes := int64(0)

	abort := error(nil)

	scheduleJob := func(jobIdx int) *vlogDictPipelineJob {
		frameStart := jobIdx * framesPerJob
		frameEnd := frameStart + framesPerJob
		if frameEnd > frameCount {
			frameEnd = frameCount
		}
		recordStart := frameStart * k
		recordEnd := frameEnd * k
		if recordEnd > len(records) {
			recordEnd = len(records)
		}
		if recordStart >= recordEnd {
			return nil
		}

		jobAny := vlogDictPipelineJobPool.Get()
		var job *vlogDictPipelineJob
		if jobAny != nil {
			job, _ = jobAny.(*vlogDictPipelineJob)
		}
		if job == nil {
			job = &vlogDictPipelineJob{}
		}
		preps := job.preps
		rawBytes := 0
		for i := recordStart; i < recordEnd; i++ {
			rawBytes += len(records[i].Value)
		}

		*job = vlogDictPipelineJob{
			dictID:        dictID,
			dict:          dict,
			encodeLevel:   encodeLevel,
			enableEntropy: enableEntropy,
			results:       results,
			jobIdx:        jobIdx,
			frameStart:    frameStart,
			frameEnd:      frameEnd,
			k:             k,
			rawBytes:      rawBytes,
			records:       records[recordStart:recordEnd],
		}
		job.preps = preps[:0]
		return job
	}

	freeJob := func(job *vlogDictPipelineJob) {
		if job == nil {
			return
		}
		inFlightBytes -= int64(job.rawBytes)
		if job.preps != nil {
			for i := range job.preps {
				rec := job.preps[i].Record
				if len(rec) > 0 {
					putVlogDictPipelineBuf(&vlogDictPipelineRecordPool, rec, 8<<20)
					job.preps[i] = valuelog.PreparedGroupedRecord{}
				}
			}
			job.preps = job.preps[:0]
		}
		job.records = nil
		job.dict = nil
		job.results = nil
		job.dictID = 0
		job.encodeLevel = 0
		job.enableEntropy = false
		job.jobIdx = 0
		job.frameStart = 0
		job.frameEnd = 0
		job.k = 0
		job.rawBytes = 0
		job.err = nil
		vlogDictPipelineJobPool.Put(job)
	}

	var nextJob *vlogDictPipelineJob

	writeReady := func() {
		for nextWrite < jobCount && pending[nextWrite] != nil {
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

			for frameIdx := 0; frameIdx < len(job.preps); frameIdx++ {
				prep := job.preps[frameIdx]
				if prep.K <= 0 || len(prep.Record) == 0 {
					abort = errors.New("cachingdb: missing prepared record")
					break
				}
				recordStart := (job.frameStart + frameIdx) * k
				recordEnd := recordStart + prep.K
				if recordStart < 0 || recordStart >= len(ptrs) || recordEnd > len(ptrs) || recordEnd < recordStart {
					abort = errors.New("cachingdb: invalid prepared record bounds")
					break
				}
				dst := ptrs[recordStart:recordEnd]
				_, stats, wErr := w.AppendPreparedGroupedRecordInto(prep, dst)
				if wErr != nil {
					abort = wErr
					break
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
			}

			freeJob(job)
			nextWrite++
		}
	}

	for nextWrite < jobCount {
		// Yield periodically so other goroutines can make progress, matching the
		// sequential append loop behavior.
		if nextWrite > 0 && nextWrite%256 == 0 {
			runtime.Gosched()
		}
		writeReady()
		if abort != nil {
			break
		}
		if received >= sent && sent >= jobCount {
			break
		}

		canSend := abort == nil && sent < jobCount
		if canSend && nextJob == nil {
			nextJob = scheduleJob(sent)
			if nextJob == nil {
				abort = errors.New("cachingdb: failed to schedule pipeline job")
			} else {
				inFlightBytes += int64(nextJob.rawBytes)
			}
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
					pending[job.jobIdx] = job
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
				pending[job.jobIdx] = job
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
		pending[job.jobIdx] = job
	}

	for nextWrite < jobCount {
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
