package caching

import (
	"errors"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

var vlogDictPipelineRawPool sync.Pool // stores []byte
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

type vlogDictPipelineJob struct {
	frameIdx int
	start    int
	end      int
	k        int

	rawPayload []byte
	rawBytes   int

	rids    [valuelog.MaxFrameK]uint64
	offsets [valuelog.MaxFrameK + 1]uint32

	encoded  []byte
	kept     bool
	encodeNs int64
	err      error
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

	jobs := make(chan *vlogDictPipelineJob, workers*2)
	results := make(chan *vlogDictPipelineJob, workers*2)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if job == nil {
					continue
				}
				if job.rawBytes > 0 {
					// Preallocate destination close to raw size (compressed payloads
					// should be smaller; allow a small overhead to avoid reallocs on
					// incompressible inputs).
					dst := getVlogDictPipelineBuf(&vlogDictPipelineEncPool, job.rawBytes+64)
					encoded, encErr := valuelog.CompressPayloadWithDictInto(dictID, dict, job.rawPayload, encodeLevel, enableEntropy, dst[:0])
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
				results <- job
			}
		}()
	}

	pending := make([]*vlogDictPipelineJob, frameCount)
	nextWrite := 0
	sent := 0
	received := 0
	inFlightBytes := int64(0)

	abort := error(nil)

	frameRawBytes := func(frameIdx int) int {
		start := frameIdx * k
		end := start + k
		if end > len(records) {
			end = len(records)
		}
		rawBytes := 0
		for i := start; i < end; i++ {
			rawBytes += len(records[i].Value)
		}
		return rawBytes
	}

	scheduleFrame := func(frameIdx int, rawBytes int) *vlogDictPipelineJob {
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
			frameIdx: frameIdx,
			start:    start,
			end:      end,
			k:        frameK,
			rawBytes: rawBytes,
		}
		job.rawPayload = getVlogDictPipelineBuf(&vlogDictPipelineRawPool, rawBytes)
		off := 0
		job.offsets[0] = 0
		for i := 0; i < frameK; i++ {
			rec := records[start+i]
			job.rids[i] = rec.RID
			off += copy(job.rawPayload[off:], rec.Value)
			job.offsets[i+1] = uint32(off)
		}
		return job
	}

	freeJob := func(job *vlogDictPipelineJob) {
		if job == nil {
			return
		}
		inFlightBytes -= int64(job.rawBytes)
		putVlogDictPipelineBuf(&vlogDictPipelineRawPool, job.rawPayload, 4<<20)
		job.rawPayload = nil
		if job.encoded != nil {
			putVlogDictPipelineBuf(&vlogDictPipelineEncPool, job.encoded, 4<<20)
			job.encoded = nil
		}
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

			payload := job.rawPayload
			kept := job.kept
			if kept && len(job.encoded) > 0 {
				payload = job.encoded
			} else {
				kept = false
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
		writeReady()
		if abort != nil {
			break
		}
		if received >= sent && sent >= frameCount {
			break
		}

		canSend := abort == nil && sent < frameCount
		rawBytes := 0
		if canSend {
			rawBytes = frameRawBytes(sent)
			oversize := int64(rawBytes) > maxInFlight
			switch {
			case oversize && inFlightBytes > 0:
				canSend = false
			case !oversize && inFlightBytes+int64(rawBytes) > maxInFlight:
				canSend = false
			}
		}

		if canSend && nextJob == nil {
			nextJob = scheduleFrame(sent, rawBytes)
			inFlightBytes += int64(nextJob.rawBytes)
		}

		if canSend && nextJob != nil {
			select {
			case jobs <- nextJob:
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

	close(jobs)
	if nextJob != nil {
		freeJob(nextJob)
		nextJob = nil
	}
	for received < sent {
		job := <-results
		received++
		pending[job.frameIdx] = job
	}
	wg.Wait()

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
