package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type keepPolicySetter interface {
	SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64)
}

type frameStatsWriter interface {
	AppendFrameWithStats(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, valuelog.FrameStats, error)
}

type frameStatsWriterInto interface {
	AppendFrameWithStatsInto(dictID uint64, dict []byte, records []valuelog.Record, dst []page.ValuePtr) ([]page.ValuePtr, valuelog.FrameStats, error)
}

type rawFrameBatchWriterInto interface {
	AppendRawFramesWritevInto(records []valuelog.Record, k int, dst []page.ValuePtr) ([]page.ValuePtr, valuelog.FrameStats, error)
}

type vlogWriterCaps struct {
	writer    valueWriter
	keep      keepPolicySetter
	stats     frameStatsWriter
	statsInto frameStatsWriterInto
	rawInto   rawFrameBatchWriterInto
}

func computeVlogWriterCaps(w valueWriter) vlogWriterCaps {
	var caps vlogWriterCaps
	if w == nil {
		return caps
	}
	caps.writer = w
	if v, ok := any(w).(keepPolicySetter); ok {
		caps.keep = v
	}
	if v, ok := any(w).(frameStatsWriter); ok {
		caps.stats = v
	}
	if v, ok := any(w).(frameStatsWriterInto); ok {
		caps.statsInto = v
	}
	if v, ok := any(w).(rawFrameBatchWriterInto); ok {
		caps.rawInto = v
	}
	return caps
}
