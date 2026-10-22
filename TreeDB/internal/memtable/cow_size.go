package memtable

import (
	"math"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowLogicalDelta belongs only to a private preparation. Each physical
// replacement contributes new-old, so repeated operands telescope without
// remembering their keys or revisiting the opening immutable root.
type cowLogicalDelta struct {
	bytes    int64
	overflow bool
}

func cowLogicalSize(keyBytes, payloadBytes int, flags byte) (int64, bool) {
	if keyBytes < 0 || payloadBytes < 0 {
		return 0, false
	}
	size, payload := int64(keyBytes), int64(payloadBytes)
	if flags&node.FlagPointer != 0 {
		if payload > math.MaxInt64-page.ValuePtrSize {
			return 0, false
		}
		payload += page.ValuePtrSize
	} else if flags&node.FlagTombstone != 0 {
		payload = 0
	}
	if payload > math.MaxInt64-size {
		return 0, false
	}
	return size + payload, true
}

func (d *cowLogicalDelta) replace(keyBytes int, added, removed cowValue) {
	// Every nonempty cowValue comes from the admitted metadata encoder, which
	// always contributes at least cowValueMetadataBytes, even for empty values.
	var next, previous int64
	var ok bool
	if added != "" {
		next, ok = cowLogicalSize(keyBytes, len(added)-cowValueMetadataBytes, added[24])
		if !ok {
			d.overflow = true
			return
		}
	}
	if removed != "" {
		previous, ok = cowLogicalSize(keyBytes, len(removed)-cowValueMetadataBytes, removed[24])
		if !ok {
			d.overflow = true
			return
		}
	}
	change := next - previous // both nonnegative int64 sizes
	if change > 0 && d.bytes > math.MaxInt64-change || change < 0 && d.bytes < math.MinInt64-change {
		d.overflow = true
		return
	}
	d.bytes += change
}

// LogicalSizeDelta reports exact physical flush bytes relative to the opening
// root. It carries no ownership or publication authority. A false result must
// be refused before accepting an irreversible write.
func (p *COWPrepared) LogicalSizeDelta() (int64, bool) {
	return p.logicalBytes, !p.logicalOverflow
}
