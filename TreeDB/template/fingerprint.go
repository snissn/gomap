package template

import (
	"encoding/binary"
	"slices"

	"github.com/zeebo/xxh3"
)

// Fingerprints computes deterministic winnowed fingerprints for value using
// an O(n) monotonic deque instead of O(n*w) nested scans.
func Fingerprints(value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	k := cfg.FingerprintK
	w := cfg.FingerprintW
	if len(value) < k || k <= 0 {
		return nil
	}
	n := len(value) - k + 1
	if n <= 0 {
		return nil
	}
	if w <= 0 || n < w {
		w = n
	}
	type dqEntry struct {
		idx int
		val uint64
	}
	var dqStack [128]dqEntry
	var dq []dqEntry
	if w <= len(dqStack) {
		dq = dqStack[:0]
	} else {
		dq = make([]dqEntry, 0, w)
	}
	var outStack [64]uint64
	out := outStack[:0]
	head := 0
	lastMin := uint64(0)
	hasLastMin := false
	for i := 0; i < n; i++ {
		h := xxh3.Hash(value[i : i+k])
		// Maintain non-decreasing deque of window hashes (strictly < pops tail).
		for len(dq) > head && dq[len(dq)-1].val > h {
			dq = dq[:len(dq)-1]
		}
		dq = append(dq, dqEntry{idx: i, val: h})
		if i+1 >= w {
			winStart := i - w + 1
			for head < len(dq) && dq[head].idx < winStart {
				head++
			}
			minVal := dq[head].val
			if !hasLastMin || minVal != lastMin {
				out = append(out, minVal)
				lastMin = minVal
				hasLastMin = true
			}
			// Periodically compact deque backing slice to keep it bounded to w.
			if head >= 64 && head*2 >= len(dq) {
				nRem := copy(dq, dq[head:])
				dq = dq[:nRem]
				head = 0
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	// Dedup in place.
	u := 0
	for i := 0; i < len(out); i++ {
		if i == 0 || out[i] != out[i-1] {
			out[u] = out[i]
			u++
		}
	}
	out = out[:u]
	if cfg.MaxFingerprints > 0 && len(out) > cfg.MaxFingerprints {
		out = out[:cfg.MaxFingerprints]
	}
	res := make([]uint64, len(out))
	copy(res, out)
	return res
}

func fingerprintsLimited(value []byte, cfg Config, limit int) []uint64 {
	if limit > 0 {
		cfg.MaxFingerprints = limit
	}
	return Fingerprints(value, cfg)
}

func lengthFingerprint(length int) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(length))
	return xxh3.Hash(buf[:])
}

const (
	routeTagLen    uint64 = 1
	routeTagPrefix uint64 = 2
	routeTagMid1   uint64 = 3
	routeTagMid2   uint64 = 4
	routeTagSuffix uint64 = 5
)

func mixRouteFP(tag uint64, h uint64) uint64 {
	// Mix in a small tag so different segment roles don't trivially collide.
	return h ^ (tag * 0x9e3779b97f4a7c15)
}

func appendUniqueFP(dst []uint64, fp uint64, limit int) []uint64 {
	if fp == 0 {
		return dst
	}
	for _, v := range dst {
		if v == fp {
			return dst
		}
	}
	if limit > 0 && len(dst) >= limit {
		return dst
	}
	return append(dst, fp)
}

// BucketFingerprints returns fingerprints for bucketing (prefix-only by default).
func BucketFingerprints(value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	k := cfg.FingerprintK
	if k <= 0 || len(value) < k {
		return nil
	}
	if cfg.LengthBucketMinLen > 0 && len(value) >= cfg.LengthBucketMinLen {
		return []uint64{lengthFingerprint(len(value))}
	}
	prefixLen := cfg.RoutePrefixBytes
	if prefixLen < k {
		prefixLen = k
	}
	if prefixLen >= len(value) {
		return Fingerprints(value, cfg)
	}
	return fingerprintsLimited(value[:prefixLen], cfg, cfg.RouteFPCount)
}

// AppendRoutingFingerprints appends deterministic routing fingerprints into dst.
// It avoids winnowing and favors a few fixed segment hashes (plus length).
func AppendRoutingFingerprints(dst []uint64, value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	return appendRoutingFingerprints(dst, value, cfg)
}

func appendRoutingFingerprints(dst []uint64, value []byte, cfg Config) []uint64 {
	k := cfg.FingerprintK
	if k <= 0 || len(value) < k {
		return dst
	}
	limit := cfg.RouteFPCount
	if limit <= 0 {
		limit = cfg.MaxFingerprints
	}
	if cfg.LengthBucketMinLen > 0 && len(value) >= cfg.LengthBucketMinLen {
		// Keep the legacy length fingerprint unmodified so templates published by
		// older versions remain discoverable.
		dst = appendUniqueFP(dst, lengthFingerprint(len(value)), limit)
	}

	prefixLen := cfg.RoutePrefixBytes
	if prefixLen < k {
		prefixLen = k
	}
	suffixLen := cfg.RouteSuffixBytes
	if suffixLen < k {
		suffixLen = k
	}
	segLen := prefixLen
	if segLen > len(value) {
		segLen = len(value)
	}
	if segLen < k {
		segLen = k
	}
	if segLen > len(value) {
		segLen = len(value)
	}
	dst = appendUniqueFP(dst, mixRouteFP(routeTagPrefix, xxh3.Hash(value[:segLen])), limit)

	if len(value) >= suffixLen {
		if suffixLen > len(value) {
			suffixLen = len(value)
		}
		dst = appendUniqueFP(dst, mixRouteFP(routeTagSuffix, xxh3.Hash(value[len(value)-suffixLen:])), limit)
	}

	// Add one or two middle probes to reduce collision/candidate list size while
	// keeping routing cost O(1) per value.
	if len(dst) < limit && segLen < len(value) {
		start := (len(value) - segLen) / 2
		dst = appendUniqueFP(dst, mixRouteFP(routeTagMid1, xxh3.Hash(value[start:start+segLen])), limit)
	}
	if len(dst) < limit && segLen < len(value) {
		start := (len(value) - segLen) / 4
		if start > 0 && start+segLen <= len(value) {
			dst = appendUniqueFP(dst, mixRouteFP(routeTagMid2, xxh3.Hash(value[start:start+segLen])), limit)
		}
	}

	return dst
}

// BucketKey computes a deterministic bucket key from fingerprints.
func BucketKey(fps []uint64) uint64 {
	if len(fps) == 0 {
		return 0
	}
	limit := 8
	if len(fps) < limit {
		limit = len(fps)
	}
	var buf [8 * 8]byte
	for i := 0; i < limit; i++ {
		binary.LittleEndian.PutUint64(buf[i*8:(i+1)*8], fps[i])
	}
	return xxh3.Hash(buf[:limit*8])
}

// RoutingFingerprints returns fingerprints used for routing/bucketing.
// Prefer AppendRoutingFingerprints to avoid allocations.
func RoutingFingerprints(value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	limit := cfg.RouteFPCount
	if limit <= 0 {
		limit = cfg.MaxFingerprints
	}
	out := make([]uint64, 0, limit)
	return appendRoutingFingerprints(out, value, cfg)
}

// RoutingFingerprintsLegacy returns deterministic winnowed routing fingerprints.
// It prefers prefix+suffix slices to avoid random middle bytes dominating.
func RoutingFingerprintsLegacy(value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	k := cfg.FingerprintK
	if k <= 0 || len(value) < k {
		return nil
	}
	if cfg.LengthBucketMinLen > 0 && len(value) >= cfg.LengthBucketMinLen {
		return []uint64{lengthFingerprint(len(value))}
	}
	limit := cfg.RouteFPCount
	if limit <= 0 {
		limit = cfg.MaxFingerprints
	}
	out := make([]uint64, 0, limit)
	seen := make(map[uint64]struct{}, limit)
	if cfg.LengthBucketMinLen > 0 && len(value) >= cfg.LengthBucketMinLen {
		fp := lengthFingerprint(len(value))
		out = append(out, fp)
		seen[fp] = struct{}{}
	}
	prefixLen := cfg.RoutePrefixBytes
	suffixLen := cfg.RouteSuffixBytes
	if prefixLen < k {
		prefixLen = k
	}
	if suffixLen < k {
		suffixLen = k
	}
	if prefixLen >= len(value) {
		return Fingerprints(value, cfg)
	}
	if prefixLen+suffixLen > len(value) {
		suffixLen = len(value) - prefixLen
	}
	prefix := value[:prefixLen]
	suffix := value[len(value)-suffixLen:]
	prefixLimit := limit / 2
	if prefixLimit < 1 {
		prefixLimit = limit
	}
	suffixLimit := limit - prefixLimit
	fps := make([]uint64, 0, limit)
	fps = append(fps, fingerprintsLimited(prefix, cfg, prefixLimit)...)
	if suffixLimit > 0 && suffixLen >= k {
		fps = append(fps, fingerprintsLimited(suffix, cfg, suffixLimit)...)
	}
	if len(fps) == 0 {
		return Fingerprints(value, cfg)
	}
	for _, fp := range fps {
		if len(out) >= limit {
			break
		}
		if _, ok := seen[fp]; ok {
			continue
		}
		out = append(out, fp)
		seen[fp] = struct{}{}
	}
	return out
}

// RouteFingerprints returns deterministic routing fingerprints for template anchors.
func RouteFingerprints(anchors [][]byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	if len(anchors) == 0 {
		return nil
	}
	// Concatenate anchors into a buffer.
	total := 0
	for _, a := range anchors {
		total += len(a)
	}
	buf := make([]byte, 0, total)
	for _, a := range anchors {
		buf = append(buf, a...)
	}
	fps := Fingerprints(buf, cfg)
	if len(fps) == 0 {
		return nil
	}
	if cfg.RouteFPCount > 0 && len(fps) > cfg.RouteFPCount {
		fps = fps[:cfg.RouteFPCount]
	}
	return fps
}
