package template

import (
	"encoding/binary"
	"sort"

	"github.com/zeebo/xxh3"
)

// Fingerprints computes deterministic winnowed fingerprints for value.
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
	hashes := make([]uint64, n)
	for i := 0; i < n; i++ {
		hashes[i] = xxh3.Hash(value[i : i+k])
	}
	fps := make(map[uint64]struct{}, n)
	if w <= 0 {
		w = n
	}
	if n < w {
		w = n
	}
	for i := 0; i+w <= n; i++ {
		min := hashes[i]
		for j := i + 1; j < i+w; j++ {
			if hashes[j] < min {
				min = hashes[j]
			}
		}
		fps[min] = struct{}{}
	}
	if len(fps) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(fps))
	for h := range fps {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	if cfg.MaxFingerprints > 0 && len(out) > cfg.MaxFingerprints {
		out = out[:cfg.MaxFingerprints]
	}
	return out
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
	indexTagLen    uint64 = 1
	indexTagPrefix uint64 = 2
	indexTagMid1   uint64 = 3
	indexTagMid2   uint64 = 4
	indexTagSuffix uint64 = 5
)

func mixIndexFP(tag uint64, h uint64) uint64 {
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
	prefixLen := cfg.IndexPrefixBytes
	if prefixLen < k {
		prefixLen = k
	}
	if prefixLen >= len(value) {
		return Fingerprints(value, cfg)
	}
	return fingerprintsLimited(value[:prefixLen], cfg, cfg.IndexFPCount)
}

// AppendIndexFingerprints appends deterministic index fingerprints into dst.
// It avoids winnowing and favors a few fixed segment hashes (plus length).
func AppendIndexFingerprints(dst []uint64, value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	return appendIndexFingerprints(dst, value, cfg)
}

func appendIndexFingerprints(dst []uint64, value []byte, cfg Config) []uint64 {
	k := cfg.FingerprintK
	if k <= 0 || len(value) < k {
		return dst
	}
	limit := cfg.IndexFPCount
	if limit <= 0 {
		limit = cfg.MaxFingerprints
	}
	if cfg.LengthBucketMinLen > 0 && len(value) >= cfg.LengthBucketMinLen {
		dst = appendUniqueFP(dst, lengthFingerprint(len(value)), limit)
	}

	prefixLen := cfg.IndexPrefixBytes
	if prefixLen < k {
		prefixLen = k
	}
	suffixLen := cfg.IndexSuffixBytes
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
	dst = appendUniqueFP(dst, mixIndexFP(indexTagPrefix, xxh3.Hash(value[:segLen])), limit)

	if len(value) >= suffixLen {
		if suffixLen > len(value) {
			suffixLen = len(value)
		}
		dst = appendUniqueFP(dst, mixIndexFP(indexTagSuffix, xxh3.Hash(value[len(value)-suffixLen:])), limit)
	}

	// Add one or two middle probes to reduce collision/candidate list size while
	// keeping index cost O(1) per value.
	if len(dst) < limit && segLen < len(value) {
		start := (len(value) - segLen) / 2
		dst = appendUniqueFP(dst, mixIndexFP(indexTagMid1, xxh3.Hash(value[start:start+segLen])), limit)
	}
	if len(dst) < limit && segLen < len(value) {
		start := (len(value) - segLen) / 4
		if start > 0 && start+segLen <= len(value) {
			dst = appendUniqueFP(dst, mixIndexFP(indexTagMid2, xxh3.Hash(value[start:start+segLen])), limit)
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

// IndexFingerprints returns fingerprints used for bucketing and index lookups.
// Prefer AppendIndexFingerprints to avoid allocations.
func IndexFingerprints(value []byte, cfg Config) []uint64 {
	cfg = NormalizeConfig(cfg)
	limit := cfg.IndexFPCount
	if limit <= 0 {
		limit = cfg.MaxFingerprints
	}
	out := make([]uint64, 0, limit)
	return appendIndexFingerprints(out, value, cfg)
}

// AnchorFingerprints returns deterministic fingerprints for indexing a template
// definition based on its anchors.
func AnchorFingerprints(anchors [][]byte, cfg Config) []uint64 {
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
	if cfg.IndexFPCount > 0 && len(fps) > cfg.IndexFPCount {
		fps = fps[:cfg.IndexFPCount]
	}
	return fps
}
