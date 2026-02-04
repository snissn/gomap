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

// RoutingFingerprints returns fingerprints used for routing/bucketing.
// It prefers prefix+suffix slices to avoid random middle bytes dominating.
func RoutingFingerprints(value []byte, cfg Config) []uint64 {
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

// BucketKey computes a deterministic bucket key from fingerprints.
func BucketKey(fps []uint64) uint64 {
	if len(fps) == 0 {
		return 0
	}
	limit := 8
	if len(fps) < limit {
		limit = len(fps)
	}
	buf := make([]byte, limit*8)
	for i := 0; i < limit; i++ {
		binary.LittleEndian.PutUint64(buf[i*8:(i+1)*8], fps[i])
	}
	return xxh3.Hash(buf)
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
