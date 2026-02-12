package db

import (
	"bytes"
	"sort"
)

// NormalizeValueLogDomainThresholds returns a deterministic longest-prefix-first
// copy suitable for hot-path threshold lookups.
//
// It filters invalid entries (empty prefix or negative thresholds) and
// de-duplicates identical prefixes after sorting.
func NormalizeValueLogDomainThresholds(in []ValueLogDomainThreshold) []ValueLogDomainThreshold {
	if len(in) == 0 {
		return nil
	}
	out := make([]ValueLogDomainThreshold, 0, len(in))
	for i := range in {
		d := in[i]
		if len(d.Prefix) == 0 || d.InlineThreshold < 0 {
			continue
		}
		cp := ValueLogDomainThreshold{
			Prefix:          append([]byte(nil), d.Prefix...),
			InlineThreshold: d.InlineThreshold,
		}
		out = append(out, cp)
	}
	if len(out) == 0 {
		return nil
	}
	sort.SliceStable(out, func(i, j int) bool {
		if len(out[i].Prefix) != len(out[j].Prefix) {
			return len(out[i].Prefix) > len(out[j].Prefix)
		}
		return bytes.Compare(out[i].Prefix, out[j].Prefix) < 0
	})

	dedup := out[:0]
	for i := range out {
		if len(dedup) > 0 && bytes.Equal(dedup[len(dedup)-1].Prefix, out[i].Prefix) {
			continue
		}
		dedup = append(dedup, out[i])
	}
	return dedup
}

// ResolveInlineThresholdForKey chooses an inline threshold for key using
// longest-prefix domain overrides and a global fallback.
//
// Callers should pass NormalizeValueLogDomainThresholds output so that the
// first match is the intended longest-prefix override.
func ResolveInlineThresholdForKey(baseThreshold int, key []byte, domains []ValueLogDomainThreshold) int {
	for i := range domains {
		d := domains[i]
		if len(d.Prefix) == 0 {
			continue
		}
		if bytes.HasPrefix(key, d.Prefix) {
			return d.InlineThreshold
		}
	}
	return baseThreshold
}
