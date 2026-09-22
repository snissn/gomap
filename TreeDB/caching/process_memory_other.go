//go:build !linux

package caching

func currentProcessRSSBytes() (rssBytes uint64, rssHWMBytes uint64, ok bool) {
	return 0, 0, false
}
