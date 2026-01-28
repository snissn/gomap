//go:build !linux

package pager

func madviseWillNeedChunk(_ []byte) {}
