//go:build !linux

package pager

func madviseChunk(_ []byte) {}
