//go:build !linux

package slab

func adviseHugepage(_ []byte) {}
