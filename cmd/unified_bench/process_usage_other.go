//go:build !linux

package main

import "errors"

const processUsageSupported = false

func readProcessUsage() (processUsageCounters, error) {
	return processUsageCounters{}, errors.New("process usage diagnostic is supported only on Linux")
}
