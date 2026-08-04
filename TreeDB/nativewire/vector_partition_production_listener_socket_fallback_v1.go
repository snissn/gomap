//go:build js || plan9 || wasip1

package nativewire

import "net"

func vectorPartitionProductionListenerIPv6OnlyV1(*net.TCPListener) (bool, bool) {
	return false, false
}
