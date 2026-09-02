//go:build !windows && !aix && !android && !darwin && !dragonfly && !freebsd && !illumos && !ios && !linux && !netbsd && !openbsd && !solaris

package nativewire

import "net"

func vectorPartitionProductionListenerIPv6OnlyV1(*net.TCPListener) (bool, bool) {
	return false, false
}
