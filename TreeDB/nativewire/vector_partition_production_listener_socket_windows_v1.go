//go:build windows

package nativewire

import (
	"net"
	"syscall"
)

func vectorPartitionProductionListenerIPv6OnlyV1(listener *net.TCPListener) (bool, bool) {
	raw, err := listener.SyscallConn()
	if err != nil {
		return false, false
	}
	var value int
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		value, socketErr = syscall.GetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY)
	})
	return value != 0, err == nil && socketErr == nil
}
