package nativewire

import (
	"fmt"
	"net"
	"os"
	"testing"
)

// This source-port/listener collision and socket-inode attribution are Linux-specific.
func TestFixedPeerListenerOutboundCollisionControlV1(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprintf("retained=%t", retained), func(t *testing.T) {
			address := fixedPeerReserveTestAddressV1(t, nil)
			if !retained {
				fixedPeerReleaseTestAddressV1(address)
			}
			sink, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			local, err := net.ResolveTCPAddr("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			conn, dialErr := (&net.Dialer{LocalAddr: local}).DialContext(t.Context(), "tcp", sink.Addr().String())
			if retained {
				if dialErr == nil {
					conn.Close()
					t.Fatal("outbound dial stole a reserved advertised role")
				}
				t.Logf("reserved role=control address=%s owner_pid=%d outbound_rejected=%v", address, os.Getpid(), dialErr)
				return
			}
			if dialErr != nil {
				t.Fatal(dialErr)
			}
			defer conn.Close()
			t.Logf("collision role=control advertised=%s actual_outbound_local=%s remote=%s owner_pid=%d", address, conn.LocalAddr(), conn.RemoteAddr(), os.Getpid())
			if tcp, ok := conn.(*net.TCPConn); ok {
				if raw, err := tcp.SyscallConn(); err == nil {
					_ = raw.Control(func(fd uintptr) {
						socket, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), fd))
						t.Logf("outbound fd=%d socket=%s", fd, socket)
					})
				}
			}
			if listeners, err := bindFixedPeerTCPListenersV1(FixedPeerTCPConfigV1{ListenAddress: address}, nil); err == nil {
				for _, listener := range listeners {
					listener.Close()
				}
				t.Fatal("released-fixture control did not reproduce runtime bind collision")
			} else {
				t.Logf("old bootstrap bind failure: %v", err)
			}
		})
	}
}
