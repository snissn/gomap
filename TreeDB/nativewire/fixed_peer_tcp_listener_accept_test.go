package nativewire

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type fixedPeerTemporaryAcceptListenerV1 struct {
	*fixedPeerCountedListenerV1
	failures int32
	attempts atomic.Int32
	failed   chan int32
}

func (l *fixedPeerTemporaryAcceptListenerV1) Accept() (net.Conn, error) {
	attempt := l.attempts.Add(1)
	if attempt <= l.failures {
		if l.failed != nil {
			l.failed <- attempt
		}
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: temporaryAcceptErrorV1{}}
	}
	return l.TCPListener.Accept()
}

func TestFixedPeerDormantListenerTemporaryAcceptRefusalV1(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	listener := &fixedPeerTemporaryAcceptListenerV1{fixedPeerCountedListenerV1: &fixedPeerCountedListenerV1{TCPListener: raw.(*net.TCPListener)}, failures: 1}
	reservation := reserveFixedPeerTCPListenerV1(listener).(*fixedPeerTCPReservationV1)
	t.Cleanup(func() { reservation.Close() })
	conn, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = conn.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("dormant role served traffic")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		stopped := false
		select {
		case <-reservation.done:
			stopped = true
		default:
		}
		t.Fatalf("temporary Accept stopped refusal: address=%s attempts=%d pump_done=%t actual_read=%v", raw.Addr(), listener.attempts.Load(), stopped, err)
	}
	if listener.attempts.Load() < 2 {
		t.Fatal("temporary Accept was not retried")
	}
}

func TestFixedPeerDormantListenerInterruptsAcceptBackoffV1(t *testing.T) {
	for _, take := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "take"}[take], func(t *testing.T) {
			raw, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { raw.Close() })
			listener := &fixedPeerTemporaryAcceptListenerV1{fixedPeerCountedListenerV1: &fixedPeerCountedListenerV1{TCPListener: raw.(*net.TCPListener)}, failures: 9, failed: make(chan int32, 9)}
			reservation := reserveFixedPeerTCPListenerV1(listener).(*fixedPeerTCPReservationV1)
			// Nine temporary errors reach the one-second backoff cap.
			for attempt := int32(1); attempt <= 9; attempt++ {
				select {
				case got := <-listener.failed:
					if got != attempt {
						t.Fatalf("Accept attempt=%d want=%d", got, attempt)
					}
				case <-reservation.done:
					t.Fatal("temporary Accept stopped the pump")
				case <-time.After(3 * time.Second):
					t.Fatal("temporary Accept did not reach backoff")
				}
			}
			stopped := make(chan error, 1)
			go func() {
				if take {
					serving, err := reservation.takeV1()
					if err == nil {
						if serving != listener {
							err = net.ErrClosed
						}
						_ = serving.Close()
					}
					stopped <- err
				} else {
					stopped <- reservation.Close()
				}
			}()
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatal("take/Close waited for the one-second Accept backoff")
			}
			select {
			case <-reservation.done:
			default:
				t.Fatal("pump survived take/Close")
			}
			if listener.closes.Load() != 1 {
				t.Fatalf("owned socket closed %d times", listener.closes.Load())
			}
			probe, err := net.Listen("tcp", raw.Addr().String())
			if err != nil {
				t.Fatalf("take/Close retained actual address: %v", err)
			}
			_ = probe.Close()
		})
	}
}
