package nativewire

import (
	"context"
	"errors"
	"net"
)

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil {
		return ErrServerClosed
	}
	if ln == nil {
		return net.ErrClosed
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if s.closed.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func() {
			_ = s.ServeConn(ctx, conn)
		}()
	}
}

func DialContext(ctx context.Context, network, address string) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	client := NewClient(conn)
	if err := client.Hello(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func NewInProcessClient(ctx context.Context, server *Server) (*Client, func() error, error) {
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- server.ServeConn(ctx, right)
	}()
	client := NewClient(left)
	if err := client.Hello(ctx); err != nil {
		_ = left.Close()
		_ = right.Close()
		return nil, nil, err
	}
	cleanup := func() error {
		err := client.Close()
		select {
		case serveErr := <-done:
			if serveErr != nil && ctx.Err() == nil {
				err = errors.Join(err, serveErr)
			}
		default:
		}
		return err
	}
	return client, cleanup, nil
}
