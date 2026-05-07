package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil {
		return ErrServerClosed
	}
	if ln == nil {
		return net.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.registerListener(ln) {
		_ = ln.Close()
		return ErrServerClosed
	}
	defer s.unregisterListener(ln)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
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
	if server == nil || server.closed.Load() {
		return nil, nil, ErrServerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- server.ServeConn(ctx, right)
	}()
	client := NewClient(left)
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func() error {
		cleanupOnce.Do(func() {
			err := client.Close()
			err = errors.Join(err, right.Close())
			serveErr := <-done
			if serveErr != nil && ctx.Err() == nil && !errors.Is(serveErr, net.ErrClosed) && !errors.Is(serveErr, io.ErrClosedPipe) {
				err = errors.Join(err, serveErr)
			}
			cleanupErr = err
		})
		return cleanupErr
	}
	if err := client.Hello(ctx); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	return client, cleanup, nil
}
