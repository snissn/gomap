package mapredis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/tidwall/redcon"
)

func TestRedisServer_SetGet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)

	writeCommand(t, w, "SET", "k", "v")
	flush(t, w)
	if resp := readResp(t, r); resp.kind != '+' || resp.line != "OK" {
		t.Fatalf("SET resp = %#v", resp)
	}

	writeCommand(t, w, "GET", "k")
	flush(t, w)
	if resp := readResp(t, r); resp.kind != '$' || string(resp.bulk) != "v" {
		t.Fatalf("GET resp = %#v", resp)
	}

	writeCommand(t, w, "GET", "missing")
	flush(t, w)
	if resp := readResp(t, r); resp.kind != '$' || resp.bulk != nil {
		t.Fatalf("GET missing resp = %#v", resp)
	}

	writeCommand(t, w, "DEL", "k")
	flush(t, w)
	if resp := readResp(t, r); resp.kind != ':' || resp.int64 != 1 {
		t.Fatalf("DEL resp = %#v", resp)
	}
}

func TestRedisServer_ClientReplyOffOn(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)

	writeCommand(t, w, "CLIENT", "REPLY", "OFF")
	flush(t, w)

	// Redis-style behavior: no reply to CLIENT REPLY OFF itself.
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = r.ReadByte()
	if err == nil {
		t.Fatalf("expected no reply after CLIENT REPLY OFF")
	}
	if !isTimeout(err) {
		t.Fatalf("expected timeout, got %T %v", err, err)
	}
	_ = c.SetReadDeadline(time.Time{})

	// All subsequent replies should be suppressed.
	writeCommand(t, w, "PING")
	flush(t, w)
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = r.ReadByte()
	if err == nil {
		t.Fatalf("expected no reply to PING while reply-off")
	}
	if !isTimeout(err) {
		t.Fatalf("expected timeout, got %T %v", err, err)
	}
	_ = c.SetReadDeadline(time.Time{})

	// Turn replies back on.
	writeCommand(t, w, "CLIENT", "REPLY", "ON")
	flush(t, w)
	if resp := readResp(t, r); resp.kind != '+' || resp.line != "OK" {
		t.Fatalf("CLIENT REPLY ON resp = %#v", resp)
	}

	writeCommand(t, w, "PING")
	flush(t, w)
	if resp := readResp(t, r); resp.kind != '+' || resp.line != "PONG" {
		t.Fatalf("PING resp = %#v", resp)
	}
}

func startTestServer(t *testing.T) (addr string, stop func()) {
	t.Helper()

	srv := NewRedisServer("")
	rc := redcon.NewServer("127.0.0.1:0", srv.handle, nil, nil)

	sig := make(chan error, 1)
	go func() {
		_ = rc.ListenServeAndSignal(sig)
	}()
	if err := <-sig; err != nil {
		t.Fatalf("ListenServeAndSignal: %v", err)
	}

	return rc.Addr().String(), func() {
		_ = rc.Close()
	}
}

func writeCommand(t *testing.T, w *bufio.Writer, args ...string) {
	t.Helper()
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		t.Fatalf("write: %v", err)
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
}

func flush(t *testing.T, w *bufio.Writer) {
	t.Helper()
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

type resp struct {
	kind  byte
	line  string
	bulk  []byte
	int64 int64
}

func readResp(t *testing.T, r *bufio.Reader) resp {
	t.Helper()

	kind, err := r.ReadByte()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	switch kind {
	case '+', '-':
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read line: %v", err)
		}
		line = trimCRLF(line)
		return resp{kind: kind, line: line}

	case ':':
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read int: %v", err)
		}
		line = trimCRLF(line)
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			t.Fatalf("parse int: %v", err)
		}
		return resp{kind: kind, int64: n}

	case '$':
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk len: %v", err)
		}
		line = trimCRLF(line)
		n, err := strconv.Atoi(line)
		if err != nil {
			t.Fatalf("parse bulk len: %v", err)
		}
		if n == -1 {
			return resp{kind: kind}
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return resp{kind: kind, bulk: buf[:n]}

	default:
		t.Fatalf("unexpected kind: %q", kind)
		return resp{}
	}
}

func trimCRLF(s string) string {
	if len(s) >= 2 && s[len(s)-2] == '\r' {
		return s[:len(s)-2]
	}
	if len(s) >= 1 && s[len(s)-1] == '\n' {
		return s[:len(s)-1]
	}
	return s
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
}
