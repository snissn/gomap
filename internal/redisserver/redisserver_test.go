package redisserver

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

type respClient struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func newRespClient(t *testing.T, addr string) *respClient {
	t.Helper()
	var conn net.Conn
	var err error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &respClient{
		t: t,
		c: conn,
		r: bufio.NewReader(conn),
	}
}

func (c *respClient) Close() {
	_ = c.c.Close()
}

func (c *respClient) Do(args ...[]byte) respValue {
	c.t.Helper()
	writeCommand(c.t, c.c, args)
	v, err := readResp(c.r)
	if err != nil {
		c.t.Fatalf("read resp: %v", err)
	}
	return v
}

func (c *respClient) DoRaw(raw []byte) {
	c.t.Helper()
	if _, err := c.c.Write(raw); err != nil {
		c.t.Fatalf("write raw: %v", err)
	}
}

type respValue struct {
	kind  byte
	str   string
	bulk  []byte
	num   int64
	array []respValue
	err   error
}

func writeCommand(t *testing.T, w net.Conn, args [][]byte) {
	if t != nil {
		t.Helper()
	}
	var buf bytes.Buffer
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(len(args)))
	buf.WriteString("\r\n")
	for _, arg := range args {
		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(len(arg)))
		buf.WriteString("\r\n")
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		if t != nil {
			t.Fatalf("write command: %v", err)
		}
	}
}

func readResp(r *bufio.Reader) (respValue, error) {
	b, err := r.ReadByte()
	if err != nil {
		return respValue{}, err
	}
	switch b {
	case '+':
		line, err := readLine(r)
		return respValue{kind: '+', str: line}, err
	case '-':
		line, err := readLine(r)
		return respValue{kind: '-', str: line, err: errors.New(line)}, err
	case ':':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return respValue{}, err
		}
		return respValue{kind: ':', num: n}, nil
	case '$':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, err
		}
		if n == -1 {
			return respValue{kind: '$'}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return respValue{}, err
		}
		return respValue{kind: '$', bulk: buf[:n]}, nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, err
		}
		if n == -1 {
			return respValue{kind: '*'}, nil
		}
		arr := make([]respValue, 0, n)
		for i := 0; i < n; i++ {
			v, err := readResp(r)
			if err != nil {
				return respValue{}, err
			}
			arr = append(arr, v)
		}
		return respValue{kind: '*', array: arr}, nil
	default:
		return respValue{}, fmt.Errorf("unexpected resp prefix: %q", b)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("invalid line ending")
	}
	return line[:len(line)-2], nil
}

func startTestServer(t *testing.T, engine string, batch bool) (addr string, cleanup func()) {
	return startTestServerWithBatchMode(t, engine, batch, true)
}

func startTestServerWithBatchMode(t *testing.T, engine string, batch bool, flushOnNonset bool) (addr string, cleanup func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		Addr:                    "127.0.0.1:0",
		Dir:                     dir,
		Engine:                  engine,
		BatchSets:               batch,
		BatchSize:               4,
		BatchFlushOnNonset:      flushOnNonset,
		BatchFlushOnNonsetSet:   true,
		TreeDBFlushThreshold:    4 * 1024 * 1024,
		TreeDBValueLogThreshold: 0,
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		_ = srv.Close()
	}
}

func TestSETGET(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			if v := c.Do([]byte("SET"), []byte("k1"), []byte("v1")); v.kind != '+' {
				t.Fatalf("SET failed: %#v", v)
			}
			v := c.Do([]byte("GET"), []byte("k1"))
			if v.kind != '$' || string(v.bulk) != "v1" {
				t.Fatalf("GET mismatch: %#v", v)
			}
			v = c.Do([]byte("GET"), []byte("missing"))
			if v.kind != '$' || v.bulk != nil {
				t.Fatalf("GET missing expected null: %#v", v)
			}
		})
	}
}

func TestHelloResp3(t *testing.T) {
	addr, cleanup := startTestServer(t, "hashdb", false)
	defer cleanup()
	c := newRespClient(t, addr)
	defer c.Close()

	v := c.Do([]byte("HELLO"), []byte("3"))
	if v.kind != '+' || v.str != "OK" {
		t.Fatalf("HELLO resp3 mismatch: %#v", v)
	}
}

func TestClientReplyOffSuppressesOK(t *testing.T) {
	addr, cleanup := startTestServer(t, "hashdb", false)
	defer cleanup()
	c := newRespClient(t, addr)
	defer c.Close()

	writeCommand(nil, c.c, [][]byte{[]byte("CLIENT"), []byte("REPLY"), []byte("OFF")})
	_ = c.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err := readResp(c.r)
	if err == nil {
		t.Fatalf("expected no reply to CLIENT REPLY OFF")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
	_ = c.c.SetReadDeadline(time.Time{})

	writeCommand(nil, c.c, [][]byte{[]byte("SET"), []byte("k1"), []byte("v1")})
	_ = c.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = readResp(c.r)
	if err == nil {
		t.Fatalf("expected no reply after CLIENT REPLY OFF")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
	_ = c.c.SetReadDeadline(time.Time{})
}

func TestClientReplyOffStillReturnsErrors(t *testing.T) {
	addr, cleanup := startTestServer(t, "hashdb", false)
	defer cleanup()
	c := newRespClient(t, addr)
	defer c.Close()

	writeCommand(nil, c.c, [][]byte{[]byte("CLIENT"), []byte("REPLY"), []byte("OFF")})
	_ = c.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err := readResp(c.r)
	if err == nil {
		t.Fatalf("expected no reply to CLIENT REPLY OFF")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
	_ = c.c.SetReadDeadline(time.Time{})

	writeCommand(nil, c.c, [][]byte{[]byte("GET")})
	v, err := readResp(c.r)
	if err != nil {
		t.Fatalf("read resp: %v", err)
	}
	if v.kind != '-' {
		t.Fatalf("expected error reply, got %#v", v)
	}
}

func TestClientReplyOnRestoresReplies(t *testing.T) {
	addr, cleanup := startTestServer(t, "hashdb", false)
	defer cleanup()
	c := newRespClient(t, addr)
	defer c.Close()

	writeCommand(nil, c.c, [][]byte{[]byte("CLIENT"), []byte("REPLY"), []byte("OFF")})
	_ = c.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err := readResp(c.r)
	if err == nil {
		t.Fatalf("expected no reply to CLIENT REPLY OFF")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
	_ = c.c.SetReadDeadline(time.Time{})

	v := c.Do([]byte("CLIENT"), []byte("REPLY"), []byte("ON"))
	if v.kind != '+' || v.str != "OK" {
		t.Fatalf("CLIENT REPLY ON mismatch: %#v", v)
	}

	v = c.Do([]byte("PING"))
	if v.kind != '+' || v.str != "PONG" {
		t.Fatalf("PING mismatch after reply on: %#v", v)
	}
}

func TestMSETMGET(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			if v := c.Do([]byte("MSET"), []byte("a"), []byte("1"), []byte("b"), []byte("2")); v.kind != '+' {
				t.Fatalf("MSET failed: %#v", v)
			}
			v := c.Do([]byte("MGET"), []byte("a"), []byte("b"), []byte("c"))
			if v.kind != '*' || len(v.array) != 3 {
				t.Fatalf("MGET response: %#v", v)
			}
			if string(v.array[0].bulk) != "1" || string(v.array[1].bulk) != "2" || v.array[2].bulk != nil {
				t.Fatalf("MGET values: %#v", v.array)
			}
		})
	}
}

func TestINCR(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			v := c.Do([]byte("INCR"), []byte("num"))
			if v.kind != ':' || v.num != 1 {
				t.Fatalf("INCR expected 1: %#v", v)
			}
			v = c.Do([]byte("INCRBY"), []byte("num"), []byte("2"))
			if v.kind != ':' || v.num != 3 {
				t.Fatalf("INCRBY expected 3: %#v", v)
			}
			v = c.Do([]byte("DECR"), []byte("num"))
			if v.kind != ':' || v.num != 2 {
				t.Fatalf("DECR expected 2: %#v", v)
			}
			v = c.Do([]byte("DECRBY"), []byte("num"), []byte("5"))
			if v.kind != ':' || v.num != -3 {
				t.Fatalf("DECRBY expected -3: %#v", v)
			}

			_ = c.Do([]byte("SET"), []byte("bad"), []byte("abc"))
			v = c.Do([]byte("INCR"), []byte("bad"))
			if v.kind != '-' {
				t.Fatalf("INCR on non-int should error: %#v", v)
			}
		})
	}
}

func TestBatchSetsFlushOnClose(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServerWithBatchMode(t, engine, true, false)
			defer cleanup()

			c := newRespClient(t, addr)
			// Write raw SETs without reading responses to simulate pipelining.
			c.DoRaw(multiSetRaw([][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}}))
			// Ensure the server processed the pipeline before closing the connection.
			v := c.Do([]byte("PING"))
			if v.kind != '+' {
				t.Fatalf("PING failed: %#v", v)
			}
			c.Close()

			c2 := newRespClient(t, addr)
			defer c2.Close()
			// Flush-on-close happens asynchronously from the client's perspective.
			// Poll briefly to avoid flakes under slower CI scheduling.
			deadline := time.Now().Add(1 * time.Second)
			for {
				v = c2.Do([]byte("GET"), []byte("k3"))
				if v.kind == '$' && string(v.bulk) == "v3" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("expected flushed batch: %#v", v)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func multiSetRaw(pairs [][2]string) []byte {
	var buf bytes.Buffer
	for _, pair := range pairs {
		buf.WriteString("*3\r\n$")
		buf.WriteString(strconv.Itoa(len("SET")))
		buf.WriteString("\r\nSET\r\n$")
		buf.WriteString(strconv.Itoa(len(pair[0])))
		buf.WriteString("\r\n")
		buf.WriteString(pair[0])
		buf.WriteString("\r\n$")
		buf.WriteString(strconv.Itoa(len(pair[1])))
		buf.WriteString("\r\n")
		buf.WriteString(pair[1])
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

func TestFLUSHDB(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			_ = c.Do([]byte("SET"), []byte("k1"), []byte("v1"))
			v := c.Do([]byte("FLUSHDB"))
			if v.kind != '+' {
				t.Fatalf("FLUSHDB failed: %#v", v)
			}
			v = c.Do([]byte("GET"), []byte("k1"))
			if v.kind != '$' || v.bulk != nil {
				t.Fatalf("expected empty after FLUSHDB: %#v", v)
			}
		})
	}
}

func TestTreeDBCommandWALAdminCompactionUnsupported(t *testing.T) {
	addr, cleanup := startTestServer(t, "treedb", false)
	defer cleanup()
	c := newRespClient(t, addr)
	defer c.Close()

	for _, command := range []string{"COMPACT", "BGREWRITEAOF"} {
		t.Run(command, func(t *testing.T) {
			v := c.Do([]byte(command))
			if v.kind != '-' || v.str != "ERR unsupported" {
				t.Fatalf("%s response=%#v, want ERR unsupported", command, v)
			}
		})
	}
}

func TestScanAndKeys(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			_ = c.Do([]byte("MSET"), []byte("aa"), []byte("1"), []byte("ab"), []byte("2"), []byte("bb"), []byte("3"))

			keys := c.Do([]byte("KEYS"), []byte("a*"))
			if keys.kind != '*' || len(keys.array) < 2 {
				t.Fatalf("KEYS response: %#v", keys)
			}

			scan := c.Do([]byte("SCAN"), []byte("0"), []byte("MATCH"), []byte("a*"), []byte("COUNT"), []byte("10"))
			if scan.kind != '*' || len(scan.array) != 2 {
				t.Fatalf("SCAN response: %#v", scan)
			}
			if scan.array[1].kind != '*' {
				t.Fatalf("SCAN keys response: %#v", scan)
			}
		})
	}
}

func TestStringOps(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			v := c.Do([]byte("SETNX"), []byte("k1"), []byte("v1"))
			if v.kind != ':' || v.num != 1 {
				t.Fatalf("SETNX expected 1: %#v", v)
			}
			v = c.Do([]byte("SETNX"), []byte("k1"), []byte("v2"))
			if v.kind != ':' || v.num != 0 {
				t.Fatalf("SETNX expected 0: %#v", v)
			}

			v = c.Do([]byte("GETSET"), []byte("k1"), []byte("v3"))
			if v.kind != '$' || string(v.bulk) != "v1" {
				t.Fatalf("GETSET old value: %#v", v)
			}

			v = c.Do([]byte("GETDEL"), []byte("k1"))
			if v.kind != '$' || string(v.bulk) != "v3" {
				t.Fatalf("GETDEL value: %#v", v)
			}
			v = c.Do([]byte("GET"), []byte("k1"))
			if v.kind != '$' || v.bulk != nil {
				t.Fatalf("GET after GETDEL expected null: %#v", v)
			}

			_ = c.Do([]byte("SET"), []byte("k2"), []byte("hi"))
			v = c.Do([]byte("APPEND"), []byte("k2"), []byte("!"))
			if v.kind != ':' || v.num != 3 {
				t.Fatalf("APPEND length: %#v", v)
			}
			v = c.Do([]byte("STRLEN"), []byte("k2"))
			if v.kind != ':' || v.num != 3 {
				t.Fatalf("STRLEN: %#v", v)
			}

			_ = c.Do([]byte("SET"), []byte("k3"), []byte("hello"))
			v = c.Do([]byte("GETRANGE"), []byte("k3"), []byte("1"), []byte("3"))
			if v.kind != '$' || string(v.bulk) != "ell" {
				t.Fatalf("GETRANGE: %#v", v)
			}
			v = c.Do([]byte("SETRANGE"), []byte("k3"), []byte("6"), []byte("!"))
			if v.kind != ':' || v.num != 7 {
				t.Fatalf("SETRANGE len: %#v", v)
			}
			v = c.Do([]byte("GET"), []byte("k3"))
			if v.kind != '$' || string(v.bulk) != "hello\x00!" {
				t.Fatalf("SETRANGE result: %#v", v)
			}
		})
	}
}

func TestRenameAndDBSize(t *testing.T) {
	for _, engine := range []string{"hashdb", "treedb"} {
		t.Run(engine, func(t *testing.T) {
			addr, cleanup := startTestServer(t, engine, false)
			defer cleanup()
			c := newRespClient(t, addr)
			defer c.Close()

			_ = c.Do([]byte("MSET"), []byte("a"), []byte("1"), []byte("b"), []byte("2"))
			v := c.Do([]byte("DBSIZE"))
			if v.kind != ':' || v.num < 2 {
				t.Fatalf("DBSIZE: %#v", v)
			}

			v = c.Do([]byte("RENAME"), []byte("a"), []byte("c"))
			if v.kind != '+' {
				t.Fatalf("RENAME: %#v", v)
			}

			v = c.Do([]byte("RENAMENX"), []byte("b"), []byte("c"))
			if v.kind != ':' || v.num != 0 {
				t.Fatalf("RENAMENX should fail: %#v", v)
			}
		})
	}
}
