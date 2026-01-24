package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	addr      string
	label     string
	clients   int
	requests  int
	testTime  time.Duration
	pipeline  int
	keyspace  int
	valueSize int
	keyPrefix string
	seed      int64
	resp3     bool
	replyOff  bool
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:6380", "server address")
	flag.StringVar(&cfg.label, "label", "run", "label to include in output")
	flag.IntVar(&cfg.clients, "clients", 64, "number of client connections")
	flag.IntVar(&cfg.requests, "requests", 100000, "requests per client")
	flag.DurationVar(&cfg.testTime, "test-time", 0, "run for this duration (overrides -requests when > 0)")
	flag.IntVar(&cfg.pipeline, "pipeline", 64, "pipeline depth")
	flag.IntVar(&cfg.keyspace, "keyspace", 100000, "keyspace size")
	flag.IntVar(&cfg.valueSize, "value-size", 128, "value size in bytes")
	flag.StringVar(&cfg.keyPrefix, "key-prefix", "nr:", "key prefix")
	flag.Int64Var(&cfg.seed, "seed", 1, "rng seed")
	flag.BoolVar(&cfg.resp3, "resp3", false, "send HELLO 3 before benchmarking")
	flag.BoolVar(&cfg.replyOff, "reply-off", true, "send CLIENT REPLY OFF before benchmarking")
	flag.Parse()

	if cfg.clients <= 0 || cfg.pipeline <= 0 || cfg.keyspace <= 0 {
		fmt.Fprintf(os.Stderr, "invalid config: clients=%d pipeline=%d keyspace=%d\n", cfg.clients, cfg.pipeline, cfg.keyspace)
		os.Exit(2)
	}
	if cfg.testTime <= 0 && cfg.requests <= 0 {
		fmt.Fprintf(os.Stderr, "invalid config: requests=%d test-time=%s\n", cfg.requests, cfg.testTime)
		os.Exit(2)
	}

	value := bytes.Repeat([]byte("v"), cfg.valueSize)
	valLen := strconv.Itoa(len(value))

	var total atomic.Uint64
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	startCh := make(chan struct{})
	errCh := make(chan error, cfg.clients)

	ready.Add(cfg.clients)
	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := runClient(cfg, id, value, valLen, startCh, &ready, &total); err != nil {
				errCh <- err
			}
		}(i)
	}

	ready.Wait()
	start := time.Now()
	close(startCh)
	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	totalReq := total.Load()
	rps := float64(totalReq) / elapsed.Seconds()
	fmt.Printf("result label=%s total=%d seconds=%.2f rps=%.2f\n", cfg.label, totalReq, elapsed.Seconds(), rps)
}

func runClient(cfg config, id int, value []byte, valLen string, startCh <-chan struct{}, ready *sync.WaitGroup, total *atomic.Uint64) error {
	readyDone := false
	defer func() {
		if !readyDone {
			ready.Done()
		}
	}()

	conn, err := net.Dial("tcp", cfg.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Keep per-connection buffers modest; large client counts can otherwise
	// exhaust kernel mbufs on loopback (macOS shows ENOBUFS).
	w := bufio.NewWriterSize(conn, 64<<10)
	r := bufio.NewReader(conn)

	if cfg.resp3 {
		if err := writeCommand(w, "HELLO", "3"); err != nil {
			return err
		}
		if err := flushWithBackoff(w); err != nil {
			return err
		}
		resp, err := readResp(r)
		if err != nil {
			return err
		}
		if resp.kind == '-' {
			return fmt.Errorf("HELLO failed: %s", resp.line)
		}
	}

	if cfg.replyOff {
		if err := writeCommand(w, "CLIENT", "REPLY", "OFF"); err != nil {
			return err
		}
		if err := flushWithBackoff(w); err != nil {
			return err
		}

		// Redis-style behavior is to suppress the reply to CLIENT REPLY OFF itself.
		// Some servers may still reply +OK; accept either by best-effort reading.
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if resp, err := readResp(r); err == nil {
			if resp.kind == '-' {
				return fmt.Errorf("CLIENT REPLY OFF failed: %s", resp.line)
			}
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// Expected: no reply.
		} else {
			return err
		}
		_ = conn.SetReadDeadline(time.Time{})

		// Verify replies are actually suppressed by sending a PING and ensuring
		// we don't receive any response.
		if err := writeCommand(w, "PING"); err != nil {
			return err
		}
		if err := flushWithBackoff(w); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if resp, err := readResp(r); err == nil {
			return fmt.Errorf("expected no reply after CLIENT REPLY OFF, got kind=%q line=%q", resp.kind, resp.line)
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// OK.
		} else {
			return err
		}
		_ = conn.SetReadDeadline(time.Time{})
	}

	readyDone = true
	ready.Done()
	<-startCh

	rng := rand.New(rand.NewSource(cfg.seed + int64(id)))
	var keyBuf [32]byte

	if cfg.testTime > 0 {
		deadline := time.Now().Add(cfg.testTime)
		for time.Now().Before(deadline) {
			for i := 0; i < cfg.pipeline; i++ {
				keyID := rng.Intn(cfg.keyspace)
				key := buildKey(keyBuf[:0], cfg.keyPrefix, keyID)
				if err := writeSet(w, key, value, valLen); err != nil {
					return err
				}
			}
			if err := flushWithBackoff(w); err != nil {
				return err
			}
			total.Add(uint64(cfg.pipeline))
		}
		return nil
	}

	remaining := cfg.requests
	for remaining > 0 {
		batch := cfg.pipeline
		if remaining < batch {
			batch = remaining
		}
		for i := 0; i < batch; i++ {
			keyID := rng.Intn(cfg.keyspace)
			key := buildKey(keyBuf[:0], cfg.keyPrefix, keyID)
			if err := writeSet(w, key, value, valLen); err != nil {
				return err
			}
		}
		if err := flushWithBackoff(w); err != nil {
			return err
		}
		total.Add(uint64(batch))
		remaining -= batch
	}

	return nil
}

func flushWithBackoff(w *bufio.Writer) error {
	backoff := 50 * time.Microsecond
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := w.Flush()
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.ENOBUFS) {
			if time.Now().After(deadline) {
				return fmt.Errorf("write: ENOBUFS (try lowering -clients/-pipeline or increasing OS socket buffers): %w", err)
			}
			time.Sleep(backoff)
			if backoff < 10*time.Millisecond {
				backoff *= 2
			}
			continue
		}
		return err
	}
}

func writeCommand(w *bufio.Writer, args ...string) error {
	if _, err := w.WriteString("*"); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.Itoa(len(args))); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := w.WriteString("$"); err != nil {
			return err
		}
		if _, err := w.WriteString(strconv.Itoa(len(arg))); err != nil {
			return err
		}
		if _, err := w.WriteString("\r\n"); err != nil {
			return err
		}
		if _, err := w.WriteString(arg); err != nil {
			return err
		}
		if _, err := w.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeSet(w *bufio.Writer, key []byte, value []byte, valLen string) error {
	if _, err := w.WriteString("*3\r\n$3\r\nSET\r\n$"); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.Itoa(len(key))); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n$"); err != nil {
		return err
	}
	if _, err := w.WriteString(valLen); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}
	return nil
}

func buildKey(dst []byte, prefix string, id int) []byte {
	dst = append(dst, prefix...)
	dst = strconv.AppendInt(dst, int64(id), 10)
	return dst
}

type resp struct {
	kind byte
	line string
}

func readResp(r *bufio.Reader) (resp, error) {
	b, err := r.ReadByte()
	if err != nil {
		return resp{}, err
	}
	switch b {
	case '+', '-', ':':
		line, err := r.ReadString('\n')
		if err != nil {
			return resp{}, err
		}
		// Strip trailing CRLF if present.
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			line = line[:len(line)-2]
		}
		return resp{kind: b, line: line}, nil
	case '$':
		line, err := r.ReadString('\n')
		if err != nil {
			return resp{}, err
		}
		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return resp{}, err
		}
		if n == -1 {
			return resp{kind: b}, nil
		}
		buf := make([]byte, n+2)
		_, err = io.ReadFull(r, buf)
		return resp{kind: b}, err
	case '*':
		line, err := r.ReadString('\n')
		if err != nil {
			return resp{}, err
		}
		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return resp{}, err
		}
		for i := 0; i < n; i++ {
			if _, err := readResp(r); err != nil {
				return resp{}, err
			}
		}
		return resp{kind: b}, nil
	case '%':
		line, err := r.ReadString('\n')
		if err != nil {
			return resp{}, err
		}
		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return resp{}, err
		}
		for i := 0; i < n*2; i++ {
			if _, err := readResp(r); err != nil {
				return resp{}, err
			}
		}
		return resp{kind: b}, nil
	default:
		return resp{kind: b}, nil
	}
}
