package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	addr      string
	label     string
	clients   int
	requests  int
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
	flag.IntVar(&cfg.pipeline, "pipeline", 64, "pipeline depth")
	flag.IntVar(&cfg.keyspace, "keyspace", 100000, "keyspace size")
	flag.IntVar(&cfg.valueSize, "value-size", 128, "value size in bytes")
	flag.StringVar(&cfg.keyPrefix, "key-prefix", "nr:", "key prefix")
	flag.Int64Var(&cfg.seed, "seed", 1, "rng seed")
	flag.BoolVar(&cfg.resp3, "resp3", false, "send HELLO 3 before benchmarking")
	flag.BoolVar(&cfg.replyOff, "reply-off", true, "send CLIENT REPLY OFF before benchmarking")
	flag.Parse()

	if cfg.clients <= 0 || cfg.requests <= 0 || cfg.pipeline <= 0 || cfg.keyspace <= 0 {
		fmt.Printf("invalid config: clients=%d requests=%d pipeline=%d keyspace=%d\n", cfg.clients, cfg.requests, cfg.pipeline, cfg.keyspace)
		return
	}

	value := bytes.Repeat([]byte("v"), cfg.valueSize)
	valLen := strconv.Itoa(len(value))

	var total atomic.Uint64
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.clients)

	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := runClient(cfg, id, value, valLen, &total); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		fmt.Printf("error: %v\n", err)
		return
	}

	elapsed := time.Since(start)
	totalReq := total.Load()
	rps := float64(totalReq) / elapsed.Seconds()
	fmt.Printf("result label=%s total=%d seconds=%.2f rps=%.2f\n", cfg.label, totalReq, elapsed.Seconds(), rps)
}

func runClient(cfg config, id int, value []byte, valLen string, total *atomic.Uint64) error {
	conn, err := net.Dial("tcp", cfg.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	w := bufio.NewWriterSize(conn, 1<<20)
	r := bufio.NewReader(conn)

	if cfg.resp3 {
		if err := writeCommand(w, "HELLO", "3"); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if _, err := readResp(r); err != nil {
			return err
		}
	}

	if cfg.replyOff {
		if err := writeCommand(w, "CLIENT", "REPLY", "OFF"); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if _, err := readResp(r); err != nil {
			return err
		}
	}

	rng := rand.New(rand.NewSource(cfg.seed + int64(id)))
	var keyBuf [32]byte

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
		if err := w.Flush(); err != nil {
			return err
		}
		total.Add(uint64(batch))
		remaining -= batch
	}

	return nil
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

func readResp(r *bufio.Reader) (byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch b {
	case '+', '-', ':':
		if _, err := r.ReadString('\n'); err != nil {
			return 0, err
		}
		return b, nil
	case '$':
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return 0, err
		}
		if n == -1 {
			return b, nil
		}
		buf := make([]byte, n+2)
		_, err = io.ReadFull(r, buf)
		return b, err
	case '*':
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return 0, err
		}
		for i := 0; i < n; i++ {
			if _, err := readResp(r); err != nil {
				return 0, err
			}
		}
		return b, nil
	case '%':
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return 0, err
		}
		for i := 0; i < n*2; i++ {
			if _, err := readResp(r); err != nil {
				return 0, err
			}
		}
		return b, nil
	default:
		return b, nil
	}
}
