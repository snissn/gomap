package stress

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCompaction(t *testing.T) {
	addr := pickFreeTCPAddr(t)

	// 1. Setup
	rootDir, _ := os.Getwd()
	// When running `go test`, the working directory is typically `HashDB/stress`.
	rootDir = filepath.Dir(rootDir)
	serverBin := filepath.Join(rootDir, "redisserver_bin_compact")
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(serverBin), ".exe") {
		serverBin += ".exe"
	}
	buildCmd := exec.Command("go", "build", "-o", serverBin, "redisserver/main.go")
	buildCmd.Dir = rootDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build server: %v\n%s", err, out)
	}
	defer os.Remove(serverBin)

	dbDir, err := os.MkdirTemp("", "compact-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbDir)

	serverCmd := startServer(t, serverBin, dbDir, addr)
	defer func() {
		serverCmd.Process.Kill()
		serverCmd.Wait()
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// 2. Generate Garbage (Updates)
	keyCount := 1000
	updates := 20
	// Write initial
	for i := 0; i < keyCount; i++ {
		setKey(t, conn, reader, i, 0)
	}

	// Check initial size
	sizeBefore := getDirSize(t, dbDir)
	t.Logf("Size after initial write: %d bytes", sizeBefore)

	// Update many times
	for u := 1; u <= updates; u++ {
		for i := 0; i < keyCount; i++ {
			setKey(t, conn, reader, i, u)
		}
		// Force flush to generate garbage on disk (prevent CacheKV coalescing)
		conn.Write([]byte("*1\r\n$4\r\nSAVE\r\n"))
		reader.ReadString('\n') // +OK (ignore error for brevity/speed)
	}

	sizeBloated := getDirSize(t, dbDir)
	t.Logf("Size after updates: %d bytes", sizeBloated)

	if sizeBloated <= sizeBefore*2 {
		t.Log("Warning: File didn't grow as expected? Maybe slab allocation is large or compression handles it?")
	}

	// 2.5. Flush to ensure we measure real bloated size
	conn.Write([]byte("*1\r\n$4\r\nSAVE\r\n"))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp, "OK") {
		t.Fatalf("SAVE failed: %s", resp)
	}

	// Remeasure sizeBloated
	sizeBloated = getDirSize(t, dbDir)
	t.Logf("Size after updates and flush: %d bytes", sizeBloated)

	// 3. Trigger Compaction
	conn.Write([]byte("*1\r\n$7\r\nCOMPACT\r\n"))
	resp, err = reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp, "OK") {
		t.Fatalf("Unexpected response: %s", resp)
	}

	sizeAfter := getDirSize(t, dbDir)
	t.Logf("Size after compaction: %d bytes", sizeAfter)

	// 5. Verify Integrity
	for i := 0; i < keyCount; i++ {
		val := getKey(t, conn, reader, i)

		var sb strings.Builder
		for k := 0; k < 100; k++ {
			sb.WriteString(fmt.Sprintf("%d-%d-", i, k))
		}
		padding := sb.String()
		if len(padding) < 1024 {
			padding += strings.Repeat("x", 1024-len(padding))
		}

		expected := fmt.Sprintf("val-%d-%d-%s", i, updates, padding)
		if val != expected {
			// Don't print huge values
			if len(val) > 50 {
				t.Errorf("Key %d corrupted. Want %s..., Got %s...", i, expected[:20], val[:20])
			} else {
				t.Errorf("Key %d corrupted. Want %s..., Got %s", i, expected[:20], val)
			}
		}
	}

	// 6. Verify Size Reduction
	if sizeAfter >= sizeBloated {
		t.Errorf("Compaction did not reduce size! Before: %d, After: %d", sizeBloated, sizeAfter)
	}
}

func setKey(t *testing.T, conn net.Conn, reader *bufio.Reader, i, v int) {
	key := fmt.Sprintf("key-%d", i)
	// Use random data to avoid compression
	// We can't use random data easily if we need to verify exact value later without storing it.
	// But we can generate pseudo-random based on i, v.
	// Simple pattern that repeats but isn't trivial for s2?
	// s2 is Snappy. Repeats are compressible.
	// Let's use a string that changes every few bytes.
	var sb strings.Builder
	for k := 0; k < 100; k++ {
		sb.WriteString(fmt.Sprintf("%d-%d-", i, k))
	}
	padding := sb.String()
	// Ensure it's long
	if len(padding) < 1024 {
		padding += strings.Repeat("x", 1024-len(padding))
	}

	val := fmt.Sprintf("val-%d-%d-%s", i, v, padding)
	cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
	conn.Write([]byte(cmd))
	reader.ReadString('\n') // +OK
}

func getKey(t *testing.T, conn net.Conn, reader *bufio.Reader, i int) string {
	key := fmt.Sprintf("key-%d", i)
	cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
	conn.Write([]byte(cmd))
	// $len
	line, _ := reader.ReadString('\n')
	if strings.HasPrefix(line, "$-1") {
		return ""
	}
	// val
	valLine, _ := reader.ReadString('\n')
	return strings.TrimRight(valLine, "\r\n")
}

func getDirSize(t *testing.T, path string) int64 {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			// Compaction creates and removes temporary folders (e.g. *-compact) while
			// this test is running. Ignore transient missing-path errors so size
			// sampling is robust under concurrent directory churn.
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return size
}
