package stress

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCompaction(t *testing.T) {
	// 1. Setup
	rootDir, _ := os.Getwd()
	if !strings.HasSuffix(rootDir, "gomap") {
		rootDir = filepath.Dir(rootDir)
	}
	serverBin := filepath.Join(rootDir, "redisserver_bin_compact")
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

	serverCmd := startServer(t, serverBin, dbDir)
	defer func() {
		serverCmd.Process.Kill()
		serverCmd.Wait()
	}()

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", ServerPort))
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
	}

	sizeBloated := getDirSize(t, dbDir)
	t.Logf("Size after updates: %d bytes", sizeBloated)

	if sizeBloated <= sizeBefore*2 {
		t.Log("Warning: File didn't grow as expected? Maybe slab allocation is large or compression handles it?")
	}

	// 3. Trigger Compaction
	conn.Write([]byte("*1\r\n$12\r\nBGREWRITEAOF\r\n"))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp, "Background") {
		t.Fatalf("Unexpected response: %s", resp)
	}

	// 4. Wait for compaction
	// Poll size or just sleep.
	// Compaction is fast for small DB.
	time.Sleep(2 * time.Second)

	sizeAfter := getDirSize(t, dbDir)
	t.Logf("Size after compaction: %d bytes", sizeAfter)

	// 5. Verify Integrity
	for i := 0; i < keyCount; i++ {
		val := getKey(t, conn, reader, i)
		expected := fmt.Sprintf("val-%d-%d", i, updates)
		if val != expected {
			t.Errorf("Key %d corrupted. Want %s, Got %s", i, expected, val)
		}
	}

	// 6. Verify Size Reduction
	if sizeAfter >= sizeBloated {
		t.Errorf("Compaction did not reduce size! Before: %d, After: %d", sizeBloated, sizeAfter)
	}
}

func setKey(t *testing.T, conn net.Conn, reader *bufio.Reader, i, v int) {
	key := fmt.Sprintf("key-%d", i)
	val := fmt.Sprintf("val-%d-%d", i, v)
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
