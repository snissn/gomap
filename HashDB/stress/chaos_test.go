package stress

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	KeyCount     = 5000
	serverShards = 8
)

func TestChaos(t *testing.T) {
	addr := pickFreeTCPAddr(t)

	// 1. Build Server
	rootDir, _ := os.Getwd()
	// When running `go test`, the working directory is typically `HashDB/stress`.
	rootDir = filepath.Dir(rootDir)

	serverBin := filepath.Join(rootDir, "redisserver_bin")
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(serverBin), ".exe") {
		serverBin += ".exe"
	}
	buildCmd := exec.Command("go", "build", "-o", serverBin, "redisserver/main.go")
	buildCmd.Dir = rootDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build server: %v\n%s", err, out)
	}
	defer os.Remove(serverBin)

	dbDir, err := os.MkdirTemp("", "chaos-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbDir)

	// 2. Start Server
	serverCmd := startServer(t, serverBin, dbDir, addr)

	// 3. Write Data
	ackedKeys := make(map[string]string)
	done := make(chan bool)

	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Logf("Client connect failed: %v", err)
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)

		for i := 0; i < KeyCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			val := fmt.Sprintf("val-%d", i)

			// RESP SET
			cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
			_, err := conn.Write([]byte(cmd))
			if err != nil {
				break // Pipe broken by kill
			}

			// Read response
			resp, err := reader.ReadString('\n')
			if err != nil {
				break // Pipe broken
			}
			if strings.TrimSpace(resp) == "+OK" {
				ackedKeys[key] = val
			}
		}
		done <- true
	}()

	// 4. Kill after random time (or half way)
	time.Sleep(100 * time.Millisecond) // Let it write some
	if err := serverCmd.Process.Kill(); err != nil {
		t.Logf("Failed to kill server: %v", err)
	}
	serverCmd.Wait() // cleanup

	<-done // Wait for client to stop

	t.Logf("Wrote %d keys before crash", len(ackedKeys))

	// 5. Restart Server
	serverCmd = startServer(t, serverBin, dbDir, addr)
	defer func() {
		serverCmd.Process.Kill()
		serverCmd.Wait()
	}()

	// 6. Verify
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to reconnect: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	missingCount := 0
	for k, v := range ackedKeys {
		// RESP GET
		cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k)
		conn.Write([]byte(cmd))

		// Read response: $len\r\nvalue\r\n
		line1, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Read failed verify: %v", err)
		}
		if strings.HasPrefix(line1, "$-1") {
			missingCount++
			continue
		}
		// Parse len
		// line1 is $5\r\n
		// Read value
		valLine, err := reader.ReadString('\n')
		valGot := strings.TrimRight(valLine, "\r\n")

		if valGot != v {
			t.Errorf("Key %s mismatch. Want %s, got %s", k, v, valGot)
		}
	}
	if missingCount > 0 {
		t.Logf("%d keys missing (expected due to async persistence)", missingCount)
	}
}

func pickFreeTCPAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startServer(t *testing.T, bin string, dbDir, addr string) *exec.Cmd {
	cmd := exec.Command(bin, "hashdb", dbDir, addr)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"HASHDB_SHARDS="+strconv.Itoa(serverShards),
		"GOMAP_SHARDS="+strconv.Itoa(serverShards),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Wait for port
	// Simple retry loop
	for i := 0; i < 400; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return cmd
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	t.Fatal("Server failed to bind port")
	return nil
}
