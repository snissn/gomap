package benchmark

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func Run() {
	cfg := ParseConfig()
	var results []BenchmarkResult

	for _, engine := range cfg.Engines {
		for _, keyCount := range cfg.KeyCounts {
			tempDir, err := os.MkdirTemp("", fmt.Sprintf("%s-db", engine))
			if err != nil {
				log.Fatalf("failed to create temp dir: %v", err)
			}

			fmt.Printf("\nRunning benchmark: engine=%s keys=%d tmpdir=%s\n", engine, keyCount, tempDir)

			// 🔥 Kill anything lingering on 6380 before starting
			_ = exec.Command("bash", "-c", "lsof -ti tcp:6380 | xargs kill -9").Run()
			time.Sleep(1 * time.Second)

			cmd := exec.Command("go", "run", "redisserver/main.go", engine, tempDir)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			if err := cmd.Start(); err != nil {
				log.Fatalf("failed to start server: %v", err)
			}

			time.Sleep(2 * time.Second) // wait for server to bind

			out, err := exec.Command("redis-benchmark", "-p", strconv.Itoa(cfg.Port), "-t", "set,get", "-n", strconv.Itoa(keyCount), "-q", "--csv").CombinedOutput()

			// ⚠ Now kill Redis server forcefully again (double-safety)
			_ = cmd.Process.Kill()
			_ = exec.Command("bash", "-c", "lsof -ti tcp:6380 | xargs kill -9").Run()

			if err := cmd.Wait(); err != nil {
				fmt.Printf("Server exited with error: %v\n", err)
			}

			time.Sleep(2 * time.Second) // Let port clear completely

			if err != nil {
				fmt.Printf("Benchmark failed: %v\nOutput:\n%s\n", err, out)
				continue // still continue benchmarking others
			}

			res := parseBenchmarkOutput(string(out))
			res.Engine = engine
			res.KeyCount = keyCount
			results = append(results, res)

			_ = os.RemoveAll(tempDir)
		}

	}

	PrintResultsTable(results)
	if err := SaveResultsToCSV(cfg.CSVPath, results); err != nil {
		log.Fatalf("failed to save CSV: %v", err)
	}
}

func parseBenchmarkOutput(output string) BenchmarkResult {
	var r BenchmarkResult

	// Clean up: only keep lines that look like CSV
	lines := strings.Split(strings.ReplaceAll(output, "\r", ""), "\n")
	var csvLines []string
	for _, line := range lines {
		if strings.HasPrefix(line, "\"") { // CSV lines start with double quotes
			csvLines = append(csvLines, line)
		}
	}

	if len(csvLines) < 2 {
		fmt.Println("No valid CSV lines found, skipping benchmark result.")
		return r
	}

	reader := csv.NewReader(strings.NewReader(strings.Join(csvLines, "\n")))
	rows, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Failed to parse CSV: %v\n", err)
		return r
	}

	for _, row := range rows {
		if len(row) < 5 {
			continue // Not enough fields
		}
		switch row[0] {
		case "SET":
			r.SetRPS, _ = strconv.ParseFloat(row[1], 64)
			r.SetP50, _ = strconv.ParseFloat(row[4], 64)
		case "GET":
			r.GetRPS, _ = strconv.ParseFloat(row[1], 64)
			r.GetP50, _ = strconv.ParseFloat(row[4], 64)
		}
	}

	return r
}
