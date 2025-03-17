package benchmark

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
)

type Config struct {
	Engines   []string
	KeyCounts []int
	Port      int
	CSVPath   string
}

func ParseConfig() *Config {
	var enginesStr string
	var keyCountsStr string
	var csvPath string
	var port int

	flag.StringVar(&enginesStr, "engines", "gomap,badger", "Comma-separated list of engines to benchmark")
	flag.StringVar(&keyCountsStr, "keycounts", "10000,50000,100000", "Comma-separated list of key counts")
	flag.IntVar(&port, "port", 6380, "Redis-compatible server port")
	flag.StringVar(&csvPath, "csv", "benchmark_results.csv", "Path to CSV output file")
	flag.Parse()

	keyCounts := parseKeyCounts(keyCountsStr)
	engines := strings.Split(enginesStr, ",")

	return &Config{
		Engines:   engines,
		KeyCounts: keyCounts,
		Port:      port,
		CSVPath:   csvPath,
	}
}

func parseKeyCounts(s string) []int {
	var result []int
	parts := strings.Split(s, ",")
	for _, part := range parts {
		val, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			fmt.Printf("Warning: invalid key count '%s', skipping\n", part)
			continue
		}
		result = append(result, val)
	}
	return result
}
