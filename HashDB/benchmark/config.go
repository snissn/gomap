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
	Scenarios []Scenario
}

type Scenario struct {
	Name string
	Args []string
}

func ParseConfig() *Config {
	var enginesStr string
	var keyCountsStr string
	var csvPath string
	var port int

	flag.StringVar(&enginesStr, "engines", "hashdb,badger,redis", "Comma-separated list of engines to benchmark")
	flag.StringVar(&keyCountsStr, "keycounts", "10000,100000", "Comma-separated list of key counts")
	flag.IntVar(&port, "port", 6380, "Redis-compatible server port")
	flag.StringVar(&csvPath, "csv", "benchmark_results.csv", "Path to CSV output file")
	flag.Parse()

	keyCounts := parseKeyCounts(keyCountsStr)
	engines := strings.Split(enginesStr, ",")

	scenarios := []Scenario{
		{
			Name: "Standard",
			Args: []string{"-t", "set,get", "-c", "50", "-q"},
		},
		{
			Name: "Pipeline16",
			Args: []string{"-t", "set,get", "-c", "50", "-P", "16", "-q"},
		},
		{
			Name: "RandomKeys",
			Args: []string{"-t", "set,get", "-c", "50", "-P", "16", "-r", "100000", "-q"},
		},
		{
			Name: "LargeVal1KB",
			Args: []string{"-t", "set,get", "-c", "50", "-P", "16", "-d", "1024", "-q"},
		},
	}

	return &Config{
		Engines:   engines,
		KeyCounts: keyCounts,
		Port:      port,
		CSVPath:   csvPath,
		Scenarios: scenarios,
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
