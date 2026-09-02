package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/snissn/gomap/HashDB/redisserver/badgerredis"
	"github.com/snissn/gomap/HashDB/redisserver/hashdbredis"
	"github.com/snissn/gomap/HashDB/redisserver/mapredis"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run redisserver/main.go [hashdb|badger|map] [dbdir] [addr-or-port?]")
		os.Exit(1)
	}

	mode := os.Args[1]
	dbdir := os.Args[2]
	addr := ":6380"
	if len(os.Args) >= 4 {
		addr = os.Args[3]
	} else if v := os.Getenv("HASHDB_REDIS_ADDR"); v != "" {
		addr = v
	} else if v := os.Getenv("GOMAP_REDIS_ADDR"); v != "" {
		addr = v
	} else if v := os.Getenv("HASHDB_REDIS_PORT"); v != "" {
		addr = ":" + v
	} else if v := os.Getenv("GOMAP_REDIS_PORT"); v != "" {
		addr = ":" + v
	}
	if len(addr) > 0 && addr[0] != ':' && !strings.Contains(addr, ":") {
		addr = ":" + addr
	}

	switch mode {
	case "hashdb", "gomap":
		server := hashdbredis.NewRedisServer(dbdir)
		fmt.Printf("Starting Redis server using HashDB on %s (dbdir=%s)\n", addr, dbdir)
		if err := server.Serve(addr); err != nil {
			fmt.Println("Server error:", err)
		}

	case "badger":
		server, err := badgerredis.NewRedisServer(dbdir)
		if err != nil {
			fmt.Println("Failed to start Badger server:", err)
			os.Exit(1)
		}
		fmt.Printf("Starting Redis server using Badger on %s (dbdir=%s)\n", addr, dbdir)
		if err := server.Serve(addr); err != nil {
			fmt.Println("Server error:", err)
		}

	case "map":
		server := mapredis.NewRedisServer(dbdir)
		fmt.Printf("Starting Redis server using Go map on %s (dbdir=%s)\n", addr, dbdir)
		if err := server.Serve(addr); err != nil {
			fmt.Println("Server error:", err)
		}

	default:
		fmt.Println("Unknown mode:", mode)
		fmt.Println("Usage: go run redisserver/main.go [hashdb|badger|map] [dbdir] [addr-or-port?]")
		os.Exit(1)
	}
}
