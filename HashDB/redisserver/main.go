package main

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/HashDB/redisserver/badgerredis"
	"github.com/snissn/gomap/HashDB/redisserver/hashdbredis"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run redisserver/main.go [hashdb|badger] [dbdir]")
		os.Exit(1)
	}

	mode := os.Args[1]
	dbdir := os.Args[2]

	switch mode {
	case "hashdb", "gomap":
		server := hashdbredis.NewRedisServer(dbdir)
		fmt.Printf("Starting Redis server using HashDB on :6380 (dbdir=%s)\n", dbdir)
		if err := server.Serve(":6380"); err != nil {
			fmt.Println("Server error:", err)
		}

	case "badger":
		server, err := badgerredis.NewRedisServer(dbdir)
		if err != nil {
			fmt.Println("Failed to start Badger server:", err)
			os.Exit(1)
		}
		fmt.Printf("Starting Redis server using Badger on :6380 (dbdir=%s)\n", dbdir)
		if err := server.Serve(":6380"); err != nil {
			fmt.Println("Server error:", err)
		}

	default:
		fmt.Println("Unknown mode:", mode)
		fmt.Println("Usage: go run redisserver/main.go [hashdb|badger] [dbdir]")
		os.Exit(1)
	}
}
