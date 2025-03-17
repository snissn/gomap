benchmark-all:
	go run cmd/benchmarkmain/main.go --engines=gomap,badger  --keycounts=1000,10000,100000,500000,1000000,5000000,10000000,20000000,30000000,40000000,50000000 --csv=benchmark/results.csv

build:
	go build -o bin/gomap-redis-wrapper redisserver/main.go

run-gomap:
	go run redisserver/main.go gomap /tmp/gomap-benchmark

run-badger:
	go run redisserver/main.go badger /tmp/badger-benchmark

clean:
	rm -rf bin/
	rm -rf /tmp/gomap-benchmark /tmp/badger-benchmark

fmt:
	go fmt ./...

test:
	go test ./...

mod-tidy:
	go mod tidy

install-deps:
	go get github.com/tidwall/redcon
	go get github.com/dgraph-io/badger/v4
