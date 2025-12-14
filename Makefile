
build:
	mkdir -p bin
	cd HashDB && go build -o ../bin/hashdb-benchmark ./cmd/benchmarkmain
	cd HashDB && go build -o ../bin/hashdb-redis-wrapper ./redisserver

benchmark-all: build
	cd HashDB && ../bin/hashdb-benchmark --engines=gomap,badger --keycounts=1000,10000,100000,500000,1000000,5000000,10000000,20000000,30000000,40000000,50000000 --csv=benchmark/results.csv


# Optional: fast local test
benchmark-quick: build
	cd HashDB && ../bin/hashdb-benchmark \
		--engines=gomap,badger \
		--keycounts=1000,10000 \
		--csv=benchmark/results_quick.csv

run-gomap:
	cd HashDB && go run ./redisserver/main.go hashdb /tmp/hashdb-benchmark

run-badger:
	cd HashDB && go run ./redisserver/main.go badger /tmp/badger-benchmark

clean:
	rm -rf bin/
	rm -rf /tmp/hashdb-benchmark /tmp/badger-benchmark

fmt:
	cd HashDB && go fmt ./...
	cd TreeDB && go fmt ./...
	cd cmd/unified_bench && go fmt ./...

test:
	cd HashDB && go test ./...
	cd TreeDB && go test ./...
	cd cmd/unified_bench && go test ./...

mod-tidy:
	cd HashDB && go mod tidy
	cd TreeDB && go mod tidy
	cd cmd/unified_bench && go mod tidy

install-deps:
	cd HashDB && go get github.com/tidwall/redcon
	cd HashDB && go get github.com/dgraph-io/badger/v4
