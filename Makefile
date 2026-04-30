HASHDB_DIR := HashDB
TREEDB_DIR := TreeDB
UNIFIED_BENCH_DIR := cmd/unified_bench
BENCHPROF_DIR := cmd/benchprof
COLLECTION_LOAD_FIXTURE_DIR := cmd/collection_load_fixture
COLLECTION_BENCH_MATRIX_DIR := cmd/collection_bench_matrix
BIN_DIR := bin

BENCH_KEYCOUNTS ?= 1,10,100,1000,10000,100000,1000000
BENCH_VALSIZE ?= 128
BENCH_BATCHSIZE ?= 1000
BENCH_RANGE_QUERIES ?= 200
BENCH_RANGE_SPAN ?= 100
BENCH_OUTDIR ?= docs/images

.PHONY: help
help:
	@echo "Common targets:"
	@echo "  make fmt            - gofmt all tracked .go files"
	@echo "  make test           - run go test in root + key dirs"
	@echo "  make hooks          - install local git hooks (gofmt on commit)"
	@echo "  make vet            - run go vet in root + key dirs"
	@echo "  make tidy           - go mod tidy (repo root)"
	@echo "  make deps           - download deps (repo root)"
	@echo "  make docs-check     - validate docs invariants"
	@echo "  make build          - build useful binaries into ./$(BIN_DIR)"
	@echo "  make bench          - run unified bench"
	@echo "  make bench-readme   - regenerate README benchmark snapshot"
	@echo "  make benchmark-all  - run HashDB redis-benchmark suite (legacy)"
	@echo "  make unified-bench  - build unified bench binary"
	@echo "  make benchprof      - build profile analyzer binary"
	@echo "  make collection-load-fixture - build kept TreeDB collection load fixture"
	@echo "  make collection-bench-matrix - build collection benchmark matrix runner"
	@echo "  make clean          - remove ./$(BIN_DIR) and temp dirs"

.PHONY: fmt
fmt:
	gofmt -w $$(git ls-files '*.go')

.PHONY: hooks
hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit

.PHONY: test test-root test-hashdb test-treedb test-unified-bench
test: test-root test-treedb test-unified-bench

test-root:
	go test ./...

test-hashdb:
	cd $(HASHDB_DIR) && go test ./...

test-treedb:
	cd $(TREEDB_DIR) && go test ./...

test-unified-bench:
	cd $(UNIFIED_BENCH_DIR) && go test ./...

.PHONY: test-race
test-race:
	go test -race ./HashDB/... ./TreeDB/db ./TreeDB/caching ./TreeDB/internal/merging

.PHONY: vet vet-root vet-hashdb vet-treedb vet-unified-bench
vet: vet-root vet-treedb vet-unified-bench

vet-root:
	go vet ./...

vet-hashdb:
	cd $(HASHDB_DIR) && go vet ./...

vet-treedb:
	cd $(TREEDB_DIR) && go vet ./...

vet-unified-bench:
	cd $(UNIFIED_BENCH_DIR) && go vet ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: deps
deps:
	go mod download

.PHONY: docs-check
docs-check:
	bash ./scripts/docs_check.sh

.PHONY: build build-hashdb build-treedb treemap treemap-bin unified-bench benchprof collection-load-fixture collection-bench-matrix
build: build-hashdb build-treedb unified-bench benchprof collection-load-fixture collection-bench-matrix

build-hashdb:
	mkdir -p $(BIN_DIR)
	cd $(HASHDB_DIR) && go build -o ../$(BIN_DIR)/hashdb-benchmark ./cmd/benchmarkmain
	cd $(HASHDB_DIR) && go build -o ../$(BIN_DIR)/hashdb-redis-wrapper ./redisserver
	cd $(HASHDB_DIR) && go build -o ../$(BIN_DIR)/hashdb-loadfactorbench ./cmd/loadfactorbench
	cd $(HASHDB_DIR) && go build -o ../$(BIN_DIR)/hashdb-resizebench ./cmd/resizebench
	cd $(HASHDB_DIR) && go build -o ../$(BIN_DIR)/hashdb-shardbench ./cmd/shardbench

build-treedb:
	mkdir -p $(BIN_DIR)
	cd $(TREEDB_DIR) && go build -o ../$(BIN_DIR)/treedb-stress ./cmd/stress
	cd $(TREEDB_DIR) && go build -o ../$(BIN_DIR)/treedb-verify ./cmd/verify
	cd $(TREEDB_DIR) && go build -o ../$(BIN_DIR)/treemap ./cmd/treemap

treemap:
	go build -o treemap ./TreeDB/cmd/treemap

treemap-bin:
	mkdir -p $(BIN_DIR)
	cd $(TREEDB_DIR) && go build -o ../$(BIN_DIR)/treemap ./cmd/treemap

unified-bench:
	mkdir -p $(BIN_DIR)
	cd $(UNIFIED_BENCH_DIR) && go build -o ../../$(BIN_DIR)/unified-bench .

benchprof:
	mkdir -p $(BIN_DIR)
	cd $(BENCHPROF_DIR) && go build -o ../../$(BIN_DIR)/benchprof .

collection-load-fixture:
	mkdir -p $(BIN_DIR)
	cd $(COLLECTION_LOAD_FIXTURE_DIR) && go build -o ../../$(BIN_DIR)/collection-load-fixture .

collection-bench-matrix:
	mkdir -p $(BIN_DIR)
	cd $(COLLECTION_BENCH_MATRIX_DIR) && go build -o ../../$(BIN_DIR)/collection-bench-matrix .

.PHONY: bench bench-readme
bench: unified-bench
	./$(BIN_DIR)/unified-bench

bench-readme: unified-bench
	./$(BIN_DIR)/unified-bench -suite readme -format markdown -seed 1 -keycounts "$(BENCH_KEYCOUNTS)" -valsize "$(BENCH_VALSIZE)" -batchsize "$(BENCH_BATCHSIZE)" -range-queries "$(BENCH_RANGE_QUERIES)" -range-span "$(BENCH_RANGE_SPAN)" -outdir "$(BENCH_OUTDIR)" -progress=false | go run ./scripts/update_readme_bench.go

.PHONY: benchmark-all benchmark-quick
benchmark-all: build-hashdb
	cd $(HASHDB_DIR) && ../$(BIN_DIR)/hashdb-benchmark --engines=hashdb,badger --keycounts=1000,10000,100000,500000,1000000,5000000,10000000,20000000,30000000,40000000,50000000 --csv=benchmark/results.csv

benchmark-quick: build-hashdb
	cd $(HASHDB_DIR) && ../$(BIN_DIR)/hashdb-benchmark --engines=hashdb,badger --keycounts=1000,10000 --csv=benchmark/results_quick.csv

.PHONY: run-hashdb run-badger
run-hashdb:
	cd $(HASHDB_DIR) && go run ./redisserver/main.go hashdb /tmp/hashdb-benchmark

run-badger:
	cd $(HASHDB_DIR) && go run ./redisserver/main.go badger /tmp/badger-benchmark

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)/
	rm -rf /tmp/hashdb-benchmark /tmp/badger-benchmark
