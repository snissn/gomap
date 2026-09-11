HASHDB_DIR := HashDB
TREEDB_DIR := TreeDB
UNIFIED_BENCH_DIR := cmd/unified_bench
BENCHPROF_DIR := cmd/benchprof
DEEP_BENCHMARK_DIR := cmd/benchmark_run_report
COLLECTION_LOAD_FIXTURE_DIR := cmd/collection_load_fixture
COLLECTION_BENCH_MATRIX_DIR := cmd/collection_bench_matrix
COLLECTION_CANONICAL_BENCH_DIR := cmd/collection_canonical_bench
TREEDB_OUT_OF_CORE_SMOKE_DIR := cmd/treedb_out_of_core_smoke
MONGO_GATEWAY_SERVER := TreeDB/mongo_gateway/server.go
BIN_DIR := bin

BENCH_KEYCOUNTS ?= 1,10,100,1000,10000,100000,1000000
BENCH_VALSIZE ?= 128
BENCH_BATCHSIZE ?= 1000
BENCH_RANGE_QUERIES ?= 200
BENCH_RANGE_SPAN ?= 100
BENCH_OUTDIR ?= docs/images
MONGO_GATEWAY_ADDR ?= 127.0.0.1:27017
MONGO_GATEWAY_DIR ?= /tmp/treedb-mongo-gateway
MONGO_GATEWAY_PROFILE ?= command_wal_durable
MONGO_GATEWAY_DOCUMENT_FORMAT ?= bson
MONGO_GATEWAY_GO_ENV ?= GOWORK=off

.PHONY: help
help:
	@echo "Common targets:"
	@echo "  make fmt            - gofmt all tracked .go files"
# actual 'canonical benchmark'
	@echo "  make deep-benchmark - run TreeDB Canonical Benchmark Report"
# actual 'canonical benchmark'
	@echo "  make test           - run root-module tests once"
	@echo "  make hooks          - install local git hooks (gofmt on commit)"
	@echo "  make vet            - run root-module vet once"
	@echo "  make tidy           - go mod tidy (repo root)"
	@echo "  make deps           - download deps (repo root)"
	@echo "  make docs-check     - validate docs invariants"
	@echo "  make workflow-check - test Makefile wiring (Python 3, no Go dependencies)"
	@echo "  make check-nativewire - build, vet, and test the native-wire feature path"
	@echo "  make build          - build useful binaries into ./$(BIN_DIR)"
	@echo "  make build-native-server - build TreeDB native-wire server"
	@echo "  make build-mongo-gateway - build TreeDB MongoDB gateway server"
	@echo "  make run-mongo-gateway - run TreeDB MongoDB gateway server"
	@echo "  make bench          - run unified bench"
	@echo "  make bench-readme   - regenerate HashDB README benchmark snapshot"
	@echo "  make benchmark-all  - run HashDB redis-benchmark suite (legacy)"
	@echo "  make unified-bench  - build unified bench binary"
	@echo "  make benchprof      - build profile analyzer binary"
	@echo "  make collection-load-fixture - build kept TreeDB collection load fixture"
	@echo "  make collection-bench-matrix - build collection benchmark matrix runner"
# incorrectly named 'canonical benchmark'
	@echo "  make collection-canonical-bench-bin - build canonical collection benchmark runner"
	@echo "  make bench-collections-canonical - run canonical TreeDB collections vs SQLite benchmark"
# incorrectly named 'canonical benchmark'
	@echo "  make bench-out-of-core-smoke - run CI-sized TreeDB out-of-core guardrail smoke"
	@echo "  make clean          - remove ./$(BIN_DIR) and temp dirs"

.PHONY: fmt
fmt:
	gofmt -w $$(git ls-files '*.go')

.PHONY: deep-benchmark
deep-benchmark:
	bash ./scripts/treedb_benchmark_run_report.sh

.PHONY: hooks
hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit

.PHONY: test test-root test-hashdb test-treedb test-unified-bench
# TreeDB and unified_bench share the root module; ./... already includes them.
test: test-root

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
vet: vet-root

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

.PHONY: build build-hashdb build-treedb build-native-server build-mongo-gateway treemap treemap-bin unified-bench benchprof collection-load-fixture collection-bench-matrix collection-canonical-bench-bin collection-canonical-bench treedb-out-of-core-smoke-bin treedb-out-of-core-smoke
build: build-hashdb build-treedb build-native-server build-mongo-gateway unified-bench benchprof collection-load-fixture collection-bench-matrix collection-canonical-bench-bin treedb-out-of-core-smoke-bin

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


build-native-server:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/treedb-native-server ./cmd/treedb-native-server

.PHONY: workflow-check check-nativewire
workflow-check:
	python3 .github/scripts/test_makefile_workflow.py

# Reuse the existing package tests, including forced-pointer checkpoint/reopen.
check-nativewire: build-native-server
	go vet ./TreeDB/nativewire ./cmd/treedb-native-server
	go test -count=1 ./TreeDB/nativewire ./cmd/treedb-native-server

build-mongo-gateway:
	mkdir -p $(BIN_DIR)
	$(MONGO_GATEWAY_GO_ENV) go build -o $(BIN_DIR)/treedb-mongo-gateway ./$(MONGO_GATEWAY_SERVER)

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

collection-canonical-bench-bin:
	mkdir -p $(BIN_DIR)
	cd $(COLLECTION_CANONICAL_BENCH_DIR) && go build -o ../../$(BIN_DIR)/collection-canonical-bench .

collection-canonical-bench: collection-canonical-bench-bin

treedb-out-of-core-smoke-bin:
	mkdir -p $(BIN_DIR)
	cd $(TREEDB_OUT_OF_CORE_SMOKE_DIR) && go build -o ../../$(BIN_DIR)/treedb-out-of-core-smoke .

treedb-out-of-core-smoke: treedb-out-of-core-smoke-bin

.PHONY: bench bench-readme
bench: unified-bench
	./$(BIN_DIR)/unified-bench

bench-readme: unified-bench
	./$(BIN_DIR)/unified-bench -suite readme -format markdown -seed 1 -keycounts "$(BENCH_KEYCOUNTS)" -valsize "$(BENCH_VALSIZE)" -batchsize "$(BENCH_BATCHSIZE)" -range-queries "$(BENCH_RANGE_QUERIES)" -range-span "$(BENCH_RANGE_SPAN)" -outdir "$(BENCH_OUTDIR)" -progress=false | sed 's#($(BENCH_OUTDIR)/#(../$(BENCH_OUTDIR)/#g' | go run ./scripts/update_readme_bench.go -readme HashDB/README.md

.PHONY: bench-collections-canonical
bench-collections-canonical:
	./scripts/bench_collections_canonical.sh

.PHONY: bench-out-of-core-smoke
bench-out-of-core-smoke:
	./scripts/bench_out_of_core_smoke.sh

.PHONY: benchmark-all benchmark-quick
benchmark-all: build-hashdb
	cd $(HASHDB_DIR) && ../$(BIN_DIR)/hashdb-benchmark --engines=hashdb,badger --keycounts=1000,10000,100000,500000,1000000,5000000,10000000,20000000,30000000,40000000,50000000 --csv=benchmark/results.csv

benchmark-quick: build-hashdb
	cd $(HASHDB_DIR) && ../$(BIN_DIR)/hashdb-benchmark --engines=hashdb,badger --keycounts=1000,10000 --csv=benchmark/results_quick.csv

.PHONY: run-mongo-gateway run-hashdb run-badger

run-mongo-gateway:
	$(MONGO_GATEWAY_GO_ENV) go run ./$(MONGO_GATEWAY_SERVER) -addr "$(MONGO_GATEWAY_ADDR)" -dir "$(MONGO_GATEWAY_DIR)" -profile "$(MONGO_GATEWAY_PROFILE)" -document-format "$(MONGO_GATEWAY_DOCUMENT_FORMAT)"

run-hashdb:
	cd $(HASHDB_DIR) && go run ./redisserver/main.go hashdb /tmp/hashdb-benchmark

run-badger:
	cd $(HASHDB_DIR) && go run ./redisserver/main.go badger /tmp/badger-benchmark

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)/
	rm -rf /tmp/hashdb-benchmark /tmp/badger-benchmark
