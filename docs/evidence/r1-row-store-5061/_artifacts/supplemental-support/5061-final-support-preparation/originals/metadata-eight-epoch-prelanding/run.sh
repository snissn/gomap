#!/usr/bin/env bash
set -euo pipefail
cd /home/mikers/gomap-r1-retained-final-f796
test "$(git rev-parse HEAD)" = f79616f2aeb99b43af81877f26d646cca51177b9
test -z "$(git status --porcelain=v1)"
export GOROOT=/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64
export GOCACHE=/home/mikers/.cache/gomap-r1-go1264
export GOMODCACHE=/home/mikers/go/pkg/mod
export GOTOOLCHAIN=local GOWORK=off GOENV=off GOFLAGS= GODEBUG= GOMAXPROCS=16 GOGC=100 GOMEMLIMIT=off PYTHONDONTWRITEBYTECODE=1
export R1_GO="$GOROOT/bin/go"
python3 scripts/r1_lifecycle_capture.py --qualification rehearsal --out /home/mikers/gomap-r1-evidence-20261005/metadata-eight-epoch-prelanding-f796 --repetitions 1 --epochs 8 --documents 32 --calls-per-epoch 8
