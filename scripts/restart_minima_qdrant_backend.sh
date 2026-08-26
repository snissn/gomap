#!/usr/bin/env bash
set -euo pipefail

mode=${1:?restart mode required}
case "$mode" in
standalone)
	binary=${2:?Qdrant binary required}
	port=${3:?Qdrant port required}
	storage=${4:?Qdrant storage path required}
	log=${5:?Qdrant log path required}
	pid_file=${6:?Qdrant PID file required}
	if [[ -s "$pid_file" ]]; then
		old_pid=$(cat "$pid_file")
		if [[ ! "$old_pid" =~ ^[1-9][0-9]*$ ]]; then
			echo "owned Qdrant PID file is invalid: $old_pid" >&2
			exit 1
		fi
		kill "$old_pid" >/dev/null 2>&1 || true
		for _ in {1..200}; do
			state=$(ps -o stat= -p "$old_pid" 2>/dev/null || true)
			if [[ -z "$state" || "$state" == Z* ]]; then
				break
			fi
			sleep 0.05
		done
		state=$(ps -o stat= -p "$old_pid" 2>/dev/null || true)
		if [[ -n "$state" && "$state" != Z* ]]; then
			echo "owned Qdrant PID $old_pid did not stop" >&2
			exit 1
		fi
	fi
	nohup env QDRANT__SERVICE__HOST=127.0.0.1 \
		QDRANT__SERVICE__HTTP_PORT="$port" \
		QDRANT__STORAGE__STORAGE_PATH="$storage" \
		"$binary" >>"$log" 2>&1 </dev/null &
	pid=$!
	printf '%s\n' "$pid" >"$pid_file"
	printf '%s\n' "$pid"
	;;
docker)
	container=${2:?Qdrant container required}
	docker restart "$container" >/dev/null
	pid=$(docker inspect --format '{{.State.Pid}}' "$container")
	if [[ ! "$pid" =~ ^[1-9][0-9]*$ ]]; then
		echo "restarted Qdrant container has invalid host PID: $pid" >&2
		exit 1
	fi
	printf '%s\n' "$pid"
	;;
*)
	echo "unsupported restart mode: $mode" >&2
	exit 2
	;;
esac
