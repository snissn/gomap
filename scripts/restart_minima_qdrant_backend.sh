#!/usr/bin/env bash
set -euo pipefail

capture_stable_identity() {
	local pid=$1
	local binary=$2
	local initial_start state current_start command identity

	initial_start=$(ps -o lstart= -p "$pid" 2>/dev/null || true)
	[[ -n "$initial_start" ]] || return 1
	for _ in {1..200}; do
		state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
		[[ -n "$state" && "$state" != Z* ]] || return 1
		current_start=$(ps -o lstart= -p "$pid" 2>/dev/null || true)
		[[ "$current_start" == "$initial_start" ]] || return 1
		command=$(ps -o command= -p "$pid" 2>/dev/null || true)
		command=${command#"${command%%[![:space:]]*}"}
		command=${command%"${command##*[![:space:]]}"}
		if [[ "$command" == "$binary" ]]; then
			identity=$(ps -o lstart= -o command= -p "$pid" 2>/dev/null || true)
			[[ -n "$identity" ]] || return 1
			printf '%s\n' "$identity"
			return 0
		fi
		sleep 0.01
	done
	return 1
}

mode=${1:?restart mode required}
case "$mode" in
standalone)
	binary=${2:?Qdrant binary required}
	port=${3:?Qdrant port required}
	storage=${4:?Qdrant storage path required}
	log=${5:?Qdrant log path required}
	pid_file=${6:?Qdrant PID file required}
	binary_dir=$(cd -- "$(dirname -- "$binary")" && pwd -P)
	binary="$binary_dir/$(basename -- "$binary")"
	if [[ -s "$pid_file" ]]; then
		{
			IFS= read -r old_pid
			IFS= read -r expected_identity || true
		} <"$pid_file"
		if [[ ! "$old_pid" =~ ^[1-9][0-9]*$ ]]; then
			echo "owned Qdrant PID file is invalid: $old_pid" >&2
			exit 1
		fi
		current_identity=$(ps -o lstart= -o command= -p "$old_pid" 2>/dev/null || true)
		if [[ -z "$expected_identity" || "$current_identity" != "$expected_identity" ]]; then
			echo "owned Qdrant PID file is stale for PID $old_pid; refusing to signal that process" >&2
		else
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
	fi
	nohup env QDRANT__SERVICE__HOST=127.0.0.1 \
		QDRANT__SERVICE__HTTP_PORT="$port" \
		QDRANT__STORAGE__STORAGE_PATH="$storage" \
		"$binary" >>"$log" 2>&1 </dev/null &
	pid=$!
	identity=$(capture_stable_identity "$pid" "$binary" || true)
	if [[ -z "$identity" ]]; then
		kill "$pid" >/dev/null 2>&1 || true
		wait "$pid" >/dev/null 2>&1 || true
		echo "restarted Qdrant exited before its process identity could be recorded" >&2
		exit 1
	fi
	printf '%s\n%s\n' "$pid" "$identity" >"$pid_file"
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
