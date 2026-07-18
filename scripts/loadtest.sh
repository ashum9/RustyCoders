#!/bin/bash

NUM_CLIENTS=${1:-100}
TARGET_PORT=${2:-8081}
TARGET_HOST=${3:-"127.0.0.1"}

echo "Starting $NUM_CLIENTS concurrent connections to $TARGET_HOST:$TARGET_PORT..."

PIDs=()

for i in $(seq 1 $NUM_CLIENTS); do
    (
        echo "client $i connected" | nc -w 10 $TARGET_HOST $TARGET_PORT >/dev/null 2>&1
    ) &
    PIDs+=($!)
done

echo "spawned $NUM_CLIENTS clients. waiting..."
wait "${PIDs[@]}"
echo "all clients finished"
