#!/bin/bash
export PEERS="node-1:localhost:8081,node-2:localhost:8082"
export KAIRO_MAX_KEYS_PER_NODE=-1
export NODE_ID="node-1"
export KAIRO_PORT="8081"
mvn compile exec:java -Dexec.mainClass="com.kairo.Main" > node1.log 2>&1 &
NODE1_PID=$!

export NODE_ID="node-2"
export KAIRO_PORT="8082"
mvn compile exec:java -Dexec.mainClass="com.kairo.Main" > node2.log 2>&1 &
NODE2_PID=$!
sleep 5

echo "Creating payload.txt"
echo "val" > payload.txt

echo "Benchmarking Async SET..."
ab -n 1000 -c 10 -p payload.txt http://localhost:8081/cache/bench-async | grep -E "Time per request:"

echo "Benchmarking Quorum SET..."
ab -n 1000 -c 10 -p payload.txt http://localhost:8081/cache/bench-quorum\?consistency=quorum | grep -E "Time per request:"

lsof -i :8081 -t | xargs kill -9 > /dev/null 2>&1 || true
lsof -i :8082 -t | xargs kill -9 > /dev/null 2>&1 || true
