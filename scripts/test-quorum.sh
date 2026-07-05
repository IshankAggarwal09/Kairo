#!/bin/bash
set -e

export PEERS="node-1:localhost:8081,node-2:localhost:8082"
export KAIRO_MAX_KEYS_PER_NODE=-1

echo "Starting node-1..."
export NODE_ID="node-1"
export KAIRO_PORT="8081"
mvn compile exec:java -Dexec.mainClass="com.kairo.Main" > node1.log 2>&1 &
NODE1_PID=$!

echo "Starting node-2..."
export NODE_ID="node-2"
export KAIRO_PORT="8082"
mvn compile exec:java -Dexec.mainClass="com.kairo.Main" > node2.log 2>&1 &
NODE2_PID=$!

sleep 7

echo -e "\n--- Test 1: Eventual Consistency (Async) ---"
time curl -s -w "\nHTTP Status: %{http_code}\n" -X POST -d "async-val" "http://localhost:8081/cache/test-async"

echo -e "\n--- Test 2: Quorum Consistency (Sync) ---"
time curl -s -w "\nHTTP Status: %{http_code}\n" -X POST -d "quorum-val" "http://localhost:8081/cache/test-quorum?consistency=quorum"

echo -e "\n--- Killing node-2 ---"
lsof -i :8082 -t | xargs kill -9 > /dev/null 2>&1 || true
sleep 3 # Give node-1 failure detector a moment to mark node-2 dead

echo -e "\n--- Test 3: Eventual Consistency (Async) with node-2 DEAD ---"
time curl -s -w "\nHTTP Status: %{http_code}\n" -X POST -d "async-val-2" "http://localhost:8081/cache/test-async-2"

echo -e "\n--- Test 4: Quorum Consistency (Sync) with node-2 DEAD ---"
time curl -s -w "\nHTTP Status: %{http_code}\n" -X POST -d "quorum-val-2" "http://localhost:8081/cache/test-quorum-2?consistency=quorum"

echo -e "\nCleaning up..."
lsof -i :8081 -t | xargs kill -9 > /dev/null 2>&1 || true
lsof -i :8082 -t | xargs kill -9 > /dev/null 2>&1 || true
