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

echo -e "\n--- Simulating Stale Replica Scenario ---"
# We inject a stale value with timestamp 1000 into node-2
curl -s -X POST -H "X-Kairo-Replication: true" -H "X-Kairo-Source-Node: fake" "http://localhost:8082/cache/test-stale?writeTimestamp=1000" -d "stale-val"
# We inject a fresh value with timestamp 2000 into node-1
curl -s -X POST -H "X-Kairo-Replication: true" -H "X-Kairo-Source-Node: fake" "http://localhost:8081/cache/test-stale?writeTimestamp=2000" -d "fresh-val"

echo -e "\n--- Test 1: Normal GET from node-2 (Stale Read) ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "http://localhost:8082/cache/test-stale?internal=true"

echo -e "\n--- Test 2: Quorum GET from node-2 (Should resolve to fresh-val) ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "http://localhost:8082/cache/test-stale?consistency=quorum"

echo -e "\nCleaning up..."
lsof -i :8081 -t | xargs kill -9 > /dev/null 2>&1 || true
lsof -i :8082 -t | xargs kill -9 > /dev/null 2>&1 || true
