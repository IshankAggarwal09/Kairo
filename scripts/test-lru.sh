#!/bin/bash
set -e

# Use 1 node for simple deterministic testing
export KAIRO_MAX_KEYS_PER_NODE=10
export NODE_ID="node-1"
export KAIRO_PORT="8081"
export PEERS=""
mvn compile exec:java -Dexec.mainClass="com.kairo.Main" > node1.log 2>&1 &
NODE1_PID=$!

echo "Starting single node with LRU capacity of 10..."
sleep 5

echo "Inserting 10 keys (key1 to key10)..."
for i in {1..10}; do
  curl -s -X POST -d "val$i" "http://localhost:8081/cache/key$i" > /dev/null
done

echo "Accessing key1 and key2 to bump their recency..."
curl -s -X GET "http://localhost:8081/cache/key1" > /dev/null
curl -s -X GET "http://localhost:8081/cache/key2" > /dev/null

echo "Inserting 5 more keys (key11 to key15)..."
for i in {11..15}; do
  curl -s -X POST -d "val$i" "http://localhost:8081/cache/key$i" > /dev/null
done

sleep 1

echo "Verifying eviction..."
present_count=0
evicted_count=0
echo "---- Keys Present ----"
for i in {1..15}; do
  res=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:8081/cache/key$i")
  if [ "$res" == "200" ]; then
    ((present_count++))
    echo "key$i is present"
  else
    ((evicted_count++))
    echo "key$i is EVICTED (404)"
  fi
done

echo "----------------------"
echo "Present: $present_count (Expected: 10)"
echo "Evicted: $evicted_count (Expected: 5)"

echo "Cleaning up..."
lsof -i :8081 -t | xargs kill -9 > /dev/null 2>&1 || true
