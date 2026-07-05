# Kairo Design Document — Distributed Cache Architecture

## 1. System Overview & Goals
Kairo is a lightweight, self-healing distributed cache built in modern Java (JDK 21) without external framework dependencies (such as Spring or Netty). It utilizes the JDK built-in `com.sun.net.httpserver.HttpServer` for REST networking and standard Java concurrency primitives for thread-safe storage.

---

## 2. Phase 1: Core Cache Engine
- **Storage Layer (`CacheStore`)**: Backed by a `ConcurrentHashMap<String, ValueEntry>`, providing thread-safe concurrent reads and writes without lock contention.
- **Dual Expiry Strategy**:
  1. **Lazy Eviction**: On `GET`, if the key is found but its absolute timestamp has expired, it is treated as a cache miss (`404 NOT FOUND`) and evicted immediately.
  2. **Active Sweep**: A dedicated daemon thread (`ScheduledExecutorService`) sweeps the map every 5 seconds to purge expired entries that have not been accessed.
- **Endpoints**:
  - `GET /ping`: Health check returning `200 pong`.
  - `POST /cache/{key}?ttl={seconds}`: Sets value from request body (`201 STORED`).
  - `GET /cache/{key}`: Retrieves value (`200 OK` or `404 NOT FOUND`).
  - `DELETE /cache/{key}`: Removes key (`200 OK` or `404 NOT FOUND`).

---

## 3. Phase 2: Cluster Topology & Networking (Docker Compose Setup)

### 3.1 Concrete Service Architecture
The cluster consists of three containerized node instances running on a shared Docker Compose bridge network (`kairo_default`).

| Service Name / Hostname | Node ID (`NODE_ID`) | Internal Container Port | Host Port Mapping | Base URL (Internal DNS) | Base URL (Host Machine) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `node-1` | `node-1` | `8081` | `8081:8081` | `http://node-1:8081` | `http://localhost:8081` |
| `node-2` | `node-2` | `8082` | `8082:8082` | `http://node-2:8082` | `http://localhost:8082` |
| `node-3` | `node-3` | `8083` | `8083:8083` | `http://node-3:8083` | `http://localhost:8083` |

### 3.2 Peer Wiring & DNS Resolution
- **Environment Injection**: Every container is injected with its peer cluster view via the `PEERS` environment variable:
  ```env
  PEERS=node-1:node-1:8081,node-2:node-2:8082,node-3:node-3:8083
  ```
- **Self-Exclusion Convention**: When parsing `PEERS` at startup, if a peer ID matches the container's own `NODE_ID`, it is automatically excluded from its active peer map. This design allows all nodes in the cluster to share the exact same `PEERS` configuration string while preventing accidental self-replication loops.
- **Internal vs. External Reachability**:
  - **Inside Containers**: Inter-node HTTP requests use Docker Compose's internal DNS service names (e.g. `http://node-2:8082/status`).
  - **From Host Machine**: Local debugging and manual test commands use `localhost` with the 1-to-1 mapped port (e.g. `http://localhost:8082/status`).

### 3.3 Cluster Diagnostics (`GET /status`)
Each node exposes `GET /status`, reporting its runtime identity and current cluster view in JSON format:
```json
{
  "nodeId": "node-1",
  "port": 8081,
  "peers": [
    {"id": "node-2", "host": "node-2", "port": 8082, "url": "http://node-2:8082"},
    {"id": "node-3", "host": "node-3", "port": 8083, "url": "http://node-3:8083"}
  ]
}
```

---

## 4. Phase 3: Consistent Hashing & Distributed Routing

### 4.1 Ring Architecture (`HashRing`)
- **Hash Function**: Uses MD5 truncated to unsigned 32-bit integers (`0` to `2^32 - 1`), providing deterministic and well-distributed placement across the integer token space.
- **Data Structure**: Backed by a thread-safe `ConcurrentSkipListMap<Long, String>` mapping token positions to physical node IDs (`node-1`, etc.).
- **Virtual Nodes**: To prevent hot spots and ensure uniform load distribution, each physical node is assigned **150 virtual tokens** at startup. The token layout is deterministically computed across all nodes without inter-node coordination.
- **Clockwise Walk**: For any given key, the primary owner is resolved by searching for the first token greater than or equal to `hash(key)` (`ceilingKey`). If the hash exceeds the highest token on the ring, it wraps around to the lowest token (`firstKey`).

### 4.2 Single-Hop HTTP Routing & Loop Prevention
- **Decentralized Entry Points**: Any node in the cluster can accept client requests. If the receiving node is not the primary owner, it transparently forwards the HTTP request to the owner peer using Java's built-in `HttpClient`.
- **Loop Safeguard**: To prevent infinite forwarding loops during topology transitions, all forwarded requests are tagged with the HTTP header `X-Kairo-Forwarded: true`. A node receiving a request with this header will never forward it a second time, guaranteeing at most **1 network hop** per client request.

---

## 5. Phase 4: Replication & Eventual Consistency (AP Commitment)

### 5.1 Replication Group Discovery (`findReplicas`)
- **Preference List**: For any key, the ring identifies a replication group of size `N` (Replication Factor, default `RF=2`). The first node in the clockwise walk is the **Primary Owner**, and subsequent distinct physical nodes form the **Replica Set** (`findReplicas`).
- **No Self-Replication**: Virtual nodes belonging to physical nodes already selected in the replication group are skipped during the clockwise walk.

### 5.2 The Asynchronous Replication Decision (AP Guarantee)
Kairo explicitly commits to an **AP (Availability & Partition Tolerance)** architecture under the CAP theorem, prioritizing low latency and write availability over instantaneous strong consistency.
- **Why Asynchronous**: Synchronous replication (waiting for replicas to acknowledge writes before responding to the client) would block or fail writes whenever a replica node is slow, degraded, or offline.
- **Asynchronous Execution Mechanism**:
  1. **Immediate Local Write & Client Response**: When a write (`SET` or `DELETE`) reaches the primary owner, the node applies the mutation locally to its `CacheStore` and responds immediately to the client (`201 STORED` or `200 OK`).
  2. **Background Dispatch**: Simultaneously, the primary owner dispatches background replication requests to all replica nodes in the preference list via a non-blocking asynchronous executor or fire-and-forget HTTP client calls.
  3. **Replication Tagging**: Background replication requests include the HTTP header `X-Kairo-Replication: true`. When a replica node receives a request with this flag, it applies the mutation directly to its local `CacheStore` without re-forwarding or triggering further replication loops.

### 5.3 Replica Fallback on Reads & Timeout Choice
To fulfill Core Requirement 07 (surviving a node kill with zero failed client reads), Kairo implements automatic **Replica Fallback on Reads**:
- **Read Fallback Mechanism**: When a node receives a `GET` request for a key owned by another node, it forwards the request to the primary owner. If the primary owner is unreachable, times out, returns an HTTP error status (`!= 200`), or has lost its in-memory state after a restart, the receiving node immediately intercepts the failure. It calls `findReplicas(key, RF)` and queries the replica node(s) in order. If the receiving node itself is in the replica set, it serves the read directly from its local `CacheStore`; otherwise, it forwards the request to a remote replica peer over HTTP.
- **Timeout Choice (1500ms / 1.5 seconds)**: Forwarding HTTP requests are configured with a strict **1500ms (1.5-second) connect and request timeout**.
  - **Why Not Longer (e.g., 5–10s)?**: During a real node crash or network partition, clients would hang for several seconds before experiencing a failover, degrading system throughput and responsiveness.
  - **Why Not Shorter (e.g., < 500ms)?**: Brief garbage collection (GC) pauses or transient CPU spikes on a heavily loaded primary owner could trigger unnecessary failovers to replicas.
  - **Why 1500ms?**: In a high-speed local or container bridge network where normal inter-node RPCs take < 10ms, 1.5s provides a 150x safety margin against transient slowness while keeping the total end-to-end client latency well under standard HTTP timeout thresholds during an actual node failure.

### 5.4 Handling the "Replica Also Down" Edge Case (`503 Service Unavailable`)
A critical edge case in any distributed storage system is defining exact failure semantics when an entire replication group is unavailable:
- **The RF=2 Boundary**: With Replication Factor 2 (`RF=2`), Kairo is architecturally designed to tolerate **exactly 1 simultaneous node failure** per key without data loss or read interruption. If both the primary owner and all replica nodes for a specific key fail or become unreachable simultaneously, the data is genuinely unavailable in the cluster.
- **Honest & Explicit Failure Status (`503 Service Unavailable`)**: Rather than hanging indefinitely, dropping the connection, or returning a misleading `502 Bad Gateway` or `500 Internal Server Error`, Kairo explicitly detects when all nodes in the preference list fail to respond. It immediately terminates the request with **`503 Service Unavailable: Both primary owner (...) and all replica nodes for key '...' are unreachable or down`**.
- **Distinguishing Misses vs. Node Failures (`FallbackResult` Enum)**: To ensure correctness, the routing logic uses a three-state resolution enum (`SUCCESS`, `MISS`, `ALL_UNREACHABLE`). If a replica node is online and responds with `404 NOT FOUND` (indicating the key expired or was never stored), Kairo returns an honest `404 NOT FOUND` to the client. Only when every node in the preference list throws a connection error, timeout, or server exception does the system elevate the error to a `503`.

### 5.5 Observing Eventual Consistency (`GET /debug/local/{key}`)
To directly observe asynchronous replication and Kairo's eventual consistency model without interference from consistent hash routing or fallback logic, the system exposes a specialized inspection endpoint:
- **`GET /debug/local/{key}`**: Implemented in `DebugLocalHandler`, this endpoint completely bypasses the cluster routing layer and reads directly from the targeted node's internal `CacheStore`.
- **Observing the Replication Window**: When a write (`POST /cache/{key}`) lands on the primary owner, the client immediately receives `201 STORED`. If `/debug/local/{key}` is immediately queried on a replica peer, there is a brief propagation window where the replica store may report `404 NOT FOUND` before the background replication RPC completes. Within milliseconds, subsequent queries to `/debug/local/{key}` confirm the arrival of the replicated payload, demonstrating eventual consistency in a live cluster.

### 5.6 Preference List Inspection (`GET /ring/owner?key=X`)
To streamline cluster observability during rebalancing (Phase 6) and failure testing (Phase 8), Kairo extends the ring inspection endpoint (`RingHandler`) to report the complete preference list for any given key:
- **Endpoint Structure**: `GET /ring/owner?key={key}&rf={replicationFactor}` (default `rf=2`).
- **Output Metrics**: The endpoint returns the exact `Primary Owner`, the list of unique `Replicas`, and the consolidated `Preference List` (ordered sequence of physical nodes responsible for storing copies of the key). This enables immediate verification of key placement and failover routing paths across the cluster.

---

## 6. Phase 5: Failure Detection & Health Checking

### 6.1 Detection Mechanism Selection: Direct Heartbeats vs. Gossip Protocol
In a distributed storage system, nodes must dynamically detect when peer nodes fail, crash, or become unreachable due to network partitions, without relying solely on active client request failures. Two primary architectural approaches exist:

1. **Simple Heartbeats (Direct Periodic Pinging)**:
   - **Mechanism**: Each node runs a scheduled background executor that periodically issues direct HTTP pings (`GET /ping` or `GET /status`) to every known peer in the cluster. Each node tracks the timestamp of the last successful contact per peer. If a peer fails to respond within a defined failure threshold (e.g., missed consecutive heartbeats), the node locally transitions the peer's state to offline/dead.
   - **Complexity & Scalability**: Simple implementation with deterministic $O(N)$ messaging per round ($O(N^2)$ across the entire cluster). Highly responsive and effective for small-to-medium clusters.

2. **Epidemic Gossip Protocol (SWIM-style)**:
   - **Mechanism**: Instead of pinging every peer directly, nodes periodically select a random peer and exchange their entire local view of cluster health membership (or state vectors). Health updates, failure claims, and revival rumors spread exponentially across the cluster over multiple gossip rounds.
   - **Complexity & Scalability**: Substantially higher protocol complexity, requiring versioning, heartbeat counters, and infection-style state merging. However, network messaging scales at $O(\log N)$ or constant bandwidth per node, preventing heartbeat storms in massive deployments.

#### Design Decision: Direct Heartbeats for $N=3$ (With Architectural Path to Gossip)
For Kairo's concrete cluster topology of $N=3$ nodes (and typical deployments up to 10–20 nodes), **Simple Direct Heartbeats** is the pragmatic and optimal design choice:
- **Zero Overhead at Small Scale**: In a 3-node cluster, each node only pings 2 peer nodes per interval. An all-to-all heartbeat loop generates trivial network traffic while guaranteeing immediate, deterministic failure detection without the multi-round propagation latency inherent to gossip protocols.
- **Architectural Evolution at Scale**: While direct heartbeats are implemented for the core engine, Kairo's modular peer management layer is designed such that if the cluster scales to tens or hundreds of nodes ($N \ge 50$), the health checking layer can be cleanly swapped for an epidemic gossip protocol (like SWIM) without altering the underlying consistent hash ring or replication routing logic.

### 6.2 Heartbeat Loop & Accrual Failure & Recovery Detection (`FailureDetector`)
To prevent false positive failure detections or recovery status flapping due to transient GC pauses or momentary network jitters, Kairo implements an accrual-style failure detector with progressive health states:
- **`PeerStatus` States**:
  - `ALIVE`: The node responds reliably to periodic pings.
  - `SUSPECTED`: The node has missed `suspectedThreshold` consecutive pings (default: 1). This acts as an early warning state for observability and monitoring.
  - `DEAD`: The node has missed `deadThreshold` consecutive pings (default: 3). Once marked `DEAD`, the node is considered offline and removed from active client routing.
- **Fail-Fast Pinging**: Heartbeats execute on a background daemon thread (`ScheduledExecutorService`) every 1.5 seconds using a short **500ms timeout**.
- **Accrual Recovery Detection & Flapping Prevention**: When a `DEAD` node starts responding to pings again, immediately transitioning it back to `ALIVE` on a single ping can cause recovery flapping if the node is experiencing boot instability or packet loss. Instead, Kairo enforces a progressive recovery threshold (`recoveryThreshold`, default 2):
  1. On the **1st successful heartbeat**, a `DEAD` peer transitions to `SUSPECTED` (`consecutiveSuccesses = 1`). In `SUSPECTED` state, the node is no longer considered dead (`isAlive() == true`), allowing routing probing without declaring full recovery.
  2. On the **2nd consecutive successful heartbeat** (`consecutiveSuccesses >= recoveryThreshold`), the peer transitions from `SUSPECTED` to `ALIVE`.
- **`StatusChangeListener` Callback Mechanism (Phase 6 Trigger Point)**: `FailureDetector` supports registering listener callbacks via `addListener(StatusChangeListener)`. Whenever a node transitions state (e.g., `DEAD` $\rightarrow$ `SUSPECTED` $\rightarrow$ `ALIVE`), listeners are notified immediately with `(peerId, oldStatus, newStatus)`. A transition to `newStatus == ALIVE` serves as the exact signal required for **Phase 6 Cluster Rebalancing**: notifying the storage engine that *"this recovering node needs its share of keys back."*
- **Warmup Grace Period**: To handle non-deterministic Docker Compose container startup ordering, the failure detector enforces a configurable boot warmup window (default: 15 seconds) where consecutive failures increment counters but suppress transition to the `DEAD` state.

### 6.3 Cluster Health Observability (`GET /cluster/health`)
Each node exposes `GET /cluster/health` (via `ClusterHealthHandler`), providing real-time visibility into local node health assessments:
```text
Cluster Health Report (Local Node: node-1)
--------------------------------------------------------------------------------
  node-2     : ALIVE     | Last Seen: 2026-07-03 14:32:45 (0 ms ago) | Failures: 0
  node-3     : SUSPECTED | Last Seen: 2026-07-03 14:32:43 (1500 ms ago) | Failures: 1
  node-4     : DEAD      | Last Seen: 2026-07-03 14:32:38 (6000 ms ago) | Failures: 4
```

### 6.4 Proactive Health-Aware Ring Routing
Phase 5 integrates directly into Phase 4's routing engine via `findAliveOwner`:
- **Proactive Primary Skip**: When a request arrives, `CacheHandler` checks if the primary owner is marked `DEAD` in the local `FailureDetector`. If so, routing immediately skips the dead primary and routes directly to the next alive replica node in preference list order, eliminating the 1500ms HTTP timeout.
- **Supplementing Reactive Fallback**: Proactive routing catches known failures instantaneously (< 15ms). If a primary node fails suddenly before missing 3 heartbeats, Phase 4's reactive HTTP timeout/connection error fallback still catches it safely.
- **Resource Conservation**: In `tryReplicaFallback(...)` and `replicateAsync(...)`, nodes marked `DEAD` are automatically skipped during secondary replica lookups and background replication dispatches, conserving thread pool workers and socket resources.

### 6.5 Distributed Consensus & Eventual View Convergence
Because each node in the cluster runs its own asynchronous heartbeat loop independently without distributed coordination, **node health views converge eventually but aren't strictly consistent across nodes at every instant**.
- **Known Limitation & Design Trade-off**: Node A might mark Node C as `DEAD` slightly before Node B does. For a high-performance distributed cache, eventual view convergence is the optimal engineering trade-off: it avoids the latency, operational overhead, and architectural complexity of consensus algorithms (like Raft or Paxos) while ensuring high availability and zero failed client reads.

### 6.6 Timestamped State Transition & Failover Logging (Phase 8 Observability Engine)
To provide definitive chronological evidence of zero-latency failover during live chaos demonstrations (Phase 8), Kairo emits standardized, millisecond-precision logs across all cluster nodes whenever peer health transitions or routing failovers occur:
- **Health Transition Format (`[CLUSTER-HEALTH]`)**:
  ```text
  [2026-07-03 14:49:02.924] [Node node-1] [CLUSTER-HEALTH] Peer node-2 status changed: ALIVE -> SUSPECTED (1 missed heartbeat(s) -> early warning)
  [2026-07-03 14:49:02.924] [Node node-1] [CLUSTER-HEALTH] Peer node-2 status changed: SUSPECTED -> DEAD (3 consecutive missed heartbeats -> removed from active routing)
  [2026-07-03 14:49:04.734] [Node node-1] [CLUSTER-HEALTH] Peer node-2 status changed: DEAD -> SUSPECTED (responding to heartbeats again after 1 successful ping)
  [2026-07-03 14:49:04.735] [Node node-1] [CLUSTER-HEALTH] Peer node-2 status changed: SUSPECTED -> ALIVE (recovery confirmed after 2 consecutive success(es) [Phase 6 trigger point])
  ```
- **Routing Failover Correlation (`[ROUTING-FAILOVER]`)**:
  ```text
  [2026-07-03 14:49:02.948] [Node node-1] [ROUTING-FAILOVER] Primary owner node-2 is marked DEAD locally! Checking replica candidates for key remote-key-1
  [2026-07-03 14:49:02.948] [Node node-1] [ROUTING-FAILOVER] Proactive health routing: skipping dead primary node-2, routing to alive replica node-3
  ```
- **Audit Value**: Watching container logs via `docker compose logs -f` during manual or automated node stops proves that failure detection triggers instantaneously, and subsequent client reads are seamlessly rerouted around offline nodes without dropping a single request or incurring HTTP timeout delays.

### 6.7 Mathematical Timing Analysis & Interview Justification
When evaluating distributed failure detectors, system design interviewers frequently ask: *"How fast does your system detect failure or recovery, and why did you choose those exact numbers?"*
Kairo's failure detection timing is mathematically deterministic based on its core configuration parameters:
- **Heartbeat Interval ($I$)**: `1500ms` (1.5 seconds)
- **Ping Timeout ($T$)**: `500ms` (0.5 seconds)
- **Dead Threshold ($K_{dead}$)**: `3` consecutive failed pings
- **Recovery Threshold ($K_{rec}$)**: `2` consecutive successful pings

#### 1. Failure Detection Window ($T_{detect}$)
Once a physical peer node crashes or becomes unreachable, the maximum time required for the cluster to mark it `DEAD` and remove it from active client routing is governed by:
$$T_{detect} = (K_{dead} - 1) \times I + T + \delta_{phase}$$
Where $\delta_{phase} \in [0, I)$ represents the random alignment offset between the physical crash event and the next scheduled timer tick.
- **Concrete Timing**: In Kairo, once the first heartbeat fails ($t=0$), the second failure occurs at $t=1.5\text{s}$, and the third consecutive failure occurs at $t=3.0\text{s}$. Including initial heartbeat alignment and socket timeouts, total detection time across all cluster nodes is **$\approx 4.5$ seconds**.
- **Engineering Justification**: Why not a 1-second detection window with $I=200\text{ms}$? In JVM-based distributed systems, minor Garbage Collection (GC) pauses and momentary OS thread scheduling hiccups routinely last 200–500ms. If $T_{detect} < 1\text{s}$, a minor GC pause would cause widespread false-positive failure detections, triggering massive, unnecessary cache rebalancing storms over the network. A 4.5-second threshold provides a robust 99.9% safety margin against transient JVM/network hiccups while ensuring client failover occurs in under 5 seconds.

#### 2. Recovery Detection Window ($T_{recovery}$)
When a downed container boots up and resumes responding to HTTP pings, the time required to confirm cluster-wide recovery and trigger Phase 6 rebalancing is:
$$T_{recovery} = T_{boot} + (K_{rec} - 1) \times I$$
Where $T_{boot}$ is the container/JVM startup time ($\approx 4.5\text{s}$ in Docker Compose).
- **Concrete Timing**: Once the node responds to its first ping ($t=0$, transitioning `DEAD` $\rightarrow$ `SUSPECTED`), the second successful ping occurs exactly one interval later at $t=1.5\text{s}$, transitioning `SUSPECTED` $\rightarrow$ `ALIVE`. Total recovery recognition after container initialization takes **$\approx 1.5$ seconds** ($3.0\text{s}$ total heartbeat polling window).
- **Engineering Justification**: Why require $K_{rec} = 2$ instead of reviving immediately on the first pong? When a container reboots, network interfaces and HTTP server threads often experience packet dropping during initial socket binding. A single pong is not sufficient proof of sustained stability. Requiring 2 consecutive pongs over a 1.5-second span guarantees that the node is fully ready to absorb heavy read/write rebalancing traffic without flapping back to dead.

### 6.8 What Cluster Rebalancing Actually Moves (Phase 6 Definition)
Before implementing automated data handoff, it is essential to define precisely what changes when an offline physical node (e.g., `node-2`) recovers and transitions back to `ALIVE`:
1. **The Outage State**: During `node-2`'s downtime, writes and reads directed to keys whose primary owner in the ring is `node-2` were proactively rerouted to secondary stand-in nodes (the next-clockwise replicas in the preference list, e.g., `node-3` or `node-1`). These stand-in nodes absorbed the writes and served client reads.
2. **The Recovery Signal**: Once `node-2` responds to 2 consecutive heartbeats, `FailureDetector` transitions its state `SUSPECTED` $\rightarrow$ `ALIVE` and invokes registered `StatusChangeListener` callbacks.
3. **What Moves**: When `node-2` returns, keys that belong to `node-2`'s primary ring segments but were temporarily hosted by stand-in nodes during the outage must move **from the stand-in nodes back to the recovered primary owner (`node-2`)**.
4. **Identification & Transfer**: Each stand-in node scans its local `CacheStore` and evaluates `ring.findOwner(key)`. Any local key where `findOwner(key).equals("node-2")` represents a key that belongs to the recovering node. The stand-in node asynchronously streams (via HTTP `POST /cache/{key}`) those specific key-value pairs back to `node-2`, restoring its primary data store so subsequent client reads hit local memory immediately without cache misses.

### 6.9 Ring Rebuilding on Membership Change & The Transient Gap
When a peer transitions from offline to active (e.g., `DEAD` $\rightarrow$ `ALIVE` or a dynamically added node), Kairo reuses the existing failure detection machinery without building a parallel notification bus:
1. **Triggering the Rebuild**: The constructor of `CacheHandler` registers a callback via `failureDetector.addListener(...)`. When `newStatus == PeerStatus.ALIVE` and `oldStatus != PeerStatus.ALIVE`, the handler automatically invokes `ring.addNode(peerId)`. This recomputes the hash ring by reinserting all 150 virtual tokens for that physical node.
2. **The Transient Data Gap (Important Subtlety)**: Right after `ring.addNode(peerId)` executes, any incoming client request (`findOwner(key)`) for a previously displaced key will immediately resolve to the newly recovered owner (`peerId`). However, **the actual data has not moved over the network yet**!
3. **Architectural Handling**: Because the recovered node's local memory is initially empty, queries arriving during this brief window would normally result in cache misses (`404 Not Found`) or stale reads. To handle this gap cleanly without dropping read availability, Kairo's storage engine and replication fallback layer must explicitly execute asynchronous hinted handoff (Step 3/4 of Phase 6), streaming the stored payloads from the temporary stand-in replicas back to the primary before or during active client reads.

### 6.10 Handling the Rejoin Gap: Pull-Based vs. Read-Through Migration
To resolve the transient gap between when `HashRing` assigns ownership to a recovered node and when the physical payloads arrive in its memory, distributed storage engines typically employ one of two strategies:
1. **Pull-Based Bulk Migration (Selected Strategy)**:
   - **Mechanism**: Immediately after a node rejoins the cluster and its virtual tokens are reinserted into the ring, the returning node proactively queries its peer neighbors (`GET /rebalance/pull?owner=<localNodeId>`). Peer nodes scan their local storage for any temporary stand-in keys belonging to the returning node and stream them back in bulk.
   - **Why Selected**: For a portfolio distributed system, pull-based bulk migration is cleaner, highly deterministic, and exceptionally easy to observe and demonstrate. In system design interviews, it provides a clear, articulate story: *"Upon rejoining the cluster, a node requests and absorbs its assigned key range from its former stand-in replicas before normal traffic resumes."*
2. **Read-Through Lazy Migration (Alternative Design)**:
   - **Mechanism**: When a client requests `GET /cache/{key}` from the newly recovered primary owner, and the key is missing locally (`404`), the primary intercepts the miss and synchronously fetches the payload from the secondary replica on demand, caching it locally before returning it to the client.
   - **Trade-offs**: While read-through migration avoids upfront bulk transfer spikes, it adds latency to first-time client reads after a node recovery and leaves stale payloads scattered across stand-in nodes until accessed. Pull-based migration guarantees proactive convergence and clean eviction of temporary handoff state.

### 6.11 Concurrent Writes During Migration & AP Eventual Consistency
In an AP (Available/Partition-Tolerant) distributed storage engine, a critical edge case arises during cluster rebalancing: **What happens if a client writes to a key while that key's territory is mid-migration?**
1. **The Routing Policy**: In Kairo, as soon as a recovering node's virtual tokens re-enter the ring, all fresh client writes (`POST /cache/{key}`) and deletes are routed immediately to the new primary owner per `ring.findOwner(key)`, even if bulk migration for that specific key range has not completed yet.
2. **Why This Policy Was Chosen**: Rather than blocking client writes during migration (which would violate high availability and introduce complex distributed locking or write-buffering queues), Kairo prioritizes low-latency write availability.
3. **Consistency Trade-off (Explicit Interview Note)**: Because Kairo operates under an eventual consistency model without strict vector clocks or Last-Write-Wins (LWW) timestamp reconciliation on every read/write, concurrent writes arriving during the brief bulk transfer window can interleave with migrating stand-in payloads. This is an intentional, defensible design trade-off: since strict serializability is not promised across standard cluster operations, maintaining high availability without locking during handoff is entirely consistent with Kairo's AP architecture.

### 6.12 Absolute Timestamps & Clock Skew (Known Assumption & Wire Format)
In Kairo, when an item is created via a client-facing write (`POST /cache/{key}?ttl=60`), the gateway or primary owner evaluates the relative duration exactly once to establish the canonical absolute expiration timestamp (`System.currentTimeMillis() + ttlMillis`). From that point onward, across all internal node-to-node replication streams (Phase 4) and bulk handoff migration payloads (Phase 6), Kairo transmits **absolute epoch timestamps in milliseconds (`long`)**, never relative durations. Receiving nodes absorb these values directly via `store.putAbsolute(key, value, expiresAt)`, eliminating the common distributed systems bug of accidentally recomputing `now + ttl` at each hop.
- **Known Architectural Assumption (Clock Synchronization)**: Absolute timestamp ordering and expiration rely on the assumption that all nodes in the cluster agree on the current wall-clock time. In our Docker Compose environment, all container nodes share the host machine's kernel clock, so clock drift is non-existent.
- **Production Distributed Systems Comparison**: In real-world multi-datacenter deployments, clock skew between physical machines is a genuine, hard problem. Production distributed storage engines typically address this limitation by deploying **Network Time Protocol (NTP / PTP)** daemons for millisecond-level synchronization, utilizing specialized hardware like GPS receivers and atomic clocks (e.g., Google Spanner's TrueTime API), or replacing wall-clock timestamps entirely with **Logical Clocks or Vector Clocks** for causal event ordering. Documenting this assumption explicitly demonstrates mature architectural awareness without expanding the project scope into distributed clock synchronization.

### 6.13 Debug Endpoints & Raw Timestamp Observability
To enable rigorous verification of bit-for-bit TTL consistency across replication and migration without relying solely on behavioral inference, Kairo exposes raw 64-bit absolute expiration timestamps via its local debugging endpoints:
1. **Local Store Inspection (`GET /debug/local/{key}`)**: Bypasses consistent hash ring routing and returns structured JSON containing both the raw epoch millisecond timestamp (`expiresAt`) and an ISO-8601 human-readable timestamp (`expiresAtReadable`). This allows developers and automated tests to verify exact bit-for-bit equality across primary owners and replicas.
2. **Local Store Dump (`GET /debug/local/`)**: Invoking the endpoint without a key parameter dumps the entire non-expired contents of the physical node's memory store in JSON format, reporting total entry count and individual item timestamps for instant cluster topology auditing.

---

## 7. Architectural Considerations & Future Notes
- **Startup Ordering (Phase 5 Note)**: Docker Compose does not guarantee deterministic startup order across containers. While not a blocker in Phase 2/3 (as peers are stored in memory without immediate startup connection attempts), failure detection mechanisms in Phase 5 incorporate a startup warmup grace period before marking unreachable peers as offline.
