package com.kairo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Handles all requests under the {@code /cache/} prefix.
 *
 * <p>Includes consistent hashing routing: before handling locally, locates
 * the owner node via {@link HashRing#findOwner}. If owned by a remote peer,
 * transparently forwards the request over HTTP using an internal {@link HttpClient}
 * and relays the response back to the client. Prevents routing loops via the
 * {@code X-Kairo-Forwarded} header.
 */
public class CacheHandler implements HttpHandler {

    private static final String PREFIX = "/cache/";

    private final CacheStore store;
    private final NodeConfig config;
    private final HashRing ring;
    private final HttpClient httpClient;
    private final int replicationFactor;
    private final ExecutorService replicationExecutor;
    private final FailureDetector failureDetector;

    public CacheHandler(CacheStore store, NodeConfig config, HashRing ring) {
        this(store, config, ring, 2, null);
    }

    public CacheHandler(CacheStore store, NodeConfig config, HashRing ring, int replicationFactor) {
        this(store, config, ring, replicationFactor, null);
    }

    public CacheHandler(CacheStore store, NodeConfig config, HashRing ring, FailureDetector failureDetector) {
        this(store, config, ring, 2, failureDetector);
    }

    public CacheHandler(CacheStore store, NodeConfig config, HashRing ring, int replicationFactor, FailureDetector failureDetector) {
        this.store = store;
        this.config = config;
        this.ring = ring;
        this.replicationFactor = replicationFactor;
        this.failureDetector = failureDetector;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofMillis(1500))
                .build();
        this.replicationExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "kairo-replicator-" + config.nodeId());
            t.setDaemon(true);
            return t;
        });

        if (this.failureDetector != null && this.ring != null) {
            this.failureDetector.addListener((peerId, oldStatus, newStatus) -> {
                if (newStatus == PeerStatus.ALIVE && oldStatus != PeerStatus.ALIVE && oldStatus != PeerStatus.MIGRATING) {
                    logRoutingEvent("Ring membership change: peer " + peerId + " transitioned to ALIVE (" + oldStatus + " -> ALIVE). Reinserting virtual nodes into hash ring.");
                    this.ring.addNode(peerId);
                    this.failureDetector.setStatus(peerId, PeerStatus.MIGRATING);
                    triggerMigrationHandoff(peerId);
                }
            });
        }
    }

    public CacheHandler(CacheStore store) {
        this(store, NodeConfig.fromEnv(), HashRing.fromConfig(NodeConfig.fromEnv(), 150));
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String key = path.substring(PREFIX.length());

            if (key.isEmpty()) {
                sendResponse(exchange, 400, "Missing key in path\n");
                return;
            }

            String owner = findAliveOwner(key);
            boolean isForwarded = exchange.getRequestHeaders().containsKey("X-Kairo-Forwarded");
            boolean isReplication = exchange.getRequestHeaders().containsKey("X-Kairo-Replication");

            // Routing: if owned by a remote peer, NOT forwarded, and NOT a replication write -> forward over HTTP!
            if (owner != null && !owner.equals(config.nodeId()) && !isForwarded && !isReplication) {
                System.out.println("Node " + config.nodeId() + " -> forwarding " + exchange.getRequestMethod() + " /cache/" + key + " to owner " + owner);
                forwardToPeer(exchange, owner, key);
                return;
            }

            if (isReplication) {
                System.out.println("Node " + config.nodeId() + " -> handling replication write " + exchange.getRequestMethod() + " /cache/" + key + " locally");
            } else if (isForwarded) {
                System.out.println("Node " + config.nodeId() + " -> handling forwarded " + exchange.getRequestMethod() + " /cache/" + key + " locally");
            } else {
                System.out.println("Node " + config.nodeId() + " -> handling " + exchange.getRequestMethod() + " /cache/" + key + " locally (owner: " + owner + ")");
            }

            switch (exchange.getRequestMethod()) {
                case "POST"   -> handleSet(exchange, key);
                case "GET"    -> handleGet(exchange, key);
                case "DELETE" -> handleDelete(exchange, key);
                default -> sendResponse(exchange, 405, "Method not allowed\n");
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal error: " + e.getMessage() + "\n");
        }
    }

    /**
     * Forwards a request to the peer responsible for the key and relays its response.
     */
    private void forwardToPeer(HttpExchange exchange, String ownerId, String key) throws IOException {
        PeerNode peer = config.peers().get(ownerId);
        if (peer == null) {
            sendResponse(exchange, 502, "Bad Gateway: Peer " + ownerId + " not found in configuration\n");
            return;
        }

        String targetUrl = peer.baseUrl() + exchange.getRequestURI().toString();
        String method = exchange.getRequestMethod();
        byte[] bodyBytes = exchange.getRequestBody().readAllBytes();

        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .timeout(java.time.Duration.ofMillis(1500))
                .header("X-Kairo-Forwarded", "true");

        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null) {
            reqBuilder.header("Content-Type", contentType);
        }

        switch (method) {
            case "POST"   -> reqBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
            case "GET"    -> reqBuilder.GET();
            case "DELETE" -> reqBuilder.DELETE();
            default -> {
                sendResponse(exchange, 405, "Method not allowed\n");
                return;
            }
        }

        try {
            HttpResponse<byte[]> response = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
            if ("GET".equals(method) && response.statusCode() != 200) {
                logRoutingEvent("Primary owner " + ownerId + " returned non-200 (" + response.statusCode() + ") for GET /cache/" + key + ". Attempting replica fallback...");
                FallbackResult fb = tryReplicaFallback(exchange, key, ownerId);
                if (fb == FallbackResult.SUCCESS) {
                    return;
                } else if (fb == FallbackResult.ALL_UNREACHABLE && response.statusCode() >= 500) {
                    sendResponse(exchange, 503, "Service Unavailable: Both primary owner (" + ownerId + ") and all replica nodes for key '" + key + "' are unreachable or down\n");
                    return;
                } else if (fb == FallbackResult.MISS) {
                    sendResponse(exchange, 404, "NOT FOUND\n");
                    return;
                }
            }
            byte[] respBody = response.body();
            exchange.sendResponseHeaders(response.statusCode(), respBody.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(respBody);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if ("GET".equals(method)) {
                logRoutingEvent("Interrupted reaching primary owner " + ownerId + " for GET /cache/" + key + ". Attempting replica fallback...");
                FallbackResult fb = tryReplicaFallback(exchange, key, ownerId);
                if (fb == FallbackResult.SUCCESS) {
                    return;
                } else if (fb == FallbackResult.ALL_UNREACHABLE) {
                    sendResponse(exchange, 503, "Service Unavailable: Both primary owner (" + ownerId + ") and all replica nodes for key '" + key + "' are unreachable or down\n");
                    return;
                } else if (fb == FallbackResult.MISS) {
                    sendResponse(exchange, 404, "NOT FOUND\n");
                    return;
                }
            }
            sendResponse(exchange, 504, "Gateway Timeout: Interrupted while forwarding to " + ownerId + "\n");
        } catch (IOException e) {
            if (failureDetector != null) {
                failureDetector.markDead(ownerId);
            }
            if ("GET".equals(method)) {
                logRoutingEvent("Failed reaching primary owner " + ownerId + " (" + e.getMessage() + ") for GET /cache/" + key + ". Attempting replica fallback...");
                FallbackResult fb = tryReplicaFallback(exchange, key, ownerId);
                if (fb == FallbackResult.SUCCESS) {
                    return;
                } else if (fb == FallbackResult.ALL_UNREACHABLE) {
                    sendResponse(exchange, 503, "Service Unavailable: Both primary owner (" + ownerId + ") and all replica nodes for key '" + key + "' are unreachable or down\n");
                    return;
                } else if (fb == FallbackResult.MISS) {
                    sendResponse(exchange, 404, "NOT FOUND\n");
                    return;
                }
            }
            sendResponse(exchange, 502, "Bad Gateway: Failed to reach peer " + ownerId + " (" + e.getMessage() + ")\n");
        }
    }

    /**
     * DELETE /cache/{key}
     *
     * Returns 200 if the key existed and was removed, 404 if it wasn't
     * present. We choose strict semantics (404 on miss) over silent 200
     * so callers can distinguish "cleaned up" from "nothing was there" —
     * the operation is still idempotent (deleting twice is safe), but
     * informative.
     */
    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        boolean isReplication = exchange.getRequestHeaders().containsKey("X-Kairo-Replication");
        if (isReplication) {
            String sourceNode = exchange.getRequestHeaders().getFirst("X-Kairo-Source-Node");
            if (sourceNode == null) sourceNode = "unknown";
            System.out.println("Node " + config.nodeId() + " -> replication write received for key " + key + " from node " + sourceNode);
        }

        boolean existed = store.delete(key);
        if (existed) {
            sendResponse(exchange, 200, "DELETED\n");
        } else {
            sendResponse(exchange, 404, "NOT FOUND\n");
        }

        if (!isReplication) {
            replicateAsync("DELETE", key, null, new byte[0]);
        }
    }

    private void handleGet(HttpExchange exchange, String key) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        boolean isInternal = query != null && query.contains("internal=true");
        boolean isQuorum = parseQuorum(query);

        if (isQuorum && !isInternal) {
            handleQuorumGet(exchange, key);
            return;
        }

        ValueEntry entry = store.getEntry(key);
        if (entry == null) {
            boolean isFallback = exchange.getRequestHeaders().containsKey("X-Kairo-Fallback");
            boolean isForwarded = exchange.getRequestHeaders().containsKey("X-Kairo-Forwarded");
            String owner = findAliveOwner(key);
            boolean isPrimaryOwner = owner != null && owner.equals(config.nodeId());
            if (!isFallback && (!isForwarded || isPrimaryOwner) && !isInternal) {
                logRoutingEvent("Key " + key + " not found locally on primary owner. Attempting replica fallback...");
                FallbackResult fb = tryReplicaFallback(exchange, key, config.nodeId());
                if (fb == FallbackResult.SUCCESS) {
                    return;
                } else if (fb == FallbackResult.ALL_UNREACHABLE) {
                    sendResponse(exchange, 503, "Service Unavailable: Primary owner cache empty and all replica nodes for key '" + key + "' are unreachable or down\n");
                    return;
                }
            }
            sendResponse(exchange, 404, "NOT FOUND\n");
        } else {
            exchange.getResponseHeaders().add("X-Kairo-Write-Timestamp", String.valueOf(entry.writeTimestamp()));
            sendResponse(exchange, 200, entry.value());
        }
    }

    private void handleQuorumGet(HttpExchange exchange, String key) throws IOException {
        List<String> replicas = ring.findReplicas(key, replicationFactor);
        String owner = ring.findOwner(key);
        List<String> candidates = new ArrayList<>(replicas);
        if (owner != null && !candidates.contains(owner)) {
            candidates.add(owner);
        }

        String bestValue = null;
        long highestTimestamp = -1;
        int successfulReads = 0;

        for (String candidateId : candidates) {
            if (candidateId.equals(config.nodeId())) {
                ValueEntry localEntry = store.getEntry(key);
                if (localEntry != null) {
                    if (localEntry.writeTimestamp() > highestTimestamp) {
                        highestTimestamp = localEntry.writeTimestamp();
                        bestValue = localEntry.value();
                    }
                }
                successfulReads++;
            } else {
                if (failureDetector != null && !failureDetector.isAlive(candidateId)) continue;
                PeerNode peer = config.peers().get(candidateId);
                if (peer == null) continue;
                try {
                    HttpRequest req = HttpRequest.newBuilder()
                            .uri(URI.create(peer.baseUrl() + "/cache/" + key + "?internal=true"))
                            .timeout(java.time.Duration.ofMillis(1500))
                            .header("X-Kairo-Forwarded", "true")
                            .GET()
                            .build();
                    HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                    if (res.statusCode() == 200) {
                        successfulReads++;
                        String tsStr = res.headers().firstValue("X-Kairo-Write-Timestamp").orElse("-1");
                        long ts = Long.parseLong(tsStr);
                        if (ts > highestTimestamp) {
                            highestTimestamp = ts;
                            bestValue = res.body();
                        }
                    } else if (res.statusCode() == 404) {
                        successfulReads++;
                    }
                } catch (Exception e) {
                    logRoutingEvent("Quorum read from " + candidateId + " failed: " + e.getMessage());
                }
            }
        }

        int totalNodes = replicationFactor + 1;
        int requiredQuorum = (totalNodes / 2) + 1;

        if (successfulReads < requiredQuorum) {
            sendResponse(exchange, 503, "Service Unavailable: Quorum not met for read\n");
            return;
        }

        if (bestValue == null) {
            sendResponse(exchange, 404, "NOT FOUND\n");
        } else {
            exchange.getResponseHeaders().add("X-Kairo-Write-Timestamp", String.valueOf(highestTimestamp));
            sendResponse(exchange, 200, bestValue);
        }
    }

    private enum FallbackResult {
        SUCCESS,          // A replica returned 200 OK
        MISS,             // A replica was reachable and responded normally with 404 / null
        ALL_UNREACHABLE   // All replica nodes were unreachable / failed
    }

    /**
     * Attempts to read a key from replica nodes when the primary owner fails or misses.
     * As a last resort, also tries the ring primary owner even if it was marked dead —
     * this handles the post-migration window where stand-in copies have been deleted
     * but the recovered node's failure detector status hasn't converged yet.
     */
    private FallbackResult tryReplicaFallback(HttpExchange exchange, String key, String failedOwnerId) {
        List<String> replicas = ring.findReplicas(key, replicationFactor);
        boolean anyReplicaReached = false;

        // Build the full candidate list: replicas first, then the ring primary owner as last resort
        List<String> candidates = new ArrayList<>(replicas);
        String ringPrimary = ring.findOwner(key);
        if (ringPrimary != null && !candidates.contains(ringPrimary)) {
            candidates.add(ringPrimary);
        }

        for (String replicaId : candidates) {
            if (replicaId.equals(failedOwnerId)) {
                continue;
            }
            if (failureDetector != null && !failureDetector.isAlive(replicaId)) {
                continue;
            }
            if (replicaId.equals(config.nodeId())) {
                anyReplicaReached = true;
                logRoutingEvent("Replica fallback: serving GET /cache/" + key + " locally from replica store");
                String val = store.get(key);
                if (val != null) {
                    try {
                        sendResponse(exchange, 200, val);
                        return FallbackResult.SUCCESS;
                    } catch (IOException e) {
                        return FallbackResult.ALL_UNREACHABLE;
                    }
                }
                continue;
            }

            PeerNode peer = config.peers().get(replicaId);
            if (peer == null) {
                continue;
            }
            try {
                String targetUrl = peer.baseUrl() + "/cache/" + key;
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .timeout(java.time.Duration.ofMillis(1500))
                        .header("X-Kairo-Forwarded", "true")
                        .header("X-Kairo-Fallback", "true")
                        .GET()
                        .build();
                logRoutingEvent("Replica fallback: forwarding GET /cache/" + key + " to replica " + replicaId);
                HttpResponse<byte[]> res = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
                if (res.statusCode() == 200) {
                    byte[] respBody = res.body();
                    exchange.sendResponseHeaders(200, respBody.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(respBody);
                    }
                    return FallbackResult.SUCCESS;
                } else if (res.statusCode() == 404) {
                    anyReplicaReached = true;
                }
            } catch (Exception e) {
                logRoutingEvent("Replica fallback to " + replicaId + " failed: " + e.getMessage());
                if (failureDetector != null && e instanceof IOException) {
                    failureDetector.markDead(replicaId);
                }
            }
        }
        boolean localMiss = failedOwnerId != null && failedOwnerId.equals(config.nodeId());
        return (anyReplicaReached || localMiss) ? FallbackResult.MISS : FallbackResult.ALL_UNREACHABLE;
    }

    private void handleSet(HttpExchange exchange, String key) throws IOException {
        byte[] bodyBytes = exchange.getRequestBody().readAllBytes();
        String value = new String(bodyBytes);
        String query = exchange.getRequestURI().getQuery();
        boolean isReplication = exchange.getRequestHeaders().containsKey("X-Kairo-Replication");
        boolean isQuorum = parseQuorum(query);
        java.util.List<String> evictedKeys = null;

        long writeTimestamp;
        if (isReplication) {
            String sourceNode = exchange.getRequestHeaders().getFirst("X-Kairo-Source-Node");
            if (sourceNode == null) sourceNode = "unknown";
            System.out.println("Node " + config.nodeId() + " -> replication write received for key " + key + " from node " + sourceNode);

            long parsedWt = parseWriteTimestamp(query);
            writeTimestamp = parsedWt > 0 ? parsedWt : System.currentTimeMillis();
        } else {
            writeTimestamp = System.currentTimeMillis();
        }

        long expiresAt;
        if (isReplication) {
            long parsedExp = parseExpiresAt(query);
            expiresAt = (parsedExp > 0) ? parsedExp : ValueEntry.NO_EXPIRY;
        } else {
            long ttlMillis = parseTtlMillis(query);
            expiresAt = (ttlMillis > 0) ? System.currentTimeMillis() + ttlMillis : ValueEntry.NO_EXPIRY;
        }

        if (expiresAt != ValueEntry.NO_EXPIRY && expiresAt <= System.currentTimeMillis()) {
            if (isReplication) {
                System.out.println("Node " + config.nodeId() + " -> replication write for key " + key + " arrived already expired! Discarding zombie entry.");
                sendResponse(exchange, 200, "OK (Expired In Transit)\n");
            } else {
                sendResponse(exchange, 201, "STORED (Expired instantly)\n");
            }
            return;
        }

        evictedKeys = store.putAbsolute(key, value, expiresAt, writeTimestamp);

        if (!isReplication) {
            if (isQuorum) {
                boolean repSuccess = replicateSync("POST", key, query, bodyBytes);
                if (!repSuccess) {
                    store.delete(key);
                    sendResponse(exchange, 503, "Service Unavailable: Quorum not met\n");
                    return;
                }
            } else {
                replicateAsync("POST", key, query, bodyBytes);
            }
            if (evictedKeys != null) {
                for (String evictedKey : evictedKeys) {
                    System.out.println("Node " + config.nodeId() + " -> replicating LRU eviction DELETE for key " + evictedKey);
                    replicateAsync("DELETE", evictedKey, null, new byte[0]);
                }
            }
        }

        sendResponse(exchange, 201, "STORED\n");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private void logRoutingEvent(String message) {
        String timestamp = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .format(java.time.LocalDateTime.now());
        System.out.printf("[%s] [Node %s] [ROUTING-FAILOVER] %s%n", timestamp, config.nodeId(), message);
    }

    /**
     * Locates the primary owner for a key. If proactive failure detection is enabled
     * and the primary owner is marked DEAD locally, skips it and returns the first
     * ALIVE replica from the preference list.
     */
    private String findAliveOwner(String key) {
        String primary = ring.findOwner(key);
        if (failureDetector == null || failureDetector.isAlive(primary)) {
            return primary;
        }
        logRoutingEvent("Primary owner " + primary + " is marked DEAD locally! Checking replica candidates for key " + key);
        List<String> candidates = ring.findReplicas(key, replicationFactor);
        for (String candidate : candidates) {
            if (failureDetector.isAlive(candidate)) {
                if (!candidate.equals(primary)) {
                    logRoutingEvent("Proactive health routing: skipping dead primary " + primary + ", routing to alive replica " + candidate);
                }
                return candidate;
            }
        }
        return primary;
    }

    /**
     * Asynchronously dispatches a replication write to all replica nodes in the preference list.
     */
    private void replicateAsync(String method, String key, String query, byte[] bodyBytes) {
        List<String> replicas = ring.findReplicas(key, replicationFactor);
        if (replicas.isEmpty()) {
            return;
        }

        String uriStr = PREFIX + key;
        if ("POST".equals(method)) {
            ValueEntry entry = store.getEntry(key);
            if (entry != null) {
                if (entry.expiresAt() != ValueEntry.NO_EXPIRY) {
                    uriStr += "?expiresAt=" + entry.expiresAt();
                }
                uriStr += (uriStr.contains("?") ? "&" : "?") + "writeTimestamp=" + entry.writeTimestamp();
            }
        }

        for (String replicaId : replicas) {
            if (failureDetector != null && !failureDetector.isAlive(replicaId)) {
                System.out.println("Node " + config.nodeId() + " -> Async replication: skipping dead replica " + replicaId);
                continue;
            }
            PeerNode peer = config.peers().get(replicaId);
            if (peer == null) {
                continue;
            }

            final String finalUriStr = uriStr;
            replicationExecutor.submit(() -> {
                try {
                    String targetUrl = peer.baseUrl() + finalUriStr;
                    HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                            .uri(URI.create(targetUrl))
                            .header("X-Kairo-Replication", "true")
                            .header("X-Kairo-Forwarded", "true")
                            .header("X-Kairo-Source-Node", config.nodeId());

                    if ("POST".equals(method)) {
                        reqBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
                    } else if ("DELETE".equals(method)) {
                        reqBuilder.DELETE();
                    }

                    System.out.println("Node " + config.nodeId() + " -> replicating " + method + " " + PREFIX + key + " asynchronously to " + replicaId);
                    HttpResponse<String> res = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
                    if (res.statusCode() >= 400) {
                        System.err.println("Node " + config.nodeId() + " -> replication of " + key + " to " + replicaId + " failed with status " + res.statusCode());
                    }
                } catch (Exception e) {
                    System.err.println("Node " + config.nodeId() + " -> async replication error for " + key + " to " + replicaId + ": " + e.getMessage());
                    if (failureDetector != null && e instanceof IOException) {
                        failureDetector.markDead(replicaId);
                    }
                }
            });
        }
    }

    private boolean replicateSync(String method, String key, String query, byte[] bodyBytes) {
        List<String> replicas = ring.findReplicas(key, replicationFactor);
        if (replicas.isEmpty()) {
            return true;
        }

        String uriStr = PREFIX + key;
        if ("POST".equals(method)) {
            ValueEntry entry = store.getEntry(key);
            if (entry != null) {
                if (entry.expiresAt() != ValueEntry.NO_EXPIRY) {
                    uriStr += "?expiresAt=" + entry.expiresAt();
                }
                uriStr += (uriStr.contains("?") ? "&" : "?") + "writeTimestamp=" + entry.writeTimestamp();
            }
        }

        boolean allSuccess = true;
        for (String replicaId : replicas) {
            if (failureDetector != null && !failureDetector.isAlive(replicaId)) {
                System.out.println("Node " + config.nodeId() + " -> Sync replication: skipping dead replica " + replicaId);
                allSuccess = false;
                continue;
            }
            PeerNode peer = config.peers().get(replicaId);
            if (peer == null) {
                allSuccess = false;
                continue;
            }

            try {
                String targetUrl = peer.baseUrl() + uriStr;
                HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .header("X-Kairo-Replication", "true")
                        .header("X-Kairo-Forwarded", "true")
                        .header("X-Kairo-Source-Node", config.nodeId())
                        .timeout(java.time.Duration.ofMillis(1500));

                if ("POST".equals(method)) {
                    reqBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
                } else if ("DELETE".equals(method)) {
                    reqBuilder.DELETE();
                }

                System.out.println("Node " + config.nodeId() + " -> replicating " + method + " " + PREFIX + key + " synchronously to " + replicaId);
                HttpResponse<String> res = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() >= 400) {
                    System.err.println("Node " + config.nodeId() + " -> sync replication of " + key + " to " + replicaId + " failed with status " + res.statusCode());
                    allSuccess = false;
                }
            } catch (Exception e) {
                System.err.println("Node " + config.nodeId() + " -> sync replication error for " + key + " to " + replicaId + ": " + e.getMessage());
                if (failureDetector != null && e instanceof IOException) {
                    failureDetector.markDead(replicaId);
                }
                allSuccess = false;
            }
        }
        return allSuccess;
    }

    /**
     * Manually parses the query string for an {@code expiresAt} parameter (absolute epoch millis).
     * Returns -1 if not present or invalid.
     */
    private long parseExpiresAt(String query) {
        if (query == null || query.isEmpty()) {
            return -1;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && "expiresAt".equals(kv[0])) {
                try {
                    return Long.parseLong(kv[1]);
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    /**
     * Manually parses the query string for a {@code writeTimestamp} parameter (absolute epoch millis).
     * Returns -1 if not present or invalid.
     */
    private long parseWriteTimestamp(String query) {
        if (query == null || query.isEmpty()) {
            return -1;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && "writeTimestamp".equals(kv[0])) {
                try {
                    return Long.parseLong(kv[1]);
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    /**
     * Manually parses the query string for a {@code ttl} parameter (in seconds).
     * Returns -1 if not present or invalid.
     */
    private long parseTtlMillis(String query) {
        if (query == null || query.isEmpty()) {
            return -1;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && "ttl".equals(kv[0])) {
                try {
                    long seconds = Long.parseLong(kv[1]);
                    return seconds > 0 ? seconds * 1000 : -1;
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    private boolean parseQuorum(String query) {
        if (query == null || query.isEmpty()) {
            return false;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && "consistency".equals(kv[0])) {
                return "quorum".equalsIgnoreCase(kv[1]);
            }
        }
        return false;
    }

    /**
     * Sends a plain-text response and closes the exchange.
     */
    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    /**
     * Pushes stand-in keys owned by a newly recovered peer to that peer via POST /internal/handoff.
     * Keeps peer in MIGRATING state during transfer, confirms deletion of stand-in copies, and promotes to ALIVE.
     */
    private void triggerMigrationHandoff(String peerId) {
        PeerNode peer = config.peers().get(peerId);
        if (peer == null) {
            if (failureDetector != null) failureDetector.setStatus(peerId, PeerStatus.ALIVE);
            return;
        }

        Map<String, ValueEntry> snapshot = store.getSnapshot();
        List<String> matchingKeys = new ArrayList<>();
        StringBuilder payload = new StringBuilder();

        for (Map.Entry<String, ValueEntry> entry : snapshot.entrySet()) {
            String key = entry.getKey();
            if (ring.findOwner(key).equals(peerId)) {
                matchingKeys.add(key);
                ValueEntry val = entry.getValue();
                try {
                    payload.append(URLEncoder.encode(key, StandardCharsets.UTF_8)).append("\t")
                           .append(URLEncoder.encode(val.value(), StandardCharsets.UTF_8)).append("\t")
                           .append(val.expiresAt()).append("\t")
                           .append(val.writeTimestamp()).append("\n");
                } catch (Exception e) {
                    // ignore encoding errors
                }
            }
        }

        System.out.printf("[%s] [CLUSTER-REBALANCE] Migration start: peer %s rejoined (status: MIGRATING), requesting handoff for %d keys.%n",
                config.nodeId(), peerId, matchingKeys.size());

        if (!matchingKeys.isEmpty()) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(peer.baseUrl() + "/internal/handoff"))
                        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                        .timeout(java.time.Duration.ofMillis(3000))
                        .build();
                HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() == 200) {
                    System.out.printf("[%s] [CLUSTER-REBALANCE] Migration progress: successfully pushed %d key(s) to rejoining owner %s.%n",
                            config.nodeId(), matchingKeys.size(), peerId);
                    int deleted = 0;
                    for (String k : matchingKeys) {
                        if (store.delete(k)) deleted++;
                    }
                    System.out.printf("[%s] [CLUSTER-REBALANCE] Migration progress: confirmed deletion of %d stand-in key(s) from old replica location.%n",
                            config.nodeId(), deleted);
                } else {
                    System.err.printf("[%s] [CLUSTER-REBALANCE] Migration failed to push handoff to %s (status %d).%n",
                            config.nodeId(), peerId, res.statusCode());
                }
            } catch (Exception e) {
                System.err.printf("[%s] [CLUSTER-REBALANCE] Migration transfer error to %s: %s%n",
                        config.nodeId(), peerId, e.getMessage());
            }
        }

        if (failureDetector != null) {
            failureDetector.setStatus(peerId, PeerStatus.ALIVE);
        }
        System.out.printf("[%s] [CLUSTER-REBALANCE] Migration complete: peer %s now serving its full key range (status: MIGRATING -> ALIVE).%n",
                config.nodeId(), peerId);
    }

    /**
     * Can be invoked by a newly started node to proactively query peers and pull its assigned key territory.
     */
    public void initiateRejoinMigration() {
        if (config.peers().isEmpty()) return;
        System.out.printf("[%s] [CLUSTER-REBALANCE] Migration start: local node rejoined, requesting handoff for key range from %d peers.%n",
                config.nodeId(), config.peers().size());

        int totalPulled = 0;
        for (PeerNode peer : config.peers().values()) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(peer.baseUrl() + "/internal/handoff?owner=" + config.nodeId()))
                        .GET()
                        .timeout(java.time.Duration.ofMillis(3000))
                        .build();
                HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() == 200 && res.body() != null && !res.body().isEmpty()) {
                    String[] lines = res.body().split("\n");
                    List<String> confirmedKeys = new ArrayList<>();
                    long now = System.currentTimeMillis();
                    for (String line : lines) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 3) {
                            String k = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                            String v = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                            long exp = Long.parseLong(parts[2]);
                            long wt = parts.length >= 4 ? Long.parseLong(parts[3]) : System.currentTimeMillis();
                            if (exp > now || exp == ValueEntry.NO_EXPIRY) {
                                store.putAbsolute(k, v, exp, wt);
                                confirmedKeys.add(k);
                                totalPulled++;
                            }
                        }
                    }
                    if (!confirmedKeys.isEmpty()) {
                        System.out.printf("[%s] [CLUSTER-REBALANCE] Migration progress: pulled %d key(s) from peer %s.%n",
                                config.nodeId(), confirmedKeys.size(), peer.id());
                        StringBuilder delBody = new StringBuilder();
                        for (String k : confirmedKeys) {
                            delBody.append(URLEncoder.encode(k, StandardCharsets.UTF_8)).append("\n");
                        }
                        HttpRequest delReq = HttpRequest.newBuilder()
                                .uri(URI.create(peer.baseUrl() + "/internal/handoff"))
                                .method("DELETE", HttpRequest.BodyPublishers.ofString(delBody.toString()))
                                .timeout(java.time.Duration.ofMillis(3000))
                                .build();
                        httpClient.send(delReq, HttpResponse.BodyHandlers.ofString());
                    }
                }
            } catch (Exception e) {
                System.err.printf("[%s] [CLUSTER-REBALANCE] Rejoin pull error from %s: %s%n",
                        config.nodeId(), peer.id(), e.getMessage());
            }
        }
        System.out.printf("[%s] [CLUSTER-REBALANCE] Migration complete: local node absorbed %d key(s) and is now serving its full key range.%n",
                config.nodeId(), totalPulled);
    }
}
