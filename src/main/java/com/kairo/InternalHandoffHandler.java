package com.kairo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Handles internal cluster rebalancing and bulk migration requests.
 *
 * <p>Supports two operations:
 * <ul>
 *   <li><b>GET /internal/handoff?owner={peerId}</b>: Peer neighbor queries this node for all local keys
 *       whose primary owner per the rebuilt hash ring is {@code peerId}. Returns a URL-encoded tab-separated
 *       stream of matching entries (key\tvalue\texpiresAt\n).</li>
 *   <li><b>POST /internal/handoff</b>: Rejoining or stand-in nodes push bulk key-value payloads to this node.
 *       The handler decodes the entries and stores them directly in the local {@link CacheStore} via
 *       {@link CacheStore#putAbsolute(String, String, long)}.</li>
 * </ul>
 */
public class InternalHandoffHandler implements HttpHandler {

    private final CacheStore store;
    private final HashRing ring;
    private final NodeConfig config;
    private FailureDetector failureDetector;

    public InternalHandoffHandler(CacheStore store, HashRing ring, NodeConfig config) {
        this.store = store;
        this.ring = ring;
        this.config = config;
    }

    public void setFailureDetector(FailureDetector failureDetector) {
        this.failureDetector = failureDetector;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equalsIgnoreCase(method)) {
            handleGet(exchange);
        } else if ("POST".equalsIgnoreCase(method)) {
            handlePost(exchange);
        } else if ("DELETE".equalsIgnoreCase(method)) {
            handleDelete(exchange);
        } else {
            sendResponse(exchange, 405, "Method Not Allowed\n");
        }
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String targetOwner = null;
        if (query != null && query.startsWith("owner=")) {
            targetOwner = query.substring("owner=".length());
        }

        if (targetOwner == null || targetOwner.isEmpty()) {
            sendResponse(exchange, 400, "Bad Request: missing owner parameter in query (expected ?owner={peerId})\n");
            return;
        }

        if (failureDetector != null) {
            failureDetector.markAlive(targetOwner);
        }

        StringBuilder responseBuilder = new StringBuilder();
        Map<String, ValueEntry> snapshot = store.getSnapshot();
        int matchedKeys = 0;

        for (Map.Entry<String, ValueEntry> entry : snapshot.entrySet()) {
            String key = entry.getKey();
            String currentRingOwner = ring != null ? ring.findOwner(key) : null;
            if (targetOwner.equals(currentRingOwner)) {
                String encKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
                String encVal = URLEncoder.encode(entry.getValue().value(), StandardCharsets.UTF_8);
                long exp = entry.getValue().expiresAt();
                long wt = entry.getValue().writeTimestamp();
                responseBuilder.append(encKey).append("\t").append(encVal).append("\t").append(exp).append("\t").append(wt).append("\n");
                matchedKeys++;
            }
        }

        if (config != null) {
            System.out.printf("[%s] [CLUSTER-REBALANCE] Scanned local store for owner %s: found %d matching key(s) for handoff transfer.%n",
                    config.nodeId(), targetOwner, matchedKeys);
        }

        sendResponse(exchange, 200, responseBuilder.toString());
    }

    private void handlePost(HttpExchange exchange) throws IOException {
        int count = 0;
        long now = System.currentTimeMillis();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\t");
                if (parts.length >= 3) {
                    String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    long expiresAt = Long.parseLong(parts[2]);
                    long writeTimestamp = parts.length >= 4 ? Long.parseLong(parts[3]) : System.currentTimeMillis();
                    if (expiresAt > now || expiresAt == ValueEntry.NO_EXPIRY) {
                        store.putAbsolute(key, value, expiresAt, writeTimestamp);
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            sendResponse(exchange, 400, "Bad Request: malformed handoff payload\n");
            return;
        }

        if (config != null) {
            System.out.printf("[%s] [CLUSTER-REBALANCE] Absorbed %d handed-off key(s) into local cache store via POST /internal/handoff.%n",
                    config.nodeId(), count);
        }

        sendResponse(exchange, 200, "Handoff received: " + count + " keys\n");
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        int deletedCount = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String key = URLDecoder.decode(line, StandardCharsets.UTF_8);
                if (store.delete(key)) {
                    deletedCount++;
                }
            }
        } catch (Exception e) {
            sendResponse(exchange, 400, "Bad Request: malformed deletion confirmation payload\n");
            return;
        }

        // If no keys were passed in body, check query param ?owner={peerId}
        if (deletedCount == 0) {
            String query = exchange.getRequestURI().getQuery();
            String targetOwner = null;
            if (query != null && query.startsWith("owner=")) {
                targetOwner = query.substring("owner=".length());
            }
            if (targetOwner != null && !targetOwner.isEmpty()) {
                Map<String, ValueEntry> snapshot = store.getSnapshot();
                for (String key : snapshot.keySet()) {
                    String currentOwner = ring != null ? ring.findOwner(key) : null;
                    if (targetOwner.equals(currentOwner)) {
                        if (store.delete(key)) {
                            deletedCount++;
                        }
                    }
                }
            }
        }

        if (config != null) {
            System.out.printf("[%s] [CLUSTER-REBALANCE] Confirmed migration handoff: deleted %d stand-in key(s) from old replica location.%n",
                    config.nodeId(), deletedCount);
        }

        sendResponse(exchange, 200, "Handoff confirmed: deleted " + deletedCount + " keys\n");
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
