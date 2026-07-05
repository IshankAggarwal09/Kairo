package com.kairo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Debug endpoint for inspecting and manipulating the consistent hash ring.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>GET /ring — dumps the current ring's virtual node positions and their owners</li>
 *   <li>GET /ring/owner?key={key} — returns the physical node ID responsible for the key</li>
 *   <li>POST /ring/remove?node={nodeId} — removes a node's virtual tokens from the ring</li>
 *   <li>POST /ring/add?node={nodeId} — adds a node's virtual tokens back to the ring</li>
 * </ul>
 */
public class RingHandler implements HttpHandler {

    private final HashRing ring;

    public RingHandler(HashRing ring) {
        this.ring = ring;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();
            String method = exchange.getRequestMethod();

            if ((path.equals("/ring") || path.equals("/ring/")) && "GET".equals(method)) {
                Map<Long, String> tokens = ring.getRingSnapshot();
                Map<String, Integer> nodeCounts = new LinkedHashMap<>();
                for (String node : tokens.values()) {
                    nodeCounts.put(node, nodeCounts.getOrDefault(node, 0) + 1);
                }

                StringBuilder sb = new StringBuilder();
                sb.append("Ring Status: ").append(tokens.size()).append(" total tokens\n");
                sb.append("Physical Nodes:\n");
                for (Map.Entry<String, Integer> e : nodeCounts.entrySet()) {
                    double pct = (e.getValue() * 100.0) / Math.max(1, tokens.size());
                    sb.append(String.format("  - %s: %d tokens (%.1f%%)\n", e.getKey(), e.getValue(), pct));
                }

                if ("true".equals(parseParam(query, "dump"))) {
                    sb.append("\nToken Map (Token -> Physical Node):\n");
                    for (Map.Entry<Long, String> e : tokens.entrySet()) {
                        sb.append(String.format("  %10d -> %s\n", e.getKey(), e.getValue()));
                    }
                } else {
                    sb.append("\nTip: add ?dump=true to view all virtual token positions.\n");
                }
                sendResponse(exchange, 200, sb.toString());
            } else if (path.equals("/ring/owner") && "GET".equals(method)) {
                String key = parseParam(query, "key");
                if (key == null) {
                    sendResponse(exchange, 400, "Missing key parameter\n");
                    return;
                }
                String owner = ring.findOwner(key);
                if (owner == null) {
                    sendResponse(exchange, 200, "RING EMPTY\n");
                    return;
                }
                String rfParam = parseParam(query, "rf");
                int rf = rfParam != null ? Integer.parseInt(rfParam) : 2;
                List<String> replicas = ring.findReplicas(key, rf);
                List<String> preferenceList = new ArrayList<>();
                preferenceList.add(owner);
                preferenceList.addAll(replicas);
                StringBuilder sb = new StringBuilder();
                sb.append("Key: ").append(key).append("\n");
                sb.append("Primary Owner: ").append(owner).append("\n");
                sb.append("Replicas (RF=").append(rf).append("): ").append(replicas).append("\n");
                sb.append("Preference List: ").append(preferenceList).append("\n");
                sendResponse(exchange, 200, sb.toString());
            } else if (path.equals("/ring/remove") && "POST".equals(method)) {
                String nodeId = parseParam(query, "node");
                if (nodeId == null) {
                    sendResponse(exchange, 400, "Missing node parameter\n");
                    return;
                }
                ring.removeNode(nodeId);
                sendResponse(exchange, 200, "Removed " + nodeId + " | Remaining tokens: " + ring.size() + "\n");
            } else if (path.equals("/ring/add") && "POST".equals(method)) {
                String nodeId = parseParam(query, "node");
                if (nodeId == null) {
                    sendResponse(exchange, 400, "Missing node parameter\n");
                    return;
                }
                ring.addNode(nodeId);
                sendResponse(exchange, 200, "Added " + nodeId + " | Total tokens: " + ring.size() + "\n");
            } else {
                sendResponse(exchange, 404, "Unknown endpoint or method\n");
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal error: " + e.getMessage() + "\n");
        }
    }

    private String parseParam(String query, String paramName) {
        if (query == null || query.isEmpty()) return null;
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2 && kv[0].equals(paramName)) {
                return kv[1];
            }
        }
        return null;
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
