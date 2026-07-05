package com.kairo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Debug endpoint reporting local view of cluster peer health.
 *
 * <p>Endpoint: {@code GET /cluster/health}
 * <p>Reports status (ALIVE/SUSPECTED/DEAD), last contact timestamp, and failure counts per peer.
 */
public class ClusterHealthHandler implements HttpHandler {

    private final FailureDetector failureDetector;
    private final NodeConfig config;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    public ClusterHealthHandler(FailureDetector failureDetector, NodeConfig config) {
        this.failureDetector = failureDetector;
        this.config = config;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed\n");
                return;
            }

            Map<String, FailureDetector.PeerHealth> allHealth = failureDetector.getAllHealth();
            StringBuilder sb = new StringBuilder();
            sb.append("Cluster Health Report (Local Node: ").append(config.nodeId()).append(")\n");
            sb.append("--------------------------------------------------------------------------------\n");

            long now = System.currentTimeMillis();
            if (allHealth.isEmpty()) {
                sb.append("No remote peers configured in cluster.\n");
            } else {
                for (FailureDetector.PeerHealth health : allHealth.values()) {
                    long elapsed = now - health.getLastSeen();
                    String timeStr = TIME_FMT.format(Instant.ofEpochMilli(health.getLastSeen()));
                    sb.append(String.format("  %-10s : %-9s | Last Seen: %s (%d ms ago) | Failures: %d\n",
                            health.getPeerId(),
                            health.getStatus(),
                            timeStr,
                            elapsed,
                            health.getConsecutiveFailures()));
                }
            }

            sendResponse(exchange, 200, sb.toString());
        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal error: " + e.getMessage() + "\n");
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] bytes = response.getBytes();
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
