package com.kairo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Map;

/**
 * Handles GET /debug/local/{key} and GET /debug/local/ requests.
 *
 * <p>Bypasses consistent hash ring routing completely and reads directly from
 * this node's local {@link CacheStore}. This endpoint is used for debugging
 * asynchronous replication, migration handoff, and TTL consistency.
 *
 * <p>Now includes the raw absolute expiry timestamp (and human-readable format)
 * to make manual verification of bit-for-bit TTL consistency fast and reliable.
 */
public class DebugLocalHandler implements HttpHandler {

    private final CacheStore store;

    public DebugLocalHandler(CacheStore store) {
        this.store = store;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method not allowed\n");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String prefix = "/debug/local/";
        String key = "";

        if (path.startsWith(prefix) && path.length() > prefix.length()) {
            key = path.substring(prefix.length());
        } else if (!path.equals("/debug/local") && !path.equals("/debug/local/")) {
            sendResponse(exchange, 400, "Bad Request: invalid path (expected /debug/local/{key} or /debug/local/)\n");
            return;
        }

        if (key.isEmpty()) {
            // Dump all items in local cache store
            Map<String, ValueEntry> snapshot = store.getSnapshot();
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"count\": ").append(snapshot.size()).append(",\n");
            sb.append("  \"entries\": [\n");
            int i = 0;
            for (Map.Entry<String, ValueEntry> e : snapshot.entrySet()) {
                ValueEntry val = e.getValue();
                String expReadable = (val.expiresAt() == ValueEntry.NO_EXPIRY)
                        ? "NO_EXPIRY"
                        : Instant.ofEpochMilli(val.expiresAt()).toString();

                sb.append("    {\"key\": \"").append(escapeJson(e.getKey()))
                  .append("\", \"value\": \"").append(escapeJson(val.value()))
                  .append("\", \"expiresAt\": ").append(val.expiresAt())
                  .append(", \"expiresAtReadable\": \"").append(expReadable).append("\"}");
                if (++i < snapshot.size()) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append("  ]\n");
            sb.append("}\n");

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            sendResponse(exchange, 200, sb.toString());
            return;
        }

        ValueEntry entry = store.getEntry(key);
        if (entry == null) {
            sendResponse(exchange, 404, "NOT FOUND\n");
        } else {
            String expReadable = (entry.expiresAt() == ValueEntry.NO_EXPIRY)
                    ? "NO_EXPIRY"
                    : Instant.ofEpochMilli(entry.expiresAt()).toString();

            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"key\": \"").append(escapeJson(key)).append("\",\n");
            sb.append("  \"value\": \"").append(escapeJson(entry.value())).append("\",\n");
            sb.append("  \"expiresAt\": ").append(entry.expiresAt()).append(",\n");
            sb.append("  \"expiresAtReadable\": \"").append(expReadable).append("\"\n");
            sb.append("}\n");

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            sendResponse(exchange, 200, sb.toString());
        }
    }

    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
