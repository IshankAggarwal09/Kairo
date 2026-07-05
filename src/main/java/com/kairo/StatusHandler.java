package com.kairo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Handles GET /status requests.
 *
 * <p>Returns the node's identity, port, and known cluster peers in JSON format.
 * Extremely useful for verifying cluster view and topology at a glance.
 */
public class StatusHandler implements HttpHandler {

    private final NodeConfig config;

    public StatusHandler(NodeConfig config) {
        this.config = config;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method not allowed\n");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"nodeId\": \"").append(config.nodeId()).append("\",\n");
        sb.append("  \"port\": ").append(config.port()).append(",\n");
        sb.append("  \"peers\": [\n");
        int i = 0;
        for (PeerNode peer : config.peers().values()) {
            sb.append("    {\"id\": \"").append(peer.id())
              .append("\", \"host\": \"").append(peer.host())
              .append("\", \"port\": ").append(peer.port())
              .append(", \"url\": \"").append(peer.baseUrl()).append("\"}");
            if (++i < config.peers().size()) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");

        byte[] bytes = sb.toString().getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
