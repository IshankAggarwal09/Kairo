package com.kairo;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Main entry point for Kairo — a self-healing distributed cache.
 *
 * Boots a lightweight HTTP server whose identity, port, and cluster
 * peers are configured via environment variables (see {@link NodeConfig}).
 */
public class Main {

    public static void main(String[] args) throws IOException {
        NodeConfig config = NodeConfig.fromEnv();

        HttpServer server = HttpServer.create(new InetSocketAddress(config.port()), 0);

        // Health check — isolates "is my HTTP plumbing correct" from cache logic.
        server.createContext("/ping", exchange -> {
            byte[] response = "pong".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });

        // Build consistent hash ring (~150 virtual nodes per physical node)
        HashRing ring = HashRing.fromConfig(config, 150);

        // Initialize FailureDetector for background peer health heartbeats
        FailureDetector failureDetector = new FailureDetector(config.nodeId(), config.peers());
        server.createContext("/cluster/health", new ClusterHealthHandler(failureDetector, config));

        // Cache endpoint — all operations under /cache/{key} with cluster routing and proactive failure detection
        CacheStore cacheStore = new CacheStore(5, config.maxKeysPerNode());
        server.createContext("/cache/", new CacheHandler(cacheStore, config, ring, failureDetector));

        // Cluster status endpoint — reports node identity and known peers
        server.createContext("/status", new StatusHandler(config));

        // Debug ring endpoint — inspect owners and simulate node removal/addition
        server.createContext("/ring", new RingHandler(ring));

        // Debug local store endpoint — reads directly from this node's local CacheStore without ring routing
        server.createContext("/debug/local/", new DebugLocalHandler(cacheStore));

        // Internal handoff endpoint — handles cluster rebalancing and bulk migration
        InternalHandoffHandler handoffHandler = new InternalHandoffHandler(cacheStore, ring, config);
        handoffHandler.setFailureDetector(failureDetector);
        server.createContext("/internal/handoff", handoffHandler);

        failureDetector.start();

        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();

        System.out.println("Node " + config.nodeId()
                + " starting on port " + config.port()
                + " | peers: " + config.peers().values()
                + " | ring tokens: " + ring.size());
    }
}
