package com.kairo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 4: Baseline Healthy Cluster Test
 *
 * Runs the LoadGenerator against a fully healthy 3-node cluster with no chaos injected.
 * Confirms 100% first-try success as the baseline — if this fails, there's a bug in
 * Phases 1–7 that must be fixed before Phase 8 can prove anything meaningful.
 *
 * <p>This test runs for ~15 seconds with aggressive request rates to generate meaningful
 * volume (500+ requests) and asserts:
 * <ul>
 *   <li>Zero hard failures (SET or GET)</li>
 *   <li>Zero retry attempts needed</li>
 *   <li>Every successful request was a first-try success</li>
 * </ul>
 */
class HealthyClusterBaselineTest {

    private HttpServer server1;
    private HttpServer server2;
    private HttpServer server3;

    private int port1;
    private int port2;
    private int port3;

    private FailureDetector fd1;
    private FailureDetector fd2;
    private FailureDetector fd3;

    @BeforeEach
    void setUp() throws IOException {
        server1 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port1 = server1.getAddress().getPort();
        server2 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port2 = server2.getAddress().getPort();
        server3 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port3 = server3.getAddress().getPort();

        Map<String, PeerNode> peers1 = new ConcurrentHashMap<>(Map.of(
                "node-2", new PeerNode("node-2", "localhost", port2),
                "node-3", new PeerNode("node-3", "localhost", port3)
        ));
        Map<String, PeerNode> peers2 = new ConcurrentHashMap<>(Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-3", new PeerNode("node-3", "localhost", port3)
        ));
        Map<String, PeerNode> peers3 = new ConcurrentHashMap<>(Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-2", new PeerNode("node-2", "localhost", port2)
        ));

        CacheStore store1 = new CacheStore();
        CacheStore store2 = new CacheStore();
        CacheStore store3 = new CacheStore();

        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);
        NodeConfig config3 = new NodeConfig("node-3", port3, peers3);

        fd1 = new FailureDetector("node-1", peers1, 150, 50, 1, 3, 2, 0L);
        fd2 = new FailureDetector("node-2", peers2, 150, 50, 1, 3, 2, 0L);
        fd3 = new FailureDetector("node-3", peers3, 150, 50, 1, 3, 2, 0L);

        HashRing ring1 = HashRing.fromConfig(config1, 150);
        HashRing ring2 = HashRing.fromConfig(config2, 150);
        HashRing ring3 = HashRing.fromConfig(config3, 150);

        CacheHandler handler1 = new CacheHandler(store1, config1, ring1, 2, fd1);
        CacheHandler handler2 = new CacheHandler(store2, config2, ring2, 2, fd2);
        CacheHandler handler3 = new CacheHandler(store3, config3, ring3, 2, fd3);

        setupServer(server1, handler1, store1, ring1, config1);
        setupServer(server2, handler2, store2, ring2, config2);
        setupServer(server3, handler3, store3, ring3, config3);

        server1.start();
        server2.start();
        server3.start();

        fd1.start();
        fd2.start();
        fd3.start();
    }

    private void setupServer(HttpServer server, CacheHandler handler, CacheStore store, HashRing ring, NodeConfig config) {
        server.createContext("/ping", exchange -> {
            byte[] response = "pong".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (java.io.OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        server.createContext("/cache/", handler);
        server.createContext("/internal/handoff", new InternalHandoffHandler(store, ring, config));
        server.setExecutor(Executors.newCachedThreadPool());
    }

    @AfterEach
    void tearDown() {
        if (fd1 != null) fd1.stop();
        if (fd2 != null) fd2.stop();
        if (fd3 != null) fd3.stop();
        if (server1 != null) server1.stop(0);
        if (server2 != null) server2.stop(0);
        if (server3 != null) server3.stop(0);
    }

    @Test
    void testHealthyClusterBaselineZeroFailures() throws Exception {
        // Allow heartbeats to converge so all nodes see each other as ALIVE
        Thread.sleep(500);

        System.out.println("================================================================================");
        System.out.println(" STEP 4: HEALTHY CLUSTER BASELINE TEST");
        System.out.println(" Running LoadGenerator against fully healthy 3-node cluster (no chaos)");
        System.out.println(" Target: 100% first-try success, 0 retries, 0 hard failures");
        System.out.println("================================================================================");

        // Run 500 requests with 10ms sleep across all 3 nodes, 1 retry allowed (should never be needed)
        LoadGenerator generator = new LoadGenerator(
                Arrays.asList(port1, port2, port3),
                /* keyPoolSize */ 50,
                /* sleepMs */ 10,
                /* maxRequests */ 500,
                /* maxRetries */ 1,
                /* retryDelayMs */ 200
        );

        generator.runLoop();

        // ---- Assertions ----

        long totalReqs = generator.getTotalRequests();
        long firstTry = generator.getFirstTrySuccess();
        long retriedOk = generator.getRetriedThenSucceeded();
        long hardFail = generator.getHardFailures();
        long retryAttempts = generator.getTotalRetryAttempts();

        System.out.println("\n======================================================");
        System.out.println("====== STEP 4 BASELINE VERIFICATION ======");
        System.out.println("======================================================");
        System.out.printf("Total Requests   : %d%n", totalReqs);
        System.out.printf("First-try Success: %d%n", firstTry);
        System.out.printf("Retry Success    : %d%n", retriedOk);
        System.out.printf("Hard Failures    : %d%n", hardFail);
        System.out.printf("Retry Attempts   : %d%n", retryAttempts);
        System.out.println("======================================================");

        // Core assertion: every request should have been executed
        assertEquals(500, totalReqs, "Expected exactly 500 requests to be executed");

        // The headline number: ZERO hard failures against a healthy cluster
        assertEquals(0, hardFail,
                "BASELINE VIOLATED: Hard failures detected against a healthy cluster! " +
                "This indicates a bug in Phases 1-7, not Phase 8.");

        // No retries should have been needed — every request should succeed on first try
        assertEquals(0, retryAttempts,
                "BASELINE VIOLATED: Retry attempts were needed against a healthy cluster! " +
                "This means some requests failed on first attempt, which shouldn't happen " +
                "when all 3 nodes are up.");

        // Every successful request should be a first-try success
        // (total successes = firstTry + retriedOk; both SET and GET successes count)
        long totalSuccesses = generator.getSetSuccess() + generator.getGetHit() + generator.getGetMiss();
        assertEquals(totalSuccesses, firstTry,
                "Not all successes were first-try. Some required retries against a healthy cluster.");

        assertEquals(0, retriedOk,
                "BASELINE VIOLATED: Some requests needed retries to succeed against a healthy cluster.");

        System.out.println("\n✅ BASELINE CONFIRMED: 100% first-try success across " + totalReqs +
                " requests against healthy 3-node cluster.");
    }
}
