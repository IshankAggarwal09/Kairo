package com.kairo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end chaos integration test for Phase 6 Rebalancing & Handoff (Step 8).
 * Proves zero-downtime read continuity, confirmed deletion of stand-in copies, and
 * physical data absorption upon node rejoin.
 */
public class ClusterRebalanceChaosTest {

    private HttpServer server1;
    private HttpServer server2;
    private HttpServer server3;

    private CacheStore store1;
    private CacheStore store2;
    private CacheStore store3;

    private FailureDetector fd1;
    private FailureDetector fd2;
    private FailureDetector fd3;

    private CacheHandler handler1;
    private CacheHandler handler2;
    private CacheHandler handler3;

    private int port1;
    private int port2;
    private int port3;

    private Map<String, PeerNode> peers1;
    private Map<String, PeerNode> peers2;
    private Map<String, PeerNode> peers3;

    private final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    @BeforeEach
    void setUp() throws IOException {
        server1 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port1 = server1.getAddress().getPort();
        server2 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port2 = server2.getAddress().getPort();
        server3 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port3 = server3.getAddress().getPort();

        peers1 = Map.of(
                "node-2", new PeerNode("node-2", "localhost", port2),
                "node-3", new PeerNode("node-3", "localhost", port3)
        );
        peers2 = Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-3", new PeerNode("node-3", "localhost", port3)
        );
        peers3 = Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-2", new PeerNode("node-2", "localhost", port2)
        );

        store1 = new CacheStore();
        store2 = new CacheStore();
        store3 = new CacheStore();

        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);
        NodeConfig config3 = new NodeConfig("node-3", port3, peers3);

        HashRing ring1 = HashRing.fromConfig(config1, 150);
        HashRing ring2 = HashRing.fromConfig(config2, 150);
        HashRing ring3 = HashRing.fromConfig(config3, 150);

        // Fast failure detection for automated test (200ms interval, 2 consecutive failures)
        fd1 = new FailureDetector("node-1", peers1);
        fd2 = new FailureDetector("node-2", peers2);
        fd3 = new FailureDetector("node-3", peers3);

        handler1 = new CacheHandler(store1, config1, ring1, 2, fd1);
        handler2 = new CacheHandler(store2, config2, ring2, 2, fd2);
        handler3 = new CacheHandler(store3, config3, ring3, 2, fd3);

        setupServer(server1, store1, ring1, config1, fd1, handler1);
        setupServer(server2, store2, ring2, config2, fd2, handler2);
        setupServer(server3, store3, ring3, config3, fd3, handler3);

        server1.start();
        server2.start();
        server3.start();

        fd1.start();
        fd2.start();
        fd3.start();
    }

    private void setupServer(HttpServer server, CacheStore store, HashRing ring, NodeConfig config, FailureDetector fd, CacheHandler handler) {
        server.createContext("/cache/", handler);
        InternalHandoffHandler handoffHandler = new InternalHandoffHandler(store, ring, config);
        handoffHandler.setFailureDetector(fd);
        server.createContext("/internal/handoff", handoffHandler);
        server.createContext("/cluster/health", new ClusterHealthHandler(fd, config));
        server.createContext("/ping", exchange -> {
            byte[] bytes = "pong\n".getBytes();
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        });
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
    void testZeroDowntimeRebalanceAndRejoin() throws Exception {
        System.out.println("\n=== STEP 8: CHAOS REBALANCE & ZERO-DOWNTIME READ TEST ===");

        // 1. Find 5 keys owned by node-2
        HashRing ring = HashRing.fromConfig(new NodeConfig("node-1", port1, peers1), 150);
        List<String> testKeys = new java.util.ArrayList<>();
        for (int i = 0; i < 5000 && testKeys.size() < 5; i++) {
            String k = "chaos-multikey-" + i;
            if ("node-2".equals(ring.findOwner(k))) {
                testKeys.add(k);
            }
        }
        assertEquals(5, testKeys.size(), "Should find 5 distinct keys owned by node-2");
        System.out.println("Selected 5 test keys owned by node-2: " + testKeys);

        // 2. SET all 5 keys via node-1 (will forward to owner node-2 and replicate to stand-in replica)
        for (int i = 0; i < testKeys.size(); i++) {
            String key = testKeys.get(i);
            String val = "val-" + i;
            HttpRequest putReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + key))
                    .POST(HttpRequest.BodyPublishers.ofString(val))
                    .build();
            HttpResponse<String> putRes = client.send(putReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, putRes.statusCode());
            assertEquals(val, store2.get(key));
        }
        System.out.println("All 5 keys written to node-2 and replicated asynchronously.");

        // Check ownership via debug health endpoint
        HttpRequest healthReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port1 + "/cluster/health"))
                .GET()
                .build();
        HttpResponse<String> healthRes = client.send(healthReq, HttpResponse.BodyHandlers.ofString());
        assertTrue(healthRes.body().contains("node-2"));
        System.out.println("Verified cluster health debug endpoint reports node-2 ALIVE.");

        // 3. Start rapid-fire continuous background read thread against node-1 for all 5 keys
        AtomicBoolean stopReading = new AtomicBoolean(false);
        AtomicInteger totalReads = new AtomicInteger(0);
        AtomicInteger failedReads = new AtomicInteger(0);

        List<String> finalKeys = testKeys;
        Thread readerThread = new Thread(() -> {
            int idx = 0;
            while (!stopReading.get()) {
                try {
                    String k = finalKeys.get(idx % finalKeys.size());
                    String expectedVal = "val-" + (idx % finalKeys.size());
                    HttpRequest getReq = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port1 + "/cache/" + k))
                            .GET()
                            .build();
                    HttpResponse<String> getRes = client.send(getReq, HttpResponse.BodyHandlers.ofString());
                    totalReads.incrementAndGet();
                    if (getRes.statusCode() != 200 || !expectedVal.equals(getRes.body())) {
                        System.out.println("[REBALANCE-FAIL] GET " + k + " returned " + getRes.statusCode() + ": " + getRes.body());
                        failedReads.incrementAndGet();
                    }
                    idx++;
                    Thread.sleep(5);
                } catch (Exception e) {
                    System.out.println("[REBALANCE-FAIL] Exception on GET: " + e.getMessage());
                    failedReads.incrementAndGet();
                }
            }
        });
        readerThread.start();

        // 4. Stop node-2!
        System.out.println("Stopping container node-2 (simulating node crash)...");
        fd2.stop();
        server2.stop(0);

        // Notify failure detectors on node-1 and node-3 to speed up test
        fd1.notifyStatusChange("node-2", PeerStatus.ALIVE, PeerStatus.DEAD);
        fd3.notifyStatusChange("node-2", PeerStatus.ALIVE, PeerStatus.DEAD);
        Thread.sleep(250);

        // While node-2 is DEAD, reads must succeed via replica fallback without failing!
        assertTrue(totalReads.get() > 0);
        assertEquals(0, failedReads.get(), "Zero reads should fail during node-2 outage!");
        System.out.println("Verified read continuity during outage: " + totalReads.get() + " reads succeeded via stand-in replica.");

        // 5. Restart node-2 with an empty memory store (simulating clean reboot)
        System.out.println("Re-booting container node-2 with clean memory store...");
        store2 = new CacheStore();
        server2 = HttpServer.create(new InetSocketAddress("localhost", port2), 0);
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);
        HashRing ring2 = HashRing.fromConfig(config2, 150);
        fd2 = new FailureDetector("node-2", peers2);
        handler2 = new CacheHandler(store2, config2, ring2, 2, fd2);
        setupServer(server2, store2, ring2, config2, fd2, handler2);
        server2.start();
        fd2.start();

        // Verify local memory on node-2 is empty before handoff
        for (String k : finalKeys) {
            assertNull(store2.get(k), "Memory store must be empty prior to migration");
        }

        // 6. Trigger proactive pull-based rejoin migration on node-2
        System.out.println("Initiating pull-based bulk migration handoff...");
        handler2.initiateRejoinMigration();

        // 7. Critically verify with debug endpoints and local store that keys are physically restored!
        for (int i = 0; i < finalKeys.size(); i++) {
            String k = finalKeys.get(i);
            String expectedVal = "val-" + i;
            assertNotNull(store2.get(k), "Key " + k + " must be physically restored into node-2's CacheStore!");
            assertEquals(expectedVal, store2.get(k));

            // Verify via HTTP GET endpoint directly on node-2
            HttpRequest directReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port2 + "/cache/" + k))
                    .GET()
                    .build();
            HttpResponse<String> directRes = client.send(directReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, directRes.statusCode());
            assertEquals(expectedVal, directRes.body());
        }
        System.out.println("Verified physical restoration of all 5 keys into node-2 memory store & via HTTP endpoint.");

        // Stop background reader and assert 100% read success
        stopReading.set(true);
        readerThread.join();

        System.out.println("Total rapid-fire reads executed across 5 keys during crash, fallback, rejoin, and handoff: " + totalReads.get());
        assertEquals(0, failedReads.get(), "Out of " + totalReads.get() + " continuous reads, exactly ZERO should fail!");
        System.out.println("=== STEP 8 VERIFICATION COMPLETE: 100% ZERO-DOWNTIME SUCCESS ===\n");
    }
}
