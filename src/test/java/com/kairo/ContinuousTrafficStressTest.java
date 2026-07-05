package com.kairo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 10: Stress Test With Continuous Traffic
 *
 * Simulates continuous, rapid-fire SETs and GETs against random cluster keys
 * while injecting a physical node crash and subsequent rejoin/handoff recovery in the middle.
 * Validates Core Requirement 07 (Zero Downtime / Read Continuity / No Corrupted Values).
 */
class ContinuousTrafficStressTest {

    private HttpServer server1;
    private HttpServer server2;
    private HttpServer server3;

    private int port1;
    private int port2;
    private int port3;

    private CacheStore store1;
    private CacheStore store2;
    private CacheStore store3;

    private FailureDetector fd1;
    private FailureDetector fd2;
    private FailureDetector fd3;

    private CacheHandler handler1;
    private CacheHandler handler2;
    private CacheHandler handler3;

    private NodeConfig config1;
    private NodeConfig config2;
    private NodeConfig config3;

    private final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(2000))
            .build();

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

        store1 = new CacheStore();
        store2 = new CacheStore();
        store3 = new CacheStore();

        config1 = new NodeConfig("node-1", port1, peers1);
        config2 = new NodeConfig("node-2", port2, peers2);
        config3 = new NodeConfig("node-3", port3, peers3);

        // Heartbeat = 150ms, timeout = 50ms, suspectedThreshold = 1, deadThreshold = 3
        fd1 = new FailureDetector("node-1", peers1, 150, 50, 1, 3, 2, 0L);
        fd2 = new FailureDetector("node-2", peers2, 150, 50, 1, 3, 2, 0L);
        fd3 = new FailureDetector("node-3", peers3, 150, 50, 1, 3, 2, 0L);

        HashRing ring1 = HashRing.fromConfig(config1, 150);
        HashRing ring2 = HashRing.fromConfig(config2, 150);
        HashRing ring3 = HashRing.fromConfig(config3, 150);

        handler1 = new CacheHandler(store1, config1, ring1, 2, fd1);
        handler2 = new CacheHandler(store2, config2, ring2, 2, fd2);
        handler3 = new CacheHandler(store3, config3, ring3, 2, fd3);

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
        InternalHandoffHandler handoffHandler = new InternalHandoffHandler(store, ring, config);
        // Look up the FailureDetector from the CacheHandler
        try {
            java.lang.reflect.Field fdField = CacheHandler.class.getDeclaredField("failureDetector");
            fdField.setAccessible(true);
            FailureDetector fd = (FailureDetector) fdField.get(handler);
            handoffHandler.setFailureDetector(fd);
        } catch (Exception e) {}
        server.createContext("/internal/handoff", handoffHandler);
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
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
    void testContinuousTrafficUnderChaos() throws Exception {
        System.out.println("=== STARTING STEP 10 CONTINUOUS TRAFFIC STRESS TEST ===");

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger writeSuccessCount = new AtomicInteger(0);
        AtomicInteger writeFailCount = new AtomicInteger(0);
        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readFailCount = new AtomicInteger(0);
        AtomicInteger wrongValueCount = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        // Tracks known written keys to their latest expected prefix
        ConcurrentHashMap<String, String> writtenKeys = new ConcurrentHashMap<>();

        // 1. Start background Writer Thread
        Thread writerThread = new Thread(() -> {
            int counter = 0;
            while (running.get()) {
                try {
                    int k = ThreadLocalRandom.current().nextInt(50);
                    String key = "stress-key-" + k;
                    String val = "val-" + counter++ + "-" + System.nanoTime();

                    // Route writes round-robin across alive gateways (node-1 and node-3)
                    int gatewayPort = (counter % 2 == 0) ? port1 : port3;
                    HttpRequest req = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + gatewayPort + "/cache/" + key))
                            .POST(HttpRequest.BodyPublishers.ofString(val))
                            .timeout(Duration.ofMillis(1500))
                            .build();

                    HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
                    if (res.statusCode() == 201) {
                        writtenKeys.put(key, val);
                        writeSuccessCount.incrementAndGet();
                    } else {
                        writeFailCount.incrementAndGet();
                    }
                    Thread.sleep(15);
                } catch (Exception e) {
                    if (running.get()) {
                        exceptionCount.incrementAndGet();
                    }
                }
            }
        }, "StressWriterThread");

        // 2. Start background Reader Thread
        Thread readerThread = new Thread(() -> {
            while (running.get()) {
                try {
                    if (writtenKeys.isEmpty()) {
                        Thread.sleep(5);
                        continue;
                    }
                    int k = ThreadLocalRandom.current().nextInt(50);
                    String key = "stress-key-" + k;
                    if (!writtenKeys.containsKey(key)) continue;

                    // Route reads round-robin across alive gateways (node-1 and node-3)
                    int gatewayPort = (ThreadLocalRandom.current().nextBoolean()) ? port1 : port3;
                    HttpRequest req = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + gatewayPort + "/cache/" + key))
                            .GET()
                            .timeout(Duration.ofMillis(1500))
                            .build();

                    HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
                    if (res.statusCode() == 200) {
                        readSuccessCount.incrementAndGet();
                        String body = res.body();
                        // Verify data integrity: value must start with "val-" and not be corrupted
                        if (body == null || !body.startsWith("val-")) {
                            wrongValueCount.incrementAndGet();
                        }
                    } else {
                        System.out.println("[STRESS-TEST-FAIL] GET " + key + " returned " + res.statusCode() + ": " + res.body().trim());
                        readFailCount.incrementAndGet();
                    }
                    Thread.sleep(10);
                } catch (Exception e) {
                    if (running.get()) {
                        exceptionCount.incrementAndGet();
                    }
                }
            }
        }, "StressReaderThread");

        writerThread.start();
        readerThread.start();

        // Phase A: Normal traffic pumping across all 3 nodes (1500ms)
        System.out.println("[STRESS-TEST] Phase A: Pumping normal continuous traffic across all 3 nodes...");
        Thread.sleep(1500);
        System.out.printf("[STRESS-TEST] Phase A Stats -> SETs: %d, GETs: %d%n", writeSuccessCount.get(), readSuccessCount.get());

        // Phase B: Manually stop node-2 in the middle of continuous traffic!
        System.out.println("[STRESS-TEST] Phase B: Injecting chaos -> Stopping server node-2 container & heartbeat!");
        server2.stop(0);
        fd2.stop();

        // Phase C: Let continuous traffic pump during node-2 outage (2000ms)
        System.out.println("[STRESS-TEST] Phase C: Pumping traffic during node-2 outage (testing failover & replica continuity)...");
        Thread.sleep(2000);
        System.out.printf("[STRESS-TEST] Phase C Stats -> SETs: %d, GETs: %d%n", writeSuccessCount.get(), readSuccessCount.get());

        // Phase D: Re-boot node-2 with clean memory store & trigger pull-based handoff
        System.out.println("[STRESS-TEST] Phase D: Re-booting node-2 container and initiating pull-based handoff...");
        server2 = HttpServer.create(new InetSocketAddress("localhost", port2), 0);
        store2 = new CacheStore();
        fd2 = new FailureDetector("node-2", config2.peers(), 150, 50, 1, 3, 2, 0L);
        HashRing ring2 = HashRing.fromConfig(config2, 150);
        handler2 = new CacheHandler(store2, config2, ring2, 2, fd2);
        setupServer(server2, handler2, store2, ring2, config2);
        server2.start();
        fd2.start();

        // Trigger handoff while continuous traffic is still pumping
        handler2.initiateRejoinMigration();

        // Phase E: Post-recovery traffic pumping (1500ms)
        System.out.println("[STRESS-TEST] Phase E: Pumping traffic post-rejoin after handoff absorption...");
        Thread.sleep(1500);

        // Cleanly terminate traffic threads
        running.set(false);
        writerThread.join();
        readerThread.join();

        System.out.println("\n=======================================================");
        System.out.println("====== STEP 10 CONTINUOUS TRAFFIC STRESS RESULTS ======");
        System.out.println("=======================================================");
        System.out.printf("Total Successful SETs : %d%n", writeSuccessCount.get());
        System.out.printf("Total Failed SETs     : %d%n", writeFailCount.get());
        System.out.printf("Total Successful GETs : %d%n", readSuccessCount.get());
        System.out.printf("Total Failed GETs     : %d%n", readFailCount.get());
        System.out.printf("Corrupted/Wrong Values: %d%n", wrongValueCount.get());
        System.out.printf("Unhandled Exceptions  : %d%n", exceptionCount.get());
        System.out.println("=======================================================\n");

        // Assertions for Core Requirement 07
        assertTrue(writeSuccessCount.get() > 50, "Must execute dozens of successful writes");
        assertTrue(readSuccessCount.get() > 100, "Must execute hundreds of successful reads");
        assertEquals(0, wrongValueCount.get(), "Must NEVER return corrupted or wrong values!");
        assertEquals(0, exceptionCount.get(), "Must NEVER encounter unhandled client exceptions!");
        assertEquals(0, readFailCount.get(), "Must have ZERO read failures during single node crash and recovery!");
    }
}
