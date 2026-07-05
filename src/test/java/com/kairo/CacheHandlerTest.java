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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CacheHandlerTest {

    private HttpServer server;
    private int port;
    private HttpClient client;
    private HashRing ring;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        client = HttpClient.newHttpClient();

        // Setup config for node-1 with a non-existent peer node-2 on port 9999
        Map<String, PeerNode> peers = new HashMap<>();
        peers.put("node-2", new PeerNode("node-2", "localhost", 9999));
        NodeConfig config = new NodeConfig("node-1", port, peers);

        // Build ring where node-2 is guaranteed to own some keys
        ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        CacheStore store = new CacheStore();
        server.createContext("/cache/", new CacheHandler(store, config, ring));
        server.setExecutor(null);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testLoopPreventionWithForwardedHeader() throws Exception {
        // Find a key that belongs to node-2
        String targetKey = "key-0";
        int counter = 0;
        while (!"node-2".equals(ring.findOwner(targetKey))) {
            targetKey = "key-" + (++counter);
        }

        // 1. Without X-Kairo-Forwarded, node-1 attempts to forward to localhost:9999 (node-2), which fails (Connection refused).
        // With replica fallback enabled, node-1 falls back to itself as replica and returns 404 NOT FOUND.
        HttpRequest reqNormal = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cache/" + targetKey))
                .GET()
                .build();
        HttpResponse<String> resNormal = client.send(reqNormal, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, resNormal.statusCode(), "Should fall back to replica node-1 and return 404 when primary owner is unreachable");

        // 2. WITH X-Kairo-Forwarded header, node-1 MUST NOT forward to node-2.
        // Instead, it serves locally and returns 404 NOT FOUND (since targetKey is not in node-1's local store).
        HttpRequest reqForwarded = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cache/" + targetKey))
                .header("X-Kairo-Forwarded", "true")
                .GET()
                .build();
        HttpResponse<String> resForwarded = client.send(reqForwarded, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, resForwarded.statusCode(), "Should serve locally instead of forwarding when X-Kairo-Forwarded is present");
        assertTrue(resForwarded.body().contains("NOT FOUND"));
    }

    @Test
    void testAsynchronousReplication() throws Exception {
        HttpServer server1 = HttpServer.create(new InetSocketAddress(0), 0);
        int port1 = server1.getAddress().getPort();

        HttpServer server2 = HttpServer.create(new InetSocketAddress(0), 0);
        int port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);

        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        CacheStore store1 = new CacheStore();
        CacheStore store2 = new CacheStore();

        server1.createContext("/cache/", new CacheHandler(store1, config1, ring));
        server1.setExecutor(null);
        server1.start();

        server2.createContext("/cache/", new CacheHandler(store2, config2, ring));
        server2.setExecutor(null);
        server2.start();

        try {
            // Find a key owned by node-1 so node-2 is guaranteed to be its replica
            String targetKey = "rep-key-0";
            int counter = 0;
            while (!"node-1".equals(ring.findOwner(targetKey))) {
                targetKey = "rep-key-" + (++counter);
            }

            // Client writes to node-1
            HttpRequest reqSet = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                    .POST(HttpRequest.BodyPublishers.ofString("replicated-value"))
                    .build();

            HttpResponse<String> resSet = client.send(reqSet, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, resSet.statusCode());
            assertEquals("replicated-value", store1.get(targetKey), "Primary owner node-1 must store value locally");

            // Allow background async replication thread to complete
            Thread.sleep(300);

            assertEquals("replicated-value", store2.get(targetKey), "Replica node-2 must receive and store replicated value asynchronously");
            assertEquals(ValueEntry.NO_EXPIRY, store2.getEntry(targetKey).expiresAt(), "Replica entry should have NO_EXPIRY");
        } finally {
            server1.stop(0);
            server2.stop(0);
        }
    }

    @Test
    void testAsynchronousReplicationWithTtl() throws Exception {
        HttpServer server1 = HttpServer.create(new InetSocketAddress(0), 0);
        int port1 = server1.getAddress().getPort();

        HttpServer server2 = HttpServer.create(new InetSocketAddress(0), 0);
        int port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);

        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        CacheStore store1 = new CacheStore();
        CacheStore store2 = new CacheStore();

        server1.createContext("/cache/", new CacheHandler(store1, config1, ring));
        server1.setExecutor(null);
        server1.start();

        server2.createContext("/cache/", new CacheHandler(store2, config2, ring));
        server2.setExecutor(null);
        server2.start();

        try {
            String targetKey = "ttl-key-0";
            int counter = 0;
            while (!"node-1".equals(ring.findOwner(targetKey))) {
                targetKey = "ttl-key-" + (++counter);
            }

            // Client writes to node-1 with TTL = 10 seconds
            HttpRequest reqSet = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey + "?ttl=10"))
                    .POST(HttpRequest.BodyPublishers.ofString("expiring-value"))
                    .build();

            client.send(reqSet, HttpResponse.BodyHandlers.ofString());
            Thread.sleep(300);

            ValueEntry entry1 = store1.getEntry(targetKey);
            ValueEntry entry2 = store2.getEntry(targetKey);

            assertNotNull(entry1);
            assertNotNull(entry2);
            assertEquals("expiring-value", entry2.value());
            assertEquals(entry1.expiresAt(), entry2.expiresAt(), "Replica must store the EXACT same absolute expiry timestamp as the primary owner!");
        } finally {
            server1.stop(0);
            server2.stop(0);
        }
    }

    @Test
    void testReplicaFallbackOnReadFailure() throws Exception {
        HttpServer server1 = HttpServer.create(new InetSocketAddress(0), 0);
        int port1 = server1.getAddress().getPort();

        HttpServer server2 = HttpServer.create(new InetSocketAddress(0), 0);
        int port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);

        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        CacheStore store1 = new CacheStore();
        CacheStore store2 = new CacheStore();

        server1.createContext("/cache/", new CacheHandler(store1, config1, ring));
        server1.setExecutor(null);
        server1.start();

        server2.createContext("/cache/", new CacheHandler(store2, config2, ring));
        server2.setExecutor(null);
        server2.start();

        try {
            // Find a key owned by node-2 (so node-1 is the replica)
            String targetKey = "fb-key-0";
            int counter = 0;
            while (!"node-2".equals(ring.findOwner(targetKey))) {
                targetKey = "fb-key-" + (++counter);
            }

            // Simulate that the value was replicated to node-1
            store1.put(targetKey, "fallback-survivor-value");

            // Kill node-2 (the primary owner)
            server2.stop(0);

            // Client requests key from node-1
            HttpRequest reqGet = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                    .GET()
                    .build();

            HttpResponse<String> resGet = client.send(reqGet, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resGet.statusCode(), "Should fall back to replica node-1 when primary owner node-2 is dead");
            assertEquals("fallback-survivor-value", resGet.body());
        } finally {
            server1.stop(0);
        }
    }

    @Test
    void testReplicaAlsoDownEdgeCase() throws Exception {
        HttpServer server1 = HttpServer.create(new InetSocketAddress(0), 0);
        int port1 = server1.getAddress().getPort();

        // Use two closed/unreachable ports for node-2 and node-3
        int port2 = 9998;
        int port3 = 9997;

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        peers1.put("node-3", new PeerNode("node-3", "localhost", port3));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        CacheStore store1 = new CacheStore();
        server1.createContext("/cache/", new CacheHandler(store1, config1, ring));
        server1.setExecutor(null);
        server1.start();

        try {
            // Find a key whose primary owner is node-2 and replica is node-3 (or vice versa), so neither is node-1
            String targetKey = "down-key-0";
            int counter = 0;
            while (true) {
                targetKey = "down-key-" + (++counter);
                String owner = ring.findOwner(targetKey);
                List<String> reps = ring.findReplicas(targetKey, 2);
                if (!owner.equals("node-1") && !reps.contains("node-1")) {
                    break;
                }
            }

            // Client requests key from node-1.
            // Primary owner (node-2 or node-3) is down -> fails.
            // Replica (node-3 or node-2) is also down -> fails.
            // Should return 503 Service Unavailable!
            HttpRequest reqGet = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                    .GET()
                    .build();

            HttpResponse<String> resGet = client.send(reqGet, HttpResponse.BodyHandlers.ofString());
            assertEquals(503, resGet.statusCode(), "Should return 503 Service Unavailable when both primary owner and replica are unreachable");
            assertTrue(resGet.body().contains("Service Unavailable"));
        } finally {
            server1.stop(0);
        }
    }

    @Test
    void testAsyncReplicationViaDebugLocalEndpoint() throws Exception {
        HttpServer server1 = HttpServer.create(new InetSocketAddress(0), 0);
        int port1 = server1.getAddress().getPort();

        HttpServer server2 = HttpServer.create(new InetSocketAddress(0), 0);
        int port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);

        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        CacheStore store1 = new CacheStore();
        CacheStore store2 = new CacheStore();

        server1.createContext("/cache/", new CacheHandler(store1, config1, ring));
        server1.createContext("/debug/local/", new DebugLocalHandler(store1));
        server1.setExecutor(null);
        server1.start();

        server2.createContext("/cache/", new CacheHandler(store2, config2, ring));
        server2.createContext("/debug/local/", new DebugLocalHandler(store2));
        server2.setExecutor(null);
        server2.start();

        try {
            String targetKey = "async-debug-key-0";
            int counter = 0;
            while (!"node-1".equals(ring.findOwner(targetKey))) {
                targetKey = "async-debug-key-" + (++counter);
            }

            // Client writes to node-1
            HttpRequest reqSet = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                    .POST(HttpRequest.BodyPublishers.ofString("async-debug-value"))
                    .build();

            HttpResponse<String> resSet = client.send(reqSet, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, resSet.statusCode());

            // Query replica's local store directly via /debug/local/ bypassing ring routing.
            // Since replication is asynchronous, we wait for the background call to arrive.
            HttpRequest reqDebug = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port2 + "/debug/local/" + targetKey))
                    .GET()
                    .build();

            int maxRetries = 20;
            HttpResponse<String> resDebug = null;
            for (int i = 0; i < maxRetries; i++) {
                resDebug = client.send(reqDebug, HttpResponse.BodyHandlers.ofString());
                if (resDebug.statusCode() == 200) {
                    break;
                }
                Thread.sleep(50);
            }

            assertNotNull(resDebug);
            assertTrue(resDebug.body().contains("\"value\": \"async-debug-value\""), "Must contain value in JSON dump");
            assertTrue(resDebug.body().contains("\"expiresAt\": " + ValueEntry.NO_EXPIRY), "Must contain raw expiresAt timestamp");
        } finally {
            server1.stop(0);
            server2.stop(0);
        }
    }

    @Test
    void testDeleteUnderReplication() throws Exception {
        HttpServer server1 = HttpServer.create(new InetSocketAddress(0), 0);
        int port1 = server1.getAddress().getPort();

        HttpServer server2 = HttpServer.create(new InetSocketAddress(0), 0);
        int port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);

        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        CacheStore store1 = new CacheStore();
        CacheStore store2 = new CacheStore();

        server1.createContext("/cache/", new CacheHandler(store1, config1, ring));
        server1.createContext("/debug/local/", new DebugLocalHandler(store1));
        server1.setExecutor(null);
        server1.start();

        server2.createContext("/cache/", new CacheHandler(store2, config2, ring));
        server2.createContext("/debug/local/", new DebugLocalHandler(store2));
        server2.setExecutor(null);
        server2.start();

        try {
            String targetKey = "del-rep-key-0";
            int counter = 0;
            while (!"node-1".equals(ring.findOwner(targetKey))) {
                targetKey = "del-rep-key-" + (++counter);
            }

            // Client writes to node-1
            HttpRequest reqSet = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                    .POST(HttpRequest.BodyPublishers.ofString("to-be-deleted"))
                    .build();
            HttpResponse<String> resSet = client.send(reqSet, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, resSet.statusCode());

            // Wait for replication to arrive on replica node-2
            Thread.sleep(300);
            assertEquals("to-be-deleted", store2.get(targetKey), "Replica must initially have the replicated value");

            // Client sends DELETE to node-1
            HttpRequest reqDel = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                    .DELETE()
                    .build();
            HttpResponse<String> resDel = client.send(reqDel, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resDel.statusCode());
            assertNull(store1.get(targetKey), "Primary owner must delete key locally");

            // Verify async replication of DELETE removes key from replica node-2
            HttpRequest reqDebug = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port2 + "/debug/local/" + targetKey))
                    .GET()
                    .build();

            int maxRetries = 20;
            HttpResponse<String> resDebug = null;
            for (int i = 0; i < maxRetries; i++) {
                resDebug = client.send(reqDebug, HttpResponse.BodyHandlers.ofString());
                if (resDebug.statusCode() == 404) {
                    break;
                }
                Thread.sleep(50);
            }

            assertNotNull(resDebug);
            assertEquals(404, resDebug.statusCode(), "Replica must eventually receive DELETE replication write and remove key");
            assertNull(store2.get(targetKey), "Replica store must no longer contain key after async DELETE replication");
        } finally {
            server1.stop(0);
            server2.stop(0);
        }
    }
}
