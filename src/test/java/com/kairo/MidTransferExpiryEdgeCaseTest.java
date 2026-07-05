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

/**
 * Step 7: Targeted test for the edge case where a key expires during replication or migration.
 * Verifies that if an item's absolute expiration timestamp has already passed by the time
 * a receiving node processes a replication write or bulk migration handoff, the node
 * discards the payload immediately without storing lingering "zombie" entries in memory.
 */
class MidTransferExpiryEdgeCaseTest {

    private HttpServer server1;
    private HttpServer server2;
    private int port1;
    private int port2;
    private HttpClient client;
    private HashRing ring;
    private CacheStore store1;
    private CacheStore store2;
    private CacheHandler handler2;

    @BeforeEach
    void setUp() throws IOException {
        ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        store1 = new CacheStore(10);
        store2 = new CacheStore(10);

        server1 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port1 = server1.getAddress().getPort();

        server2 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);
        server1.createContext("/cache/", new CacheHandler(store1, config1, ring, 2, null));
        server1.createContext("/internal/handoff", new InternalHandoffHandler(store1, ring, config1));
        server1.start();

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);
        handler2 = new CacheHandler(store2, config2, ring, 2, null);
        server2.createContext("/cache/", handler2);
        server2.createContext("/internal/handoff", new InternalHandoffHandler(store2, ring, config2));
        server2.start();

        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server1 != null) server1.stop(0);
        if (server2 != null) server2.stop(0);
    }

    @Test
    void testKeyExpiresMidReplicationIsDiscarded() throws Exception {
        System.out.println("=== TEST 1: KEY EXPIRES MID-REPLICATION ===");
        String key = "zombie-rep-key";
        long pastTimestamp = System.currentTimeMillis() - 200; // Expired 200ms ago

        // Simulate a replication write arriving at node-2 with an already expired timestamp
        String targetUrl = "http://localhost:" + port2 + "/cache/" + key + "?expiresAt=" + pastTimestamp;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .header("X-Kairo-Replication", "true")
                .header("X-Kairo-Source-Node", "node-1")
                .POST(HttpRequest.BodyPublishers.ofString("zombie-val"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("Expired In Transit"),
                "Response should confirm item was discarded because it arrived expired");

        // Verify that node-2 did NOT store a zombie entry in its memory map
        assertFalse(store2.getSnapshot().containsKey(key), "store2 snapshot must NOT contain zombie key");
        assertNull(store2.getEntry(key), "store2 getEntry must return null for zombie key");

        System.out.println("[EDGE-CASE] Replication write arriving after expiry timestamp was cleanly discarded!");
    }

    @Test
    void testKeyExpiresMidMigrationIsDiscarded() throws Exception {
        System.out.println("=== TEST 2: KEY EXPIRES MID-MIGRATION (PUSH HANDOFF) ===");
        String key = "zombie-mig-key";
        long pastTimestamp = System.currentTimeMillis() - 500; // Expired 500ms ago

        // Simulate a push handoff payload arriving at node-2 with an expired timestamp
        String payload = key + "\tzombie-val\t" + pastTimestamp + "\n";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port2 + "/internal/handoff"))
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());

        // Verify that node-2 did NOT store the zombie entry
        assertFalse(store2.getSnapshot().containsKey(key), "store2 snapshot must NOT contain zombie migration key");
        assertNull(store2.getEntry(key), "store2 getEntry must return null");

        System.out.println("[EDGE-CASE] Push migration handoff arriving after expiry timestamp was cleanly discarded!");
    }

    @Test
    void testEndToEndExpiredKeyNotMigratedOnRejoin() throws Exception {
        System.out.println("=== TEST 3: END-TO-END PULL MIGRATION OF EXPIRED KEY ===");

        // Find a key owned by node-2 and replicated to node-1
        int i = 0;
        String key;
        while (true) {
            key = "e2e-zombie-key-" + i++;
            if ("node-2".equals(ring.findOwner(key))) {
                List<String> reps = ring.findReplicas(key, 2);
                if (!reps.isEmpty() && "node-1".equals(reps.get(0))) {
                    break;
                }
            }
        }

        // 1. SET key on node-2 with a very short TTL of 1 second
        String targetUrl = "http://localhost:" + port2 + "/cache/" + key + "?ttl=1";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .POST(HttpRequest.BodyPublishers.ofString("short-lived-data"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        // Wait 300ms for replication to node-1
        Thread.sleep(300);
        assertNotNull(store1.getEntry(key), "node-1 must have replicated copy initially");

        // 2. Simulate node-2 crash / memory wipe
        store2.delete(key);
        assertNull(store2.getEntry(key));

        // 3. Wait 1.0 second so the key expires while node-2 is offline
        System.out.println("[EDGE-CASE] Waiting 1000ms for item to expire while node-2 is down...");
        Thread.sleep(1000);

        // Confirm node-1's copy has expired
        assertNull(store1.getEntry(key), "node-1 must treat item as expired");

        // 4. Trigger Phase 6 pull-based handoff on rejoin
        System.out.println("[EDGE-CASE] Initiating pull-based rejoin migration on node-2...");
        handler2.initiateRejoinMigration();

        // 5. Confirm node-2 did not pull or store any zombie entry!
        assertFalse(store2.getSnapshot().containsKey(key), "Rejoined node-2 must NOT inherit expired zombie entry!");
        assertNull(store2.getEntry(key), "Rejoined node-2 must remain clean without zombie items");

        System.out.println("[EDGE-CASE] Success! Rejoin migration cleanly filtered out expired keys without creating zombie entries!");
    }
}
