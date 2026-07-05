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
 * Step 5: Targeted test for Replication + TTL together.
 * Verifies that absolute expiry timestamps are transmitted bit-for-bit identically
 * from primary owners to replicas, and that both nodes independently expire entries
 * via active sweep and lazy eviction without discrepancies.
 */
class ReplicationTtlPrecisionTest {

    private HttpServer server1;
    private HttpServer server2;
    private int port1;
    private int port2;
    private HttpClient client;
    private HashRing ring;
    private CacheStore store1;
    private CacheStore store2;

    @BeforeEach
    void setUp() throws IOException {
        ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        // Use 1-second active sweep interval for rapid testing
        store1 = new CacheStore(1);
        store2 = new CacheStore(1);

        server1 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port1 = server1.getAddress().getPort();

        server2 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);
        server1.createContext("/cache/", new CacheHandler(store1, config1, ring, 2, null));
        server1.start();

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));
        NodeConfig config2 = new NodeConfig("node-2", port2, peers2);
        server2.createContext("/cache/", new CacheHandler(store2, config2, ring, 2, null));
        server2.start();

        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server1 != null) server1.stop(0);
        if (server2 != null) server2.stop(0);
    }

    private String findKeyForOwnerAndReplica(String owner, String replica) {
        int i = 0;
        while (true) {
            String key = "test-ttl-key-" + i++;
            if (owner.equals(ring.findOwner(key))) {
                List<String> reps = ring.findReplicas(key, 2);
                if (!reps.isEmpty() && replica.equals(reps.get(0))) {
                    return key;
                }
            }
        }
    }

    @Test
    void testReplicationTtlBitForBitEqualityAndActiveSweep() throws Exception {
        String key = findKeyForOwnerAndReplica("node-1", "node-2");
        System.out.printf("[TTL-TEST] Selected key '%s' (owner=node-1, replica=node-2)%n", key);

        // SET key with 3 seconds TTL on owner node-1
        String targetUrl = "http://localhost:" + port1 + "/cache/" + key + "?ttl=3";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .POST(HttpRequest.BodyPublishers.ofString("expiring-value"))
                .build();

        long beforeSetTime = System.currentTimeMillis();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        long afterSetTime = System.currentTimeMillis();

        assertEquals(201, response.statusCode());

        // Allow 400ms for async HTTP replication to complete
        Thread.sleep(400);

        ValueEntry entry1 = store1.getEntry(key);
        ValueEntry entry2 = store2.getEntry(key);

        assertNotNull(entry1, "Primary owner store1 must contain key");
        assertNotNull(entry2, "Replica store2 must contain replicated key");

        System.out.printf("[TTL-TEST] Owner entry1 expiresAt: %d%n", entry1.expiresAt());
        System.out.printf("[TTL-TEST] Replica entry2 expiresAt: %d%n", entry2.expiresAt());

        // 1. Bit-for-bit equality check
        assertEquals(entry1.expiresAt(), entry2.expiresAt(),
                "Expiry timestamps must be bit-for-bit identical between owner and replica!");

        // Verify the timestamp is approximately currentTime + 3000ms
        assertTrue(entry1.expiresAt() >= beforeSetTime + 3000, "Timestamp should be at least beforeSetTime + 3000");
        assertTrue(entry1.expiresAt() <= afterSetTime + 3000, "Timestamp should be at most afterSetTime + 3000");

        // 2. Wait until just past TTL (sleep 3.2 seconds total from set time)
        long remainingWait = (entry1.expiresAt() - System.currentTimeMillis()) + 1500;
        if (remainingWait > 0) {
            System.out.printf("[TTL-TEST] Waiting %d ms for TTL to expire and active sweep to run...%n", remainingWait);
            Thread.sleep(remainingWait);
        }

        // 3. Confirm both owner and replica independently swept the item
        assertFalse(store1.getSnapshot().containsKey(key),
                "Owner store1 should have purged expired key via active sweep!");
        assertFalse(store2.getSnapshot().containsKey(key),
                "Replica store2 should have purged expired key via active sweep!");

        System.out.println("[TTL-TEST] Both nodes successfully expired and purged the key independently without discrepancy!");
    }

    @Test
    void testReplicationTtlLazyEvictionOnBothNodes() throws Exception {
        String key = findKeyForOwnerAndReplica("node-1", "node-2");

        // SET key with 2 seconds TTL
        String targetUrl = "http://localhost:" + port1 + "/cache/" + key + "?ttl=2";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .POST(HttpRequest.BodyPublishers.ofString("lazy-val"))
                .build();

        client.send(request, HttpResponse.BodyHandlers.ofString());
        Thread.sleep(300); // Wait for async replication

        ValueEntry entry1 = store1.getEntry(key);
        ValueEntry entry2 = store2.getEntry(key);
        assertNotNull(entry1);
        assertNotNull(entry2);
        assertEquals(entry1.expiresAt(), entry2.expiresAt(), "Bit-for-bit timestamp equality required");

        // Wait just past TTL (2.2 seconds total from start)
        long timeToExpiry = entry1.expiresAt() - System.currentTimeMillis();
        if (timeToExpiry > 0) {
            Thread.sleep(timeToExpiry + 200);
        }

        // Test lazy eviction via HTTP GET on Owner
        HttpRequest getOwner = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port1 + "/cache/" + key))
                .GET()
                .build();
        HttpResponse<String> resOwner = client.send(getOwner, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, resOwner.statusCode(), "Owner should lazily evict expired item on GET");

        // Test lazy eviction via HTTP GET directly on Replica
        HttpRequest getReplica = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port2 + "/cache/" + key))
                .GET()
                .build();
        HttpResponse<String> resReplica = client.send(getReplica, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, resReplica.statusCode(), "Replica should lazily evict expired item on GET");

        System.out.println("[TTL-TEST] Lazy eviction verified on both primary owner and replica!");
    }
}
