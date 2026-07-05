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
 * Step 6: Targeted test for Migration + TTL together.
 * Verifies that when a node crashes and rejoins while an item is within its TTL window,
 * the Phase 6 bulk handoff payload preserves the absolute timestamp bit-for-bit,
 * rather than restarting a fresh TTL duration window from the moment of migration.
 */
class MigrationTtlPrecisionTest {

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

    private String findKeyOwnedByNode2() {
        int i = 0;
        while (true) {
            String key = "migration-ttl-key-" + i++;
            if ("node-2".equals(ring.findOwner(key))) {
                List<String> reps = ring.findReplicas(key, 2);
                if (!reps.isEmpty() && "node-1".equals(reps.get(0))) {
                    return key;
                }
            }
        }
    }

    @Test
    void testMigrationPreservesAbsoluteTtlBitForBit() throws Exception {
        String key = findKeyOwnedByNode2();
        System.out.printf("[MIGRATION-TTL] Selected key '%s' (owner=node-2, replica=node-1)%n", key);

        // 1. SET key with 10 seconds TTL
        String targetUrl = "http://localhost:" + port2 + "/cache/" + key + "?ttl=10";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .POST(HttpRequest.BodyPublishers.ofString("valuable-session-data"))
                .build();

        long beforeSetTime = System.currentTimeMillis();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        // Wait 400ms for replication to finish and time to elapse
        Thread.sleep(400);

        ValueEntry originalEntry = store2.getEntry(key);
        ValueEntry replicaEntry = store1.getEntry(key);

        assertNotNull(originalEntry, "Primary owner node-2 must have stored item initially");
        assertNotNull(replicaEntry, "Replica node-1 must have received item via replication");
        assertEquals(originalEntry.expiresAt(), replicaEntry.expiresAt(), "Initial replication timestamp must match");

        long originalExpiresAt = originalEntry.expiresAt();
        System.out.printf("[MIGRATION-TTL] Original absolute expiresAt timestamp: %d%n", originalExpiresAt);

        // 2. Simulate node-2 crash & memory loss (clear store2)
        System.out.println("[MIGRATION-TTL] Simulating node-2 crash and wipe of in-memory store...");
        store2.delete(key);
        assertNull(store2.getEntry(key), "node-2 memory store must be clean after restart");

        // Allow another 1000ms to pass while node-2 is down
        Thread.sleep(1000);

        long rejoinTime = System.currentTimeMillis();
        System.out.printf("[MIGRATION-TTL] node-2 rejoining cluster at epoch ms: %d (%.1f seconds after SET)%n",
                rejoinTime, (rejoinTime - beforeSetTime) / 1000.0);

        // 3. Trigger Phase 6 pull-based handoff on rejoin
        System.out.println("[MIGRATION-TTL] Initiating pull-based rejoin migration on node-2...");
        handler2.initiateRejoinMigration();

        // 4. Inspect the migrated item on node-2
        ValueEntry migratedEntry = store2.getEntry(key);
        assertNotNull(migratedEntry, "Rejoined node-2 must have pulled its key from stand-in replica node-1!");

        long migratedExpiresAt = migratedEntry.expiresAt();
        System.out.printf("[MIGRATION-TTL] Migrated absolute expiresAt timestamp: %d%n", migratedExpiresAt);

        // 5. Assert BIT-FOR-BIT equality with the original timestamp
        assertEquals(originalExpiresAt, migratedExpiresAt,
                "Migrated copy must have the exact same expiry timestamp bit-for-bit as originally established!");

        // 6. Verify it did NOT start a fresh 10-second window from rejoinTime!
        long remainingTtlMillis = migratedExpiresAt - rejoinTime;
        System.out.printf("[MIGRATION-TTL] Remaining TTL on migrated item: %d ms (approx 8.6s, NOT a fresh 10s window!)%n", remainingTtlMillis);

        assertTrue(remainingTtlMillis < 9000,
                "Remaining TTL must be less than 9 seconds because ~1.4s already elapsed since original SET!");
        assertTrue(remainingTtlMillis > 7500,
                "Remaining TTL should be around 8.6 seconds");

        System.out.println("[MIGRATION-TTL] Success! Phase 6 handoff strictly preserves absolute expiry timestamps across cluster rejoin events!");
    }
}
