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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verification test for Phase 6, Step 9: Testing New Node Join (Not Just Rejoin).
 * Proves that when a genuinely new node (node-4) is added to an existing cluster,
 * the hash ring assigns it a slice of virtual tokens, ownership of existing keys shifts to it,
 * and pull-based migration correctly transfers physical data onto the new node.
 */
public class ClusterNewNodeJoinTest {

    private HttpServer server1;
    private HttpServer server2;
    private HttpServer server3;
    private HttpServer server4;

    private CacheStore store1;
    private CacheStore store2;
    private CacheStore store3;
    private CacheStore store4;

    private FailureDetector fd1;
    private FailureDetector fd2;
    private FailureDetector fd3;
    private FailureDetector fd4;

    private CacheHandler handler1;
    private CacheHandler handler2;
    private CacheHandler handler3;
    private CacheHandler handler4;

    private HashRing ring1;
    private HashRing ring2;
    private HashRing ring3;
    private HashRing ring4;

    private NodeConfig config1;
    private NodeConfig config2;
    private NodeConfig config3;
    private NodeConfig config4;

    private int port1;
    private int port2;
    private int port3;
    private int port4;

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

        Map<String, PeerNode> peers1 = new HashMap<>(Map.of(
                "node-2", new PeerNode("node-2", "localhost", port2),
                "node-3", new PeerNode("node-3", "localhost", port3)
        ));
        Map<String, PeerNode> peers2 = new HashMap<>(Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-3", new PeerNode("node-3", "localhost", port3)
        ));
        Map<String, PeerNode> peers3 = new HashMap<>(Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-2", new PeerNode("node-2", "localhost", port2)
        ));

        store1 = new CacheStore();
        store2 = new CacheStore();
        store3 = new CacheStore();

        config1 = new NodeConfig("node-1", port1, peers1);
        config2 = new NodeConfig("node-2", port2, peers2);
        config3 = new NodeConfig("node-3", port3, peers3);

        ring1 = HashRing.fromConfig(config1, 150);
        ring2 = HashRing.fromConfig(config2, 150);
        ring3 = HashRing.fromConfig(config3, 150);

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
        if (fd4 != null) fd4.stop();
        if (server1 != null) server1.stop(0);
        if (server2 != null) server2.stop(0);
        if (server3 != null) server3.stop(0);
        if (server4 != null) server4.stop(0);
    }

    @Test
    void testBrandNewNodeJoinAndHandoff() throws Exception {
        System.out.println("\n=== STEP 9: BRAND NEW NODE JOIN (GROWING THE CLUSTER) ===");

        // 1. SET 100 keys across the original 3-node cluster
        int totalKeys = 100;
        for (int i = 0; i < totalKeys; i++) {
            String key = "join-test-key-" + i;
            String val = "join-val-" + i;
            HttpRequest putReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + key))
                    .POST(HttpRequest.BodyPublishers.ofString(val))
                    .build();
            HttpResponse<String> putRes = client.send(putReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, putRes.statusCode());
        }
        System.out.println("Written " + totalKeys + " keys across 3-node cluster.");

        // Verify initial ring size is 450 virtual nodes (150 * 3)
        assertEquals(450, ring1.size(), "Initial ring must have 450 virtual nodes");

        // 2. Instantiate genuinely brand new node-4 (was not in cluster originally)
        server4 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port4 = server4.getAddress().getPort();
        store4 = new CacheStore();

        PeerNode peer4 = new PeerNode("node-4", "localhost", port4);
        Map<String, PeerNode> peers4 = Map.of(
                "node-1", new PeerNode("node-1", "localhost", port1),
                "node-2", new PeerNode("node-2", "localhost", port2),
                "node-3", new PeerNode("node-3", "localhost", port3)
        );
        config4 = new NodeConfig("node-4", port4, peers4);
        ring4 = HashRing.fromConfig(config4, 150);
        fd4 = new FailureDetector("node-4", peers4);
        handler4 = new CacheHandler(store4, config4, ring4, 2, fd4);
        setupServer(server4, store4, ring4, config4, fd4, handler4);
        server4.start();
        fd4.start();

        System.out.println("Started brand new node-4 on port " + port4 + " with clean memory store.");
        assertEquals(0, store4.getSnapshot().size(), "node-4 memory store must start empty");

        // 3. Dynamically add node-4 to existing nodes' configurations and hash rings
        System.out.println("Adding node-4 to cluster peer maps and recomputing hash rings...");
        config1.addPeer(peer4); fd1.addPeer(peer4); ring1.addNode("node-4");
        config2.addPeer(peer4); fd2.addPeer(peer4); ring2.addNode("node-4");
        config3.addPeer(peer4); fd3.addPeer(peer4); ring3.addNode("node-4");
        ring4.addNode("node-4"); // ensure ring4 knows all 4 nodes

        // Verify new ring size is 600 virtual nodes (150 * 4)
        assertEquals(600, ring1.size(), "Ring must now expand to 600 virtual nodes");

        // 4. Identify which keys have shifted ownership to new node-4
        List<String> shiftedKeys = new ArrayList<>();
        for (int i = 0; i < totalKeys; i++) {
            String key = "join-test-key-" + i;
            if ("node-4".equals(ring1.findOwner(key))) {
                shiftedKeys.add(key);
            }
        }
        assertTrue(shiftedKeys.size() > 0, "A percentage of keys must shift ownership to newly added node-4!");
        System.out.printf("Ring math shifted ownership of %d out of %d keys (~%d%%) to node-4.%n",
                shiftedKeys.size(), totalKeys, (shiftedKeys.size() * 100 / totalKeys));

        // 5. Trigger pull-based handoff on node-4 to pull its newly assigned key slice
        System.out.println("Initiating pull-based migration handoff on node-4...");
        handler4.initiateRejoinMigration();

        // 6. Critically verify: physical data absorption in store4 & seamless HTTP read routing
        for (String k : shiftedKeys) {
            String expectedVal = "join-val-" + k.substring("join-test-key-".length());
            
            // Assert key is physically stored in node-4's local memory store
            assertNotNull(store4.get(k), "Shifted key " + k + " must be physically pulled into store4!");
            assertEquals(expectedVal, store4.get(k));

            // Query via node-1: node-1 must check updated ring, see node-4 is owner, and forward over HTTP!
            HttpRequest getReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port1 + "/cache/" + k))
                    .GET()
                    .build();
            HttpResponse<String> getRes = client.send(getReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, getRes.statusCode());
            assertEquals(expectedVal, getRes.body(), "node-1 must transparently route read to new owner node-4");
        }
        System.out.println("Verified physical absorption of all " + shiftedKeys.size() + " shifted keys into node-4 & transparent forwarding.");
        System.out.println("=== STEP 9 VERIFICATION COMPLETE: NEW NODE JOIN SUCCESS ===\n");
    }
}
