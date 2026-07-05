package com.kairo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProactiveHealthRoutingTest {

    private HttpServer server1;
    private HttpServer server3;
    private int port1;
    private int port3;
    private HttpClient client;
    private HashRing ring;
    private CacheStore store1;
    private CacheStore store3;
    private FailureDetector fd1;

    @BeforeEach
    void setUp() throws IOException {
        ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        server3 = HttpServer.create(new InetSocketAddress(0), 0);
        port3 = server3.getAddress().getPort();
        store3 = new CacheStore();
        Map<String, PeerNode> peers3 = new HashMap<>();
        NodeConfig config3 = new NodeConfig("node-3", port3, peers3);
        server3.createContext("/cache/", new CacheHandler(store3, config3, ring, 2, null));
        server3.setExecutor(null);
        server3.start();

        server1 = HttpServer.create(new InetSocketAddress(0), 0);
        port1 = server1.getAddress().getPort();
        store1 = new CacheStore();

        Map<String, PeerNode> peers1 = new HashMap<>();
        // node-2 is at a port that is NOT listening (simulating dead node)
        peers1.put("node-2", new PeerNode("node-2", "localhost", 1));
        peers1.put("node-3", new PeerNode("node-3", "localhost", port3));
        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);

        fd1 = new FailureDetector("node-1", peers1, 100, 100, 1, 3, 0);
        server1.createContext("/cache/", new CacheHandler(store1, config1, ring, 2, fd1));
        server1.setExecutor(null);
        server1.start();

        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server1 != null) server1.stop(0);
        if (server3 != null) server3.stop(0);
    }

    @Test
    void testProactiveRoutingSkipsDeadPrimaryToLocalReplica() throws Exception {
        // Find a key where node-2 is primary owner and node-1 is replica
        String targetKey = null;
        for (int i = 0; i < 1000; i++) {
            String key = "test-key-" + i;
            String owner = ring.findOwner(key);
            List<String> replicas = ring.findReplicas(key, 2);
            if ("node-2".equals(owner) && !replicas.isEmpty() && "node-1".equals(replicas.get(0))) {
                targetKey = key;
                break;
            }
        }
        assertNotNull(targetKey, "Must find key owned by node-2 with replica node-1");

        // Pre-populate replica store on node-1
        store1.put(targetKey, "replica-value-on-node-1");

        // Mark node-2 explicitly DEAD in node-1's FailureDetector
        FailureDetector.PeerHealth health2 = fd1.getHealth("node-2");
        health2.recordFailure(1, 3, false);
        health2.recordFailure(1, 3, false);
        health2.recordFailure(1, 3, false);
        assertEquals(PeerStatus.DEAD, health2.getStatus());
        assertFalse(fd1.isAlive("node-2"));

        // Send GET request to node-1
        long startTime = System.currentTimeMillis();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        long elapsed = System.currentTimeMillis() - startTime;

        assertEquals(200, res.statusCode());
        assertEquals("replica-value-on-node-1", res.body());
        // Since node-2 was skipped proactively, the request should be instantaneous (< 500ms, no 1500ms timeout!)
        assertTrue(elapsed < 500, "Proactive routing must skip dead node immediately without waiting for HTTP timeout (took " + elapsed + "ms)");
    }

    @Test
    void testProactiveRoutingSkipsDeadPrimaryToRemoteReplica() throws Exception {
        // Find a key where node-2 is primary owner and node-3 is replica
        String targetKey = null;
        for (int i = 0; i < 1000; i++) {
            String key = "remote-key-" + i;
            String owner = ring.findOwner(key);
            List<String> replicas = ring.findReplicas(key, 2);
            if ("node-2".equals(owner) && !replicas.isEmpty() && "node-3".equals(replicas.get(0))) {
                targetKey = key;
                break;
            }
        }
        assertNotNull(targetKey, "Must find key owned by node-2 with replica node-3");

        // Pre-populate store on node-3
        store3.put(targetKey, "replica-value-on-node-3");

        // Mark node-2 explicitly DEAD in node-1's FailureDetector
        FailureDetector.PeerHealth health2 = fd1.getHealth("node-2");
        health2.recordFailure(1, 3, false);
        health2.recordFailure(1, 3, false);
        health2.recordFailure(1, 3, false);
        assertFalse(fd1.isAlive("node-2"));

        // Send GET request to node-1 — it should skip node-2 and forward directly to node-3
        long startTime = System.currentTimeMillis();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port1 + "/cache/" + targetKey))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        long elapsed = System.currentTimeMillis() - startTime;

        assertEquals(200, res.statusCode());
        assertEquals("replica-value-on-node-3", res.body());
        assertTrue(elapsed < 500, "Proactive routing must forward to alive replica node-3 without waiting for node-2 timeout (took " + elapsed + "ms)");
    }
}
