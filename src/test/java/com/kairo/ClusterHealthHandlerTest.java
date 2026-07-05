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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ClusterHealthHandlerTest {

    private HttpServer server;
    private int port;
    private HttpClient client;
    private FailureDetector failureDetector;
    private NodeConfig config;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testGetClusterHealthEmptyPeers() throws Exception {
        config = new NodeConfig("node-1", 8080, Collections.emptyMap());
        failureDetector = new FailureDetector("node-1", Collections.emptyMap());
        server.createContext("/cluster/health", new ClusterHealthHandler(failureDetector, config));
        server.setExecutor(null);
        server.start();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cluster/health"))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("Cluster Health Report (Local Node: node-1)"));
        assertTrue(res.body().contains("No remote peers configured in cluster."));
    }

    @Test
    void testGetClusterHealthWithPeers() throws Exception {
        HttpServer deadServer = HttpServer.create(new InetSocketAddress(0), 0);
        int deadPort = deadServer.getAddress().getPort();
        deadServer.stop(0);

        Map<String, PeerNode> peers = new HashMap<>();
        peers.put("node-2", new PeerNode("node-2", "localhost", deadPort));
        config = new NodeConfig("node-1", 8080, peers);

        failureDetector = new FailureDetector("node-1", peers, 100, 100, 1, 3, 0);
        server.createContext("/cluster/health", new ClusterHealthHandler(failureDetector, config));
        server.setExecutor(null);
        server.start();

        // Trip 1 failure to transition node-2 to SUSPECTED
        failureDetector.pingAllPeers();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cluster/health"))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("node-2"));
        assertTrue(res.body().contains("SUSPECTED"));
        assertTrue(res.body().contains("Failures: 1"));
    }

    @Test
    void testMethodNotAllowed() throws Exception {
        config = new NodeConfig("node-1", 8080, Collections.emptyMap());
        failureDetector = new FailureDetector("node-1", Collections.emptyMap());
        server.createContext("/cluster/health", new ClusterHealthHandler(failureDetector, config));
        server.setExecutor(null);
        server.start();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cluster/health"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(405, res.statusCode());
    }

    @Test
    void testMigratingStatusReported() throws Exception {
        Map<String, PeerNode> peers = Map.of(
                "node-2", new PeerNode("node-2", "localhost", 8082)
        );
        config = new NodeConfig("node-1", 8080, peers);
        failureDetector = new FailureDetector("node-1", peers);
        failureDetector.setStatus("node-2", PeerStatus.MIGRATING);

        server.createContext("/cluster/health", new ClusterHealthHandler(failureDetector, config));
        server.setExecutor(null);
        server.start();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cluster/health"))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("node-2"));
        assertTrue(res.body().contains("MIGRATING"));
    }
}
