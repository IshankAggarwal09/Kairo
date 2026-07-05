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

import static org.junit.jupiter.api.Assertions.*;

class RingHandlerTest {

    private HttpServer server;
    private int port;
    private HttpClient client;
    private HashRing ring;

    @BeforeEach
    void setUp() throws IOException {
        ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        server.createContext("/ring", new RingHandler(ring));
        server.setExecutor(null);
        server.start();

        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testRingOwnerEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/ring/owner?key=test-key-1"))
                .GET()
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        String body = res.body();

        assertTrue(body.contains("Key: test-key-1"));
        assertTrue(body.contains("Primary Owner:"));
        assertTrue(body.contains("Replicas (RF=2):"));
        assertTrue(body.contains("Preference List:"));
    }

    @Test
    void testRingStatusEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/ring"))
                .GET()
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("Ring Status: 450 total tokens"));
        assertTrue(res.body().contains("node-1: 150 tokens"));
    }
}
