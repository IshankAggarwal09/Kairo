package com.kairo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class InternalHandoffHandlerTest {

    private HttpServer server;
    private CacheStore store;
    private HashRing ring;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        store = new CacheStore();
        ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/internal/handoff", new InternalHandoffHandler(store, ring, new NodeConfig("node-1", port, Collections.emptyMap())));
        server.setExecutor(null);
        server.start();

        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
        store.shutdown();
    }

    @Test
    void testGetHandoffReturnsOnlyKeysForTargetOwner() throws Exception {
        // Find keys belonging to node-1 vs node-2
        String keyNode1 = null;
        String keyNode2 = null;
        for (int i = 0; i < 100; i++) {
            String k = "test-key-" + i;
            String owner = ring.findOwner(k);
            if ("node-1".equals(owner) && keyNode1 == null) keyNode1 = k;
            if ("node-2".equals(owner) && keyNode2 == null) keyNode2 = k;
            if (keyNode1 != null && keyNode2 != null) break;
        }

        assertNotNull(keyNode1);
        assertNotNull(keyNode2);

        // Put both keys in local store
        long future = System.currentTimeMillis() + 60_000;
        store.putAbsolute(keyNode1, "val-1", future);
        store.putAbsolute(keyNode2, "val-2", future);

        // Request handoff keys for node-2
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/internal/handoff?owner=node-2"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        String body = response.body();

        assertTrue(body.contains(URLEncoder.encode(keyNode2, StandardCharsets.UTF_8)), "Response must contain key belonging to node-2");
        assertFalse(body.contains(URLEncoder.encode(keyNode1, StandardCharsets.UTF_8)), "Response must NOT contain key belonging to node-1");
    }

    @Test
    void testPostHandoffStoresKeysLocally() throws Exception {
        long future = System.currentTimeMillis() + 60_000;
        String payload = URLEncoder.encode("migrated-key-1", StandardCharsets.UTF_8) + "\t" +
                         URLEncoder.encode("migrated-value-1", StandardCharsets.UTF_8) + "\t" + future + "\n" +
                         URLEncoder.encode("migrated-key-2", StandardCharsets.UTF_8) + "\t" +
                         URLEncoder.encode("migrated-value-2", StandardCharsets.UTF_8) + "\t" + future + "\n";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/internal/handoff"))
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("migrated-value-1", store.get("migrated-key-1"));
        assertEquals("migrated-value-2", store.get("migrated-key-2"));
    }

    @Test
    void testDeleteHandoffConfirmsAndRemovesKeys() throws Exception {
        long future = System.currentTimeMillis() + 60_000;
        store.putAbsolute("del-key-1", "val-1", future);
        store.putAbsolute("del-key-2", "val-2", future);
        store.putAbsolute("keep-key-3", "val-3", future);

        String payload = URLEncoder.encode("del-key-1", StandardCharsets.UTF_8) + "\n" +
                         URLEncoder.encode("del-key-2", StandardCharsets.UTF_8) + "\n";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/internal/handoff"))
                .DELETE()
                .method("DELETE", HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("Handoff confirmed: deleted 2 keys\n", response.body());

        assertNull(store.get("del-key-1"));
        assertNull(store.get("del-key-2"));
        assertEquals("val-3", store.get("keep-key-3"));
    }
}
