package com.kairo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LoadGeneratorTest {

    private HttpServer server1;
    private HttpServer server2;
    private int port1;
    private int port2;

    @BeforeEach
    public void setUp() throws Exception {
        server1 = HttpServer.create(new InetSocketAddress(0), 0);
        port1 = server1.getAddress().getPort();

        server2 = HttpServer.create(new InetSocketAddress(0), 0);
        port2 = server2.getAddress().getPort();

        Map<String, PeerNode> peers1 = new HashMap<>();
        peers1.put("node-2", new PeerNode("node-2", "localhost", port2));

        Map<String, PeerNode> peers2 = new HashMap<>();
        peers2.put("node-1", new PeerNode("node-1", "localhost", port1));

        NodeConfig config1 = new NodeConfig("node-1", port1, peers1);
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
        Thread.sleep(200);
    }

    @AfterEach
    public void tearDown() {
        if (server1 != null) server1.stop(0);
        if (server2 != null) server2.stop(0);
    }

    @Test
    public void testLoadGeneratorExecution() {
        System.out.println("=== TESTING LOAD GENERATOR AGAINST 2-NODE CLUSTER ===");
        List<Integer> ports = Arrays.asList(port1, port2);
        // Run 20 requests with 5ms sleep between requests against key pool of 10
        LoadGenerator generator = new LoadGenerator(ports, 10, 5, 20);
        generator.runLoop();
        System.out.println("=== LOAD GENERATOR TEST COMPLETED SUCCESSFULLY ===");
    }
}
