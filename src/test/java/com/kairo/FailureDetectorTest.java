package com.kairo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FailureDetectorTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        server.createContext("/ping", exchange -> {
            byte[] response = "pong".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
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
    void testSuccessfulHeartbeat() {
        Map<String, PeerNode> peers = new HashMap<>();
        peers.put("peer-1", new PeerNode("peer-1", "localhost", port));

        FailureDetector fd = new FailureDetector("node-1", peers, 100, 100, 3, 0);
        assertTrue(fd.isAlive("peer-1"), "Peer must initially be alive");

        fd.pingAllPeers();
        assertTrue(fd.isAlive("peer-1"), "Peer must remain alive after successful ping");
        assertEquals(0, fd.getHealth("peer-1").getConsecutiveFailures());
        assertTrue(fd.getHealth("peer-1").getLastSeen() > 0);
    }

    @Test
    void testConsecutiveFailureThresholdAndRevival() throws IOException {
        java.util.concurrent.atomic.AtomicBoolean serverHealthy = new java.util.concurrent.atomic.AtomicBoolean(false);

        HttpServer testServer = HttpServer.create(new InetSocketAddress(0), 0);
        int testPort = testServer.getAddress().getPort();
        testServer.createContext("/ping", exchange -> {
            if (serverHealthy.get()) {
                byte[] response = "pong".getBytes();
                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            } else {
                exchange.sendResponseHeaders(500, -1);
                exchange.close();
            }
        });
        testServer.setExecutor(null);
        testServer.start();

        try {
            Map<String, PeerNode> peers = new HashMap<>();
            peers.put("flaky-peer", new PeerNode("flaky-peer", "localhost", testPort));

            // warmupGracePeriodMs = 0 so failure threshold trips immediately (suspected=1, dead=3)
            FailureDetector fd = new FailureDetector("node-1", peers, 100, 100, 1, 3, 0);

            // 1st failed ping (returns 500) — still alive, but SUSPECTED!
            fd.pingAllPeers();
            assertTrue(fd.isAlive("flaky-peer"), "Must not mark dead after 1 missed ping");
            assertEquals(PeerStatus.SUSPECTED, fd.getStatus("flaky-peer"));
            assertEquals(1, fd.getHealth("flaky-peer").getConsecutiveFailures());

            // 2nd failed ping — still alive and SUSPECTED
            fd.pingAllPeers();
            assertTrue(fd.isAlive("flaky-peer"), "Must not mark dead after 2 missed pings");
            assertEquals(PeerStatus.SUSPECTED, fd.getStatus("flaky-peer"));
            assertEquals(2, fd.getHealth("flaky-peer").getConsecutiveFailures());

            // 3rd failed ping — threshold tripped, must be marked DEAD!
            fd.pingAllPeers();
            assertFalse(fd.isAlive("flaky-peer"), "Must mark dead after 3 consecutive missed pings");
            assertEquals(PeerStatus.DEAD, fd.getStatus("flaky-peer"));
            assertEquals(3, fd.getHealth("flaky-peer").getConsecutiveFailures());

            // Now revive the server by setting flag to true
            serverHealthy.set(true);

            // Next ping succeeds — must revive the node!
            fd.pingAllPeers();
            assertTrue(fd.isAlive("flaky-peer"), "Must revive dead node upon receiving successful pong");
            assertEquals(PeerStatus.ALIVE, fd.getStatus("flaky-peer"));
            assertEquals(0, fd.getHealth("flaky-peer").getConsecutiveFailures());
        } finally {
            testServer.stop(0);
        }
    }

    @Test
    void testWarmupGracePeriod() throws IOException {
        HttpServer deadServer = HttpServer.create(new InetSocketAddress(0), 0);
        int deadPort = deadServer.getAddress().getPort();
        deadServer.stop(0);

        Map<String, PeerNode> peers = new HashMap<>();
        peers.put("booting-peer", new PeerNode("booting-peer", "localhost", deadPort));

        // warmupGracePeriodMs = 60000 (1 minute)
        FailureDetector fd = new FailureDetector("node-1", peers, 100, 100, 3, 60000);

        // Even after 5 missed pings, node should NOT be marked dead because we are inside warmup grace period!
        for (int i = 0; i < 5; i++) {
            fd.pingAllPeers();
        }
        assertEquals(5, fd.getHealth("booting-peer").getConsecutiveFailures());
        assertTrue(fd.isAlive("booting-peer"), "Must not mark node dead during warmup grace period");
    }

    @Test
    void testAccrualRecoveryAndStatusListener() throws IOException {
        java.util.concurrent.atomic.AtomicBoolean serverHealthy = new java.util.concurrent.atomic.AtomicBoolean(false);

        HttpServer testServer = HttpServer.create(new InetSocketAddress(0), 0);
        int testPort = testServer.getAddress().getPort();
        testServer.createContext("/ping", exchange -> {
            if (serverHealthy.get()) {
                byte[] response = "pong".getBytes();
                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            } else {
                exchange.sendResponseHeaders(500, -1);
                exchange.close();
            }
        });
        testServer.setExecutor(null);
        testServer.start();

        try {
            Map<String, PeerNode> peers = new HashMap<>();
            peers.put("recovering-peer", new PeerNode("recovering-peer", "localhost", testPort));

            // recoveryThreshold = 2, warmupGracePeriodMs = 0
            FailureDetector fd = new FailureDetector("node-1", peers, 100, 100, 1, 3, 2, 0);

            java.util.List<String> recordedTransitions = new java.util.concurrent.CopyOnWriteArrayList<>();
            fd.addListener((peerId, oldStatus, newStatus) -> {
                recordedTransitions.add(oldStatus + "->" + newStatus);
            });

            // 1st ping fails (SUSPECTED)
            fd.pingAllPeers();
            // 2nd ping fails
            fd.pingAllPeers();
            // 3rd ping fails (DEAD)
            fd.pingAllPeers();
            assertEquals(PeerStatus.DEAD, fd.getStatus("recovering-peer"));
            assertTrue(recordedTransitions.contains("ALIVE->SUSPECTED"));
            assertTrue(recordedTransitions.contains("SUSPECTED->DEAD"));

            recordedTransitions.clear();

            // Revive server
            serverHealthy.set(true);

            // 1st successful ping -> must transition DEAD -> SUSPECTED (recovery threshold = 2)
            fd.pingAllPeers();
            assertEquals(PeerStatus.SUSPECTED, fd.getStatus("recovering-peer"));
            assertEquals(1, fd.getHealth("recovering-peer").getConsecutiveSuccesses());
            assertEquals(List.of("DEAD->SUSPECTED"), recordedTransitions);

            // 2nd successful ping -> must transition SUSPECTED -> ALIVE!
            fd.pingAllPeers();
            assertEquals(PeerStatus.ALIVE, fd.getStatus("recovering-peer"));
            assertEquals(0, fd.getHealth("recovering-peer").getConsecutiveFailures());
            assertEquals(2, fd.getHealth("recovering-peer").getConsecutiveSuccesses());
            assertEquals(List.of("DEAD->SUSPECTED", "SUSPECTED->ALIVE"), recordedTransitions);
        } finally {
            testServer.stop(0);
        }
    }
}
