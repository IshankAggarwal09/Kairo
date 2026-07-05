package com.kairo;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class NodeConfigTest {

    @Test
    void parsePeersExcludesSelf() {
        String peersStr = "node-1:8081,node-2:8082,node-3:8083";
        Map<String, PeerNode> peers = NodeConfig.parsePeers(peersStr, "node-1");

        assertEquals(2, peers.size());
        assertFalse(peers.containsKey("node-1"), "Self (node-1) should be excluded");
        assertTrue(peers.containsKey("node-2"));
        assertTrue(peers.containsKey("node-3"));

        assertEquals("http://node-2:8082", peers.get("node-2").baseUrl());
    }

    @Test
    void parsePeersWithExplicitHost() {
        String peersStr = "node-1:localhost:8081,node-2:localhost:8082";
        Map<String, PeerNode> peers = NodeConfig.parsePeers(peersStr, "node-3");

        assertEquals(2, peers.size());
        assertEquals("localhost", peers.get("node-1").host());
        assertEquals(8081, peers.get("node-1").port());
        assertEquals("http://localhost:8081", peers.get("node-1").baseUrl());
    }
}
