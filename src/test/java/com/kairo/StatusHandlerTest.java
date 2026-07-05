package com.kairo;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class StatusHandlerTest {

    @Test
    void testStatusFormat() {
        Map<String, PeerNode> peers = NodeConfig.parsePeers("node-2:8082,node-3:8083", "node-1");
        // We can verify parsePeers produces expected URLs
        assertEquals("http://node-2:8082", peers.get("node-2").baseUrl());
        assertEquals("http://node-3:8083", peers.get("node-3").baseUrl());
    }
}
