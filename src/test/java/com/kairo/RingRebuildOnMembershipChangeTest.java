package com.kairo;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RingRebuildOnMembershipChangeTest {

    @Test
    void testRingRebuiltWhenPeerTransitionsToAlive() {
        // 1. Initialize ring with only node-1 and node-3 (node-2 is absent)
        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-3");

        assertEquals(300, ring.size(), "Ring should have 300 virtual nodes (150 * 2 nodes)");

        // Verify no key resolves to node-2
        for (int i = 0; i < 100; i++) {
            assertNotEquals("node-2", ring.findOwner("key-" + i), "No key should map to node-2 before it joins ring");
        }

        // 2. Create FailureDetector and CacheHandler (which wires the StatusChangeListener)
        Map<String, PeerNode> peers = new HashMap<>();
        peers.put("node-2", new PeerNode("node-2", "localhost", 8082));
        peers.put("node-3", new PeerNode("node-3", "localhost", 8083));
        NodeConfig config = new NodeConfig("node-1", 8081, peers);

        FailureDetector fd = new FailureDetector("node-1", peers, 1500, 500, 1, 3, 0);
        CacheStore store = new CacheStore();
        
        // Instantiating CacheHandler registers the StatusChangeListener on fd
        new CacheHandler(store, config, ring, 2, fd);

        // 3. Simulate FailureDetector detecting that node-2 is now ALIVE (e.g. recovery from DEAD/SUSPECTED)
        // We trigger the notification loop directly
        fd.notifyStatusChange("node-2", PeerStatus.DEAD, PeerStatus.ALIVE);

        // 4. Verify ring was rebuilt automatically by inserting node-2's virtual points
        assertEquals(450, ring.size(), "Ring should now have 450 virtual nodes (150 * 3 nodes)");

        boolean foundNode2Owner = false;
        for (int i = 0; i < 1000; i++) {
            if ("node-2".equals(ring.findOwner("test-key-" + i))) {
                foundNode2Owner = true;
                break;
            }
        }
        assertTrue(foundNode2Owner, "After ring rebuild, findOwner(key) must immediately start returning node-2 for previously displaced keys!");
    }
}
