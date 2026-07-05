package com.kairo;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class HashRingTest {

    @Test
    void testEmptyRing() {
        HashRing ring = new HashRing();
        assertTrue(ring.isEmpty());
        assertNull(ring.findOwner("my-key"));
    }

    @Test
    void testSingleNodeFindOwner() {
        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        assertEquals("node-1", ring.findOwner("any-key-1"));
        assertEquals("node-1", ring.findOwner("any-key-2"));
    }

    @Test
    void testMultiNodeFindOwner() {
        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        assertEquals(450, ring.size());

        // Test determinism
        String owner1 = ring.findOwner("user:1001");
        String owner2 = ring.findOwner("user:1001");
        assertEquals(owner1, owner2, "findOwner must be deterministic for same key");
        assertNotNull(owner1);
        assertTrue(owner1.startsWith("node-"));
    }

    @Test
    void testRemoveNode() {
        HashRing ring = new HashRing(10);
        ring.addNode("node-1");
        ring.addNode("node-2");
        assertEquals(20, ring.size());

        ring.removeNode("node-1");
        assertEquals(10, ring.size());

        for (int i = 0; i < 50; i++) {
            assertEquals("node-2", ring.findOwner("key-" + i),
                    "Only node-2 should remain responsible for all keys");
        }
    }

    @Test
    void testConsistentHashingRemapFraction() {
        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        int totalKeys = 10_000;
        Map<String, String> initialOwnership = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();

        for (int i = 0; i < totalKeys; i++) {
            String key = "key-" + i;
            String owner = ring.findOwner(key);
            initialOwnership.put(key, owner);
            counts.put(owner, counts.getOrDefault(owner, 0) + 1);
        }

        // Verify initial distribution is roughly balanced across all 3 nodes (between 25% and 40% each)
        for (String node : new String[]{"node-1", "node-2", "node-3"}) {
            int count = counts.getOrDefault(node, 0);
            assertTrue(count > 2500 && count < 4000,
                    "Node " + node + " should own roughly 1/3 of keys, owned: " + count);
        }

        // Remove node-1
        ring.removeNode("node-1");

        int remappedTotal = 0;
        int node2And3Retained = 0;
        int node2And3InitialTotal = counts.get("node-2") + counts.get("node-3");

        for (int i = 0; i < totalKeys; i++) {
            String key = "key-" + i;
            String initialOwner = initialOwnership.get(key);
            String newOwner = ring.findOwner(key);

            if (!initialOwner.equals(newOwner)) {
                remappedTotal++;
                // 1. Prove that ONLY keys previously owned by node-1 were remapped!
                assertEquals("node-1", initialOwner,
                        "Only keys owned by the removed node should ever remap!");
            } else {
                node2And3Retained++;
            }
        }

        // 2. Proving 100% retention: every key originally on node-2 or node-3 stayed on node-2 or node-3!
        assertEquals(node2And3InitialTotal, node2And3Retained,
                "Keys on surviving nodes must not move when a node leaves");

        // 3. Proving minimal disruption: exact number of remapped keys equals node-1's original share (~33%)
        assertEquals(counts.get("node-1").intValue(), remappedTotal);
        assertTrue(remappedTotal > 2500 && remappedTotal < 4000,
                "Only around 1/3 of total keys should remap (was: " + remappedTotal + " out of " + totalKeys + ")");
    }

    @Test
    void testFindReplicas() {
        HashRing ring = new HashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        for (int i = 0; i < 50; i++) {
            String key = "test-key-" + i;
            String owner = ring.findOwner(key);

            // Test RF = 2 -> 1 replica
            java.util.List<String> replicasRf2 = ring.findReplicas(key, 2);
            assertEquals(1, replicasRf2.size(), "RF=2 must return exactly 1 replica");
            assertNotEquals(owner, replicasRf2.get(0), "Replica must not be the same as primary owner");
            assertTrue(replicasRf2.get(0).startsWith("node-"));

            // Test RF = 3 -> 2 replicas
            java.util.List<String> replicasRf3 = ring.findReplicas(key, 3);
            assertEquals(2, replicasRf3.size(), "RF=3 must return exactly 2 replicas");
            assertNotEquals(owner, replicasRf3.get(0), "Replica 1 must not be primary owner");
            assertNotEquals(owner, replicasRf3.get(1), "Replica 2 must not be primary owner");
            assertNotEquals(replicasRf3.get(0), replicasRf3.get(1), "Replicas must be distinct physical nodes");
        }
    }

    @Test
    void testFindReplicasEdgeCases() {
        HashRing ring = new HashRing(150);
        ring.addNode("node-1");

        // With only 1 node in cluster, cannot replicate to self -> should return empty list
        assertTrue(ring.findReplicas("any-key", 2).isEmpty(), "Should return empty list when only 1 node exists");
        assertTrue(ring.findReplicas("any-key", 1).isEmpty(), "Should return empty list when RF=1");
    }
}
