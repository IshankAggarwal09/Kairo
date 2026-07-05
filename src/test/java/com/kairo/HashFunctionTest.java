package com.kairo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HashFunctionTest {

    @Test
    void testHashIsDeterministic() {
        long hash1 = HashFunction.hash("node-1");
        long hash2 = HashFunction.hash("node-1");
        assertEquals(hash1, hash2, "Hashing same input must be deterministic");
    }

    @Test
    void testHashRange() {
        String[] samples = {"node-1", "node-2", "node-3", "my-test-key", "another-key", ""};
        for (String sample : samples) {
            long hash = HashFunction.hash(sample);
            assertTrue(hash >= 0L && hash <= 4294967295L,
                    "Hash must be in 32-bit unsigned range [0, 2^32-1]: " + hash);
        }
    }

    @Test
    void testDifferentInputsProduceDifferentHashes() {
        long hash1 = HashFunction.hash("node-1#vnode0");
        long hash2 = HashFunction.hash("node-1#vnode1");
        assertNotEquals(hash1, hash2);
    }
}
