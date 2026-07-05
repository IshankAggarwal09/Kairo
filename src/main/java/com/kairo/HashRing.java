package com.kairo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Thread-safe consistent hashing ring backed by a {@link ConcurrentSkipListMap}.
 *
 * <p>Maps ring positions (unsigned 32-bit integer tokens from 0 to 2^32 - 1)
 * to physical node IDs (e.g. "node-1").
 *
 * <p><b>Why {@link ConcurrentSkipListMap}:</b>
 * A sorted map is essential for consistent hashing because locating the responsible
 * node requires a "clockwise walk" to find the first token greater than or equal to
 * the key's hash. {@code ConcurrentSkipListMap} provides thread-safe, lock-free
 * {@link #ceilingEntry} and {@link #firstEntry} operations natively, allowing concurrent
 * HTTP reads and cluster topology modifications without blocking or ReadWriteLocks.
 */
public class HashRing {

    private final ConcurrentSkipListMap<Long, String> ring = new ConcurrentSkipListMap<>();
    private final int vnodes;

    /**
     * Creates a hash ring with the specified number of virtual nodes per physical node.
     *
     * @param vnodes Number of virtual nodes per physical node
     */
    public HashRing(int vnodes) {
        if (vnodes <= 0) {
            throw new IllegalArgumentException("vnodes must be greater than 0");
        }
        this.vnodes = vnodes;
    }

    /**
     * Creates a hash ring with 1 token per physical node (no replication / virtual nodes).
     */
    public HashRing() {
        this(1);
    }

    /**
     * Builds and populates a HashRing from a NodeConfig, including self and all peers.
     */
    public static HashRing fromConfig(NodeConfig config, int vnodes) {
        HashRing ring = new HashRing(vnodes);
        ring.addNode(config.nodeId()); // Include self
        for (String peerId : config.peers().keySet()) {
            ring.addNode(peerId); // Include remote peers
        }
        return ring;
    }

    /**
     * Adds a physical node to the ring, placing its tokens across the ring space.
     *
     * @param nodeId Physical node ID (e.g. "node-1")
     */
    public void addNode(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) return;
        for (int i = 0; i < vnodes; i++) {
            String vnodeName = nodeId + "#" + i;
            long token = HashFunction.hash(vnodeName);
            ring.put(token, nodeId);
        }
    }

    /**
     * Removes a physical node and all its virtual tokens from the ring.
     *
     * @param nodeId Physical node ID to remove
     */
    public void removeNode(String nodeId) {
        if (nodeId == null) return;
        ring.entrySet().removeIf(entry -> entry.getValue().equals(nodeId));
    }

    /**
     * Finds the physical node responsible for a given cache key by walking clockwise on the ring.
     *
     * <ol>
     *   <li>Hashes the key using {@link HashFunction#hash} to find its position on the ring.</li>
     *   <li>Calls {@link ConcurrentSkipListMap#ceilingKey} to find the first virtual token at or after the hash.</li>
     *   <li>If nothing is found (hashed past the highest position), wraps around to {@link ConcurrentSkipListMap#firstKey}.</li>
     *   <li>Looks up which physical node that virtual position belongs to.</li>
     * </ol>
     *
     * @param key Cache key
     * @return Responsible physical node ID, or null if the ring is empty
     */
    public String findOwner(String key) {
        if (ring.isEmpty() || key == null) {
            return null;
        }
        long hash = HashFunction.hash(key);
        Long token = ring.ceilingKey(hash);
        if (token == null) {
            // Wraparound: clock wraps past highest token back to 0 (firstKey)
            token = ring.firstKey();
        }
        return ring.get(token);
    }

    public String locate(String key) {
        return findOwner(key);
    }

    /**
     * Finds the replica physical nodes for a given cache key by walking clockwise past the primary owner.
     *
     * <p>Skips any virtual nodes that map back to the primary owner or to any physical node
     * already selected in the replica set, preventing self-replication or duplicate replication.
     *
     * @param key   Cache key
     * @param count Total replication factor (e.g. 2 for 1 primary owner + 1 replica node)
     * @return List of up to (count - 1) distinct replica physical node IDs
     */
    public List<String> findReplicas(String key, int count) {
        if (ring.isEmpty() || key == null || count <= 1) {
            return Collections.emptyList();
        }
        long hash = HashFunction.hash(key);
        Long token = ring.ceilingKey(hash);
        if (token == null) {
            token = ring.firstKey();
        }
        String owner = ring.get(token);

        List<String> replicas = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        seen.add(owner);

        int steps = 0;
        int maxSteps = ring.size();
        Long currentToken = token;

        while (replicas.size() < (count - 1) && steps < maxSteps) {
            currentToken = ring.higherKey(currentToken);
            if (currentToken == null) {
                currentToken = ring.firstKey();
            }
            String physicalNode = ring.get(currentToken);
            if (physicalNode != null && seen.add(physicalNode)) {
                replicas.add(physicalNode);
            }
            steps++;
        }

        return replicas;
    }

    /**
     * Returns the total number of virtual tokens currently placed on the ring.
     */
    public int size() {
        return ring.size();
    }

    /**
     * Returns true if there are no physical nodes registered on the ring.
     */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    /**
     * Returns an unmodifiable snapshot of the underlying token map for diagnostics.
     */
    public Map<Long, String> getRingSnapshot() {
        return Map.copyOf(ring);
    }
}
