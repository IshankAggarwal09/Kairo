package com.kairo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory key-value store backed by a {@link ConcurrentHashMap} with an LRU Eviction policy.
 *
 * <p>Memory Pressure Management (Phase 9):
 * Maintains a strict Least Recently Used (LRU) ordering using a doubly-linked list.
 * If the cache exceeds {@code maxKeysPerNode}, the oldest entries are evicted.
 *
 * <p>Expired entries are cleaned up in two ways:
 * <ol>
 *   <li><b>Lazy</b> — on every {@link #get}, stale entries are removed on access.</li>
 *   <li><b>Active</b> — a background sweep runs every {@code sweepIntervalSeconds},
 *       iterating the map and evicting anything past its expiry.</li>
 * </ol>
 */
public class CacheStore {

    private static final long DEFAULT_SWEEP_INTERVAL_SECONDS = 5;

    private static class LruNode {
        final String key;
        volatile ValueEntry value;
        LruNode prev;
        LruNode next;

        LruNode(String key, ValueEntry value) {
            this.key = key;
            this.value = value;
        }
    }

    private final int maxKeysPerNode;
    private final ConcurrentHashMap<String, LruNode> store = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sweeper;

    // LRU doubly-linked list pointers and lock
    private final ReentrantLock lruLock = new ReentrantLock();
    private final LruNode head = new LruNode(null, null);
    private final LruNode tail = new LruNode(null, null);

    /**
     * Creates a CacheStore with infinite capacity and default sweep interval.
     */
    public CacheStore() {
        this(DEFAULT_SWEEP_INTERVAL_SECONDS, -1);
    }

    /**
     * Creates a CacheStore with infinite capacity and custom sweep interval.
     */
    public CacheStore(long sweepIntervalSeconds) {
        this(sweepIntervalSeconds, -1);
    }

    /**
     * Creates a CacheStore with a custom sweep interval and max key limit.
     *
     * @param sweepIntervalSeconds how often the background sweep runs
     * @param maxKeysPerNode maximum number of keys before LRU eviction triggers (-1 for infinite)
     */
    public CacheStore(long sweepIntervalSeconds, int maxKeysPerNode) {
        this.maxKeysPerNode = maxKeysPerNode;
        head.next = tail;
        tail.prev = head;

        // Single daemon thread — lightweight periodic job, won't prevent JVM exit.
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kairo-expiry-sweeper");
            t.setDaemon(true);
            return t;
        });

        sweeper.scheduleAtFixedRate(
                this::evictExpired,
                sweepIntervalSeconds,
                sweepIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    // ── LRU List Operations (Must be called with lruLock held) ─────────

    private void removeNodeFromList(LruNode node) {
        if (node.prev != null && node.next != null) {
            node.prev.next = node.next;
            node.next.prev = node.prev;
            node.prev = null;
            node.next = null;
        }
    }

    private void addNodeToHead(LruNode node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private void moveToHead(LruNode node) {
        lruLock.lock();
        try {
            removeNodeFromList(node);
            addNodeToHead(node);
        } finally {
            lruLock.unlock();
        }
    }

    private java.util.List<String> evictIfNeeded() {
        java.util.List<String> evictedKeys = new java.util.ArrayList<>();
        if (maxKeysPerNode < 0) return evictedKeys;

        while (store.size() > maxKeysPerNode) {
            LruNode toRemove = null;
            lruLock.lock();
            try {
                if (tail.prev != head) {
                    toRemove = tail.prev;
                    removeNodeFromList(toRemove);
                }
            } finally {
                lruLock.unlock();
            }

            if (toRemove != null) {
                System.out.println("[EVICTION] Memory pressure threshold (" + maxKeysPerNode + ") exceeded. Evicting LRU key: " + toRemove.key);
                store.remove(toRemove.key);
                evictedKeys.add(toRemove.key);
            } else {
                break; // List is empty
            }
        }
        return evictedKeys;
    }

    // ── Core Operations ────────────────────────────────────────────────

    private java.util.List<String> putInternal(String key, ValueEntry value) {
        lruLock.lock();
        try {
            LruNode node = store.get(key);
            if (node == null) {
                node = new LruNode(key, value);
                store.put(key, node);
                addNodeToHead(node);
            } else {
                node.value = value;
                removeNodeFromList(node);
                addNodeToHead(node);
            }
        } finally {
            lruLock.unlock();
        }
        return evictIfNeeded();
    }

    public java.util.List<String> put(String key, String value) {
        return putInternal(key, new ValueEntry(value));
    }

    public java.util.List<String> put(String key, String value, long ttlMillis) {
        long expiresAt = ttlMillis > 0
                ? System.currentTimeMillis() + ttlMillis
                : ValueEntry.NO_EXPIRY;
        return putInternal(key, new ValueEntry(value, expiresAt));
    }

    public java.util.List<String> putAbsolute(String key, String value, long expiresAt) {
        return putInternal(key, new ValueEntry(value, expiresAt, System.currentTimeMillis()));
    }

    public java.util.List<String> putAbsolute(String key, String value, long expiresAt, long writeTimestamp) {
        return putInternal(key, new ValueEntry(value, expiresAt, writeTimestamp));
    }

    public ValueEntry getEntry(String key) {
        LruNode node = store.get(key);
        if (node == null) {
            return null;
        }
        if (node.value.isExpired()) {
            delete(key);
            return null;
        }
        moveToHead(node);
        return node.value;
    }

    public String get(String key) {
        ValueEntry entry = getEntry(key);
        return entry != null ? entry.value() : null;
    }

    public boolean delete(String key) {
        LruNode node = store.remove(key);
        if (node != null) {
            lruLock.lock();
            try {
                removeNodeFromList(node);
            } finally {
                lruLock.unlock();
            }
            return true;
        }
        return false;
    }

    public java.util.Map<String, ValueEntry> getSnapshot() {
        java.util.Map<String, ValueEntry> snapshot = new java.util.HashMap<>();
        long now = System.currentTimeMillis();
        for (java.util.Map.Entry<String, LruNode> e : store.entrySet()) {
            ValueEntry ve = e.getValue().value;
            if (now <= ve.expiresAt()) {
                snapshot.put(e.getKey(), ve);
            }
        }
        return snapshot;
    }

    public void shutdown() {
        sweeper.shutdownNow();
    }

    private void evictExpired() {
        long now = System.currentTimeMillis();
        for (java.util.Map.Entry<String, LruNode> e : store.entrySet()) {
            if (now > e.getValue().value.expiresAt()) {
                LruNode node = e.getValue();
                if (store.remove(e.getKey(), node)) {
                    lruLock.lock();
                    try {
                        removeNodeFromList(node);
                    } finally {
                        lruLock.unlock();
                    }
                }
            }
        }
    }
}
