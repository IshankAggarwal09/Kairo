package com.kairo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically monitors cluster peer health via direct HTTP heartbeats (GET /ping).
 *
 * <p>Each peer is polled every interval (default 1500ms) with a fast timeout (default 500ms).
 * To prevent flapping from transient network hiccups or momentary GC pauses, a peer is only
 * marked offline after missing a consecutive failure threshold (default 3 missed pings).
 *
 * <p><b>Intermediate SUSPECTED State:</b> After a smaller threshold of missed pings (default 1),
 * the peer transitions to {@link PeerStatus#SUSPECTED} before reaching {@link PeerStatus#DEAD}.
 * This mirrors Phi Accrual failure detectors and provides an early warning window.
 *
 * <p><b>Accrual Recovery Detection:</b> Once a DEAD peer starts responding again, it first
 * transitions to SUSPECTED (1st ping) and then back to ALIVE after consecutive successes
 * (default 2), preventing flapping during recovery and signaling Phase 6 rebalancing.
 *
 * <p><b>Warmup Grace Period:</b> As documented in DESIGN.md Section 7, container orchestration
 * platforms (like Docker Compose) do not guarantee deterministic startup ordering. To prevent
 * false-positive dead marking during cluster boot, nodes ignore failure threshold tripping
 * during an initial warmup grace period (default 10000ms).
 */
public class FailureDetector {

    @FunctionalInterface
    public interface StatusChangeListener {
        void onStatusChange(String peerId, PeerStatus oldStatus, PeerStatus newStatus);
    }

    private final String localNodeId;
    private final Map<String, PeerHealth> peerHealthMap;
    private final HttpClient client;
    private final long intervalMs;
    private final int timeoutMs;
    private final int suspectedThreshold;
    private final int deadThreshold;
    private final int recoveryThreshold;
    private final long warmupGracePeriodMs;
    private final long startTime;
    private final List<StatusChangeListener> listeners = new CopyOnWriteArrayList<>();

    private ScheduledExecutorService executor;

    /**
     * Proactively mark a peer as DEAD (e.g. if we get a connection refused during normal operation).
     * This avoids waiting for the heartbeat timeout and helps the cluster converge instantly on hard failures.
     */
    public void markDead(String peerId) {
        PeerHealth health = peerHealthMap.get(peerId);
        if (health != null && !health.isDead()) {
            System.out.println("Node " + localNodeId + " -> PROACTIVE FAILURE DETECTION: Marking " + peerId + " as DEAD");
            PeerStatus oldStatus = health.getStatus();
            health.setStatus(PeerStatus.DEAD);
            System.out.println("Node " + localNodeId + " -> PROACTIVE FAILURE DETECTION: Marked " + peerId + " as DEAD");
        }
    }

    public FailureDetector(String localNodeId, Map<String, PeerNode> peers) {
        this(localNodeId, peers, 1500, 500, 1, 3, 2, 10000);
    }

    public FailureDetector(String localNodeId, Map<String, PeerNode> peers,
                           long intervalMs, int timeoutMs, int deadThreshold, long warmupGracePeriodMs) {
        this(localNodeId, peers, intervalMs, timeoutMs, Math.max(1, deadThreshold / 2), deadThreshold, 1, warmupGracePeriodMs);
    }

    public FailureDetector(String localNodeId, Map<String, PeerNode> peers,
                           long intervalMs, int timeoutMs, int suspectedThreshold, int deadThreshold, long warmupGracePeriodMs) {
        this(localNodeId, peers, intervalMs, timeoutMs, suspectedThreshold, deadThreshold, 1, warmupGracePeriodMs);
    }

    public FailureDetector(String localNodeId, Map<String, PeerNode> peers,
                           long intervalMs, int timeoutMs, int suspectedThreshold, int deadThreshold, int recoveryThreshold, long warmupGracePeriodMs) {
        this.localNodeId = localNodeId;
        this.intervalMs = intervalMs;
        this.timeoutMs = timeoutMs;
        this.suspectedThreshold = suspectedThreshold;
        this.deadThreshold = deadThreshold;
        this.recoveryThreshold = recoveryThreshold;
        this.warmupGracePeriodMs = warmupGracePeriodMs;
        this.startTime = System.currentTimeMillis();

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();

        this.peerHealthMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, PeerNode> entry : peers.entrySet()) {
            peerHealthMap.put(entry.getKey(), new PeerHealth(entry.getKey(), entry.getValue().baseUrl(), this));
        }
    }

    public String getLocalNodeId() {
        return localNodeId;
    }

    public void addListener(StatusChangeListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public void removeListener(StatusChangeListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }

    public void addPeer(PeerNode peer) {
        if (peer != null && !peer.id().equals(localNodeId)) {
            peerHealthMap.putIfAbsent(peer.id(), new PeerHealth(peer.id(), peer.baseUrl(), this));
        }
    }

    public void removePeer(String peerId) {
        if (peerId != null) {
            peerHealthMap.remove(peerId);
        }
    }

    void notifyStatusChange(String peerId, PeerStatus oldStatus, PeerStatus newStatus) {
        for (StatusChangeListener listener : listeners) {
            try {
                listener.onStatusChange(peerId, oldStatus, newStatus);
            } catch (Exception e) {
                System.err.println("Error in StatusChangeListener: " + e.getMessage());
            }
        }
    }

    public int getRecoveryThreshold() {
        return recoveryThreshold;
    }

    public synchronized void start() {
        if (executor != null && !executor.isShutdown()) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FailureDetector-" + localNodeId);
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(this::pingAllPeers, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        System.out.println("Node " + localNodeId + " -> FailureDetector started (interval="
                + intervalMs + "ms, timeout=" + timeoutMs + "ms, suspectedThreshold="
                + suspectedThreshold + ", deadThreshold=" + deadThreshold + ", recoveryThreshold=" + recoveryThreshold + ")");
    }

    public synchronized void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    void pingAllPeers() {
        boolean inWarmup = (System.currentTimeMillis() - startTime) < warmupGracePeriodMs;
        for (PeerHealth health : peerHealthMap.values()) {
            pingPeer(health, inWarmup);
        }
    }

    void pingPeer(PeerHealth health, boolean inWarmup) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(health.getBaseUrl() + "/ping"))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200 && "pong".equals(response.body())) {
                health.recordSuccess();
            } else {
                health.recordFailure(suspectedThreshold, deadThreshold, inWarmup);
            }
        } catch (Exception e) {
            health.recordFailure(suspectedThreshold, deadThreshold, inWarmup);
        }
    }

    /**
     * Checks if a peer is currently considered alive (i.e. not DEAD).
     * Returns true if the peer is unknown (or self) or currently ALIVE/SUSPECTED.
     */
    public boolean isAlive(String peerId) {
        PeerHealth health = peerHealthMap.get(peerId);
        return health == null || health.isAlive();
    }

    public PeerStatus getStatus(String peerId) {
        PeerHealth health = peerHealthMap.get(peerId);
        return health == null ? PeerStatus.ALIVE : health.getStatus();
    }

    public void setStatus(String peerId, PeerStatus newStatus) {
        PeerHealth health = peerHealthMap.get(peerId);
        if (health != null) {
            health.setStatus(newStatus);
        }
    }

    public void markAlive(String peerId) {
        PeerHealth health = peerHealthMap.get(peerId);
        if (health != null) {
            health.setStatus(PeerStatus.ALIVE);
            health.recordSuccess();
        }
    }

    public PeerHealth getHealth(String peerId) {
        return peerHealthMap.get(peerId);
    }

    public Map<String, PeerHealth> getAllHealth() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(peerHealthMap));
    }

    public static class PeerHealth {
        private final String peerId;
        private final String baseUrl;
        private final FailureDetector detector;
        private volatile PeerStatus status = PeerStatus.ALIVE;
        private volatile long lastSeen;
        private volatile int consecutiveFailures = 0;
        private volatile int consecutiveSuccesses = 0;

        public PeerHealth(String peerId, String baseUrl) {
            this(peerId, baseUrl, null);
        }

        public PeerHealth(String peerId, String baseUrl, FailureDetector detector) {
            this.peerId = peerId;
            this.baseUrl = baseUrl;
            this.detector = detector;
            this.lastSeen = System.currentTimeMillis();
        }

        public String getPeerId() { return peerId; }
        public String getBaseUrl() { return baseUrl; }
        public PeerStatus getStatus() { return status; }
        public boolean isAlive() { return status != PeerStatus.DEAD; }
        public boolean isDead() { return status == PeerStatus.DEAD; }
        public boolean isSuspected() { return status == PeerStatus.SUSPECTED; }
        public long getLastSeen() { return lastSeen; }
        public int getConsecutiveFailures() { return consecutiveFailures; }
        public int getConsecutiveSuccesses() { return consecutiveSuccesses; }

        public synchronized void setStatus(PeerStatus newStatus) {
            if (this.status != newStatus) {
                PeerStatus oldStatus = this.status;
                this.status = newStatus;
                logTransition(oldStatus, newStatus, "cluster rebalancing state change [Phase 6 trigger point]");
                if (detector != null) detector.notifyStatusChange(peerId, oldStatus, newStatus);
            }
        }

        private void logTransition(PeerStatus oldStatus, PeerStatus newStatus, String reason) {
            String nodeId = detector != null ? detector.getLocalNodeId() : "local-node";
            String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .format(LocalDateTime.now());
            System.out.printf("[%s] [Node %s] [CLUSTER-HEALTH] Peer %s status changed: %s -> %s (%s)%n",
                    timestamp, nodeId, peerId, oldStatus, newStatus, reason);
        }

        public synchronized void recordSuccess(int recoveryThresh) {
            this.lastSeen = System.currentTimeMillis();
            this.consecutiveFailures = 0;
            this.consecutiveSuccesses++;

            if (this.status != PeerStatus.ALIVE) {
                if (this.status == PeerStatus.DEAD && recoveryThresh > 1 && this.consecutiveSuccesses < recoveryThresh) {
                    PeerStatus oldStatus = this.status;
                    this.status = PeerStatus.SUSPECTED;
                    logTransition(oldStatus, this.status, "responding to heartbeats again after 1 successful ping");
                    if (detector != null) detector.notifyStatusChange(peerId, oldStatus, this.status);
                } else if (this.consecutiveSuccesses >= recoveryThresh || recoveryThresh <= 1) {
                    PeerStatus oldStatus = this.status;
                    this.status = PeerStatus.ALIVE;
                    logTransition(oldStatus, this.status, "recovery confirmed after " + consecutiveSuccesses + " consecutive success(es) [Phase 6 trigger point]");
                    if (detector != null) detector.notifyStatusChange(peerId, oldStatus, this.status);
                }
            }
        }

        public synchronized void recordSuccess() {
            recordSuccess(detector != null ? detector.getRecoveryThreshold() : 1);
        }

        public synchronized PeerStatus recordFailure(int suspectedThresh, int deadThresh, boolean inWarmupGracePeriod) {
            this.consecutiveFailures++;
            this.consecutiveSuccesses = 0;
            if (!inWarmupGracePeriod) {
                if (this.consecutiveFailures >= deadThresh && this.status != PeerStatus.DEAD) {
                    PeerStatus oldStatus = this.status;
                    this.status = PeerStatus.DEAD;
                    logTransition(oldStatus, this.status, consecutiveFailures + " consecutive missed heartbeats -> removed from active routing");
                    if (detector != null) detector.notifyStatusChange(peerId, oldStatus, this.status);
                } else if (this.consecutiveFailures >= suspectedThresh && this.status == PeerStatus.ALIVE) {
                    PeerStatus oldStatus = this.status;
                    this.status = PeerStatus.SUSPECTED;
                    logTransition(oldStatus, this.status, consecutiveFailures + " missed heartbeat(s) -> early warning");
                    if (detector != null) detector.notifyStatusChange(peerId, oldStatus, this.status);
                }
            }
            return this.status;
        }
    }
}
