package com.kairo;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Step 1 & 2: Standalone Load-Generating Client Test Harness with Retry Policy.
 *
 * <p>Simulates real-world client traffic against a Kairo cluster:
 * <ul>
 *   <li>Picks random keys from a fixed pool (default: 50 keys)</li>
 *   <li>Randomly chooses between SET (write) and GET (read) operations</li>
 *   <li>Sends requests to randomly chosen cluster ports (client is topology-unaware)</li>
 *   <li>Logs timestamp, operation, key, target node, outcome, and latency for raw audit evidence</li>
 *   <li>Prints rolling summary statistics every 50 requests and on shutdown</li>
 * </ul>
 *
 * <p><strong>Retry Policy (Core Requirement 07 compliance):</strong>
 * The spec allows "zero failed client requests beyond one configurable retry."
 * If a request fails with a retryable condition (timeout, connection refused, HTTP 5xx),
 * the client waits briefly and retries up to {@code maxRetries} times. Only the final
 * outcome after all retry attempts counts toward the failure metric.
 *
 * <p>One retry is sufficient given Phase 5's failure detection timing (~1.5–4.5 seconds)
 * because the retry delay allows the cluster's routing layer to detect the dead node
 * and reroute through replica fallback by the time the retry arrives.
 */
public class LoadGenerator {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    // Configurable parameters via environment variables or constructor
    private final List<Integer> targetPorts;
    private final int keyPoolSize;
    private final long sleepMs;
    private final long maxRequests; // 0 = infinite
    private final int maxRetries;
    private final long retryDelayMs;

    private final HttpClient client;
    private final Random random = new Random();

    // Statistics
    private final AtomicLong totalReqs = new AtomicLong(0);
    private final AtomicLong setSuccess = new AtomicLong(0);
    private final AtomicLong setFail = new AtomicLong(0);
    private final AtomicLong getHit = new AtomicLong(0);
    private final AtomicLong getMiss = new AtomicLong(0);
    private final AtomicLong getFail = new AtomicLong(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);
    private final AtomicLong maxLatencyMs = new AtomicLong(0);
    private final AtomicLong firstTrySuccess = new AtomicLong(0);
    private final AtomicLong retriedThenSucceeded = new AtomicLong(0);
    private final AtomicLong totalRetryAttempts = new AtomicLong(0);
    private volatile boolean running = true;
    private volatile long startTimeMs;

    /**
     * Creates a LoadGenerator with default retry policy (1 retry, 200ms delay).
     */
    public LoadGenerator(List<Integer> targetPorts, int keyPoolSize, long sleepMs, long maxRequests) {
        this(targetPorts, keyPoolSize, sleepMs, maxRequests, 1, 200);
    }

    /**
     * Creates a LoadGenerator with configurable retry policy.
     *
     * @param targetPorts  cluster node ports to randomly target
     * @param keyPoolSize  number of distinct keys in the random key pool
     * @param sleepMs      delay between consecutive requests (ms)
     * @param maxRequests  total requests to execute (0 = infinite)
     * @param maxRetries   number of retry attempts on retryable failure (0 = no retries)
     * @param retryDelayMs delay before each retry attempt (ms)
     */
    public LoadGenerator(List<Integer> targetPorts, int keyPoolSize, long sleepMs, long maxRequests,
                         int maxRetries, long retryDelayMs) {
        this.targetPorts = targetPorts;
        this.keyPoolSize = keyPoolSize;
        this.sleepMs = sleepMs;
        this.maxRequests = maxRequests;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(1000))
                .build();
    }

    public static void main(String[] args) {
        // Parse ports from env KAIRO_CLIENT_PORTS or default to 8081, 8082, 8083
        String portsEnv = System.getenv("KAIRO_CLIENT_PORTS");
        List<Integer> ports;
        if (portsEnv != null && !portsEnv.trim().isEmpty()) {
            ports = Arrays.stream(portsEnv.split(","))
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        } else {
            ports = Arrays.asList(8081, 8082, 8083);
        }

        int poolSize = Integer.parseInt(getEnvOrDefault("KAIRO_CLIENT_POOL_SIZE", "50"));
        long sleep = Long.parseLong(getEnvOrDefault("KAIRO_CLIENT_SLEEP_MS", "50"));
        long maxReqs = Long.parseLong(getEnvOrDefault("KAIRO_CLIENT_MAX_REQS", "0"));
        int retries = Integer.parseInt(getEnvOrDefault("KAIRO_CLIENT_MAX_RETRIES", "1"));
        long retryDelay = Long.parseLong(getEnvOrDefault("KAIRO_CLIENT_RETRY_DELAY_MS", "200"));

        LoadGenerator generator = new LoadGenerator(ports, poolSize, sleep, maxReqs, retries, retryDelay);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            generator.stop();
            generator.printSummary();
        }));

        System.out.println("================================================================================");
        System.out.println(" Kairo Load Generator Started | Targets: " + ports + " | Key Pool: " + poolSize + " keys");
        System.out.println(" Sleep: " + sleep + "ms | Max Reqs: " + (maxReqs == 0 ? "INFINITE" : maxReqs));
        System.out.println(" Retry Policy: max " + retries + " retry(s) with " + retryDelay + "ms delay");
        System.out.println("================================================================================");

        generator.runLoop();
    }

    private static String getEnvOrDefault(String name, String def) {
        String val = System.getenv(name);
        return (val != null && !val.trim().isEmpty()) ? val.trim() : def;
    }

    public void stop() {
        this.running = false;
    }

    public void runLoop() {
        startTimeMs = System.currentTimeMillis();

        // Launch a daemon thread that prints a live dashboard every 5 seconds
        ScheduledExecutorService dashboardTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LiveDashboard");
            t.setDaemon(true);
            return t;
        });
        dashboardTimer.scheduleAtFixedRate(this::printLiveDashboard, 5, 5, TimeUnit.SECONDS);

        try {
            while (running) {
                long reqNum = totalReqs.incrementAndGet();
                if (maxRequests > 0 && reqNum > maxRequests) {
                    totalReqs.decrementAndGet();
                    break;
                }

                executeOneRequest();

                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            dashboardTimer.shutdownNow();
        }
        printSummary();
    }

    private void executeOneRequest() {
        // 1. Pick random key
        String key = "load-key-" + random.nextInt(keyPoolSize);

        // 2. Pick random target node port
        int targetPort = targetPorts.get(random.nextInt(targetPorts.size()));
        String host = "localhost:" + targetPort;

        // 3. Pick random operation (50% SET, 50% GET)
        boolean isSet = random.nextBoolean();
        String opStr = isSet ? "SET" : "GET";

        long startTime = System.currentTimeMillis();
        String outcomeStr;

        // Execute with retry policy
        RequestOutcome outcome = executeWithRetry(isSet, key, host, startTime);
        outcomeStr = outcome.description;

        // Record final metrics based on outcome after all retries
        long finalLatency = System.currentTimeMillis() - startTime;
        recordLatency(finalLatency);
        if (isSet) {
            if (outcome.success) setSuccess.incrementAndGet(); else setFail.incrementAndGet();
        } else {
            switch (outcome.type) {
                case HIT -> getHit.incrementAndGet();
                case MISS -> getMiss.incrementAndGet();
                case FAIL -> getFail.incrementAndGet();
            }
        }

        String timestampStr = TIME_FMT.format(Instant.ofEpochMilli(startTime));
        String retryTag = outcome.attemptsTaken > 1
                ? " [RETRIED x" + (outcome.attemptsTaken - 1) + "]"
                : "";

        // Format: [timestamp] [OP] key=... target=... outcome=... latency=...ms
        System.out.printf("[%s] [%-3s] key=%-12s target=%-14s outcome=%-28s latency=%dms%s%n",
                timestampStr, opStr, key, host, outcomeStr, finalLatency, retryTag);
    }

    /**
     * Executes a single HTTP request with up to {@code maxRetries} retry attempts
     * on retryable failures (timeout, connection refused, HTTP 5xx).
     *
     * <p>On retry, the client picks a DIFFERENT node from the target pool rather than
     * hitting the same dead node again — this is what any real client with multiple
     * endpoints would do, and it's critical for surviving a single-node failure.
     */
    private RequestOutcome executeWithRetry(boolean isSet, String key, String host, long startTime) {
        RequestOutcome lastOutcome = null;
        String currentHost = host;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                // Pick a DIFFERENT node for the retry
                currentHost = pickAlternateHost(host);
                totalRetryAttempts.incrementAndGet();
                System.out.printf("  >> RETRY #%d for %s /cache/%s on %s (was %s, delay %dms)%n",
                        attempt, isSet ? "SET" : "GET", key, currentHost, host, retryDelayMs);
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            lastOutcome = executeSingleAttempt(isSet, key, currentHost);

            if (lastOutcome.success || !lastOutcome.retryable) {
                // Either succeeded or hit a non-retryable condition (e.g., 404 cache miss)
                if (attempt == 0 && lastOutcome.success) {
                    firstTrySuccess.incrementAndGet();
                } else if (attempt > 0 && lastOutcome.success) {
                    retriedThenSucceeded.incrementAndGet();
                }
                lastOutcome = lastOutcome.withAttemptsTaken(attempt + 1);
                return lastOutcome;
            }
        }

        // All attempts exhausted — return the last failure
        return lastOutcome != null
                ? lastOutcome.withAttemptsTaken(maxRetries + 1)
                : new RequestOutcome(false, false, OutcomeType.FAIL, "ERROR (no attempts made)", 1);
    }

    /**
     * Executes a single HTTP request attempt (no retries).
     * Returns the outcome including whether the failure is retryable.
     */
    private RequestOutcome executeSingleAttempt(boolean isSet, String key, String host) {
        try {
            if (isSet) {
                String value = "val-" + System.currentTimeMillis();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + host + "/cache/" + key))
                        .timeout(Duration.ofMillis(3000))
                        .POST(HttpRequest.BodyPublishers.ofString(value))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 201 || response.statusCode() == 200) {
                    return new RequestOutcome(true, false, OutcomeType.HIT,
                            "SUCCESS (" + response.statusCode() + ")", 1);
                } else if (response.statusCode() >= 500) {
                    // 5xx is retryable
                    return new RequestOutcome(false, true, OutcomeType.FAIL,
                            "FAIL (HTTP " + response.statusCode() + ": " + response.body().trim() + ")", 1);
                } else {
                    // 4xx client error — not retryable
                    return new RequestOutcome(false, false, OutcomeType.FAIL,
                            "FAIL (HTTP " + response.statusCode() + ": " + response.body().trim() + ")", 1);
                }
            } else {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + host + "/cache/" + key))
                        .timeout(Duration.ofMillis(3000))
                        .GET()
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    return new RequestOutcome(true, false, OutcomeType.HIT,
                            "HIT (200, val=" + response.body().trim() + ")", 1);
                } else if (response.statusCode() == 404) {
                    // Cache miss — normal, not a failure, not retryable
                    return new RequestOutcome(true, false, OutcomeType.MISS,
                            "MISS (404)", 1);
                } else if (response.statusCode() >= 500) {
                    // 5xx is retryable
                    return new RequestOutcome(false, true, OutcomeType.FAIL,
                            "FAIL (HTTP " + response.statusCode() + ": " + response.body().trim() + ")", 1);
                } else {
                    return new RequestOutcome(false, false, OutcomeType.FAIL,
                            "FAIL (HTTP " + response.statusCode() + ": " + response.body().trim() + ")", 1);
                }
            }
        } catch (HttpTimeoutException e) {
            return new RequestOutcome(false, true, OutcomeType.FAIL,
                    "TIMEOUT (" + e.getMessage() + ")", 1);
        } catch (ConnectException e) {
            ejectDeadNode(host);
            return new RequestOutcome(false, true, OutcomeType.FAIL,
                    "CONN_REFUSED (" + e.getMessage() + ")", 1);
        } catch (IOException e) {
            // Network I/O errors are retryable
            return new RequestOutcome(false, true, OutcomeType.FAIL,
                    "IO_ERROR (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")", 1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new RequestOutcome(false, false, OutcomeType.FAIL,
                    "INTERRUPTED", 1);
        } catch (Exception e) {
            // Unknown exceptions are not retryable
            return new RequestOutcome(false, false, OutcomeType.FAIL,
                    "ERROR (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")", 1);
        }
    }

    private void recordLatency(long latencyMs) {
        totalLatencyMs.addAndGet(latencyMs);
        maxLatencyMs.updateAndGet(curr -> Math.max(curr, latencyMs));
    }

    /**
     * Picks a different host from the target pool for retry purposes.
     * If we only have one target, returns the same host (no alternative available).
     */
    private String pickAlternateHost(String failedHost) {
        synchronized (targetPorts) {
            if (targetPorts.size() <= 1) {
                return failedHost; // No alternative available
            }
            // Pick a random port that is NOT the failed one
            int failedPort = Integer.parseInt(failedHost.split(":")[1]);
            List<Integer> alternatives = targetPorts.stream()
                    .filter(p -> p != failedPort)
                    .collect(java.util.stream.Collectors.toList());
            if (alternatives.isEmpty()) return failedHost;
            int altPort = alternatives.get(random.nextInt(alternatives.size()));
            return "localhost:" + altPort;
        }
    }
    
    private void ejectDeadNode(String host) {
        synchronized (targetPorts) {
            int port = Integer.parseInt(host.split(":")[1]);
            if (targetPorts.size() > 1) {
                targetPorts.remove(Integer.valueOf(port));
                System.out.println("  [!] EJECTED dead node from client pool: " + host);
            }
        }
    }

    /**
     * Live dashboard printed every 5 seconds by the background timer thread.
     * Designed to be compelling to watch during a demo — shows at a glance
     * whether the system is holding together under continuous traffic.
     */
    private void printLiveDashboard() {
        long reqs = totalReqs.get();
        long elapsed = System.currentTimeMillis() - startTimeMs;
        double elapsedSec = elapsed / 1000.0;
        double reqsPerSec = elapsedSec > 0 ? reqs / elapsedSec : 0;
        long first = firstTrySuccess.get();
        long retried = retriedThenSucceeded.get();
        long hardFail = setFail.get() + getFail.get();
        long avgLat = reqs > 0 ? totalLatencyMs.get() / reqs : 0;

        System.out.println();
        System.out.println("┌─────────────────────────── LIVE DASHBOARD ────────────────────────────┐");
        System.out.printf( "│  ⏱  Elapsed: %.1fs  │  📊 %d reqs (%.1f req/s)  │  ⚡ Avg %dms      │%n",
                elapsedSec, reqs, reqsPerSec, avgLat);
        System.out.printf( "│  ✅ First-try success: %-6d │  🔄 Retry success: %-4d │  ❌ Hard fail: %-3d │%n",
                first, retried, hardFail);
        System.out.printf( "│  SET ok:%-5d fail:%-4d │  GET hit:%-5d miss:%-5d fail:%-4d          │%n",
                setSuccess.get(), setFail.get(), getHit.get(), getMiss.get(), getFail.get());
        System.out.println("└───────────────────────────────────────────────────────────────────────┘");
        System.out.println();
    }

    public void printSummary() {
        long reqs = totalReqs.get();
        long elapsed = System.currentTimeMillis() - startTimeMs;
        double elapsedSec = elapsed / 1000.0;
        double reqsPerSec = elapsedSec > 0 ? reqs / elapsedSec : 0;
        long avgLat = reqs > 0 ? totalLatencyMs.get() / reqs : 0;
        long hardFail = setFail.get() + getFail.get();
        System.out.println("\n================================================================================");
        System.out.println("                    KAIRO LOAD GENERATOR AUDIT SUMMARY");
        System.out.println("================================================================================");
        System.out.printf(" Duration                : %.1f seconds%n", elapsedSec);
        System.out.printf(" Total Requests Executed : %d (%.1f req/s)%n", reqs, reqsPerSec);
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" Successful SETs         : %d%n", setSuccess.get());
        System.out.printf(" Failed SETs             : %d%n", setFail.get());
        System.out.printf(" Successful GETs (HIT)   : %d%n", getHit.get());
        System.out.printf(" Successful GETs (MISS)  : %d%n", getMiss.get());
        System.out.printf(" Failed GETs (Errors)    : %d%n", getFail.get());
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" ✅ First-try success     : %d%n", firstTrySuccess.get());
        System.out.printf(" 🔄 Retried → Succeeded   : %d%n", retriedThenSucceeded.get());
        System.out.printf(" ❌ Hard Failures          : %d (after all %d retry(s) exhausted)%n",
                hardFail, maxRetries);
        System.out.printf(" Total Retry Attempts    : %d%n", totalRetryAttempts.get());
        System.out.printf(" Retry Policy            : max %d retry(s), %dms delay%n", maxRetries, retryDelayMs);
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" Average Latency         : %d ms%n", avgLat);
        System.out.printf(" Maximum Latency         : %d ms%n", maxLatencyMs.get());
        System.out.println("================================================================================");
    }

    // ---- Public accessors for test assertions ----

    public long getTotalRequests()       { return totalReqs.get(); }
    public long getFirstTrySuccess()     { return firstTrySuccess.get(); }
    public long getRetriedThenSucceeded(){ return retriedThenSucceeded.get(); }
    public long getTotalRetryAttempts()  { return totalRetryAttempts.get(); }
    public long getSetSuccess()          { return setSuccess.get(); }
    public long getSetFail()             { return setFail.get(); }
    public long getGetHit()              { return getHit.get(); }
    public long getGetMiss()             { return getMiss.get(); }
    public long getGetFail()             { return getFail.get(); }
    public long getHardFailures()        { return setFail.get() + getFail.get(); }

    // ---- Internal result types ----

    enum OutcomeType { HIT, MISS, FAIL }

    /**
     * Captures the result of a single request attempt or the final result after retries.
     */
    static class RequestOutcome {
        final boolean success;
        final boolean retryable;
        final OutcomeType type;
        final String description;
        final int attemptsTaken;

        RequestOutcome(boolean success, boolean retryable, OutcomeType type, String description, int attemptsTaken) {
            this.success = success;
            this.retryable = retryable;
            this.type = type;
            this.description = description;
            this.attemptsTaken = attemptsTaken;
        }

        RequestOutcome withAttemptsTaken(int attempts) {
            return new RequestOutcome(success, retryable, type, description, attempts);
        }
    }
}

