#!/usr/bin/env bash
# ================================================================================
#  Step 6: Combined Test Orchestrator
#  Runs LoadGenerator + Chaos Trigger together and captures results.
# ================================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_FILE="$PROJECT_DIR/scripts/combined-test-results.log"

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] [ORCHESTRATOR] $*" | tee -a "$RESULTS_FILE"; }

# Clear previous results
> "$RESULTS_FILE"

log "╔══════════════════════════════════════════════════════════════════╗"
log "║          STEP 6: COMBINED LOAD + CHAOS TEST                    ║"
log "╚══════════════════════════════════════════════════════════════════╝"

# ---- Pre-flight ----
log "Pre-flight: verifying cluster health..."
ALL_UP=true
for p in 8081 8082 8083; do
    if curl -s --connect-timeout 2 "http://localhost:$p/ping" > /dev/null 2>&1; then
        log "  ✅ localhost:$p — UP"
    else
        log "  ❌ localhost:$p — DOWN"
        ALL_UP=false
    fi
done

if ! $ALL_UP; then
    log "ERROR: Not all nodes are healthy. Run 'docker compose up -d' first."
    exit 1
fi
log "All 3 nodes healthy. Starting combined test..."
echo

# ---- Phase 1: Start LoadGenerator in background ----
log "Phase 1: Starting LoadGenerator (50 keys, 20ms sleep, 1 retry, 200ms retry delay)..."

KAIRO_CLIENT_PORTS=8081,8082,8083 \
KAIRO_CLIENT_POOL_SIZE=50 \
KAIRO_CLIENT_SLEEP_MS=20 \
KAIRO_CLIENT_MAX_REQS=0 \
KAIRO_CLIENT_MAX_RETRIES=1 \
KAIRO_CLIENT_RETRY_DELAY_MS=200 \
java -cp "$PROJECT_DIR/target/kairo-1.0-SNAPSHOT.jar" com.kairo.LoadGenerator \
    >> "$RESULTS_FILE" 2>&1 &
LG_PID=$!
log "LoadGenerator started (PID: $LG_PID)"

# Ensure cleanup on exit
cleanup() {
    if kill -0 "$LG_PID" 2>/dev/null; then
        log "Stopping LoadGenerator (PID: $LG_PID)..."
        kill -TERM "$LG_PID" 2>/dev/null || true
        wait "$LG_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ---- Phase 2: Baseline warm-up ----
WARMUP=10
log "Phase 2: Baseline warm-up (${WARMUP}s with all nodes healthy)..."
sleep "$WARMUP"
log "Warm-up complete."
echo

# ---- Phase 3: Kill a node ----
TARGET="node-2"
log "═══════════════════════════════════════════════════════════════"
log "💀 Phase 3: KILLING $TARGET via 'docker kill' (SIGKILL — abrupt crash)"
log "═══════════════════════════════════════════════════════════════"
docker kill "$TARGET" 2>&1 | tee -a "$RESULTS_FILE"
log "$TARGET is now DOWN."
echo

# ---- Phase 4: Degraded operation ----
DEGRADED=15
log "Phase 4: Degraded operation (${DEGRADED}s with $TARGET down)..."
log "  Failure detector will notice within ~3-6s."
log "  Watch for retries in the LoadGenerator output."
sleep "$DEGRADED"
log "Degraded operation phase complete."
echo

# ---- Phase 5: Recover the node ----
log "═══════════════════════════════════════════════════════════════"
log "🔄 Phase 5: RECOVERING $TARGET via 'docker compose start'"
log "═══════════════════════════════════════════════════════════════"
docker compose start "$TARGET" 2>&1 | tee -a "$RESULTS_FILE"
log "$TARGET is restarting. Waiting for heartbeat convergence + rebalancing..."

RECOVERY=15
sleep "$RECOVERY"

# Health check after recovery
log "Post-recovery health check:"
for p in 8081 8082 8083; do
    if curl -s --connect-timeout 2 "http://localhost:$p/ping" > /dev/null 2>&1; then
        log "  ✅ localhost:$p — UP"
    else
        log "  ❌ localhost:$p — still recovering"
    fi
done
echo

# ---- Phase 6: Post-recovery steady state ----
STEADY=10
log "Phase 6: Post-recovery steady state (${STEADY}s — all nodes should be healthy)..."
sleep "$STEADY"

# ---- Phase 7: Stop LoadGenerator and collect results ----
log "═══════════════════════════════════════════════════════════════"
log "Phase 7: Stopping LoadGenerator and collecting final results..."
log "═══════════════════════════════════════════════════════════════"

kill -TERM "$LG_PID" 2>/dev/null || true
wait "$LG_PID" 2>/dev/null || true
# Prevent double-cleanup in trap
trap - EXIT

echo
log "═══════════════════════════════════════════════════════════════"
log "✅ Combined test complete! Results written to:"
log "   $RESULTS_FILE"
log "═══════════════════════════════════════════════════════════════"
echo
log "Extracting final summary from results..."
echo
echo "======== FINAL AUDIT SUMMARY ========"
grep -A 20 "KAIRO LOAD GENERATOR AUDIT SUMMARY" "$RESULTS_FILE" | head -25
echo "======================================"
