#!/usr/bin/env bash
# ================================================================================
#  Kairo Chaos Trigger — Phase 8, Step 5
#
#  Runs alongside the LoadGenerator to demonstrate the full failure lifecycle:
#    1. Healthy cluster baseline (configurable warm-up)
#    2. Abrupt node kill (docker kill — simulates a real crash, no graceful shutdown)
#    3. Cluster adapts via failure detection + replica fallback
#    4. Node recovery (docker compose start — triggers pull-based rebalancing)
#    5. Cluster rebalances and returns to full health
#
#  Usage:
#    ./scripts/chaos-trigger.sh [options]
#
#  Options:
#    --target <service>      Node to kill (default: random from node-1, node-2, node-3)
#    --kill-delay <secs>     Seconds to wait before killing (default: 10)
#    --recovery-delay <secs> Seconds after kill before restarting (default: 15)
#    --mode <kill|stop>      kill = SIGKILL (abrupt crash), stop = SIGTERM (default: kill)
#    --no-recovery           Skip the recovery phase (only test failure, not rejoin)
#    --dry-run               Print what would happen without executing
#
#  Why `docker kill` over `docker compose stop`:
#    The spec says "live kill." docker kill sends SIGKILL — no graceful shutdown,
#    no chance for the process to clean up. This is the honest test of whether
#    failure detection and replica fallback actually work under a real crash,
#    not just a clean shutdown path.
# ================================================================================

set -euo pipefail

# ---- Configuration (overridable via flags) ----
NODES=("node-1" "node-2" "node-3")
TARGET=""
KILL_DELAY=10
RECOVERY_DELAY=15
MODE="kill"       # "kill" = SIGKILL (abrupt), "stop" = SIGTERM (graceful)
SKIP_RECOVERY=false
DRY_RUN=false

# ---- Parse CLI flags ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)         TARGET="$2";         shift 2 ;;
        --kill-delay)     KILL_DELAY="$2";     shift 2 ;;
        --recovery-delay) RECOVERY_DELAY="$2"; shift 2 ;;
        --mode)           MODE="$2";           shift 2 ;;
        --no-recovery)    SKIP_RECOVERY=true;  shift   ;;
        --dry-run)        DRY_RUN=true;        shift   ;;
        -h|--help)
            head -28 "$0" | tail -24
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ---- Pick target node ----
if [[ -z "$TARGET" ]]; then
    TARGET="${NODES[$((RANDOM % ${#NODES[@]}))]}"
fi

# Validate target
if [[ ! " ${NODES[*]} " =~ " ${TARGET} " ]]; then
    echo "❌ Invalid target: $TARGET (must be one of: ${NODES[*]})" >&2
    exit 1
fi

# ---- Helpers ----
timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

log() {
    echo "[$(timestamp)] [CHAOS] $*"
}

run_or_dry() {
    if $DRY_RUN; then
        echo "  [DRY-RUN] Would execute: $*"
    else
        "$@"
    fi
}

check_container_health() {
    local node="$1"
    local port
    case "$node" in
        node-1) port=8081 ;;
        node-2) port=8082 ;;
        node-3) port=8083 ;;
    esac

    if curl -s --connect-timeout 1 "http://localhost:${port}/ping" > /dev/null 2>&1; then
        echo "UP"
    else
        echo "DOWN"
    fi
}

# ================================================================================
#  PHASE 1: Pre-flight checks
# ================================================================================
log "╔══════════════════════════════════════════════════════════════════════╗"
log "║                    KAIRO CHAOS TRIGGER                             ║"
log "╠══════════════════════════════════════════════════════════════════════╣"
log "║  Target node     : $TARGET"
log "║  Kill mode       : $MODE ($([ "$MODE" = "kill" ] && echo "SIGKILL — abrupt crash" || echo "SIGTERM — graceful stop"))"
log "║  Kill delay      : ${KILL_DELAY}s after script start"
log "║  Recovery delay  : ${RECOVERY_DELAY}s after kill"
log "║  Skip recovery   : $SKIP_RECOVERY"
log "║  Dry run         : $DRY_RUN"
log "╚══════════════════════════════════════════════════════════════════════╝"
echo

# Verify all nodes are up before starting
log "Pre-flight: checking cluster health..."
ALL_UP=true
for node in "${NODES[@]}"; do
    status=$(check_container_health "$node")
    if [[ "$status" == "UP" ]]; then
        log "  ✅ $node: $status"
    else
        log "  ❌ $node: $status"
        ALL_UP=false
    fi
done

if ! $ALL_UP && ! $DRY_RUN; then
    log "⚠️  WARNING: Not all nodes are healthy. Consider running 'docker compose up -d' first."
    echo -n "[CHAOS] Continue anyway? (y/N) "
    read -r answer
    if [[ "$answer" != "y" && "$answer" != "Y" ]]; then
        log "Aborted."
        exit 1
    fi
fi

# ================================================================================
#  PHASE 2: Warm-up — let the LoadGenerator establish a baseline
# ================================================================================
log "⏳ Phase 1: Warm-up — waiting ${KILL_DELAY}s for baseline traffic..."
log "  (Start the LoadGenerator now if you haven't already)"
log "  LoadGenerator command:"
log "    KAIRO_CLIENT_PORTS=8081,8082,8083 KAIRO_CLIENT_SLEEP_MS=20 mvn exec:java -Dexec.mainClass=com.kairo.LoadGenerator"
echo

if ! $DRY_RUN; then
    sleep "$KILL_DELAY"
fi

# ================================================================================
#  PHASE 3: Kill the target node — the moment of truth
# ================================================================================
log "═══════════════════════════════════════════════════════════════"
log "💀 Phase 2: KILLING $TARGET via 'docker $MODE'"
log "═══════════════════════════════════════════════════════════════"

if [[ "$MODE" == "kill" ]]; then
    run_or_dry docker kill "$TARGET"
else
    run_or_dry docker compose stop "$TARGET"
fi

log "  $TARGET is now DOWN. Failure detector should notice within ~1.5-4.5s."
log "  Watch the LoadGenerator output for retry activity..."
echo

# Quick health check after kill
if ! $DRY_RUN; then
    sleep 2
    log "Post-kill health check:"
    for node in "${NODES[@]}"; do
        status=$(check_container_health "$node")
        if [[ "$node" == "$TARGET" ]]; then
            log "  💀 $node: $status (killed)"
        elif [[ "$status" == "UP" ]]; then
            log "  ✅ $node: $status (surviving)"
        else
            log "  ❌ $node: $status (unexpected!)"
        fi
    done
    echo
fi

# ================================================================================
#  PHASE 4: Recovery — bring the node back and trigger rebalancing
# ================================================================================
if $SKIP_RECOVERY; then
    log "⏭️  Skipping recovery phase (--no-recovery flag set)"
    log "  The cluster will continue operating in degraded mode."
    log "  The LoadGenerator should show 0 hard failures thanks to replica fallback."
else
    log "⏳ Phase 3: Degraded operation — waiting ${RECOVERY_DELAY}s before recovery..."
    log "  The cluster is now running on 2 nodes. Observe the LoadGenerator dashboard."
    
    if ! $DRY_RUN; then
        sleep "$RECOVERY_DELAY"
    fi

    log "═══════════════════════════════════════════════════════════════"
    log "🔄 Phase 4: RECOVERING $TARGET via 'docker compose start'"
    log "═══════════════════════════════════════════════════════════════"
    
    run_or_dry docker compose start "$TARGET"
    
    log "  $TARGET is restarting. Failure detector will notice recovery in ~1.5-4.5s."
    log "  Pull-based rebalancing will then transfer the node's key range back."
    echo

    # Wait for recovery and check health
    if ! $DRY_RUN; then
        log "  Waiting 8s for heartbeat convergence and migration..."
        sleep 8
        
        log "Post-recovery health check:"
        for node in "${NODES[@]}"; do
            status=$(check_container_health "$node")
            if [[ "$status" == "UP" ]]; then
                log "  ✅ $node: $status"
            else
                log "  ❌ $node: $status (still recovering?)"
            fi
        done
        echo
    fi
fi

# ================================================================================
#  PHASE 5: Summary
# ================================================================================
log "═══════════════════════════════════════════════════════════════"
log "✅ Chaos scenario complete!"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  Full lifecycle demonstrated:"
log "    1. Healthy cluster baseline         (${KILL_DELAY}s warm-up)"
log "    2. Abrupt node failure              ($TARGET killed via $MODE)"
log "    3. Cluster adapted via failover     (failure detection + replica reads)"
if ! $SKIP_RECOVERY; then
    log "    4. Node recovered                   ($TARGET restarted)"
    log "    5. Cluster rebalanced               (pull-based migration)"
fi
log ""
log "  Check the LoadGenerator's final summary for:"
log "    ✅ First-try success count"
log "    🔄 Retried → Succeeded count (should be small)"
log "    ❌ Hard failures (should be ZERO)"
log ""
log "  Stop the LoadGenerator with Ctrl+C to see the final audit summary."
