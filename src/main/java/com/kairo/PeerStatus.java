package com.kairo;

/**
 * Represents the health status of a remote peer node in the cluster.
 *
 * <ul>
 *   <li>{@link #ALIVE}: The peer is responding cleanly to periodic heartbeats.</li>
 *   <li>{@link #SUSPECTED}: The peer has missed 1 or 2 heartbeats. This intermediate state
 *       provides a warning before declaring death (similar to Phi Accrual failure detectors).</li>
 *   <li>{@link #DEAD}: The peer has missed the full failure threshold of heartbeats
 *       and is considered offline.</li>
 * </ul>
 */
public enum PeerStatus {
    ALIVE,
    SUSPECTED,
    DEAD,
    MIGRATING
}
