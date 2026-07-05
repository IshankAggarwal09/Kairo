package com.kairo;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Identity and network configuration for a single Kairo node.
 *
 * <p>All values are read from environment variables at startup so that
 * each container instance can be configured differently without
 * rebuilding the image.
 *
 * <table>
 *   <tr><th>Env Var</th><th>Default</th><th>Purpose</th></tr>
 *   <tr><td>NODE_ID</td><td>node-0</td><td>Human-readable node name (used in logs, replication headers)</td></tr>
 *   <tr><td>KAIRO_PORT</td><td>8080</td><td>HTTP port this node listens on</td></tr>
 *   <tr><td>PEERS</td><td>(empty)</td><td>Comma-separated list of cluster peers, e.g. "node-1:8081,node-2:8082" or "node-1:localhost:8081"</td></tr>
 *   <tr><td>PEER_HOST</td><td>(peer ID)</td><td>Default hostname for 2-part peer configs when running locally outside Docker (e.g. "localhost")</td></tr>
 * </table>
 *
 * <p><b>Design Decision (Excluding Self):</b>
 * When parsing {@code PEERS}, if an entry's ID matches this node's {@code NODE_ID},
 * it is automatically excluded from the peer list. This allows all containers/nodes
 * in a deployment (e.g. Docker Compose) to be injected with the exact same
 * {@code PEERS} string while preventing accidental self-replication loops.
 */
public class NodeConfig {

    private final String nodeId;
    private final int port;
    private final Map<String, PeerNode> peers;
    private final int maxKeysPerNode;

    NodeConfig(String nodeId, int port, Map<String, PeerNode> peers) {
        this(nodeId, port, peers, -1); // Default to infinite
    }

    NodeConfig(String nodeId, int port, Map<String, PeerNode> peers, int maxKeysPerNode) {
        this.nodeId = nodeId;
        this.port = port;
        this.peers = new ConcurrentHashMap<>(peers);
        this.maxKeysPerNode = maxKeysPerNode;
    }

    /**
     * Builds a {@code NodeConfig} from environment variables.
     */
    public static NodeConfig fromEnv() {
        String nodeId = System.getenv().getOrDefault("NODE_ID", "node-0");
        String portStr = System.getenv("NODE_PORT");
        if (portStr == null) {
            portStr = System.getenv().getOrDefault("KAIRO_PORT", "8080");
        }
        int port = Integer.parseInt(portStr);

        String peersEnv = System.getenv("PEERS");
        Map<String, PeerNode> peersMap = parsePeers(peersEnv, nodeId);

        String maxKeysStr = System.getenv().getOrDefault("KAIRO_MAX_KEYS_PER_NODE", "-1");
        int maxKeysPerNode = Integer.parseInt(maxKeysStr);

        return new NodeConfig(nodeId, port, peersMap, maxKeysPerNode);
    }

    /**
     * Parses the PEERS environment variable string into a peer map, excluding self.
     *
     * <p>Supported item formats:
     * <ul>
     *   <li>{@code id:host:port} — explicit host (e.g. "node-1:localhost:8081")</li>
     *   <li>{@code id:port} — host defaults to {@code id} or PEER_HOST env var</li>
     * </ul>
     */
    static Map<String, PeerNode> parsePeers(String peersStr, String selfId) {
        if (peersStr == null || peersStr.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, PeerNode> map = new LinkedHashMap<>();
        String defaultHost = System.getenv("PEER_HOST");

        for (String item : peersStr.split(",")) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) continue;

            String[] parts = trimmed.split(":");
            String peerId;
            String host;
            int peerPort;

            if (parts.length == 3) {
                peerId = parts[0].trim();
                host = parts[1].trim();
                peerPort = Integer.parseInt(parts[2].trim());
            } else if (parts.length == 2) {
                peerId = parts[0].trim();
                host = defaultHost != null ? defaultHost : peerId;
                peerPort = Integer.parseInt(parts[1].trim());
            } else {
                continue; // Ignore malformed entries
            }

            // Design Decision: exclude self from active peer map
            if (peerId.equals(selfId)) {
                continue;
            }

            map.put(peerId, new PeerNode(peerId, host, peerPort));
        }

        return map;
    }

    public String nodeId() {
        return nodeId;
    }

    public int port() {
        return port;
    }

    public int maxKeysPerNode() {
        return maxKeysPerNode;
    }

    /**
     * Returns an unmodifiable map of remote peer ID &rarr; {@link PeerNode}.
     * This node's own ID is guaranteed not to be in this map.
     */
    public Map<String, PeerNode> peers() {
        return peers;
    }

    public void addPeer(PeerNode peer) {
        if (peer != null && !peer.id().equals(nodeId)) {
            peers.put(peer.id(), peer);
        }
    }

    public void removePeer(String peerId) {
        if (peerId != null) {
            peers.remove(peerId);
        }
    }

    @Override
    public String toString() {
        return nodeId + " (port " + port + "), peers: " + peers.values();
    }
}
