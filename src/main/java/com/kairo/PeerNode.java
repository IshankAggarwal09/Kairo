package com.kairo;

/**
 * Represents a remote peer node in the Kairo cluster.
 *
 * @param id   The unique identifier of the peer (e.g., "node-1")
 * @param host The hostname or IP address of the peer (e.g., "localhost" or "node-1")
 * @param port The HTTP port the peer listens on (e.g., 8081)
 */
public record PeerNode(String id, String host, int port) {

    /**
     * Returns the base HTTP URL for this peer, e.g. "http://node-1:8081".
     */
    public String baseUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return id + "(" + host + ":" + port + ")";
    }
}
