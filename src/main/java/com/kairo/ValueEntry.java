package com.kairo;

/**
 * Holds a cached value alongside its absolute expiry timestamp.
 *
 * @param value     the cached payload
 * @param expiresAt absolute epoch millis when this entry expires,
 *                  or {@link Long#MAX_VALUE} for "never expires"
 */
public record ValueEntry(String value, long expiresAt, long writeTimestamp) {

    /** Sentinel meaning "this entry never expires." */
    public static final long NO_EXPIRY = Long.MAX_VALUE;

    /**
     * Convenience constructor for entries that never expire.
     */
    public ValueEntry(String value) {
        this(value, NO_EXPIRY, System.currentTimeMillis());
    }

    /**
     * Convenience constructor for entries that have a specific expiry but default write time.
     */
    public ValueEntry(String value, long expiresAt) {
        this(value, expiresAt, System.currentTimeMillis());
    }

    /**
     * @return true if this entry has passed its expiry time
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > expiresAt;
    }
}
