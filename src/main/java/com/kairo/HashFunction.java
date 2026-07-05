package com.kairo;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Consistent hashing hash function for Kairo.
 *
 * <p>Uses MD5 truncated to 32 bits, returned as an unsigned 32-bit integer
 * stored in a Java {@code long} (range 0 to 2^32 - 1).
 *
 * <p><b>Why MD5 over {@code String.hashCode()}:</b>
 * Java's {@code String.hashCode()} has poor distribution properties and
 * frequent collisions for similar strings. MD5 provides a uniform distribution
 * across the 32-bit ring space without requiring any external dependencies.
 * Since this is used purely for distribution on a consistent hash ring and
 * not for cryptographic security, MD5 truncated to 32 bits is ideal and standard.
 */
public class HashFunction {

    private static final String ALGORITHM = "MD5";

    /**
     * Hashes the input string into a 32-bit unsigned integer (range 0 to 4,294,967,295).
     *
     * @param input Arbitrary string (node ID, virtual node ID, or cache key)
     * @return Ring token between 0 and 2^32 - 1
     */
    public static long hash(String input) {
        if (input == null) {
            return 0L;
        }
        try {
            MessageDigest md = MessageDigest.getInstance(ALGORITHM);
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            // Take the first 4 bytes and interpret as big-endian unsigned 32-bit integer
            return ((long) (digest[0] & 0xFF) << 24)
                 | ((long) (digest[1] & 0xFF) << 16)
                 | ((long) (digest[2] & 0xFF) << 8)
                 | ((long) (digest[3] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available in JVM", e);
        }
    }
}
