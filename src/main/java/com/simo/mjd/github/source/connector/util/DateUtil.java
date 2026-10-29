package com.simo.mjd.github.source.connector.util;

import java.time.Instant;

/**
 * Utility functions for date and timestamp calculations.
 */
public final class DateUtil {

    private DateUtil() {
        throw new IllegalStateException("Utility class – prevent instantiation");
    }

    /**
     * Returns the greater (more recent) of two {@link Instant} instances.
     *
     * @param i1 first instant
     * @param i2 second instant
     * @return the later instant between {@code i1} and {@code i2}
     */
    public static Instant maxInstant(Instant i1, Instant i2) {
        return i1.compareTo(i2) > 0 ? i1 : i2;
    }
}
