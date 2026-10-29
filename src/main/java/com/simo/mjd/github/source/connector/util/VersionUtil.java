package com.simo.mjd.github.source.connector.util;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility helper to retrieve the connector version at runtime from JAR package metadata.
 */
@Slf4j
public final class VersionUtil {

    private VersionUtil() {
        // Utility class – prevent instantiation
    }

    /**
     * Retrieves the implementation version of the connector package, falling back to "1.0-SNAPSHOT" if unavailable.
     *
     * @return version string
     */
    public static String getVersion() {
        try {
            String version = VersionUtil.class.getPackage().getImplementationVersion();
            if (version != null && !version.isBlank()) {
                return version;
            }
        } catch (Exception e) {
            log.error("Failed to resolve package version: {}", e.getMessage());
        }
        return "1.0-SNAPSHOT";
    }
}
