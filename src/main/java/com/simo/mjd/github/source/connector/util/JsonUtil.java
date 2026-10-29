package com.simo.mjd.github.source.connector.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Shared utility provider for JSON serialization and deserialization via Jackson {@link ObjectMapper}.
 */
public final class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonUtil() {
        // Utility class – prevent instantiation
    }

    /**
     * Returns the singleton preconfigured {@link ObjectMapper} instance supporting Java time types.
     *
     * @return the configured {@link ObjectMapper}
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}
