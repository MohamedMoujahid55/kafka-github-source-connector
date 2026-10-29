package com.simo.mjd.github.source.connector.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Validates that the timestamp configuration follows standard ISO-8601 format (e.g., {@code 2026-01-01T00:00:00Z}).
 */
public class TimestampValidator implements ConfigDef.Validator {

    /**
     * Ensures the given value represents a valid ISO-8601 instant string.
     *
     * @param name  the configuration property name
     * @param value the configured value
     * @throws ConfigException if parsing fails or value is not formatted properly
     */
    @Override
    public void ensureValid(String name, Object value) {
        String date = (String) value;
        try {
            Instant.parse(date);
        } catch (DateTimeParseException e) {
            throw new ConfigException(name, value, "Invalid date format, please make sure it is formatted according to ISO-8601 standards.");
        }
    }
}
