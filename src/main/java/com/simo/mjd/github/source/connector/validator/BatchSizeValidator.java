package com.simo.mjd.github.source.connector.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validates that the configured batch size falls within GitHub REST API limits (1 to 100).
 */
public class BatchSizeValidator implements ConfigDef.Validator {

    /**
     * Ensures the configured batch size is between 1 and 100 inclusive.
     *
     * @param name  the configuration property name
     * @param value the configured value
     * @throws ConfigException if value is null, non-integer, or outside [1, 100]
     */
    @Override
    public void ensureValid(String name, Object value) {
        if (!(value instanceof Integer batchSize)) {
            throw new ConfigException(name, value, "Batch size must be an integer");
        }
        if (batchSize <= 0 || batchSize > 100) {
            throw new ConfigException(name, value, "Batch size must be a positive integer between 1 and 100");
        }
    }
}
