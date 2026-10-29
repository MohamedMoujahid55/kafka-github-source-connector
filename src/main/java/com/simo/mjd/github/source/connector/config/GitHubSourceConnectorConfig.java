package com.simo.mjd.github.source.connector.config;

import com.simo.mjd.github.source.connector.validator.BatchSizeValidator;
import com.simo.mjd.github.source.connector.validator.TimestampValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Shared configuration definition and parser for the GitHub Source Connector.
 *
 * <p>Defines configuration properties including GitHub repository owner, repository name,
 * since timestamp, batch size, target Kafka topic, and optional Bearer token authentication.
 */
public class GitHubSourceConnectorConfig extends AbstractConfig {

    // ── Config key names ─────────────────────────────────────────────────────

    /** Kafka topic name to which issue records are published. */
    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "Kafka topic to write to.";

    /** Owner (organization or user) of the GitHub repository. */
    public static final String OWNER_CONFIG = "github.owner";
    private static final String OWNER_DOC = "Owner of the repository you would like to follow.";

    /** Name of the GitHub repository. */
    public static final String REPO_CONFIG = "github.repo";
    private static final String REPO_DOC = "Name of the repository you would like to follow.";

    /** Initial ISO-8601 timestamp filter for issues. */
    public static final String SINCE_CONFIG = "since.timestamp";
    private static final String SINCE_DOC = "Only issues updated at or after this time are returned. " +
            "This is a timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ";

    /** Personal Access Token (PAT) for GitHub API authentication. */
    public static final String BEARER_AUTH_TOKEN = "bearer.github.pat.token";
    private static final String BEARER_AUTH_TOKEN_DOC = "Personal Access Token (PAT) for GitHub API calls. Optional for public repositories.";

    /** Maximum number of records fetched per GitHub REST API request (1-100). */
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Number of records to fetch per poll (1-100).";
    private static final int BATCH_SIZE_DEFAULT = 100;

    // ── ConfigDef ────────────────────────────────────────────────────────────

    /**
     * Configuration definition schema for the connector.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
        .define(OWNER_CONFIG, Type.STRING, Importance.HIGH, OWNER_DOC)
        .define(REPO_CONFIG, Type.STRING, Importance.HIGH, REPO_DOC)
        .define(SINCE_CONFIG, Type.STRING, ZonedDateTime.now(ZoneId.systemDefault()).minusYears(1).toInstant().toString(),
                new TimestampValidator(), Importance.HIGH, SINCE_DOC)
        .define(BEARER_AUTH_TOKEN, Type.PASSWORD, "", Importance.HIGH, BEARER_AUTH_TOKEN_DOC)
        .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, new BatchSizeValidator(), Importance.LOW, BATCH_SIZE_DOC);

    // ── Constructor ──────────────────────────────────────────────────────────

    /**
     * Constructs a configuration instance from a property map.
     *
     * @param originals configuration properties
     */
    public GitHubSourceConnectorConfig(Map<String, String> originals) {
        super(CONFIG_DEF, originals);
    }

    // ── Typed accessors ──────────────────────────────────────────────────────

    /**
     * Gets the configured target Kafka topic.
     *
     * @return topic name
     */
    public String getTopic() {
        return getString(TOPIC_CONFIG);
    }

    /**
     * Gets the configured batch size per poll request.
     *
     * @return batch size integer
     */
    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    /**
     * Gets the GitHub repository owner.
     *
     * @return repository owner
     */
    public String getOwner() {
        return getString(OWNER_CONFIG);
    }

    /**
     * Gets the GitHub repository name.
     *
     * @return repository name
     */
    public String getRepo() {
        return getString(REPO_CONFIG);
    }

    /**
     * Gets the parsed initial cutoff {@link Instant}.
     *
     * @return since timestamp as {@link Instant}
     */
    public Instant getSince() {
        return Instant.parse(getString(SINCE_CONFIG));
    }

    /**
     * Gets the Bearer auth token if configured, trimmed and unmasked.
     *
     * @return token string or null if empty
     */
    public String getBearerAuthToken() {
        Password password = getPassword(BEARER_AUTH_TOKEN);
        if (password != null) {
            String value = password.value();
            return (value != null && !value.isBlank()) ? value.trim() : null;
        }
        return null;
    }
}
