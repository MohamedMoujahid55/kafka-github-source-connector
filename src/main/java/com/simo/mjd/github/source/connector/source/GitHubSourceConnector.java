package com.simo.mjd.github.source.connector.source;

import com.simo.mjd.github.source.connector.config.GitHubSourceConnectorConfig;
import com.simo.mjd.github.source.connector.util.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * GitHubSourceConnector – Kafka Connect Source Connector that monitors a GitHub repository
 * and streams issues and pull requests into an Apache Kafka topic.
 *
 * <p>Kafka Connect instantiates this class, invokes {@link #start(Map)} once,
 * and calls {@link #taskConfigs(int)} to generate configuration mappings for
 * {@link GitHubSourceTask} workers.
 *
 * <p>Example distributed REST API registration:
 * <pre>{@code
 * POST /connectors
 * {
 *   "name": "github-source-connector",
 *   "config": {
 *     "connector.class": "com.simo.mjd.github.source.connector.source.GitHubSourceConnector",
 *     "tasks.max": "1",
 *     "topic": "github-issues",
 *     "github.owner": "apache",
 *     "github.repo": "kafka",
 *     "since.timestamp": "2026-01-01T00:00:00Z",
 *     "batch.size": "100"
 *   }
 * }
 * }</pre>
 */
public class GitHubSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(GitHubSourceConnector.class);

    private GitHubSourceConnectorConfig config;

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /**
     * Initializes and validates the connector configuration.
     *
     * @param props configuration properties map
     */
    @Override
    public void start(Map<String, String> props) {
        this.config = new GitHubSourceConnectorConfig(props);
        log.info("Starting GitHubSourceConnector for owner={}, repo={}, topic={}",
                config.getOwner(), config.getRepo(), config.getTopic());
    }

    /**
     * Stops the connector and releases any connector-level resources.
     */
    @Override
    public void stop() {
        log.info("Stopping GitHubSourceConnector");
    }

    // ── Task delegation ──────────────────────────────────────────────────────

    /**
     * Returns the execution task class implemented by this connector.
     *
     * @return {@link GitHubSourceTask} class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return GitHubSourceTask.class;
    }

    /**
     * Builds task configurations for active workers.
     *
     * @param maxTasks upper bound defined by {@code tasks.max}
     * @return list of task configuration property maps
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Building task configs for up to {} tasks", maxTasks);
        List<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(config.originalsStrings());
        return configs;
    }

    // ── Metadata ─────────────────────────────────────────────────────────────

    /**
     * Returns the connector configuration definition schema.
     *
     * @return {@link ConfigDef} schema
     */
    @Override
    public ConfigDef config() {
        return GitHubSourceConnectorConfig.CONFIG_DEF;
    }

    /**
     * Returns the connector version.
     *
     * @return version string
     */
    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
