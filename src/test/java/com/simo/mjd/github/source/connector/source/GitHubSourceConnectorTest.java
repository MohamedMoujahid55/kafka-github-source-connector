package com.simo.mjd.github.source.connector.source;

import com.simo.mjd.github.source.connector.config.GitHubSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GitHubSourceConnectorTest {

    private GitHubSourceConnector connector;

    @BeforeEach
    void setUp() {
        connector = new GitHubSourceConnector();
    }

    @AfterEach
    void tearDown() {
        connector.stop();
    }

    @Test
    void version_shouldNotBeNull() {
        assertNotNull(connector.version());
    }

    @Test
    void taskClass_shouldBeGitHubSourceTask() {
        assertEquals(GitHubSourceTask.class, connector.taskClass());
    }

    @Test
    void start_withValidConfig_shouldSucceed() {
        connector.start(validConfig());
    }

    @Test
    void taskConfigs_shouldReturnAtLeastOneConfig() {
        connector.start(validConfig());
        List<Map<String, String>> configs = connector.taskConfigs(1);
        assertFalse(configs.isEmpty());
        assertEquals("test-topic", configs.get(0).get(GitHubSourceConnectorConfig.TOPIC_CONFIG));
        assertEquals("apache", configs.get(0).get(GitHubSourceConnectorConfig.OWNER_CONFIG));
        assertEquals("kafka", configs.get(0).get(GitHubSourceConnectorConfig.REPO_CONFIG));
    }

    @Test
    void config_shouldReturnConfigDef() {
        ConfigDef configDef = connector.config();
        assertNotNull(configDef);
        assertEquals(ConfigDef.Type.PASSWORD,
                configDef.configKeys().get(GitHubSourceConnectorConfig.BEARER_AUTH_TOKEN).type);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private Map<String, String> validConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(GitHubSourceConnectorConfig.TOPIC_CONFIG, "test-topic");
        props.put(GitHubSourceConnectorConfig.OWNER_CONFIG, "apache");
        props.put(GitHubSourceConnectorConfig.REPO_CONFIG, "kafka");
        props.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "10");
        return props;
    }
}
