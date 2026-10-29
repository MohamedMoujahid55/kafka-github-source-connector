package com.simo.mjd.github.source.connector.source;

import com.simo.mjd.github.source.connector.config.GitHubSourceConnectorConfig;
import com.simo.mjd.github.source.connector.rest.GitHubAPIHttpClient;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONArray;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GitHubSourceTaskTest {

    private GitHubSourceTask task;
    private Map<String, String> configProps;

    private static final String SAMPLE_ISSUE_JSON = """
            {
              "url": "https://api.github.com/repos/apache/kafka/issues/100",
              "number": 100,
              "title": "Fix consumer lag metric",
              "state": "open",
              "created_at": "2026-01-01T12:00:00Z",
              "updated_at": "2026-01-02T15:30:00Z",
              "user": {
                "id": 9999999999,
                "login": "testuser",
                "url": "https://api.github.com/users/testuser"
              },
              "pull_request": {
                "url": "https://api.github.com/repos/apache/kafka/pulls/100",
                "html_url": "https://github.com/apache/kafka/pull/100"
              }
            }
            """;

    @BeforeEach
    void setUp() {
        task = new GitHubSourceTask();
        configProps = new HashMap<>();
        configProps.put(GitHubSourceConnectorConfig.TOPIC_CONFIG, "github-issues");
        configProps.put(GitHubSourceConnectorConfig.OWNER_CONFIG, "apache");
        configProps.put(GitHubSourceConnectorConfig.REPO_CONFIG, "kafka");
        configProps.put(GitHubSourceConnectorConfig.SINCE_CONFIG, "2026-01-01T00:00:00Z");
        configProps.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "100");
    }

    @Test
    @DisplayName("Should build valid SourceRecords conforming to schemas without DataException")
    void shouldProduceValidSourceRecord() throws Exception {
        task.start(configProps);

        // Inject mock HTTP client returning 1 issue
        GitHubAPIHttpClient mockClient = new GitHubAPIHttpClient(new GitHubSourceConnectorConfig(configProps)) {
            @Override
            public JSONArray getNextIssues(Integer page, Instant since) {
                return new JSONArray("[" + SAMPLE_ISSUE_JSON + "]");
            }

            @Override
            public void sleep() {}

            @Override
            public void sleepIfNeed() {}
        };

        Field clientField = GitHubSourceTask.class.getDeclaredField("gitHubAPIHttpClient");
        clientField.setAccessible(true);
        clientField.set(task, mockClient);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(1, records.size());

        SourceRecord record = records.get(0);
        assertEquals("github-issues", record.topic());

        // Check key
        Struct keyStruct = (Struct) record.key();
        assertEquals("apache", keyStruct.get("owner"));
        assertEquals("kafka", keyStruct.get("repository"));
        assertEquals(100, keyStruct.get("number"));

        // Check value
        Struct valueStruct = (Struct) record.value();
        assertEquals("Fix consumer lag metric", valueStruct.get("title"));
        assertEquals(100, valueStruct.get("number"));

        Struct userStruct = (Struct) valueStruct.get("user");
        assertNotNull(userStruct);
        assertEquals(9999999999L, userStruct.get("id"));
        assertEquals("testuser", userStruct.get("login"));

        // Check source partition & offset
        assertEquals("apache", record.sourcePartition().get("owner"));
        assertEquals("kafka", record.sourcePartition().get("repository"));
        assertNotNull(record.sourceOffset().get("updated_at"));
    }

    @Test
    @DisplayName("Should handle empty results gracefully without NullPointerException")
    void shouldHandleEmptyResultsWithoutNpe() throws Exception {
        task.start(configProps);

        GitHubAPIHttpClient emptyClient = new GitHubAPIHttpClient(new GitHubSourceConnectorConfig(configProps)) {
            @Override
            public JSONArray getNextIssues(Integer page, Instant since) {
                return new JSONArray();
            }

            @Override
            public void sleep() {}

            @Override
            public void sleepIfNeed() {}
        };

        Field clientField = GitHubSourceTask.class.getDeclaredField("gitHubAPIHttpClient");
        clientField.setAccessible(true);
        clientField.set(task, emptyClient);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertTrue(records.isEmpty());
    }
}
