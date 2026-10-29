package com.simo.mjd.github.source.connector.source;

import com.simo.mjd.github.source.connector.config.GitHubSourceConnectorConfig;
import com.simo.mjd.github.source.connector.model.Issue;
import com.simo.mjd.github.source.connector.model.PullRequest;
import com.simo.mjd.github.source.connector.model.User;
import com.simo.mjd.github.source.connector.rest.GitHubAPIHttpClient;
import com.simo.mjd.github.source.connector.util.VersionUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.simo.mjd.github.source.connector.schema.GitHubSchema.*;

/**
 * GitHubSourceTask – polls GitHub REST API for issues and pull requests,
 * converts them into Kafka Connect {@link SourceRecord} instances with strict schemas,
 * and tracks incremental offsets using {@code updated_at} and issue numbers.
 */
public class GitHubSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(GitHubSourceTask.class);

    // ── State ────────────────────────────────────────────────────────────────
    private String topic;
    protected Instant nextQuerySince;
    protected Integer lastIssueNumber = -1;
    protected Integer nextPageToVisit = 1;
    private Instant lastUpdatedAt;

    private GitHubAPIHttpClient gitHubAPIHttpClient;
    private GitHubSourceConnectorConfig config;

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /**
     * Initializes the task with configuration properties, restores previous offsets,
     * and sets up the GitHub HTTP client.
     *
     * @param props task configuration properties
     */
    @Override
    public void start(Map<String, String> props) {
        config = new GitHubSourceConnectorConfig(props);
        topic = config.getTopic();
        initData();
        gitHubAPIHttpClient = new GitHubAPIHttpClient(config);

        log.info("Starting GitHubSourceTask – topic={}, owner={}, repo={}, since={}",
                topic, config.getOwner(), config.getRepo(), nextQuerySince);
    }

    /**
     * Shuts down the task.
     */
    @Override
    public void stop() {
        log.info("Stopping GitHubSourceTask");
    }

    // ── Polling ──────────────────────────────────────────────────────────────

    /**
     * Polls the GitHub REST API for issues and returns a list of {@link SourceRecord} objects.
     *
     * @return list of source records to send to Kafka, or empty list if no new records
     * @throws InterruptedException if thread sleeping or waiting is interrupted
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        gitHubAPIHttpClient.sleepIfNeed();

        final ArrayList<SourceRecord> sourceRecords = new ArrayList<>();
        JSONArray issues = gitHubAPIHttpClient.getNextIssues(nextPageToVisit, nextQuerySince);

        int count = 0;
        for (Object obj : issues) {
            Issue issue = Issue.fromJson(String.valueOf(obj));
            SourceRecord sourceRecord = getSourceRecord(issue);

            sourceRecords.add(sourceRecord);
            count += 1;
            lastUpdatedAt = issue.getUpdatedAt();
            lastIssueNumber = issue.getNumber();
        }

        if (count == config.getBatchSize()) {
            // Full batch reached: continue to next page for the current time window
            nextPageToVisit += 1;
        } else {
            // Less than full batch (or empty): all current updates fetched
            if (lastUpdatedAt != null) {
                nextQuerySince = lastUpdatedAt.plusSeconds(1);
            }
            nextPageToVisit = 1;
            gitHubAPIHttpClient.sleep();
        }

        return sourceRecords;
    }

    /**
     * Constructs a Kafka Connect {@link SourceRecord} from a deserialized {@link Issue}.
     *
     * @param issue the GitHub issue entity
     * @return populated {@link SourceRecord}
     */
    private SourceRecord getSourceRecord(Issue issue) {
        Map<String, ?> sourcePartition = sourcePartition();
        Map<String, ?> sourceOffset = sourceOffset(issue.getUpdatedAt(), issue.getNumber(), nextPageToVisit);

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                KEY_SCHEMA,
                buildRecordKey(issue),
                VALUE_SCHEMA,
                buildRecordValue(issue),
                issue.getUpdatedAt().toEpochMilli()
        );
    }

    /**
     * Generates the partition map identifying the tracked repository.
     *
     * @return partition map with owner and repository
     */
    private Map<String, String> sourcePartition() {
        Map<String, String> partition = new HashMap<>();
        partition.put(OWNER_FIELD, config.getOwner());
        partition.put(REPOSITORY_FIELD, config.getRepo());
        return partition;
    }

    /**
     * Generates the offset map storing the current progress.
     *
     * @param updatedAt   latest issue update timestamp
     * @param issueNumber latest issue number
     * @param nextPage    next page number to query
     * @return offset map
     */
    private Map<String, Object> sourceOffset(Instant updatedAt, Integer issueNumber, Integer nextPage) {
        Map<String, Object> offset = new HashMap<>();
        offset.put(UPDATED_AT_FIELD, updatedAt.toString());
        offset.put(NUMBER_FIELD, issueNumber);
        offset.put(NEXT_PAGE_FIELD, nextPage);
        return offset;
    }

    /**
     * Builds the record value {@link Struct} according to {@link com.simo.mjd.github.source.connector.schema.GitHubSchema#VALUE_SCHEMA}.
     *
     * @param issue the GitHub issue
     * @return record value struct
     */
    private Struct buildRecordValue(Issue issue) {
        Struct valueStruct = new Struct(VALUE_SCHEMA)
                .put(URL_FIELD, issue.getUrl())
                .put(TITLE_FIELD, issue.getTitle())
                .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
                .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
                .put(NUMBER_FIELD, issue.getNumber())
                .put(STATE_FIELD, issue.getState());

        // User is optional in case of deleted/ghost accounts
        User user = issue.getUser();
        if (user != null) {
            Struct userStruct = new Struct(USER_SCHEMA)
                    .put(USER_URL_FIELD, user.getUrl())
                    .put(USER_ID_FIELD, user.getId())
                    .put(USER_LOGIN_FIELD, user.getLogin());
            valueStruct.put(USER_FIELD, userStruct);
        }

        // Pull request is optional
        PullRequest pullRequest = issue.getPullRequest();
        if (pullRequest != null) {
            Struct prStruct = new Struct(PR_SCHEMA)
                    .put(PR_URL_FIELD, pullRequest.getUrl())
                    .put(PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
            valueStruct.put(PR_FIELD, prStruct);
        }

        return valueStruct;
    }

    /**
     * Builds the record key {@link Struct} according to {@link com.simo.mjd.github.source.connector.schema.GitHubSchema#KEY_SCHEMA}.
     *
     * @param issue the GitHub issue
     * @return record key struct
     */
    private Struct buildRecordKey(Issue issue) {
        return new Struct(KEY_SCHEMA)
                .put(OWNER_FIELD, config.getOwner())
                .put(REPOSITORY_FIELD, config.getRepo())
                .put(NUMBER_FIELD, issue.getNumber());
    }

    // ── Metadata ─────────────────────────────────────────────────────────────

    /**
     * Returns task implementation version.
     *
     * @return version string
     */
    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    /**
     * Restores offset state from Connect's offset storage reader or initializes defaults.
     */
    private void initData() {
        Map<String, Object> offsetData = null;
        if (context != null && context.offsetStorageReader() != null) {
            offsetData = context.offsetStorageReader().offset(sourcePartition());
        }

        if (offsetData == null) {
            nextQuerySince = config.getSince();
            lastIssueNumber = -1;
            nextPageToVisit = 1;
        } else {
            Object updatedAt = offsetData.get(UPDATED_AT_FIELD);
            Object issueNumber = offsetData.get(NUMBER_FIELD);
            Object nextPage = offsetData.get(NEXT_PAGE_FIELD);

            if (updatedAt instanceof String updatedAtString) {
                nextQuerySince = Instant.parse(updatedAtString);
                lastUpdatedAt = nextQuerySince;
            }
            if (issueNumber instanceof Number n) {
                lastIssueNumber = n.intValue();
            } else if (issueNumber instanceof String lastIssueNumberString) {
                lastIssueNumber = Integer.valueOf(lastIssueNumberString);
            }
            if (nextPage instanceof Number n) {
                nextPageToVisit = n.intValue();
            } else if (nextPage instanceof String nextPageString) {
                nextPageToVisit = Integer.valueOf(nextPageString);
            }
        }
    }
}
