package com.simo.mjd.github.source.connector.model;

import com.simo.mjd.github.source.connector.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.Instant;
import java.util.List;

/**
 * Data model representing a GitHub Issue or Pull Request returned by the GitHub REST API.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Issue {

    @JsonProperty("url")
    private String url;

    @JsonProperty("repository_url")
    private String repositoryUrl;

    @JsonProperty("labels_url")
    private String labelsUrl;

    @JsonProperty("comments_url")
    private String commentsUrl;

    @JsonProperty("events_url")
    private String eventsUrl;

    @JsonProperty("html_url")
    private String htmlUrl;

    @JsonProperty("id")
    private Long id;

    @JsonProperty("node_id")
    private String nodeId;

    @JsonProperty("number")
    private Integer number;

    @JsonProperty("title")
    private String title;

    @JsonProperty("user")
    private User user;

    @JsonProperty("labels")
    private List<Label> labels;

    @JsonProperty("state")
    private String state;

    @JsonProperty("locked")
    private Boolean locked;

    @JsonProperty("assignees")
    private List<User> assignees;

    @JsonProperty("milestone")
    private Milestone milestone;

    @JsonProperty("comments")
    private Integer comments;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    @JsonProperty("closed_at")
    private Instant closedAt;

    @JsonProperty("author_association")
    private String authorAssociation;

    @JsonProperty("issue_field_values")
    private List<Object> issueFieldValues;

    @JsonProperty("type")
    private String type;

    @JsonProperty("active_lock_reason")
    private String activeLockReason;

    @JsonProperty("draft")
    private Boolean draft;

    @JsonProperty("pull_request")
    private PullRequest pullRequest;

    @JsonProperty("body")
    private String body;

    @JsonProperty("closed_by")
    private User closedBy;

    @JsonProperty("reactions")
    private Reactions reactions;

    @JsonProperty("timeline_url")
    private String timelineUrl;

    @JsonProperty("performed_via_github_app")
    private Object performedViaGithubApp;

    @JsonProperty("state_reason")
    private String stateReason;

    /**
     * Fluent setter for pull request details.
     *
     * @param pullRequest the pull request metadata
     * @return this issue instance
     */
    public Issue withPullRequest(PullRequest pullRequest) {
        this.pullRequest = pullRequest;
        return this;
    }

    /**
     * Deserializes a raw JSON string into an {@link Issue} instance.
     *
     * @param json raw JSON string
     * @return deserialized {@link Issue}
     * @throws RuntimeException if deserialization fails
     */
    public static Issue fromJson(String json) {
        try {
            return JsonUtil.getObjectMapper().readValue(json, Issue.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize Issue from JSON: " + json, e);
        }
    }

    /**
     * Converts a Jackson {@link JsonNode} into an {@link Issue} instance.
     *
     * @param jsonNode parsed JSON tree node
     * @return deserialized {@link Issue}
     */
    public static Issue fromJson(JsonNode jsonNode) {
        return JsonUtil.getObjectMapper().convertValue(jsonNode, Issue.class);
    }
}
