package com.simo.mjd.github.source.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Data model representing a GitHub Milestone grouping issues and pull requests.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Milestone {

    @JsonProperty("url")
    private String url;

    @JsonProperty("html_url")
    private String htmlUrl;

    @JsonProperty("labels_url")
    private String labelsUrl;

    @JsonProperty("id")
    private Long id;

    @JsonProperty("node_id")
    private String nodeId;

    @JsonProperty("number")
    private Integer number;

    @JsonProperty("state")
    private String state;

    @JsonProperty("title")
    private String title;

    @JsonProperty("description")
    private String description;

    @JsonProperty("creator")
    private User creator;

    @JsonProperty("open_issues")
    private Integer openIssues;

    @JsonProperty("closed_issues")
    private Integer closedIssues;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    @JsonProperty("closed_at")
    private Instant closedAt;

    @JsonProperty("due_on")
    private Instant dueOn;
}
