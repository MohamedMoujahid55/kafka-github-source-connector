package com.simo.mjd.github.source.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Data model representing a GitHub Label tag applied to an issue.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Label {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("node_id")
    private String nodeId;

    @JsonProperty("url")
    private String url;

    @JsonProperty("name")
    private String name;

    @JsonProperty("color")
    private String color;

    @JsonProperty("default")
    private Boolean isDefault;

    @JsonProperty("description")
    private String description;

    @JsonProperty("archived_at")
    private Instant archivedAt;

    @JsonProperty("archived_by")
    private String archivedBy;
}
