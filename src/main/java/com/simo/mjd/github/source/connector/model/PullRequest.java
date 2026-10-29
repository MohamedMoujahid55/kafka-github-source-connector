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

/**
 * Data model representing the nested pull request metadata within a GitHub Issue.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PullRequest {

    @JsonProperty("url")
    private String url;

    @JsonProperty("html_url")
    private String htmlUrl;

    @JsonProperty("diff_url")
    private String diffUrl;

    @JsonProperty("patch_url")
    private String patchUrl;

    @JsonProperty("merged_at")
    private Instant mergedAt;

    /**
     * Deserializes a raw JSON string into a {@link PullRequest} instance.
     *
     * @param json raw JSON string
     * @return deserialized {@link PullRequest}
     * @throws RuntimeException if deserialization fails
     */
    public static PullRequest fromJson(String json) {
        try {
            return JsonUtil.getObjectMapper().readValue(json, PullRequest.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize PullRequest from JSON: " + json, e);
        }
    }

    /**
     * Converts a Jackson {@link JsonNode} into a {@link PullRequest} instance.
     *
     * @param jsonNode parsed JSON tree node
     * @return deserialized {@link PullRequest}
     */
    public static PullRequest fromJson(JsonNode jsonNode) {
        return JsonUtil.getObjectMapper().convertValue(jsonNode, PullRequest.class);
    }
}
