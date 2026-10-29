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

/**
 * Data model representing a GitHub User or Bot account.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

    @JsonProperty("login")
    private String login;

    @JsonProperty("id")
    private Long id;

    @JsonProperty("node_id")
    private String nodeId;

    @JsonProperty("avatar_url")
    private String avatarUrl;

    @JsonProperty("gravatar_id")
    private String gravatarId;

    @JsonProperty("url")
    private String url;

    @JsonProperty("html_url")
    private String htmlUrl;

    @JsonProperty("followers_url")
    private String followersUrl;

    @JsonProperty("following_url")
    private String followingUrl;

    @JsonProperty("gists_url")
    private String gistsUrl;

    @JsonProperty("starred_url")
    private String starredUrl;

    @JsonProperty("subscriptions_url")
    private String subscriptionsUrl;

    @JsonProperty("organizations_url")
    private String organizationsUrl;

    @JsonProperty("repos_url")
    private String reposUrl;

    @JsonProperty("events_url")
    private String eventsUrl;

    @JsonProperty("received_events_url")
    private String receivedEventsUrl;

    @JsonProperty("type")
    private String type;

    @JsonProperty("user_view_type")
    private String userViewType;

    @JsonProperty("site_admin")
    private Boolean siteAdmin;

    /**
     * Deserializes a raw JSON string into a {@link User} instance.
     *
     * @param json raw JSON string
     * @return deserialized {@link User}
     * @throws RuntimeException if deserialization fails
     */
    public static User fromJson(String json) {
        try {
            return JsonUtil.getObjectMapper().readValue(json, User.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize User from JSON: " + json, e);
        }
    }

    /**
     * Converts a Jackson {@link JsonNode} into a {@link User} instance.
     *
     * @param jsonNode parsed JSON tree node
     * @return deserialized {@link User}
     */
    public static User fromJson(JsonNode jsonNode) {
        return JsonUtil.getObjectMapper().convertValue(jsonNode, User.class);
    }
}
