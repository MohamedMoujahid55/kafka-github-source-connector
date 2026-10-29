package com.simo.mjd.github.source.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data model representing reaction emoji counts on a GitHub Issue or Pull Request.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Reactions {

    @JsonProperty("url")
    private String url;

    @JsonProperty("total_count")
    private Integer totalCount;

    @JsonProperty("+1")
    private Integer plusOne;

    @JsonProperty("-1")
    private Integer minusOne;

    @JsonProperty("laugh")
    private Integer laugh;

    @JsonProperty("hooray")
    private Integer hooray;

    @JsonProperty("confused")
    private Integer confused;

    @JsonProperty("heart")
    private Integer heart;

    @JsonProperty("rocket")
    private Integer rocket;

    @JsonProperty("eyes")
    private Integer eyes;
}
