package com.simo.mjd.github.source.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

/**
 * Data model representing a GitHub user assigned to an issue.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Assignee extends User {
    private Map<String, Object> additionalProperties = new HashMap<>();
}
