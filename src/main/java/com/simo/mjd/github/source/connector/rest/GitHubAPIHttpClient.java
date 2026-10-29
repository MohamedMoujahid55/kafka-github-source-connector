package com.simo.mjd.github.source.connector.rest;

import com.simo.mjd.github.source.connector.config.GitHubSourceConnectorConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * GitHub API HTTP Client implemented using standard Java {@link HttpClient}.
 * Handles Bearer token authentication, rate limiting headers, and pagination.
 */
public class GitHubAPIHttpClient {

    private static final Logger log = LoggerFactory.getLogger(GitHubAPIHttpClient.class);

    /** Header name for maximum allowed requests per hour. */
    public static final String X_RATE_LIMIT_LIMIT_HEADER = "X-RateLimit-Limit";
    /** Header name for remaining allowed requests in current window. */
    public static final String X_RATE_LIMIT_REMAINING_HEADER = "X-RateLimit-Remaining";
    /** Header name for rate limit reset timestamp (UTC epoch seconds). */
    public static final String X_RATE_LIMIT_RESET_HEADER = "X-RateLimit-Reset";

    private final GitHubSourceConnectorConfig config;
    private final HttpClient httpClient;

    // GitHub Rate Limit state
    private int xRateLimit = 9999;
    private int xRateRemaining = 9999;
    private long xRateReset = Instant.MAX.getEpochSecond();

    /**
     * Constructs a client using the provided configuration and default HTTP/2 client.
     *
     * @param config connector configuration
     */
    public GitHubAPIHttpClient(GitHubSourceConnectorConfig config) {
        this(config, HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build());
    }

    /**
     * Constructor for dependency injection and custom {@link HttpClient} testing.
     *
     * @param config     connector configuration
     * @param httpClient configured HTTP client
     */
    public GitHubAPIHttpClient(GitHubSourceConnectorConfig config, HttpClient httpClient) {
        this.config = config;
        this.httpClient = httpClient;
    }

    /**
     * Fetches the next batch of issues from GitHub.
     *
     * @param page  page number (1-based index)
     * @param since filter issues updated on or after this timestamp
     * @return {@link JSONArray} of issues
     * @throws InterruptedException if thread sleep is interrupted
     */
    public JSONArray getNextIssues(Integer page, Instant since) throws InterruptedException {
        HttpResponse<String> response;
        try {
            response = getNextIssuesAPI(page, since);

            // Update rate limit status from response headers
            updateRateLimits(response.headers());

            int statusCode = response.statusCode();
            return switch (statusCode) {
                case 200 -> new JSONArray(response.body());
                case 401 -> throw new ConnectException("Bad GitHub credentials provided, please check your bearer token.");
                case 403, 429 -> {
                    // GitHub Rate limit reached or secondary rate limit triggered
                    logRateLimitWarning(response.body());
                    long sleepTime = Math.max(xRateReset - Instant.now().getEpochSecond(), 1);
                    log.info("Rate limit reset at {}. Sleeping for {} seconds before retrying",
                            LocalDateTime.ofInstant(Instant.ofEpochSecond(xRateReset), ZoneId.systemDefault()),
                            sleepTime);
                    TimeUnit.SECONDS.sleep(sleepTime);
                    yield getNextIssues(page, since);
                }
                default -> {
                    log.error("Unexpected response from {}: status={}, body={}, headers={}",
                            constructUrl(page, since),
                            statusCode,
                            response.body(),
                            response.headers().map());
                    log.error("Unknown error: Sleeping 5 seconds before retrying...");
                    TimeUnit.SECONDS.sleep(5);
                    yield getNextIssues(page, since);
                }
            };
        } catch (IOException e) {
            log.error("I/O error while calling GitHub API at page={} since={}: {}", page, since, e.getMessage(), e);
            TimeUnit.SECONDS.sleep(5);
            return new JSONArray();
        }
    }

    /**
     * Builds the HTTP GET request with all required headers and parameters.
     *
     * @param page  page number
     * @param since update timestamp filter
     * @return constructed {@link HttpRequest}
     */
    protected HttpRequest buildRequest(Integer page, Instant since) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(constructUri(page, since))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "Kafka-Connect-GitHub-Source")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .GET();

        // Bearer token authorization
        String token = config.getBearerAuthToken();
        if (token != null && !token.isBlank()) {
            String authHeader = token.startsWith("Bearer ") ? token : "Bearer " + token.trim();
            requestBuilder.header("Authorization", authHeader);
        }

        return requestBuilder.build();
    }

    /**
     * Sends the HTTP GET request using Java {@link HttpClient}.
     *
     * @param page  page number
     * @param since update timestamp filter
     * @return {@link HttpResponse} string response
     * @throws IOException          on transport error
     * @throws InterruptedException if interrupted
     */
    protected HttpResponse<String> getNextIssuesAPI(Integer page, Instant since) throws IOException, InterruptedException {
        HttpRequest request = buildRequest(page, since);
        log.debug("Executing GET {}", request.uri());
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    /**
     * Constructs the target GitHub issues API {@link URI}.
     *
     * @param page  page number
     * @param since update timestamp filter
     * @return target URI
     */
    protected URI constructUri(Integer page, Instant since) {
        String sinceParam = since != null ? since.toString() : Instant.EPOCH.toString();
        String query = "page=%d&per_page=%d&since=%s&state=all&direction=asc&sort=updated".formatted(
                page,
                config.getBatchSize(),
                URLEncoder.encode(sinceParam, StandardCharsets.UTF_8));

        String encodedOwner = URLEncoder.encode(config.getOwner(), StandardCharsets.UTF_8);
        String encodedRepo = URLEncoder.encode(config.getRepo(), StandardCharsets.UTF_8);

        return URI.create("https://api.github.com/repos/%s/%s/issues?%s".formatted(
                encodedOwner,
                encodedRepo,
                query));
    }

    /**
     * Helper returning URL string for backward-compatibility and logging.
     *
     * @param page  page number
     * @param since update timestamp filter
     * @return formatted URL string
     */
    protected String constructUrl(Integer page, Instant since) {
        return constructUri(page, since).toString();
    }

    /**
     * Extracts GitHub rate limit headers safely.
     *
     * @param headers HTTP response headers
     */
    private void updateRateLimits(HttpHeaders headers) {
        headers.firstValue(X_RATE_LIMIT_LIMIT_HEADER)
                .flatMap(this::safeParseInt)
                .ifPresent(val -> this.xRateLimit = val);

        headers.firstValue(X_RATE_LIMIT_REMAINING_HEADER)
                .flatMap(this::safeParseInt)
                .ifPresent(val -> this.xRateRemaining = val);

        headers.firstValue(X_RATE_LIMIT_RESET_HEADER)
                .flatMap(this::safeParseLong)
                .ifPresent(val -> this.xRateReset = val);
    }

    private Optional<Integer> safeParseInt(String val) {
        try {
            return Optional.of(Integer.parseInt(val));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private Optional<Long> safeParseLong(String val) {
        try {
            return Optional.of(Long.parseLong(val));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private void logRateLimitWarning(String body) {
        try {
            JSONObject json = new JSONObject(body);
            if (json.has("message")) {
                log.warn("GitHub rate limit response message: {}", json.getString("message"));
            }
        } catch (Exception ignored) {
            log.warn("GitHub 403/429 response body: {}", body);
        }
        log.info("Your rate limit is {}", xRateLimit);
        log.info("Your remaining calls: {}", xRateRemaining);
    }

    /**
     * Sleeps dynamically to space out requests and respect rate limits.
     *
     * @throws InterruptedException if interrupted during sleep
     */
    public void sleep() throws InterruptedException {
        long remainingSeconds = xRateReset - Instant.now().getEpochSecond();
        if (remainingSeconds > 0 && xRateRemaining > 0) {
            long sleepTime = (long) Math.ceil((double) remainingSeconds / xRateRemaining);
            log.debug("Sleeping for {} seconds to throttle requests", sleepTime);
            TimeUnit.SECONDS.sleep(sleepTime);
        }
    }

    /**
     * Checks if remaining requests are low, and sleeps if needed.
     *
     * @throws InterruptedException if interrupted during sleep
     */
    public void sleepIfNeed() throws InterruptedException {
        if (xRateRemaining <= 10 && xRateRemaining > 0) {
            log.info("Approaching rate limit soon, only {} requests left", xRateRemaining);
            sleep();
        }
    }

    /**
     * Returns total rate limit allowed per hour.
     *
     * @return rate limit maximum
     */
    public int getXRateLimit() {
        return xRateLimit;
    }

    /**
     * Returns number of requests remaining in current window.
     *
     * @return remaining requests
     */
    public int getXRateRemaining() {
        return xRateRemaining;
    }

    /**
     * Returns UTC epoch second when rate limit resets.
     *
     * @return reset epoch seconds
     */
    public long getXRateReset() {
        return xRateReset;
    }
}
