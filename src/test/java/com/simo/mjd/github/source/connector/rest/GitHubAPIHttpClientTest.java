package com.simo.mjd.github.source.connector.rest;

import com.simo.mjd.github.source.connector.config.GitHubSourceConnectorConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONArray;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class GitHubAPIHttpClientTest {

    private GitHubSourceConnectorConfig config;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(GitHubSourceConnectorConfig.TOPIC_CONFIG, "test-topic");
        props.put(GitHubSourceConnectorConfig.OWNER_CONFIG, "apache");
        props.put(GitHubSourceConnectorConfig.REPO_CONFIG, "kafka");
        props.put(GitHubSourceConnectorConfig.BEARER_AUTH_TOKEN, "github_pat_test123");
        props.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "50");

        config = new GitHubSourceConnectorConfig(props);
    }

    @Test
    @DisplayName("Should construct correct GitHub API URI with query params")
    void shouldConstructCorrectUri() {
        GitHubAPIHttpClient client = new GitHubAPIHttpClient(config);
        Instant since = Instant.parse("2026-01-01T00:00:00Z");
        URI uri = client.constructUri(1, since);

        assertEquals("api.github.com", uri.getHost());
        assertEquals("/repos/apache/kafka/issues", uri.getPath());
        assertTrue(uri.getQuery().contains("page=1"));
        assertTrue(uri.getQuery().contains("per_page=50"));
        assertTrue(uri.getQuery().contains("since="));
        assertTrue(uri.getQuery().contains("state=all"));
    }

    @Test
    @DisplayName("Should build HttpRequest with Bearer authorization and GitHub headers")
    void shouldBuildRequestWithBearerHeader() {
        GitHubAPIHttpClient client = new GitHubAPIHttpClient(config);
        Instant since = Instant.parse("2026-01-01T00:00:00Z");

        HttpRequest request = client.buildRequest(1, since);

        assertEquals("Bearer github_pat_test123", request.headers().firstValue("Authorization").orElse(null));
        assertEquals("application/vnd.github+json", request.headers().firstValue("Accept").orElse(null));
        assertEquals("Kafka-Connect-GitHub-Source", request.headers().firstValue("User-Agent").orElse(null));
        assertEquals("2022-11-28", request.headers().firstValue("X-GitHub-Api-Version").orElse(null));
    }

    @Test
    @DisplayName("Should parse 200 response body and update rate limit headers")
    void shouldReturnJsonArrayOn200() throws Exception {
        HttpHeaders headers = HttpHeaders.of(
                Map.of(
                        GitHubAPIHttpClient.X_RATE_LIMIT_LIMIT_HEADER, List.of("5000"),
                        GitHubAPIHttpClient.X_RATE_LIMIT_REMAINING_HEADER, List.of("4990"),
                        GitHubAPIHttpClient.X_RATE_LIMIT_RESET_HEADER, List.of("1700000000")
                ),
                (k, v) -> true
        );

        StubHttpResponse stubResponse = new StubHttpResponse(200, "[{\"id\": 101, \"title\": \"Bug fix\"}]", headers);

        GitHubAPIHttpClient client = new GitHubAPIHttpClient(config) {
            @Override
            protected HttpResponse<String> getNextIssuesAPI(Integer page, Instant since) {
                return stubResponse;
            }
        };

        JSONArray issues = client.getNextIssues(1, Instant.now());

        assertNotNull(issues);
        assertEquals(1, issues.length());
        assertEquals(101, issues.getJSONObject(0).getInt("id"));
        assertEquals("Bug fix", issues.getJSONObject(0).getString("title"));

        assertEquals(5000, client.getXRateLimit());
        assertEquals(4990, client.getXRateRemaining());
        assertEquals(1700000000L, client.getXRateReset());
    }

    @Test
    @DisplayName("Should throw ConnectException on 401 Unauthorized")
    void shouldThrowConnectExceptionOn401() {
        HttpHeaders headers = HttpHeaders.of(Map.of(), (k, v) -> true);
        StubHttpResponse stubResponse = new StubHttpResponse(401, "{\"message\": \"Bad credentials\"}", headers);

        GitHubAPIHttpClient client = new GitHubAPIHttpClient(config) {
            @Override
            protected HttpResponse<String> getNextIssuesAPI(Integer page, Instant since) {
                return stubResponse;
            }
        };

        assertThrows(ConnectException.class, () -> client.getNextIssues(1, Instant.now()));
    }

    @Test
    @DisplayName("Should handle network IOException gracefully by returning empty JSONArray")
    void shouldHandleIOExceptionGracefully() throws Exception {
        GitHubAPIHttpClient client = new GitHubAPIHttpClient(config) {
            @Override
            protected HttpResponse<String> getNextIssuesAPI(Integer page, Instant since) throws IOException {
                throw new IOException("Connection reset");
            }
        };

        JSONArray result = client.getNextIssues(1, Instant.now());
        assertNotNull(result);
        assertEquals(0, result.length());
    }

    /**
     * Minimal stub for HttpResponse to avoid mocking JDK internal classes in unit tests.
     */
    private static class StubHttpResponse implements HttpResponse<String> {
        private final int statusCode;
        private final String body;
        private final HttpHeaders headers;

        StubHttpResponse(int statusCode, String body, HttpHeaders headers) {
            this.statusCode = statusCode;
            this.body = body;
            this.headers = headers;
        }

        @Override public int statusCode() { return statusCode; }
        @Override public HttpRequest request() { return null; }
        @Override public Optional<HttpResponse<String>> previousResponse() { return Optional.empty(); }
        @Override public HttpHeaders headers() { return headers; }
        @Override public String body() { return body; }
        @Override public Optional<SSLSession> sslSession() { return Optional.empty(); }
        @Override public URI uri() { return URI.create("https://api.github.com"); }
        @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_2; }
    }
}
