package com.simo.mjd.github.source.connector.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class IssueDeserializationTest {

    private static final String JSON = """
            {
              "url": "https://api.github.com/repos/kubernetes/kubernetes/issues/143325",
              "repository_url": "https://api.github.com/repos/kubernetes/kubernetes",
              "labels_url": "https://api.github.com/repos/kubernetes/kubernetes/issues/143325/labels{/name}",
              "comments_url": "https://api.github.com/repos/kubernetes/kubernetes/issues/143325/comments",
              "events_url": "https://api.github.com/repos/kubernetes/kubernetes/issues/143325/events",
              "html_url": "https://github.com/kubernetes/kubernetes/pull/143325",
              "id": 5984152187,
              "node_id": "PR_kwDOAToIks8AAAABKFCatg",
              "number": 143325,
              "title": "Track compaction RV in snapshotter and keep last snapshot behind compaction window",
              "user": {
                "login": "serathius",
                "id": 5873433,
                "node_id": "MDQ6VXNlcjU4NzM0MzM=",
                "avatar_url": "https://avatars.githubusercontent.com/u/5873433?v=4",
                "gravatar_id": "",
                "url": "https://api.github.com/users/serathius",
                "html_url": "https://github.com/serathius",
                "followers_url": "https://api.github.com/users/serathius/followers",
                "following_url": "https://api.github.com/users/serathius/following{/other_user}",
                "gists_url": "https://api.github.com/users/serathius/gists{/gist_id}",
                "starred_url": "https://api.github.com/users/serathius/starred{/owner}{/repo}",
                "subscriptions_url": "https://api.github.com/users/serathius/subscriptions",
                "organizations_url": "https://api.github.com/users/serathius/orgs",
                "repos_url": "https://api.github.com/users/serathius/repos",
                "events_url": "https://api.github.com/users/serathius/events{/privacy}",
                "received_events_url": "https://api.github.com/users/serathius/received_events",
                "type": "User",
                "user_view_type": "public",
                "site_admin": false
              },
              "labels": [
                {
                  "id": 136601536,
                  "node_id": "MDU6TGFiZWwxMzY2MDE1MzY=",
                  "url": "https://api.github.com/repos/kubernetes/kubernetes/labels/area/apiserver",
                  "name": "area/apiserver",
                  "color": "0052cc",
                  "default": false,
                  "description": null,
                  "archived_at": null,
                  "archived_by": null
                }
              ],
              "state": "open",
              "locked": false,
              "assignees": [],
              "milestone": null,
              "comments": 4,
              "created_at": "2026-10-22T17:19:57Z",
              "updated_at": "2026-10-22T19:00:45Z",
              "closed_at": null,
              "author_association": "MEMBER",
              "issue_field_values": [],
              "type": null,
              "active_lock_reason": null,
              "draft": false,
              "pull_request": {
                "url": "https://api.github.com/repos/kubernetes/kubernetes/pulls/143325",
                "html_url": "https://github.com/kubernetes/kubernetes/pull/143325",
                "diff_url": "https://github.com/kubernetes/kubernetes/pull/143325.diff",
                "patch_url": "https://github.com/kubernetes/kubernetes/pull/143325.patch",
                "merged_at": null
              },
              "body": "/kind feature",
              "closed_by": null,
              "reactions": {
                "url": "https://api.github.com/repos/kubernetes/kubernetes/issues/143325/reactions",
                "total_count": 0,
                "+1": 0,
                "-1": 0,
                "laugh": 0,
                "hooray": 0,
                "confused": 0,
                "heart": 0,
                "rocket": 0,
                "eyes": 0
              },
              "timeline_url": "https://api.github.com/repos/kubernetes/kubernetes/issues/143325/timeline",
              "performed_via_github_app": null,
              "state_reason": null
            }
            """;

    @Test
    void shouldDeserializeIssueViaObjectMapper() {
        Issue issue = Issue.fromJson(JSON);

        assertNotNull(issue);
        assertEquals(5984152187L, issue.getId());
        assertEquals(143325, issue.getNumber());
        assertEquals("open", issue.getState());
        assertFalse(issue.getLocked());
        assertFalse(issue.getDraft());
        assertEquals(4, issue.getComments());
        assertEquals("Track compaction RV in snapshotter and keep last snapshot behind compaction window", issue.getTitle());
        assertEquals(Instant.parse("2026-10-22T17:19:57Z"), issue.getCreatedAt());

        // Nested User
        assertNotNull(issue.getUser());
        assertEquals("serathius", issue.getUser().getLogin());
        assertEquals(5873433L, issue.getUser().getId());
        assertEquals("User", issue.getUser().getType());
        assertFalse(issue.getUser().getSiteAdmin());

        // Nested Label
        assertNotNull(issue.getLabels());
        assertEquals(1, issue.getLabels().size());
        Label label = issue.getLabels().get(0);
        assertEquals(136601536L, label.getId());
        assertEquals("area/apiserver", label.getName());
        assertEquals("0052cc", label.getColor());
        assertFalse(label.getIsDefault());

        // Nested PullRequest
        assertNotNull(issue.getPullRequest());
        assertEquals("https://api.github.com/repos/kubernetes/kubernetes/pulls/143325", issue.getPullRequest().getUrl());
        assertNull(issue.getPullRequest().getMergedAt());

        // Nested Reactions
        assertNotNull(issue.getReactions());
        assertEquals(0, issue.getReactions().getTotalCount());
        assertEquals(0, issue.getReactions().getPlusOne());
        assertEquals(0, issue.getReactions().getMinusOne());
    }

    @Test
    void shouldDeserializePullRequestViaObjectMapper() {
        String prJson = """
                {
                  "url": "https://api.github.com/repos/kubernetes/kubernetes/pulls/143325",
                  "html_url": "https://github.com/kubernetes/kubernetes/pull/143325",
                  "diff_url": "https://github.com/kubernetes/kubernetes/pull/143325.diff",
                  "patch_url": "https://github.com/kubernetes/kubernetes/pull/143325.patch",
                  "merged_at": null
                }
                """;

        PullRequest pr = PullRequest.fromJson(prJson);
        assertNotNull(pr);
        assertEquals("https://api.github.com/repos/kubernetes/kubernetes/pulls/143325", pr.getUrl());
        assertEquals("https://github.com/kubernetes/kubernetes/pull/143325", pr.getHtmlUrl());
        assertNull(pr.getMergedAt());
    }
}
