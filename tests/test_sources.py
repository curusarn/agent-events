from __future__ import annotations

import copy
import os
import time
import unittest
from unittest import mock

from agent_events.sources import TopicError, parse
from agent_events.sources.base import SourceError
from agent_events.sources import docs, github, linear, slack


class ParseTests(unittest.TestCase):
    def topic(self, raw):
        source, parts = parse(raw)
        return "/".join([source.type] + parts)

    def test_github(self):
        self.assertEqual(self.topic("github/Curusarn/Agent-Events/12"), "github/curusarn/agent-events/12")
        self.assertEqual(self.topic("github/o/r/pull/12"), "github/o/r/12")
        self.assertEqual(self.topic("https://github.com/o/r/pull/12/files#r1"), "github/o/r/12")
        self.assertRaises(TopicError, parse, "github/o/r")
        self.assertRaises(TopicError, parse, "github/o/r/abc")

    def test_linear(self):
        self.assertEqual(self.topic("linear/t-123"), "linear/T-123")
        self.assertEqual(self.topic("linear/T-123/5C3BF24B"), "linear/T-123/5c3bf24b")
        self.assertEqual(self.topic("https://linear.app/ws/issue/T-123/some-slug"), "linear/T-123")
        self.assertEqual(self.topic("https://linear.app/ws/issue/T-123/some-slug#comment-5c3bf24b"),
                         "linear/T-123/5c3bf24b")
        self.assertRaises(TopicError, parse, "linear/123")

    def test_slack(self):
        self.assertEqual(self.topic("slack/c0123abcd"), "slack/C0123ABCD")
        self.assertEqual(self.topic("slack/C0123ABCD/p1712345678123456"), "slack/C0123ABCD/1712345678.123456")
        self.assertEqual(self.topic("https://x.slack.com/archives/C0123ABCD/p1712345678123456"),
                         "slack/C0123ABCD/1712345678.123456")
        self.assertEqual(self.topic("https://x.slack.com/archives/C0123ABCD/p1712345699000100"
                                    "?thread_ts=1712345678.123456&cid=C0123ABCD"),
                         "slack/C0123ABCD/1712345678.123456")
        self.assertEqual(self.topic("https://x.slack.com/archives/C0123ABCD"), "slack/C0123ABCD")
        self.assertRaises(TopicError, parse, "slack/C0123ABCD/notats")

    def test_mc(self):
        self.assertEqual(self.topic("hf/agent/0a1b2c3d4e"), "hf/agent/0a1b2c3d4e")
        self.assertEqual(self.topic("/MC/Agent/Some_Id-1.x/"), "hf/agent/Some_Id-1.x")
        self.assertEqual(self.topic("hf/thread/t1"), "hf/thread/t1")
        self.assertTrue(parse("hf/agent/x")[0].push)
        self.assertFalse(parse("github/o/r/1")[0].push)
        for bad in ("mc/foo", "mc/foo/x", "hf/agent", "hf/agent/a b", "hf/agent/x/extra", "hf/agent/a:b"):
            with self.assertRaises(TopicError, msg=bad) as cm:
                parse(bad)
            self.assertIn("hf/agent/<id>", str(cm.exception))

    def test_docs(self):
        self.assertEqual(self.topic("docs/U-9225"), "docs/U-9225")
        self.assertEqual(self.topic("docs/U-9225/docs-uptime-monitor"), "docs/U-9225/docs-uptime-monitor")
        self.assertEqual(self.topic("docs/docs-maint~docs-fix~x-20260924T081318Z"), "docs/docs-maint~docs-fix~x-20260924T081318Z")
        self.assertEqual(self.topic("https://betterstack.com/admin/articles/unpublished/docs-bot/x-1?tab=diff"),
                         "docs/docs-bot~x-1")
        self.assertEqual(self.topic("https://betterstack.com/admin/articles/unpublished/uptime%23123"), "docs/uptime#123")
        self.assertRaises(TopicError, parse, "docs")
        self.assertRaises(TopicError, parse, "docs/a/b/c")
        self.assertRaises(TopicError, parse, "docs/a b")

    def test_unknown(self):
        self.assertRaises(TopicError, parse, "https://example.com/x")
        self.assertRaises(TopicError, parse, "jira/X-1")


def gh_snapshot(**over):
    s = {
        "viewer": "me", "title": "Fix it", "url": "https://github.com/o/r/pull/1", "body": "desc",
        "state": "OPEN", "draft": False, "base": "main", "head_ref": "fix", "mergeable": "MERGEABLE",
        "review_decision": "REVIEW_REQUIRED", "author": "me", "merged_by": None,
        "assignees": [], "labels": [], "reviewers": ["bob"], "head": "aaa",
        "commits": [{"oid": "aaa", "headline": "first"}], "total_commits": 1,
        "checks": {"test": {"s": "IN_PROGRESS", "url": "u"}}, "rollup": "PENDING",
        "comments": {}, "reviews": {}, "threads": {},
    }
    s.update(over)
    return s


class GitHubDiffTests(unittest.TestCase):
    def kinds(self, old, new):
        return [(e.kind, e.summary) for e in github.diff(old, new, "github/o/r/1")]

    def test_no_change_no_events(self):
        self.assertEqual(self.kinds(gh_snapshot(), gh_snapshot()), [])

    def test_ci(self):
        old = gh_snapshot()
        new = gh_snapshot(checks={"test": {"s": "FAILURE", "url": "u"}}, rollup="FAILURE")
        self.assertEqual(self.kinds(old, new), [
            ("check_failed", "check failed: test (failure)"),
            ("checks", "CI failure: 0 passed, 1 failed, 1 total"),
        ])
        green = gh_snapshot(checks={"test": {"s": "SUCCESS", "url": "u"}}, rollup="SUCCESS")
        self.assertEqual([k for k, _ in self.kinds(old, green)], ["checks"])

    def test_shadow_checks_are_ignored(self):
        old = gh_snapshot(checks={"test": {"s": "IN_PROGRESS", "url": "u"},
                                  "CI (shadow): test": {"s": "IN_PROGRESS", "url": "u"}})
        # Only the shadow run failed: GitHub's rollup says failure, the PR's CI passed.
        new = gh_snapshot(checks={"test": {"s": "SUCCESS", "url": "u"},
                                  "CI (shadow): test": {"s": "FAILURE", "url": "u"}}, rollup="FAILURE")
        self.assertEqual(self.kinds(old, new), [("checks", "CI success: 1 passed, 0 failed, 1 total")])
        # The real check still running: no result yet.
        running = gh_snapshot(checks={"test": {"s": "IN_PROGRESS", "url": "u"},
                                      "CI (shadow): test": {"s": "FAILURE", "url": "u"}}, rollup="FAILURE")
        self.assertEqual(self.kinds(old, running), [])
        both = gh_snapshot(checks={"test": {"s": "FAILURE", "url": "u"},
                                   "CI (shadow): test": {"s": "FAILURE", "url": "u"}}, rollup="FAILURE")
        self.assertEqual(self.kinds(old, both), [("check_failed", "check failed: test (failure)"),
                                                 ("checks", "CI failure: 0 passed, 1 failed, 1 total")])

    def test_push_and_force_push(self):
        pushed = gh_snapshot(head="bbb", commits=[{"oid": "aaa", "headline": "first"},
                                                   {"oid": "bbb", "headline": "second"}], total_commits=2)
        self.assertEqual(self.kinds(gh_snapshot(), pushed)[0], ("push", "1 new commit pushed to fix"))
        forced = gh_snapshot(head="ccc", commits=[{"oid": "ccc", "headline": "rewritten"}])
        self.assertEqual(self.kinds(gh_snapshot(), forced)[0], ("push", "force-pushed (1 new commit)"))

    def test_merge_and_meta(self):
        new = gh_snapshot(state="MERGED", merged_by="bob", labels=["bug"], reviewers=[], title="Fix it now")
        kinds = self.kinds(gh_snapshot(), new)
        self.assertIn(("merged", "PR merged into main by @bob"), kinds)
        self.assertIn(("labels", "label added: bug"), kinds)
        self.assertIn(("reviewers", "review request removed: bob"), kinds)
        self.assertIn(("title", "title changed: 'Fix it' → 'Fix it now'"), kinds)

    def test_mergeable_unknown_is_carried_forward(self):
        src = github.GitHub()
        with mock.patch.object(github, "graphql"), mock.patch.object(github, "snapshot") as snap:
            snap.return_value = gh_snapshot(mergeable="UNKNOWN")
            r1 = src.poll(["o", "r", "1"], gh_snapshot(mergeable="MERGEABLE"))
            self.assertEqual((r1.state["mergeable"], r1.events), ("MERGEABLE", []))
            snap.return_value = gh_snapshot(mergeable="CONFLICTING")
            r2 = src.poll(["o", "r", "1"], r1.state)
            self.assertEqual([e.kind for e in r2.events], ["conflicts"])

    def test_token_without_read_org_leaves_team_slugs_out(self):
        src = github.GitHub()
        pr = {"repository": {"pullRequest": {"number": 1}}}
        denied = SourceError("The 'slug' field requires one of the following scopes: ['read:org', 'read:discussion']")
        with mock.patch.object(github, "_no_read_org", False), mock.patch.object(github, "snapshot") as snap, \
                mock.patch.object(github, "graphql", side_effect=[denied, pr, pr]) as gql:
            snap.return_value = gh_snapshot()
            src.poll(["o", "r", "1"], None)
            src.poll(["o", "r", "1"], None)
            queries = [c.args[0] for c in gql.call_args_list]
            self.assertEqual(queries, [github.QUERY, github.NO_TEAM_QUERY, github.NO_TEAM_QUERY])
            self.assertNotIn("Team", github.NO_TEAM_QUERY)

    def test_old_comment_sliding_into_window_is_not_new(self):
        old = gh_snapshot(comments={"C2": {"kind": "comment", "author": "a", "body": "x", "url": "u",
                                           "updated": "t", "created": "2026-02-01"}})
        new = gh_snapshot(comments={"C1": {"kind": "comment", "author": "a", "body": "old", "url": "u",
                                           "updated": "t", "created": "2026-01-01"}})
        self.assertEqual(self.kinds(old, new), [])

    def test_comments_reviews_threads(self):
        c = {"kind": "review_comment", "author": "bob", "body": "rename", "url": "u", "updated": "t1", "where": "a.py:3",
             "created": "2026-10-05T10:45:00Z"}
        old = gh_snapshot(threads={"T": {"resolved": False, "where": "a.py:3", "url": "u"}})
        new = gh_snapshot(
            comments={"C1": c, "C2": {"kind": "comment", "author": "me", "body": "done", "url": "u2", "updated": "t"}},
            reviews={"R1": {"author": "bob", "state": "CHANGES_REQUESTED", "body": "pls", "url": "u", "updated": "t"},
                     "R2": {"author": "bob", "state": "COMMENTED", "body": "", "url": "u", "updated": "t"}},
            threads={"T": {"resolved": True, "where": "a.py:3", "url": "u"}})
        events = github.diff(old, new, "github/o/r/1")
        self.assertEqual([(e.kind, e.summary) for e in events], [
            ("review_comment", "review comment on a.py:3 by @bob"),
            ("comment", "comment by @me"),
            ("review", "review by @bob: changes requested"),
            ("thread", "review thread on a.py:3 resolved"),
        ])
        self.assertEqual([e.data.get("mine") for e in events[:2]], [False, True])
        self.assertEqual(events[0].at, 1791197100.0)  # the comment's createdAt, not when we saw it
        edited = copy.deepcopy(new)
        edited["comments"]["C1"]["body"] = "rename it"
        self.assertEqual(self.kinds(new, edited), [("review_comment_edited", "review comment on a.py:3 edited by @bob")])

    def test_comment_posted_through_linear(self):
        def comment(body, db):
            return {"kind": "comment", "author": "me", "body": body, "url": "u", "updated": "t", "db": db}

        src = github.GitHub()
        old = gh_snapshot(comments={"C0": comment("earlier", "100")})
        new = gh_snapshot(comments={"C0": comment("earlier", "100"), "C1": comment("auto merge", "101"),
                                    "C2": comment("🤖 done", "102")})
        slugs = {"101": "linear-code\n", "102": "\n"}
        run = mock.Mock(side_effect=lambda cmd, **kw: mock.Mock(returncode=0, stdout=slugs[cmd[2].rsplit("/", 1)[1]]))
        with mock.patch.object(github, "graphql"), mock.patch.object(github, "snapshot", return_value=new), \
                mock.patch("subprocess.run", run):
            r = src.poll(["o", "r", "1"], old)
        self.assertEqual([(e.summary, e.data.get("via"), e.data["mine"]) for e in r.events], [
            ("comment by @me via Linear", "linear-code", False),  # the operator in Linear, not an agent
            ("comment by @me", None, True),
        ])
        self.assertEqual(run.call_count, 2)  # only the new comments, not C0
        edited = copy.deepcopy(r.state)
        edited["comments"]["C1"]["body"] = "auto merge pls"
        github.add_apps("o", "r", r.state, edited)  # "via" carried over, no second lookup
        self.assertEqual(self.kinds(r.state, edited), [("comment_edited", "comment edited by @me via Linear")])


def lin_snapshot(**over):
    s = {"viewer": "U1", "id": "x", "identifier": "T-1", "url": "https://linear.app/i/T-1", "title": "Task",
         "description": "d", "state": "Todo", "assignee": None, "priority": "Low", "project": None, "cycle": None,
         "estimate": None, "due": None, "parent": None, "labels": [], "comments": {}, "truncated": False}
    s.update(over)
    return s


def lin_comment(parent=None, author="Ann", body="hi", author_id="U2"):
    return {"parent": parent, "author": author, "author_id": author_id, "body": body,
            "hash": linear.short_hash(body), "url": "u", "created": "t"}


class LinearDiffTests(unittest.TestCase):
    def test_fields_and_comment_threads(self):
        root = "aaaaaaaa-1111"
        old = lin_snapshot(comments={root: lin_comment()})
        new = lin_snapshot(state="In Progress", assignee="Simon", labels=["bug"],
                           comments={root: lin_comment(), "bbbbbbbb-2222": lin_comment(parent=root, author="Me", author_id="U1"),
                                     "cccccccc-3333": dict(lin_comment(body="new thread"),
                                                           created="2026-10-05T10:45:00.000Z")})
        events = linear.diff(old, new, "linear/T-1")
        self.assertEqual(events[-1].at, 1791197100.0)  # Majda's 10:45 comment is stamped 10:45
        got = [(e.topic, e.summary, e.data.get("mine")) for e in events]
        self.assertEqual(got, [
            ("linear/T-1", "status: Todo → In Progress", None),
            ("linear/T-1", "assignee: none → Simon", None),
            ("linear/T-1", "label added: bug", None),
            ("linear/T-1/aaaaaaaa", "reply by Me", True),
            ("linear/T-1/cccccccc", "comment by Ann", False),
        ])

    def test_self_qa_tells_the_agent_to_run_the_pm_check(self):
        events = linear.diff(lin_snapshot(state="In Review"), lin_snapshot(state="Self QA"), "linear/T-1")
        self.assertEqual([(e.summary, e.body) for e in events], [("status: In Review → Self QA", linear.SELF_QA_HINT)])
        self.assertIn("PM check", linear.SELF_QA_HINT)
        later = linear.diff(lin_snapshot(state="Self QA"), lin_snapshot(state="PM review"), "linear/T-1")
        self.assertEqual(later[0].body, "")

    def test_resolve_thread(self):
        state = lin_snapshot(comments={"aaaaaaaa-1": lin_comment(), "bbbbbbbb-2": lin_comment(parent="aaaaaaaa-1")})
        src = linear.Linear()
        self.assertEqual(src.resolve(["T-1", "bbbbbbbb"], state)[0], ["T-1", "aaaaaaaa"])  # reply → its thread
        self.assertRaises(TopicError, src.resolve, ["T-1", "ffffffff"], state)

    def test_deleted_and_edited(self):
        old = lin_snapshot(comments={"a": lin_comment(), "b": lin_comment(body="x")})
        new = lin_snapshot(comments={"a": lin_comment(body="changed")})
        kinds = [e.kind for e in linear.diff(old, new, "linear/T-1")]
        self.assertEqual(kinds, ["comment_edited", "comment_deleted"])


def docs_member(article, vid, published_at=None, by=None, app=None):
    return {"article_canonical_id": article, "version_id": vid, "published": bool(published_at),
            "published_at": published_at, "published_by": by and {"id": 1, "name": by}, "published_by_app": app}


class DocsDiffTests(unittest.TestCase):
    def snap(self, *members, ref="U-1"):
        return docs.snapshot(ref, list(members))

    def test_publish_of_a_whole_batch(self):
        old = self.snap(docs_member("a", 1), docs_member("b/c", 2), docs_member("d", 3, "2026-10-01T10:00:00Z", "Ann"))
        new = self.snap(docs_member("a", 1, "2026-10-18T16:54:51Z", "Simon Let"),
                        docs_member("b/c", 2, "2026-10-18T16:54:52Z", "Simon Let"),
                        docs_member("d", 3, "2026-10-01T10:00:00Z", "Ann"))
        events = docs.diff(old, new)
        got = [(e.topic, e.kind, e.summary, e.data.get("status")) for e in events]
        self.assertEqual(got, [
            ("docs/U-1/a", "published", "published: a (batch U-1) by Simon Let", "published"),
            ("docs/U-1/b~c", "published", "published: b/c (batch U-1) by Simon Let", "published"),
            ("docs/U-1", "batch", "batch U-1 published: 2 published, 0 discarded", "published"),
        ])
        self.assertEqual(events[0].data, {"article": "a", "version": 1, "batch": "U-1", "status": "published"})
        self.assertEqual(events[0].actor, "Simon Let")
        self.assertEqual(events[0].at, 1792342491.0)
        self.assertEqual((events[0].body, events[2].body), ("", "published: a\npublished: b/c\n" + docs.DONE_HINT))
        self.assertEqual(events[0].url, "https://betterstack.com/admin/articles/unpublished/U-1")
        self.assertEqual(events[2].data["published"], ["a", "b/c"])
        self.assertEqual(docs.diff(new, new), [])

    def test_partial_publish_has_no_batch_event(self):
        events = docs.diff(self.snap(docs_member("a", 1), docs_member("b", 2)),
                           self.snap(docs_member("a", 1, "2026-10-18T16:54:51Z", app="docs-terminal"), docs_member("b", 2)))
        self.assertEqual([(e.kind, e.summary) for e in events], [("published", "published: a (batch U-1) by docs-terminal")])

    def test_changes_to_a_staged_version(self):
        def staged(digest, draft=False, stale=False, edited_by=None):
            return [{"id": 1, "staged_digest": digest, "draft": draft, "stale": stale, "edited_by_app": edited_by,
                     "source_app": "docs-terminal", "admin_url": "https://betterstack.com/admin/articles/a/versions/1/diff"}]

        members = [docs_member("a", 1)]
        old = docs.snapshot("U-1", members, staged("d1", draft=True))
        new = docs.snapshot("U-1", members, staged("d2", draft=False, stale=True, edited_by="docs-bot"))
        events = docs.diff(old, new)
        self.assertEqual([(e.topic, e.kind, e.summary, e.data["status"]) for e in events], [
            ("docs/U-1/a", "revised", "revised: a (batch U-1) by docs-bot", "staged"),
            ("docs/U-1/a", "promoted", "ready for review: a (batch U-1)", "staged"),
            ("docs/U-1/a", "stale", "stale: a (batch U-1): its live article changed since it was staged, so it "
             "can't publish until it's restaged", "stale"),
        ])
        self.assertEqual((events[0].actor, events[0].url), ("docs-bot", "https://betterstack.com/admin/articles/a/versions/1/diff"))
        self.assertEqual(docs.diff(new, new), [])
        # A state from before staged details were polled: nothing to compare yet, no events.
        self.assertEqual(docs.diff(docs.snapshot("U-1", members), new), [])

    def test_poll_reads_staged_details_only_while_something_is_staged(self):
        src = docs.Docs()
        with mock.patch.object(docs, "fetch_batch", return_value=[docs_member("a", 1, "2026-10-18T16:54:51Z", "Ann")]), \
                mock.patch.object(docs, "fetch_unpublished") as unpublished:
            src.poll(["U-1"], None)
        unpublished.assert_not_called()
        with mock.patch.object(docs, "fetch_batch", return_value=[docs_member("a", 1)]), \
                mock.patch.object(docs, "fetch_unpublished", return_value=[{"id": 1, "staged_digest": "d"}]):
            r = src.poll(["U-1"], None)
        self.assertEqual(r.state["members"]["1"]["digest"], "d")

    def test_discard_restage_and_stage(self):
        old = self.snap(docs_member("a", 1), docs_member("b", 2))
        new = self.snap(docs_member("b", 5), docs_member("c", 6))
        got = [(e.topic, e.kind, e.data.get("version"), e.data.get("status")) for e in docs.diff(old, new)]
        self.assertEqual(got, [
            ("docs/U-1/a", "discarded", 1, "discarded"),
            ("docs/U-1/b", "restaged", 5, "staged"),
            ("docs/U-1/c", "staged", 6, "staged"),
        ])

    def test_discarded_batch(self):
        events = docs.diff(self.snap(docs_member("a", 1), docs_member("b", 2, "2026-10-01T10:00:00Z", "Ann")),
                           self.snap(docs_member("b", 2, "2026-10-01T10:00:00Z", "Ann")))
        self.assertEqual([(e.kind, e.summary, e.body) for e in events], [
            ("discarded", "discarded: a (batch U-1)", ""),
            ("batch", "batch U-1 discarded: 0 published, 1 discarded", "discarded: a"),
        ])

    def test_resolve(self):
        src = docs.Docs()
        state = self.snap(docs_member("a/b", 1), ref="x/y")
        self.assertEqual(src.resolve(["x~y", "a~b"], state), (["x~y", "a~b"], "a/b: staged"))
        self.assertRaises(TopicError, src.resolve, ["x~y", "zz"], state)
        self.assertRaises(TopicError, src.resolve, ["nope"], self.snap(ref="nope"))
        self.assertEqual(src.poll_key(["x~y", "a~b"]), ["x~y"])


class SlackThreadTests(unittest.TestCase):
    def setUp(self):
        slack._team.update(url="https://ws.slack.com/", user_id="UME")
        slack._users.update(UA="ann", UME="simon")
        slack._channels["C1"] = "#dev"

    def poll(self, messages, state):
        with mock.patch.object(slack, "replies", return_value=messages):
            return slack.poll_thread("C1", "100.000001", state)

    def test_thread_changes(self):
        root = {"ts": "100.000001", "user": "UA", "text": "question <@UME>?"}
        base = self.poll([root], None)
        self.assertEqual(base.events, [])
        self.assertIn("question", base.summary)
        reply = {"ts": "101.000001", "user": "UME", "text": "answer", "thread_ts": "100.000001"}
        root2 = dict(root, reactions=[{"name": "eyes", "count": 1}])
        r = self.poll([root2, reply], base.state)
        got = [(e.kind, e.summary, e.data.get("mine")) for e in r.events]
        self.assertEqual(got, [("reaction", ":eyes: added to message by ann", None),
                               ("message", "reply by simon", True)])
        self.assertEqual(r.events[1].dedupe, "msg:101.000001")
        self.assertEqual(r.events[1].at, 101.000001)  # stamped with the message's own time
        self.assertIn("thread_ts=100.000001", r.events[1].url)
        edited = self.poll([root2, dict(reply, text="better answer")], r.state)
        self.assertEqual([e.kind for e in edited.events], ["message_edited"])
        deleted = self.poll([root2], edited.state)
        self.assertEqual([e.kind for e in deleted.events], ["message_deleted"])

    def test_channel_new_message_with_quick_reply(self):
        state = {"latest": "100.000001", "threads": {}}
        history = {"messages": [{"ts": "200.000001", "user": "UA", "text": "q", "reply_count": 1,
                                 "latest_reply": "201.000001", "thread_ts": "200.000001"}]}
        reply = [{"ts": "200.000001", "user": "UA", "text": "q"},
                 {"ts": "201.000001", "user": "UME", "text": "a", "thread_ts": "200.000001"}]
        with mock.patch.object(slack, "call", return_value=history), \
                mock.patch.object(slack, "replies", return_value=reply):
            r = slack.poll_channel("C1", state)
        self.assertEqual([(e.topic, e.summary) for e in r.events], [
            ("slack/C1", "message by ann in #dev"),
            ("slack/C1/200.000001", "reply by simon in #dev thread"),
        ])

    def test_failed_channel_lookup_is_retried(self):
        # A failed lookup shows the id but isn't kept for good: once Slack
        # answers again, the name comes back (Simon, 2026-10-06).
        slack._channels.pop("C9", None); slack._failed.pop("C9", None)
        with mock.patch.object(slack, "call", side_effect=slack.SourceError("no token")):
            self.assertEqual(slack.channel_name("C9"), "C9")
        ok = {"channel": {"name": "engineering"}}
        with mock.patch.object(slack, "call", return_value=ok) as call:
            self.assertEqual(slack.channel_name("C9"), "C9")   # within the retry wait
            call.assert_not_called()
            with mock.patch.object(slack.time, "time", return_value=slack.time.time() + slack.RETRY_FAILED + 1):
                self.assertEqual(slack.channel_name("C9"), "#engineering")
        self.assertEqual(slack.channel_name("C9"), "#engineering")   # cached now

    def test_render(self):
        self.assertEqual(slack.render("hi <@UA> see <https://x.io|docs> in <#C1|dev> &amp; <!here>"),
                         "hi @ann see docs (https://x.io) in #dev & @here")


if __name__ == "__main__":
    unittest.main()


class SecretTests(unittest.TestCase):
    def setUp(self):
        from agent_events import config
        self.config = config
        config._secret_cache.clear()
        config._secret_failed.clear()

    def test_failed_op_read_is_not_retried_soon(self):
        failed = mock.Mock(returncode=1, stdout="")
        env = {"AGENT_EVENTS_X_OP_REF": "op://v/x/credential"}
        with mock.patch.dict("os.environ", env), mock.patch("subprocess.run", return_value=failed) as run:
            self.assertIsNone(self.config.secret(["X_TOKEN_UNSET"], "AGENT_EVENTS_X_OP_REF", None))
            self.assertIsNone(self.config.secret(["X_TOKEN_UNSET"], "AGENT_EVENTS_X_OP_REF", None))
            self.assertEqual(run.call_count, 1)  # the second lookup didn't prompt again
            with mock.patch.object(self.config, "SECRET_RETRY", 0):
                self.config.secret(["X_TOKEN_UNSET"], "AGENT_EVENTS_X_OP_REF", None)
            self.assertEqual(run.call_count, 2)

    def test_parallel_lookups_share_one_op_read(self):
        import threading
        started = threading.Event()

        def slow_read(*args, **kwargs):  # an op read waiting for the operator's Approve
            started.set()
            time.sleep(0.2)
            return mock.Mock(returncode=0, stdout="tok\n")

        env = {"AGENT_EVENTS_X_OP_REF": "op://v/x/credential"}
        got = []
        with mock.patch.dict("os.environ", env), mock.patch("subprocess.run", side_effect=slow_read) as run:
            threads = [threading.Thread(target=lambda: got.append(
                self.config.secret(["X_TOKEN_UNSET"], "AGENT_EVENTS_X_OP_REF", None))) for _ in range(3)]
            threads[0].start()
            started.wait(1)
            for t in threads[1:]:
                t.start()
            for t in threads:
                t.join(2)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(got, ["tok"] * 3)

    def test_no_op_ref_means_no_op_read(self):
        with mock.patch("subprocess.run") as run:
            self.assertIsNone(self.config.secret(["X_TOKEN_UNSET"], "AGENT_EVENTS_X_OP_REF_UNSET", None))
        run.assert_not_called()


class CredentialDefaultsTest(unittest.TestCase):
    """A fresh Mac: no token unless configured, read through Hyperfocus's op broker."""

    def test_no_default_refs(self):
        from agent_events.sources import linear, slack
        self.assertIsNone(linear.DEFAULT_OP_REF)
        self.assertIsNone(slack.DEFAULT_OP_REF)

    def test_op_binary_prefers_the_hyperfocus_broker(self):
        import tempfile
        from unittest import mock
        from agent_events import config
        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {"HF_HOME": home}, clear=False):
                os.environ.pop("AGENT_EVENTS_OP", None)
                self.assertEqual(config.op_binary(), "op")
                broker = os.path.join(home, "bin", "op")
                os.makedirs(os.path.dirname(broker))
                with open(broker, "w") as f:
                    f.write("#!/bin/sh\n")
                os.chmod(broker, 0o755)
                self.assertEqual(config.op_binary(), broker)
                with mock.patch.dict(os.environ, {"AGENT_EVENTS_OP": "/x/op"}):
                    self.assertEqual(config.op_binary(), "/x/op")
