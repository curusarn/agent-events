from __future__ import annotations

import io
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from unittest import mock

from helpers import DaemonTest

from agent_events import cli, client, config


def watch_raw(agent, timeout=5.0, ack=True, passive=False):
    """One watch round-trip over the socket, like `agent-events watch` does."""
    s = client._connect(timeout + 5)
    client.send(s, {"op": "watch", "agent": agent, "timeout": timeout, "passive": passive})
    resp = client.recv(s)
    if ack and resp and resp.get("events"):
        client.send(s, {"ack": max(e["id"] for e in resp["events"])})
    s.close()
    return resp


class SubscriptionTests(DaemonTest):
    def test_subscribe_list_unsubscribe(self):
        self.fake.things["a"] = {"version": 0, "events": []}
        r = self.req(op="subscribe", agent="ag", topic="fake/a")
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["topic"], "fake/a")
        self.assertTrue(r["new"])
        self.assertEqual(r["summary"], "thing a v0")
        self.assertFalse(self.req(op="subscribe", agent="ag", topic="fake/a/")["new"])
        self.req(op="subscribe", agent="ag", topic="fake/a/thread")
        topics = [s["topic"] for s in self.req(op="list", agent="ag")["subscriptions"]]
        self.assertEqual(topics, ["fake/a", "fake/a/thread"])
        # Unsubscribing a parent also removes nested subscriptions.
        r = self.req(op="unsubscribe", agent="ag", topics=["fake/a"])
        self.assertEqual(sorted(r["removed"]), ["fake/a", "fake/a/thread"])
        self.assertEqual(self.req(op="list", agent="ag")["subscriptions"], [])

    def test_subscribe_failure_leaves_nothing_behind(self):
        r = self.req(op="subscribe", agent="ag", topic="fake/missing")
        self.assertFalse(r["ok"])
        self.assertIn("unavailable", r["error"])
        self.assertIsNone(self.daemon.store.target("fake/missing"))
        self.assertEqual(self.req(op="list", agent="ag")["subscriptions"], [])

    def test_bad_topics(self):
        for topic in ("nope/x", "github/only-owner", "linear/notanissue", "slack/lowercase-nonsense", ""):
            r = self.req(op="subscribe", agent="ag", topic=topic)
            self.assertFalse(r["ok"], topic)


class WatchTests(DaemonTest):
    def setUp(self):
        super().setUp()
        self.fake.things["a"] = {"version": 0, "events": []}
        self.fake.things["b"] = {"version": 0, "events": []}

    def test_event_wakes_watch(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        result = {}
        t = threading.Thread(target=lambda: result.update(watch_raw("ag")))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        self.fake.change("a", ("", "hello"))
        t.join(10)
        self.assertEqual([e["summary"] for e in result["events"]], ["hello"])
        self.assertEqual(result["events"][0]["target"], "thing a v1")
        # Acked: not delivered again.
        self.assertTrue(watch_raw("ag", timeout=0.5).get("timeout"))

    def test_unacked_events_are_redelivered(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        self.fake.change("a", ("", "one"))
        first = watch_raw("ag", ack=False)
        self.assertEqual([e["summary"] for e in first["events"]], ["one"])
        again = watch_raw("ag")
        self.assertEqual([e["summary"] for e in again["events"]], ["one"])

    def test_nested_topics(self):
        self.req(op="subscribe", agent="parent", topic="fake/a")
        self.req(op="subscribe", agent="child", topic="fake/a/t1")
        self.fake.change("a", ("", "ticket changed"), ("/t1", "comment in t1"), ("/t2", "comment in t2"))
        self.assertTrue(self.wait_for(lambda: len(self.daemon.store.pending("parent")) == 3))
        parent = [e["summary"] for e in watch_raw("parent")["events"]]
        child = [e["summary"] for e in watch_raw("child")["events"]]
        self.assertEqual(parent, ["ticket changed", "comment in t1", "comment in t2"])
        self.assertEqual(child, ["comment in t1"])

    def test_new_subscriber_does_not_get_old_events(self):
        self.req(op="subscribe", agent="early", topic="fake/a")
        self.fake.change("a", ("", "old news"))
        self.assertTrue(self.wait_for(lambda: self.daemon.store.pending("early")))
        self.req(op="subscribe", agent="late", topic="fake/a")
        self.assertTrue(watch_raw("late", timeout=0.5).get("timeout"))
        self.assertEqual(len(watch_raw("early")["events"]), 1)

    def test_only_subscribed_topics(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        self.req(op="subscribe", agent="other", topic="fake/b")
        self.fake.change("b", ("", "b changed"))
        self.assertTrue(self.wait_for(lambda: self.daemon.store.pending("other")))
        self.assertTrue(watch_raw("ag", timeout=0.5).get("timeout"))

    def test_no_subscriptions(self):
        self.assertTrue(watch_raw("nobody")["no_subscriptions"])

    def test_unsubscribing_everything_ends_watch(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        result = {}
        t = threading.Thread(target=lambda: result.update(watch_raw("ag")))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        self.req(op="unsubscribe", agent="ag", all=True)
        t.join(10)
        self.assertTrue(result.get("no_subscriptions"))

    def test_second_watch_supersedes_first(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        first = {}
        t = threading.Thread(target=lambda: first.update(watch_raw("ag")))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        second = {}
        t2 = threading.Thread(target=lambda: second.update(watch_raw("ag")))
        t2.start()
        t.join(10)
        self.assertTrue(first.get("superseded"))
        self.fake.change("a", ("", "x"))
        t2.join(10)
        self.assertEqual([e["summary"] for e in second["events"]], ["x"])

    def test_dead_watch_client_is_noticed(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        s = client._connect(10)
        client.send(s, {"op": "watch", "agent": "ag"})
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        self.assertTrue(self.wait_for(lambda: "ag" not in self.daemon.watchers, timeout=8))

    def test_persistent_failure_is_reported_once_then_recovery(self):
        self.req(op="subscribe", agent="ag", topic="fake/a")
        # Speed up backoff for the test.
        with mock.patch("agent_events.config.poll_interval", return_value=0.01):
            self.fake.fail.add("a")
            self.assertTrue(self.wait_for(lambda: self.daemon.store.pending("ag"), timeout=10))
            events = watch_raw("ag")["events"]
            self.assertEqual([e["kind"] for e in events], ["error"])
            self.assertIn("unavailable", events[0]["summary"])
            self.fake.fail.discard("a")
            self.assertTrue(self.wait_for(lambda: self.daemon.store.pending("ag"), timeout=10))
            self.assertEqual([e["kind"] for e in watch_raw("ag")["events"]], ["recovered"])

    def test_session_start_retires_previous_session_of_same_process(self):
        self.req(op="subscribe", agent="old", topic="fake/a", pid=os.getpid())
        self.assertIn("fake/a", self.daemon.active_keys())
        r = self.req(op="session_start", agent="new", pid=os.getpid())
        self.assertEqual(r["retired"], ["old"])
        self.assertNotIn("fake/a", self.daemon.active_keys())
        self.assertEqual(len(self.req(op="list", agent="old")["subscriptions"]), 1)  # kept, just dormant
        self.req(op="session_start", agent="old", pid=os.getpid() + 1)  # `claude --resume old`
        self.assertIn("fake/a", self.daemon.active_keys())

    def test_stopped_agents_are_still_polled(self):
        """A stopped agent (no process, not seen for hours) keeps getting its events, stamped with
        the source's time, so they wait for it and show up as unread."""
        self.fake.things["a"] = {"version": 0, "events": []}
        self.req(op="subscribe", agent="ag", topic="fake/a")
        self.daemon.store.x("UPDATE agents SET last_seen = ?, pid = NULL", (time.time() - 6 * 3600,))
        self.assertIn("fake/a", self.daemon.active_keys())
        self.fake.change("a", ("", "reply while stopped"))
        self.assertTrue(self.wait_for(lambda: self.daemon.store.pending("ag"), timeout=5))
        unread = self.req(op="unread")["agents"]
        self.assertEqual(unread["ag"]["count"], 1)
        self.assertEqual(unread["ag"]["topics"], ["fake/a"])
        from agent_events.sources import Event
        store = self.daemon.store
        store.save_poll("fake/b", {"v": 1}, "b", time.time() + 60,
                        [Event("fake/a", "change", "an hour ago", at=time.time() - 3600, dedupe="old"),
                         Event("fake/a", "change", "clock skew", at=time.time() + 3600, dedupe="future")])
        times = {e["summary"]: e["created_at"] for e in store.pending("ag")}
        self.assertAlmostEqual(times["an hour ago"], time.time() - 3600, delta=5)
        self.assertLessEqual(times["clock skew"], time.time())  # never in the future
        self.assertAlmostEqual(self.req(op="unread")["agents"]["ag"]["oldest"], time.time() - 3600, delta=5)
        self.assertEqual(self.req(op="unread", since=time.time() - 60)["agents"]["ag"]["count"], 2)
        self.assertEqual(self.req(op="unread", agents=["other"])["agents"], {})

    def test_unread_skips_news_older_than_what_the_agent_already_got(self):
        from agent_events.sources import Event
        self.req(op="subscribe", agent="ag", topic="fake/a")
        store = self.daemon.store
        store.save_poll("fake/x", {"v": 1}, "x", time.time() + 60, [Event("fake/a", "change", "seen", dedupe="1")])
        store.ack("ag", max(e["id"] for e in store.pending("ag")))  # delivered to the agent
        store.save_poll("fake/x", {"v": 2}, "x", time.time() + 60,
                        [Event("fake/a", "change", "a day-old CI result", at=time.time() - 86400, dedupe="2"),
                         Event("fake/a", "change", "new", dedupe="3")])
        unread = self.req(op="unread", events=5)["agents"]["ag"]
        self.assertEqual([e["summary"] for e in unread["events"]], ["new"])

    def test_unread_skips_fyi_and_excluded_topics(self):
        self.req(op="subscribe", agent="ag", topic="hf/agent/x")
        self.req(op="publish", topic="hf/agent/x", summary="fyi", quiet=True)
        self.assertEqual(self.req(op="unread")["agents"], {})
        self.req(op="publish", topic="hf/agent/x", summary="a post")
        self.assertEqual(self.req(op="unread")["agents"]["ag"]["count"], 1)
        self.assertEqual(self.req(op="unread", exclude=["hf/"])["agents"], {})
        self.req(op="publish", topic="hf/agent/x", summary="polling is failing", kind="error")
        self.req(op="publish", topic="hf/agent/x", summary="polling works again", kind="recovered")
        self.assertEqual(self.req(op="unread")["agents"]["ag"]["count"], 1)  # health events don't count
        listed = self.req(op="unread", events=5)["agents"]["ag"]["events"]
        self.assertEqual([(e["topic"], e["summary"]) for e in listed], [("hf/agent/x", "a post")])
        self.assertNotIn("events", self.req(op="unread")["agents"]["ag"])


class PushTests(DaemonTest):
    def test_subscribe_push_topic_needs_no_target(self):
        r = self.req(op="subscribe", agent="ag", topic="hf/agent/abc123")
        self.assertEqual(r, {"ok": True, "topic": "hf/agent/abc123", "new": True, "summary": "", "detail": ""})
        self.assertIsNone(self.daemon.store.target("hf/agent/abc123"))
        self.assertNotIn("hf/agent/abc123", self.daemon.active_keys())
        self.assertEqual(self.req(op="refresh", agent="ag")["keys"], [])
        subs = self.req(op="list", agent="ag")["subscriptions"]
        self.assertEqual([(s["topic"], s["push"]) for s in subs], [("hf/agent/abc123", True)])
        self.assertFalse(self.req(op="subscribe", agent="ag", topic="hf/agent/abc123")["new"])

    def test_publish_wakes_watch_fast(self):
        self.req(op="subscribe", agent="ag", topic="hf/agent/abc123")
        result = {}
        with mock.patch.object(config, "WATCH_SETTLE", 3.0):
            t = threading.Thread(target=lambda: result.update(watch_raw("ag")))
            t.start()
            self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
            start = time.time()
            r = self.req(op="publish", topic="hf/agent/abc123", summary="hi", actor="orchestrator", body="text")
            t.join(10)
            elapsed = time.time() - start
        self.assertTrue(r["ok"], r)
        self.assertEqual((r["topic"], r["duplicate"], r["subscribers"], r["watching"]),
                         ("hf/agent/abc123", False, ["ag"], ["ag"]))
        self.assertLess(elapsed, 1.0)
        [e] = result["events"]
        self.assertEqual((e["id"], e["summary"], e["actor"], e["body"], e["kind"]),
                         (r["id"], "hi", "orchestrator", "text", "message"))

    def test_publish_id_is_idempotent(self):
        self.req(op="subscribe", agent="ag", topic="hf/agent/x")
        first = self.req(op="publish", topic="hf/agent/x", summary="once", id="msg-1")
        again = self.req(op="publish", topic="hf/agent/x", summary="once", id="msg-1")
        self.assertFalse(first["duplicate"])
        self.assertTrue(again["duplicate"])
        self.assertEqual(first["id"], again["id"])
        # Without an id every publish is distinct (no empty-dedupe collisions).
        self.assertFalse(self.req(op="publish", topic="hf/agent/x", summary="a")["duplicate"])
        self.assertFalse(self.req(op="publish", topic="hf/agent/x", summary="b")["duplicate"])
        self.assertEqual([e["summary"] for e in watch_raw("ag")["events"]], ["once", "a", "b"])

    def test_publish_rejects_bad_requests(self):
        self.fake.things["a"] = {"version": 0, "events": []}
        for topic in ("github/o/r/1", "linear/T-1", "slack/C0123456", "fake/a", "mc/foo", "nope/x"):
            r = self.req(op="publish", topic=topic, summary="x")
            self.assertFalse(r["ok"], topic)
        self.assertIn("only push topics", self.req(op="publish", topic="github/o/r/1", summary="x")["error"])
        self.assertFalse(self.req(op="publish", topic="hf/agent/x", summary=" ")["ok"])
        self.assertFalse(self.req(op="publish", topic="hf/agent/x", summary="x", data=[1])["ok"])
        self.assertEqual(self.daemon.store.max_event_id(), 0)

    def test_publish_does_not_register_an_agent(self):
        self.req(op="publish", topic="hf/agent/x", summary="nobody listens", agent="app")
        self.assertEqual(self.daemon.store.agents(), [])

    def test_old_mc_name_is_the_same_topic(self):
        """Mission Control became Hyperfocus: mc/... still works during the transition."""
        self.assertEqual(self.req(op="subscribe", agent="ag", topic="mc/agent/x")["topic"], "hf/agent/x")
        self.req(op="publish", topic="hf/agent/x", summary="new name")
        self.req(op="publish", topic="mc/agent/x", summary="old name")
        resp = watch_raw("ag")
        self.assertEqual([(e["topic"], e["summary"]) for e in resp["events"]],
                         [("hf/agent/x", "new name"), ("hf/agent/x", "old name")])
        self.assertEqual(self.req(op="unsubscribe", agent="ag", topics=["mc/agent/x"])["removed"], ["hf/agent/x"])

    def test_stored_mc_topics_are_renamed_on_startup(self):
        from agent_events.store import Store
        store = self.daemon.store
        store.touch_agent("ag")
        store.touch_agent("both")
        for agent, topic in (("ag", "mc/agent/x"), ("both", "mc/agent/y"), ("both", "hf/agent/y")):
            store.x("INSERT INTO subscriptions (agent_id, topic, since, created_at) VALUES (?, ?, 0, ?)",
                    (agent, topic, time.time()))
        store.x("INSERT INTO events (topic, kind, dedupe, summary, created_at) VALUES ('mc/agent/x', 'message', 'd', "
                "'pending from before', ?)", (time.time(),))
        Store(config.db_path())  # what a restarted daemon does
        self.assertEqual(sorted((s["agent_id"], s["topic"]) for s in store.all_subs()),
                         [("ag", "hf/agent/x"), ("both", "hf/agent/y")])
        resp = watch_raw("ag", timeout=2)
        self.assertEqual([e["summary"] for e in resp["events"]], ["pending from before"], resp)


class PassiveWatchTests(DaemonTest):
    """Passive watches (the Claude Code hook's) never take over from a normal watch."""

    def setUp(self):
        super().setUp()
        self.req(op="subscribe", agent="ag", topic="hf/agent/x")

    def publish(self, summary):
        self.req(op="publish", topic="hf/agent/x", summary=summary)

    def start(self, **kw):
        result = {}
        t = threading.Thread(target=lambda: result.update(watch_raw("ag", **kw)))
        t.start()
        return t, result

    def test_passive_waits_for_normal_watch_then_takes_over(self):
        t1, normal = self.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        t2, passive = self.start(passive=True)
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.passive))
        self.publish("one")
        t1.join(10)
        self.assertEqual([e["summary"] for e in normal["events"]], ["one"])
        self.assertTrue(t2.is_alive())  # the standby did not get (or duplicate) it
        self.assertTrue(self.wait_for(lambda: self.daemon.watchers.get("ag") is self.daemon.passive.get("ag")))
        self.publish("two")
        t2.join(10)
        self.assertEqual([e["summary"] for e in passive["events"]], ["two"])

    def test_normal_watch_takes_over_from_passive_which_stays_on_standby(self):
        t1, passive = self.start(passive=True)
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        t2, normal = self.start()
        self.assertTrue(self.wait_for(lambda: self.daemon.watchers.get("ag") is not self.daemon.passive.get("ag")))
        self.publish("one")
        t2.join(10)
        self.assertEqual([e["summary"] for e in normal["events"]], ["one"])
        self.assertTrue(t1.is_alive())
        self.publish("two")
        t1.join(10)
        self.assertEqual([e["summary"] for e in passive["events"]], ["two"])

    def test_newer_passive_supersedes_older(self):
        t1, first = self.start(passive=True)
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.passive))
        t2, second = self.start(passive=True)
        t1.join(10)
        self.assertTrue(first.get("superseded"))
        self.publish("x")
        t2.join(10)
        self.assertEqual([e["summary"] for e in second["events"]], ["x"])

    def test_unwatch_ends_watches(self):
        t1, passive = self.start(passive=True)
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.passive))
        self.req(op="unwatch", agent="ag")
        t1.join(10)
        self.assertTrue(passive.get("superseded"))
        self.assertNotIn("ag", self.daemon.watchers)

    def test_take_hands_over_pending_once(self):
        self.publish("one")
        self.assertEqual([e["summary"] for e in self.req(op="take", agent="ag")["events"]], ["one"])
        self.assertEqual(self.req(op="take", agent="ag")["events"], [])
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)

    def test_take_leaves_events_to_a_normal_watch(self):
        t1, normal = self.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        self.daemon.inflight_batches.add("ag")  # hold delivery so take races the watch
        self.publish("one")
        self.assertEqual(self.req(op="take", agent="ag")["events"], [])
        self.daemon.inflight_batches.discard("ag")
        t1.join(10)
        self.assertEqual([e["summary"] for e in normal["events"]], ["one"])


class IntervalTests(DaemonTest):
    def test_interval_stretches_with_active_keys(self):
        self.fake.min_spacing = 0.5
        self.daemon.last_active = {"fake/x"}
        self.assertEqual(self.daemon.interval(self.fake), 0.5)  # max(0.2 base, 1 * 0.5)
        self.daemon.last_active = {f"fake/{i}" for i in range(10)} | {"github/o/r/1"}
        self.assertEqual(self.daemon.interval(self.fake), 5.0)


class CliTest(DaemonTest):
    """Runs the CLI in-process, against a settings file of its own."""

    def setUp(self):
        super().setUp()
        self.settings = os.path.join(self.tmp, "settings.json")
        patch = mock.patch.object(cli, "_settings_path", lambda: cli.Path(self.settings))
        patch.start()
        self.addCleanup(patch.stop)

    def run_cli(self, *argv, stdin=None, stderr=None):
        out = io.StringIO()
        with redirect_stdout(out), mock.patch("sys.stdin", io.StringIO(stdin or "")), \
                mock.patch("sys.stderr", stderr or io.StringIO()):
            code = cli.main(list(argv))
        return code, out.getvalue()

    def read_settings(self):
        with open(self.settings) as f:
            return json.load(f)

    def install_hook_watch(self):
        with mock.patch.object(cli.config, "BIN", cli.Path("/x/agent-events/bin/agent-events")):
            cli.install_hooks()



class CliTests(CliTest):
    def test_watch_command_prints_events_and_acks(self):
        self.fake.things["a"] = {"version": 0, "events": []}
        self.run_cli("--agent", "ag", "subscribe", "fake/a")
        self.fake.change("a", ("", "something happened"))
        code, out = self.run_cli("--agent", "ag", "watch", "--timeout", "5")
        self.assertEqual(code, 0)
        self.assertIn("1 new event", out)
        self.assertIn("something happened", out)
        self.assertIn("agent-events watch", out)  # the reminder to re-run
        code, out = self.run_cli("--agent", "ag", "watch", "--timeout", "0.5")
        self.assertIn("No events", out)

    def test_publish_then_watch(self):
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/abc123")
        code, out = self.run_cli("publish", "hf/agent/abc123", "please review PR 12", "--actor", "orchestrator",
                                 "--body", "-", "--id", "m1", stdin="line one\nline two")
        self.assertEqual(code, 0)
        self.assertRegex(out, r"^✓ published #\d+ to hf/agent/abc123 \(subscribers: 1, watching: 0\)")
        code, out = self.run_cli("--agent", "ag", "watch", "--timeout", "5")
        self.assertIn("hf/agent/abc123\n", out)
        self.assertRegex(out, r"\n  \d\d:\d\d · from orchestrator · please review PR 12\n    line one\n    line two\n")
        self.assertIn("already published", self.run_cli("publish", "hf/agent/abc123", "x", "--id", "m1")[1])

    def test_message_bodies_are_not_clipped_like_other_events(self):
        body = "\n".join(f"line {i}" for i in range(40))
        ev = {"created_at": time.time(), "kind": "message", "summary": "s", "body": body, "actor": "a"}
        out = cli.format_events([dict(ev, topic="hf/agent/x")])
        self.assertIn("line 39", out)
        out = cli.format_events([dict(ev, topic="fake/a")])
        self.assertNotIn("line 39", out)
        self.assertNotIn("from a", out)  # polled sources name the author in their summary

    def test_agent_on_behalf_does_not_send_pid_or_cwd(self):
        with mock.patch.dict(os.environ, {"CLAUDE_PID": str(os.getpid()), "CLAUDE_CODE_SESSION_ID": "me"}):
            args = cli.argparse.Namespace(agent="other")
            self.assertEqual((cli.agent_req(args, "list")["pid"], cli.agent_req(args, "list")["cwd"]), (None, None))
            args = cli.argparse.Namespace(agent="me")
            self.assertEqual(cli.agent_req(args, "list")["pid"], os.getpid())
            args = cli.argparse.Namespace(agent=None)
            req = cli.agent_req(args, "list")
            self.assertEqual((req["agent"], req["pid"], req["cwd"]), ("me", os.getpid(), os.getcwd()))

    def test_hook(self):
        self.fake.things["a"] = {"version": 0, "events": []}
        hook_in = json.dumps({"session_id": "ag", "hook_event_name": "Stop", "stop_hook_active": False})
        self.assertEqual(self.run_cli("hook", stdin=hook_in)[1], "")  # no subscriptions: silent
        self.run_cli("--agent", "ag", "subscribe", "fake/a")
        out = json.loads(self.run_cli("hook", stdin=hook_in)[1])
        self.assertEqual(out["decision"], "block")
        self.assertIn("agent-events watch", out["reason"])
        loop = json.dumps({"session_id": "ag", "hook_event_name": "Stop", "stop_hook_active": True})
        self.assertEqual(self.run_cli("hook", stdin=loop)[1], "")
        prompt = json.dumps({"session_id": "ag", "hook_event_name": "UserPromptSubmit"})
        ctx = json.loads(self.run_cli("hook", stdin=prompt)[1])
        self.assertIn("fake/a", ctx["hookSpecificOutput"]["additionalContext"])
        # With a watch connected the hook is silent.
        t = threading.Thread(target=lambda: watch_raw("ag", timeout=3))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        self.assertEqual(self.run_cli("hook", stdin=hook_in)[1], "")
        self.fake.change("a", ("", "done"))
        t.join(10)


    def test_header_does_not_repeat_the_body(self):
        body = "Since the installed agent-events runs from this checkout, every running agent uses it.\nMore."
        ev = {"topic": "hf/agent/x", "created_at": time.time(), "kind": "message", "actor": "agent orchestrator",
              "summary": "[Improve how agents receive…] Since the installed agent-events runs from this chec",
              "body": body}
        out = cli.format_events([ev], title=False)
        self.assertEqual(out.splitlines()[1][7:], ' · from agent orchestrator · thread "Improve how agents receive…"')
        self.assertEqual(out.count("Since the installed"), 1)
        out = cli.format_events([dict(ev, summary="short summary", data={"thread": "6ea3b104", "thread_title": "T"})])
        self.assertIn(' · from agent orchestrator · thread 6ea3b104 "T" · short summary\n', out)

    def test_header_shows_when_a_held_post_was_written(self):
        written = time.mktime((2026, 10, 6, 21, 10, 44, 0, 0, -1))
        ev = {"topic": "hf/agent/x", "created_at": written + 45, "kind": "message", "actor": "agent o",
              "summary": "s", "body": "s", "data": {"sent_at": str(int(written))}}
        self.assertIn("21:10 · from agent o", cli.format_events([ev], title=False))
        ev["data"] = {"sent_at": "junk"}
        self.assertIn(time.strftime("%H:%M", time.localtime(written + 45)), cli.format_events([ev], title=False))

    def test_watch_follow_keeps_printing(self):
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        result = {}
        t = threading.Thread(target=lambda: result.update(out=self.run_cli("--agent", "ag", "watch", "-f",
                                                                           "--timeout", "3")[1]))
        t.start()
        for i in range(2):
            self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
            self.req(op="publish", topic="hf/agent/x", summary=f"msg {i}")
            self.assertTrue(self.wait_for(lambda: self.req(op="list", agent="ag")["pending"] == 0))
        t.join(10)
        self.assertIn("msg 0", result["out"])
        self.assertIn("msg 1", result["out"])
        self.assertNotIn("agent-events watch", result["out"])  # nothing to restart
        self.assertIn("No more events within 3s", result["out"])

    def test_hook_command_quotes_a_path_with_spaces(self):
        with mock.patch.object(cli.config, "BIN", cli.Path("/Users/a b/agent-events/bin/agent-events")):
            cmd = cli._hook_command(watch=True)
        self.assertEqual(cmd, "'/Users/a b/agent-events/bin/agent-events' hook --watch")
        self.assertTrue(cli._is_our_hook(cmd, watch=True))

    def test_install_hooks(self):
        with open(self.settings, "w") as f:
            json.dump({"hooks": {"Stop": [{"hooks": [{"type": "command", "command": "/old/agent-events hook"}]},
                                          {"hooks": [{"type": "command", "command": "other"}]}]}}, f)
        self.assertFalse(cli.hook_watch_installed())
        self.install_hook_watch()
        hooks = self.read_settings()["hooks"]
        cmds = {ev: [h["command"].split("bin/")[-1] for g in gs for h in g["hooks"]] for ev, gs in hooks.items()}
        self.assertEqual(cmds, {"Stop": ["other", "agent-events hook --watch"],
                                "SessionStart": ["agent-events hook", "agent-events hook --watch"],
                                "UserPromptSubmit": ["agent-events hook", "agent-events hook --watch"],
                                "SessionEnd": ["agent-events hook"]})
        self.assertTrue(hooks["Stop"][1]["hooks"][0]["asyncRewake"])
        self.assertTrue(cli.hook_watch_installed())
        cli.install_hooks(remove=True)
        self.assertEqual(self.read_settings()["hooks"], {"Stop": [{"hooks": [{"type": "command",
                                                                                        "command": "other"}]}]})

    def test_hook_watch_wakes_with_events(self):
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        hook_in = json.dumps({"session_id": "ag", "hook_event_name": "Stop"})
        result, err = {}, io.StringIO()
        real_ensure = client.ensure_daemon

        def noisy_ensure(*a, **kw):
            print("agent-events: daemon is running older code, restarting it", file=sys.stderr)
            return real_ensure(*a, **kw)

        def run():
            with mock.patch.object(client, "ensure_daemon", noisy_ensure):
                result.update(r=self.run_cli("hook", "--watch", stdin=hook_in, stderr=err))
        t = threading.Thread(target=run)
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.passive))
        self.req(op="publish", topic="hf/agent/x", summary="hello", body="the body", actor="orchestrator")
        t.join(10)
        self.assertEqual(result["r"], (2, ""))
        self.assertTrue(err.getvalue().startswith(cli.HOOK_DELIVERY))
        self.assertIn("from orchestrator · hello\n    the body", err.getvalue())
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)  # acked

    def test_hook_watch_exits_quietly(self):
        hook_in = json.dumps({"session_id": "ag", "hook_event_name": "Stop"})
        self.assertEqual(self.run_cli("hook", "--watch", stdin=hook_in), (0, ""))  # no subscriptions
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_ENTRYPOINT": "sdk-cli"}):
            self.assertEqual(self.run_cli("hook", "--watch", stdin=hook_in), (0, ""))  # headless
        result = {}
        t = threading.Thread(target=lambda: result.update(r=self.run_cli("hook", "--watch", stdin=hook_in)))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.passive))
        self.req(op="unwatch", agent="ag")  # SessionEnd
        t.join(10)
        self.assertEqual(result["r"], (0, ""))
        with mock.patch.object(cli, "pid_alive", lambda pid: False), mock.patch.object(cli, "claude_pid", lambda: 1):
            self.assertEqual(self.run_cli("hook", "--watch", stdin=hook_in), (0, ""))  # Claude is gone

    def test_hook_with_hook_watch_installed_delivers_instead_of_nagging(self):
        self.install_hook_watch()
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        prompt = json.dumps({"session_id": "ag", "hook_event_name": "UserPromptSubmit"})
        self.assertEqual(self.run_cli("hook", stdin=prompt)[1], "")  # no watch, but no nag either
        self.req(op="publish", topic="hf/agent/x", summary="hello", body="the body", actor="orchestrator")
        ctx = json.loads(self.run_cli("hook", stdin=prompt)[1])["hookSpecificOutput"]["additionalContext"]
        self.assertTrue(ctx.startswith(cli.HOOK_DELIVERY))
        self.assertIn("the body", ctx)
        self.assertEqual(self.run_cli("hook", stdin=prompt)[1], "")  # delivered once
        end = json.dumps({"session_id": "ag", "hook_event_name": "SessionEnd"})
        self.assertEqual(self.run_cli("hook", stdin=end)[1], "")
        # An explicit watch says there is no need to restart it.
        self.req(op="publish", topic="hf/agent/x", summary="again")
        out = self.run_cli("--agent", "ag", "watch", "--timeout", "5")[1]
        self.assertIn(cli.NO_WATCH_NEEDED, out)

    def test_resume_then_post_is_delivered_in_a_turn(self):
        """`claude --resume` without a prompt: SessionStart context starts no turn, so the sync hook
        must not ack there; the SessionStart hook watch wakes the agent with the event instead."""
        self.install_hook_watch()
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        start = json.dumps({"session_id": "ag", "hook_event_name": "SessionStart", "source": "resume"})
        self.req(op="publish", topic="hf/agent/x", summary="hello", body="posted right after the resume")
        self.assertEqual(self.run_cli("hook", stdin=start)[1], "")  # nothing injected (and nothing acked)
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 1)
        err = io.StringIO()
        self.assertEqual(self.run_cli("hook", "--watch", stdin=start, stderr=err), (2, ""))
        self.assertIn("posted right after the resume", err.getvalue())
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)

    def test_session_start_without_hook_watch_shows_but_does_not_ack(self):
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        self.req(op="publish", topic="hf/agent/x", summary="hello", body="the body")
        start = json.dumps({"session_id": "ag", "hook_event_name": "SessionStart"})
        ctx = json.loads(self.run_cli("hook", stdin=start)[1])["hookSpecificOutput"]["additionalContext"]
        self.assertIn("the body", ctx)
        self.assertIn("still unread", ctx)
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 1)
        prompt = json.dumps({"session_id": "ag", "hook_event_name": "UserPromptSubmit"})
        self.assertIn("the body", self.run_cli("hook", stdin=prompt)[1])
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)

    def test_hook_without_hook_watch_delivers_pending_with_nag(self):
        self.run_cli("--agent", "ag", "subscribe", "hf/agent/x")
        self.req(op="publish", topic="hf/agent/x", summary="hello", body="the body")
        stop = json.dumps({"session_id": "ag", "hook_event_name": "Stop", "stop_hook_active": True})
        reason = json.loads(self.run_cli("hook", stdin=stop)[1])["reason"]
        self.assertIn("the body", reason)
        self.assertIn("run_in_background: `agent-events watch`", reason)


class QuietTests(CliTest):
    """FYI events (published with "quiet") never wake an agent; they go along with the next events
    that do, or into its next turn."""

    def setUp(self):
        super().setUp()
        self.req(op="subscribe", agent="ag", topic="hf/agent/x")

    def publish(self, summary, **kw):
        return self.req(op="publish", topic="hf/agent/x", summary=summary, **kw)

    def test_quiet_events_do_not_wake_a_watch_but_go_along_with_the_next(self):
        result = {}
        t = threading.Thread(target=lambda: result.update(watch_raw("ag", timeout=8)))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.watchers))
        self.publish("fyi one", quiet=True)
        self.publish("fyi two", data={"quiet": "true"})  # how Hyperfocus flags it
        time.sleep(1.0)
        self.assertTrue(t.is_alive(), "an FYI event woke the watch")
        self.publish("wake")
        t.join(10)
        self.assertEqual([e["summary"] for e in result["events"]], ["fyi one", "fyi two", "wake"])
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)

    def test_stop_take_leaves_fyi_alone_for_the_next_turn(self):
        self.publish("fyi", quiet=True)
        self.assertEqual(self.req(op="take", agent="ag", wake_only=True)["events"], [])
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 1)
        self.publish("wake")
        self.assertEqual([e["summary"] for e in self.req(op="take", agent="ag", wake_only=True)["events"]],
                         ["fyi", "wake"])

    def test_hooks(self):
        self.install_hook_watch()
        self.publish("fyi", body="just so you know", quiet=True)
        stop = json.dumps({"session_id": "ag", "hook_event_name": "Stop"})
        self.assertEqual(self.run_cli("hook", stdin=stop)[1], "")  # the turn ends; nothing woken
        prompt = json.dumps({"session_id": "ag", "hook_event_name": "UserPromptSubmit"})
        ctx = json.loads(self.run_cli("hook", stdin=prompt)[1])["hookSpecificOutput"]["additionalContext"]
        self.assertTrue(ctx.startswith(cli.FYI_DELIVERY))
        self.assertIn(" · FYI\n    just so you know", ctx)
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)
        # Mixed: the normal preamble, the FYI one still marked.
        self.publish("fyi 2", quiet=True)
        self.publish("wake 2")
        out = json.loads(self.run_cli("hook", stdin=stop)[1])
        self.assertEqual(out["decision"], "block")
        self.assertTrue(out["reason"].startswith(cli.HOOK_DELIVERY))
        self.assertIn("fyi 2 · FYI", out["reason"])

    def test_hook_watch_sleeps_through_fyi(self):
        hook_in = json.dumps({"session_id": "ag", "hook_event_name": "Stop"})
        result, err = {}, io.StringIO()
        t = threading.Thread(target=lambda: result.update(r=self.run_cli("hook", "--watch", stdin=hook_in,
                                                                         stderr=err)))
        t.start()
        self.assertTrue(self.wait_for(lambda: "ag" in self.daemon.passive))
        self.publish("fyi", quiet=True)
        time.sleep(1.0)
        self.assertTrue(t.is_alive(), "an FYI event woke the agent")
        self.publish("BLOCKED: need a decision")
        t.join(10)
        self.assertEqual(result["r"], (2, ""))
        self.assertTrue(err.getvalue().startswith(cli.HOOK_DELIVERY))
        self.assertIn("fyi · FYI", err.getvalue())
        self.assertIn("BLOCKED: need a decision", err.getvalue())

    def test_publish_cli_quiet(self):
        self.run_cli("publish", "hf/agent/x", "note", "--quiet")
        [e] = self.req(op="take", agent="ag")["events"]
        self.assertTrue(e["data"]["quiet"])


class FilterTests(DaemonTest):
    """A subscription with a filter (--about, --mine): only events mentioning those reach the
    agent, e.g. a busy deploy channel with just its own PRs."""

    def setUp(self):
        super().setUp()
        self.fake.things["chan"] = {"version": 0, "events": []}

    def sub(self, **flt):
        return self.req(op="subscribe", agent="ag", topic="fake/chan", filter=flt)

    def pending(self):
        return [e["summary"] for e in self.req(op="take", agent="ag")["events"]]

    def test_about_terms_and_skipping(self):
        self.sub(about=["app#123", "welcome email"])
        self.fake.change("chan", ("", "Deployed app#123: email copy fixed"), ("", "api deploy, all green"),
                         ("", "Welcome Email looks fine on prod"), ("", "app#1230 is unrelated"))
        self.daemon.poll("fake/chan")
        self.assertEqual(self.pending(), ["Deployed app#123: email copy fixed", "Welcome Email looks fine on prod"])
        # Nothing wanted: nothing waits (and the cursor moves past the noise).
        self.fake.change("chan", ("", "someone else's deploy"))
        self.daemon.poll("fake/chan")
        self.assertEqual(self.req(op="list", agent="ag")["pending"], 0)
        self.assertEqual(self.req(op="list", agent="ag")["subscriptions"][0]["filter"],
                         {"about": ["app#123", "welcome email"]})

    def test_mine_follows_the_prs_it_watches(self):
        self.store_sub("github/acme/app/123")
        self.sub(mine=True)
        self.fake.change("chan", ("", "QA: app#123 is live, login crash fixed"),
                         ("", "https://github.com/acme/app/pull/123 merged"), ("", "app#120 live"))
        self.daemon.poll("fake/chan")
        self.assertEqual(len(self.pending()), 2)

    def test_a_thread_counts_when_its_first_message_does(self):
        from agent_events.sources.base import Event
        self.sub(about=["#6306"])
        store = self.daemon.store
        store._insert_events([Event("fake/chan", "message", "QA: #6306 deployed", dedupe="msg:111"),
                              Event("fake/chan/111", "message", "reply: crash on login", dedupe="msg:112"),
                              Event("fake/chan/222", "message", "reply in another thread", dedupe="msg:223")], time.time())
        self.assertEqual(self.pending(), ["QA: #6306 deployed", "reply: crash on login"])

    def test_subscribing_again_without_a_filter_delivers_everything(self):
        self.sub(about=["x-term"])
        self.req(op="subscribe", agent="ag", topic="fake/chan")
        self.fake.change("chan", ("", "anything"))
        self.daemon.poll("fake/chan")
        self.assertEqual(self.pending(), ["anything"])

    def store_sub(self, topic):
        self.daemon.store.touch_agent("ag")
        self.daemon.store.add_sub("ag", topic)


class TransferTests(DaemonTest):
    """A conversation continued as a fresh one (Hyperfocus' clear context, or a handoff) keeps
    what it was subscribed to, filters included; its inbox stays its own."""

    def test_transfer_moves_subscriptions_but_not_push_topics(self):
        self.fake.things["pr"] = {"version": 0, "events": []}
        self.fake.things["chan"] = {"version": 0, "events": []}
        self.req(op="subscribe", agent="old", topic="fake/pr")
        self.req(op="subscribe", agent="old", topic="fake/chan", filter={"about": ["#12"]})
        self.req(op="subscribe", agent="old", topic="hf/agent/abc")
        r = self.req(op="transfer", **{"from": "old", "to": "new", "move": True})
        self.assertEqual(sorted(r["topics"]), ["fake/chan", "fake/pr"])
        new = {s["topic"]: s["filter"] for s in self.req(op="list", agent="new")["subscriptions"]}
        self.assertEqual(new, {"fake/pr": None, "fake/chan": {"about": ["#12"]}})
        old = [s["topic"] for s in self.req(op="list", agent="old")["subscriptions"]]
        self.assertEqual(old, ["hf/agent/abc"], "moved away; its inbox stays")
        # Events after the transfer reach the new conversation.
        self.fake.change("pr", ("", "review requested"))
        self.daemon.poll("fake/pr")
        self.assertEqual([e["summary"] for e in self.req(op="take", agent="new")["events"]], ["review requested"])

    def test_copy_keeps_the_old_ones(self):
        self.fake.things["pr"] = {"version": 0, "events": []}
        self.req(op="subscribe", agent="old", topic="fake/pr")
        self.req(op="transfer", **{"from": "old", "to": "new"})
        self.assertEqual(len(self.req(op="list", agent="old")["subscriptions"]), 1)
        self.assertFalse(self.req(op="transfer", **{"from": "old", "to": "old"})["ok"])


if __name__ == "__main__":
    unittest.main()


class DaemonEnvironmentTests(unittest.TestCase):
    """A daemon started by an agent's command serves every agent: none of that agent's identity."""

    def test_spawned_daemon_gets_no_agent_identity(self):
        agent = {"HF_SESSION_ID": "abc", "HF_SESSION_NAME": "builder", "CLAUDE_CODE_SESSION_ID": "c1", "HF_SOCKET": "/s"}
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, agent), \
                mock.patch.object(client.subprocess, "Popen") as popen, mock.patch.object(client, "ping", return_value={"pid": 1}), \
                mock.patch.object(config, "home", return_value=config.Path(tmp)):
            client.start_background()
        env = popen.call_args.kwargs["env"]
        self.assertNotIn("HF_SESSION_ID", env)
        self.assertNotIn("HF_SESSION_NAME", env)
        self.assertNotIn("CLAUDE_CODE_SESSION_ID", env)
        self.assertEqual(env["HF_SOCKET"], "/s")

    def test_no_launchd_job_for_a_state_dir_of_its_own(self):
        with mock.patch.dict(os.environ, {"AGENT_EVENTS_HOME": "/tmp/ae.x"}):
            self.assertIsNone(client.launchd_job())
