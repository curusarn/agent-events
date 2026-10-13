from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest import mock

from agent_events.sources import PollResult, Source, SourceError, TopicError, register


class FakeSource(Source):
    """fake/<thing>[/<sub>...]: the test controls what each poll of <thing> returns."""
    type = "fake"
    interval = 0.2
    min_spacing = 0.01

    def __init__(self):
        self.things: dict[str, dict] = {}  # thing -> {"version": n, "events": [(topic_suffix, summary)]}
        self.fail: set[str] = set()
        self.polls: dict[str, int] = {}

    def normalize(self, parts):
        if not parts:
            raise TopicError("expected fake/<thing>")
        return parts

    def poll_key(self, parts):
        return parts[:1]

    def poll(self, key, state):
        thing = key[0]
        self.polls[thing] = self.polls.get(thing, 0) + 1
        if thing in self.fail or thing not in self.things:
            raise SourceError(f"{thing} unavailable")
        spec = self.things[thing]
        ev = []
        if state is not None and state["version"] != spec["version"]:
            from agent_events.sources.base import EventList
            ev = EventList(f"fake/{thing}")
            for suffix, summary in spec["events"]:
                ev.add("change", summary, topic=f"fake/{thing}{suffix}")
            spec["events"] = []
        return PollResult({"version": spec["version"]}, ev, f"thing {thing} v{spec['version']}")

    def change(self, thing, *events):
        spec = self.things[thing]
        spec["version"] += 1
        spec["events"] = list(events)


class DaemonTest(unittest.TestCase):
    """Runs a real daemon in-process against a temp state dir."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ae", dir="/tmp")  # short: unix socket paths are length-limited
        self.old_env = dict(os.environ)
        os.environ["AGENT_EVENTS_HOME"] = self.tmp
        os.environ["AGENT_EVENTS_WATCH_SETTLE"] = "0.1"
        for k in ("AGENT_EVENTS_AGENT", "CLAUDE_PID", "AGENT_EVENTS_SOCKET"):
            os.environ.pop(k, None)
        from agent_events import client, config, daemon
        config.WATCH_SETTLE = 0.1
        self.log_patch = mock.patch.object(daemon, "log", lambda msg: None)
        self.log_patch.start()
        client._checked = False
        self.fake = FakeSource()
        register(self.fake)
        self.daemon = daemon.Daemon()
        self.server = daemon.Server(str(config.socket_path()), daemon.Handler)
        self.server.daemon = self.daemon
        self.daemon.server = self.server
        threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
        threading.Thread(target=self.daemon.poller, daemon=True).start()

    def tearDown(self):
        self.log_patch.stop()
        self.daemon.stop.set()
        self.server.shutdown()
        self.server.server_close()
        os.environ.clear()
        os.environ.update(self.old_env)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def req(self, **kw):
        from agent_events import client
        return client.raw_request(kw, timeout=10)

    def wait_for(self, predicate, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end:
            if predicate():
                return True
            time.sleep(0.05)
        return False
