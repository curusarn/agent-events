"""A poller that hangs is noticed and replaced in-process, and the orchestrator is told (2026-11-18:
the Mac's poller hung on the store lock and nothing polled for 16 hours)."""
from __future__ import annotations

import os
import threading
import time
import unittest
from unittest import mock

from helpers import DaemonTest

from agent_events import daemon
from agent_events.locks import TimedRLock


class WatchdogTests(DaemonTest):
    def setUp(self):
        super().setUp()
        self.alert_file = os.path.join(self.tmp, "alert.txt")
        os.environ["AGENT_EVENTS_ALERT_CMD"] = f"cat > {self.alert_file}"
        self.logged = []
        self.log_patch.stop()
        self.log_patch = mock.patch.object(daemon, "log", self.logged.append)
        self.log_patch.start()
        for name, value in (("STALL", 1.0), ("POLL_STALL", 1.0), ("WATCHDOG_EVERY", 0.2), ("KEY_WAIT", 0.5)):
            p = mock.patch.object(daemon, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.release = threading.Event()  # set at the end, so hung threads can finish
        self.addCleanup(self.release.set)
        self.fake.things["a"] = {"version": 0, "events": []}
        self.req(op="subscribe", agent="ag", topic="fake/a")
        threading.Thread(target=self.daemon.watchdog, daemon=True).start()

    def alert(self):
        if not os.path.exists(self.alert_file):
            return ""
        with open(self.alert_file) as f:
            return f.read()

    def test_hung_poller_loop_is_replaced_and_reported(self):
        real = self.daemon.active_keys
        hung = []

        def active_keys():
            if threading.current_thread().name != "MainThread" and not hung:
                hung.append(threading.current_thread())
                self.release.wait()  # the poller blocks here, like it did on the store lock
            return real()
        with mock.patch.object(self.daemon, "active_keys", active_keys):
            self.assertTrue(self.wait_for(lambda: hung, timeout=3))
            polls = self.fake.polls.get("a", 0)
            self.fake.change("a", ("", "while the poller hung"))
            # Within STALL plus a watchdog round: a new poller thread, and polls resume.
            self.assertTrue(self.wait_for(lambda: self.daemon.store.pending("ag"), timeout=5))
            self.assertGreater(self.fake.polls.get("a", 0), polls)
            self.assertTrue(self.wait_for(lambda: "polling had stopped" in self.alert(), timeout=5))
            self.assertIn("the poller loop made no round", self.alert())
            self.assertIn("no daemon restart", self.alert())
            stall_logs = [m for m in self.logged if m.startswith("watchdog: polling stopped")]
            self.assertEqual(len(stall_logs), 1)
            self.assertIn("active_keys", stall_logs[0])  # the stuck thread's Python stack
            # The hung poller, once it wakes up, sees it was replaced and quits.
            self.release.set()
            hung[0].join(3)
            self.assertFalse(hung[0].is_alive())

    def test_hung_poll_is_noticed_and_other_keys_keep_polling(self):
        self.fake.things["b"] = {"version": 0, "events": []}
        self.req(op="subscribe", agent="ag", topic="fake/b")
        real_poll = self.fake.poll

        def poll(key, state):
            if key[0] == "b" and state is not None and not self.release.is_set():
                self.release.wait()  # this poll never returns
            return real_poll(key, state)
        with mock.patch.object(self.fake, "poll", poll):
            self.daemon.store.schedule(["fake/b"], 0)
            self.assertTrue(self.wait_for(lambda: "fake/b" in self.daemon.inflight, timeout=3))
            self.assertTrue(self.wait_for(lambda: "polling had stopped" in self.alert(), timeout=5))
            self.assertIn("fake/b", self.alert())
            # Other keys poll on; a new poll of the stuck key gives up after KEY_WAIT instead of hanging.
            self.fake.change("a", ("", "a still polls"))
            self.assertTrue(self.wait_for(lambda: any(e["summary"] == "a still polls"
                                                      for e in self.daemon.store.pending("ag")), timeout=5))
            self.assertTrue(self.wait_for(lambda: "is still running after" in (self.daemon.store.target("fake/b")["error"] or ""),
                                          timeout=5))

    def test_sleep_is_not_a_hang(self):
        # The Mac slept for an hour with a poll in flight: the wall clock moved on, awake time didn't.
        self.daemon.inflight["fake/slept"] = time.monotonic()
        real = time.time
        with mock.patch("time.time", lambda: real() + 3600):
            self.assertIsNone(self.daemon.check_stall())
        self.assertIn("fake/slept", self.daemon.inflight)
        self.daemon.inflight["fake/slept"] = time.monotonic() - daemon.POLL_STALL - 1
        self.assertIn("1 poll(s) running", self.daemon.check_stall())

    def test_one_post_an_hour(self):
        self.daemon.poller_beat = time.monotonic() - 60
        self.assertIsNotNone(self.daemon.check_stall())
        self.assertTrue(self.wait_for(lambda: self.alert(), timeout=5))
        os.remove(self.alert_file)
        self.daemon.poller_beat = time.monotonic() - 60
        self.assertIsNotNone(self.daemon.check_stall())
        time.sleep(0.5)
        self.assertEqual(self.alert(), "")
        self.assertEqual(len(self.daemon.stalls), 2)

    def test_healthy_daemon_is_left_alone(self):
        time.sleep(1.5)
        self.assertEqual(self.daemon.poller_gen, 0)  # the setUp poller (not started by start_poller)
        self.assertEqual(self.alert(), "")


class TimedRLockTests(unittest.TestCase):
    def test_reentrant_and_exclusive(self):
        lock = TimedRLock()
        with lock:
            with lock:
                got = []
                t = threading.Thread(target=lambda: got.append(lock.acquire(timeout=0.2)))
                t.start()
                t.join()
                self.assertEqual(got, [False])
        t = threading.Thread(target=lambda: got.append(lock.acquire(timeout=0.2)) or lock.release())
        t.start()
        t.join()
        self.assertEqual(got, [False, True])
        with self.assertRaises(RuntimeError):
            lock.release()

    def test_condition_on_it(self):
        cond = threading.Condition(TimedRLock())
        ready = []

        def waiter():
            with cond:
                with cond:  # held twice across the wait
                    cond.wait_for(lambda: ready, timeout=5)
                    ready.append("woke")
        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)
        with cond:
            ready.append(1)
            cond.notify_all()
        t.join(5)
        self.assertEqual(ready, [1, "woke"])

    def test_a_lost_wakeup_costs_at_most_a_retry(self):
        """A waiter whose wakeup never comes (simulated: the lock is freed without waking anyone)
        still gets the lock within RETRY."""
        lock = TimedRLock()
        lock.acquire()
        got = []
        t = threading.Thread(target=lambda: got.append(lock.acquire() and time.monotonic()))
        with mock.patch("agent_events.locks.RETRY", 0.2):
            t.start()
            time.sleep(0.1)
            # Free the inner lock behind the waiter's back: no release() signal reaches it.
            lock._owner, lock._count = None, 0
            inner = lock._lock
            lock._lock = threading.Lock()
            freed = time.monotonic()
            t.join(3)
            inner.release()
        self.assertTrue(got and got[0] - freed < 1.0)


if __name__ == "__main__":
    unittest.main()
