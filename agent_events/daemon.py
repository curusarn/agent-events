"""The daemon: owns the database, polls watched things, and serves clients over a unix socket.

Protocol: newline-delimited JSON. The client sends one request object with an "op"; the
daemon answers with one object ({"ok": true, ...} or {"ok": false, "error": "..."}).
`watch` is the exception: the daemon holds the connection open until events are ready,
sends {"ok": true, "events": [...]}, and the client answers {"ack": <last id>} once it has
printed them, which advances the agent's cursor. An unacked batch is redelivered.

One watch per agent delivers. A new watch supersedes the current one, except that a passive
watch (`"passive": true`, run by the Claude Code hook) never takes over from a normal one: it
waits as a standby until the normal watch is gone. A newer passive watch replaces an older one.
"""
from __future__ import annotations

import fcntl
import json
import os
import select
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from . import config
from .locks import TimedRLock
from .sources import SOURCES, Event, SourceError, TopicError, is_push, matches, parse, poll_key
from .store import Store, is_urgent, wakes_agent

# Events about the source itself, not about the thing watched.
HEALTH_KINDS = ("error", "recovered")
# Consecutive failures before subscribers are told polling is broken (single blips are noise).
WAKE_WINDOW = 24 * 3600
ERROR_REPORT_AFTER = 3
# The watchdog: the poller loop goes round every second, and no poll takes longer than an `op read`
# Approve (340 s) plus its request. Past these, polling has stopped (2026-11-18: 16 hours unnoticed).
# A subscription that woke its agent this often in the last hour, with events a wait would hold,
# gets a note in the delivery offering a wait (at most once per NUDGE_EVERY).
BUSY_WAKES = int(config._float_env("AGENT_EVENTS_BUSY_WAKES", 6))
NUDGE_EVERY = config._float_env("AGENT_EVENTS_NUDGE_EVERY", 3600)
STALL = config._float_env("AGENT_EVENTS_STALL", 180)  # the poller loop made no round for this long
POLL_STALL = config._float_env("AGENT_EVENTS_POLL_STALL", 900)  # one poll has been running this long
WATCHDOG_EVERY = config._float_env("AGENT_EVENTS_WATCHDOG_EVERY", 30)
KEY_WAIT = config._float_env("AGENT_EVENTS_KEY_WAIT", 60)  # a poll waits this long for one of the same key
ALERT_EVERY = 3600.0  # at most one post to the orchestrator an hour, however often polling stalls


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", file=sys.stderr, flush=True)


def duration(seconds: float) -> str:
    return f"{seconds:.0f} s" if seconds < 120 else f"{seconds / 60:.0f} min"


def thread_stacks() -> str:
    """Every thread's Python stack, for the log when polling stalls."""
    names = {t.ident: t.name for t in threading.enumerate()}
    out = []
    for ident, frame in sys._current_frames().items():
        out.append(f"--- thread {names.get(ident, ident)}\n{''.join(traceback.format_stack(frame)).rstrip()}")
    return "\n".join(out)


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class Daemon:
    def __init__(self):
        self.store = Store(config.db_path())
        self.cond = threading.Condition(TimedRLock())
        self.generation = 0  # bumped whenever events or subscriptions change
        self.watchers: dict[str, object] = {}  # agent -> token of the watch that delivers its events
        self.passive: dict[str, object] = {}  # agent -> token of its newest passive (hook) watch
        self.inflight_batches: set[str] = set()  # agents with a batch sent to a watch but not yet acked
        self.key_locks: dict[str, TimedRLock] = defaultdict(TimedRLock)
        # Poll ages and the poller's beat are measured in awake time (time.monotonic(), which stops
        # while the machine sleeps, as socket timeouts do): on the wall clock, a poll in flight when
        # the Mac's lid closed looked hung for the whole sleep, and woke the watchdog on every wake
        # (4 false "polling stopped" alerts in 24 h, 2026-11-23/24).
        self.inflight: dict[str, float] = {}  # poll key -> when (awake time) its poll was handed to a worker
        self.inflight_lock = TimedRLock()
        self.stop = threading.Event()
        self.started = time.time()
        self.version = config.LOADED_VERSION
        self.last_active: set[str] = set()  # poll keys found active by the last poller pass
        self.holds_logged: dict[str, float] = {}  # source -> the hold last logged
        self.poller_gen = 0  # the poller thread whose generation this is runs; an older one quits
        self.poller_beat = time.monotonic()  # when (awake time) the poller loop last went round
        self.stalls: list[float] = []  # when the watchdog found polling stopped
        self.last_alert = 0.0

    def notify(self) -> None:
        with self.cond:
            self.generation += 1
            self.cond.notify_all()

    # --- polling --------------------------------------------------------------------

    def poll(self, key: str) -> None:
        """Poll one key and store the resulting events. Raises on failure."""
        failures = self.poll_many([key])
        if failures:
            raise failures[0][1]

    def poll_many(self, keys: list[str]) -> list[tuple[str, Exception]]:
        """Poll keys of one source, in one request where it can (Source.batch), and store each
        key's events or failure. Returns the failures."""
        source = SOURCES[keys[0].split("/", 1)[0]]
        locks = []
        for k in sorted(set(keys)):
            lock = self.key_locks[k]
            if not lock.acquire(timeout=KEY_WAIT):  # a poll of k is stuck: fail these and back off
                for held in locks:
                    held.release()
                e = SourceError(f"an earlier poll of {k} is still running after {duration(KEY_WAIT)} of waiting")
                for key in keys:
                    target = self.store.target(key)
                    state = json.loads(target["state"]) if target and target["state"] else None
                    self._failed(key, target, state, e, self.interval(source))
                return [(key, e) for key in keys]
            locks.append(lock)
        try:
            targets = [self.store.target(k) for k in keys]
            states = [json.loads(t["state"]) if t and t["state"] else None for t in targets]
            parts = [k.split("/")[1:] for k in keys]
            try:
                results = source.poll_batch(parts, states) if len(keys) > 1 else [source.poll(parts[0], states[0])]
            except Exception as e:
                results = [e] * len(keys)
            interval = self.interval(source)
            failures = []
            for key, target, state, result in zip(keys, targets, states, results):
                if isinstance(result, Exception):
                    failures.append((key, result))
                    self._failed(key, target, state, result, interval)
                else:
                    self._polled(key, target, state, result, interval)
            return failures
        finally:
            for lock in locks:
                lock.release()

    def _failed(self, key, target, state, e: Exception, interval: float) -> None:
        msg = str(e) or type(e).__name__
        if not isinstance(e, SourceError):
            log(f"poll {key} crashed:\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}")
        fails = (target["fails"] if target else 0) + 1
        events = []
        if state is not None and fails == ERROR_REPORT_AFTER:
            events.append(Event(key, "error", f"polling is failing: {msg}", dedupe=f"error@{time.time()}"))
        self.store.save_error(key, msg, fails, time.time() + min(interval * 2 ** min(fails, 6), 900), events)
        if events:
            self.notify()

    def _polled(self, key, target, state, result, interval: float) -> None:
        events = list(result.events) if state is not None else []
        if target and target["fails"] >= ERROR_REPORT_AFTER:
            events.append(Event(key, "recovered", "polling works again", dedupe=f"recovered@{time.time()}"))
        if self.store.save_poll(key, result.state, result.summary, time.time() + interval, events):
            self.notify()
        if events:
            log(f"{key}: {len(events)} event(s)")

    def interval(self, source) -> float:
        """Poll interval for one key of this source, stretched so that all active keys of the
        source together stay within its API budget (min_spacing seconds between requests)."""
        base = config.poll_interval(source.type, source.interval)
        n = sum(1 for k in self.last_active if k.split("/", 1)[0] == source.type)
        requests = -(-n // max(1, source.batch))
        return max(base, requests * source.min_spacing)

    def alive_agents(self) -> set[str]:
        now = time.time()
        with self.cond:
            watching = set(self.watchers)
        return {a["id"] for a in self.store.agents()
                if a["id"] in watching or now - a["last_seen"] < config.AGENT_IDLE
                or (now - a["last_seen"] < config.AGENT_TTL and pid_alive(a["pid"]))}

    def active_keys(self) -> set[str]:
        """Poll keys of every subscription, whether its agent runs or not: a stopped agent (e.g. one
        Hyperfocus stopped) must have what happened waiting for it when it is resumed, and its
        unread events show that it has something to do. Not for retired sessions (replaced in their
        process by /clear), and agents gone for AGENT_TTL are forgotten with their subscriptions."""
        retired = {a["id"] for a in self.store.agents() if a["retired"]}
        keys = set()
        for s in self.store.all_subs():
            if s["agent_id"] in retired:
                continue
            try:
                source, parts = parse(s["topic"])
            except TopicError:
                continue
            if not source.push:
                keys.add(poll_key(source, parts))
        return keys

    def _poll_bg(self, keys: list[str]) -> None:
        try:
            if self.stop.is_set():
                return
            for key, e in self.poll_many(keys):
                log(f"poll {key} failed: {e}")
        except Exception:
            log(f"poll {keys} crashed:\n{traceback.format_exc()}")
        finally:
            with self.inflight_lock:
                for k in keys:
                    self.inflight.pop(k, None)

    def due_batches(self, now: float) -> list[list[str]]:
        """Groups of active keys to poll now, one request each. A source that polls several keys in
        one request polls all its keys that work whenever one of them is due, so they share their
        next poll and a round takes ceil(keys / batch) requests. Failing keys keep their own backoff."""
        targets = self.store.targets()
        by_source: dict[str, list[str]] = defaultdict(list)
        for key in sorted(self.last_active):
            source = SOURCES.get(key.split("/", 1)[0])
            if source and source.hold_until <= now:
                by_source[source.type].append(key)
            elif source and self.holds_logged.get(source.type) != source.hold_until:
                self.holds_logged[source.type] = source.hold_until
                log(f"{source.type}: polling paused until {time.strftime('%H:%M:%S', time.localtime(source.hold_until))}: "
                    f"{source.hold_reason}")
        batches = []
        with self.inflight_lock:
            for stype, keys in by_source.items():
                keys = [k for k in keys if k not in self.inflight]
                due = [k for k in keys if k not in targets or targets[k]["next_poll"] <= now]
                if not due:
                    continue
                size = max(1, SOURCES[stype].batch)
                healthy = [k for k in keys if k in targets and not targets[k]["fails"]]
                if size > 1 and any(k not in targets or not targets[k]["fails"] for k in due):
                    due = sorted(set(due) | set(healthy))
                for i in range(0, len(due), size):
                    batches.append(due[i:i + size])
                    self.inflight.update(dict.fromkeys(due[i:i + size], time.monotonic()))
        return batches

    def start_poller(self) -> None:
        """Start a poller thread, with its own pool of poll workers. A poller started before it quits
        when it next goes round, so a stuck one that wakes up again doesn't poll twice."""
        self.poller_gen += 1
        self.poller_beat = time.monotonic()
        threading.Thread(target=self.poller, args=(self.poller_gen,), name=f"poller-{self.poller_gen}",
                         daemon=True).start()

    def poller(self, gen: int | None = None) -> None:
        gen = self.poller_gen if gen is None else gen
        pool = ThreadPoolExecutor(max_workers=4)
        last_maintenance = 0.0
        while not self.stop.is_set() and gen == self.poller_gen:
            self.poller_beat = time.monotonic()
            try:
                config.load_env_file()
                now = time.time()
                if now - last_maintenance > 600:
                    last_maintenance = now
                    self.maintenance()
                self.last_active = self.active_keys()
                for batch in self.due_batches(now):
                    pool.submit(self._poll_bg, batch)
            except Exception:
                log(f"poller loop error:\n{traceback.format_exc()}")
            self.stop.wait(1)
        pool.shutdown(wait=False, cancel_futures=True)

    # --- watchdog -------------------------------------------------------------------

    def watchdog(self) -> None:
        while not self.stop.wait(WATCHDOG_EVERY):
            try:
                self.check_stall()
            except Exception:
                log(f"watchdog error:\n{traceback.format_exc()}")

    def check_stall(self) -> str | None:
        """If polling stopped (the poller loop silent for STALL, or a poll running for POLL_STALL),
        start a new poller thread in this process and tell the orchestrator. The daemon itself is not
        restarted: a new process reads its tokens again, and 1Password refuses that while the operator
        is away or the Mac is locked. Returns what stalled, or None."""
        now = time.time()
        awake = time.monotonic()
        silent = awake - self.poller_beat
        stuck = []
        if self.inflight_lock.acquire(timeout=10):  # the watchdog itself must never hang on a lock
            try:
                stuck = sorted(k for k, t in self.inflight.items() if awake - t > POLL_STALL)
                for k in stuck:
                    self.inflight.pop(k, None)  # due again; the new poller's polls of it wait KEY_WAIT at most
            finally:
                self.inflight_lock.release()
        if silent <= STALL and not stuck:
            return None
        if silent > STALL:
            what = (f"the poller loop made no round for {duration(silent)} "
                    f"(since {time.strftime('%H:%M', time.localtime(now - silent))})")
        else:
            what = (f"{len(stuck)} poll(s) running for over {duration(POLL_STALL)} "
                    f"({', '.join(stuck[:3])}{', …' if len(stuck) > 3 else ''})")
        self.stalls = [t for t in self.stalls if now - t < 86400] + [now]
        stacks = thread_stacks()
        log(f"watchdog: polling stopped: {what}; starting a new poller thread\n{stacks}")
        self.start_poller()
        if now - self.last_alert >= ALERT_EVERY:
            self.last_alert = now
            again = (f" Polling stopped {len(self.stalls)} times in 24 h: if a new poller doesn't help, the daemon "
                     f"needs a restart, done only while the operator is at the Mac (a restart reads the tokens again)."
                     if len(self.stalls) > 1 else "")
            self.alert(f"agent-events on {socket.gethostname()}: polling had stopped: {what}. The daemon started a "
                       f"new poller thread by itself (no daemon restart, tokens kept), so polls resume within a minute."
                       f"{again} Python stacks of the stuck threads are in {config.log_path()}.")
        return what

    def alert(self, text: str) -> None:
        """Post to the orchestrator: AGENT_EVENTS_ALERT_CMD (a shell command, the text on stdin), else
        Hyperfocus's `hf post --to orchestrator` where hf is installed, posting as the job agent-events."""
        cmd = os.environ.get("AGENT_EVENTS_ALERT_CMD")
        hf = shutil.which("hf") or str(config.home().parent / ".hyperfocus" / "bin" / "hf")
        if cmd:
            args, shell = cmd, True
        elif os.access(hf, os.X_OK):
            args, shell = [hf, "post", "--to", "orchestrator", "--file", "-"], False
        else:
            log(f"alert (nowhere to post it): {text}")
            return
        env = dict(config.daemon_env(), HF_JOB="agent-events")

        def send():
            try:
                out = subprocess.run(args, shell=shell, input=text, capture_output=True, text=True, env=env,
                                     timeout=60)
                log(f"alert posted: {text}" if out.returncode == 0 else
                    f"alert failed ({out.returncode}): {(out.stderr or out.stdout).strip()[:300]}")
            except (OSError, subprocess.TimeoutExpired) as e:
                log(f"alert failed: {e}")
        threading.Thread(target=send, name="alert", daemon=True).start()

    def maintenance(self) -> None:
        now = time.time()
        with self.cond:
            watching = set(self.watchers)
        for a in self.store.agents():
            if a["id"] not in watching and now - a["last_seen"] > config.AGENT_TTL:
                log(f"forgetting agent {a['id']} (inactive since {time.ctime(a['last_seen'])})")
                self.store.delete_agent(a["id"])
        self.store.prune_events(now - config.EVENT_TTL)
        self.store.prune_wakes(now - 86400)
        for source in SOURCES.values():
            n = source.requests_last_hour()
            if n:
                log(f"{source.type}: {n} requests in the last hour")
        wanted = set()
        for s in self.store.all_subs():
            try:
                wanted.add(poll_key(*parse(s["topic"])))
            except TopicError:
                pass
        for key, t in self.store.targets().items():
            if key not in wanted and now - (t["last_poll"] or 0) > 3600:
                self.store.delete_target(key)

    # --- requests -------------------------------------------------------------------

    def dispatch(self, req: dict) -> dict:
        op = req.get("op")
        handler = getattr(self, f"op_{op}", None) if op and op != "watch" else None
        if not handler:
            return {"ok": False, "error": f"unknown op {op!r}"}
        try:
            return handler(req)
        except (TopicError, SourceError) as e:
            return {"ok": False, "error": str(e)}

    def _agent(self, req: dict) -> str:
        agent = req.get("agent")
        if not agent:
            raise TopicError("missing agent id")
        self.store.touch_agent(agent, req.get("pid"), req.get("cwd"))
        return agent

    def _annotate(self, events: list[dict]) -> list[dict]:
        """Attach the watched thing's one-line summary (PR title, ticket, thread) to each event."""
        targets = self.store.targets()
        for e in events:
            try:
                t = targets.get(poll_key(*parse(e["topic"])))
            except TopicError:
                t = None
            e["target"] = t["summary"] if t else ""
        return events

    def _pending(self, agent: str, limit: int = 100) -> list[dict]:
        """The agent's pending events, with those its waits hold back marked (held_until)."""
        return self.store.mark_held(agent, self.store.pending(agent, limit))

    def _woke(self, agent: str, events: list[dict]) -> list[dict]:
        """These events wake the agent now: count the wake against each subscription a wait could
        have batched (an urgent event wakes it whatever the wait), and note a busy one."""
        subs = self.store.subs(agent)
        by_topic = {s["topic"]: s for s in subs}
        waking = {}
        for e in events:
            sub = self.store.sub_of(subs, e["topic"])
            if sub and by_topic[sub]["wait"]:
                e["wait"] = by_topic[sub]["wait"]
            if sub and wakes_agent(e) and not is_urgent(e) and not e.get("wait"):
                waking.setdefault(sub, e)
        if not waking:
            return events
        self.store.record_wake(agent, list(waking))
        counts = self.store.wake_counts(agent, time.time() - 3600)
        for sub, first in waking.items():
            n = counts.get(sub, 0)
            nudged = by_topic[sub]["nudged_at"] or 0
            if n >= BUSY_WAKES and time.time() - nudged > NUDGE_EVERY:
                first["busy"], first["busy_topic"] = n, sub
                self.store.nudged(agent, sub)
        return events

    def op_ping(self, req):
        return {"ok": True, "pid": os.getpid(), "started": self.started, "version": self.version}

    def op_subscribe(self, req):
        agent = self._agent(req)
        source, parts = parse(req["topic"])
        # Only events about these (e.g. a busy deploy channel: just my PRs).
        flt = {k: v for k, v in (req.get("filter") or {}).items() if k in ("about", "mine") and v} or None
        if source.push:  # nothing to poll or validate remotely; events are published into it
            topic = "/".join([source.type] + parts)
            created = self.store.add_sub(agent, topic, flt)
            if req.get("wait") is not None:
                self.store.set_wait(agent, topic, float(req["wait"]))
            self.notify()
            log(f"{agent} subscribed to {topic}" if created else f"{agent} re-subscribed to {topic}")
            return {"ok": True, "topic": topic, "new": created, "summary": "", "detail": ""}
        key = poll_key(source, parts)
        target = self.store.target(key)
        interval = self.interval(source)
        # Poll now if we have no baseline, or it is stale (so catch-up changes from before this
        # subscription aren't delivered to it as if they were new).
        if not target or not target["state"] or time.time() - (target["last_poll"] or 0) > 2 * interval:
            try:
                self.poll(key)
            except Exception as e:
                if not target or not target["state"]:
                    if not self._key_in_use(key):
                        self.store.delete_target(key)
                    return {"ok": False, "error": f"cannot watch {key}: {e}"}
            target = self.store.target(key)
        if not target or not target["state"]:
            return {"ok": False, "error": f"cannot watch {key}: no data yet, try again"}
        canonical, extra = source.resolve(parts, json.loads(target["state"]))
        topic = "/".join([source.type] + canonical)
        created = self.store.add_sub(agent, topic, flt)
        if req.get("wait") is not None:
            self.store.set_wait(agent, topic, float(req["wait"]))
        self.notify()
        log(f"{agent} subscribed to {topic}" if created else f"{agent} re-subscribed to {topic}")
        return {"ok": True, "topic": topic, "new": created, "summary": target["summary"], "detail": extra}

    def _key_in_use(self, key: str) -> bool:
        for s in self.store.all_subs():
            try:
                if poll_key(*parse(s["topic"])) == key:
                    return True
            except TopicError:
                pass
        return False

    def op_unsubscribe(self, req):
        agent = self._agent(req)
        topics = None
        if not req.get("all"):
            topics = []
            for raw in req.get("topics") or []:
                try:
                    source, parts = parse(raw)
                    target = self.store.target(poll_key(source, parts))
                    if target and target["state"]:
                        try:
                            parts, _ = source.resolve(parts, json.loads(target["state"]))
                        except TopicError:
                            pass
                    topics.append("/".join([source.type] + parts))
                except TopicError:
                    topics.append(raw.strip().strip("/"))  # still allow removing odd leftovers
        removed = self.store.remove_subs(agent, topics)
        self.notify()
        return {"ok": True, "removed": removed}

    def op_wait(self, req):
        """Set (seconds > 0) or clear (0) the wait on one of the agent's subscriptions: its events
        wake the agent together once the wait has passed since the first of them arrived."""
        agent = self._agent(req)
        raw = (req.get("topic") or "").strip()
        seconds = float(req.get("seconds") or 0)
        subs = [s["topic"] for s in self.store.subs(agent)]
        topic = raw.strip("/")
        try:
            source, parts = parse(raw)
            topic = "/".join([source.type] + parts)
            if not source.push:
                target = self.store.target(poll_key(source, parts))
                if target and target["state"]:
                    try:
                        topic = "/".join([source.type] + source.resolve(parts, json.loads(target["state"]))[0])
                    except TopicError:
                        pass
        except TopicError:
            pass
        if topic not in subs:
            listed = "\n  ".join(subs) or "(none)"
            raise TopicError(f"not subscribed to {topic}; your subscriptions:\n  {listed}")
        self.store.set_wait(agent, topic, seconds)
        self.notify()  # a shorter wait may make held events due now
        log(f"{agent} set a {seconds:g}s wait on {topic}" if seconds > 0 else f"{agent} cleared the wait on {topic}")
        return {"ok": True, "topic": topic, "wait": seconds if seconds > 0 else None}

    def op_transfer(self, req):
        """Hand one agent's subscriptions (with their filters) to another: a conversation that
        continues as a fresh one (Hyperfocus' clear context), or an agent's next phase. Push topics
        are left out (each agent has its own inbox). "move": the old agent loses them."""
        old, new = req.get("from") or "", req.get("to") or ""
        if not old or not new or old == new:
            raise TopicError("transfer needs a different 'from' and 'to' agent")
        self.store.touch_agent(new)
        moved = []
        for s in self.store.subs(old):
            try:
                if parse(s["topic"])[0].push:
                    continue
            except TopicError:
                pass
            self.store.add_sub(new, s["topic"], json.loads(s["filter"]) if s["filter"] else None)
            if s["wait"]:
                self.store.set_wait(new, s["topic"], s["wait"])
            moved.append(s["topic"])
        if req.get("move") and moved:
            self.store.remove_subs(old, moved)
        self.notify()
        log(f"transferred {len(moved)} subscription(s) from {old} to {new}")
        return {"ok": True, "topics": moved}

    def op_list(self, req):
        agent = self._agent(req)
        targets = self.store.targets()
        out = []
        for s in self.store.subs(agent):
            push = False
            try:
                source, parts = parse(s["topic"])
                push = source.push
                t = None if push else targets.get(poll_key(source, parts))
            except TopicError:
                t = None
            out.append({"topic": s["topic"], "since": s["created_at"], "push": push,
                        "summary": t["summary"] if t else None, "last_poll": t["last_poll"] if t else None,
                        "error": t["error"] if t else None,
                        "filter": json.loads(s["filter"]) if s["filter"] else None, "wait": s["wait"]})
        with self.cond:
            watching = agent in self.watchers
        return {"ok": True, "agent": agent, "subscriptions": out, "watching": watching,
                "pending": len(self.store.pending(agent))}

    def op_agent_status(self, req):
        agent = req.get("agent") or ""
        with self.cond:
            watching = agent in self.watchers
        subs = [s["topic"] for s in self.store.subs(agent)]
        return {"ok": True, "subscriptions": subs, "watching": watching}

    def op_take(self, req):
        """Hand the agent's pending events to the caller and ack them at once (the Claude Code hook
        injects them into the conversation). Returns nothing while a normal watch is connected or
        a batch is on its way to a watch, so events are never delivered twice. With "ack": false
        they are only shown and stay pending (the conversation may not act on them yet). With
        "wake_only" (the Stop hook: handing events over there continues the turn) only when at
        least one of them is not FYI; FYI events alone wait for the next turn."""
        agent = self._agent(req)
        with self.cond:
            owner = self.watchers.get(agent)
            if agent in self.inflight_batches or (owner is not None and owner is not self.passive.get(agent)):
                return {"ok": True, "events": []}
            events = self._annotate(self._pending(agent))
            if req.get("wake_only") and not any(wakes_agent(e) for e in events):
                return {"ok": True, "events": []}
            if events and req.get("ack", True):
                if req.get("wake_only"):
                    self._woke(agent, events)
                self.store.ack(agent, max(e["id"] for e in events))
        return {"ok": True, "events": events}

    def op_unread(self, req):
        """Per agent, what is waiting for it (pending, not FYI): count, the oldest event's time
        (the source's), the newest id and topics. For apps that remind about stopped agents; it
        does not touch the agents. `agents` limits the answer; `exclude` drops topic prefixes
        (Hyperfocus counts its own hf/ posts itself); `since` (epoch seconds) drops events that
        happened before it; `events: N` also returns up to N of them (oldest first).

        Events a wait held back count from when it ended (their due time), not when they happened.
        Not counted, since there is nothing in them to act on: source-health events ("polling is
        failing" / "works again"), and events that happened before the last one the agent was
        handed (late, out-of-order arrivals of old news)."""
        want = req.get("agents")
        exclude = tuple(req.get("exclude") or ())
        since = float(req.get("since") or 0)
        out = {}
        for agent in sorted({s["agent_id"] for s in self.store.all_subs()}):
            if want is not None and agent not in want:
                continue
            floor = max(since, self.store.cursor_time(agent))
            events = [e for e in self._pending(agent, limit=1000)
                      if wakes_agent(e) and e["kind"] not in HEALTH_KINDS
                      and not (exclude and e["topic"].startswith(exclude)) and e["created_at"] >= floor]
            if events:
                # A held event has waited only since its wait ended: Hyperfocus's idle check
                # (WakeWatch) nudges an agent once its oldest event is 3 minutes old, and held
                # ones came due looking 30 minutes old, before the hook could deliver them.
                out[agent] = {"count": len(events), "oldest": min(max(e["created_at"], e.get("due_at") or 0)
                                                                  for e in events),
                              "newest_id": max(e["id"] for e in events),
                              "topics": sorted({e["topic"] for e in events})[:10]}
                n = int(req.get("events") or 0)
                if n > 0:
                    out[agent]["events"] = [{k: e[k] for k in ("id", "topic", "kind", "summary", "url", "actor",
                                                                "created_at")} for e in events[:n]]
        return {"ok": True, "agents": out}

    def op_wakes(self, req):
        """Per agent, how many pending comments are worth resuming it for, stopped: first seen here
        in the last WAKE_WINDOW (seen, not written: a comment that arrives after days offline still
        counts) and written after `since`. Only reads the flagged comments, so an app can ask about
        every agent each minute; `unread` loads each agent's whole backlog (16.8s for 75 agents)."""
        return {"ok": True, "agents": self.store.wakes(time.time() - WAKE_WINDOW, float(req.get("since") or 0))}

    def op_unwatch(self, req):
        """End the agent's watches (its Claude Code session ended)."""
        agent = req.get("agent") or ""
        with self.cond:
            self.watchers.pop(agent, None)
            self.passive.pop(agent, None)
            self.generation += 1
            self.cond.notify_all()
        return {"ok": True}

    def op_session_start(self, req):
        """A Claude Code session started in process `pid`: any other agent recorded with that
        pid is a previous session of the same process (e.g. before /clear, or a resume into
        another conversation) and is no longer running. It is the same agent continuing, so its
        subscriptions (filters included; not push topics, each session has its own inbox) move
        to the new session, and the old one is retired."""
        agent, pid = req.get("agent"), req.get("pid")
        if not agent or not pid:
            return {"ok": True, "retired": [], "moved": []}
        retired = [a["id"] for a in self.store.agents() if a["pid"] == pid and a["id"] != agent]
        moved = []
        for a in retired:
            moved += self.op_transfer({"from": a, "to": agent, "move": True})["topics"]
            self.store.x("UPDATE agents SET pid = NULL, last_seen = MIN(last_seen, ?), retired = ? WHERE id = ?",
                         (time.time() - config.AGENT_IDLE, time.time(), a))
        if self.store.subs(agent):
            self.store.touch_agent(agent, pid)
        return {"ok": True, "retired": retired, "moved": moved}

    def op_log(self, req):
        agent = self._agent(req)
        topic = None
        if req.get("topic"):
            source, parts = parse(req["topic"])
            topic = "/".join([source.type] + parts)
        return {"ok": True, "events": self._annotate(self.store.recent(agent, int(req.get("limit") or 20), topic))}

    def op_refresh(self, req):
        """Poll an agent's topics right now, or only `topics` (any already polled: Hyperfocus asks
        for a PR the moment it is marked ready, so its tab doesn't show the draft it just left)."""
        topics = req.get("topics")
        if topics is None:
            topics = [s["topic"] for s in self.store.subs(self._agent(req))]
        keys = set()
        for t in topics:
            try:
                source, parts = parse(t)
            except TopicError:
                continue
            if not source.push:
                keys.add(poll_key(source, parts))
        self.store.schedule(sorted(keys), 0)
        return {"ok": True, "keys": sorted(keys)}

    def op_publish(self, req):
        """Publish an event into a push topic (e.g. a Hyperfocus message to an agent's inbox).
        Not tied to an agent: the publisher is usually the Hyperfocus app, not a subscriber."""
        source, parts = parse(req.get("topic") or "")
        topic = "/".join([source.type] + parts)
        if not source.push:
            push = ", ".join(f"{s.type}/..." for s in SOURCES.values() if s.push)
            raise TopicError(f"cannot publish to {topic}: {source.type} topics are polled, not published "
                             f"into; only push topics ({push}) accept published events")
        summary = req.get("summary")
        if not isinstance(summary, str) or not summary.strip():
            raise TopicError("publish needs a non-empty summary")
        data = req.get("data") or {}
        if not isinstance(data, dict):
            raise TopicError("publish data must be a JSON object")
        if req.get("quiet"):
            data = dict(data, quiet=True)
        dedupe = str(req.get("id") or "") or uuid.uuid4().hex
        event = Event(topic, str(req.get("kind") or "message"), summary.strip(), body=str(req.get("body") or ""),
                      url=str(req.get("url") or ""), actor=str(req.get("actor") or ""), dedupe=dedupe, data=data)
        event_id = self.store.insert_event(event)
        if event_id is not None:
            self.notify()
            log(f"published #{event_id} to {topic}" + (f" from {event.actor}" if event.actor else ""))
        subscribers = sorted({s["agent_id"] for s in self.store.all_subs() if matches(s["topic"], topic)})
        with self.cond:
            watching = [a for a in subscribers if a in self.watchers]
        return {"ok": True, "id": event_id if event_id is not None else self.store.event_id(topic, dedupe),
                "topic": topic, "duplicate": event_id is None, "subscribers": subscribers, "watching": watching}

    def op_status(self, req):
        now = time.time()
        with self.cond:
            watching = set(self.watchers)
        alive = self.alive_agents()
        subs = self.store.all_subs()
        agents = []
        for a in self.store.agents():
            agents.append({"id": a["id"], "cwd": a["cwd"], "last_seen": a["last_seen"], "watching": a["id"] in watching,
                           "alive": a["id"] in alive,
                           "subscriptions": [s["topic"] for s in subs if s["agent_id"] == a["id"]]})
        targets = [{"key": k, "summary": t["summary"], "last_poll": t["last_poll"], "error": t["error"],
                    "next_poll_in": max(0, (t["next_poll"] or 0) - now)} for k, t in self.store.targets().items()]
        requests = {s.type: n for s in SOURCES.values() if (n := s.requests_last_hour()) is not None}
        return {"ok": True, "pid": os.getpid(), "started": self.started, "version": self.version,
                "agents": agents, "targets": targets, "active_keys": sorted(self.active_keys()),
                "requests_last_hour": requests}

    def op_shutdown(self, req):
        log("shutdown requested")
        threading.Thread(target=self.shutdown, daemon=True).start()
        return {"ok": True}

    def shutdown(self) -> None:
        self.stop.set()
        self.notify()
        server = getattr(self, "server", None)
        if server:
            server.shutdown()

    # --- watch ----------------------------------------------------------------------

    def op_watch_stream(self, req: dict, handler: "Handler") -> None:
        agent = self._agent(req)
        timeout = req.get("timeout")
        deadline = time.time() + float(timeout) if timeout else None
        passive = bool(req.get("passive"))
        token = object()
        with self.cond:
            if passive:
                if self.watchers.get(agent) in (None, self.passive.get(agent)):
                    self.watchers[agent] = token
                self.passive[agent] = token
            else:
                self.watchers[agent] = token
            self.generation += 1
            self.cond.notify_all()  # wakes an older watch of this agent so it can step aside
        last_touch = time.time()
        try:
            while True:
                with self.cond:
                    generation = self.generation
                    owner = self.watchers.get(agent)
                    if passive and self.passive.get(agent) is token and owner is None:
                        self.watchers[agent] = owner = token  # the normal watch is gone; take over
                    superseded = self.passive.get(agent) is not token if passive else owner is not token
                    standby = passive and owner is not token
                if superseded:
                    handler.send({"ok": True, "events": [], "superseded": True})
                    return
                if self.stop.is_set():
                    return
                if not self.store.subs(agent):
                    handler.send({"ok": True, "events": [], "no_subscriptions": True})
                    return
                pending = [] if standby else self._pending(agent)
                # FYI events never wake the agent: they go along with the next ones that do. Nor
                # do events a wait holds back, until it is over.
                if any(wakes_agent(e) for e in pending):
                    # Let a burst (e.g. a review with many comments) land. Published messages are
                    # already complete, so those only wait a moment.
                    push_only = all(is_push(e["topic"]) for e in pending)
                    time.sleep(config.PUSH_SETTLE if push_only else config.WATCH_SETTLE)
                    with self.cond:
                        if self.watchers.get(agent) is not token or agent in self.inflight_batches:
                            continue  # another watch owns delivery now, or is still delivering
                        events = self._pending(agent)
                        if not any(wakes_agent(e) for e in events):
                            continue  # taken by the hook in the meantime
                        self.inflight_batches.add(agent)
                    try:
                        handler.send({"ok": True, "events": self._woke(agent, self._annotate(events))})
                        ack = handler.read(timeout=120)
                        if ack and ack.get("ack"):
                            self.store.ack(agent, int(ack["ack"]))
                    finally:
                        with self.cond:
                            self.inflight_batches.discard(agent)
                    return
                if deadline and time.time() >= deadline:
                    handler.send({"ok": True, "events": [], "timeout": True})
                    return
                if handler.closed():
                    return
                if time.time() - last_touch > 60:
                    self.store.touch_agent(agent)
                    last_touch = time.time()
                # Wake up when the first event a wait holds back is due.
                due = min((e["held_until"] for e in pending if e.get("held_until")), default=None)
                with self.cond:
                    if self.generation == generation:
                        wait = 5.0 if deadline is None else max(0.0, min(5.0, deadline - time.time()))
                        if due is not None:
                            wait = max(0.02, min(wait, due - time.time()))
                        self.cond.wait(wait)
        except (BrokenPipeError, ConnectionResetError, OSError):
            return
        finally:
            with self.cond:
                if self.watchers.get(agent) is token:
                    del self.watchers[agent]
                if self.passive.get(agent) is token:
                    del self.passive[agent]
                self.generation += 1
                self.cond.notify_all()  # a passive standby may take over now


class Handler(socketserver.StreamRequestHandler):
    def send(self, obj: dict) -> None:
        self.wfile.write(json.dumps(obj).encode() + b"\n")
        self.wfile.flush()

    def read(self, timeout: float | None = None) -> dict | None:
        self.connection.settimeout(timeout)
        try:
            line = self.rfile.readline()
        except (socket.timeout, OSError):
            return None
        finally:
            self.connection.settimeout(None)
        try:
            return json.loads(line) if line else None
        except ValueError:
            return None

    def closed(self) -> bool:
        """True if the client went away (e.g. the agent's background watch was killed)."""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable) and self.connection.recv(1, socket.MSG_PEEK) == b""
        except OSError:
            return True

    def handle(self) -> None:
        daemon: Daemon = self.server.daemon  # type: ignore[attr-defined]
        req = self.read(timeout=30)
        if not req:
            return
        try:
            if req.get("op") == "watch":
                daemon.op_watch_stream(req, self)
            else:
                self.send(daemon.dispatch(req))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            log(f"request {req.get('op')} crashed:\n{traceback.format_exc()}")
            try:
                self.send({"ok": False, "error": f"daemon error: {e}"})
            except OSError:
                pass


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


def run(wait_lock: bool = False) -> int:
    """Run the daemon in the foreground. Returns an exit code.

    With wait_lock (used by launchd) it waits as a standby while another daemon runs.
    """
    # Started from an agent's shell (`daemon --replace`): gh and op run without its identity.
    for key in config.AGENT_ENV:
        os.environ.pop(key, None)
    home = config.home()
    home.mkdir(parents=True, exist_ok=True)
    lock_file = open(config.lock_path(), "a+")
    for attempt in range(10):  # clients briefly probe the lock; don't mistake that for a daemon
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | (0 if wait_lock else fcntl.LOCK_NB))
            break
        except BlockingIOError:
            if attempt == 9:
                print("agent-events daemon is already running", file=sys.stderr)
                return 1
            time.sleep(0.05)
    if wait_lock and config.code_version() > config.LOADED_VERSION:
        # The code changed while we waited; restart so we serve the current version.
        # (The lock fd is not inherited across exec, so the new process competes for it again.)
        os.execv(sys.executable, [sys.executable, str(config.BIN), "daemon", "--foreground", "--wait-lock"])
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()

    config.load_env_file()
    sock = config.socket_path()
    try:
        sock.unlink()
    except FileNotFoundError:
        pass
    daemon = Daemon()
    server = Server(str(sock), Handler)
    os.chmod(sock, 0o600)
    server.daemon = daemon  # type: ignore[attr-defined]
    daemon.server = server  # type: ignore[attr-defined]

    def on_signal(signum, frame):
        log(f"got signal {signum}, shutting down")
        threading.Thread(target=daemon.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    daemon.start_poller()
    threading.Thread(target=daemon.watchdog, name="watchdog", daemon=True).start()
    log(f"agent-events daemon started (pid {os.getpid()}, socket {sock}, db {config.db_path()})")
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        daemon.stop.set()
        server.server_close()
        try:
            if sock.exists():
                sock.unlink()
        except OSError:
            pass
        log("agent-events daemon stopped")
    # Don't wait for in-flight polls (gh / op subprocesses) at interpreter exit; the DB is
    # consistent after every commit.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
