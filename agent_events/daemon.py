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
import signal
import socket
import socketserver
import sys
import threading
import time
import traceback
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from . import config
from .sources import SOURCES, Event, SourceError, TopicError, is_push, matches, parse, poll_key
from .store import Store, is_quiet

# Events about the source itself, not about the thing watched.
HEALTH_KINDS = ("error", "recovered")
# Consecutive failures before subscribers are told polling is broken (single blips are noise).
ERROR_REPORT_AFTER = 3


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", file=sys.stderr, flush=True)


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
        self.cond = threading.Condition()
        self.generation = 0  # bumped whenever events or subscriptions change
        self.watchers: dict[str, object] = {}  # agent -> token of the watch that delivers its events
        self.passive: dict[str, object] = {}  # agent -> token of its newest passive (hook) watch
        self.inflight_batches: set[str] = set()  # agents with a batch sent to a watch but not yet acked
        self.key_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self.inflight: set[str] = set()
        self.inflight_lock = threading.Lock()
        self.stop = threading.Event()
        self.started = time.time()
        self.version = config.LOADED_VERSION
        self.last_active: set[str] = set()  # poll keys found active by the last poller pass

    def notify(self) -> None:
        with self.cond:
            self.generation += 1
            self.cond.notify_all()

    # --- polling --------------------------------------------------------------------

    def poll(self, key: str) -> None:
        """Poll one key and store the resulting events. Raises on failure."""
        source = SOURCES[key.split("/", 1)[0]]
        parts = key.split("/")[1:]
        with self.key_locks[key]:
            target = self.store.target(key)
            state = json.loads(target["state"]) if target and target["state"] else None
            try:
                result = source.poll(parts, state)
            except Exception as e:
                msg = str(e) or type(e).__name__
                if not isinstance(e, SourceError):
                    log(f"poll {key} crashed:\n{traceback.format_exc()}")
                fails = (target["fails"] if target else 0) + 1
                interval = self.interval(source)
                events = []
                if state is not None and fails == ERROR_REPORT_AFTER:
                    events.append(Event(key, "error", f"polling is failing: {msg}", dedupe=f"error@{time.time()}"))
                self.store.save_error(key, msg, fails, time.time() + min(interval * 2 ** min(fails, 6), 900), events)
                if events:
                    self.notify()
                raise
            events = list(result.events) if state is not None else []
            if target and target["fails"] >= ERROR_REPORT_AFTER:
                events.append(Event(key, "recovered", "polling works again", dedupe=f"recovered@{time.time()}"))
            interval = self.interval(source)
            if self.store.save_poll(key, result.state, result.summary, time.time() + interval, events):
                self.notify()
            if events:
                log(f"{key}: {len(events)} event(s)")

    def interval(self, source) -> float:
        """Poll interval for one key of this source, stretched so that all active keys of the
        source together stay within its API budget (min_spacing seconds between requests)."""
        base = config.poll_interval(source.type, source.interval)
        n = sum(1 for k in self.last_active if k.split("/", 1)[0] == source.type)
        return max(base, n * source.min_spacing)

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

    def _poll_bg(self, key: str) -> None:
        try:
            if self.stop.is_set():
                return
            self.poll(key)
        except Exception as e:
            log(f"poll {key} failed: {e}")
        finally:
            with self.inflight_lock:
                self.inflight.discard(key)

    def poller(self) -> None:
        pool = ThreadPoolExecutor(max_workers=4)
        last_maintenance = 0.0
        while not self.stop.is_set():
            try:
                config.load_env_file()
                now = time.time()
                if now - last_maintenance > 600:
                    last_maintenance = now
                    self.maintenance()
                targets = self.store.targets()
                self.last_active = self.active_keys()
                for key in self.last_active:
                    t = targets.get(key)
                    if t and t["next_poll"] > now:
                        continue
                    with self.inflight_lock:
                        if key in self.inflight:
                            continue
                        self.inflight.add(key)
                    pool.submit(self._poll_bg, key)
            except Exception:
                log(f"poller loop error:\n{traceback.format_exc()}")
            self.stop.wait(1)
        pool.shutdown(wait=False, cancel_futures=True)

    def maintenance(self) -> None:
        now = time.time()
        with self.cond:
            watching = set(self.watchers)
        for a in self.store.agents():
            if a["id"] not in watching and now - a["last_seen"] > config.AGENT_TTL:
                log(f"forgetting agent {a['id']} (inactive since {time.ctime(a['last_seen'])})")
                self.store.delete_agent(a["id"])
        self.store.prune_events(now - config.EVENT_TTL)
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
                        "filter": json.loads(s["filter"]) if s["filter"] else None})
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
            events = self._annotate(self.store.pending(agent))
            if req.get("wake_only") and all(is_quiet(e) for e in events):
                return {"ok": True, "events": []}
            if events and req.get("ack", True):
                self.store.ack(agent, max(e["id"] for e in events))
        return {"ok": True, "events": events}

    def op_unread(self, req):
        """Per agent, what is waiting for it (pending, not FYI): count, the oldest event's time
        (the source's), the newest id and topics. For apps that remind about stopped agents; it
        does not touch the agents. `agents` limits the answer; `exclude` drops topic prefixes
        (Hyperfocus counts its own hf/ posts itself); `since` (epoch seconds) drops events that
        happened before it; `events: N` also returns up to N of them (oldest first).

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
            events = [e for e in self.store.pending(agent, limit=1000)
                      if not is_quiet(e) and e["kind"] not in HEALTH_KINDS
                      and not (exclude and e["topic"].startswith(exclude)) and e["created_at"] >= floor]
            if events:
                out[agent] = {"count": len(events), "oldest": min(e["created_at"] for e in events),
                              "newest_id": max(e["id"] for e in events),
                              "topics": sorted({e["topic"] for e in events})[:10]}
                n = int(req.get("events") or 0)
                if n > 0:
                    out[agent]["events"] = [{k: e[k] for k in ("id", "topic", "kind", "summary", "url", "actor",
                                                                "created_at")} for e in events[:n]]
        return {"ok": True, "agents": out}

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
        agent = self._agent(req)
        keys = set()
        for s in self.store.subs(agent):
            try:
                source, parts = parse(s["topic"])
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
        return {"ok": True, "pid": os.getpid(), "started": self.started, "version": self.version,
                "agents": agents, "targets": targets, "active_keys": sorted(self.active_keys())}

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
                pending = [] if standby else self.store.pending(agent)
                # FYI events never wake the agent: they go along with the next ones that do.
                if any(not is_quiet(e) for e in pending):
                    # Let a burst (e.g. a review with many comments) land. Published messages are
                    # already complete, so those only wait a moment.
                    push_only = all(is_push(e["topic"]) for e in pending)
                    time.sleep(config.PUSH_SETTLE if push_only else config.WATCH_SETTLE)
                    with self.cond:
                        if self.watchers.get(agent) is not token or agent in self.inflight_batches:
                            continue  # another watch owns delivery now, or is still delivering
                        events = self.store.pending(agent)
                        if all(is_quiet(e) for e in events):
                            continue  # taken by the hook in the meantime
                        self.inflight_batches.add(agent)
                    try:
                        handler.send({"ok": True, "events": self._annotate(events)})
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
                with self.cond:
                    if self.generation == generation:
                        wait = 5.0 if deadline is None else max(0.0, min(5.0, deadline - time.time()))
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
    poller = threading.Thread(target=daemon.poller, name="poller", daemon=True)
    poller.start()
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
