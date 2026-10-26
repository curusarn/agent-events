"""SQLite persistence. Only the daemon writes; the hook may read when the daemon is down."""
from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY,
  pid INTEGER,               -- Claude Code process; while alive the agent counts as active
  cwd TEXT,
  created_at REAL NOT NULL,
  last_seen REAL NOT NULL,
  cursor INTEGER NOT NULL,   -- last event id delivered to (and acked by) this agent
  retired REAL               -- when a newer session of its Claude process replaced it (/clear)
);
CREATE TABLE IF NOT EXISTS subscriptions (
  agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
  topic TEXT NOT NULL,
  since INTEGER NOT NULL,    -- events up to this id predate the subscription
  created_at REAL NOT NULL,
  PRIMARY KEY (agent_id, topic)
);
CREATE TABLE IF NOT EXISTS targets (  -- one row per poll key
  key TEXT PRIMARY KEY,
  state TEXT,
  summary TEXT,
  last_poll REAL,
  next_poll REAL NOT NULL DEFAULT 0,
  error TEXT,
  fails INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  kind TEXT NOT NULL,
  dedupe TEXT NOT NULL,
  summary TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  actor TEXT NOT NULL DEFAULT '',
  data TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  UNIQUE (topic, dedupe)
);
CREATE INDEX IF NOT EXISTS events_created ON events(created_at);
"""

# An event matches a subscription to its exact topic or any topic nested under it.
MATCH = "(e.topic = s.topic OR substr(e.topic, 1, length(s.topic) + 1) = s.topic || '/')"


def is_quiet(event: dict) -> bool:
    """An FYI event (published with "quiet"): it never wakes the agent. It is handed over with
    the next events that do, or at the start of the agent's next turn."""
    return (event.get("data") or {}).get("quiet") in (True, 1, "1", "true")


GITHUB_PR_RE = re.compile(r"^github/[^/]+/([^/]+)/(\d+)$")


def mine_terms(topics: list[str]) -> list[str]:
    """What a message about one of these PRs says: app#123, pull/123, #123."""
    out = []
    for t in topics:
        m = GITHUB_PR_RE.match(t)
        if m:
            repo, n = m.groups()
            out += [f"{repo}#{n}", f"pull/{n}", f"#{n}"]
    return out


def passes(terms: list[str], texts: list[str]) -> bool:
    """Any term in any text, as a whole word (case ignored): '#124' doesn't match '#1240'."""
    for term in terms:
        term = term.strip()
        if not term:
            continue
        rx = re.compile(r"(?<![\w])" + re.escape(term) + r"(?![\w])", re.I)
        if any(rx.search(t) for t in texts):
            return True
    return False


def event_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["data"] = json.loads(d.get("data") or "{}")
    d.pop("dedupe", None)
    return d


class Store:
    def __init__(self, path: Path, readonly: bool = False):
        if readonly:
            self.db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False, timeout=5)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.readonly = readonly
        self.lock = threading.RLock()
        if not readonly:
            with self.lock:
                self.db.executescript(SCHEMA)
                self.db.execute("PRAGMA foreign_keys=ON")
                if "retired" not in {r[1] for r in self.db.execute("PRAGMA table_info(agents)")}:
                    self.db.execute("ALTER TABLE agents ADD COLUMN retired REAL")
                if "filter" not in {r[1] for r in self.db.execute("PRAGMA table_info(subscriptions)")}:
                    self.db.execute("ALTER TABLE subscriptions ADD COLUMN filter TEXT")
                self._rename_topics("mc/", "hf/")

    def _rename_topics(self, old: str, new: str) -> None:
        """Move stored topics to a type's new name (Mission Control's mc/ became Hyperfocus' hf/),
        so old subscriptions and pending events match topics that are now parsed to the new name."""
        n = len(old)
        prefix = f"substr(topic, 1, {n}) = ?"
        self.db.execute(f"UPDATE OR IGNORE subscriptions SET topic = ? || substr(topic, {n + 1}) WHERE {prefix}",
                        (new, old))
        self.db.execute(f"DELETE FROM subscriptions WHERE {prefix}", (old,))  # duplicates of existing new ones
        self.db.execute(f"UPDATE OR IGNORE events SET topic = ? || substr(topic, {n + 1}) WHERE {prefix}",
                        (new, old))

    def q(self, sql: str, args=()) -> list:
        with self.lock:
            return self.db.execute(sql, args).fetchall()

    def x(self, sql: str, args=()) -> sqlite3.Cursor:
        with self.lock:
            return self.db.execute(sql, args)

    def max_event_id(self) -> int:
        return self.q("SELECT COALESCE(MAX(id), 0) FROM events")[0][0]

    # --- agents & subscriptions -----------------------------------------------------

    def touch_agent(self, agent: str, pid: int | None = None, cwd: str | None = None) -> None:
        now = time.time()
        with self.lock:
            cur = self.db.execute(
                "UPDATE agents SET last_seen = ?, pid = COALESCE(?, pid), cwd = COALESCE(?, cwd), "
                "retired = CASE WHEN ? IS NULL THEN retired END WHERE id = ?",  # running again (resumed)
                (now, pid, cwd, pid, agent))
            if cur.rowcount == 0:
                self.db.execute("INSERT INTO agents (id, pid, cwd, created_at, last_seen, cursor) VALUES (?,?,?,?,?,?)",
                                (agent, pid, cwd, now, now, self.max_event_id()))

    def agents(self) -> list:
        return self.q("SELECT * FROM agents")

    def delete_agent(self, agent: str) -> None:
        with self.lock:
            self.db.execute("DELETE FROM subscriptions WHERE agent_id = ?", (agent,))
            self.db.execute("DELETE FROM agents WHERE id = ?", (agent,))

    def add_sub(self, agent: str, topic: str, filter: dict | None = None) -> bool:
        """Subscribe; with `filter` ({"about": [terms], "mine": bool}) only matching events are
        delivered (see passes). Subscribing again replaces the filter (None: everything)."""
        flt = json.dumps(filter) if filter else None
        with self.lock:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO subscriptions (agent_id, topic, since, created_at, filter) VALUES (?,?,?,?,?)",
                (agent, topic, self.max_event_id(), time.time(), flt))
            if cur.rowcount == 0:
                self.db.execute("UPDATE subscriptions SET filter = ? WHERE agent_id = ? AND topic = ?", (flt, agent, topic))
            return cur.rowcount == 1

    def remove_subs(self, agent: str, topics: list[str] | None) -> list[str]:
        """Remove the given topics (and anything nested under them), or all when None."""
        with self.lock:
            existing = [r["topic"] for r in self.subs(agent)]
            if topics is None:
                gone = existing
            else:
                gone = [t for t in existing if any(t == x or t.startswith(x + "/") for x in topics)]
            for t in gone:
                self.db.execute("DELETE FROM subscriptions WHERE agent_id = ? AND topic = ?", (agent, t))
            return gone

    def subs(self, agent: str) -> list:
        return self.q("SELECT * FROM subscriptions WHERE agent_id = ? ORDER BY created_at", (agent,))

    def all_subs(self) -> list:
        return self.q("SELECT s.*, a.pid, a.last_seen FROM subscriptions s JOIN agents a ON a.id = s.agent_id")

    # --- targets --------------------------------------------------------------------

    def target(self, key: str) -> sqlite3.Row | None:
        rows = self.q("SELECT * FROM targets WHERE key = ?", (key,))
        return rows[0] if rows else None

    def targets(self) -> dict:
        return {r["key"]: r for r in self.q("SELECT key, summary, last_poll, next_poll, error, fails FROM targets")}

    def save_poll(self, key: str, state: dict, summary: str, next_poll: float, events: list) -> int:
        """Store a successful poll and its events atomically; returns how many events were new."""
        now = time.time()
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute(
                    "INSERT INTO targets (key, state, summary, last_poll, next_poll, error, fails) "
                    "VALUES (?,?,?,?,?,NULL,0) ON CONFLICT(key) DO UPDATE SET state = excluded.state, "
                    "summary = excluded.summary, last_poll = excluded.last_poll, next_poll = excluded.next_poll, "
                    "error = NULL, fails = 0",
                    (key, json.dumps(state), summary, now, next_poll))
                inserted = self._insert_events(events, now)
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            return inserted

    def save_error(self, key: str, error: str, fails: int, next_poll: float, events: list) -> None:
        now = time.time()
        with self.lock:
            self.db.execute(
                "INSERT INTO targets (key, next_poll, error, fails) VALUES (?,?,?,?) ON CONFLICT(key) DO UPDATE "
                "SET next_poll = excluded.next_poll, error = excluded.error, fails = excluded.fails",
                (key, next_poll, error, fails))
            self._insert_events(events, now)

    def schedule(self, keys: list[str], when: float) -> None:
        with self.lock:
            for k in keys:
                self.db.execute("UPDATE targets SET next_poll = ? WHERE key = ?", (when, k))

    def delete_target(self, key: str) -> None:
        self.x("DELETE FROM targets WHERE key = ?", (key,))

    def insert_event(self, e) -> int | None:
        """Insert one published event; returns its id, or None if (topic, dedupe) already exists."""
        with self.lock:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO events (topic, kind, dedupe, summary, body, url, actor, data, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (e.topic, e.kind, e.dedupe, e.summary, e.body, e.url, e.actor, json.dumps(e.data), time.time()))
            return cur.lastrowid if cur.rowcount == 1 else None

    def event_id(self, topic: str, dedupe: str) -> int | None:
        rows = self.q("SELECT id FROM events WHERE topic = ? AND dedupe = ?", (topic, dedupe))
        return rows[0][0] if rows else None

    def _insert_events(self, events: list, now: float) -> int:
        n = 0
        for e in events:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO events (topic, kind, dedupe, summary, body, url, actor, data, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (e.topic, e.kind, e.dedupe, e.summary, e.body, e.url, e.actor, json.dumps(e.data),
                 min(getattr(e, "at", None) or now, now)))  # the source's time when it gives one
            n += cur.rowcount
        return n

    # --- delivery -------------------------------------------------------------------

    def pending(self, agent: str, limit: int = 100) -> list[dict]:
        def page(after: int | None) -> list:
            return self.q(
                f"SELECT e.* FROM events e WHERE e.id > COALESCE(:after, (SELECT cursor FROM agents WHERE id = :a)) "
                f"AND EXISTS (SELECT 1 FROM subscriptions s WHERE s.agent_id = :a AND e.id > s.since AND {MATCH}) "
                f"ORDER BY e.id LIMIT :n", {"a": agent, "after": after, "n": limit})
        rows = page(None)
        subs = self.subs(agent)
        if not any(s["filter"] for s in subs):
            return [event_dict(r) for r in rows]
        # Filtered subscriptions: only what the agent asked for. Events nobody here wants
        # before the first wanted one are skipped for good (the cursor moves past them), so
        # a busy channel never piles up behind the filter. Read on past a page of noise: an
        # FYI left at the front holds the cursor, and a post that wakes can sit behind
        # hundreds of a busy channel's unwanted messages (u9626-weekly-report missed its
        # parent's posts for a day behind #support, 2026-10-25).
        mine = mine_terms([s["topic"] for s in subs])
        out, skip = [], None
        while rows:
            for r in rows:
                e = event_dict(r)
                if self._wanted(e, r["id"], subs, mine):
                    out.append(e)
                elif not out:
                    skip = r["id"]
            if len(out) >= limit or len(rows) < limit:
                break
            rows = page(rows[-1]["id"])
        if skip is not None and not self.readonly:
            self.ack(agent, skip)
        return out[:limit]

    def _wanted(self, e: dict, event_id: int, subs: list, mine: list[str]) -> bool:
        for s in subs:
            if event_id <= s["since"] or not (e["topic"] == s["topic"] or e["topic"].startswith(s["topic"] + "/")):
                continue
            flt = json.loads(s["filter"]) if s["filter"] else None
            if not flt:
                return True
            terms = list(flt.get("about") or []) + (mine if flt.get("mine") else [])
            texts = [e.get("summary") or "", e.get("body") or ""]
            if e["topic"] != s["topic"]:
                # A reply in a thread: wanted when the thread's first message was.
                ts = e["topic"].rsplit("/", 1)[-1]
                for p in self.q("SELECT summary, body FROM events WHERE topic = ? AND dedupe = ?", (s["topic"], f"msg:{ts}")):
                    texts += [p["summary"] or "", p["body"] or ""]
            if passes(terms, texts):
                return True
        return False

    def cursor_time(self, agent: str) -> float:
        """When the last event handed to the agent happened (0 if none is known)."""
        rows = self.q("SELECT e.created_at FROM events e JOIN agents a ON e.id = a.cursor WHERE a.id = ?", (agent,))
        return rows[0][0] if rows else 0.0

    def ack(self, agent: str, event_id: int) -> None:
        self.x("UPDATE agents SET cursor = MAX(cursor, ?) WHERE id = ?", (event_id, agent))

    def recent(self, agent: str, limit: int = 20, topic: str | None = None) -> list[dict]:
        """Recent events for the agent's subscriptions (delivered or not), newest last."""
        if topic:
            rows = self.q("SELECT * FROM events e WHERE e.topic = ? OR substr(e.topic, 1, length(?) + 1) = ? || '/' "
                          "ORDER BY id DESC LIMIT ?", (topic, topic, topic, limit))
        else:
            rows = self.q(f"SELECT e.* FROM events e WHERE EXISTS (SELECT 1 FROM subscriptions s "
                          f"WHERE s.agent_id = ? AND {MATCH}) ORDER BY e.id DESC LIMIT ?", (agent, limit))
        return [event_dict(r) for r in reversed(rows)]

    def prune_events(self, older_than: float) -> None:
        self.x("DELETE FROM events WHERE created_at < ?", (older_than,))
