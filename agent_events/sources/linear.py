"""linear/<ISSUE-ID>[/<thread-id>] — a Linear ticket, or one comment thread inside it.

The ticket topic gets everything: field changes (status, assignee, priority, project,
cycle, labels, estimate, due date, parent, title, description) and every comment. Comment
events are published on `linear/<ISSUE>/<thread>` where <thread> is the first 8 hex chars
of the thread's top-level comment id (what Linear puts in `#comment-xxxxxxxx` links), so
subscribing to a thread gets only that thread's comments.
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request

from .. import config
from .base import EventList, PollResult, Source, SourceError, TopicError, first_line, list_changes, short_hash, text_diff

API = "https://api.linear.app/graphql"
OP_REF_ENV = "AGENT_EVENTS_LINEAR_OP_REF"
# No default: set LINEAR_API_KEY, or AGENT_EVENTS_LINEAR_OP_REF=op://... to read it with `op read`,
# in ~/.agent-events/env.
DEFAULT_OP_REF = None

ISSUE_FIELDS = """
    id identifier title description url priorityLabel estimate dueDate
    state { name type }
    assignee { id name }
    project { name }
    cycle { number name }
    parent { identifier }
    labels(first: 50) { nodes { name } }
    comments(first: 250) {
      nodes { id body url createdAt updatedAt user { id name } botActor { name } externalUser { name } parent { id } }
    }
"""  # comments come newest first


def batch_query(n: int) -> str:
    """One request for n issues ($i0..), each under its own alias (i0..). A poll costs Linear ~20 of
    its 3M complexity points an hour, so requests (2,500 an hour per key) are what runs out."""
    params = ", ".join(f"$i{j}: String!" for j in range(n))
    issues = " ".join(f"i{j}: issue(id: $i{j}) {{{ISSUE_FIELDS}}}" for j in range(n))
    return f"query({params}) {{ viewer {{ id name }} {issues} }}"


# The key is shared: other hosts' daemons and every agent's and check's own Linear calls use it too.
# Polling stops while it has fewer than this many requests left, so those keep working.
RESERVE = config._float_env("AGENT_EVENTS_LINEAR_RESERVE", 500)
HOLD = 300.0  # how long a spent budget stops polling before one request looks again
CALLS: list[float] = []  # when this process sent each request in the last hour

ISSUE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*-\d+$")
URL_RE = re.compile(r"^https?://linear\.app/[^/]+/issue/([A-Za-z][A-Za-z0-9]*-\d+)(?:/[^#?]*)?(?:\?[^#]*)?(?:#comment-([0-9a-f]+))?", re.I)

FIELDS = [  # (snapshot key, label)
    ("state", "status"), ("assignee", "assignee"), ("priority", "priority"), ("project", "project"),
    ("cycle", "cycle"), ("estimate", "estimate"), ("due", "due date"), ("parent", "parent"), ("title", "title"),
]
JULIE = "Julie Blanchard"
NOT_PEOPLE = ("Better Stack Workbench", "Claude Terminals")
SELF_QA_HINT = ("Your issue reached Self QA: if your task built it, run the PM check now (linear skill: verify on "
                "production, comment 🤖 PM:). Simple change: move it to PM review. Needs Simon's product taste: "
                "leave it in Self QA and tell your parent.")


def api_key() -> str:
    key = config.secret(["LINEAR_KEY", "LINEAR_API_KEY"], OP_REF_ENV, DEFAULT_OP_REF)
    if not key:
        raise SourceError(f"no Linear API key: set LINEAR_API_KEY in {config.env_path()}, or "
                          f"{OP_REF_ENV}=op://... to read it with the 1Password CLI (`op read`)")
    return key


def _hold(reason: str) -> None:
    Linear.hold_until = time.time() + HOLD
    Linear.hold_reason = reason


def _note_budget(headers) -> None:
    try:
        left = int(headers.get("x-ratelimit-requests-remaining"))
    except (TypeError, ValueError):
        return
    if left < RESERVE:
        _hold(f"the Linear key has {left} requests left this hour, under the {RESERVE:.0f} kept for others")


def request(query: str, variables: dict) -> tuple[dict | None, list]:
    """One GraphQL request: (data, errors). Raises SourceError when there's no usable answer."""
    key = api_key()
    req = urllib.request.Request(API, data=json.dumps({"query": query, "variables": variables}).encode(), headers={
        "Content-Type": "application/json",
        # Personal API keys go in raw; OAuth tokens need the Bearer prefix.
        "Authorization": key if key.startswith("lin_api_") else f"Bearer {key}",
    })
    now = time.time()
    CALLS.append(now)
    del CALLS[:sum(1 for t in CALLS if t < now - 3600)]
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _note_budget(resp.headers)
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        _note_budget(e.headers)
        body = e.read().decode(errors="replace")[:400]
        if e.code in (401, 403):
            config.forget_secret(OP_REF_ENV, DEFAULT_OP_REF)
        if e.code == 429 or "RATELIMITED" in body:
            _hold("Linear answered 429 (rate limited)")
        raise SourceError(f"Linear API HTTP {e.code}: {body}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise SourceError(f"Linear API request failed: {e}") from None
    errors = data.get("errors") or []
    if any((e.get("extensions") or {}).get("code") == "RATELIMITED" for e in errors):
        _hold("Linear answered RATELIMITED")
    if errors and not data.get("data") and not any(e.get("path") for e in errors):
        raise SourceError(error_text(errors))
    return data.get("data"), errors


def error_text(errors: list) -> str:
    return ("Linear: " + "; ".join(e.get("message", str(e)) for e in errors))[:400]


def graphql(query: str, **variables) -> dict:
    data, errors = request(query, variables)
    if errors:
        raise SourceError(error_text(errors))
    return data


def snapshot(data: dict) -> dict:
    issue = data["issue"]
    comments = {}
    for c in issue["comments"]["nodes"]:
        comments[c["id"]] = {
            "parent": (c.get("parent") or {}).get("id"), "author": ((c.get("user") or c.get("externalUser") or c.get("botActor") or {}).get("name")
                       or "integration"),
            "author_id": (c.get("user") or {}).get("id"),
            "body": c["body"], "hash": short_hash(c["body"]), "url": c["url"], "created": c["createdAt"],
            "updated": c.get("updatedAt"),
        }
    cycle = issue.get("cycle")
    return {
        "viewer": (data.get("viewer") or {}).get("id"),
        "id": issue["id"], "identifier": issue["identifier"], "url": issue["url"],
        "title": issue["title"], "description": issue.get("description") or "",
        "state": (issue.get("state") or {}).get("name"),
        "state_type": (issue.get("state") or {}).get("type"),
        "assignee": (issue.get("assignee") or {}).get("name"),
        "priority": issue.get("priorityLabel"),
        "project": (issue.get("project") or {}).get("name"),
        "cycle": (cycle.get("name") or f"Cycle {cycle.get('number')}") if cycle else None,
        "estimate": issue.get("estimate"), "due": issue.get("dueDate"),
        "parent": (issue.get("parent") or {}).get("identifier"),
        "labels": sorted(n["name"] for n in issue["labels"]["nodes"]),
        "comments": comments,
        "truncated": len(comments) >= 250,
    }


def thread_root(comments: dict, cid: str) -> str:
    seen = set()
    while comments.get(cid, {}).get("parent") and cid not in seen:
        seen.add(cid)
        cid = comments[cid]["parent"]
    return cid


def wakes_stopped(c: dict, state_type: str | None) -> bool:
    """Whether this new comment is worth resuming a stopped agent that follows the issue: a person's,
    on an issue still open. Julie's reviews only when they raise a ⚠️ or 🚨; other agents never (they
    sign with 🤖, on Simon's account or their own). Everything else waits for the agent's next turn."""
    if state_type in ("completed", "canceled") or not c["author_id"] or c["author"] in NOT_PEOPLE:
        return False
    if c["author"] == JULIE:
        verdict = c["body"].split("+++ Details")[0]
        return "**⚠" in verdict or "**🚨" in verdict
    return not c["body"].lstrip().startswith("🤖")


def diff(old: dict, new: dict, topic: str) -> EventList:
    ev = EventList(topic, new["url"])
    for key, label in FIELDS:
        if old.get(key) != new.get(key):
            hint = SELF_QA_HINT if key == "state" and (new.get(key) or "").lower() == "self qa" else ""
            ev.add(key, f"{label}: {old.get(key) or 'none'} → {new.get(key) or 'none'}", body=hint)
    added, removed = list_changes(old["labels"], new["labels"])
    for x in added:
        ev.add("labels", f"label added: {x}")
    for x in removed:
        ev.add("labels", f"label removed: {x}")
    if old["description"] != new["description"]:
        ev.add("description", "description edited", body=text_diff(old["description"], new["description"]))

    comments = new["comments"]
    for cid, c in comments.items():
        root = thread_root(comments, cid)
        where = f"{topic}/{root[:8]}"
        mine = {"mine": bool(new.get("viewer") and c["author_id"] == new["viewer"])}
        prev = old["comments"].get(cid)
        what = "reply" if c["parent"] else "comment"
        if prev is None:
            if old["truncated"] and c["created"] < min(o["created"] for o in old["comments"].values()):
                continue  # an old comment that slid into the newest-250 window after a deletion
            wake = {"wakes_stopped_at": time.time()} if wakes_stopped(c, new.get("state_type")) else {}
            ev.add("comment", f"{what} by {c['author']}", body=c["body"], url=c["url"], actor=c["author"],
                   topic=where, dedupe=f"comment:{cid}", at=c["created"], **mine, **wake)
        elif prev["hash"] != c["hash"]:
            ev.add("comment_edited", f"{what} by {c['author']} edited", body=c["body"], url=c["url"],
                   actor=c["author"], topic=where, at=c.get("updated"), **mine)
    if not new["truncated"]:
        for cid, c in old["comments"].items():
            if cid not in comments:
                root = thread_root(old["comments"], cid)
                ev.add("comment_deleted", f"comment by {c['author']} deleted", body=c["body"],
                       topic=f"{topic}/{root[:8]}")
    return ev


def describe(s: dict) -> str:
    return f"{s['identifier']}: {s['title']} [{s['state']}] {s['url']}"


class Linear(Source):
    type = "linear"
    interval = 60.0
    min_spacing = 3.0  # per request: at most 1,200 an hour from this host, of the key's 2,500
    batch = 20
    help = ("A Linear ticket (status, assignee, priority, project, labels, description, comments, ...), "
            "or just one comment thread in it")
    examples = ["linear/<ISSUE-ID>", "linear/<ISSUE-ID>/<thread-id>",
                "https://linear.app/<ws>/issue/<ISSUE-ID>/...#comment-<id>"]

    def normalize(self, parts):
        if not parts or len(parts) > 2 or not ISSUE_RE.match(parts[0]):
            raise TopicError("expected linear/<ISSUE-ID> or linear/<ISSUE-ID>/<thread-id>")
        out = [parts[0].upper()]
        if len(parts) == 2:
            thread = parts[1].lower().replace("comment-", "")
            if not re.match(r"^[0-9a-f-]{4,}$", thread):
                raise TopicError(f"bad comment thread id {parts[1]!r}")
            out.append(thread)
        return out

    def poll_key(self, parts):
        return parts[:1]

    def poll(self, key, state):
        result = self.poll_batch([key], [state])[0]
        if isinstance(result, Exception):
            raise result
        return result

    def poll_batch(self, keys, states):
        ids = [k[0] for k in keys]
        try:
            found, failed = self._fetch(ids)
        except SourceError as e:
            if len(ids) == 1 or "not found" not in str(e).lower():
                raise
            # A missing issue the answer doesn't point at: ask for each on its own, this once.
            return [r for key, state in zip(keys, states) for r in Source.poll_batch(self, [key], [state])]
        if failed and any(i not in found and i not in failed for i in ids):
            # An issue that fails nulls the whole answer (it can't be null), so the others weren't
            # read: ask again without it.
            rest = [i for i in ids if i not in failed]
            if rest:
                more, also = self._fetch(rest)
                found.update(more)
                failed.update(also)
        out = []
        for key, state in zip(keys, states):
            issue = found.get(key[0])
            if issue is None:
                why = failed.get(key[0], "")
                out.append(SourceError(f"Linear issue {key[0]} not found" if not why or "not found" in why.lower()
                                       else f"Linear: {why}"))
                continue
            new = snapshot({"viewer": found.get(None), "issue": issue})
            events = diff(state, new, f"{self.type}/{key[0]}") if state else []
            out.append(PollResult(new, events, describe(new)))
        return out

    @staticmethod
    def _fetch(ids: list) -> tuple[dict, dict]:
        """({id: issue, None: viewer}, {id: error message}) for one request. Raises SourceError when
        the request failed as a whole."""
        data, errors = request(batch_query(len(ids)), {f"i{j}": i for j, i in enumerate(ids)})
        failed = {}
        for e in errors:
            path = e.get("path") or []
            alias = str(path[0]) if path else ""
            if alias[:1] == "i" and alias[1:].isdigit() and int(alias[1:]) < len(ids):
                failed.setdefault(ids[int(alias[1:])], e.get("message", str(e)))
        if errors and not failed:
            raise SourceError(error_text(errors))
        found = {i: (data or {}).get(f"i{j}") for j, i in enumerate(ids)}
        found = {i: v for i, v in found.items() if v}
        found[None] = (data or {}).get("viewer")
        return found, failed

    def requests_last_hour(self):
        now = time.time()
        return sum(1 for t in CALLS if t > now - 3600)

    def resolve(self, parts, state):
        if len(parts) == 1:
            return parts, ""
        matches = [cid for cid in state["comments"] if cid.startswith(parts[1])]
        if not matches:
            raise TopicError(f"no comment {parts[1]} on {parts[0]}")
        root = thread_root(state["comments"], matches[0])
        c = state["comments"][root]
        return [parts[0], root[:8]], f"thread by {c['author']}: {first_line(c['body'])}"

    def from_url(self, url):
        m = URL_RE.match(url.strip())
        if not m:
            return None
        return f"linear/{m.group(1)}" + (f"/{m.group(2)}" if m.group(2) else "")
