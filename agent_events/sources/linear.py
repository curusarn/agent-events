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
import urllib.error
import urllib.request

from .. import config
from .base import EventList, PollResult, Source, SourceError, TopicError, first_line, list_changes, short_hash, text_diff

API = "https://api.linear.app/graphql"
OP_REF_ENV = "AGENT_EVENTS_LINEAR_OP_REF"
# No default: set LINEAR_API_KEY, or AGENT_EVENTS_LINEAR_OP_REF=op://... to read it with `op read`,
# in ~/.agent-events/env.
DEFAULT_OP_REF = None

QUERY = """
query($id: String!) {
  viewer { id name }
  issue(id: $id) {
    id identifier title description url priorityLabel estimate dueDate
    state { name }
    assignee { id name }
    project { name }
    cycle { number name }
    parent { identifier }
    labels(first: 50) { nodes { name } }
    comments(first: 250) {  # newest first
      nodes { id body url createdAt updatedAt user { id name } botActor { name } externalUser { name } parent { id } }
    }
  }
}
"""

ISSUE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*-\d+$")
URL_RE = re.compile(r"^https?://linear\.app/[^/]+/issue/([A-Za-z][A-Za-z0-9]*-\d+)(?:/[^#?]*)?(?:\?[^#]*)?(?:#comment-([0-9a-f]+))?", re.I)

FIELDS = [  # (snapshot key, label)
    ("state", "status"), ("assignee", "assignee"), ("priority", "priority"), ("project", "project"),
    ("cycle", "cycle"), ("estimate", "estimate"), ("due", "due date"), ("parent", "parent"), ("title", "title"),
]
SELF_QA_HINT = ("Your issue reached Self QA: run the PM check now if your task built it (linear skill: verify on "
                "production, comment 🤖 PM:, then move it to PM review only if it needs Simon's product taste "
                "check; a simple task goes to Done).")


def api_key() -> str:
    key = config.secret(["LINEAR_KEY", "LINEAR_API_KEY"], OP_REF_ENV, DEFAULT_OP_REF)
    if not key:
        raise SourceError(f"no Linear API key: set LINEAR_API_KEY in {config.env_path()}, or "
                          f"{OP_REF_ENV}=op://... to read it with the 1Password CLI (`op read`)")
    return key


def graphql(query: str, **variables) -> dict:
    key = api_key()
    req = urllib.request.Request(API, data=json.dumps({"query": query, "variables": variables}).encode(), headers={
        "Content-Type": "application/json",
        # Personal API keys go in raw; OAuth tokens need the Bearer prefix.
        "Authorization": key if key.startswith("lin_api_") else f"Bearer {key}",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:400]
        if e.code in (401, 403):
            config.forget_secret(OP_REF_ENV, DEFAULT_OP_REF)
        raise SourceError(f"Linear API HTTP {e.code}: {body}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise SourceError(f"Linear API request failed: {e}") from None
    if data.get("errors"):
        raise SourceError("Linear: " + "; ".join(e.get("message", str(e)) for e in data["errors"])[:400])
    return data["data"]


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
            ev.add("comment", f"{what} by {c['author']}", body=c["body"], url=c["url"], actor=c["author"],
                   topic=where, dedupe=f"comment:{cid}", at=c["created"], **mine)
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
    min_spacing = 3.0  # 2500 requests/hour per API key, shared with the skills
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
        data = graphql(QUERY, id=key[0])
        if not data.get("issue"):
            raise SourceError(f"Linear issue {key[0]} not found")
        new = snapshot(data)
        events = diff(state, new, f"{self.type}/{key[0]}") if state else []
        return PollResult(new, events, describe(new))

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
