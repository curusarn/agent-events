"""slack/<channel-id>[/<thread-ts>] — a Slack channel, or (mostly) one thread in it.

Thread topics are polled with conversations.replies and report new replies, edits,
deletions and reaction changes. Channel topics are polled with conversations.history and
report new top-level messages plus new replies in recently active threads (published on
the thread's topic, so a thread subscriber also gets them).
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from .. import config
from .base import EventList, PollResult, Source, SourceError, TopicError, first_line, short_hash

API = "https://slack.com/api/"
OP_REF_ENV = "AGENT_EVENTS_SLACK_OP_REF"
# No default: set SLACK_USER_TOKEN, or AGENT_EVENTS_SLACK_OP_REF=op://... to read it with `op read`,
# in ~/.agent-events/env.
DEFAULT_OP_REF = None

CHANNEL_RE = re.compile(r"^[CDG][A-Z0-9]{6,}$", re.I)
URL_RE = re.compile(r"^https?://[^/]*slack\.com/archives/([A-Z0-9]+)(?:/p(\d{10})(\d{6}))?(?:\?(\S*))?", re.I)

_users: dict[str, str] = {}
_channels: dict[str, str] = {}
_team: dict[str, str] = {}
# A failed name lookup (no token yet, a network blip): the id stands in, and the
# lookup is tried again after this long, not never. Caching the id for good left
# a chip reading "C0123ABCD thread" instead of "#engineering thread" for the
# daemon's whole life (Simon, 2026-10-06).
RETRY_FAILED = 600.0
_failed: dict[str, float] = {}


def _lookup(cache: dict[str, str], key: str, fetch) -> str:
    """cache[key], else fetch() (cached); on SourceError the key itself, retried later."""
    if key in cache:
        return cache[key]
    if time.time() < _failed.get(key, 0):
        return key
    try:
        cache[key] = fetch()
        _failed.pop(key, None)
        return cache[key]
    except SourceError:
        _failed[key] = time.time() + RETRY_FAILED
        return key


def token() -> str:
    tok = config.secret(["SLACK_USER_TOKEN", "SLACK_TOKEN"], OP_REF_ENV, DEFAULT_OP_REF)
    if not tok:
        raise SourceError(f"no Slack token: set SLACK_USER_TOKEN in {config.env_path()}, or "
                          f"{OP_REF_ENV}=op://... to read it with the 1Password CLI (`op read`)")
    return tok


def call(method: str, **params) -> dict:
    url = API + method + "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token()}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        raise SourceError(f"Slack {method} HTTP {e.code}" + (" (rate limited)" if e.code == 429 else "")) from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise SourceError(f"Slack {method} failed: {e}") from None
    if not data.get("ok"):
        if data.get("error") in ("invalid_auth", "token_revoked", "not_authed"):
            config.forget_secret(OP_REF_ENV, DEFAULT_OP_REF)
        raise SourceError(f"Slack {method}: {data.get('error')}")
    return data


def team() -> dict:
    if not _team:
        data = call("auth.test")
        _team.update(url=data.get("url") or "https://slack.com/", user_id=data.get("user_id") or "")
    return _team


def user_name(uid: str | None) -> str:
    if not uid:
        return "unknown"
    def fetch():
        u = call("users.info", user=uid)["user"]
        return u.get("profile", {}).get("display_name") or u.get("real_name") or u.get("name") or uid
    return _lookup(_users, uid, fetch)


def channel_name(cid: str) -> str:
    def fetch():
        ch = call("conversations.info", channel=cid)["channel"]
        return "#" + ch["name"] if ch.get("name") else ("DM" if ch.get("is_im") else cid)
    return _lookup(_channels, cid, fetch)


def author(m: dict) -> str:
    if m.get("user"):
        return user_name(m["user"])
    return (m.get("bot_profile") or {}).get("name") or m.get("username") or "bot"


def render(text: str | None) -> str:
    """Slack mrkdwn → plain text: resolve user mentions, unwrap links and channel refs."""
    text = text or ""
    text = re.sub(r"<@([UW][A-Z0-9]+)(?:\|[^>]*)?>", lambda m: "@" + user_name(m.group(1)), text)
    text = re.sub(r"<#[A-Z0-9]+\|([^>]*)>", r"#\1", text)
    text = re.sub(r"<!(here|channel|everyone)>", r"@\1", text)
    text = re.sub(r"<(https?://[^|>]+)\|([^>]+)>", r"\2 (\1)", text)
    text = re.sub(r"<(https?://[^>]+)>", r"\1", text)
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def permalink(channel: str, ts: str, thread: str | None = None) -> str:
    base = team()["url"].rstrip("/")
    link = f"{base}/archives/{channel}/p{ts.replace('.', '')}"
    return link + (f"?thread_ts={thread}&cid={channel}" if thread and thread != ts else "")


def ts_from_p(p: str) -> str:
    return f"{p[:10]}.{p[10:]}"


def replies(channel: str, thread: str, oldest: str | None = None) -> list:
    out, cursor = [], None
    while True:
        data = call("conversations.replies", channel=channel, ts=thread, oldest=oldest, limit=200, cursor=cursor)
        out += data.get("messages", [])
        cursor = (data.get("response_metadata") or {}).get("next_cursor")
        if not cursor or len(out) > 2000:
            return out


def poll_thread(channel: str, thread: str, state: dict | None) -> PollResult:
    msgs = replies(channel, thread)
    if not msgs:
        raise SourceError(f"Slack thread {channel}/{thread} not found")
    me = team()["user_id"]
    snap = {}
    for m in msgs:
        snap[m["ts"]] = {
            "user": m.get("user"), "hash": short_hash(m.get("text")), "edited": (m.get("edited") or {}).get("ts"),
            # Who reacted, per emoji: Slack lists the users with each reaction.
            "reactions": {r["name"]: sorted(r.get("users") or []) for r in m.get("reactions") or []},
        }
    root = msgs[0]
    new = {"msgs": snap, "root_text": first_line(render(root.get("text")))}
    summary = f"{channel_name(channel)} thread by {author(root)}: {new['root_text']} {permalink(channel, thread)}"
    if state is None:
        return PollResult(new, [], summary)

    topic = f"slack/{channel}/{thread}"
    ev = EventList(topic, permalink(channel, thread))
    old = state["msgs"]
    for m in msgs:
        ts, cur, prev = m["ts"], snap[m["ts"]], old.get(m["ts"])
        who = author(m)
        url = permalink(channel, ts, thread)
        mine = {"mine": bool(me and m.get("user") == me)}
        if prev is None:
            ev.add("message", f"reply by {who}", body=render(m.get("text")), url=url, actor=who,
                   dedupe=f"msg:{ts}", at=ts, **mine)
            continue
        if prev["hash"] != cur["hash"]:
            ev.add("message_edited", f"message by {who} edited", body=render(m.get("text")), url=url, actor=who,
                   at=cur.get("edited"), **mine)
        for name, users in cur["reactions"].items():
            before = prev["reactions"].get(name, [])
            if isinstance(before, int):  # a snapshot from before reactors were kept: counts only
                if len(users) > before:
                    ev.add("reaction", f"someone added :{name}: to {who}'s message",
                           body=first_line(render(m.get("text"))), url=url)
                continue
            added = [u for u in users if u not in before]
            if added:
                by = ", ".join(user_name(u) for u in added)
                ev.add("reaction", f"{by} added :{name}: to {who}'s message", body=first_line(render(m.get("text"))),
                       url=url, actor=by)
        for name, before in prev["reactions"].items():
            users = cur["reactions"].get(name, [])
            if isinstance(before, int):
                if not users:
                    ev.add("reaction", f":{name}: removed from {who}'s message", url=url)
                continue
            removed = [u for u in before if u not in users]
            if removed:
                by = ", ".join(user_name(u) for u in removed)
                ev.add("reaction", f"{by} removed :{name}: from {who}'s message", url=url, actor=by)
    for ts in old:
        if ts not in snap:
            ev.add("message_deleted", "a message in the thread was deleted")
    return PollResult(new, ev, summary)


def poll_channel(channel: str, state: dict | None) -> PollResult:
    msgs = call("conversations.history", channel=channel, limit=100).get("messages", [])
    me = team()["user_id"]
    top = [m for m in msgs if not m.get("thread_ts") or m.get("thread_ts") == m["ts"]]
    latest = max([m["ts"] for m in top] + [(state or {}).get("latest", "0")], key=float)
    threads = {m["ts"]: m["latest_reply"] for m in top if m.get("latest_reply")}
    summary = f"{channel_name(channel)} {permalink(channel, latest)}" if top else channel_name(channel)
    if state is None:
        return PollResult({"latest": latest, "threads": threads}, [], summary)

    topic = f"slack/{channel}"
    ev = EventList(topic)
    for m in sorted(top, key=lambda m: float(m["ts"])):
        if float(m["ts"]) > float(state["latest"]):
            who = author(m)
            ev.add("message", f"message by {who} in {channel_name(channel)}", body=render(m.get("text")),
                   url=permalink(channel, m["ts"]), actor=who, dedupe=f"msg:{m['ts']}", at=m["ts"],
                   mine=bool(me and m.get("user") == me))
    for ts, last in threads.items():
        seen = state["threads"].get(ts) or ts  # a brand-new message: all its replies are new
        if float(last) <= float(seen):
            continue
        for r in replies(channel, ts, oldest=seen):
            if r["ts"] == ts or float(r["ts"]) <= float(seen):
                continue
            who = author(r)
            ev.add("message", f"reply by {who} in {channel_name(channel)} thread", body=render(r.get("text")),
                   url=permalink(channel, r["ts"], ts), actor=who, topic=f"{topic}/{ts}", dedupe=f"msg:{r['ts']}",
                   at=r["ts"], mine=bool(me and r.get("user") == me))
    # Keep tracking threads that scrolled out of the 100-message window for a while.
    merged = dict(state["threads"])
    merged.update(threads)
    if len(merged) > 500:
        merged = dict(sorted(merged.items(), key=lambda kv: float(kv[1]))[-500:])
    return PollResult({"latest": latest, "threads": merged}, ev, summary)


class Slack(Source):
    type = "slack"
    interval = 30.0
    min_spacing = 3.0  # conversations.* are Tier 3 (~50/min), shared with the skills
    help = "A Slack thread (replies, edits, reactions) or a whole channel (new messages + thread replies)"
    examples = ["slack/<channel-id>/<thread-ts>", "slack/<channel-id>",
                "https://<ws>.slack.com/archives/<channel-id>/p<ts>"]

    def normalize(self, parts):
        if not parts or len(parts) > 2 or not CHANNEL_RE.match(parts[0]):
            raise TopicError("expected slack/<channel-id> or slack/<channel-id>/<thread-ts>")
        out = [parts[0].upper()]
        if len(parts) == 2:
            ts = parts[1]
            if re.match(r"^p\d{16}$", ts):
                ts = ts_from_p(ts[1:])
            if not re.match(r"^\d{10}\.\d{6}$", ts):
                raise TopicError(f"bad thread ts {parts[1]!r}; expected like 1712345678.123456")
            out.append(ts)
        return out

    def poll(self, key, state):
        return poll_thread(key[0], key[1], state) if len(key) == 2 else poll_channel(key[0], state)

    def from_url(self, url):
        m = URL_RE.match(url.strip())
        if not m:
            return None
        channel, sec, frac, query = m.groups()
        if not sec:
            return f"slack/{channel.upper()}"
        thread = urllib.parse.parse_qs(query or "").get("thread_ts", [f"{sec}.{frac}"])[0]
        return f"slack/{channel.upper()}/{thread}"
