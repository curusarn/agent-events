"""The contract every event type implements, plus small helpers shared by the sources.

A topic is `<type>/<segment>/<segment>...`; the type decides what the segments mean.
Several topics can share one *poll key* (e.g. a Linear ticket and a comment thread inside
it are both served by polling the ticket). Each poll returns fresh state plus the events
derived from diffing it against the previous state. Events are published on the most
specific topic that applies, and a subscription to `a/b` also receives events on `a/b/...`.
"""
from __future__ import annotations

import datetime
import difflib
import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Any


class TopicError(ValueError):
    """The topic is malformed or refers to something that doesn't exist."""


class SourceError(RuntimeError):
    """Polling failed (network, auth, API error)."""


@dataclass
class Event:
    topic: str
    kind: str
    summary: str
    body: str = ""
    url: str = ""
    actor: str = ""
    # Unique per topic; lets overlapping polls (e.g. a Slack channel and a thread in it)
    # report the same message without delivering it twice.
    dedupe: str = ""
    data: dict = field(default_factory=dict)
    # When it happened at the source (epoch seconds), if the source says; else when it was seen.
    at: float | None = None


@dataclass
class PollResult:
    state: dict
    events: list  # of Event; ignored on the first (baseline) poll
    summary: str  # one line describing the watched thing, shown in `list`


class Source:
    type = ""
    interval = 60.0
    # Seconds of API budget one poll uses: with N active keys each is polled at most every
    # N * min_spacing seconds. Keeps many subscriptions from exhausting rate limits.
    min_spacing = 1.0
    # Push sources are never polled: events are published into them (the `publish` op), and
    # subscribing needs no baseline.
    push = False
    # Shown by `agent-events types`.
    help = ""
    examples: list = []

    def normalize(self, parts: list[str]) -> list[str]:
        """Validate/canonicalize the segments after the type. Raise TopicError if malformed."""
        raise NotImplementedError

    def poll_key(self, parts: list[str]) -> list[str]:
        """Segments identifying what must be polled to serve this topic."""
        return parts

    def poll(self, key: list[str], state: dict | None) -> PollResult:
        raise NotImplementedError

    def resolve(self, parts: list[str], state: dict) -> tuple[list[str], str]:
        """After a successful poll: final canonical segments + extra description.

        Lets a source check that e.g. a comment thread exists, which it can only know
        after fetching the ticket.
        """
        return parts, ""

    def from_url(self, url: str) -> str | None:
        """Turn a web URL into a topic, or None if the URL isn't for this source."""
        return None


# --- helpers ----------------------------------------------------------------------------

class EventList(list):
    """Collects events for one poll, stamping unique dedupe keys where none are natural."""

    def __init__(self, default_topic: str, default_url: str = ""):
        super().__init__()
        self.topic = default_topic
        self.url = default_url
        self.stamp = f"{time.time():.3f}"

    def add(self, kind: str, summary: str, body: str = "", url: str = "", actor: str = "",
            topic: str | None = None, dedupe: str | None = None, at: Any = None, **data: Any) -> None:
        self.append(Event(
            topic=topic or self.topic, kind=kind, summary=summary, body=body or "",
            url=url or self.url, actor=actor or "",
            dedupe=dedupe or f"{kind}@{self.stamp}#{len(self)}", data=data, at=timestamp(at),
        ))


def timestamp(value: Any) -> float | None:
    """A source's time as epoch seconds: an ISO 8601 string (GitHub, Linear), a Slack ts or a
    number. None if missing or unparsable."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        text = str(value).strip().replace("Z", "+00:00")
        m = re.match(r"^(.*T\d\d:\d\d:\d\d)(\.\d+)?(.*)$", text)
        if m:  # Python 3.9's fromisoformat wants exactly 3 or 6 fraction digits
            frac = (m.group(2) or "")[1:7].ljust(6, "0") if m.group(2) else ""
            text = m.group(1) + (f".{frac}" if frac else "") + m.group(3)
        dt = datetime.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


def short_hash(text: str | None) -> str:
    return hashlib.sha1((text or "").encode()).hexdigest()[:12]


def text_diff(old: str | None, new: str | None, max_lines: int = 40) -> str:
    lines = list(difflib.unified_diff(
        (old or "").splitlines(), (new or "").splitlines(), lineterm="", n=1))[2:]
    if len(lines) > max_lines:
        lines = lines[:max_lines] + [f"... ({len(lines) - max_lines} more diff lines)"]
    return "\n".join(lines)


def list_changes(old: list, new: list) -> tuple[list, list]:
    return [x for x in new if x not in old], [x for x in old if x not in new]


def first_line(text: str | None, limit: int = 80) -> str:
    line = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    return line if len(line) <= limit else line[: limit - 1] + "…"
