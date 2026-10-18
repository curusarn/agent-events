"""Registry of event types. Adding a type = add a Source subclass and list it here."""
from __future__ import annotations

from .base import Event, PollResult, Source, SourceError, TopicError
from .docs import Docs
from .github import GitHub
from .hf import HF
from .linear import Linear
from .slack import Slack

SOURCES: dict[str, Source] = {}
ALIASES: dict[str, str] = {}  # old type name -> current one (e.g. mc -> hf)


def register(source: Source) -> None:
    SOURCES[source.type] = source
    for alias in getattr(source, "aliases", ()):
        ALIASES[alias] = source.type


for _s in (GitHub(), Linear(), HF(), Slack(), Docs()):
    register(_s)


def parse(raw: str) -> tuple[Source, list[str]]:
    """Topic string or URL → (source, normalized segments after the type)."""
    raw = raw.strip()
    if raw.startswith(("http://", "https://")):
        for source in SOURCES.values():
            topic = source.from_url(raw)
            if topic:
                raw = topic
                break
        else:
            raise TopicError(f"don't know how to watch {raw}; supported: {', '.join(SOURCES)}")
    parts = [p for p in raw.strip("/").split("/") if p]
    if not parts:
        raise TopicError("empty topic")
    name = parts[0].lower()
    source = SOURCES.get(ALIASES.get(name, name))
    if not source:
        raise TopicError(f"unknown event type {parts[0]!r}; known: {', '.join(SOURCES)}")
    return source, source.normalize(parts[1:])


def poll_key(source: Source, parts: list[str]) -> str:
    return "/".join([source.type] + source.poll_key(parts))


def is_push(topic: str) -> bool:
    """True for topics of a push source (published into, never polled)."""
    try:
        return parse(topic)[0].push
    except TopicError:
        return False


def matches(subscription: str, topic: str) -> bool:
    return topic == subscription or topic.startswith(subscription + "/")


__all__ = ["Event", "PollResult", "Source", "SourceError", "TopicError", "SOURCES", "register", "parse",
           "poll_key", "matches", "is_push"]
