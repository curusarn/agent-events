"""hf/agent/<id>, hf/thread/<id> — Hyperfocus messaging (a push source).

Nothing is polled: the Hyperfocus app (or anyone, via `agent-events publish`) publishes
events into these topics over the daemon socket, and subscribed agents' watches wake at once.
Each agent hosted in Hyperfocus gets an inbox `hf/agent/<hyperfocus-agent-id>`; the
app subscribes the agent to it when it spawns the agent.

`mc/...` (from when the app was called Mission Control) is still accepted and means the same
topic: `parse` maps it to `hf/...`, and the store renames stored `mc/` topics on startup.
"""
from __future__ import annotations

import re

from .base import PollResult, Source, SourceError, TopicError

ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
KINDS = ("agent", "thread")


class HF(Source):
    type = "hf"
    aliases = ("mc",)
    push = True
    help = ("Hyperfocus messages between agents, published by the Hyperfocus app or "
            "`agent-events publish`; events carry the sender (mc/... is accepted as an old alias)")
    examples = ["hf/agent/<hyperfocus-agent-id>", "hf/thread/<thread-id>"]

    def normalize(self, parts):
        if len(parts) != 2 or parts[0].lower() not in KINDS or not ID_RE.match(parts[1]):
            got = "/".join(["hf"] + list(parts))
            raise TopicError(f"expected hf/agent/<id> or hf/thread/<id> (id: letters, digits, '.', '_', '-'), "
                             f"got {got!r}")
        return [parts[0].lower(), parts[1]]

    def poll(self, key, state) -> PollResult:
        raise SourceError("hf topics are push-only; nothing to poll")
