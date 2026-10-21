"""agentmail/<address> — new mail to one AgentMail address, plus-alias included.

Mail to `name+alias@domain` lands in the base inbox `name@domain` with the alias in `to`, so
every address of one inbox shares one poll: GET /v0/inboxes/<base>/messages (newest first,
metadata with a short `preview`). Each new received message gives one `mail` event on
`agentmail/<address>` for every address of the inbox in its To or Cc, matched exactly: the base
address's topic doesn't get mail sent only to an alias, and each alias is its own topic.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from email.utils import getaddresses

from .. import config
from .base import EventList, PollResult, Source, SourceError, TopicError, first_line, timestamp

OP_REF_ENV = "AGENT_EVENTS_AGENTMAIL_OP_REF"
# No default: set AGENTMAIL_API_KEY, or AGENT_EVENTS_AGENTMAIL_OP_REF=op://... in ~/.agent-events/env.
DEFAULT_OP_REF = None

ADDRESS_RE = re.compile(r"^[a-z0-9._%+=-]+@[a-z0-9.-]+\.[a-z]{2,}$")
# Newest messages read per poll; more than this arriving between two polls loses the oldest.
PAGE = 50
PREVIEW = 300


def base_url() -> str:
    return (os.environ.get("AGENTMAIL_API_URL") or "https://api.agentmail.to/v0").rstrip("/")


def api_key() -> str:
    key = config.secret(["AGENTMAIL_API_KEY"], OP_REF_ENV, DEFAULT_OP_REF)
    if not key:
        raise SourceError(f"no AgentMail API key: set AGENTMAIL_API_KEY in {config.env_path()}, or "
                          f"{OP_REF_ENV}=op://... to read it with the 1Password CLI (`op read`)")
    return key


def inbox_of(address: str) -> str:
    """The inbox plus-addressed mail lands in: name+alias@domain → name@domain."""
    local, domain = address.split("@", 1)
    return f"{local.split('+', 1)[0]}@{domain}"


def fetch_messages(inbox: str) -> list:
    req = urllib.request.Request(
        f"{base_url()}/inboxes/{urllib.parse.quote(inbox, safe='')}/messages?limit={PAGE}",
        headers={"Authorization": f"Bearer {api_key()}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise TopicError(f"no AgentMail inbox {inbox}") from None
        # The key is not re-read here: a rejected key would be read again on every poll.
        raise SourceError(f"AgentMail API HTTP {e.code}: {e.read().decode(errors='replace')[:300]}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise SourceError(f"AgentMail API request failed: {e}") from None
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        raise SourceError(f"AgentMail API: unexpected response {str(data)[:300]}")
    return data["messages"]


def addresses(value) -> list:
    """Lowercased bare addresses of a to/cc field (a string or a list, maybe with names)."""
    items = [value] if isinstance(value, str) else list(value or [])
    return [a.lower() for _, a in getaddresses([str(x) for x in items]) if "@" in a]


def snapshot(inbox: str, messages: list) -> dict:
    out = []
    for m in messages:
        out.append({
            "id": m["message_id"], "thread": m.get("thread_id") or "", "at": m.get("timestamp") or "",
            "from": m.get("from") or "", "subject": m.get("subject") or "",
            "to": addresses(m.get("to")), "cc": addresses(m.get("cc")),
            "received": "received" in (m.get("labels") or []),
            "preview": (m.get("preview") or "")[:PREVIEW],
        })
    return {"inbox": inbox, "messages": out}


def diff(old: dict, new: dict) -> EventList:
    inbox = new["inbox"]
    ev = EventList(f"agentmail/{inbox}")
    seen = {m["id"] for m in old["messages"]}
    # A message older than everything last seen isn't new: it moved into the page because a
    # newer one was deleted.
    floor = min((timestamp(m["at"]) or 0 for m in old["messages"]), default=0)
    for m in reversed(new["messages"]):  # oldest first
        if m["id"] in seen or not m["received"] or (timestamp(m["at"]) or 0) < floor:
            continue
        sender = m["from"]
        for address in dict.fromkeys(m["to"] + m["cc"]):
            if inbox_of(address) != inbox:
                continue
            ev.add("mail", f"mail to {address} from {sender}: {first_line(m['subject'], 120) or '(no subject)'}",
                   body=m["preview"], actor=sender, topic=f"agentmail/{address}", dedupe=f"mail:{m['id']}",
                   at=m["at"], address=address, message_id=m["id"], thread_id=m["thread"], subject=m["subject"],
                   **{"from": sender})
    return ev


class AgentMail(Source):
    type = "agentmail"
    interval = 60.0
    help = ("Mail to one AgentMail address, plus-alias included (each alias is its own topic; the base "
            "address gets only mail sent to it): sender, subject, time, message id, a short preview")
    examples = ["agentmail/simon-says@agentmail.to", "agentmail/simon-says+push-delays@agentmail.to"]

    def normalize(self, parts):
        if len(parts) != 1 or not ADDRESS_RE.match(parts[0].lower()):
            raise TopicError("expected agentmail/<address>, e.g. agentmail/name+alias@agentmail.to")
        return [parts[0].lower()]

    def poll_key(self, parts):
        return [inbox_of(parts[0])]

    def poll(self, key, state):
        new = snapshot(key[0], fetch_messages(key[0]))
        events = diff(state, new) if state else []
        return PollResult(new, events, f"AgentMail inbox {key[0]}")
