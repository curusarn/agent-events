"""docs/<batch>[/<article>] — a staged Better Stack docs batch, or one article version in it.

Docs edits are staged as unpublished article versions, grouped into a batch by their
source_ref (usually the Linear issue, e.g. U-9225), and a human publishes or discards them in
article admin. This polls the docs admin API's GET /batches/<source_ref> (betterstack
DocsAdmin::VersionsController#batch), which lists a batch's versions published or not:

- a version that gains a published_at was published (`published`, with who published it);
- an unpublished version that is gone was discarded (`discarded`: removal deletes the row),
  unless the same article has a new unpublished version in the batch (`restaged`);
- a new version is `staged`;
- a staged version revised in place (new text under the same id, by its own app or another) is
  `revised`, one marked ready for review is `promoted`, and one whose live article changed under
  it is `stale` (it must be restaged before it can publish). These come from GET
  /unpublished?source_ref=, polled only while the batch has unpublished versions;
- when the last unpublished version of the batch is published or discarded, the batch topic
  also gets one `batch` event listing every article's outcome.

Article events go on `docs/<batch>/<article>`. A `/` in a source_ref or canonical_id is written
as `~` in the topic (as docs-terminal names its article files), since topics split on `/`.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

from .. import config
from .base import EventList, PollResult, Source, SourceError, TopicError

OP_REF_ENV = "AGENT_EVENTS_DOCS_OP_REF"
# No default: set DOCS_ADMIN_API_KEY, or AGENT_EVENTS_DOCS_OP_REF=op://... in ~/.agent-events/env.
DEFAULT_OP_REF = None

SEGMENT_RE = re.compile(r"^[A-Za-z0-9._#~-]+$")
ADMIN_URL_RE = re.compile(r"^https?://[^/]+/admin/articles/unpublished/([^?#]+)", re.I)
# Only on the batch event: the Linear issue moves on once the whole batch is out, not one article.
DONE_HINT = ("The batch is live: if your task staged it, move its Linear issue on now and comment 🤖 with "
             "the link.")


def base_url() -> str:
    return (os.environ.get("DOCS_ADMIN_API_URL") or "https://betterstack.com").rstrip("/")


def api_key() -> str:
    key = config.secret(["DOCS_ADMIN_API_KEY"], OP_REF_ENV, DEFAULT_OP_REF)
    if not key:
        raise SourceError(f"no docs admin API key: set DOCS_ADMIN_API_KEY in {config.env_path()}, or "
                          f"{OP_REF_ENV}=op://... to read it with the 1Password CLI (`op read`)")
    return key


def admin_url(ref: str) -> str:
    return f"{base_url()}/admin/articles/unpublished/{quote_glob(ref)}"


def quote_glob(ref: str) -> str:
    """A source_ref in a Rails glob path: encode each segment, keep the `/` separators."""
    return "/".join(urllib.parse.quote(part, safe="") for part in ref.split("/"))


def get_json(path: str) -> dict:
    """GET a docs admin API path; {} when it doesn't exist."""
    req = urllib.request.Request(f"{base_url()}/api/internal/docs-admin{path}", headers={
        "Authorization": f"Bearer {api_key()}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {}
        if e.code in (401, 403):
            config.forget_secret(OP_REF_ENV, DEFAULT_OP_REF)
        raise SourceError(f"docs admin API HTTP {e.code}: {e.read().decode(errors='replace')[:300]}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise SourceError(f"docs admin API request failed: {e}") from None
    return data if isinstance(data, dict) else {}


def fetch_batch(ref: str) -> list:
    return get_json(f"/batches/{quote_glob(ref)}").get("versions") or []


def fetch_unpublished(ref: str) -> list:
    """The batch's unpublished versions with their staged state (digest, draft, stale)."""
    return get_json(f"/unpublished?source_ref={urllib.parse.quote(ref, safe='')}").get("unpublished_versions") or []


def snapshot(ref: str, versions: list, unpublished: list = ()) -> dict:
    members = {}
    for v in versions:
        by = v.get("published_by") or {}
        members[str(v["version_id"])] = {
            "article": v["article_canonical_id"], "published_at": v.get("published_at"),
            "by": by.get("name") or v.get("published_by_app") or "",
        }
    for u in unpublished:
        m = members.get(str(u.get("id")))
        if m is not None:
            m.update(digest=u.get("staged_digest"), draft=bool(u.get("draft")), stale=bool(u.get("stale")),
                     editor=u.get("edited_by_app") or u.get("source_app") or "", admin=u.get("admin_url") or "")
    return {"ref": ref, "members": members}


def topic_of(ref: str, article: str | None = None) -> str:
    parts = ["docs", ref.replace("/", "~")]
    if article:
        parts.append(article.replace("/", "~"))
    return "/".join(parts)


def diff(old: dict, new: dict) -> EventList:
    ref = new["ref"]
    ev = EventList(topic_of(ref), admin_url(ref))
    before, after = old["members"], new["members"]
    restaged = set()  # new version ids that replace a discarded one of the same article

    def add(kind, vid, m, summary, status, body="", at=None, dedupe=None, actor=""):
        ev.add(kind, summary, body=body, actor=actor or (m.get("by", "") if kind == "published" else ""),
               topic=topic_of(ref, m["article"]), dedupe=dedupe or f"{kind}:{vid}", at=at,
               url=m.get("admin") or "", article=m["article"], version=int(vid), batch=ref, status=status)

    # Changes to a version still staged, known only when both polls read its staged state.
    for vid, m in after.items():
        prev = before.get(vid)
        if m["published_at"] or not prev or prev["published_at"] or "digest" not in m or "digest" not in prev:
            continue
        if m["digest"] and prev["digest"] and m["digest"] != prev["digest"]:
            add("revised", vid, m, f"revised: {m['article']} (batch {ref}) by {m['editor'] or 'unknown'}", "staged",
                dedupe=f"revised:{vid}:{m['digest'][:12]}", actor=m["editor"])
        if prev["draft"] and not m["draft"]:
            add("promoted", vid, m, f"ready for review: {m['article']} (batch {ref})", "staged")
        if m["stale"] and not prev["stale"]:
            add("stale", vid, m, f"stale: {m['article']} (batch {ref}): its live article changed since it was "
                "staged, so it can't publish until it's restaged", "stale", dedupe=f"stale:{vid}@{ev.stamp}")

    for vid, m in after.items():
        prev = before.get(vid)
        if m["published_at"] and not (prev and prev["published_at"]):
            add("published", vid, m, f"published: {m['article']} (batch {ref}) by {m['by'] or 'unknown'}",
                "published", at=m["published_at"])
    for vid, m in before.items():
        if vid in after or m["published_at"]:
            continue
        again = [n for n, x in after.items()
                 if n not in before and x["article"] == m["article"] and not x["published_at"]]
        if again:
            restaged.add(again[0])
            add("restaged", again[0], after[again[0]],
                f"restaged: {m['article']} (batch {ref}), version {vid} → {again[0]}", "staged")
        else:
            add("discarded", vid, m, f"discarded: {m['article']} (batch {ref})", "discarded")
    for vid, m in after.items():
        if vid not in before and not m["published_at"] and vid not in restaged:
            add("staged", vid, m, f"staged: {m['article']} (batch {ref})", "staged")

    def pending(members):
        return [m for m in members.values() if not m["published_at"]]

    if pending(before) and not pending(after):
        published = sorted({x["article"] for v, x in after.items()
                            if x["published_at"] and not (before.get(v) or {}).get("published_at")})
        discarded = sorted({x["article"] for v, x in before.items() if v not in after and not x["published_at"]})
        outcome = "published" if not discarded else "discarded" if not published else "settled"
        lines = [f"published: {a}" for a in published] + [f"discarded: {a}" for a in discarded]
        ev.add("batch", f"batch {ref} {outcome}: {len(published)} published, {len(discarded)} discarded",
               body="\n".join(lines + ([DONE_HINT] if published else [])), dedupe=f"batch:{ref}@{ev.stamp}",
               batch=ref, status=outcome, published=published, discarded=discarded)
    return ev


def describe(s: dict) -> str:
    members = s["members"].values()
    live = sum(1 for m in members if m["published_at"])
    return f"docs batch {s['ref']}: {len(members) - live} staged, {live} published {admin_url(s['ref'])}"


class Docs(Source):
    type = "docs"
    interval = 60.0
    help = ("A staged Better Stack docs batch (source_ref, e.g. U-9225) or one article version in it: "
            "published (with who), discarded, restaged, staged, revised, promoted, stale; '/' in a ref or article is written '~'")
    examples = ["docs/<source_ref>", "docs/<source_ref>/<canonical_id>",
                "https://betterstack.com/admin/articles/unpublished/<source_ref>"]

    def normalize(self, parts):
        if not parts or len(parts) > 2 or not all(SEGMENT_RE.match(p) for p in parts):
            raise TopicError("expected docs/<source_ref> or docs/<source_ref>/<canonical_id> "
                             "(write a '/' inside either as '~')")
        return list(parts)

    def poll_key(self, parts):
        return parts[:1]

    def poll(self, key, state):
        ref = key[0].replace("~", "/")
        versions = fetch_batch(ref)
        pending = any(not v.get("published_at") for v in versions)
        new = snapshot(ref, versions, fetch_unpublished(ref) if pending else [])
        events = diff(state, new) if state else []
        return PollResult(new, events, describe(new))

    def resolve(self, parts, state):
        if not state["members"]:
            raise TopicError(f"no docs batch {state['ref']!r}: nothing was ever staged under that source_ref")
        if len(parts) == 1:
            return parts, ""
        article = parts[1].replace("~", "/")
        versions = {v: m for v, m in state["members"].items() if m["article"] == article}
        if not versions:
            raise TopicError(f"{article} is not in docs batch {state['ref']}")
        latest = versions[max(versions, key=int)]
        return parts, f"{article}: {'published' if latest['published_at'] else 'staged'}"

    def from_url(self, url):
        m = ADMIN_URL_RE.match(url.strip())
        return topic_of(urllib.parse.unquote(m.group(1)).rstrip("/")) if m else None
