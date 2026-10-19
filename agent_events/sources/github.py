"""github/<owner>/<repo>/<pr-number> — everything that happens on a pull request.

Polls one GraphQL query through the `gh` CLI (reuses its auth) and diffs: state
(merged/closed/reopened/draft), title, description, base branch, labels, assignees,
requested reviewers, review decision, merge conflicts, pushed commits (incl. force
pushes), CI (failed checks individually, overall result as one event), comments,
inline review comments, reviews, and resolved/unresolved review threads.
"""
from __future__ import annotations

import json
import os
import re
import subprocess

from .base import EventList, PollResult, Source, SourceError, TopicError, list_changes, text_diff

QUERY = """
query($owner: String!, $repo: String!, $number: Int!) {
  viewer { login }
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      title url state isDraft body baseRefName headRefName headRefOid mergeable reviewDecision
      author { login }
      mergedBy { login }
      assignees(first: 20) { nodes { login } }
      labels(first: 30) { nodes { name } }
      reviewRequests(first: 20) {
        nodes { requestedReviewer { __typename ... on User { login } ... on Team { slug } ... on Bot { login } } }
      }
      comments(last: 100) { nodes { id fullDatabaseId author { login } body url createdAt updatedAt } }
      reviews(last: 100) { nodes { id author { login } state body url submittedAt updatedAt } }
      reviewThreads(last: 100) {
        nodes {
          id isResolved path line
          comments(last: 50) { nodes { id author { login } body url createdAt updatedAt state } }
        }
      }
      commits(last: 30) {
        totalCount
        nodes {
          commit {
            oid messageHeadline
            statusCheckRollup {
              state
              contexts(first: 100) {
                nodes {
                  __typename
                  ... on CheckRun { name status conclusion detailsUrl }
                  ... on StatusContext { context state targetUrl }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# A token without read:org (a fine-grained or narrow classic token, e.g. on a
# Hyperfocus host) can't read a requested team's slug: the query then leaves it out.
TEAM_SLUG = " ... on Team { slug }"
NO_TEAM_QUERY = QUERY.replace(TEAM_SLUG, "")
_no_read_org = False

FAILED = {"FAILURE", "ERROR", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE"}
PASSED = {"SUCCESS", "NEUTRAL", "SKIPPED", "STALE"}
TERMINAL_ROLLUP = {"SUCCESS", "FAILURE", "ERROR"}
# Checks that report nothing an agent should act on (e.g. a shadow CI run beside the real one):
# no events, and left out of the pass/fail counts and the overall result. A regex on the name.
IGNORE_CHECKS_ENV = "AGENT_EVENTS_IGNORE_CHECKS"
IGNORE_CHECKS_DEFAULT = r"\(shadow\)"


def counted(snap: dict) -> tuple[dict, str | None]:
    """The checks that count, and the overall result without the ignored ones."""
    pattern = os.environ.get(IGNORE_CHECKS_ENV, IGNORE_CHECKS_DEFAULT)
    checks = {n: c for n, c in snap["checks"].items() if not (pattern and re.search(pattern, n))}
    rollup = snap["rollup"]
    if len(checks) == len(snap["checks"]) or rollup not in TERMINAL_ROLLUP:
        return checks, rollup
    states = [c["s"] for c in checks.values()]
    if any(s in FAILED for s in states):
        return checks, rollup
    return checks, "SUCCESS" if all(s in PASSED for s in states) else "PENDING"
# The GitHub App a PR comment was posted through (REST performed_via_github_app; GraphQL doesn't
# have it), by slug. Agents share their operator's account, so "via Linear" tells a comment the
# person wrote in Linear's review UI apart from one an agent posted with the same login.
APP_NAMES = {"linear-code": "Linear"}
URL_RE = re.compile(r"^https?://(?:www\.)?github\.com/([^/]+)/([^/]+)/pull/(\d+)", re.I)


def graphql(query: str, **variables) -> dict:
    cmd = [os.environ.get("AGENT_EVENTS_GH", "gh"), "api", "graphql", "-f", f"query={query}"]
    for name, value in variables.items():
        cmd += ["-F" if isinstance(value, int) else "-f", f"{name}={value}"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise SourceError(f"gh api failed: {e}") from e
    try:
        data = json.loads(out.stdout)
    except ValueError:
        raise SourceError(f"gh api failed: {(out.stderr or out.stdout).strip()[:500]}") from None
    if data.get("errors"):
        raise SourceError("; ".join(e.get("message", str(e)) for e in data["errors"])[:500])
    if out.returncode != 0 or "data" not in data:
        raise SourceError(f"gh api failed: {(out.stderr or out.stdout).strip()[:500]}")
    return data["data"]


def app_slug(owner: str, repo: str, comment_id: str) -> str:
    """The slug of the GitHub App a PR (issue) comment was posted through, "" if none or unknown."""
    cmd = [os.environ.get("AGENT_EVENTS_GH", "gh"), "api", f"repos/{owner}/{repo}/issues/comments/{comment_id}",
           "--jq", ".performed_via_github_app.slug // \"\""]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def add_apps(owner: str, repo: str, old: dict, new: dict) -> None:
    """Fill in "via" for the PR comments this poll reports (new or edited); keep it for the rest."""
    for cid, c in new["comments"].items():
        prev = old["comments"].get(cid)
        if prev and "via" in prev:
            c["via"] = prev["via"]
        elif c["kind"] == "comment" and c.get("db") and (prev is None or prev["body"] != c["body"]):
            c["via"] = app_slug(owner, repo, c["db"])


def login(node: dict | None) -> str:
    return ((node or {}).get("author") or {}).get("login") or "ghost"


def snapshot(data: dict) -> dict:
    pr = data["repository"]["pullRequest"]
    commits = [n["commit"] for n in pr["commits"]["nodes"]]
    head = commits[-1] if commits else {}
    rollup = head.get("statusCheckRollup") or {}
    checks = {}
    for c in ((rollup.get("contexts") or {}).get("nodes") or []):
        if c["__typename"] == "CheckRun":
            checks[c["name"]] = {"s": c["conclusion"] or c["status"], "url": c.get("detailsUrl")}
        elif c["__typename"] == "StatusContext":
            checks[c["context"]] = {"s": c["state"], "url": c.get("targetUrl")}

    comments = {}
    for c in pr["comments"]["nodes"]:
        comments[c["id"]] = {"kind": "comment", "author": login(c), "body": c["body"],
                             "url": c["url"], "updated": c["updatedAt"], "created": c["createdAt"],
                             "db": c.get("fullDatabaseId")}
    threads = {}
    for t in pr["reviewThreads"]["nodes"]:
        where = f"{t['path']}:{t['line']}" if t.get("line") else t["path"]
        first = (t["comments"]["nodes"] or [{}])[0]
        threads[t["id"]] = {"resolved": t["isResolved"], "where": where, "url": first.get("url")}
        for c in t["comments"]["nodes"]:
            if c.get("state") == "PENDING":  # part of an unsubmitted draft review
                continue
            comments[c["id"]] = {"kind": "review_comment", "author": login(c), "body": c["body"],
                                 "url": c["url"], "updated": c["updatedAt"], "where": where,
                                 "created": c["createdAt"]}
    reviews = {}
    for r in pr["reviews"]["nodes"]:
        if r["state"] == "PENDING":  # the viewer's own unsubmitted draft
            continue
        reviews[r["id"]] = {"author": login(r), "state": r["state"], "body": r["body"],
                            "url": r["url"], "updated": r["updatedAt"] or r["submittedAt"],
                            "created": r["submittedAt"] or r["updatedAt"]}

    reviewers = []
    for n in pr["reviewRequests"]["nodes"]:
        rv = n.get("requestedReviewer") or {}
        name = rv.get("login") or (f"team:{rv['slug']}" if rv.get("slug") else None)
        if name:
            reviewers.append(name)

    return {
        "viewer": (data.get("viewer") or {}).get("login"),
        "title": pr["title"], "url": pr["url"], "body": pr["body"] or "",
        "state": "MERGED" if pr["state"] == "MERGED" else pr["state"],
        "draft": pr["isDraft"], "base": pr["baseRefName"], "head_ref": pr["headRefName"],
        "mergeable": pr["mergeable"], "review_decision": pr["reviewDecision"],
        "author": login(pr), "merged_by": (pr.get("mergedBy") or {}).get("login"),
        "assignees": sorted(n["login"] for n in pr["assignees"]["nodes"]),
        "labels": sorted(n["name"] for n in pr["labels"]["nodes"]),
        "reviewers": sorted(reviewers),
        "head": pr["headRefOid"],
        "commits": [{"oid": c["oid"], "headline": c["messageHeadline"]} for c in commits],
        "total_commits": pr["commits"]["totalCount"],
        "checks": checks, "rollup": rollup.get("state"),
        "comments": comments, "reviews": reviews, "threads": threads,
    }


def describe(s: dict) -> str:
    state = s["state"].lower() + (", draft" if s["draft"] and s["state"] == "OPEN" else "")
    return f"{s['title']} ({state}) {s['url']}"


def diff(old: dict, new: dict, topic: str) -> EventList:
    ev = EventList(topic, new["url"])
    me = new.get("viewer")

    def by(name: str | None) -> dict:
        return {"actor": name or "", "mine": bool(me and name == me)}

    if old["state"] != new["state"]:
        if new["state"] == "MERGED":
            ev.add("merged", f"PR merged into {new['base']}" + (f" by @{new['merged_by']}" if new["merged_by"] else ""),
                   **by(new["merged_by"]))
        elif new["state"] == "CLOSED":
            ev.add("closed", "PR closed without merging")
        else:
            ev.add("reopened", "PR reopened")
    if old["draft"] != new["draft"]:
        ev.add("draft", "PR converted to draft" if new["draft"] else "PR marked ready for review")
    if old["title"] != new["title"]:
        ev.add("title", f"title changed: {old['title']!r} → {new['title']!r}")
    if old["base"] != new["base"]:
        ev.add("base", f"base branch changed: {old['base']} → {new['base']}")
    if old["body"] != new["body"]:
        ev.add("description", "PR description edited", body=text_diff(old["body"], new["body"]))
    for key, label in (("labels", "label"), ("assignees", "assignee"), ("reviewers", "review requested from")):
        added, removed = list_changes(old[key], new[key])
        for x in added:
            ev.add(key, f"{label} added: {x}" if key != "reviewers" else f"review requested from {x}")
        for x in removed:
            ev.add(key, f"{label} removed: {x}" if key != "reviewers" else f"review request removed: {x}")
    if old["review_decision"] != new["review_decision"] and new["state"] == "OPEN":
        ev.add("review_decision", f"review decision: {(old['review_decision'] or 'none').lower()} → "
                                  f"{(new['review_decision'] or 'none').lower()}")
    # GitHub computes mergeability lazily; UNKNOWN is noise.
    if old["mergeable"] != new["mergeable"] and "UNKNOWN" not in (old["mergeable"], new["mergeable"]):
        if new["mergeable"] == "CONFLICTING":
            ev.add("conflicts", f"PR now has merge conflicts with {new['base']}")
        elif old["mergeable"] == "CONFLICTING":
            ev.add("conflicts", "merge conflicts resolved")

    if old["head"] != new["head"]:
        old_oids = {c["oid"] for c in old["commits"]}
        pushed = [c for c in new["commits"] if c["oid"] not in old_oids]
        forced = old["head"] not in {c["oid"] for c in new["commits"]} and new["total_commits"] <= len(new["commits"])
        lines = "\n".join(f"{c['oid'][:8]} {c['headline']}" for c in pushed[-15:])
        what = f"{len(pushed)} new commit{'s' if len(pushed) != 1 else ''}"
        ev.add("push", f"force-pushed ({what})" if forced else f"{what} pushed to {new['head_ref']}",
               body=lines, url=f"{new['url']}/commits")

    same_head = old["head"] == new["head"]
    (old_checks, old_rollup), (new_checks, new_rollup) = counted(old), counted(new)
    for name, c in new_checks.items():
        prev = old_checks.get(name) if same_head else None
        if c["s"] in FAILED and (prev is None or prev["s"] != c["s"]):
            ev.add("check_failed", f"check failed: {name} ({c['s'].lower()})", url=c.get("url") or "")
    if new_rollup in TERMINAL_ROLLUP and (not same_head or old_rollup != new_rollup):
        states = [c["s"] for c in new_checks.values()]
        ok = sum(s in ("SUCCESS", "NEUTRAL", "SKIPPED") for s in states)
        bad = sum(s in FAILED for s in states)
        ev.add("checks", f"CI {new_rollup.lower()}: {ok} passed, {bad} failed, {len(states)} total",
               url=f"{new['url']}/checks")

    def fresh(item: dict, old_items: dict) -> bool:
        """New to us, not an old item that slid into the (last N) window after a deletion."""
        created = [o.get("created") or "" for o in old_items.values()]
        return not created or (item.get("created") or "9") >= min(created)

    for cid, c in new["comments"].items():
        where = f" on {c['where']}" if c.get("where") else ""
        what = "review comment" if c["kind"] == "review_comment" else "comment"
        app = c.get("via") if c.get("via") != c["author"] else ""  # an app's own bot needs no "via"
        # Posted through an app (Linear's review UI), so the person, not an agent on their login.
        who = {**by(c["author"]), "via": app, "mine": False} if app else by(c["author"])
        through = f" via {APP_NAMES.get(app, app)}" if app else ""
        prev = old["comments"].get(cid)
        if prev is None:
            if not fresh(c, old["comments"]):
                continue
            ev.add(c["kind"], f"{what}{where} by @{c['author']}{through}", body=c["body"], url=c["url"],
                   at=c.get("created"), **who)
        elif prev["body"] != c["body"]:
            ev.add(c["kind"] + "_edited", f"{what}{where} edited by @{c['author']}{through}",
                   body=c["body"], url=c["url"], at=c.get("updated"), **who)
    for rid, r in new["reviews"].items():
        prev = old["reviews"].get(rid)
        verdict = r["state"].lower().replace("_", " ")
        if prev is None:
            if not fresh(r, old["reviews"]) or (r["state"] == "COMMENTED" and not r["body"].strip()):
                continue  # just a container for inline comments, reported individually
            ev.add("review", f"review by @{r['author']}: {verdict}", body=r["body"], url=r["url"], at=r.get("created"),
                   **by(r["author"]))
        elif prev["state"] != r["state"]:
            ev.add("review", f"review by @{r['author']} is now {verdict}", url=r["url"], **by(r["author"]))
        elif prev["body"] != r["body"]:
            ev.add("review_edited", f"review by @{r['author']} edited", body=r["body"], url=r["url"],
                   at=r.get("updated"), **by(r["author"]))
    for tid, t in new["threads"].items():
        prev = old["threads"].get(tid)
        if prev is not None and prev["resolved"] != t["resolved"]:
            ev.add("thread", f"review thread on {t['where']} {'resolved' if t['resolved'] else 'unresolved'}",
                   url=t.get("url") or "")
    return ev


class GitHub(Source):
    type = "github"
    interval = 60.0
    min_spacing = 2.0  # 5000 GraphQL points/hour, shared with every gh call
    help = "A GitHub pull request: comments, reviews, CI, pushes, merges, labels, reviewers, ..."
    examples = ["github/<owner>/<repo>/<pr-number>", "https://github.com/<owner>/<repo>/pull/<n>"]

    def normalize(self, parts):
        if len(parts) == 4 and parts[2] in ("pull", "pulls"):
            parts = [parts[0], parts[1], parts[3]]
        if len(parts) != 3 or not parts[2].isdigit():
            raise TopicError("expected github/<owner>/<repo>/<pr-number>")
        return [parts[0].lower(), parts[1].lower(), str(int(parts[2]))]

    def poll(self, key, state):
        global _no_read_org
        owner, repo, number = key
        data = None
        if not _no_read_org:
            try:
                data = graphql(QUERY, owner=owner, repo=repo, number=int(number))
            except SourceError as e:
                if "read:org" not in str(e):
                    raise
                _no_read_org = True
        if data is None:
            data = graphql(NO_TEAM_QUERY, owner=owner, repo=repo, number=int(number))
        if not (data.get("repository") or {}).get("pullRequest"):
            raise SourceError(f"pull request {owner}/{repo}#{number} not found")
        new = snapshot(data)
        if state and new["mergeable"] == "UNKNOWN":
            # GitHub computes mergeability lazily; keep the last known value so a
            # MERGEABLE → UNKNOWN → CONFLICTING sequence is still seen as a change.
            new["mergeable"] = state["mergeable"]
        if state:
            add_apps(owner, repo, state, new)
        events = diff(state, new, "/".join([self.type] + key)) if state else []
        return PollResult(new, events, describe(new))

    def from_url(self, url):
        m = URL_RE.match(url.strip())
        return f"github/{m.group(1)}/{m.group(2)}/{m.group(3)}" if m else None
