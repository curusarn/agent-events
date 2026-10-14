"""agent-events command line."""
from __future__ import annotations

import argparse
import json
import os
import plistlib
import re
import select
import shlex
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from . import client, config
from .client import DaemonDown

WATCH_AGAIN = "Handle these, then run `agent-events watch` in the background again to keep receiving events."
NO_WATCH_NEEDED = ("Handle these. You don't need to start `agent-events watch` again: the agent-events hook keeps "
                   "watching for you and wakes you with the next events.")
HOOK_DELIVERY = ("agent-events: new events for you, delivered by the agent-events hook (this is not an error). "
                 "Handle them; there is nothing to restart, the hook keeps watching.")
# Only FYI events (they never wake an agent; shown at the start of its next turn).
FYI_DELIVERY = ("agent-events: FYI since your last turn (you were not woken for these). Act on one only if it "
                "concerns your task; no acknowledgement needed.")
FYI_MARK = " · FYI"


def delivery_text(events: list[dict]) -> str:
    """The text that hands events to an agent: the hook preamble, then the events."""
    from .store import is_quiet
    head = FYI_DELIVERY if events and all(is_quiet(e) for e in events) else HOOK_DELIVERY
    return head + "\n\n" + format_events(events)
# Longest a hook watch lives (Claude Code kills it at the hook timeout); agents idle longer than
# this are forgotten by the daemon anyway.
HOOK_WATCH_TIMEOUT = 7 * 86400


def agent_id(args) -> str:
    agent = getattr(args, "agent", None) or os.environ.get("AGENT_EVENTS_AGENT") or os.environ.get("CLAUDE_CODE_SESSION_ID")
    if not agent:
        sys.exit("agent-events: no agent id. Run inside Claude Code (uses $CLAUDE_CODE_SESSION_ID), "
                 "or pass --agent / set AGENT_EVENTS_AGENT.")
    return agent


def agent_req(args, op: str, **kw) -> dict:
    agent = agent_id(args)
    explicit = getattr(args, "agent", None)
    if explicit and explicit != os.environ.get("CLAUDE_CODE_SESSION_ID"):
        # Acting on behalf of another agent (e.g. Hyperfocus subscribing an agent it spawned):
        # our pid/cwd are not that agent's, so don't overwrite what the daemon knows about it.
        return {"op": op, "agent": agent, "pid": None, "cwd": None, **kw}
    pid = os.environ.get("CLAUDE_PID")
    return {"op": op, "agent": agent, "pid": int(pid) if pid and pid.isdigit() else None,
            "cwd": os.getcwd(), **kw}


def call(req: dict, timeout: float | None = 120) -> dict:
    try:
        resp = client.request(req, timeout=timeout)
    except DaemonDown as e:
        sys.exit(f"agent-events: daemon unavailable: {e}")
    if not resp.get("ok"):
        sys.exit(f"agent-events: {resp.get('error')}")
    return resp


def ago(ts: float | None) -> str:
    if not ts:
        return "never"
    s = max(0, time.time() - ts)
    if s < 90:
        return f"{int(s)}s ago"
    if s < 5400:
        return f"{int(s / 60)}m ago"
    if s < 172800:
        return f"{s / 3600:.1f}h ago"
    return f"{int(s / 86400)}d ago"


def clip(text: str, max_lines: int = 20, max_chars: int = 2000) -> str:
    text = (text or "").replace("\r\n", "\n").strip()
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + " …"
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[:max_lines] + [f"… ({len(lines) - max_lines} more lines)"]
    return "\n".join(lines)


BRACKET_RE = re.compile(r"^\[([^\]]*)\]\s*")


def _squash(text: str) -> str:
    return " ".join((text or "").split())


def event_header(e: dict, push: bool) -> str:
    """One compact line: time, sender, thread, and the summary unless the body already says it."""
    data = e.get("data") or {}
    # When it was written (Hyperfocus's sent_at); a held post is published later.
    try:
        at = float(data.get("sent_at") or e["created_at"])
    except (TypeError, ValueError):
        at = e["created_at"]
    parts = [time.strftime("%H:%M", time.localtime(at))]
    if push and e.get("actor"):
        # Polled sources already name the author in the summary; published messages don't.
        parts.append(f"from {e['actor']}")
    summary = e.get("summary") or ""
    thread, title = str(data.get("thread") or ""), str(data.get("thread_title") or "")
    m = BRACKET_RE.match(summary) if push else None
    if m:  # Hyperfocus's "[thread title] first line…" summaries
        title = title or m.group(1)
        summary = summary[m.end():]
    if thread or title:
        parts.append(" ".join(["thread"] + ([thread] if thread else []) + ([f'"{title}"'] if title else [])))
    rest = _squash(summary).rstrip("…").rstrip()
    if rest and not _squash(e.get("body") or "").startswith(rest):
        parts.append(summary.strip())
    header = " · ".join(parts)
    if data.get("quiet") in (True, 1, "1", "true"):
        header += FYI_MARK
    if data.get("mine"):
        header += " (you — may be your own action)"
    return header


def format_events(events: list[dict], title: bool = True) -> str:
    from .sources import is_push
    out = [f"{len(events)} new event{'s' if len(events) != 1 else ''}:"] if title else []
    by_topic: dict[str, list] = {}
    for e in events:
        by_topic.setdefault(e["topic"], []).append(e)
    for topic, evs in by_topic.items():
        if out:
            out.append("")
        target = evs[0].get("target") or ""
        out.append(f"{topic}" + (f"  —  {target}" if target else ""))
        push = is_push(topic)
        for e in evs:
            out.append(f"  {event_header(e, push)}")
            if e.get("body"):
                # A published message is the whole point of the event; don't cut a normal one short.
                limits = (60, 8000) if push and e.get("kind") == "message" else (20, 2000)
                out.append(textwrap.indent(clip(e["body"], *limits), "    "))
            if e.get("url") and e["url"] not in target:
                out.append(f"    {e['url']}")
    return "\n".join(out)


# --- commands ----------------------------------------------------------------------------

def cmd_subscribe(args):
    failed = False
    for topic in args.topics:
        try:
            flt = {"about": [t.strip() for a in (args.about or []) for t in a.split(",") if t.strip()],
                   "mine": bool(args.mine)}
            resp = client.request(agent_req(args, "subscribe", topic=topic, filter=flt), timeout=180)
        except DaemonDown as e:
            sys.exit(f"agent-events: daemon unavailable: {e}")
        if not resp.get("ok"):
            print(f"✗ {topic}: {resp.get('error')}", file=sys.stderr)
            failed = True
            continue
        verb = "subscribed to" if resp["new"] else "already subscribed to"
        print(f"✓ {verb} {resp['topic']}")
        for line in (resp.get("summary"), resp.get("detail")):
            if line:
                print(f"    {line}")
    status = call(agent_req(args, "list"))
    if hook_watch_installed():
        print("\nThe agent-events hook wakes you with new events; you don't need to run `agent-events watch`.")
    elif not status["watching"]:
        print("\nNo watch is running for this agent: start `agent-events watch` in the background.")
    return 1 if failed else 0


def cmd_unsubscribe(args):
    if not args.topics and not args.all:
        sys.exit("agent-events: give topics to unsubscribe from, or --all")
    resp = call(agent_req(args, "unsubscribe", topics=args.topics, all=args.all))
    if not resp["removed"]:
        print("No matching subscriptions.")
    for t in resp["removed"]:
        print(f"✓ unsubscribed from {t}")


def cmd_list(args):
    resp = call(agent_req(args, "list"))
    if args.json:
        print(json.dumps(resp, indent=2))
        return
    subs = resp["subscriptions"]
    print(f"Agent {resp['agent']}")
    if not subs:
        print("  No subscriptions.")
        return
    if resp["watching"]:
        watch = "running"
    elif hook_watch_installed():
        watch = "armed by the agent-events hook when your turn ends"
    else:
        watch = "NOT running — start `agent-events watch` in the background"
    print(f"Watch: {watch}" + (f"; {resp['pending']} event(s) waiting" if resp["pending"] else ""))
    for s in subs:
        print(f"\n  {s['topic']}  (since {ago(s['since'])})")
        if s.get("push"):
            print("    push topic (events are published into it, not polled)")
            continue
        if s.get("filter"):
            f = s["filter"]
            what = ", ".join(([f"my PRs"] if f.get("mine") else []) + list(f.get("about") or []))
            print(f"    only about: {what}")
        if s["summary"]:
            print(f"    {s['summary']}")
        print(f"    polled {ago(s['last_poll'])}" + (f" — ERROR: {s['error']}" if s["error"] else ""))


def wait_for_events(args, emit, timeout: float | None = None, passive: bool = False, alive=None) -> dict:
    """Run one watch against the daemon (reconnecting if it restarts) until it answers. Events
    are handed to emit() and acked after it returns. Returns the daemon's answer; {"timeout": True}
    if the deadline passes, {"gone": True} if alive() turns false."""
    agent = agent_id(args)
    deadline = time.time() + timeout if timeout else None
    delay = 0.5
    while True:
        if alive and not alive():
            return {"ok": True, "events": [], "gone": True}
        remaining = None if deadline is None else max(1, deadline - time.time())
        try:
            client.ensure_daemon()
            s = client._connect(None)
        except (DaemonDown, OSError):
            if deadline and time.time() >= deadline:
                return {"ok": True, "events": [], "timeout": True, "unreachable": True}
            time.sleep(delay)
            delay = min(delay * 2, 10)
            client._checked = False
            continue
        try:
            req = agent_req(args, "watch", timeout=remaining, passive=passive)
            req["agent"] = agent
            client.send(s, req)
            while alive and not select.select([s], [], [], 5)[0]:
                if not alive():
                    s.close()
                    return {"ok": True, "events": [], "gone": True}
            resp = client.recv(s)
            if resp is None:  # daemon restarted underneath us; reconnect
                raise ConnectionError
        except (OSError, ValueError, ConnectionError):
            s.close()
            if deadline and time.time() >= deadline:
                return {"ok": True, "events": [], "timeout": True, "unreachable": True}
            client._checked = False
            time.sleep(delay)
            delay = min(delay * 2, 10)
            continue
        events = resp.get("events") or []
        if events:
            emit(events)
            try:
                client.send(s, {"ack": max(e["id"] for e in events)})
            except OSError:
                pass
        s.close()
        return resp


def cmd_watch(args):
    deadline = time.time() + args.timeout if args.timeout else None
    trailer = "" if args.follow else (NO_WATCH_NEEDED if hook_watch_installed() else WATCH_AGAIN)

    def emit(events):
        if args.json:
            for e in events:
                print(json.dumps(e))
        else:
            print(format_events(events))
            print(f"\n{trailer}" if trailer else "")
        sys.stdout.flush()

    while True:
        remaining = None if deadline is None else deadline - time.time()
        if remaining is not None and remaining <= 0:
            resp = {"ok": True, "timeout": True}
        else:
            resp = wait_for_events(args, emit, timeout=remaining)
        if resp.get("events") and args.follow:
            continue
        if resp.get("events"):
            return 0
        if not resp.get("ok"):
            sys.exit(f"agent-events: {resp.get('error')}")
        if resp.get("superseded"):
            print("A newer `agent-events watch` for this agent took over; this one exits. Nothing to do.")
        elif resp.get("no_subscriptions"):
            print("This agent has no subscriptions, so there is nothing to watch. "
                  "Subscribe with `agent-events subscribe <topic>` first.")
        elif resp.get("timeout"):
            unreachable = " (daemon unreachable)" if resp.get("unreachable") else ""
            print(f"No {'more ' if args.follow else ''}events within {args.timeout:g}s{unreachable}.")
        sys.stdout.flush()
        return 0


def cmd_log(args):
    resp = call(agent_req(args, "log", topic=args.topic, limit=args.limit))
    if args.json:
        for e in resp["events"]:
            print(json.dumps(e))
    elif resp["events"]:
        print(format_events(resp["events"]).split("\n", 1)[1].lstrip("\n"))
    else:
        print("No events yet.")


def cmd_refresh(args):
    resp = call(agent_req(args, "refresh"))
    print(f"Polling now: {', '.join(resp['keys']) or 'nothing'}")


def cmd_publish(args):
    body = sys.stdin.read() if args.body == "-" else (args.body or "")
    data = {}
    if args.data:
        try:
            data = json.loads(args.data)
        except ValueError as e:
            sys.exit(f"agent-events: --data is not valid JSON: {e}")
        if not isinstance(data, dict):
            sys.exit("agent-events: --data must be a JSON object")
    req = {"op": "publish", "topic": args.topic, "summary": args.summary, "kind": args.kind, "body": body,
           "actor": args.actor or "", "url": args.url or "", "data": data}
    if args.id:
        req["id"] = args.id
    if args.quiet:
        req["quiet"] = True
    resp = call(req)
    if args.json:
        print(json.dumps(resp, indent=2))
        return
    counts = f"(subscribers: {len(resp['subscribers'])}, watching: {len(resp['watching'])})"
    if resp["duplicate"]:
        print(f"✓ already published #{resp['id']} to {resp['topic']} (same --id; not delivered again) {counts}")
    else:
        print(f"✓ published #{resp['id']} to {resp['topic']} {counts}")


def cmd_types(args):
    from .sources import SOURCES
    for s in SOURCES.values():
        how = "push: published into, not polled" if s.push else \
            f"polled every {config.poll_interval(s.type, s.interval):g}s"
        print(f"{s.type}  ({how})\n    {s.help}")
        for ex in s.examples:
            print(f"      {ex}")
        print()
    print("Subscribing to a topic also delivers events on topics nested under it.")


def cmd_daemon(args):
    if args.foreground:
        from . import daemon
        return daemon.run(wait_lock=args.wait_lock)
    info = client.ping()
    if info and not args.replace:
        print(f"agent-events daemon already running (pid {info['pid']}, up {ago(info['started']).replace(' ago', '')}). "
              f"Use --replace to restart it.")
        return 0
    if info or client.lock_held():
        client.stop_daemon()
        print("Stopped the running daemon.")
    if args.background:
        info = client.start_background()
        print(f"agent-events daemon started in the background (pid {info['pid']}), log: {config.log_path()}")
        return 0
    from . import daemon
    return daemon.run()


def cmd_stop(args):
    print("Daemon stopped." if client.stop_daemon() else "Daemon was not running.")
    print("Note: running `agent-events watch` processes and the next command will start it again.")
    if (Path.home() / "Library/LaunchAgents" / f"{LAUNCHD_LABEL}.plist").exists():
        print("Note: launchd will start it again; `agent-events uninstall --launchd` to stop that.")


def cmd_status(args):
    info = client.ping()
    if not info:
        print("Daemon: not running" + (" (starting up)" if client.lock_held() else ""))
        return 1
    resp = client.raw_request({"op": "status"})
    if args.json:
        print(json.dumps(resp, indent=2))
        return 0
    stale = " — running older code, will restart on next command" if info["version"] + 0.001 < config.code_version() else ""
    print(f"Daemon: pid {resp['pid']}, up {ago(resp['started']).replace(' ago', '')}{stale}")
    print(f"State:  {config.home()}")
    print(f"\nAgents ({len(resp['agents'])}):")
    for a in resp["agents"]:
        flags = ", ".join(x for x in ("watching" if a["watching"] else "", "active" if a["alive"] else "idle") if x)
        print(f"  {a['id']}  [{flags}] seen {ago(a['last_seen'])}  {a['cwd'] or ''}")
        for t in a["subscriptions"]:
            print(f"      {t}")
    print(f"\nPolled things ({len(resp['targets'])}):")
    active = set(resp["active_keys"])
    for t in sorted(resp["targets"], key=lambda t: t["key"]):
        state = "" if t["key"] in active else " [paused: only retired sessions subscribe]"
        err = f"\n      ERROR: {t['error']}" if t["error"] else ""
        print(f"  {t['key']}  polled {ago(t['last_poll'])}{state}\n      {t['summary'] or ''}{err}")
    return 0


# --- Claude Code hook --------------------------------------------------------------------

def hook_message(subs: list[str], daemon_down: bool) -> str:
    shown = ", ".join(subs[:5]) + (f" and {len(subs) - 5} more" if len(subs) > 5 else "")
    return (f"agent-events: you are subscribed to {len(subs)} topic(s) ({shown}) but no `agent-events watch` is "
            f"running{' (the daemon is down; watch will start it)' if daemon_down else ''}, so you would miss "
            f"updates. Start it now with the Bash tool using run_in_background: `agent-events watch`. "
            f"If some subscriptions are no longer relevant, `agent-events unsubscribe <topic>` them first. "
            f"(To never have to do this: `agent-events install` sets up hooks that watch for you.)")


def hook_watch_installed() -> bool:
    """True if Claude Code runs `agent-events hook --watch` as an asyncRewake hook, i.e. agents
    are woken with their events without keeping a watch running themselves."""
    try:
        settings = json.loads(_settings_path().read_text())
    except (OSError, ValueError):
        return False
    for groups in (settings.get("hooks") or {}).values():
        for g in groups if isinstance(groups, list) else []:
            for h in g.get("hooks") or []:
                if h.get("asyncRewake") and _is_our_hook(h.get("command") or "", watch=True):
                    return True
    return False


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def claude_pid() -> int | None:
    """PID of the Claude Code process running this hook (the nearest `claude` ancestor)."""
    if os.environ.get("CLAUDE_PID", "").isdigit():
        return int(os.environ["CLAUDE_PID"])
    pid = os.getppid()
    for _ in range(6):
        try:
            out = subprocess.run(["ps", "-o", "ppid=,comm=", "-p", str(pid)], capture_output=True, text=True,
                                 timeout=2).stdout.split(None, 1)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if len(out) < 2:
            return None
        if os.path.basename(out[1].strip()) == "claude":
            return pid
        pid = int(out[0])
    return None


def cmd_hook(args):
    """Claude Code hook. Without --watch (SessionStart / UserPromptSubmit / Stop / SessionEnd):
    injects pending events into the conversation, or, when no hook watch is installed, nags agents
    that have subscriptions but no watch running. With --watch it is an asyncRewake hook: it waits
    for events in the background and exits 2 with them, which wakes the agent with the events."""
    try:
        data = json.load(sys.stdin)
    except ValueError:
        data = {}
    agent = data.get("session_id") or os.environ.get("CLAUDE_CODE_SESSION_ID")
    event = data.get("hook_event_name") or args.event or ""
    if not agent:
        return 0
    args.agent = agent
    if args.watch:
        return hook_watch(args, agent)
    if event == "SessionEnd":
        try:
            client.raw_request({"op": "unwatch", "agent": agent}, timeout=3)
        except (DaemonDown, OSError, ValueError):
            pass
        return 0
    daemon_down = False
    if event == "SessionStart":
        # After /clear (or a resume) the same Claude process continues as a different session
        # id; tell the daemon so the previous session's subscriptions move to this one.
        try:
            client.raw_request({"op": "session_start", "agent": agent, "pid": claude_pid()}, timeout=3)
        except (DaemonDown, OSError, ValueError):
            pass
    events: list[dict] = []
    installed = hook_watch_installed()
    # Context added at SessionStart starts no turn (e.g. `claude --resume` without a prompt), so
    # events shown there must stay unread: with the hook watch installed, leave them to it (it wakes
    # the agent with them) or to the first prompt's hook; without it, show them but don't ack.
    peek = event == "SessionStart"
    try:
        resp = client.raw_request({"op": "agent_status", "agent": agent}, timeout=3)
        subs, watching = resp.get("subscriptions") or [], resp.get("watching")
        if subs and not (peek and installed):
            # At Stop, handing events over continues the turn: FYI events alone wait for the next.
            events = client.raw_request({"op": "take", "agent": agent, "ack": not peek,
                                         "wake_only": event == "Stop"}, timeout=3).get("events") or []
    except (DaemonDown, OSError, ValueError):
        daemon_down, watching = True, False
        try:
            from .store import Store
            subs = [r["topic"] for r in Store(config.db_path(), readonly=True).subs(agent)]
        except Exception:
            subs = []
    msg = ""
    if events:
        msg = (delivery_text(events) +
               ("\n\n(Shown at session start and still unread: a watch will deliver them again.)" if peek else ""))
    if subs and not watching and not installed:
        msg = (msg + "\n\n" if msg else "") + hook_message(subs, daemon_down)
    if not msg:
        return 0
    if event == "Stop":
        if data.get("stop_hook_active") and not events:
            return 0  # we already blocked once in this chain; don't loop
        print(json.dumps({"decision": "block", "reason": msg}))
    else:
        print(json.dumps({"hookSpecificOutput": {"hookEventName": event or "UserPromptSubmit",
                                                 "additionalContext": msg}}))
    return 0


def hook_watch(args, agent: str) -> int:
    """`agent-events hook --watch`: wait (passively: an explicit `agent-events watch` has priority)
    until the agent has events, then print them to stderr and exit 2 so Claude Code wakes the agent
    with them. Exits 0 silently when superseded by a newer hook watch, when the session ends, or
    when there is nothing to watch."""
    if os.environ.get("CLAUDE_CODE_ENTRYPOINT", "").startswith("sdk"):
        return 0  # headless (claude -p / SDK): a pending async hook would keep the run from ending
    try:
        if not client.raw_request({"op": "agent_status", "agent": agent}, timeout=5).get("subscriptions"):
            return 0
    except (DaemonDown, OSError, ValueError):
        pass  # wait_for_events starts the daemon
    pid = claude_pid()
    delivered: list[str] = []
    # Claude Code shows the agent our stderr; keep incidental client messages (e.g. "restarting
    # the daemon") out of it.
    out, sys.stderr = sys.stderr, open(os.devnull, "w")

    def emit(events):
        delivered.append(delivery_text(events))
        out.write(delivered[-1] + "\n")
        out.flush()

    try:
        wait_for_events(args, emit, timeout=HOOK_WATCH_TIMEOUT - 60, passive=True,
                        alive=(lambda: pid_alive(pid)) if pid else None)
    finally:
        sys.stderr.close()
        sys.stderr = out
    return 2 if delivered else 0


# --- install -----------------------------------------------------------------------------

LAUNCHD_LABEL = client.LAUNCHD_LABEL
# event -> (sync `hook`, asyncRewake `hook --watch`)
HOOKS = {"SessionStart": (True, True), "UserPromptSubmit": (True, True), "Stop": (False, True),
         "SessionEnd": (True, False)}
HOOK_EVENTS = tuple(HOOKS)


def _settings_path() -> Path:
    return Path.home() / ".claude" / "settings.json"


def _hook_command(watch: bool = False) -> str:
    return f"{shlex.quote(str(config.BIN))} hook" + (" --watch" if watch else "")


def _is_our_hook(command: str, watch: bool | None = None) -> bool:
    if "agent-events" not in command:
        return False
    if watch is None:
        return command.endswith(" hook") or command.endswith(" hook --watch")
    return command.endswith(" hook --watch" if watch else " hook")


def install_hooks(remove: bool = False) -> None:
    path = _settings_path()
    settings = json.loads(path.read_text()) if path.exists() else {}
    hooks = settings.setdefault("hooks", {})
    for event in set(HOOK_EVENTS) | set(hooks):
        groups = [g for g in hooks.get(event, [])
                  if not any(_is_our_hook(h.get("command", "")) for h in g.get("hooks", []))]
        sync, watch = HOOKS.get(event, (False, False))
        if not remove and sync:
            groups.append({"hooks": [{"type": "command", "command": _hook_command(), "timeout": 10}]})
        if not remove and watch:
            groups.append({"hooks": [{"type": "command", "command": _hook_command(watch=True), "asyncRewake": True,
                                      "timeout": HOOK_WATCH_TIMEOUT}]})
        if groups:
            hooks[event] = groups
        else:
            hooks.pop(event, None)
    if not hooks:
        settings.pop("hooks")
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(settings, indent=2) + "\n")
    tmp.replace(path)


def _symlink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink() or link.exists():
        if link.is_symlink() and Path(os.readlink(link)) == target:
            return
        if link.is_dir() and not link.is_symlink():
            sys.exit(f"agent-events: {link} exists and is not a symlink; move it away first")
        link.unlink()
    link.symlink_to(target)


def install_launchd(remove: bool = False) -> None:
    plist = Path.home() / "Library/LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", f"{domain}/{LAUNCHD_LABEL}"], capture_output=True)
    if remove:
        plist.unlink(missing_ok=True)
        return
    path = ":".join(dict.fromkeys([str(Path.home() / ".local/bin"), "/opt/homebrew/bin", "/usr/local/bin",
                                   "/usr/bin", "/bin", "/usr/sbin", "/sbin"]))
    plist.parent.mkdir(parents=True, exist_ok=True)
    with open(plist, "wb") as f:
        plistlib.dump({
            "Label": LAUNCHD_LABEL,
            # bin/agent-events picks its python3 at each start (never the Xcode shim).
            "ProgramArguments": [str(config.BIN), "daemon", "--foreground", "--wait-lock"],
            "RunAtLoad": True,
            "KeepAlive": True,
            "ThrottleInterval": 10,
            "EnvironmentVariables": {"PATH": path, "HOME": str(Path.home())},
            "StandardOutPath": str(config.log_path()),
            "StandardErrorPath": str(config.log_path()),
        }, f)
    config.home().mkdir(parents=True, exist_ok=True)
    # bootout returns before the job is gone, and bootstrapping it meanwhile fails ("5: Input/output
    # error"), so retry for a few seconds.
    for _ in range(20):
        if subprocess.run(["launchctl", "bootstrap", domain, str(plist)], capture_output=True).returncode == 0:
            return
        time.sleep(0.5)
    print(f"agent-events: launchctl bootstrap {domain} {plist} failed; run it by hand", file=sys.stderr)


def cmd_install(args):
    bin_link = Path.home() / ".local/bin/agent-events"
    skill_link = Path.home() / ".claude/skills/events"
    _symlink(bin_link, config.BIN)
    print(f"✓ {bin_link} → {config.BIN}")
    _symlink(skill_link, config.REPO_DIR / "skills" / "events")
    print(f"✓ {skill_link} → skills/events (the /events skill)")
    if not args.no_hooks:
        install_hooks()
        print(f"✓ Claude Code hooks ({', '.join(HOOK_EVENTS)}) → `agent-events hook [--watch]` in {_settings_path()}")
    if not args.no_launchd:
        client.stop_daemon()
        install_launchd()
        print(f"✓ launchd agent {LAUNCHD_LABEL} keeps the daemon running")
    info = None
    for _ in range(50):
        info = client.ping()
        if info:
            break
        time.sleep(0.1)
    if not info:
        info = client.start_background()
    print(f"✓ daemon running (pid {info['pid']})")


def cmd_uninstall(args):
    everything = not (args.hooks or args.launchd)
    if everything or args.launchd:
        install_launchd(remove=True)
        print("✓ launchd agent removed")
    if everything or args.hooks:
        install_hooks(remove=True)
        print("✓ hooks removed")
    if everything:
        client.stop_daemon()
        for link in (Path.home() / ".local/bin/agent-events", Path.home() / ".claude/skills/events"):
            if link.is_symlink():
                link.unlink()
                print(f"✓ removed {link}")
        print(f"State kept in {config.home()} (delete it to forget all subscriptions).")


# --- entry point -------------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="agent-events", description=(
        "Subscribe agents to GitHub / Linear / Slack events. Subscriptions are kept by a daemon; "
        "Claude Code hooks (`agent-events install`) wake the agent with its events; without them the agent "
        "runs `agent-events watch` in the background, which exits when events arrive."))
    p.add_argument("--agent", help="agent id (default: $AGENT_EVENTS_AGENT or $CLAUDE_CODE_SESSION_ID)")
    sub = p.add_subparsers(dest="cmd", metavar="COMMAND")

    def add(name, fn, help, aliases=()):
        sp = sub.add_parser(name, help=help, aliases=list(aliases))
        sp.set_defaults(fn=fn)
        sp.add_argument("--agent", help=argparse.SUPPRESS, default=argparse.SUPPRESS)
        return sp

    sp = add("subscribe", cmd_subscribe, "subscribe this agent to topics or URLs", ["sub"])
    sp.add_argument("topics", nargs="+", metavar="TOPIC", help="e.g. github/owner/repo/123, linear/T-123, a URL")
    sp.add_argument("--about", action="append", metavar="TERMS",
                    help="only events mentioning one of these (comma-separated; whole words, any case), "
                         "e.g. a busy channel: --about 'app#123,welcome email'. A thread counts when its "
                         "first message does. Subscribing again without it delivers everything again")
    sp.add_argument("--mine", action="store_true",
                    help="only events mentioning the PRs this agent is subscribed to (repo#N, pull/N, #N)")
    sp = add("unsubscribe", cmd_unsubscribe, "remove subscriptions (nested ones too)", ["unsub"])
    sp.add_argument("topics", nargs="*", metavar="TOPIC")
    sp.add_argument("--all", action="store_true", help="remove every subscription of this agent")
    sp = add("list", cmd_list, "show this agent's subscriptions", ["ls"])
    sp.add_argument("--json", action="store_true")
    sp = add("watch", cmd_watch, "block until events arrive for this agent, print them, exit")
    sp.add_argument("--follow", "-f", action="store_true",
                    help="keep running and print each batch of events as it arrives (e.g. under Monitor)")
    sp.add_argument("--timeout", type=float, help="give up after this many seconds (default: wait forever)")
    sp.add_argument("--json", action="store_true", help="print events as JSON lines")
    sp = add("log", cmd_log, "show recent events (already delivered or not)")
    sp.add_argument("topic", nargs="?")
    sp.add_argument("-n", "--limit", type=int, default=20)
    sp.add_argument("--json", action="store_true")
    sp = add("publish", cmd_publish, "publish an event into a push topic (e.g. a message to hf/agent/<id>)")
    sp.add_argument("topic", metavar="TOPIC", help="a push topic, e.g. hf/agent/<id>")
    sp.add_argument("summary", metavar="SUMMARY", help="one-line summary shown to subscribers")
    sp.add_argument("--kind", default="message", help="event kind (default: message)")
    sp.add_argument("--body", help="full text; '-' reads it from stdin")
    sp.add_argument("--actor", help="who sent it (shown to subscribers)")
    sp.add_argument("--url")
    sp.add_argument("--data", help="extra JSON object stored with the event")
    sp.add_argument("--id", help="idempotency key: publishing the same id to a topic again is a no-op")
    sp.add_argument("--quiet", action="store_true",
                    help="FYI: never wakes the subscriber; handed over with its next events or at its next turn")
    sp.add_argument("--json", action="store_true", help="print the daemon's response as JSON")
    add("refresh", cmd_refresh, "poll this agent's topics right now")
    add("types", cmd_types, "list event types and their topic formats")
    sp = add("daemon", cmd_daemon, "run the daemon (says so if one is already running)")
    sp.add_argument("--background", "-b", action="store_true", help="detach and run in the background")
    sp.add_argument("--replace", "-r", action="store_true", help="stop a running daemon and start a new one")
    sp.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    sp.add_argument("--wait-lock", action="store_true", help=argparse.SUPPRESS)
    add("stop", cmd_stop, "stop the daemon")
    sp = add("status", cmd_status, "daemon status: agents, subscriptions, polling errors")
    sp.add_argument("--json", action="store_true")
    sp = add("hook", cmd_hook, "Claude Code hook: deliver events into the conversation (see README)")
    sp.add_argument("--event", help=argparse.SUPPRESS)
    sp.add_argument("--watch", action="store_true", help="asyncRewake hook: wait for events, exit 2 with them")
    sp = add("install", cmd_install, "symlink CLI + /events skill, add Claude Code hooks, set up launchd")
    sp.add_argument("--no-hooks", action="store_true")
    sp.add_argument("--no-launchd", action="store_true")
    sp = add("uninstall", cmd_uninstall, "undo install (state in ~/.agent-events is kept)")
    sp.add_argument("--hooks", action="store_true", help="only remove the hooks")
    sp.add_argument("--launchd", action="store_true", help="only remove the launchd agent")

    args = p.parse_args(argv)
    if not getattr(args, "fn", None):
        p.print_help()
        return 0
    try:
        return args.fn(args) or 0
    except KeyboardInterrupt:
        return 130
