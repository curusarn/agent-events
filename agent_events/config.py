"""Paths, credentials and tunables. Everything lives under ~/.agent-events unless overridden."""
from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_DIR = PACKAGE_DIR.parent
BIN = REPO_DIR / "bin" / "agent-events"


def home() -> Path:
    return Path(os.environ.get("AGENT_EVENTS_HOME") or "~/.agent-events").expanduser()


def socket_path() -> Path:
    # macOS caps unix socket paths at 104 bytes; the override exists for long temp dirs.
    return Path(os.environ.get("AGENT_EVENTS_SOCKET") or home() / "daemon.sock")


def db_path() -> Path:
    return home() / "events.db"


def lock_path() -> Path:
    return home() / "daemon.lock"


def log_path() -> Path:
    return home() / "daemon.log"


def env_path() -> Path:
    return home() / "env"


def code_version() -> float:
    """Newest mtime of the package sources; lets clients notice a daemon running stale code."""
    return max(p.stat().st_mtime for p in PACKAGE_DIR.rglob("*.py"))


# The version of the code actually loaded in this process.
LOADED_VERSION = code_version()


# --- tunables (seconds) ---------------------------------------------------------------

def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def poll_interval(source_type: str, default: float) -> float:
    return _float_env(f"AGENT_EVENTS_POLL_{source_type.upper()}", default)


# An agent counts as alive (its topics get polled) while it has a watch connected, its
# Claude Code process is running, or it talked to the daemon within this window.
AGENT_IDLE = _float_env("AGENT_EVENTS_AGENT_IDLE", 3600)
# Subscriptions of agents that are gone for this long are deleted. As long as events are kept:
# an agent stopped for a week and then resumed still has them (7 days dropped those).
AGENT_TTL = _float_env("AGENT_EVENTS_AGENT_TTL", 30 * 86400)
EVENT_TTL = _float_env("AGENT_EVENTS_EVENT_TTL", 30 * 86400)
# After the first event shows up, a watch waits this long to batch a burst together.
WATCH_SETTLE = _float_env("AGENT_EVENTS_WATCH_SETTLE", 1.5)
# ...unless every pending event is on a push topic (a message published directly): deliver
# those almost immediately, just batching messages published back to back.
PUSH_SETTLE = _float_env("AGENT_EVENTS_PUSH_SETTLE", 0.05)


# --- credentials ------------------------------------------------------------------------

_env_mtime: float | None = None


def load_env_file() -> None:
    """Load KEY=VALUE lines from ~/.agent-events/env. Re-reads only when the file changed."""
    global _env_mtime
    try:
        mtime = env_path().stat().st_mtime
    except FileNotFoundError:
        return
    if mtime == _env_mtime:
        return
    _env_mtime = mtime
    _secret_failed.clear()  # the refs may have been fixed
    for line in env_path().read_text().splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[len("export "):]
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip().strip("'\"")


_secret_cache: dict[str, str] = {}
# The daemon serves every agent, so it must not carry the identity of the agent
# whose command happened to start it: Hyperfocus's op proxy refuses a process
# that claims an agent it doesn't belong to, and Slack and Linear polling broke
# for every agent when a code change restarted the daemon from an `hf post` (2026-10-09).
AGENT_ENV = ("HF_SESSION_ID", "HF_SESSION_NAME", "MC_SESSION_ID", "MC_SESSION_NAME", "CLAUDE_CODE_SESSION_ID",
             "CLAUDECODE", "CLAUDE_PID", "CLAUDE_CODE_ENTRYPOINT", "AGENT_EVENTS_AGENT", "TMUX", "TMUX_PANE")


def daemon_env() -> dict[str, str]:
    """This process's environment without any agent's identity."""
    return {k: v for k, v in os.environ.items() if k not in AGENT_ENV}


_secret_failed: dict[str, float] = {}  # op ref -> when `op read` last failed
# Don't retry a failing `op read` sooner than this: it may prompt (Touch ID) every time.
SECRET_RETRY = _float_env("AGENT_EVENTS_SECRET_RETRY", 600)
# A host's read refused because the Mac is asleep or offline showed no prompt: try again soon, so
# polls come back within seconds of the Mac waking rather than up to SECRET_RETRY later (hyperfocus-1
# after its restart while the Mac slept, 2026-10-30).
UNREACHABLE_RETRY = _float_env("AGENT_EVENTS_UNREACHABLE_RETRY", 30)
UNREACHABLE = "Hyperfocus on the Mac is not reachable"
_secret_unreachable: set[str] = set()  # op refs whose last read failed only for that
# One `op read` per ref at a time: polls of one source run in parallel, and each read can prompt.
_secret_locks: dict[str, threading.Lock] = {}
_secret_locks_guard = threading.Lock()


def op_binary() -> str:
    """The `op` to read secrets with: AGENT_EVENTS_OP, else Hyperfocus's op broker when the app
    is installed (~/.hyperfocus/bin/op: the app reads 1Password once, as Hyperfocus, and keeps
    allowed values in memory, so the daemon never triggers its own Touch ID prompts; with the app
    not running it hands over to the real op), else `op` on PATH."""
    if os.environ.get("AGENT_EVENTS_OP"):
        return os.environ["AGENT_EVENTS_OP"]
    broker = Path(os.environ.get("HF_HOME") or "~/.hyperfocus").expanduser() / "bin" / "op"
    return str(broker) if os.access(broker, os.X_OK) else "op"


def secret(env_names: list[str], op_ref_env: str, default_op_ref: str | None) -> str | None:
    """Credential lookup: env vars first, then `op read` (the op shim caches it on disk).

    The op ref can be overridden (or disabled with an empty value) via `op_ref_env`.
    """
    for name in env_names:
        if os.environ.get(name):
            return os.environ[name]
    ref = os.environ.get(op_ref_env, default_op_ref)
    if not ref:
        return None
    with _secret_locks_guard:
        lock = _secret_locks.setdefault(ref, threading.Lock())
    with lock:
        return _read_secret(ref)


def _read_secret(ref: str) -> str | None:
    if ref in _secret_cache:
        return _secret_cache[ref]
    wait = UNREACHABLE_RETRY if ref in _secret_unreachable else SECRET_RETRY
    if time.time() - _secret_failed.get(ref, 0) < wait:
        return None
    try:
        # Hyperfocus's op waits up to 330 s for the operator's Approve: wait it out, or a
        # first read Approved after 2 minutes still counts as failed for SECRET_RETRY.
        out = subprocess.run([op_binary(), "read", ref], capture_output=True, text=True, timeout=340)
        value = out.stdout.strip() if out.returncode == 0 else ""
        unreachable = not value and UNREACHABLE in (out.stderr or "")
    except (OSError, subprocess.TimeoutExpired):
        value, unreachable = "", False
    if not value:
        _secret_failed[ref] = time.time()
        if unreachable:
            _secret_unreachable.add(ref)
        else:
            _secret_unreachable.discard(ref)
        return None
    _secret_cache[ref] = value
    return value


def forget_secret(op_ref_env: str, default_op_ref: str | None) -> None:
    """Drop a cached secret, e.g. after the API rejected it."""
    _secret_cache.pop(os.environ.get(op_ref_env, default_op_ref) or "", None)
