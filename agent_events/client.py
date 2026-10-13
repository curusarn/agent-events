"""Talking to the daemon: connect, auto-start, restart on code change, stop."""
from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import time

from . import config


class DaemonDown(Exception):
    pass


def _connect(timeout: float | None) -> socket.socket:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
        s.connect(str(config.socket_path()))
    except (FileNotFoundError, ConnectionRefusedError, socket.timeout) as e:
        s.close()
        raise DaemonDown(str(e)) from None
    s.settimeout(timeout)
    return s


def send(s: socket.socket, obj: dict) -> None:
    s.sendall(json.dumps(obj).encode() + b"\n")


def recv(s: socket.socket) -> dict | None:
    buf = b""
    while not buf.endswith(b"\n"):
        chunk = s.recv(65536)
        if not chunk:
            return None
        buf += chunk
    return json.loads(buf)


def raw_request(req: dict, timeout: float | None = 120) -> dict:
    s = _connect(timeout)
    try:
        send(s, req)
        resp = recv(s)
    finally:
        s.close()
    if resp is None:
        raise DaemonDown("daemon closed the connection")
    return resp


def ping() -> dict | None:
    try:
        return raw_request({"op": "ping"}, timeout=5)
    except (DaemonDown, OSError, ValueError):
        return None


def daemon_pid() -> int | None:
    try:
        return int(config.lock_path().read_text().strip() or 0) or None
    except (FileNotFoundError, ValueError):
        return None


def lock_held() -> bool:
    try:
        with open(config.lock_path(), "a") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(f, fcntl.LOCK_UN)
            return False
    except FileNotFoundError:
        return False


def start_background() -> dict:
    """Spawn a detached daemon and wait until it answers."""
    config.home().mkdir(parents=True, exist_ok=True)
    log = config.log_path()
    if log.exists() and log.stat().st_size > 5_000_000:
        log.replace(log.with_suffix(".log.1"))
    with open(log, "a") as out:
        subprocess.Popen([sys.executable, str(config.BIN), "daemon", "--foreground"], stdin=subprocess.DEVNULL,
                         stdout=out, stderr=out, start_new_session=True, close_fds=True, env=config.daemon_env())
    deadline = time.time() + 15
    while time.time() < deadline:
        info = ping()
        if info:
            return info
        time.sleep(0.1)
    raise DaemonDown(f"daemon did not come up; see {log}")


LAUNCHD_LABEL = "com.curusarn.agent-events"


def launchd_job() -> str | None:
    """The launchd job that keeps the daemon running (macOS, after `install`), if any."""
    if sys.platform != "darwin" or os.environ.get("AGENT_EVENTS_HOME") or os.environ.get("AGENT_EVENTS_SOCKET"):
        return None  # not launchd's daemon (tests, experiments with a state dir of their own)
    plist = os.path.expanduser(f"~/Library/LaunchAgents/{LAUNCHD_LABEL}.plist")
    return f"gui/{os.getuid()}/{LAUNCHD_LABEL}" if os.path.exists(plist) else None


def restart_daemon(only_pid: int | None = None) -> None:
    """Onto new code: through launchd where it runs the daemon (its clean environment,
    not this client's), else stop it here and let the caller start a new one."""
    job = launchd_job()
    if job and (not only_pid or (ping() or {}).get("pid") == only_pid):
        # Restarts launchd's process (the daemon, or its standby) on the new code.
        subprocess.run(["launchctl", "kickstart", "-k", job], capture_output=True, timeout=15)
    stop_daemon(only_pid=only_pid)


def stop_daemon(timeout: float = 10, only_pid: int | None = None) -> bool:
    """Stop the running daemon. Returns False if none was running.

    With only_pid, do nothing unless that exact daemon is still the one running (several
    clients may notice stale code at once; only the first should restart it).
    """
    pid = daemon_pid()
    info = ping()
    if only_pid and (not info or info.get("pid") != only_pid):
        return False
    if not info and not lock_held():
        return False
    try:
        raw_request({"op": "shutdown"}, timeout=5)
    except (DaemonDown, OSError, ValueError):
        pass
    pid = (info or {}).get("pid") or pid

    def gone() -> bool:
        # A launchd standby grabs the lock as soon as it's free, so watch the old process
        # itself rather than the lock.
        if pid:
            try:
                os.kill(pid, 0)
                return False
            except ProcessLookupError:
                return True
            except PermissionError:
                return False
        return not lock_held()

    deadline = time.time() + timeout
    while time.time() < deadline and not gone():
        time.sleep(0.05)
    if not gone() and pid:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                break
            end = time.time() + 5
            while time.time() < end and not gone():
                time.sleep(0.05)
            if gone():
                break
    return True


_checked = False


def ensure_daemon(autostart: bool = True) -> None:
    """Make sure a daemon runs current code, starting or restarting it as needed."""
    global _checked
    if _checked:
        return
    info = ping()
    if info and info.get("version", 0) + 0.001 < config.code_version():
        print("agent-events: daemon is running older code, restarting it", file=sys.stderr)
        restart_daemon(only_pid=info.get("pid"))
        info = ping()
        if info and info.get("version", 0) + 0.001 < config.code_version():
            info = None  # still the old one (shutting down); wait for its successor below
    if not info:
        if not autostart:
            raise DaemonDown("daemon is not running")
        if lock_held() or launchd_job():  # starting up right now (e.g. launchd, another client) - give it a moment
            for _ in range(150 if launchd_job() else 50):
                if ping():
                    break
                time.sleep(0.1)
        if not ping():
            start_background()
    _checked = True


def request(req: dict, timeout: float | None = 120, autostart: bool = True) -> dict:
    ensure_daemon(autostart)
    try:
        return raw_request(req, timeout)
    except DaemonDown:
        if not autostart:
            raise
        global _checked
        _checked = False
        ensure_daemon(True)
        return raw_request(req, timeout)
