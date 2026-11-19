"""A lock whose waits never sleep forever.

On 18 Nov 2026 the Mac's daemon stopped polling for 16 hours: its poller, two poll workers and 22
requests sat in an endless wait for the store's RLock while other threads kept taking and releasing
that same lock: their wakeups never came (a native thread sample of the stuck process showed it).
A wait here is a timed wait, retried: a waiter whose wakeup is lost looks again within RETRY seconds.
"""
from __future__ import annotations

import threading
import time

RETRY = 1.0  # seconds a waiter sleeps before it looks at the lock again


class TimedRLock:
    """Reentrant like threading.RLock, usable as a Condition's lock, and every wait is timed."""

    def __init__(self):
        self._lock = threading.Lock()
        self._owner: int | None = None
        self._count = 0

    def _take(self, deadline: float | None) -> bool:
        while True:
            wait = RETRY if deadline is None else min(RETRY, deadline - time.monotonic())
            if wait <= 0:
                return self._lock.acquire(False)
            if self._lock.acquire(True, wait):
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        me = threading.get_ident()
        if self._owner == me:
            self._count += 1
            return True
        if not blocking:
            ok = self._lock.acquire(False)
        else:
            ok = self._take(None if timeout is None or timeout < 0 else time.monotonic() + timeout)
        if ok:
            self._owner, self._count = me, 1
        return ok

    def release(self) -> None:
        if self._owner != threading.get_ident():
            raise RuntimeError("cannot release un-acquired lock")
        self._count -= 1
        if not self._count:
            self._owner = None
            self._lock.release()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()

    # What threading.Condition uses, so a Condition built on this lock waits the same way.
    def _is_owned(self) -> bool:
        return self._owner == threading.get_ident()

    def _release_save(self):
        state = (self._owner, self._count)
        self._owner, self._count = None, 0
        self._lock.release()
        return state

    def _acquire_restore(self, state) -> None:
        self._take(None)
        self._owner, self._count = state
