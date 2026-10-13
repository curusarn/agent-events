# agent-events — dev notes

- Pure stdlib Python, must run on macOS system `python3` (3.9): `from __future__ import annotations`, no `match`, no deps.
- Tests: `PYTHONPATH=.:tests python3 -m unittest discover -s tests` (spins up in-process daemons with a fake source; no network).
- Source diff logic is tested with synthetic snapshots in `tests/test_sources.py`; when changing a source, also sanity-check it live against a real PR/ticket/thread.
- The running daemon (launchd, `~/.agent-events`) restarts itself onto new code on the next CLI call, since clients compare source mtimes. You can also run `agent-events daemon -b --replace`. On a Mac with the launchd job, a code change restarts it through `launchctl kickstart -k`, so the daemon never takes a client's (an agent's) environment.
- For experiments use an isolated state dir: `AGENT_EVENTS_HOME=$(mktemp -d /tmp/ae.XXXX) AGENT_EVENTS_AGENT=test bin/agent-events ...` (keep it short: unix socket paths max out at 104 bytes).
- Never post to Slack/Linear to test; mutate the stored snapshot in `targets.state` and `refresh` instead.
