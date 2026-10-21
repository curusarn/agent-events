# agent-events

Agents are bad at watching things. You tell them to keep an eye on a PR, a Linear ticket or a Slack
thread, and a few turns later they've forgotten, because checking is expensive and nothing reminds them.

`agent-events` turns that around. A small local daemon does the watching, and it doesn't need an LLM.
An agent subscribes to a topic **once**. The daemon keeps the subscription, polls the source and
queues the changes. Claude Code hooks installed by `agent-events install` wake the agent with new
events, so the agent never has to run a watch itself. Without the hooks (or outside Claude Code), the
agent keeps one background command running, `agent-events watch`, which exits with the events.

```
agent ──subscribe github/o/r/12──▶ ┌──────────────────────────┐ ◀── polls ── GitHub (gh api graphql)
agent ──subscribe linear/T-123───▶ │  agent-events daemon     │ ◀── polls ── Linear GraphQL
hook ───watch (async hook)───────▶ │  SQLite: subs, snapshots,│ ◀── polls ── Slack Web API
agent ◀── woken with new events ── │  events, cursors         │
                                   └──────────────────────────┘
```

## Install

Needs the macOS system `python3` (3.9+; stdlib only) and `gh` logged in. Linear and Slack need a
token, either in `~/.agent-events/env` or read from 1Password with `op read` (see below).

```bash
git clone git@github.com:curusarn/agent-events.git ~/git/agent-events
~/git/agent-events/bin/agent-events install
```

`install` does four things:

- symlinks `~/.local/bin/agent-events`
- symlinks the `/events` skill into `~/.claude/skills/events`
- adds `SessionStart`, `UserPromptSubmit`, `Stop` and `SessionEnd` hooks to `~/.claude/settings.json`
  (see [Claude Code hooks](#claude-code-hooks))
- loads a launchd agent (`com.curusarn.agent-events`) that keeps the daemon running

`agent-events uninstall` undoes all of it; `--hooks` or `--launchd` undoes just that part.

## Usage

```bash
agent-events subscribe https://github.com/owner/repo/pull/12   # or github/owner/repo/12
agent-events subscribe linear/T-123 linear/T-123/5c3bf24b      # ticket; one comment thread
agent-events subscribe 'https://x.slack.com/archives/C0123/p1712345678123456'   # thread
agent-events subscribe slack/C0123 --mine --about 'welcome email'     # a busy channel: only my PRs, or these words
agent-events list                  # this agent's subscriptions, poll status, is a watch running
agent-events watch                 # block until events arrive, print them, exit (not needed with the hooks)
agent-events watch --follow        # keep running, print each batch as it arrives (e.g. under Monitor)
agent-events log [topic]           # recent events, delivered or not
agent-events unsubscribe <topic>   # also removes nested topics; --all for everything
agent-events refresh               # poll my topics now
agent-events types                 # supported event types and topic formats
agent-events publish hf/agent/<id> "summary" [--body TEXT|-] [--actor NAME] [--id KEY] [--quiet]   # push topics only

agent-events daemon                # run in foreground, or say that one is already running
agent-events daemon -b             # same, detached;  --replace restarts a running one
agent-events status | stop
```

The agent id defaults to `$CLAUDE_CODE_SESSION_ID`, which Claude Code sets. Outside Claude Code,
pass `--agent <id>` or set `AGENT_EVENTS_AGENT`.

## Topics

A topic is a readable path. The first segment is the event type, and the type decides what the
remaining segments mean. A subscription receives events on its own topic and on every topic nested
under it.

| Topic | Poll | Events |
|---|---|---|
| `github/<owner>/<repo>/<pr>` | 60s | comments ("via Linear" when posted through Linear's review UI), review comments, reviews, review requests, failed checks + CI result, pushes / force-pushes, merged / closed / reopened, draft / ready, conflicts, labels, assignees, title / description, threads resolved |
| `linear/<ISSUE>` | 60s | status, assignee, priority, project, cycle, labels, estimate, due, parent, title, description, comments (published on `linear/<ISSUE>/<thread>`) |
| `linear/<ISSUE>/<thread>` | (shares the ticket's poll) | comments in one thread; `<thread>` = first 8 hex chars of the top-level comment id, as in `#comment-xxxxxxxx` links |
| `slack/<channel>/<thread-ts>` | 30s | replies, edits, deletions, reactions |
| `slack/<channel>` | 30s | new messages, replies in recently active threads (published on the thread topic) |
| `docs/<source_ref>` | 60s | a staged Better Stack docs batch: per article published (and by whom) / discarded / restaged / staged / revised in place (and by which app) / promoted to ready for review / gone stale (published on `docs/<source_ref>/<canonical_id>`), plus one `batch` event when nothing in it is left unpublished; `/` in a ref or canonical_id is written `~` |
| `docs/<source_ref>/<canonical_id>` | (shares the batch's poll) | one article's version in the batch |
| `hf/agent/<id>`, `hf/thread/<id>` | push (not polled) | messages published by the Hyperfocus app (or `agent-events publish`); each carries its sender (`actor`) |

The first poll of anything is a silent baseline. After that, each poll is diffed against the stored
snapshot. Everything subscribed is polled, also for agents that aren't running (e.g. stopped in
Hyperfocus): their events wait for them, stamped with the source's own time (a comment's creation
time, not when it was seen), and the `unread` op (`{"op": "unread", "agents"?, "exclude"?, "since"?, "events"?}`) tells
an app which agents have something waiting. When a newer session of the same Claude process
replaces one (e.g. after /clear), the old session's subscriptions move to the new one: it is the
same agent continuing. After 30 days of inactivity an agent and its subscriptions are dropped.

Override poll intervals with `AGENT_EVENTS_POLL_GITHUB=30` and similar. GitHub checks whose name matches
`AGENT_EVENTS_IGNORE_CHECKS` (a regex, default `\(shadow\)`: shadow CI runs beside the real one; empty
turns it off) give no events and are left out of the pass/fail counts and the overall CI result. Put these, or tokens
(`LINEAR_API_KEY`, `SLACK_USER_TOKEN`, `DOCS_ADMIN_API_KEY`), in `~/.agent-events/env`. The daemon re-reads that file
when it changes. Instead of a token, `AGENT_EVENTS_LINEAR_OP_REF`, `AGENT_EVENTS_SLACK_OP_REF` and
`AGENT_EVENTS_DOCS_OP_REF` there read it from 1Password (e.g. `op://<vault>/<item>/credential`); with neither, that source
is off. The docs source polls the docs admin API's `GET /batches/<source_ref>` (any key; `DOCS_ADMIN_API_URL`
overrides `https://betterstack.com`).
It reads through Hyperfocus's op broker (`~/.hyperfocus/bin/op`) when that is installed, so the
app's 1Password access and memory cache serve it; `AGENT_EVENTS_OP` names another `op`. Nothing is
read until a topic needing it is subscribed, and a failing `op read` is retried only every 10
minutes (`AGENT_EVENTS_SECRET_RETRY`), since it may ask for Touch ID each time.

## Delivery semantics

- Each agent has a cursor. `watch` returns every event newer than the cursor that matches one of the
  agent's subscriptions, with a ~1.5s settle so a burst arrives as one batch. The cursor advances
  only after the client acknowledges that it printed the batch, so a killed watch loses nothing.
- A new subscription only sees events that happen after it was created.
- Push topics (`hf/...`, for Hyperfocus agent messaging) are not polled. Events are published
  into them with the `publish` op (`{"op": "publish", "topic", "summary", "body", "actor", "kind",
  "url", "data", "id"}`); `id` makes publishing idempotent. A watch whose pending events are all on
  push topics settles for only ~0.05s (`AGENT_EVENTS_PUSH_SETTLE`), and the header names the sender
  (`from <actor>`). Subscribe an agent on its behalf with
  `agent-events --agent <session-id> subscribe hf/agent/<id>`; that doesn't record the caller's
  pid/cwd for it. The old name `mc/...` (from Mission Control) still works and means the same topic;
  stored `mc/` topics are renamed to `hf/` when the daemon starts.
- FYI events: a publish with `"quiet": true` (CLI `--quiet`; Hyperfocus sets `data.quiet`) never
  wakes the agent. No watch returns for FYI events alone, and the `Stop` hook leaves them pending.
  They go along with the next events that do wake it, or into its next turn through the
  `UserPromptSubmit` hook, headed "agent-events: FYI since your last turn" and marked `· FYI`.
  Hyperfocus uses this for posts an agent only follows or gets as a notice (broadcasts).
- Filtered subscriptions (`--about <terms>`, `--mine`): only events whose text mentions a term (whole
  words, any case) reach the agent; `--mine` adds `repo#N`, `pull/N` and `#N` for each PR the agent is
  subscribed to. A reply in a thread counts when the thread's first message does. Other events are
  skipped (the cursor moves past them). Subscribing again without them delivers everything again.
- `{"op": "transfer", "from": <agent>, "to": <agent>, "move": true}` hands one conversation's
  subscriptions (filters included, push topics not) to another, e.g. when Hyperfocus continues an
  agent in a fresh conversation. Without "move" the old one keeps them too.
- One watch per agent. Starting another one supersedes the old one, which exits harmlessly. The
  hook's watch is passive: it never takes over from an `agent-events watch`, it waits behind it.
- Each event prints as one compact header (time, sender, thread) followed by its text once. For
  push messages, `data.thread` / `data.thread_title` name the thread; a `[title] …` prefix in the
  summary is read as the thread title, and a summary that just repeats the start of the body is
  left out.
- If the daemon restarts, a running `watch` reconnects on its own. Clients restart a daemon that is
  running older code.
- A source that fails 3 polls in a row sends its subscribers an `error` event, then a `recovered`
  event when polling works again. Retries back off exponentially, up to 15 minutes.
- Events authored by your own account (the `gh` viewer, the Linear API key's user, the Slack token's
  user) are tagged `(you)`. Agents act as you, so these are often their own actions echoing back.

## Claude Code hooks

Claude Code only wakes an idle agent when a background task ends or a hook asks it to. `install`
adds an `asyncRewake` hook, `agent-events hook --watch`, on `Stop`, `UserPromptSubmit` and
`SessionStart`. Claude Code runs it in the background. It waits on the daemon (a passive watch), and
when the agent has events it prints them and exits 2, which wakes the agent with the events as a
message. Nobody has to restart it: every turn end arms it again, a newer one replaces an older
one, and it stays out of the way of a manually started `agent-events watch`. It exits quietly when
the session ends (`SessionEnd` tells the daemon), when the Claude process is gone, when the agent
has no subscriptions, and in headless sessions (`claude -p` / SDK, `CLAUDE_CODE_ENTRYPOINT=sdk-*`),
where an unfinished async hook would keep the run from ending.

The plain `agent-events hook` (`UserPromptSubmit`) injects any pending events into the turn as
context, so an agent that is already awake gets them at once. It does not do that at
`SessionStart`, because context added there starts no turn (`claude --resume` without a prompt
would leave the events unread but acked). Instead the async hook wakes the agent with them. Without
the async hook, `SessionStart` shows them but leaves them unread. Events count as read (the cursor
advances) only once a turn has them. If
the async hook is not installed (older installs), it also falls back to the old nag: on **Stop** it
blocks the turn once and tells the agent to start `agent-events watch` in the background; on the
other events it adds that as context.

Without these hooks, run `agent-events watch` in the background and start it again after each batch,
or run `agent-events watch --follow` under a tool that streams output, such as Claude Code's
Monitor. Monitor expires after 30 minutes at most, so the hook is the better choice for agents.

## Layout

```
bin/agent-events             entry point
agent_events/cli.py          commands, output formatting, hook, install
agent_events/client.py       socket client, daemon auto-start / restart / stop
agent_events/daemon.py       socket server, poller, watch delivery
agent_events/store.py        SQLite schema and queries (~/.agent-events/events.db)
agent_events/sources/        one module per event type (base.py has the contract)
skills/events/SKILL.md       the /events skill for agents
tests/                       python3 -m unittest (see CLAUDE.md)
```

### Adding an event type

Write a `Source` subclass in `agent_events/sources/<type>.py` and register it in
`sources/__init__.py`. It implements four things:

- `normalize(parts)`: validate and canonicalize the segments after the type.
- `poll_key(parts)`: what actually has to be fetched. Several topics can share one poll.
- `poll(key, state)`: fetch, build a JSON-able snapshot, and diff it against `state` to produce
  `Event`s on the most specific topic.
- Optionally `resolve()` and `from_url()`.

A push source (`push = True`, like `sources/hf.py`) only implements `normalize()`; it is never
polled, and events reach it through `publish`.

## Later

- Remote daemons, so one poller can serve several machines. The protocol is already JSON lines over
  a socket, so this mostly means a TCP listener with auth and a client-side `AGENT_EVENTS_REMOTE`.
- More push sources (GitHub webhooks, Slack Socket Mode) next to polling; `hf/` is the first.
- Wildcard topics.
