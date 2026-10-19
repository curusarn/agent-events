---
name: events
description: Subscribe to GitHub PRs, Linear tickets / comment threads and Slack threads / channels (plus Hyperfocus messages on hf/ topics), and get woken up when they change — instead of polling or remembering to check. Use when you take ownership of a PR, ticket or thread (opening/working a PR, picking up a Linear task, being handed a Slack link), when the user says "watch/follow/keep an eye on X", when agent-events delivers events or a hook says a watch isn't running, or for /events subscribe|unsubscribe|list|watch|log|status.
allowed-tools: Bash
---

# Events

A local daemon (`agent-events`) does the watching for you. You tell it **once** what you care about;
it remembers your subscriptions (keyed by your Claude Code session id), polls GitHub/Linear/Slack, and
keeps new events for you. A Claude Code hook delivers them: when events arrive, you are woken with a
message that starts with `agent-events: new events for you` and contains the events in full (Claude
Code labels it "Stop hook feedback" / "Stop hook blocking error"; it is not an error). Nothing to
run, read or restart. FYI events (marked `· FYI`; e.g. Hyperfocus posts you only follow) never wake
you: they arrive with your next events or at the start of your next turn. Act on one only if it
concerns your task, and don't acknowledge it.

You never need to remember what you are subscribed to — the daemon does. Your only job is: subscribe
when you take something on, and handle events when they come.

## Arguments

- `/events subscribe <topic-or-url>...` → `agent-events subscribe ...` (it says if you also need to start a watch)
- `/events unsubscribe <topic>...` (or `--all`) → `agent-events unsubscribe ...`
- `/events list` → `agent-events list`
- `/events watch` → only without the hook (see below): start `agent-events watch` in the background
- `/events log [topic]` → `agent-events log [topic]` (recent events, already delivered or not)
- `/events status` → `agent-events status` (daemon, all agents, polling errors)
- no arguments → `agent-events list` (it says whether you need to start a watch)

## Topics

Paths are `<type>/<segments>`. Subscribing to a topic also gets you everything nested under it.
URLs are accepted anywhere a topic is and converted for you (prefer passing the URL you were given).

| Topic | What you get |
|---|---|
| `github/<owner>/<repo>/<pr>` | Everything on the PR: comments (a comment posted through Linear's review UI says "via Linear": its author wrote it there, even when an agent shares the login), inline review comments, reviews, review requests, CI failures and final CI result, pushes/force-pushes, merge/close/reopen, draft/ready, conflicts, labels, assignees, title/description edits, threads resolved |
| `linear/<ISSUE-ID>` | Everything on the ticket: status, assignee, priority, project, cycle, labels, estimate, due date, parent, title, description, all comments |
| `linear/<ISSUE-ID>/<thread>` | Only one comment thread (`<thread>` = the 8 hex chars from a `#comment-xxxxxxxx` link; any comment in the thread works) |
| `slack/<channel-id>/<thread-ts>` | One thread: replies, edits, deletions, reactions |
| `slack/<channel-id>` | A whole channel: new messages + replies in its threads. Noisy: prefer threads, or filter it (below) |
| `docs/<source_ref>` | A staged Better Stack docs batch (e.g. `docs/U-9225`): an event per article when it is published (with who), discarded, restaged or staged, plus one `batch` event once nothing in it is left unpublished. A `/` in the ref is written `~`. docs-terminal's `bin/docs-api-version stage` subscribes you by itself |
| `docs/<source_ref>/<canonical_id>` | One article's version in that batch |
| `hf/agent/<id>`, `hf/thread/<id>` | Hyperfocus messages from other agents / the operator. Push topics: nothing is polled, messages arrive within a second. Hyperfocus subscribes you to your inbox when it spawns you |

`agent-events types` prints this list from the code.

**Busy channels (e.g. deploy channels):** subscribe with a filter so you're
woken only for what's yours: `agent-events subscribe slack/<channel> --mine` (messages mentioning the PRs
you're subscribed to: `repo#N`, `pull/N`, `#N`), plus `--about 'word,other words'` for anything else that
marks your work. Replies in a thread come along when its first message matched. Without a filter every
other team's deploy wakes you.

## When to subscribe (do it without being asked)

Subscribe the moment you take ownership of something whose changes you'd need to react to:

- You open a PR, or are asked to work on / babysit / review-fix an existing PR → subscribe to the PR.
- You pick up a Linear ticket → subscribe to the ticket (or just the relevant comment thread, if the
  ticket is busy and you were pointed at a specific comment).
- You are given a Slack message link → subscribe to its thread (`agent-events subscribe <link>` —
  a link to a reply resolves to its thread).
- You stage docs in a batch → subscribe to `docs/<source_ref>` (docs-terminal's stage does it for you);
  when the `batch` event says it is published, move the Linear issue on.
- The user asks you to "watch", "follow", "keep an eye on" something.

```bash
agent-events subscribe https://github.com/owner/repo/pull/123
agent-events subscribe linear/T-12345
agent-events subscribe 'https://team.slack.com/archives/C0123ABCD/p1712345678123456?thread_ts=1712345000.000100'
```

`subscribe` validates the topic by fetching it (it fails loudly on typos / missing access) and prints
what it is watching.

## Receiving events

Normally there is nothing to do: the `agent-events hook --watch` hook (installed by
`agent-events install`) waits for your events in the background and wakes you with them, and re-arms
itself every time your turn ends. Pending events are also added to your context when a prompt
arrives. Don't run `agent-events watch` yourself, and don't `sleep`/poll GitHub, Linear or Slack to
wait for changes.

Only if `agent-events list` says the watch is "NOT running — start `agent-events watch` in the
background", or a hook tells you to, is the hook missing. Then start the watch **with the Bash
tool's `run_in_background: true`**:

```bash
agent-events watch
```

- It blocks until there are events, prints them and exits. Read its output file, act on the events,
  then start it again.
- Starting one while one runs is safe: the newest takes over, the older exits with "took over;
  nothing to do".
- `agent-events watch --follow` stays running and prints each batch as it arrives, for Claude
  Code's Monitor tool (each batch becomes one notification; Monitor expires after 30 minutes, so
  re-arm it then).
- If it prints "no subscriptions", there is nothing to watch; don't restart it.

Events are acknowledged only once delivered, so nothing is lost; events that arrive while nothing
is watching wait for the next delivery.

## Handling events

- Events are grouped by topic with a one-line description of the PR/ticket/thread. Each event has a
  compact header (time, sender, thread, summary), then its text (comments, messages, description
  diffs) and a link.
- **"(you)"** marks events authored by the user's own account. Agents act as that account, so these
  are often your own actions echoing back (your comment, your push) — skip those. But the user also
  uses that account, so if it's something you didn't do, treat it as the user talking to you.
- **`hf/...` events** are messages sent to you through Hyperfocus. The header shows
  `from <sender>` and the thread, and the message text follows. Answer the way the message asks
  (usually via the `hf` CLI / mission-control skill).
- Fetch more context when needed with the usual tools (`gh pr view`, the /linear or /slack skills).
- Reply, fix, push etc. as your task requires.

## Keep subscriptions relevant

Subscriptions outlive your memory of them — that's the point — so prune them:

- When you're done with something (PR merged/closed and no follow-ups, ticket finished, thread
  resolved), `agent-events unsubscribe <topic>`.
- If events feel noisy, run `agent-events list`, drop what no longer matters, or narrow a ticket
  subscription to the specific comment thread (`linear/T-123/<thread>`) or a channel to a thread.
- `agent-events unsubscribe --all` when your whole task is complete.

## Troubleshooting

- `agent-events list` shows per-subscription poll errors (auth, not found). `agent-events status`
  shows the daemon and every agent. `agent-events refresh` polls your topics immediately.
- The daemon starts automatically on first use and restarts itself when its code changes.
  `agent-events daemon` says whether one is running; `agent-events daemon -b --replace` restarts it (in the background, without your agent's identity).
- Logs: `~/.agent-events/daemon.log`. Credentials: `gh` auth for GitHub; Linear and Slack tokens come
  from 1Password via `op read` (or `LINEAR_API_KEY` / `SLACK_USER_TOKEN` in `~/.agent-events/env`).
- Your agent id is `$CLAUDE_CODE_SESSION_ID`; outside Claude Code pass `--agent <id>`.
