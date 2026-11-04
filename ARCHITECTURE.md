# Architecture

This document explains how pi-claude-code works internally. For installation
and usage, see [README.md](README.md).

## Overview

The extension registers a Pi provider that routes all Claude model
communication through the official Claude Code CLI. It talks to Claude Code
through Claude Code's native `stream-json` protocol and translates streamed
responses back into Pi's message format.

Pi remains responsible for conversation history, approvals, compaction, and tool
execution. The extension exposes Pi's tools to Claude Code through an inert MCP
inventory. Claude can request a tool, but Claude Code cannot execute it; the
request is returned to Pi instead.

## Authentication

Claude Code owns and stores its credentials. The extension does not read its
credential files, refresh its tokens, or persist them. Authenticated request
headers do pass transiently through the local turn relay described below.

The provider has an ambient API-key-style auth method without a login flow. Its
`check()` runs `claude auth status` and reports the provider as configured only
when Claude Code is installed and logged in, so Pi hides the models otherwise.
Requests resolve to a placeholder key. It reports Claude
subscription logins as OAuth-shaped auth so that Pi labels list-price accounting
as a subscription. The provider also declares an `oauth` method, marked
`isSubscription`, only so Pi's subscription-aware UI works. Its `login()` never
starts a flow; it tells the user to run `claude auth login`.

Claude Code child processes do not inherit `ANTHROPIC_API_KEY`. The parent
environment is unchanged. Inherited auth-token, proxy, Bedrock, Vertex, and
Foundry overrides are still rejected to avoid silently changing authentication
or billing.

## Usage-credit guard

Claude Code emits a `rate_limit_event` for each turn. If it reports
`isUsingOverage`, the response was charged to usage credits rather than plan
limits, and the turn fails with a non-retryable `Claude Code billing` error
unless `PI_CLAUDE_CODE_ALLOW_USAGE_CREDITS` is set. Upstream rejections that
ask for usage credits (for example third-party app classification) are
rewritten into the same kind of error. The latest rate-limit status is attached
to the assistant message as a `claude-code.rate-limit` diagnostic.

## Process reuse

Linear conversations may reuse a warm Claude Code process. Only exact linear
continuations reuse one. Branches, edits, configuration changes, errors, and
tool-use boundaries fall back to a fresh process, which replays Pi's full
conversation. At most two processes are retained, and no process is retained
for more than five minutes.

## Local turn relay

Each Claude Code worker receives an `ANTHROPIC_BASE_URL` that points to a
random, loopback-only HTTP route owned by this extension.

The relay exists because Pi, rather than Claude Code, owns the agent loop and
tool execution. In particular, it:

- forwards exactly one Anthropic Messages request for each Pi turn and rejects
  additional native recovery attempts. Claude Code can otherwise issue another
  generation after an inert MCP tool call, even with one turn and HTTP retries
  disabled;
- captures and streams the first upstream response before Claude Code processes
  it, preserving signed thinking, tool arguments, usage, and the native stop
  reason. This lets Pi retain a valid tool request even when Claude Code later
  reports an `error_max_turns` boundary or a synthetic recovery error;
- rebuilds every user message from Pi's own history. Claude Code adds
  reminders to user messages and tool results (auto-memory, CLAUDE.md, the
  account email, attachments). The relay matches Pi's blocks to Claude Code's
  in order and drops the rest. It keeps Claude Code's copies of images, because
  it may have downscaled them and added a note on how to map coordinates back
  to the original. Workers also disable auto-memory and CLAUDE.md loading, so
  those reminders are not created in the first place. If a request no longer
  lines up with Pi's history, the turn fails rather than sending a guess;
- records Claude Code's initial native system messages (environment, date),
  then restores their stable layout during full-history replay. It also adjusts
  cache breakpoints so tool rounds do not continually invalidate the
  prompt-cache prefix;
- injects supported mid-conversation tool additions and removals at their
  original assistant boundaries and adds the corresponding Anthropic beta
  header; and
- ties cancellation to the active upstream socket as well as the Claude Code
  process; and
- applies Pi's per-request `onPayload` hook to the exact body sent upstream.

### Payload hook and cache stripping

Pi's `onPayload` and the Pi `Model` are bound to each turn when the turn
begins, never when the worker is created, so a warm continuation calls the
current turn's hook. Neither is part of the worker fingerprint.

For the turn's single forwarded request the relay:

1. reads Claude Code's request body;
2. applies the replay rewrite described above, if any;
3. when Pi requested `cacheRetention: "none"`, removes every `cache_control`
   key recursively from `system`, `messages`, and `tools` (the turn also skips
   warm-process reuse);
4. awaits `onPayload(parsedBody, piModel)`. A non-`undefined` return value
   replaces the body and is serialized as-is; `undefined` keeps the body; and
5. forwards the result. `requestBody` is exactly the forwarded bytes.

If the turn is aborted while the hook is pending, nothing is forwarded. If the
hook (or stripping) throws, nothing is forwarded either: Claude Code receives a
502, the turn fails with an error that includes the thrown message
(`request rejected: <message>`), and Pi's stream ends with
`stopReason: "error"`. Any retried Claude Code request in that turn is denied
without calling the hook again.

Every model this provider registers sets `compat.supportsPayloadHook: true`.
This is a convention of this package, not a Pi core field, so consumers that
must inspect or cap the outgoing payload (for example, its top-level
`max_tokens`) can tell that `onPayload` is honored before sending anything.

## Tool schemas

Tool schemas are normalized for Claude Code. Nullable variants and schema
composition such as `oneOf`, `allOf`, and `anyOf` may be simplified or removed.

Exact mid-conversation tool additions and removals, including prompt-cache
preservation, work only on supported models. Other cases fall back to the
current tool state and may lose prompt-cache reuse.
