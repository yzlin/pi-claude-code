import assert from "node:assert/strict";
import * as http from "node:http";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { normalizeContext, type AssistantMessage, type Model } from "@earendil-works/pi-ai";
import {
  closeClaudeProcesses,
  runClaudeRequest,
  setUpstreamUrlForTests,
  type RunClaudeInput,
} from "../src/claude-cli.ts";
import { CLAUDE_CODE_MODELS } from "../src/model-catalog.ts";
import { streamClaudeCode } from "../src/stream.ts";

// A fake Claude Code CLI that, like the real one, POSTs each queried turn to
// `${ANTHROPIC_BASE_URL}/v1/messages?beta=true` and reports a stream-json result.
const FAKE_CLAUDE = `#!/usr/bin/env node
import readline from "node:readline";
if (process.argv.includes("auth") && process.argv.includes("status")) {
  console.log(JSON.stringify({loggedIn: true, authMethod: "claude.ai", subscriptionType: "test"}));
  process.exit(0);
}
const cache = {type: "ephemeral", ttl: "1h"};
const input = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
// Like the real CLI, a warm process resends its whole conversation each turn.
const history = [];
for await (const line of input) {
  const frame = JSON.parse(line);
  if (frame.type === "user" || frame.type === "assistant") history.push(frame.message);
  if (frame.shouldQuery === false) {
    console.log(JSON.stringify({type: "result", num_turns: 0}));
    continue;
  }
  if (frame.type !== "user") continue;
  const messages = structuredClone(history);
  const last = messages.at(-1).content;
  last[last.length - 1] = {...last.at(-1), cache_control: cache};
  const body = {
    model: "claude-sonnet-5",
    max_tokens: 32000,
    system: [{type: "text", text: "identity", cache_control: cache}],
    messages,
    tools: [{name: "mcp__pi__read", input_schema: {type: "object"}, cache_control: cache}],
    stream: true,
  };
  const response = await fetch(process.env.ANTHROPIC_BASE_URL + "/v1/messages?beta=true", {
    method: "POST",
    headers: {"content-type": "application/json"},
    body: JSON.stringify(body),
  }).catch(() => undefined);
  await response?.text().catch(() => undefined);
  const usage = {input_tokens: 3, output_tokens: 1};
  if (response?.status === 200) {
    history.push({role: "assistant", content: [{type: "text", text: "upstream"}]});
    console.log(JSON.stringify({type: "assistant", message: {id: "m_up", role: "assistant", content: [{type: "text", text: "upstream"}], stop_reason: "end_turn", usage}}));
    console.log(JSON.stringify({type: "result", subtype: "success", is_error: false, usage}));
  } else {
    console.log(JSON.stringify({type: "assistant", error: "unknown", message: {role: "assistant", content: [{type: "text", text: "API Error"}]}}));
    console.log(JSON.stringify({type: "result", subtype: "success", is_error: true, usage}));
  }
}
`;

interface Upstream {
  url: string;
  bodies: Array<Record<string, unknown>>;
  close(): Promise<void>;
}

async function startUpstream(): Promise<Upstream> {
  const bodies: Array<Record<string, unknown>> = [];
  const server = http.createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    bodies.push(JSON.parse(body) as Record<string, unknown>);
    const usage = { input_tokens: 3, output_tokens: 1, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 };
    const events = [
      { type: "message_start", message: { id: `m_up_${bodies.length}`, role: "assistant", content: [], usage } },
      { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
      { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "upstream" } },
      { type: "content_block_stop", index: 0 },
      { type: "message_delta", delta: { stop_reason: "end_turn" }, usage },
      { type: "message_stop" },
    ];
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing address");
  return {
    url: `http://127.0.0.1:${address.port}`,
    bodies,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

function countCacheControl(value: unknown): number {
  if (Array.isArray(value)) return value.reduce((total: number, child) => total + countCacheControl(child), 0);
  if (!value || typeof value !== "object") return 0;
  return Object.entries(value).reduce(
    (total, [key, child]) => total + (key === "cache_control" ? 1 : countCacheControl(child)),
    0,
  );
}

const piModel = CLAUDE_CODE_MODELS.find((model) => model.id === "claude-sonnet-5")! as Model<any>;

async function fixture(): Promise<{ root: string; executable: string; upstream: Upstream }> {
  const root = await mkdtemp(join(tmpdir(), "pi-claude-code-hook-"));
  const executable = join(root, "fake-claude.mjs");
  await writeFile(executable, FAKE_CLAUDE, "utf8");
  await chmod(executable, 0o755);
  return { root, executable, upstream: await startUpstream() };
}

function baseInput(executable: string, upstreamUrl: string, text: string): RunClaudeInput {
  return {
    model: "sonnet",
    frames: [{ type: "user", message: { role: "user", content: [{ type: "text", text }] } }],
    systemPrompt: "system",
    manifest: [],
    extraBody: { tools: [] },
    env: { PI_CLAUDE_CODE_COMMAND: executable },
    timeoutMs: 5_000,
    upstreamUrl,
  };
}

test("runClaudeRequest forwards the payload hook's replacement through a real CLI process", async () => {
  const { root, executable, upstream } = await fixture();
  try {
    const seen: Array<{ payload: Record<string, unknown>; model: unknown }> = [];
    const output = await runClaudeRequest({
      ...baseInput(executable, upstream.url, "one"),
      payloadHook: {
        model: piModel,
        onPayload: async (payload, model) => {
          seen.push({ payload: structuredClone(payload) as Record<string, unknown>, model });
          return { ...(payload as Record<string, unknown>), max_tokens: 1234 };
        },
      },
    });
    assert.equal(seen.length, 1);
    assert.equal(seen[0]!.model, piModel);
    assert.equal(seen[0]!.payload.max_tokens, 32000);
    assert.equal(countCacheControl(seen[0]!.payload), 3);
    assert.equal(upstream.bodies.length, 1);
    assert.equal(upstream.bodies[0]!.max_tokens, 1234);
    assert.equal(output.relay.upstreamRequests, 1);
    assert.equal(output.message.content[0]?.type, "text");
  } finally {
    await closeClaudeProcesses();
    await upstream.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("runClaudeRequest rejects without contacting upstream when the payload hook throws", async () => {
  const { root, executable, upstream } = await fixture();
  try {
    let calls = 0;
    await assert.rejects(
      runClaudeRequest({
        ...baseInput(executable, upstream.url, "one"),
        payloadHook: {
          model: piModel,
          onPayload: () => {
            calls += 1;
            throw new Error("cap exceeded");
          },
        },
      }),
      /Incomplete Claude Code upstream response.*request rejected: cap exceeded/,
    );
    assert.equal(calls, 1);
    assert.equal(upstream.bodies.length, 0);
  } finally {
    await closeClaudeProcesses();
    await upstream.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a warm continuation calls only the second turn's payload hook", async () => {
  const { root, executable, upstream } = await fixture();
  try {
    const firstCalls: unknown[] = [];
    const secondCalls: unknown[] = [];
    const first = await runClaudeRequest({
      ...baseInput(executable, upstream.url, "one"),
      sessionId: "hook-session",
      payloadHook: { model: piModel, onPayload: (payload) => void firstCalls.push(payload) },
    });
    const second = await runClaudeRequest({
      ...baseInput(executable, upstream.url, "two"),
      sessionId: "hook-session",
      frames: [
        { type: "user", message: { role: "user", content: [{ type: "text", text: "one" }] }, shouldQuery: false },
        { type: "assistant", message: { role: "assistant", content: first.message.content } },
        { type: "user", message: { role: "user", content: [{ type: "text", text: "two" }] } },
      ],
      payloadHook: {
        model: piModel,
        onPayload: (payload) => {
          secondCalls.push(payload);
          return { ...(payload as Record<string, unknown>), turn: 2 };
        },
      },
    });
    assert.equal(second.message.id, "m_up_2");
    assert.equal(firstCalls.length, 1);
    assert.equal(secondCalls.length, 1);
    assert.equal(upstream.bodies.length, 2);
    assert.equal(upstream.bodies[0]!.turn, undefined);
    assert.equal(upstream.bodies[1]!.turn, 2);
  } finally {
    await closeClaudeProcesses();
    await upstream.close();
    await rm(root, { recursive: true, force: true });
  }
});

async function finalMessage(stream: ReturnType<typeof streamClaudeCode>): Promise<AssistantMessage> {
  for await (const event of stream) {
    if (event.type === "done") return event.message;
    if (event.type === "error") return event.error;
  }
  throw new Error("stream ended without a terminal event");
}

test("streamClaudeCode passes onPayload, the Pi model, and cacheRetention none to the relay", async () => {
  const { root, executable, upstream } = await fixture();
  setUpstreamUrlForTests(upstream.url);
  try {
    const context = normalizeContext({
      systemPrompt: "system",
      messages: [{ role: "user", content: "hello", timestamp: 1 }],
    });
    const seen: Array<{ payload: Record<string, unknown>; model: unknown }> = [];
    const ok = await finalMessage(
      streamClaudeCode(piModel, context, {
        env: { PI_CLAUDE_CODE_COMMAND: executable },
        cacheRetention: "none",
        onPayload: (payload, model) => {
          seen.push({ payload: structuredClone(payload) as Record<string, unknown>, model });
          return { ...(payload as Record<string, unknown>), max_tokens: 77 };
        },
      }),
    );
    assert.equal(ok.stopReason, "stop", ok.errorMessage);
    assert.equal(seen.length, 1);
    assert.equal(seen[0]!.model, piModel);
    assert.equal(countCacheControl(seen[0]!.payload), 0);
    assert.equal(upstream.bodies.length, 1);
    assert.equal(upstream.bodies[0]!.max_tokens, 77);
    assert.equal(countCacheControl(upstream.bodies[0]), 0);

    // Default retention keeps Claude Code's cache markers.
    await finalMessage(streamClaudeCode(piModel, context, { env: { PI_CLAUDE_CODE_COMMAND: executable } }));
    assert.equal(countCacheControl(upstream.bodies[1]), 3);

    const failed = await finalMessage(
      streamClaudeCode(piModel, context, {
        env: { PI_CLAUDE_CODE_COMMAND: executable },
        onPayload: () => {
          throw new Error("cap exceeded");
        },
      }),
    );
    assert.equal(failed.stopReason, "error");
    assert.equal(upstream.bodies.length, 2);
  } finally {
    setUpstreamUrlForTests(undefined);
    await closeClaudeProcesses();
    await upstream.close();
    await rm(root, { recursive: true, force: true });
  }
});
