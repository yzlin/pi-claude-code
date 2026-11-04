import assert from "node:assert/strict";
import * as http from "node:http";
import test from "node:test";
import { describeFailure, TurnRelay } from "../src/turn-relay.ts";

function listen(server: http.Server): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("missing address");
      resolve(address.port);
    });
  });
}

test("relays one upstream Messages request and captures its SSE response", async () => {
  const upstreamBodies: string[] = [];
  const upstreamBetas: string[] = [];
  const upstream = http.createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    upstreamBodies.push(body);
    upstreamBetas.push(String(request.headers["anthropic-beta"] ?? ""));
    response.writeHead(200, { "content-type": "text/event-stream", "request-id": "req_test" });
    const usage = {
      input_tokens: 2,
      output_tokens: 1,
      cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0,
    };
    const events = [
      { type: "message_start", message: { id: "m1", role: "assistant", content: [], usage } },
      { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
      { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "hello" } },
      { type: "content_block_stop", index: 0 },
      { type: "message_delta", delta: { stop_reason: "end_turn" }, usage },
      { type: "message_stop" },
    ];
    response.end(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""));
  });
  const upstreamPort = await listen(upstream);
  const observed: string[] = [];
  const relay = new TurnRelay(
    `http://127.0.0.1:${upstreamPort}`,
    5_000,
    (event) => observed.push(event.type),
    (body) => Buffer.from(JSON.stringify({ ...JSON.parse(body.toString("utf8")), rewritten: true })),
    ["native-tools-test"],
  );

  try {
    const baseUrl = await relay.start();
    const first = await fetch(`${baseUrl}/v1/messages`, {
      method: "POST",
      headers: { "anthropic-beta": "existing-test" },
      body: "{}",
    });
    assert.equal(first.status, 200);
    await first.text();
    const second = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: "{}" });
    assert.equal(second.status, 400);
    await relay.settled();

    assert.equal(upstreamBodies.length, 1);
    assert.deepEqual(JSON.parse(upstreamBodies[0]!), { rewritten: true });
    assert.deepEqual(JSON.parse(relay.originalRequestBody!.toString("utf8")), {});
    assert.equal(upstreamBetas[0], "existing-test,native-tools-test");
    assert.equal(relay.used, true);
    assert.equal(relay.denied, 1);
    assert.equal(relay.capture.complete, true);
    assert.equal(relay.capture.message?.content[0]?.type, "text");
    assert.equal((relay.capture.message?.content[0] as { text: string }).text, "hello");
    assert.equal(relay.requestId, "req_test");
    assert.deepEqual(observed.at(-1), "message_stop");

    relay.finishTurn();
    const nextObserved: string[] = [];
    relay.beginTurn((event) => nextObserved.push(event.type));
    const next = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: '{"turn":2}' });
    assert.equal(next.status, 200);
    await next.text();
    await relay.settled();
    assert.equal(upstreamBodies.length, 2);
    assert.deepEqual(JSON.parse(upstreamBodies[1]!), { turn: 2 });
    assert.equal(relay.denied, 0);
    assert.equal(relay.capture.complete, true);
    assert.equal(nextObserved.at(-1), "message_stop");
  } finally {
    await relay.close();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});

test("surfaces an error event from an otherwise successful SSE stream", async () => {
  const upstream = http.createServer(async (request, response) => {
    for await (const _chunk of request);
    response.writeHead(200, { "content-type": "text/event-stream" });
    const usage = { input_tokens: 2, output_tokens: 0 };
    response.end(
      [
        `event: message_start\ndata: ${JSON.stringify({ type: "message_start", message: { id: "m1", role: "assistant", content: [], usage } })}\n\n`,
        `event: error\ndata: ${JSON.stringify({ type: "error", error: { type: "overloaded_error", message: "Overloaded" } })}\n\n`,
      ].join(""),
    );
  });
  const upstreamPort = await listen(upstream);
  const relay = new TurnRelay(`http://127.0.0.1:${upstreamPort}`, 5_000);

  try {
    const baseUrl = await relay.start();
    const response = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: "{}" });
    assert.equal(response.status, 200);
    await response.text();
    await relay.settled();

    assert.equal(relay.capture.complete, false);
    assert.equal(relay.errorText(), "overloaded_error: Overloaded");
  } finally {
    await relay.close();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});

test("describes relay failures with their error code and nested errors", () => {
  const refused = Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:1"), { code: "ECONNREFUSED" });
  assert.equal(describeFailure(refused), "Error: connect ECONNREFUSED 127.0.0.1:1");
  const timeout = Object.assign(new Error("connect ETIMEDOUT 10.0.0.1:443"), { code: "ETIMEDOUT" });
  const every = Object.assign(new AggregateError([refused, timeout]), { code: "ETIMEDOUT" });
  assert.equal(
    describeFailure(every),
    "AggregateError (ETIMEDOUT) [Error: connect ECONNREFUSED 127.0.0.1:1; Error: connect ETIMEDOUT 10.0.0.1:443]",
  );
  assert.equal(describeFailure(Object.assign(new Error(""), { code: "ECONNRESET" })), "Error (ECONNRESET)");
  assert.equal(describeFailure("nope"), "UnknownError");
});

function sseUpstream(): { server: http.Server; bodies: string[] } {
  const bodies: string[] = [];
  const server = http.createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    bodies.push(body);
    response.writeHead(200, { "content-type": "text/event-stream" });
    const usage = { input_tokens: 2, output_tokens: 1 };
    const events = [
      { type: "message_start", message: { id: "m1", role: "assistant", content: [], usage } },
      { type: "message_delta", delta: { stop_reason: "end_turn" }, usage },
      { type: "message_stop" },
    ];
    response.end(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""));
  });
  return { server, bodies };
}

const hookModel = { id: "claude-sonnet-5", api: "claude-code-stream-json" } as never;

function countCacheControl(value: unknown): number {
  if (Array.isArray(value)) return value.reduce((total: number, child) => total + countCacheControl(child), 0);
  if (!value || typeof value !== "object") return 0;
  return Object.entries(value).reduce(
    (total, [key, child]) => total + (key === "cache_control" ? 1 : countCacheControl(child)),
    0,
  );
}

const cachedBody = {
  max_tokens: 32_000,
  system: [{ type: "text", text: "sys", cache_control: { type: "ephemeral" } }],
  messages: [{ role: "user", content: [{ type: "text", text: "hi", cache_control: { type: "ephemeral" } }] }],
  tools: [{ name: "mcp__pi__read", cache_control: { type: "ephemeral" } }],
};

async function withRelay(
  run: (relay: TurnRelay, baseUrl: string, bodies: string[]) => Promise<void>,
): Promise<void> {
  const upstream = sseUpstream();
  const port = await listen(upstream.server);
  const relay = new TurnRelay(`http://127.0.0.1:${port}`, 5_000);
  try {
    await run(relay, await relay.start(), upstream.bodies);
  } finally {
    await relay.close();
    await new Promise<void>((resolve) => upstream.server.close(() => resolve()));
  }
}

test("payload hook sees the rewritten body and its replacement is forwarded", async () => {
  await withRelay(async (relay, baseUrl, bodies) => {
    const seen: Array<{ payload: unknown; model: unknown }> = [];
    relay.beginTurn(
      undefined,
      async (body) => Buffer.from(JSON.stringify({ ...JSON.parse(body.toString("utf8")), rewritten: true })),
      [],
      {
        payloadHook: {
          model: hookModel,
          onPayload: async (payload, model) => {
            seen.push({ payload: structuredClone(payload), model });
            return { replaced: true, max_tokens: 10 };
          },
        },
      },
    );
    const response = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: '{"max_tokens":99}' });
    assert.equal(response.status, 200);
    await response.text();
    await relay.settled();

    assert.deepEqual(seen, [{ payload: { max_tokens: 99, rewritten: true }, model: hookModel }]);
    assert.equal(bodies.length, 1);
    assert.deepEqual(JSON.parse(bodies[0]!), { replaced: true, max_tokens: 10 });
    assert.equal(relay.requestBody?.toString("utf8"), bodies[0]);
  });
});

test("payload hook returning undefined keeps the body", async () => {
  await withRelay(async (relay, baseUrl, bodies) => {
    relay.beginTurn(undefined, undefined, [], {
      payloadHook: { model: hookModel, onPayload: () => undefined },
    });
    const response = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: '{"max_tokens":99}' });
    await response.text();
    await relay.settled();
    assert.deepEqual(JSON.parse(bodies[0]!), { max_tokens: 99 });
    assert.equal(relay.requestBody?.toString("utf8"), bodies[0]);
  });
});

test("throwing payload hook forwards nothing and retries are denied", async () => {
  await withRelay(async (relay, baseUrl, bodies) => {
    let calls = 0;
    relay.beginTurn(undefined, undefined, [], {
      payloadHook: {
        model: hookModel,
        onPayload: () => {
          calls += 1;
          throw new Error("hook refused");
        },
      },
    });
    const first = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: "{}" });
    assert.equal(first.status, 502);
    await first.text();
    const retry = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: "{}" });
    assert.equal(retry.status, 400);
    await retry.text();
    await relay.settled();
    assert.equal(bodies.length, 0);
    assert.equal(calls, 1);
    assert.equal(relay.denied, 1);
    assert.notEqual(relay.status, 200);
    assert.equal(relay.rejection, "hook refused");
  });
});

test("abort while an async payload hook is pending forwards nothing", async () => {
  await withRelay(async (relay, baseUrl, bodies) => {
    let entered!: () => void;
    const hookEntered = new Promise<void>((resolve) => (entered = resolve));
    let release!: () => void;
    relay.beginTurn(undefined, undefined, [], {
      payloadHook: {
        model: hookModel,
        onPayload: async () => {
          entered();
          await new Promise<void>((resolve) => (release = resolve));
          return { late: true };
        },
      },
    });
    const pending = fetch(`${baseUrl}/v1/messages`, { method: "POST", body: "{}" }).then(
      (response) => response.text(),
      () => undefined,
    );
    await hookEntered;
    relay.abort();
    release();
    await pending;
    await relay.settled();
    assert.equal(bodies.length, 0);
  });
});

test("stripCache removes every cache_control marker; default keeps them", async () => {
  await withRelay(async (relay, baseUrl, bodies) => {
    let hooked: unknown;
    relay.beginTurn(undefined, undefined, [], {
      stripCache: true,
      payloadHook: { model: hookModel, onPayload: (payload) => void (hooked = structuredClone(payload)) },
    });
    const stripped = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: JSON.stringify(cachedBody) });
    await stripped.text();
    await relay.settled();
    assert.equal(countCacheControl(hooked), 0);
    assert.equal(countCacheControl(JSON.parse(bodies[0]!)), 0);
    assert.equal((JSON.parse(bodies[0]!) as { max_tokens: number }).max_tokens, 32_000);

    relay.finishTurn();
    relay.beginTurn();
    const kept = await fetch(`${baseUrl}/v1/messages`, { method: "POST", body: JSON.stringify(cachedBody) });
    await kept.text();
    await relay.settled();
    assert.equal(countCacheControl(JSON.parse(bodies[1]!)), 3);
  });
});
