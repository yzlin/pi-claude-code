import * as http from "node:http";
import * as https from "node:https";
import { randomBytes } from "node:crypto";
import type { AddressInfo } from "node:net";
import type { Api, Model } from "@earendil-works/pi-ai";
import type { AnthropicStreamEvent, NativeContentBlock, NativeMessage } from "./native-types.ts";

function copy<T>(value: T): T {
  return structuredClone(value);
}

export function withoutCacheControl(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withoutCacheControl);
  if (!value || typeof value !== "object") return value;
  const output: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (key !== "cache_control") output[key] = withoutCacheControl(child);
  }
  return output;
}

export type RequestTransform = (body: Buffer) => Buffer | Promise<Buffer>;

/** Pi's `onPayload` bound to the Pi model of one turn (not Claude Code's native route). */
export interface PayloadHook {
  onPayload: (payload: unknown, model: Model<Api>) => unknown | undefined | Promise<unknown | undefined>;
  model: Model<Api>;
}

export interface TurnRequestOptions {
  payloadHook?: PayloadHook;
  /** Remove every `cache_control` marker (Pi `cacheRetention: "none"`). */
  stripCache?: boolean;
}

async function finalizeRequest(body: Buffer, options: TurnRequestOptions): Promise<Buffer> {
  if (!options.stripCache && !options.payloadHook) return body;
  let payload = JSON.parse(body.toString("utf8")) as Record<string, unknown>;
  if (options.stripCache) {
    for (const key of ["system", "messages", "tools"]) {
      if (key in payload) payload[key] = withoutCacheControl(payload[key]);
    }
  }
  if (options.payloadHook) {
    const replacement = await options.payloadHook.onPayload(payload, options.payloadHook.model);
    if (replacement !== undefined) payload = replacement as Record<string, unknown>;
  }
  return Buffer.from(JSON.stringify(payload));
}

export class AnthropicCapture {
  message?: NativeMessage;
  complete = false;
  /** Error reported inside an otherwise successful (HTTP 200) event stream. */
  error?: string;
  private pending = "";
  private readonly decoder = new TextDecoder();
  private readonly argumentsByIndex = new Map<number, string>();

  constructor(private readonly onEvent?: (event: AnthropicStreamEvent) => void) {}

  feed(chunk: Uint8Array): void {
    this.pending += this.decoder.decode(chunk, { stream: true });
    this.consumeFrames();
  }

  finish(): void {
    this.pending += this.decoder.decode();
    this.consumeFrames();
  }

  private consumeFrames(): void {
    for (;;) {
      const match = /\r?\n\r?\n/u.exec(this.pending);
      if (!match) return;
      const frame = this.pending.slice(0, match.index);
      this.pending = this.pending.slice(match.index + match[0].length);
      const data = frame
        .split(/\r?\n/u)
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart())
        .join("\n");
      if (!data || data === "[DONE]") continue;
      const event = JSON.parse(data) as AnthropicStreamEvent;
      this.apply(event);
      this.onEvent?.(event);
    }
  }

  private apply(event: AnthropicStreamEvent): void {
    switch (event.type) {
      case "message_start":
        this.message = copy(event.message);
        break;
      case "content_block_start":
        if (this.message) this.message.content[event.index] = copy(event.content_block);
        break;
      case "content_block_delta": {
        const block = this.message?.content[event.index];
        if (!block) break;
        const delta = event.delta;
        if (delta.type === "text_delta" && block.type === "text") block.text += delta.text;
        else if (delta.type === "thinking_delta" && block.type === "thinking") block.thinking += delta.thinking;
        else if (delta.type === "signature_delta" && block.type === "thinking") {
          block.signature = (block.signature ?? "") + delta.signature;
        } else if (delta.type === "input_json_delta" && block.type === "tool_use") {
          this.argumentsByIndex.set(event.index, (this.argumentsByIndex.get(event.index) ?? "") + delta.partial_json);
        }
        break;
      }
      case "content_block_stop": {
        const block = this.message?.content[event.index];
        if (block?.type === "tool_use" && this.argumentsByIndex.has(event.index)) {
          const raw = this.argumentsByIndex.get(event.index) ?? "";
          block.input = raw.trim() ? (JSON.parse(raw) as Record<string, unknown>) : {};
          this.argumentsByIndex.delete(event.index);
        }
        break;
      }
      case "message_delta":
        if (this.message) {
          Object.assign(this.message, event.delta);
          if (event.usage) this.message.usage = { ...this.message.usage, ...event.usage } as NativeMessage["usage"];
        }
        break;
      case "message_stop":
        this.complete = !!this.message?.stop_reason && this.argumentsByIndex.size === 0;
        break;
      case "error": {
        const { type, message } = event.error ?? {};
        this.error = [type, message].filter(Boolean).join(": ") || "unknown stream error";
        break;
      }
    }
  }
}

interface Destroyable {
  destroy(error?: Error): void;
}

const STRIPPED_REQUEST_HEADERS = new Set([
  "host",
  "connection",
  "content-length",
  "transfer-encoding",
  "proxy-authorization",
  "proxy-connection",
  "accept-encoding",
]);

const STRIPPED_RESPONSE_HEADERS = new Set(["connection", "transfer-encoding", "server", "date"]);

export class TurnRelay {
  capture: AnthropicCapture;
  used = false;
  denied = 0;
  status?: number;
  requestId?: string;
  failure?: string;
  errorBody = Buffer.alloc(0);
  /** Message of the error that rejected the request before forwarding (payload hook or finalization). */
  rejection?: string;
  originalRequestBody?: Buffer;
  requestBody?: Buffer;

  private readonly upstream: URL;
  private readonly prefix = `/turn/${randomBytes(32).toString("base64url")}`;
  private readonly server: http.Server;
  private readonly sockets = new Set<Destroyable>();
  private readonly active = new Set<Promise<void>>();
  private cancelled = false;
  private closed = false;
  private baseUrl?: string;
  private transformRequest?: RequestTransform;
  private turnOptions: TurnRequestOptions = {};
  private turnToken = 0;
  private requestBetas: string[] = [];

  constructor(
    upstream = "https://api.anthropic.com",
    private readonly timeoutMs = 180_000,
    onEvent?: (event: AnthropicStreamEvent) => void,
    transformRequest?: RequestTransform,
    requestBetas: string[] = [],
  ) {
    this.upstream = new URL(upstream);
    const loopback = ["127.0.0.1", "::1", "localhost"].includes(this.upstream.hostname);
    if (
      (this.upstream.protocol !== "https:" && !(this.upstream.protocol === "http:" && loopback)) ||
      this.upstream.username ||
      this.upstream.password ||
      this.upstream.search ||
      this.upstream.hash
    ) {
      throw new Error("Claude Code upstream must be HTTPS or a loopback HTTP fixture");
    }
    this.capture = new AnthropicCapture(onEvent);
    this.transformRequest = transformRequest;
    this.requestBetas = requestBetas;
    this.server = http.createServer((request, response) => {
      const operation = this.handle(request, response).finally(() => this.active.delete(operation));
      this.active.add(operation);
    });
  }

  /** Arm the persistent relay for one new Claude turn. */
  beginTurn(
    onEvent?: (event: AnthropicStreamEvent) => void,
    transformRequest?: RequestTransform,
    requestBetas: string[] = [],
    turnOptions: TurnRequestOptions = {},
  ): void {
    if (this.closed) throw new Error("Claude Code turn relay is closed");
    if (this.active.size > 0) throw new Error("Claude Code turn relay still has an active request");
    this.capture = new AnthropicCapture(onEvent);
    this.used = false;
    this.denied = 0;
    this.status = undefined;
    this.requestId = undefined;
    this.failure = undefined;
    this.errorBody = Buffer.alloc(0);
    this.rejection = undefined;
    this.originalRequestBody = undefined;
    this.requestBody = undefined;
    this.transformRequest = transformRequest;
    this.turnOptions = turnOptions;
    this.turnToken += 1;
    this.requestBetas = requestBetas;
    this.cancelled = false;
  }

  /** Disarm the current turn without closing the worker-scoped HTTP server. */
  finishTurn(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    for (const socket of this.sockets) socket.destroy(new Error("Claude Code request aborted"));
    this.sockets.clear();
    this.server.closeAllConnections?.();
  }

  async start(): Promise<string> {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      this.server.once("error", onError);
      this.server.listen(0, "127.0.0.1", () => {
        this.server.off("error", onError);
        resolve();
      });
    });
    const address = this.server.address() as AddressInfo;
    this.baseUrl = `http://127.0.0.1:${address.port}${this.prefix}`;
    return this.baseUrl;
  }

  errorText(): string {
    if (this.capture.error) return this.capture.error;
    const text = this.errorBody.toString("utf8");
    try {
      const payload = JSON.parse(text) as { error?: { message?: unknown } };
      return typeof payload.error?.message === "string" ? payload.error.message : text;
    } catch {
      return text;
    }
  }

  abort(): void {
    this.finishTurn();
  }

  async settled(): Promise<void> {
    await Promise.allSettled([...this.active]);
  }

  async close(): Promise<void> {
    this.closed = true;
    this.abort();
    await new Promise<void>((resolve) => {
      if (!this.server.listening) return resolve();
      this.server.close(() => resolve());
    });
    await this.settled();
  }

  private async handle(request: http.IncomingMessage, response: http.ServerResponse): Promise<void> {
    const requestUrl = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method !== "POST" || requestUrl.pathname !== `${this.prefix}/v1/messages` || request.headers.origin) {
      response.writeHead(404).end();
      return;
    }

    if (this.cancelled || this.used) {
      this.denied += 1;
      const body = Buffer.from(
        JSON.stringify({
          type: "error",
          error: { type: "invalid_request_error", message: "PI_CLAUDE_CODE_TURN_RELAY_CONSUMED" },
        }),
      );
      response.writeHead(400, { "content-type": "application/json", "content-length": body.length }).end(body);
      return;
    }
    this.used = true;
    this.sockets.add(request);
    this.sockets.add(response);
    // Bind this request to the turn that accepted it; awaits below must not
    // pick up a later turn's transform, hook, or cancellation state.
    const token = this.turnToken;
    const transform = this.transformRequest;
    const turnOptions = this.turnOptions;

    let upstreamRequest: http.ClientRequest | undefined;
    try {
      let body = await this.readBody(request);
      this.originalRequestBody = Buffer.from(body);
      if (transform) body = await transform(body);
      try {
        body = await finalizeRequest(body, turnOptions);
      } catch (error) {
        if (token === this.turnToken) this.rejection = error instanceof Error ? error.message : String(error);
        throw error;
      }
      if (this.cancelled || token !== this.turnToken) return;
      this.requestBody = Buffer.from(body);

      const headers: http.OutgoingHttpHeaders = {};
      for (const [name, value] of Object.entries(request.headers)) {
        if (!STRIPPED_REQUEST_HEADERS.has(name.toLowerCase()) && value !== undefined) headers[name] = value;
      }
      headers["accept-encoding"] = "identity";
      headers["content-length"] = body.length;
      if (this.requestBetas.length > 0) {
        const existing = Array.isArray(headers["anthropic-beta"])
          ? headers["anthropic-beta"].join(",")
          : String(headers["anthropic-beta"] ?? "");
        headers["anthropic-beta"] = [
          ...new Set([
            ...existing.split(",").map((value) => value.trim()).filter(Boolean),
            ...this.requestBetas,
          ]),
        ].join(",");
      }

      const pathPrefix = this.upstream.pathname.replace(/\/$/u, "");
      const transport = this.upstream.protocol === "https:" ? https : http;
      upstreamRequest = transport.request({
        protocol: this.upstream.protocol,
        hostname: this.upstream.hostname,
        port: this.upstream.port || undefined,
        method: "POST",
        path: `${pathPrefix}/v1/messages${requestUrl.search}`,
        headers,
        timeout: this.timeoutMs,
      });
      this.sockets.add(upstreamRequest);

      const upstreamResponse = await new Promise<http.IncomingMessage>((resolve, reject) => {
        upstreamRequest!.once("response", resolve);
        upstreamRequest!.once("error", reject);
        upstreamRequest!.end(body);
      });
      this.sockets.add(upstreamResponse);
      this.status = upstreamResponse.statusCode;
      this.requestId =
        String(upstreamResponse.headers["request-id"] ?? upstreamResponse.headers["x-request-id"] ?? "") || undefined;

      const responseHeaders: http.OutgoingHttpHeaders = {};
      for (const [name, value] of Object.entries(upstreamResponse.headers)) {
        if (!STRIPPED_RESPONSE_HEADERS.has(name.toLowerCase()) && value !== undefined) responseHeaders[name] = value;
      }
      responseHeaders.connection = "close";
      response.writeHead(upstreamResponse.statusCode ?? 502, responseHeaders);

      for await (const rawChunk of upstreamResponse) {
        const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
        if (this.status === 200) this.capture.feed(chunk);
        else if (this.errorBody.length < 65_536) {
          this.errorBody = Buffer.concat([this.errorBody, chunk]).subarray(0, 65_536);
        }
        if (!response.write(chunk)) await this.waitForDrain(response);
      }
      if (this.status === 200) this.capture.finish();
      response.end();
    } catch (error) {
      this.failure = error instanceof Error ? error.name : "UnknownError";
      if (!response.headersSent) response.writeHead(this.cancelled ? 499 : 502).end();
      else response.destroy();
    } finally {
      if (upstreamRequest) this.sockets.delete(upstreamRequest);
      this.sockets.delete(request);
      this.sockets.delete(response);
      upstreamRequest?.destroy();
    }
  }

  private async waitForDrain(response: http.ServerResponse): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        response.off("drain", onDrain);
        response.off("close", onClose);
        response.off("error", onError);
      };
      const onDrain = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error("Claude Code disconnected from the turn relay"));
      };
      const onError = (error: Error) => {
        cleanup();
        reject(error);
      };
      response.once("drain", onDrain);
      response.once("close", onClose);
      response.once("error", onError);
    });
  }

  private async readBody(request: http.IncomingMessage): Promise<Buffer> {
    const chunks: Buffer[] = [];
    for await (const rawChunk of request) chunks.push(Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk));
    return Buffer.concat(chunks);
  }
}
