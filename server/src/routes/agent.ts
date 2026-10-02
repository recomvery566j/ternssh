import { Hono } from "hono";
import { getServer } from "../db/servers";
import { jsonError } from "../lib/http";
import type { Variables } from "../types";

export const agentRoutes = new Hono<{
  Bindings: Env;
  Variables: Variables;
}>();

const DEFAULT_TIMEOUT_MS = 15_000;
const MIN_TIMEOUT_MS = 1_000;
const MAX_TIMEOUT_MS = 60_000;
const MAX_COMMAND_LENGTH = 8_192;

function parseTimeoutMs(raw: unknown): number {
  if (typeof raw !== "number" || !Number.isFinite(raw)) {
    return DEFAULT_TIMEOUT_MS;
  }

  const value = Math.trunc(raw);
  if (value < MIN_TIMEOUT_MS) return MIN_TIMEOUT_MS;
  if (value > MAX_TIMEOUT_MS) return MAX_TIMEOUT_MS;
  return value;
}

// POST /api/v1/agent/exec
// Stateless single-shot command execution for AI agents.
// Runs on the exec-only SSH channel (no PTY, no xterm traffic), see
// server/src/ssh/session.ts -> execCommand().
agentRoutes.post("/exec", async (c) => {
  const user = c.get("user");

  let body: { node_id?: unknown; command?: unknown; timeout_ms?: unknown };
  try {
    body = await c.req.json();
  } catch {
    return jsonError(c, 400, "invalid JSON body");
  }

  const nodeId = typeof body.node_id === "string" ? body.node_id.trim() : "";
  if (!nodeId) {
    return jsonError(c, 400, "node_id is required");
  }

  const command = typeof body.command === "string" ? body.command : "";
  if (!command.trim()) {
    return jsonError(c, 400, "command is required");
  }
  if (command.length > MAX_COMMAND_LENGTH) {
    return jsonError(c, 400, `command too long (max ${MAX_COMMAND_LENGTH})`);
  }

  const server = await getServer(c.env.DB, user.id, nodeId);
  if (!server) {
    return jsonError(c, 404, "server not found");
  }

  const doId = c.env.SSH_SESSION.idFromName(`agent:${user.id}:${server.id}`);
  const stub = c.env.SSH_SESSION.get(doId);

  return stub.fetch(
    new Request("https://ssh-session.internal/agent/exec", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        userId: user.id,
        serverId: server.id,
        command,
        timeoutMs: parseTimeoutMs(body.timeout_ms),
      }),
    }),
  );
});

// ---------------------------------------------------------------------------
// File transfer (SFTP)
//
// Same authentication and node lookup as /exec. The Durable Object opens a
// dedicated SFTP channel on the agent session and drives it through
// SFTPHandler's direct-result methods.
//
// Bytes always cross to the DO as base64, so the DO only ever deals with one
// representation. "utf8" is offered to callers purely for convenience because
// text is far easier to read and hand-write; it is converted here.
// ---------------------------------------------------------------------------

const DEFAULT_MAX_FILE_BYTES = 4 * 1024 * 1024;
const HARD_MAX_FILE_BYTES = 32 * 1024 * 1024;

type FileEncoding = "utf8" | "base64";

function parseEncoding(raw: unknown): FileEncoding {
  return raw === "base64" ? "base64" : "utf8";
}

function parseMaxBytes(raw: unknown): number {
  if (typeof raw !== "number" || !Number.isFinite(raw)) {
    return DEFAULT_MAX_FILE_BYTES;
  }
  const value = Math.trunc(raw);
  if (value < 1) return 1;
  if (value > HARD_MAX_FILE_BYTES) return HARD_MAX_FILE_BYTES;
  return value;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 8_192;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    // Chunked so a large file does not blow the argument limit of apply().
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    out[i] = binary.charCodeAt(i);
  }
  return out;
}

/** Resolve {node_id, path} into a DO stub, or return an error Response. */
async function resolveSftpTarget(
  c: any,
  nodeIdRaw: unknown,
  pathRaw: unknown,
): Promise<{ stub: DurableObjectStub; userId: string; serverId: string; path: string } | Response> {
  const nodeId = typeof nodeIdRaw === "string" ? nodeIdRaw.trim() : "";
  if (!nodeId) {
    return jsonError(c, 400, "node_id is required");
  }

  const path = typeof pathRaw === "string" ? pathRaw.trim() : "";
  if (!path) {
    return jsonError(c, 400, "path is required");
  }

  const user = c.get("user");
  const server = await getServer(c.env.DB, user.id, nodeId);
  if (!server) {
    return jsonError(c, 404, "server not found");
  }

  const doId = c.env.SSH_SESSION.idFromName(`agent:${user.id}:${server.id}`);

  return {
    stub: c.env.SSH_SESSION.get(doId),
    userId: user.id,
    serverId: server.id,
    path,
  };
}

// POST /api/v1/agent/sftp/list  { node_id, path }
agentRoutes.post("/sftp/list", async (c) => {
  let body: { node_id?: unknown; path?: unknown };
  try {
    body = await c.req.json();
  } catch {
    return jsonError(c, 400, "invalid JSON body");
  }

  const target = await resolveSftpTarget(c, body.node_id, body.path);
  if (target instanceof Response) return target;

  return target.stub.fetch(
    new Request("https://ssh-session.internal/agent/sftp/list", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        userId: target.userId,
        serverId: target.serverId,
        path: target.path,
      }),
    }),
  );
});

// POST /api/v1/agent/sftp/read  { node_id, path, encoding?, max_bytes? }
agentRoutes.post("/sftp/read", async (c) => {
  let body: { node_id?: unknown; path?: unknown; encoding?: unknown; max_bytes?: unknown };
  try {
    body = await c.req.json();
  } catch {
    return jsonError(c, 400, "invalid JSON body");
  }

  const target = await resolveSftpTarget(c, body.node_id, body.path);
  if (target instanceof Response) return target;

  return target.stub.fetch(
    new Request("https://ssh-session.internal/agent/sftp/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        userId: target.userId,
        serverId: target.serverId,
        path: target.path,
        encoding: parseEncoding(body.encoding),
        maxBytes: parseMaxBytes(body.max_bytes),
      }),
    }),
  );
});

// POST /api/v1/agent/sftp/write  { node_id, path, content, encoding? }
agentRoutes.post("/sftp/write", async (c) => {
  let body: { node_id?: unknown; path?: unknown; content?: unknown; encoding?: unknown };
  try {
    body = await c.req.json();
  } catch {
    return jsonError(c, 400, "invalid JSON body");
  }

  const target = await resolveSftpTarget(c, body.node_id, body.path);
  if (target instanceof Response) return target;

  if (typeof body.content !== "string") {
    return jsonError(c, 400, "content is required");
  }

  const encoding = parseEncoding(body.encoding);

  let payload: Uint8Array;
  try {
    payload = encoding === "base64"
      ? base64ToBytes(body.content)
      : new TextEncoder().encode(body.content);
  } catch {
    // Fail fast here rather than letting a malformed payload surface as a
    // confusing transport error from deeper down.
    return jsonError(c, 400, "content is not valid base64");
  }

  return target.stub.fetch(
    new Request("https://ssh-session.internal/agent/sftp/write", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        userId: target.userId,
        serverId: target.serverId,
        path: target.path,
        contentBase64: bytesToBase64(payload),
      }),
    }),
  );
});
