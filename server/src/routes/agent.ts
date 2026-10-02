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
