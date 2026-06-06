/**
 * mcp-binding — Access binding (thin protocol adapter, read side).
 *
 * Standalone MCP server (stdio) that lets agents query "what is running" from
 * the snapshot the live Sentinel writes. It is read-only and owns no logic —
 * it routes to the `process-registry` query layer.
 *
 * Why standalone (not wired into src/main.ts): an MCP stdio transport owns the
 * process's stdin/stdout, which would collide with the always-on Sentinel's
 * Pino logging. The MCP client (e.g. an agent) spawns THIS file on demand:
 *
 *   bun src/access/bindings/mcp-binding.ts
 *
 * It reads the same snapshots/sentinel-latest.json the launchd Sentinel writes.
 * Configure SNAPSHOT_PATH to point at the file if it differs from the default.
 *
 * Every tool response carries a first-class `data_age_seconds` field so agents
 * can decide whether to trigger a fresh collection.
 */
import { resolve } from 'node:path';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  readProcessRegistry,
  parseClaudeSessions,
  summarize,
} from '../query/process-registry.js';

const SNAPSHOT_PATH = resolve(
  process.env['SNAPSHOT_PATH'] ?? 'snapshots/sentinel-latest.json'
);

function asText(value: unknown) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(value, null, 2) }] };
}

const NOT_READY = {
  error: 'no_snapshot',
  message:
    'No snapshot found yet. Ensure the switchboard Sentinel is running (launchd) and has completed at least one poll tick.',
};

const server = new McpServer({ name: 'switchboard-registry', version: '0.1.0' });

// Summary tool — counts + freshness.
server.tool(
  'get_status',
  'Summary of what is running on this machine: command count, claude session count, and snapshot freshness (data_age_seconds).',
  async () => {
    const result = await readProcessRegistry(SNAPSHOT_PATH);
    if (!result) return asText(NOT_READY);
    return asText(summarize(result));
  }
);

// Detail tool — the full process/session registry.
server.tool(
  'get_process_registry',
  'Full live registry: raw output of every polled command (ps / tmux / pgrep) plus data_age_seconds and a stale flag.',
  async () => {
    const result = await readProcessRegistry(SNAPSHOT_PATH);
    if (!result) return asText(NOT_READY);
    return asText({
      generated_at: result.generated_at,
      data_age_seconds: Math.round(result.data_age_ms / 1000),
      stale: result.stale,
      captured_at: result.data.captured_at,
      results: result.data.results,
    });
  }
);

// Domain-specific tool — live claude sessions.
server.tool(
  'list_claude_sessions',
  'List live `claude` processes (pid + command line) discovered on this machine, with snapshot freshness.',
  async () => {
    const result = await readProcessRegistry(SNAPSHOT_PATH);
    if (!result) return asText(NOT_READY);
    return asText({
      generated_at: result.generated_at,
      data_age_seconds: Math.round(result.data_age_ms / 1000),
      stale: result.stale,
      sessions: parseClaudeSessions(result.data),
    });
  }
);

const transport = new StdioServerTransport();
await server.connect(transport);
// Note: do not write to stdout here — the transport owns it. Diagnostics go to stderr.
process.stderr.write(`[mcp-binding] serving registry from ${SNAPSHOT_PATH}\n`);
