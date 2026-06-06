/**
 * switchboard — AppySentinel entry point.
 *
 * This is the minimal walking-skeleton scaffold. It boots a Sentinel,
 * subscribes a console logger, emits a single startup Signal, and then
 * sits waiting for SIGINT/SIGTERM.
 *
 * Build your Sentinel by adding collectors, storage, interfaces, and
 * transports. Run `claude` inside this project to start building.
 */

import { createSentinel } from '@appydave/appysentinel-core';
import { hostname } from 'node:os';
import { pollCommand } from './collect/poll-command.js';
import { snapshotStore } from './storage/snapshot-store.js';
import { sseDeliver } from './deliver/sse-deliver.js';

const sentinel = createSentinel({
  name: 'switchboard',
  machine: process.env['MACHINE_NAME'] ?? hostname(),
});

// Collect: poll live process + session state on a timer (read-only).
pollCommand(sentinel, {
  interval: 15_000,
  commands: [
    { key: 'processes', command: 'ps -Ao pid,ppid,user,pcpu,pmem,etime,comm' },
    {
      key: 'tmux_windows',
      command:
        "tmux list-windows -a -F '#{session_name}:#{window_index} #{window_name} active=#{window_active}'",
    },
    { key: 'claude_pids', command: 'pgrep -fl claude' },
  ],
});

// Storage: persist the latest state snapshot for read-side consumers.
snapshotStore(sentinel, { path: 'snapshots/sentinel-latest.json' });

// Deliver: push Signals OUT over topic-filtered SSE with a durable event log.
// Reconnecting clients (incl. a Claude Code Monitor) replay via Last-Event-ID.
// Topic = signal.name; subscribe with `GET /sse?subscribe=process.snapshot`.
// Host-local by default; override port via SSE_PORT.
sseDeliver(sentinel, {
  port: Number(process.env['SSE_PORT'] ?? 5099),
  logPath: 'snapshots/sse-eventlog.jsonl',
});

// Access: the MCP binding (src/access/bindings/mcp-binding.ts) is a STANDALONE
// stdio entry point spawned by an agent's MCP client — it reads the snapshot
// above and is intentionally NOT wired here (stdio would collide with logging).

// Default subscriber: log every Signal to the Pino logger.
sentinel.on((signal) => {
  sentinel.logger.info(
    {
      kind: signal.kind,
      name: signal.name,
      source: signal.source,
      payload: signal.payload,
    },
    'signal'
  );
});

await sentinel.start();

sentinel.emit({
  source: 'lifecycle',
  kind: 'event',
  name: 'sentinel.started',
  payload: { sentinelId: sentinel.sentinelId, machine: sentinel.machine },
});

sentinel.logger.info('switchboard is running. Press Ctrl-C to stop.');
