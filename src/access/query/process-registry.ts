/**
 * process-registry — query layer (Access / read side, CQRS-lite).
 *
 * Pure read logic over the snapshot file written by `snapshot-store`. No
 * transport knowledge — returns `QueryResult<T>` with first-class freshness
 * metadata (data_age_ms / stale) so agents can decide whether to recollect.
 */
import { readFile } from 'node:fs/promises';
import type { QueryResult, Signal } from '@appydave/appysentinel-core';
import type {
  CommandResult,
  ProcessSnapshotPayload,
} from '../../collect/poll-command.js';

export type { CommandResult, ProcessSnapshotPayload };

/** Above this age (ms) the snapshot is reported stale. */
export const DEFAULT_STALE_MS = 30_000;

export interface ReadOptions {
  staleThresholdMs?: number;
  /** Override clock (ms epoch). For testing. */
  now?: number;
}

/**
 * Read the latest process/session snapshot. Returns `null` if no snapshot has
 * been written yet (Sentinel not started, or first tick not landed).
 */
export async function readProcessRegistry(
  snapshotPath: string,
  opts: ReadOptions = {}
): Promise<QueryResult<ProcessSnapshotPayload> | null> {
  let raw: string;
  try {
    raw = await readFile(snapshotPath, 'utf8');
  } catch {
    return null;
  }

  const signal = JSON.parse(raw) as Signal<ProcessSnapshotPayload>;
  const generatedAt = signal.ts;
  const now = opts.now ?? Date.now();
  const dataAgeMs = Math.max(0, now - Date.parse(generatedAt));
  const stale = dataAgeMs > (opts.staleThresholdMs ?? DEFAULT_STALE_MS);

  return {
    data: signal.payload,
    generated_at: generatedAt,
    data_age_ms: dataAgeMs,
    stale,
  };
}

/** A live `claude` process discovered via `pgrep -fl claude`. */
export interface ClaudeSession {
  pid: number;
  command: string;
}

/** Parse claude sessions from a `pgrep -fl claude`-style command result. */
export function parseClaudeSessions(payload: ProcessSnapshotPayload): ClaudeSession[] {
  const result = payload.results['claude_pids'];
  if (!result || !result.stdout.trim()) return [];
  return result.stdout
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const space = line.indexOf(' ');
      const pid = Number.parseInt(space === -1 ? line : line.slice(0, space), 10);
      const command = space === -1 ? '' : line.slice(space + 1).trim();
      return { pid, command };
    })
    .filter((s) => Number.isFinite(s.pid));
}

/** Summary view — counts and freshness only. */
export interface RegistrySummary {
  generated_at: string;
  data_age_seconds: number;
  stale: boolean;
  command_count: number;
  claude_session_count: number;
}

export function summarize(result: QueryResult<ProcessSnapshotPayload>): RegistrySummary {
  return {
    generated_at: result.generated_at,
    data_age_seconds: Math.round(result.data_age_ms / 1000),
    stale: result.stale,
    command_count: Object.keys(result.data.results).length,
    claude_session_count: parseClaudeSessions(result.data).length,
  };
}
