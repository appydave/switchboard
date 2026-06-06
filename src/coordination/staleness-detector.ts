/**
 * staleness-detector — coordination recipe (Coordination zone).
 *
 * The safety-net half of the "reaper": Switchboard DETECTS and TELLS, it never
 * kills. This recipe is strictly observer-only — it reads tmux + process state
 * and EMITS a `session.stale` Signal. Acting on that Signal (e.g. killing a
 * stuck window) is the job of an external daemon/operator, NOT this Sentinel.
 *
 * On a timer (default 30s, env STALE_CHECK_MS) it scans for tmux windows whose
 * name matches `swagger-*` and whose underlying `claude` process has been alive
 * longer than STALE_MINUTES (default 10). The premise: a healthy Swagger
 * self-closes quickly on success, so a long-lived one is likely stuck.
 *
 * The emitted `event` Signal rides the existing sse-deliver fan-out: a client
 * subscribed with `?subscribe=session.stale` receives one event per stale window.
 *
 * v1 heuristic only — wall-clock process age. No semantic liveness yet.
 * TODO(v2): also QUERY AngelEye for each session's `last_active` timestamp and
 * flag semantic staleness (process alive but no recent activity). AngelEye is
 * offline today, hence the wall-clock proxy here.
 */
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import type { Sentinel, SignalPayload } from '@appydave/appysentinel-core';

const execAsync = promisify(exec);

export interface StalenessDetectorOptions {
  /** Scan interval in milliseconds. Default: 30_000 (env STALE_CHECK_MS upstream). */
  intervalMs?: number;
  /** Age threshold in minutes past which a swagger window is "stale". Default: 10. */
  staleMinutes?: number;
  /** tmux window-name glob prefix to watch. Default: 'swagger-'. */
  windowPrefix?: string;
  /** Per-command timeout in ms. Default: 10_000. */
  timeoutMs?: number;
}

/** Payload of the emitted `session.stale` event Signal. */
export interface SessionStalePayload extends SignalPayload {
  /** Fully-qualified tmux window name (e.g. 'swagger-stale'). */
  window: string;
  /** PID of the underlying claude process. */
  pid: number;
  /** Seconds the claude process has been alive. */
  age_seconds: number;
  /** Human-readable explanation of why it was flagged. */
  reason: string;
}

interface ProcRow {
  pid: number;
  ppid: number;
  etimeSeconds: number;
  command: string;
}

/**
 * Parse ps `etime` ([[DD-]HH:]MM:SS) into seconds. macOS ps has no `etimes`.
 */
function parseEtime(etime: string): number {
  const trimmed = etime.trim();
  let days = 0;
  let rest = trimmed;
  const dash = rest.indexOf('-');
  if (dash !== -1) {
    days = Number(rest.slice(0, dash));
    rest = rest.slice(dash + 1);
  }
  const parts = rest.split(':').map(Number);
  let h = 0;
  let m = 0;
  let s = 0;
  if (parts.length === 3) [h, m, s] = parts as [number, number, number];
  else if (parts.length === 2) [m, s] = parts as [number, number];
  else if (parts.length === 1) [s] = parts as [number];
  return days * 86_400 + h * 3_600 + m * 60 + s;
}

/** Snapshot every process as pid → row, including elapsed time and full command. */
async function readProcesses(timeoutMs: number): Promise<Map<number, ProcRow>> {
  const { stdout } = await execAsync('ps -Ao pid=,ppid=,etime=,command=', {
    timeout: timeoutMs,
    maxBuffer: 4 * 1024 * 1024,
  });
  const map = new Map<number, ProcRow>();
  for (const line of stdout.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const m = trimmed.match(/^(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/);
    if (!m) continue;
    const pid = Number(m[1]);
    map.set(pid, {
      pid,
      ppid: Number(m[2]),
      etimeSeconds: parseEtime(m[3]!),
      command: m[4]!,
    });
  }
  return map;
}

/** swagger-* tmux windows as window-name → pane pid. */
async function readSwaggerPanes(
  prefix: string,
  timeoutMs: number
): Promise<Map<string, number>> {
  const windows = new Map<string, number>();
  try {
    const { stdout } = await execAsync(
      "tmux list-panes -a -F '#{window_name}\t#{pane_pid}'",
      { timeout: timeoutMs, maxBuffer: 4 * 1024 * 1024 }
    );
    for (const line of stdout.split('\n')) {
      const [name, pid] = line.split('\t');
      if (!name || !pid) continue;
      if (!name.startsWith(prefix)) continue;
      // First pane per window wins — the claude process lives under the window.
      if (!windows.has(name)) windows.set(name, Number(pid));
    }
  } catch {
    // No tmux server / no panes is normal — nothing to scan this tick.
  }
  return windows;
}

/**
 * Find the claude descendant of a pane pid. BFS the process tree (panes spawn a
 * shell which spawns claude) and return the first process whose command names
 * claude — excluding our own ps/grep noise.
 */
function findClaudeDescendant(
  panePid: number,
  procs: Map<number, ProcRow>
): ProcRow | undefined {
  const childrenOf = new Map<number, ProcRow[]>();
  for (const row of procs.values()) {
    const arr = childrenOf.get(row.ppid) ?? [];
    arr.push(row);
    childrenOf.set(row.ppid, arr);
  }
  const queue: number[] = [panePid];
  const seen = new Set<number>();
  while (queue.length) {
    const current = queue.shift()!;
    if (seen.has(current)) continue;
    seen.add(current);
    for (const child of childrenOf.get(current) ?? []) {
      if (/(^|\/|\s)claude(\s|$)/.test(child.command) || /\bclaude\b/.test(child.command)) {
        return child;
      }
      queue.push(child.pid);
    }
  }
  return undefined;
}

export function stalenessDetector(
  sentinel: Sentinel,
  options: StalenessDetectorOptions = {}
): void {
  const intervalMs = options.intervalMs ?? 30_000;
  const staleMinutes = options.staleMinutes ?? 10;
  const windowPrefix = options.windowPrefix ?? 'swagger-';
  const timeoutMs = options.timeoutMs ?? 10_000;
  const thresholdSeconds = staleMinutes * 60;

  let timer: ReturnType<typeof setInterval> | undefined;
  let running = false;

  const tick = async (): Promise<void> => {
    if (running) return; // skip overlapping scans
    running = true;
    try {
      const panes = await readSwaggerPanes(windowPrefix, timeoutMs);
      if (panes.size === 0) return;
      const procs = await readProcesses(timeoutMs);

      for (const [window, panePid] of panes) {
        const claude = findClaudeDescendant(panePid, procs);
        if (!claude) continue;
        if (claude.etimeSeconds <= thresholdSeconds) continue;

        // DETECT + TELL only. We never kill — an external reaper/operator acts.
        sentinel.emit<SessionStalePayload>({
          source: 'staleness-detector',
          kind: 'event',
          name: 'session.stale',
          attributes: { window, pid: claude.pid, age_seconds: claude.etimeSeconds },
          payload: {
            window,
            pid: claude.pid,
            age_seconds: claude.etimeSeconds,
            reason: `claude in tmux window '${window}' alive ${claude.etimeSeconds}s (> ${staleMinutes}m threshold); a healthy Swagger self-closes quickly, so this is likely stuck`,
          },
        });
      }
    } catch (err) {
      sentinel.logger.error({ err }, 'staleness-detector: tick failed');
    } finally {
      running = false;
    }
  };

  sentinel.lifecycle.onStart(async () => {
    await tick(); // scan once immediately on start
    timer = setInterval(() => void tick(), intervalMs);
    sentinel.logger.info(
      { intervalMs, staleMinutes, windowPrefix },
      'staleness-detector: started (observer-only; emits session.stale)'
    );
  });

  sentinel.lifecycle.onStop(async () => {
    if (timer) clearInterval(timer);
    timer = undefined;
  });
}
