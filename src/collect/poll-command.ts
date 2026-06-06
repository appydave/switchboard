/**
 * poll-command — input collector (Collect zone).
 *
 * Runs a set of read-only shell commands on a timer and emits one combined
 * `state` Signal per tick: a live registry of process + session state. Pairs
 * with `snapshot-store` (state kind → one overwriting snapshot).
 *
 * Observer-only: the default commands (ps / tmux list-windows / pgrep claude)
 * only read system state. Never wire a mutating command here.
 */
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import type { Sentinel, SignalPayload } from '@appydave/appysentinel-core';

const execAsync = promisify(exec);

/** One command to run each tick. `key` names its slot in the snapshot payload. */
export interface PollCommandSpec {
  key: string;
  command: string;
}

export interface PollCommandOptions {
  /** Poll interval in milliseconds. */
  interval: number;
  /** Commands to run each tick. */
  commands: PollCommandSpec[];
  /** Semantic name for the emitted state Signal. Default: 'process.snapshot'. */
  signalName?: string;
  /** Per-command timeout in ms. Default: 10_000. */
  timeoutMs?: number;
  /** Max stdout/stderr buffer per command in bytes. Default: 4 MiB. */
  maxBuffer?: number;
}

/** Result of running a single command in a tick. */
export interface CommandResult {
  command: string;
  /** Process exit code. 0 = success; non-zero is captured, not thrown. */
  exitCode: number;
  stdout: string;
  stderr: string;
  durationMs: number;
  /** Set only when the command failed to spawn or timed out. */
  error?: string;
}

/** Payload of the emitted `state` Signal. */
export interface ProcessSnapshotPayload extends SignalPayload {
  captured_at: string;
  results: Record<string, CommandResult>;
}

async function runOne(
  spec: PollCommandSpec,
  timeoutMs: number,
  maxBuffer: number
): Promise<CommandResult> {
  const started = Date.now();
  try {
    const { stdout, stderr } = await execAsync(spec.command, {
      timeout: timeoutMs,
      maxBuffer,
    });
    return {
      command: spec.command,
      exitCode: 0,
      stdout: stdout.toString(),
      stderr: stderr.toString(),
      durationMs: Date.now() - started,
    };
  } catch (err) {
    // exec rejects on non-zero exit / timeout. Capture rather than throw —
    // a tmux server that isn't running or pgrep with no matches is normal.
    const e = err as { code?: number | string; stdout?: string; stderr?: string; message?: string };
    return {
      command: spec.command,
      exitCode: typeof e.code === 'number' ? e.code : 1,
      stdout: (e.stdout ?? '').toString(),
      stderr: (e.stderr ?? '').toString(),
      durationMs: Date.now() - started,
      error: typeof e.code === 'number' ? undefined : (e.message ?? 'command failed'),
    };
  }
}

export function pollCommand(sentinel: Sentinel, options: PollCommandOptions): void {
  const signalName = options.signalName ?? 'process.snapshot';
  const timeoutMs = options.timeoutMs ?? 10_000;
  const maxBuffer = options.maxBuffer ?? 4 * 1024 * 1024;
  let timer: ReturnType<typeof setInterval> | undefined;
  let running = false;

  const tick = async (): Promise<void> => {
    // Skip overlapping ticks if a previous run is still in flight.
    if (running) return;
    running = true;
    try {
      const settled = await Promise.all(
        options.commands.map((spec) => runOne(spec, timeoutMs, maxBuffer))
      );
      const results: Record<string, CommandResult> = {};
      options.commands.forEach((spec, i) => {
        results[spec.key] = settled[i]!;
      });
      sentinel.emit<ProcessSnapshotPayload>({
        source: 'poll-command',
        kind: 'state',
        name: signalName,
        attributes: { command_count: options.commands.length },
        payload: { captured_at: new Date().toISOString(), results },
      });
    } catch (err) {
      sentinel.logger.error({ err }, 'poll-command: tick failed');
    } finally {
      running = false;
    }
  };

  sentinel.lifecycle.onStart(async () => {
    await tick(); // emit an initial snapshot immediately on start
    timer = setInterval(() => void tick(), options.interval);
    sentinel.logger.info(
      { interval: options.interval, commands: options.commands.map((c) => c.key) },
      'poll-command: started'
    );
  });

  sentinel.lifecycle.onStop(async () => {
    if (timer) clearInterval(timer);
    timer = undefined;
  });
}
