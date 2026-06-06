/**
 * api-binding — Access binding (thin protocol adapter, write side / command).
 *
 * Inbound HTTP ingest for job tickets. A client POSTs a job ticket as JSON and
 * this binding publishes it onto the Sentinel's Signal bus as an `event` Signal
 * named `job.queued`, with the ticket as payload. Because it rides the SAME bus
 * the live Sentinel emits on, the existing `sse-deliver` recipe fans the queued
 * job straight out to any SSE client subscribed with `?subscribe=job.queued` —
 * no new transport, no second event path. This binding only emits; sse-deliver
 * delivers.
 *
 * Unlike `mcp-binding` (a standalone stdio process, read side), this is wired
 * into `src/main.ts` because it needs the live `sentinel.emit` path — its whole
 * job is to inject Signals into the running bus.
 *
 * Durability: the bus is broadcast/replay, NOT exactly-once — it is only the
 * WAKE. So in addition to emitting, POST /jobs FIRST atomically persists the
 * ticket to `queue/<queue_id>.json` (the exactly-once source of truth), THEN
 * emits the wake. A consumer later claims a ticket by atomically renaming its
 * file out of the queue dir (watchtower-engine claim-next.sh pattern).
 *
 * Endpoints (host-local by default):
 *   POST /jobs      body: { queue_id, kind, prompt?, workflow?, args? }
 *                   → 202 { accepted: true, signalId, name: 'job.queued', queued }
 *                   → 400 { error, message } on a malformed ticket.
 *                   → 500 { error, message } if the durable write fails (no wake).
 *   GET  /health    tiny JSON liveness probe.
 *
 * CQRS-lite: this is the Command side of Access — it accepts work into the
 * Sentinel's own bus. It never mutates an observed system, only the Sentinel's
 * own queue stream. Bind to 127.0.0.1 by default (observer-only posture).
 */
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { serve, type ServerType } from '@hono/node-server';
import { Hono } from 'hono';
import { atomicWrite, type Sentinel, type Signal } from '@appydave/appysentinel-core';

/** The Signal name every queued job rides under. SSE topic = this string. */
export const JOB_QUEUED = 'job.queued';

/** A job ticket — the POST body and the `job.queued` Signal payload. */
export interface JobTicket {
  /** Logical queue this job belongs to. */
  queue_id: string;
  /** Job kind discriminator (e.g. 'prompt', 'workflow'). */
  kind: string;
  /** Free-form prompt for prompt-kind jobs. */
  prompt?: string;
  /** Named workflow for workflow-kind jobs. */
  workflow?: string;
  /** Arbitrary workflow/prompt arguments. */
  args?: unknown;
}

export interface ApiBindingOptions {
  /** TCP port to listen on. MUST differ from the sse-deliver port. */
  port: number;
  /** Bind address. Default '127.0.0.1' (host-local only). */
  host?: string;
  /** Path that accepts job tickets. Default '/jobs'. */
  path?: string;
  /**
   * Durable, claimable queue directory. Each accepted ticket is atomically
   * written here as `<queue_id>.json` BEFORE the wake is emitted. Default
   * 'queue'. This dir is the exactly-once source of truth — the bus is only
   * the wake (broadcast/replay, not exactly-once). A consumer claims a ticket
   * by atomically renaming its file out of this dir (watchtower-engine
   * claim-next.sh pattern). Gitignored runtime state.
   */
  queueDir?: string;
}

/** A validated ticket, or a reason it was rejected. */
type ValidationResult =
  | { ok: true; ticket: JobTicket }
  | { ok: false; message: string };

/** Validate an untrusted POST body into a JobTicket. Pure — no side effects. */
export function validateTicket(body: unknown): ValidationResult {
  if (typeof body !== 'object' || body === null) {
    return { ok: false, message: 'body must be a JSON object' };
  }
  const b = body as Record<string, unknown>;
  if (typeof b['queue_id'] !== 'string' || b['queue_id'].length === 0) {
    return { ok: false, message: 'queue_id is required and must be a non-empty string' };
  }
  // queue_id becomes a filename in the durable queue dir — reject anything that
  // could escape it (path separators, traversal, NUL).
  if (/[/\\\0]/.test(b['queue_id']) || b['queue_id'].includes('..')) {
    return { ok: false, message: 'queue_id must not contain path separators or ".."' };
  }
  if (typeof b['kind'] !== 'string' || b['kind'].length === 0) {
    return { ok: false, message: 'kind is required and must be a non-empty string' };
  }
  if (b['prompt'] !== undefined && typeof b['prompt'] !== 'string') {
    return { ok: false, message: 'prompt, when present, must be a string' };
  }
  if (b['workflow'] !== undefined && typeof b['workflow'] !== 'string') {
    return { ok: false, message: 'workflow, when present, must be a string' };
  }
  const ticket: JobTicket = {
    queue_id: b['queue_id'],
    kind: b['kind'],
    ...(b['prompt'] !== undefined ? { prompt: b['prompt'] as string } : {}),
    ...(b['workflow'] !== undefined ? { workflow: b['workflow'] as string } : {}),
    ...(b['args'] !== undefined ? { args: b['args'] } : {}),
  };
  return { ok: true, ticket };
}

/**
 * A durable, claimable queue record: the ticket plus the id of the wake Signal
 * that announced it. Written atomically to `<queueDir>/<queue_id>.json`.
 */
export interface QueuedTicketRecord extends JobTicket {
  /** Id of the `job.queued` wake Signal correlated with this ticket. */
  signalId: string;
}

/**
 * Persist a ticket to the durable, claimable queue dir as
 * `<queueDir>/<queue_id>.json`, atomically (temp-file + rename via
 * `atomicWrite`) so a crash never leaves a torn record. This file — not the
 * bus — is the exactly-once source of truth; the bus is only the wake. A
 * consumer claims a ticket by atomically renaming its file out of the queue
 * dir (watchtower-engine claim-next.sh pattern).
 *
 * Returns the path written. Caller persists BEFORE emitting the wake, so a
 * consumer woken by the bus is guaranteed to find the durable ticket.
 */
export async function persistJobTicket(
  queueDir: string,
  ticket: JobTicket,
  signalId: string
): Promise<string> {
  const filePath = join(queueDir, `${ticket.queue_id}.json`);
  const record: QueuedTicketRecord = { ...ticket, signalId };
  await atomicWrite(filePath, `${JSON.stringify(record, null, 2)}\n`, { encoding: 'utf8' });
  return filePath;
}

/**
 * The publish path, isolated for testing: mint + emit a `job.queued` Signal
 * onto the live bus. Whatever the Sentinel emits, sse-deliver delivers — so
 * this is the single seam between HTTP ingest and SSE fan-out. Pass `id` to
 * correlate the wake with an already-persisted durable ticket record.
 */
export function publishJobTicket(sentinel: Sentinel, ticket: JobTicket, id?: string): Signal {
  return sentinel.emit({
    source: 'api-binding',
    kind: 'event',
    name: JOB_QUEUED,
    payload: ticket,
    attributes: { queue_id: ticket.queue_id, kind: ticket.kind },
    ...(id !== undefined ? { id } : {}),
  });
}

export function apiBinding(sentinel: Sentinel, options: ApiBindingOptions): void {
  const host = options.host ?? '127.0.0.1';
  const jobsPath = options.path ?? '/jobs';
  const queueDir = options.queueDir ?? 'queue';

  let server: ServerType | undefined;

  const app = new Hono();

  app.post(jobsPath, async (c) => {
    let body: unknown;
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'bad_request', message: 'body must be valid JSON' }, 400);
    }
    const result = validateTicket(body);
    if (!result.ok) {
      return c.json({ error: 'invalid_ticket', message: result.message }, 400);
    }
    // Durable FIRST, wake SECOND. Pre-mint the wake id, persist the ticket to
    // the claimable queue dir (exactly-once source of truth), then emit the
    // bus wake carrying that same id. If the durable write fails we never wake
    // a consumer for a ticket it can't find.
    const signalId = randomUUID();
    let queuePath: string;
    try {
      queuePath = await persistJobTicket(queueDir, result.ticket, signalId);
    } catch (err) {
      sentinel.logger.error(
        { queue_id: result.ticket.queue_id, err: (err as Error).message },
        'api-binding: failed to persist job ticket'
      );
      return c.json({ error: 'persist_failed', message: 'could not durably enqueue ticket' }, 500);
    }
    const signal = publishJobTicket(sentinel, result.ticket, signalId);
    sentinel.logger.info(
      {
        queue_id: result.ticket.queue_id,
        kind: result.ticket.kind,
        signalId: signal.id,
        queuePath,
      },
      'api-binding: job queued'
    );
    return c.json({ accepted: true, signalId: signal.id, name: JOB_QUEUED, queued: queuePath }, 202);
  });

  app.get('/health', (c) =>
    c.json({ status: 'ok', accepts: jobsPath, emits: JOB_QUEUED })
  );

  sentinel.lifecycle.onStart(async () => {
    // Ensure the durable queue dir exists before atomicWrite (temp-file +
    // rename needs the destination directory present).
    await mkdir(queueDir, { recursive: true });
    await new Promise<void>((resolveListen) => {
      server = serve({ fetch: app.fetch, port: options.port, hostname: host }, () =>
        resolveListen()
      );
    });
    sentinel.logger.info(
      { url: `http://${host}:${options.port}${jobsPath}`, emits: JOB_QUEUED, queueDir },
      'api-binding: started'
    );
  });

  sentinel.lifecycle.onStop(async () => {
    if (server) {
      await new Promise<void>((resolveClose) => server!.close(() => resolveClose()));
      server = undefined;
    }
  });
}
