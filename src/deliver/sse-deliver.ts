/**
 * sse-deliver — transport recipe (Deliver zone).
 *
 * Pushes Signals OUT over Server-Sent Events (SSE), topic-filtered, with a
 * durable event log so a reconnecting client replays missed events via the
 * standard `Last-Event-ID` header.
 *
 * Ported to TypeScript from the proven `synapse-probe` spike
 * (dark-factory/.../spikes/synapse-probe/server.py + PROOF.md). The spike
 * proved: SSE push + server-side topic subscription + monotonic event ids feed
 * a Claude Code `Monitor` directly — no filesystem in the hot path. The spike's
 * one open gap was durability (ephemeral SSE); this recipe closes it with an
 * append-only JSONL event log and `Last-Event-ID` replay.
 *
 * Endpoints (host-local by default):
 *   GET /sse?subscribe=a,b   open an event-stream; receive ONLY events whose
 *                            topic ∈ {a,b}. Empty/absent subscribe = firehose.
 *                            On (re)connect, replays buffered events with
 *                            id > Last-Event-ID (header or ?lastEventId=).
 *   GET /health              tiny JSON liveness probe.
 *
 * SSE frame: `id: <n>\nevent: <topic>\ndata: <json-signal>\n\n` where <n> is a
 * monotonic event id — the basis for replay.
 *
 * Deliver, not Collect/Access: this never reads or mutates observed systems; it
 * only fans the Sentinel's own Signal bus outward. Bind to 127.0.0.1 by default
 * (observer-only posture — no unsolicited exposure).
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { appendFile, mkdir, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { URL } from 'node:url';
import type { Sentinel, Signal } from '@appydave/appysentinel-core';

/** One persisted event = one delivered Signal, tagged with a monotonic id + topic. */
export interface SseEvent {
  /** Monotonic event id — the value a client echoes back as Last-Event-ID. */
  id: number;
  /** Topic this event was published under (server-side subscription key). */
  topic: string;
  /** The originating Signal's id (for de-dup / tracing). */
  signalId: string;
  /** ISO timestamp the event was logged. */
  ts: string;
  /** JSON-encoded Signal — the SSE `data:` body. */
  data: string;
}

export interface SseDeliverOptions {
  /** TCP port to listen on. */
  port: number;
  /** Bind address. Default '127.0.0.1' (host-local only). */
  host?: string;
  /** Path that opens the event-stream. Default '/sse'. */
  path?: string;
  /** Durable append-only event log. Default 'snapshots/sse-eventlog.jsonl'. */
  logPath?: string;
  /**
   * Maps a Signal to its topic string (the server-side subscription key).
   * Default: `signal.name` (e.g. 'process.snapshot', 'file.created').
   */
  topicOf?: (signal: Signal) => string;
  /** Predicate selecting which Signals to deliver. Default: deliver all. */
  match?: (signal: Signal) => boolean;
  /**
   * Max events held in memory for replay (and reloaded from the log tail on
   * start). Older events stay on disk but are not replayed. Default 1000.
   */
  replayBufferSize?: number;
  /** Keep-alive comment ping interval in ms. Default 25_000. 0 disables. */
  heartbeatMs?: number;
}

/** A live SSE subscriber: its topic filter + bound frame/comment writers. */
interface Subscriber {
  topics: Set<string>;
  /** Write one event as an SSE frame. */
  write: (event: SseEvent) => void;
  /** Write an SSE comment (keep-alive ping; never seen as an event by clients). */
  ping: () => void;
}

/** Format one event as an SSE frame. Multi-line data is split per spec. */
function frameOf(event: SseEvent): string {
  const dataLines = event.data
    .split('\n')
    .map((line) => `data: ${line}`)
    .join('\n');
  return `id: ${event.id}\nevent: ${event.topic}\n${dataLines}\n\n`;
}

/** Does an empty topic set mean firehose; else exact membership. */
function topicMatches(subTopics: Set<string>, topic: string): boolean {
  return subTopics.size === 0 || subTopics.has(topic);
}

export function sseDeliver(sentinel: Sentinel, options: SseDeliverOptions): void {
  const host = options.host ?? '127.0.0.1';
  const ssePath = options.path ?? '/sse';
  const logPath = resolve(options.logPath ?? 'snapshots/sse-eventlog.jsonl');
  const topicOf = options.topicOf ?? ((s: Signal) => s.name);
  const match = options.match ?? (() => true);
  const bufferSize = options.replayBufferSize ?? 1000;
  const heartbeatMs = options.heartbeatMs ?? 25_000;

  // In-memory replay ring (bounded) + monotonic counter. Reloaded from the log
  // tail on start so replay survives a Sentinel restart.
  const buffer: SseEvent[] = [];
  let counter = 0;
  const subscribers = new Set<Subscriber>();

  let server: Server | undefined;
  let unsubscribe: (() => void) | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  // Serialize appends so concurrent emits never interleave a partial line.
  let writeChain: Promise<void> = Promise.resolve();

  const pushToBuffer = (event: SseEvent): void => {
    buffer.push(event);
    if (buffer.length > bufferSize) buffer.shift();
  };

  /** Append one event to the durable log, serialized. Errors logged, not thrown. */
  const persist = (event: SseEvent): void => {
    writeChain = writeChain
      .then(() => appendFile(logPath, JSON.stringify(event) + '\n'))
      .catch((err) => sentinel.logger.error({ err, logPath }, 'sse-deliver: log append failed'));
  };

  /** Reload the buffer + counter from the existing log tail (durability on restart). */
  const reloadFromLog = async (): Promise<void> => {
    let raw: string;
    try {
      raw = await readFile(logPath, 'utf8');
    } catch {
      return; // no prior log — fresh start
    }
    const lines = raw.split('\n').filter(Boolean);
    for (const line of lines) {
      try {
        const event = JSON.parse(line) as SseEvent;
        if (typeof event.id === 'number' && event.id > counter) counter = event.id;
        pushToBuffer(event);
      } catch {
        // skip a torn/partial trailing line
      }
    }
    if (lines.length > 0) {
      sentinel.logger.info(
        { restored: buffer.length, nextId: counter + 1 },
        'sse-deliver: replay buffer restored from log'
      );
    }
  };

  /**
   * Publish one matching Signal to the bus subscribers + the durable log.
   * Synchronous through counter bump, buffer push, and fan-out — Node's
   * single-threaded model means no live event can slip between a connecting
   * client's replay snapshot and its live subscription (see handleSse).
   */
  const publish = (signal: Signal): void => {
    const topic = topicOf(signal);
    counter += 1;
    const event: SseEvent = {
      id: counter,
      topic,
      signalId: signal.id,
      ts: new Date().toISOString(),
      data: JSON.stringify(signal),
    };
    pushToBuffer(event);
    persist(event);
    for (const sub of subscribers) {
      if (topicMatches(sub.topics, topic)) sub.write(event);
    }
  };

  const handleSse = (req: IncomingMessage, res: ServerResponse, url: URL): void => {
    const rawSubscribe = url.searchParams.get('subscribe') ?? '';
    const topics = new Set(rawSubscribe.split(',').filter(Boolean));

    // Last-Event-ID: browser EventSource sends a header on auto-reconnect;
    // curl/manual clients can pass ?lastEventId= instead.
    const headerId = req.headers['last-event-id'];
    const queryId = url.searchParams.get('lastEventId');
    const rawLastId = (Array.isArray(headerId) ? headerId[0] : headerId) ?? queryId ?? '';
    const lastId = Number.parseInt(rawLastId, 10);
    const replayFrom = Number.isFinite(lastId) ? lastId : 0;

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    // Retry hint for reconnecting EventSource clients (ms).
    res.write('retry: 3000\n\n');

    const sub: Subscriber = {
      topics,
      write: (event: SseEvent) => {
        try {
          res.write(frameOf(event));
        } catch {
          // socket gone — disconnect handler will clean up
        }
      },
      ping: () => {
        try {
          res.write(': ping\n\n');
        } catch {
          // socket gone — disconnect handler will clean up
        }
      },
    };

    // Replay snapshot + live registration in one synchronous block: any event
    // published after this returns is delivered live; anything <= the snapshot
    // is in the replay. No gap, no duplicate.
    const replay = buffer.filter((e) => e.id > replayFrom && topicMatches(topics, e.topic));
    for (const event of replay) sub.write(event);
    subscribers.add(sub);

    sentinel.logger.info(
      {
        topics: [...topics],
        replayFrom,
        replayed: replay.length,
        subscribers: subscribers.size,
      },
      'sse-deliver: client subscribed'
    );

    const cleanup = (): void => {
      subscribers.delete(sub);
    };
    req.on('close', cleanup);
    res.on('error', cleanup);
  };

  const handler = (req: IncomingMessage, res: ServerResponse): void => {
    const url = new URL(req.url ?? '/', `http://${host}`);
    if (req.method === 'GET' && url.pathname === ssePath) {
      handleSse(req, res, url);
      return;
    }
    if (req.method === 'GET' && url.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: 'ok',
          subscribers: subscribers.size,
          lastEventId: counter,
          buffered: buffer.length,
        })
      );
      return;
    }
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found');
  };

  sentinel.lifecycle.onStart(async () => {
    await mkdir(dirname(logPath), { recursive: true });
    await reloadFromLog();

    unsubscribe = sentinel.on((signal) => {
      if (!match(signal)) return;
      publish(signal);
    });

    server = createServer(handler);
    await new Promise<void>((resolveListen, reject) => {
      server!.once('error', reject);
      server!.listen(options.port, host, () => {
        server!.off('error', reject);
        resolveListen();
      });
    });

    if (heartbeatMs > 0) {
      heartbeat = setInterval(() => {
        for (const sub of subscribers) sub.ping();
      }, heartbeatMs);
      heartbeat.unref?.();
    }

    sentinel.logger.info(
      { url: `http://${host}:${options.port}${ssePath}`, logPath },
      'sse-deliver: started'
    );
  });

  sentinel.lifecycle.onStop(async () => {
    if (heartbeat) clearInterval(heartbeat);
    heartbeat = undefined;
    unsubscribe?.();
    unsubscribe = undefined;
    subscribers.clear();
    if (server) {
      await new Promise<void>((resolveClose) => server!.close(() => resolveClose()));
      server = undefined;
    }
  });
}
