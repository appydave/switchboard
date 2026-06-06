/**
 * snapshot-store — storage recipe (sits between Collect and Access).
 *
 * Subscribes to the Signal bus and persists the latest matching Signal to a
 * single overwriting JSON file via AtomicWrite. Consumers do one JSON.parse —
 * no cursors, no pagination. Convention: snapshots/sentinel-latest.json.
 *
 * Rule (CLAUDE.md): `state` kind → one overwriting snapshot. This store keeps
 * only the most recent matching Signal.
 */
import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { atomicWrite } from '@appydave/appysentinel-core';
import type { Sentinel, Signal } from '@appydave/appysentinel-core';

export interface SnapshotStoreOptions {
  /** Destination path. Convention: 'snapshots/sentinel-latest.json'. */
  path: string;
  /**
   * Predicate selecting which Signals to persist. Default: state-kind signals.
   */
  match?: (signal: Signal) => boolean;
}

export function snapshotStore(sentinel: Sentinel, options: SnapshotStoreOptions): void {
  const filePath = resolve(options.path);
  const match = options.match ?? ((s: Signal) => s.kind === 'state');
  let unsubscribe: (() => void) | undefined;

  sentinel.lifecycle.onStart(async () => {
    await mkdir(dirname(filePath), { recursive: true });
    unsubscribe = sentinel.on(async (signal) => {
      if (!match(signal)) return;
      try {
        // Persist the whole Signal: its `ts` becomes the snapshot's
        // generated_at, its payload is the snapshot body.
        await atomicWrite(filePath, JSON.stringify(signal, null, 2));
      } catch (err) {
        sentinel.logger.error({ err, filePath }, 'snapshot-store: write failed');
      }
    });
    sentinel.logger.info({ filePath }, 'snapshot-store: started');
  });

  sentinel.lifecycle.onStop(async () => {
    unsubscribe?.();
    unsubscribe = undefined;
  });
}
