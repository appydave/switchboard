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

const sentinel = createSentinel({
  name: 'switchboard',
  machine: process.env['MACHINE_NAME'] ?? hostname(),
});

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
