import { describe, it, expect } from 'vitest';
import { createSentinel } from '@appydave/appysentinel-core';

describe('sentinel smoke test', () => {
  it('starts and stops cleanly', async () => {
    const sentinel = createSentinel({
      name: 'test-sentinel',
      machine: 'test-machine',
      installSignalHandlers: false,
    });

    await sentinel.start();
    expect(sentinel.lifecycle.health().status).toBe('running');

    await sentinel.stop('manual');
    expect(sentinel.lifecycle.health().status).toBe('stopped');
  });
});
