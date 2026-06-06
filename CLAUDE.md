# switchboard — Claude Instructions

This is an **AppySentinel** project. AppySentinel builds headless, always-on local
data coordinators ("Sentinels"). No UI. No dashboard. Visualisation is a separate app
that consumes this Sentinel through its expose surfaces.

---

## Three moments in a Sentinel's life

| Moment | What happens | How |
|--------|-------------|-----|
| **Scaffold** | Project skeleton created | `npx create-appysentinel` — already done |
| **Build** | Recipes wired, Sentinel made useful | `claude` in this directory — you are here |
| **Deploy** | Registered as always-on service | `bash scripts/install-service.sh` on the target machine |

Build and deploy are separate. Build on your dev machine; deploy runs wherever the Sentinel lives permanently.

---

## How to add a capability (recipe pattern)

A recipe is a function: takes `sentinel` + options, registers hooks, subscribes to the bus, emits Signals.

```typescript
// src/recipes/my-collector.ts
import type { Sentinel } from '@appydave/appysentinel-core';

export function myCollector(sentinel: Sentinel, options: { interval: number }) {
  sentinel.lifecycle.onStart(async () => {
    // open connections, start timers
  });

  sentinel.lifecycle.onStop(async () => {
    // clean up — always implement this
  });

  // emit a signal
  sentinel.emit({ source: 'my-collector', kind: 'event', name: 'thing.happened', payload: { ... } });
}
```

Wire it in `src/main.ts` before `sentinel.start()`:

```typescript
import { myCollector } from './recipes/my-collector.js';
myCollector(sentinel, { interval: 60_000 });
await sentinel.start();
```

---

## The Signal envelope

Every record emitted by this Sentinel is a `Signal`:

```
source: string        which recipe emitted it (e.g. 'watch-directory')
kind:   log | metric | event | state | span
name:   string        semantic label (e.g. 'file.created', 'fleet.snapshot')
payload: {}           recipe-specific — define a typed interface per recipe
```

Rule: `state` kind → one overwriting snapshot (use `snapshot-store`). `event`/`log` kind → append (use `jsonl-store`).

---

## Three zones

- **Collect** — data flows IN. Recipes in `src/collect/`. File watchers, SSH polls, HTTP webhooks, DB diffs, shell commands.
- **Access** — bidirectional interface layer. `src/access/` has three sub-layers:
    - `query/`    — read logic. Pure functions over snapshots. Returns `QueryResult<T>`. No transport knowledge.
    - `command/`  — sentinel self-management. Config changes, triggered collections, pause/resume. Never mutates observed systems.
    - `bindings/` — thin protocol adapters. MCP, HTTP, CLI. Call `query/` or `command/`, translate to protocol.
  Design pattern: **CQRS-lite** — Query is the read side, Command is the write side.
  CQRS applies to Access only. Collect and Deliver are separate patterns.
- **Deliver** — data flows OUT. Recipes in `src/deliver/`. HTTP push, Supabase, OTLP, file relay.

Storage and enrichment sit between zones — they process data after collection, before access or delivery.

OpenTelemetry: we follow OTel conventions (signal kinds, attributes, timestamps). We do not depend on OTel libraries.

---

## Testing your Sentinel

**Two modes — do not confuse them:**

| Mode | Command | What it does |
|------|---------|-------------|
| **Live run** | `bun src/main.ts` | Starts the real loop. Blocks. SIGINT (Ctrl-C) to stop. Registers OS signal handlers. |
| **Tests** | `bun run test` / `bun run test:watch` | Vitest. Does NOT run the live loop. Must start and stop the sentinel within each test. |

**Rule for every test:** always pass `installSignalHandlers: false` to `createSentinel()`. Without it, the sentinel registers SIGINT/SIGTERM handlers and Vitest cannot exit cleanly.

```typescript
// src/__tests__/my-recipe.test.ts
const sentinel = createSentinel({
  name: 'test',
  machine: 'test-machine',
  installSignalHandlers: false,   // required — prevents signal handler leak in Vitest
});
await sentinel.start();
// ... test assertions ...
await sentinel.stop('manual');
```

**Watch mode while developing:** `bun run test:watch` — Vitest re-runs only tests affected by files you change. Leave it open in a terminal split while wiring recipes.

**Pre-push hook (Husky):** `.husky/pre-push` runs `bun run test && bun run typecheck` automatically before every push. Fix failures — do not bypass with `--no-verify`.

---

## Hard rules

- **Observer-only by default** — read, never mutate the systems this Sentinel observes.
- **File-based storage is the default** — SQLite must earn its place (query complexity, multi-reader).
- **Recipes own their deps** — add libraries to `package.json` only when wiring a recipe that needs them.
- **`src/main.ts` is wiring only** — no business logic in main; recipes go in `src/collect/`, `src/access/`, or `src/deliver/`.
- **Run `bun src/main.ts` after every recipe addition** — smoke-test before moving on.

---

## Useful commands

```bash
bun src/main.ts                    # run in dev (Ctrl-C to stop)
bun run test                       # run tests once
bun run test:watch                 # run tests in watch mode (leave open while developing)
bun run typecheck                  # TypeScript check
bash scripts/install-service.sh   # register as always-on service (deploy time)
bash scripts/uninstall-service.sh  # remove service registration
```
