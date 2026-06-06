# switchboard

A per-machine, observer-only telemetry collector built on [AppySentinel](https://github.com/appydave/appysentinal).

This project was scaffolded by `create-appysentinel`. It currently contains only the **walking skeleton** — `createSentinel()` is wired up, but no collectors, storage, interface, or transport are configured yet.

## Run it (dev)

```bash
bun install
bun src/main.ts
```

You should see a single `sentinel.started` event on stdout, then the process keeps running until you Ctrl-C.

## Install as an always-on service

Run once on the target machine when you're ready to deploy:

```bash
bash scripts/install-service.sh
```

Supports macOS (launchd) and Linux (systemd). Starts on login, restarts on crash.

To remove:

```bash
bash scripts/uninstall-service.sh
```

## Configure it

Open Claude Code inside this project to start wiring recipes:

```bash
claude
```

Recipes add real capabilities: file watchers, SSH orchestration, storage, HTTP/MCP expose surfaces, outbound transports.

## What's baked in vs what's a recipe

**Baked in** (this template + `@appydave/appysentinel-core`):
- Signal envelope, SignalBus, lifecycle harness (SIGINT/SIGTERM/SIGHUP), config loader, atomic write, serial queue, Pino logger, `createSentinel()` factory.
- Service registration scripts (`scripts/`).

**Added by recipes** (written during your build session):
- Collectors, storage, interfaces, transports, enrichment.

## License

MIT
