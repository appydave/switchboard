#!/usr/bin/env bash
# Register switchboard as an always-on background service.
# Run once on the target machine after scaffolding.
set -euo pipefail

PROJECT_NAME="switchboard"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUN_PATH="$(command -v bun 2>/dev/null || echo "/usr/local/bin/bun")"

echo "  Registering $PROJECT_NAME as a background service..."
echo "  Project dir : $PROJECT_DIR"
echo "  Bun         : $BUN_PATH"

mkdir -p "$PROJECT_DIR/logs"

case "$(uname -s)" in
  Darwin)
    AGENTS_DIR="$HOME/Library/LaunchAgents"
    PLIST_DEST="$AGENTS_DIR/com.appydave.$PROJECT_NAME.plist"
    mkdir -p "$AGENTS_DIR"
    sed \
      -e "s|{{PROJECT_DIR}}|$PROJECT_DIR|g" \
      -e "s|{{BUN_PATH}}|$BUN_PATH|g" \
      -e "s|{{HOME_DIR}}|$HOME|g" \
      "$SCRIPT_DIR/launchd.plist" > "$PLIST_DEST"
    launchctl load "$PLIST_DEST"
    echo "  ✓ Registered with launchd"
    echo "  ✓ Plist: $PLIST_DEST"
    echo ""
    echo "  Useful commands:"
    echo "    launchctl list com.appydave.$PROJECT_NAME   # check status"
    echo "    launchctl stop com.appydave.$PROJECT_NAME   # stop (launchd restarts it)"
    echo "    bash scripts/uninstall-service.sh           # remove service"
    ;;
  Linux)
    SERVICE_DEST="$HOME/.config/systemd/user/$PROJECT_NAME.service"
    mkdir -p "$(dirname "$SERVICE_DEST")"
    sed \
      -e "s|{{PROJECT_DIR}}|$PROJECT_DIR|g" \
      -e "s|{{BUN_PATH}}|$BUN_PATH|g" \
      "$SCRIPT_DIR/systemd.service" > "$SERVICE_DEST"
    systemctl --user daemon-reload
    systemctl --user enable "$PROJECT_NAME"
    systemctl --user start "$PROJECT_NAME"
    echo "  ✓ Registered with systemd"
    echo "  ✓ Unit: $SERVICE_DEST"
    echo ""
    echo "  Useful commands:"
    echo "    systemctl --user status $PROJECT_NAME       # check status"
    echo "    systemctl --user restart $PROJECT_NAME      # restart"
    echo "    bash scripts/uninstall-service.sh           # remove service"
    ;;
  *)
    echo "  Unsupported platform: $(uname -s)"
    echo "  Register manually using scripts/launchd.plist or scripts/systemd.service as a reference."
    exit 1
    ;;
esac
