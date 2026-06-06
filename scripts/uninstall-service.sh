#!/usr/bin/env bash
# Unregister switchboard from the system service manager.
set -euo pipefail

PROJECT_NAME="switchboard"

case "$(uname -s)" in
  Darwin)
    PLIST="$HOME/Library/LaunchAgents/com.appydave.$PROJECT_NAME.plist"
    if [ -f "$PLIST" ]; then
      launchctl unload "$PLIST"
      rm "$PLIST"
      echo "  ✓ Removed launchd service"
    else
      echo "  Service not registered (plist not found at $PLIST)"
    fi
    ;;
  Linux)
    systemctl --user stop "$PROJECT_NAME" 2>/dev/null || true
    systemctl --user disable "$PROJECT_NAME" 2>/dev/null || true
    rm -f "$HOME/.config/systemd/user/$PROJECT_NAME.service"
    systemctl --user daemon-reload
    echo "  ✓ Removed systemd service"
    ;;
  *)
    echo "  Unsupported platform: $(uname -s)"
    exit 1
    ;;
esac
