#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# build_app.sh — Build a macOS .app bundle for GOD MODE Media Library
#
# Creates a proper macOS application that:
#   1. Starts the gml web server in the background
#   2. Opens the browser to the UI
#   3. Shows a menu-bar–visible process while running
#   4. Cleans up the server on quit
#
# Output: dist/GOD MODE Media Library.app  (+ optional .dmg)
# ─────────────────────────────────────────────────────────────────
set -euo pipefail
export COPYFILE_DISABLE=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VERSION="$(python3 -c "import tomllib, pathlib; p=pathlib.Path('$ROOT/pyproject.toml'); print(tomllib.loads(p.read_text(encoding='utf-8'))['project']['version'])")"

APP_NAME="GOD MODE Media Library"
BUNDLE_ID="com.marekgaletka.godmode-media-library"
DIST_DIR="$ROOT/dist"
APP_DIR="$DIST_DIR/$APP_NAME.app"
CONTENTS="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS/MacOS"
RESOURCES_DIR="$CONTENTS/Resources"

ICON_SRC="$ROOT/installer/macos/AppIcon.icns"

echo "Building $APP_NAME.app v$VERSION …"

# ── Clean previous build ────────────────────────────────────────
rm -rf "$APP_DIR"
mkdir -p "$MACOS_DIR" "$RESOURCES_DIR" "$DIST_DIR"

# ── Generate icon if missing ────────────────────────────────────
if [ ! -f "$ICON_SRC" ]; then
    echo "Generating icon…"
    python3 "$ROOT/installer/macos/generate_icon.py"
fi

# ── Copy icon ───────────────────────────────────────────────────
cp "$ICON_SRC" "$RESOURCES_DIR/AppIcon.icns"

# ── Info.plist ──────────────────────────────────────────────────
cat > "$CONTENTS/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>$APP_NAME</string>
    <key>CFBundleDisplayName</key>
    <string>$APP_NAME</string>
    <key>CFBundleIdentifier</key>
    <string>$BUNDLE_ID</string>
    <key>CFBundleVersion</key>
    <string>$VERSION</string>
    <key>CFBundleShortVersionString</key>
    <string>$VERSION</string>
    <key>CFBundleExecutable</key>
    <string>launcher</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleSignature</key>
    <string>????</string>
    <key>LSMinimumSystemVersion</key>
    <string>12.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>LSUIElement</key>
    <false/>
    <key>NSSupportsAutomaticTermination</key>
    <true/>
    <key>NSSupportsSuddenTermination</key>
    <false/>
</dict>
</plist>
PLIST

# ── Launcher script ────────────────────────────────────────────
cat > "$MACOS_DIR/launcher" <<'LAUNCHER'
#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# GOD MODE Media Library — macOS .app launcher
#
# Lifecycle:
#   1. Find the gml binary (venv in /Applications, pip, PATH)
#   2. Pick a free port (default 8777)
#   3. Start "gml serve" in background
#   4. Wait for server to be ready (up to 15s)
#   5. Open browser
#   6. Wait — the .app stays "running" so it appears in Dock
#   7. On quit (Cmd+Q / kill), shut down the server cleanly
# ─────────────────────────────────────────────────────────────
set -euo pipefail

APP_SUPPORT="$HOME/Library/Application Support/GOD MODE Media Library"
LOG_FILE="$APP_SUPPORT/server.log"
PID_FILE="$APP_SUPPORT/server.pid"
PORT="${GML_PORT:-8777}"
HOST="${GML_HOST:-127.0.0.1}"

mkdir -p "$APP_SUPPORT"

# ── Find gml binary ────────────────────────────────────────────
find_gml() {
    # 1. Installed via .pkg into /Applications
    local app_venv="/Applications/GOD MODE Media Library/venv/bin/gml"
    if [ -x "$app_venv" ]; then echo "$app_venv"; return; fi

    # 2. User pip install (common locations)
    local user_bin="$HOME/.local/bin/gml"
    if [ -x "$user_bin" ]; then echo "$user_bin"; return; fi

    # 3. Homebrew / system PATH
    if command -v gml >/dev/null 2>&1; then command -v gml; return; fi

    # 4. pyenv / conda common paths
    for p in "$HOME/.pyenv/shims/gml" "$HOME/miniconda3/bin/gml" "$HOME/anaconda3/bin/gml"; do
        if [ -x "$p" ]; then echo "$p"; return; fi
    done

    return 1
}

GML_BIN="$(find_gml)" || {
    osascript -e 'display dialog "GOD MODE Media Library is not installed.\n\nInstall it with:\n  pip install godmode-media-library[web]" buttons {"OK"} default button "OK" with title "GOD MODE" with icon caution'
    exit 1
}

# ── Check if already running ───────────────────────────────────
if [ -f "$PID_FILE" ]; then
    OLD_PID="$(cat "$PID_FILE")"
    if kill -0 "$OLD_PID" 2>/dev/null; then
        # Already running — just open browser and exit
        open "http://$HOST:$PORT"
        exit 0
    fi
    rm -f "$PID_FILE"
fi

# ── Check port availability ────────────────────────────────────
if lsof -iTCP:"$PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
    # Port in use — try to detect if it's our server
    if curl -s "http://$HOST:$PORT/api/stats" >/dev/null 2>&1; then
        open "http://$HOST:$PORT"
        exit 0
    fi
    # Port used by something else — try next port
    PORT=$((PORT + 1))
fi

# ── Cleanup handler ────────────────────────────────────────────
cleanup() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid="$(cat "$PID_FILE")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            # Give it 3 seconds for graceful shutdown
            for _ in 1 2 3; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            # Force kill if still running
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi
}

trap cleanup EXIT INT TERM

# ── Start server ───────────────────────────────────────────────
"$GML_BIN" serve --host "$HOST" --port "$PORT" --no-browser \
    >> "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$PID_FILE"

# ── Wait for server ready ──────────────────────────────────────
MAX_WAIT=15
for i in $(seq 1 $MAX_WAIT); do
    if curl -s "http://$HOST:$PORT/api/stats" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        osascript -e "display dialog \"Server failed to start. Check log at:\n$LOG_FILE\" buttons {\"OK\"} default button \"OK\" with title \"GOD MODE\" with icon stop"
        exit 1
    fi
    sleep 1
done

# ── Open browser ───────────────────────────────────────────────
open "http://$HOST:$PORT"

# ── Keep app alive (visible in Dock) ──────────────────────────
# Wait for the server process — when it dies, the app quits.
# When user quits the app (Cmd+Q), the trap kills the server.
wait "$SERVER_PID" 2>/dev/null || true
LAUNCHER
chmod 755 "$MACOS_DIR/launcher"

# ── Clean macOS metadata ───────────────────────────────────────
if command -v dot_clean >/dev/null 2>&1; then
    dot_clean -m "$APP_DIR" >/dev/null 2>&1 || true
fi
if command -v xattr >/dev/null 2>&1; then
    xattr -cr "$APP_DIR" >/dev/null 2>&1 || true
fi

echo "✓ Built: $APP_DIR"
echo ""

# ── Optional: create DMG ───────────────────────────────────────
DMG_PATH="$DIST_DIR/$APP_NAME.dmg"
if command -v hdiutil >/dev/null 2>&1; then
    # Create a temporary directory with the .app and an Applications symlink
    DMG_STAGING="$ROOT/build/dmg_staging"
    rm -rf "$DMG_STAGING"
    mkdir -p "$DMG_STAGING"
    cp -R "$APP_DIR" "$DMG_STAGING/"
    ln -s /Applications "$DMG_STAGING/Applications"

    hdiutil create \
        -volname "$APP_NAME" \
        -srcfolder "$DMG_STAGING" \
        -ov \
        -format UDZO \
        "$DMG_PATH" >/dev/null

    rm -rf "$DMG_STAGING"
    echo "✓ DMG:   $DMG_PATH"
fi

echo ""
echo "To install: drag '$APP_NAME.app' to /Applications"
echo "To run now: open '$APP_DIR'"
