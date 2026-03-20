#!/usr/bin/env bash
set -euo pipefail
export COPYFILE_DISABLE=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VERSION="$(python3 -c "import tomllib, pathlib; p=pathlib.Path('$ROOT/pyproject.toml'); print(tomllib.loads(p.read_text(encoding='utf-8'))['project']['version'])")"
STAMP="$(date +%Y%m%d_%H%M%S)"

APP_NAME="GOD MODE Media Library"
APP_INSTALL_PATH="/Applications/$APP_NAME"
VENV_INSTALL_PATH="$APP_INSTALL_PATH/venv"

BUILD_DIR="$ROOT/build/installer_$STAMP"
STAGING_ROOT="$BUILD_DIR/root"
SCRIPTS_DIR="$BUILD_DIR/scripts"
APP_PAYLOAD="$STAGING_ROOT$APP_INSTALL_PATH"
BIN_DIR="$STAGING_ROOT/usr/local/bin"
DIST_DIR="$ROOT/dist"

IDENTIFIER="com.marekgaletka.godmode-media-library"
PKG_NAME="GodModeMediaLibrary-${VERSION}-macos.pkg"
DMG_NAME="GodModeMediaLibrary-${VERSION}-macos.dmg"
PKG_PATH="$DIST_DIR/$PKG_NAME"
DMG_PATH="$DIST_DIR/$DMG_NAME"

mkdir -p "$APP_PAYLOAD" "$BIN_DIR" "$SCRIPTS_DIR" "$DIST_DIR"

python3 -m venv "$APP_PAYLOAD/venv"
"$APP_PAYLOAD/venv/bin/pip" install --upgrade pip >/dev/null
"$APP_PAYLOAD/venv/bin/pip" install "$ROOT" >/dev/null

cp "$ROOT/README.md" "$APP_PAYLOAD/README.md"
mkdir -p "$APP_PAYLOAD/docs"
cp "$ROOT/docs/GOD_MODE_POLICY.md" "$APP_PAYLOAD/docs/GOD_MODE_POLICY.md"

# Install web dependencies into venv
"$APP_PAYLOAD/venv/bin/pip" install fastapi uvicorn[standard] >/dev/null

# Generate icon if needed and build .app bundle into /Applications
ICON_SRC="$ROOT/installer/macos/AppIcon.icns"
if [ ! -f "$ICON_SRC" ]; then
    python3 "$ROOT/installer/macos/generate_icon.py"
fi
BUNDLE_DIR="$STAGING_ROOT/Applications/$APP_NAME.app"
mkdir -p "$BUNDLE_DIR/Contents/MacOS" "$BUNDLE_DIR/Contents/Resources"
cp "$ICON_SRC" "$BUNDLE_DIR/Contents/Resources/AppIcon.icns"
# Info.plist
cat > "$BUNDLE_DIR/Contents/Info.plist" <<IPLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key><string>$APP_NAME</string>
    <key>CFBundleDisplayName</key><string>$APP_NAME</string>
    <key>CFBundleIdentifier</key><string>$IDENTIFIER</string>
    <key>CFBundleVersion</key><string>$VERSION</string>
    <key>CFBundleShortVersionString</key><string>$VERSION</string>
    <key>CFBundleExecutable</key><string>launcher</string>
    <key>CFBundleIconFile</key><string>AppIcon</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>LSMinimumSystemVersion</key><string>12.0</string>
    <key>NSHighResolutionCapable</key><true/>
    <key>LSUIElement</key><false/>
</dict>
</plist>
IPLIST
# Launcher script (starts gml serve + opens browser)
cat > "$BUNDLE_DIR/Contents/MacOS/launcher" <<'LSCRIPT'
#!/usr/bin/env bash
set -euo pipefail
APP_SUPPORT="$HOME/Library/Application Support/GOD MODE Media Library"
LOG_FILE="$APP_SUPPORT/server.log"
PID_FILE="$APP_SUPPORT/server.pid"
PORT="${GML_PORT:-8777}"
HOST="${GML_HOST:-127.0.0.1}"
mkdir -p "$APP_SUPPORT"

find_gml() {
    local app_venv="/Applications/GOD MODE Media Library/venv/bin/gml"
    if [ -x "$app_venv" ]; then echo "$app_venv"; return; fi
    if [ -x "$HOME/.local/bin/gml" ]; then echo "$HOME/.local/bin/gml"; return; fi
    if command -v gml >/dev/null 2>&1; then command -v gml; return; fi
    return 1
}
GML_BIN="$(find_gml)" || {
    osascript -e 'display dialog "gml not found. Install with:\n  pip install godmode-media-library[web]" buttons {"OK"} default button "OK" with title "GOD MODE" with icon caution'
    exit 1
}

if [ -f "$PID_FILE" ]; then
    OLD_PID="$(cat "$PID_FILE")"
    if kill -0 "$OLD_PID" 2>/dev/null; then
        open "http://$HOST:$PORT"; exit 0
    fi
    rm -f "$PID_FILE"
fi

if lsof -iTCP:"$PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
    if curl -s "http://$HOST:$PORT/api/stats" >/dev/null 2>&1; then
        open "http://$HOST:$PORT"; exit 0
    fi
    PORT=$((PORT + 1))
fi

cleanup() {
    if [ -f "$PID_FILE" ]; then
        local pid; pid="$(cat "$PID_FILE")"
        kill "$pid" 2>/dev/null || true
        for _ in 1 2 3; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        kill -9 "$pid" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
}
trap cleanup EXIT INT TERM

"$GML_BIN" serve --host "$HOST" --port "$PORT" --no-browser >> "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$PID_FILE"

for i in $(seq 1 15); do
    curl -s "http://$HOST:$PORT/api/stats" >/dev/null 2>&1 && break
    kill -0 "$SERVER_PID" 2>/dev/null || {
        osascript -e "display dialog \"Server failed. Check:\n$LOG_FILE\" buttons {\"OK\"} default button \"OK\" with title \"GOD MODE\" with icon stop"
        exit 1
    }
    sleep 1
done

open "http://$HOST:$PORT"
wait "$SERVER_PID" 2>/dev/null || true
LSCRIPT
chmod 755 "$BUNDLE_DIR/Contents/MacOS/launcher"

cat > "$APP_PAYLOAD/GodMode Media Library.command" <<EOF
#!/bin/bash
exec "$VENV_INSTALL_PATH/bin/gml" "\$@"
EOF
chmod 755 "$APP_PAYLOAD/GodMode Media Library.command"

cat > "$BIN_DIR/gml" <<EOF
#!/bin/bash
exec "$VENV_INSTALL_PATH/bin/gml" "\$@"
EOF
chmod 755 "$BIN_DIR/gml"

cp "$ROOT/installer/macos/postinstall" "$SCRIPTS_DIR/postinstall"
chmod 755 "$SCRIPTS_DIR/postinstall"

if command -v dot_clean >/dev/null 2>&1; then
  dot_clean -m "$STAGING_ROOT" >/dev/null 2>&1 || true
fi
if command -v xattr >/dev/null 2>&1; then
  xattr -cr "$STAGING_ROOT" >/dev/null 2>&1 || true
fi

pkgbuild \
  --root "$STAGING_ROOT" \
  --scripts "$SCRIPTS_DIR" \
  --identifier "$IDENTIFIER" \
  --version "$VERSION" \
  --filter '(^|/)\._[^/]+$' \
  --filter '(^|/)\.DS_Store$' \
  --install-location "/" \
  "$PKG_PATH" >/dev/null

hdiutil create \
  -volname "GodModeMediaLibrary Installer" \
  -srcfolder "$PKG_PATH" \
  -ov \
  -format UDZO \
  "$DMG_PATH" >/dev/null

echo "pkg=$PKG_PATH"
echo "dmg=$DMG_PATH"
