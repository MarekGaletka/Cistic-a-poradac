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
