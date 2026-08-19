#!/bin/bash
# bundle-worker.sh — Wrap nativelink in a macOS .app bundle
#
# macOS ties Local Network permission to CFBundleIdentifier. A bare binary
# loses the permission on every rebuild. This script copies the release
# binary into a minimal .app with a stable bundle ID so the permission
# persists across rebuilds.
set -euo pipefail

BINARY="/Users/user/src/nativelink/target/release/nativelink"
APP_DIR="/Users/user/Applications/NativeLink.app"
CONTENTS="${APP_DIR}/Contents"
MACOS="${CONTENTS}/MacOS"

if [[ ! -f "${BINARY}" ]]; then
    echo "ERROR: Binary not found at ${BINARY}" >&2
    exit 1
fi

# Clean and recreate bundle structure.
rm -rf "${APP_DIR}"
mkdir -p "${MACOS}"

cp "${BINARY}" "${MACOS}/nativelink"
chmod 755 "${MACOS}/nativelink"

cat > "${CONTENTS}/Info.plist" << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleIdentifier</key>
    <string>com.nativelink.worker</string>
    <key>CFBundleName</key>
    <string>NativeLink</string>
    <key>CFBundleExecutable</key>
    <string>nativelink</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleVersion</key>
    <string>1.0</string>
    <key>LSBackgroundOnly</key>
    <true/>
    <key>NSLocalNetworkUsageDescription</key>
    <string>NativeLink shares build artifacts with peers over the local network.</string>
</dict>
</plist>
PLIST

# Ad-hoc codesign with network entitlements.
ENTITLEMENTS=$(mktemp)
cat > "${ENTITLEMENTS}" << 'XML'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.network.server</key>
    <true/>
    <key>com.apple.security.network.client</key>
    <true/>
    <key>com.apple.security.get-task-allow</key>
    <true/>
</dict>
</plist>
XML

codesign -s - --force --deep --entitlements "${ENTITLEMENTS}" "${APP_DIR}"
rm -f "${ENTITLEMENTS}"

echo "Bundled: ${MACOS}/nativelink"
