### macOS app bundle packaging
### Build, sign, install, and manage the NativeLink macOS app bundle.

FL_CODESIGN_CLIENT := env_var_or_default("FL_CODESIGN_CLIENT", env_var_or_default("HOME", "/Users/user") + "/fl/bazel-bin/signing_server/fl_codesign_client")
FL_CODESIGN_AUTH_TOKEN_FILE := env_var_or_default("FL_CODESIGN_AUTH_TOKEN_FILE", "/srv/casdata/signing_server/auth-token")
FL_CODESIGN_CA_CERT_FILE := env_var_or_default("FL_CODESIGN_CA_CERT_FILE", env_var_or_default("HOME", "/Users/user") + "/fl/signing_server/infra/signing-server-ca.pem")
DIST_DIR := "dist"

# Build macOS .app bundle, sign via FL signing server
release-macos:
	#!/usr/bin/env bash
	set -euo pipefail

	APP_NAME="NativeLink"
	APP_DIR="{{DIST_DIR}}/${APP_NAME}.app"
	CONTENTS_DIR="${APP_DIR}/Contents"
	MACOS_DIR="${CONTENTS_DIR}/MacOS"
	ENTITLEMENTS="packaging/macos/entitlements.plist"
	INFO_PLIST="packaging/macos/Info.plist"

	echo "Building nativelink (release)..."
	cargo build --release --bin nativelink

	echo "Creating macOS app bundle..."
	rm -rf "${APP_DIR}"
	mkdir -p "${MACOS_DIR}"

	cp target/release/nativelink "${MACOS_DIR}/nativelink"
	chmod u+wx "${MACOS_DIR}/nativelink"
	cp "${INFO_PLIST}" "${CONTENTS_DIR}/Info.plist"

	# Bundle log rotation script
	mkdir -p "${CONTENTS_DIR}/Resources"
	cp packaging/macos/rotate-log.sh "${CONTENTS_DIR}/Resources/rotate-log.sh"
	chmod +x "${CONTENTS_DIR}/Resources/rotate-log.sh"

	# Sign via FL signing server
	echo "Signing via FL signing server..."
	export FL_CODESIGN_AUTH_TOKEN="$(cat "{{FL_CODESIGN_AUTH_TOKEN_FILE}}")"
	export FL_CODESIGN_CA_CERT="$(cat "{{FL_CODESIGN_CA_CERT_FILE}}")"
	FL_CODESIGN_ENTITLEMENTS_FILE="${ENTITLEMENTS}" "{{FL_CODESIGN_CLIENT}}" "${APP_DIR}"

	echo "Build complete: ${APP_DIR}"

# Install signed macOS .app bundle to ~/Applications and load via launchd
install-macos: release-macos
	#!/usr/bin/env bash
	set -euo pipefail

	APP_NAME="NativeLink"
	SRC="{{DIST_DIR}}/${APP_NAME}.app"
	DEST="${HOME}/Applications/${APP_NAME}.app"
	PLIST_SRC="packaging/macos/com.tracemachina.nativelink.plist"
	PLIST_DEST="${HOME}/Library/LaunchAgents/com.tracemachina.nativelink.plist"

	echo "Installing ${APP_NAME} to ${DEST}..."
	mkdir -p "${HOME}/Applications"
	# Unload existing agent if loaded
	launchctl bootout "gui/$(id -u)/com.tracemachina.nativelink" 2>/dev/null || true
	rm -rf "${DEST}"
	cp -R "${SRC}" "${DEST}"

	echo "Installing launchd plist to ${PLIST_DEST}..."
	mkdir -p "${HOME}/Library/LaunchAgents"
	cp "${PLIST_SRC}" "${PLIST_DEST}"

	echo "Loading launch agent..."
	launchctl bootstrap "gui/$(id -u)" "${PLIST_DEST}"

	# Install log rotation
	ROTATE_PLIST_SRC="packaging/macos/com.tracemachina.nativelink.rotate-log.plist"
	ROTATE_PLIST_DEST="${HOME}/Library/LaunchAgents/com.tracemachina.nativelink.rotate-log.plist"
	launchctl bootout "gui/$(id -u)/com.tracemachina.nativelink.rotate-log" 2>/dev/null || true
	cp "${ROTATE_PLIST_SRC}" "${ROTATE_PLIST_DEST}"
	launchctl bootstrap "gui/$(id -u)" "${ROTATE_PLIST_DEST}"

	echo "Done. ${APP_NAME} installed and loaded."

# Load the nativelink launch agent and log rotation
launchd-load:
	launchctl bootstrap "gui/$(id -u)" "${HOME}/Library/LaunchAgents/com.tracemachina.nativelink.plist"
	launchctl bootstrap "gui/$(id -u)" "${HOME}/Library/LaunchAgents/com.tracemachina.nativelink.rotate-log.plist"

# Unload the nativelink launch agent and log rotation
launchd-unload:
	launchctl bootout "gui/$(id -u)/com.tracemachina.nativelink"
	launchctl bootout "gui/$(id -u)/com.tracemachina.nativelink.rotate-log"

### Protocol verification gate (TLA+) -----------------------------------------
### See specs/README.md "Verification Gate" for full rationale.

# Layer 1: SANY + TLC sweep over every spec in specs/.
# Pass --strict to convert "no tla2tools.jar" / "no specs/" into errors.
verify-tla *ARGS:
	bash scripts/verify_tla.sh {{ARGS}}

# Layer 2: assert any protocol-relevant change comes with a .tla mod or
# a [no-tla-needed: ...] waiver. Reads `git diff --name-only` against
# the merge-base with origin/main on stdin.
check-protocol *ARGS:
	#!/usr/bin/env bash
	set -euo pipefail
	BASE="$(git merge-base origin/main HEAD)"
	DIFF_TMP="$(mktemp)"
	trap 'rm -f "${DIFF_TMP}"' EXIT
	git diff "${BASE}..HEAD" > "${DIFF_TMP}"
	# Last-commit message is what reviewers see; pass it for waiver lookup.
	COMMIT_MSG_TMP="$(mktemp)"
	git log -1 --pretty=%B > "${COMMIT_MSG_TMP}"
	trap 'rm -f "${DIFF_TMP}" "${COMMIT_MSG_TMP}"' EXIT
	git diff --name-only "${BASE}..HEAD" \
	    | COMMIT_MSG_FILE="${COMMIT_MSG_TMP}" \
	      bash scripts/check_protocol_diff.sh --diff-file "${DIFF_TMP}" {{ARGS}}

# Both layers — what CI runs end-to-end.
verify-protocol-gate: check-protocol verify-tla

# Self-tests for the gate scripts themselves.
test-protocol-gate:
	bash scripts/tests/test_check_protocol_diff.sh
	bash scripts/tests/test_verify_tla.sh

### Data-plane benchmark suite (#495 Phase 1 v3-anchoring) -----------------
### Operator doc: benchmarks/README.md
### Pre-flight gate refuses to run if buildcache is serving live production
### traffic; pass `--force` to override (only acceptable for the first
### baseline collection in a known quiet window).

# Run the data-plane benchmark suite and write a baseline JSON file.
# Usage: just bench-data-plane                   # default args, writes to baselines/<ts>-<sha>.json
#        just bench-data-plane --fast --force    # fast smoke + bypass gate
bench-data-plane *ARGS:
	#!/usr/bin/env bash
	set -euo pipefail
	SHA="$(git rev-parse --short=12 HEAD)"
	TS="$(date -u +%Y%m%dT%H%M%SZ)"
	OUT="benchmarks/baselines/${TS}-${SHA}.json"
	mkdir -p "$(dirname "$OUT")"
	echo "[just] running data_plane_bench → ${OUT}"
	cargo run --release --bin data_plane_bench --features chunked_fast_slow -p nativelink-benchmarks -- \
	    --output "${OUT}" {{ARGS}}
	echo "[just] baseline: ${OUT}"

# Print the pre-flight verdict only; useful to check whether the host is
# clear to run benches without actually running them.
bench-preflight:
	cargo run --release --bin data_plane_bench --features chunked_fast_slow -p nativelink-benchmarks -- --preflight-only
