#!/bin/bash
# Log rotation for NativeLink worker on macOS.
# Runs hourly via launchd. Truncates in place so launchd's file descriptor
# stays valid — no service restart needed.
#
# #560: prior version pointed at `nativelink.log` (wrong path) so rotation
# was a silent no-op; workers accreted 33-38 GB live `nativelink-worker.log`
# between manual interventions. Fix: target `nativelink-worker.log` AND
# `nativelink-worker.err`, plus tighten to hourly cadence so the maximum
# live-log size is bounded by one hour of traffic (~1.5 GB observed).
set -euo pipefail

MAX_BYTES=$((512 * 1024 * 1024))  # 512 MB — fires within ~1h on busy worker
KEEP=5

rotate_one() {
    local LOGFILE="$1"
    [ ! -f "$LOGFILE" ] && return 0

    local SIZE
    SIZE=$(stat -f%z "$LOGFILE" 2>/dev/null || echo 0)
    [ "$SIZE" -lt "$MAX_BYTES" ] && return 0

    # Shift compressed archives (oldest first)
    rm -f "${LOGFILE}.${KEEP}.gz"
    for ((i=KEEP-1; i>=1; i--)); do
        [ -f "${LOGFILE}.${i}.gz" ] && mv "${LOGFILE}.${i}.gz" "${LOGFILE}.$((i+1)).gz"
    done

    # Compress current log, then truncate in place (launchd fd stays valid)
    gzip -c "$LOGFILE" > "${LOGFILE}.1.gz"
    : > "$LOGFILE"
}

rotate_one "${HOME}/Library/Logs/nativelink-worker.log"
rotate_one "${HOME}/Library/Logs/nativelink-worker.err"
