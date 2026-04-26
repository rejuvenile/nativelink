#!/usr/bin/env bash
# check_protocol_diff.sh — Layer 2 of the protocol-verification gate.
#
# Reads the changed-file list from stdin (one path per line — `git diff
# --name-only origin/main..HEAD` is the typical caller), classifies each
# file as protocol-relevant or not, and asserts that ANY protocol-relevant
# change is paired with a `*.tla` modification or a commit-message waiver.
#
# Exit codes:
#   0 = no protocol change detected; OR protocol change paired with .tla
#       change; OR waiver present in commit message.
#   1 = protocol change detected, no .tla change, no waiver — BLOCK.
#
# Triggers (HARD — any of these alone forces the gate):
#   * nativelink-service/src/{worker_api_server,cas_server,bytestream_server}.rs
#   * nativelink-worker/src/{local_worker,running_actions_manager}.rs
#   * any *.proto under nativelink-proto/
#   * nativelink-util/src/store_trait.rs
#
# Triggers (SOFT — flagged with WARN; promoted to HARD only when a
# --diff-file is supplied AND that diff adds a `pub fn` or `pub trait`):
#   * nativelink-store/src/*_store.rs
#
# Waiver format (in commit message, parsed from $COMMIT_MSG_FILE):
#   [no-tla-needed: <reason>]
# Reason must be non-empty. Stripped of leading/trailing whitespace.
#
# Env vars:
#   COMMIT_MSG_FILE  path to the commit message (optional; CI passes it)
#
# Args:
#   --diff-file PATH  git diff to scan for new pub fn / pub trait additions
#                     in store files (promotes SOFT trigger to HARD).

set -euo pipefail

DIFF_FILE=""

while (( $# > 0 )); do
    case "$1" in
        --diff-file) DIFF_FILE="$2"; shift 2 ;;
        -h|--help)
            sed -n '1,40p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "check_protocol_diff.sh: unknown arg '$1'" >&2
            exit 2
            ;;
    esac
done

note() { printf '[protocol-diff] %s\n' "$*"; }
warn() { printf '[protocol-diff][WARN] %s\n' "$*"; }
err()  { printf '[protocol-diff][BLOCK] %s\n' "$*"; }

# --- Trigger classifiers --------------------------------------------------

is_hard_trigger() {
    case "$1" in
        nativelink-service/src/worker_api_server.rs)        return 0 ;;
        nativelink-service/src/cas_server.rs)               return 0 ;;
        nativelink-service/src/bytestream_server.rs)        return 0 ;;
        nativelink-worker/src/local_worker.rs)              return 0 ;;
        nativelink-worker/src/running_actions_manager.rs)   return 0 ;;
        nativelink-util/src/store_trait.rs)                 return 0 ;;
        nativelink-proto/*.proto|*.proto)                   return 0 ;;
    esac
    # Match any nativelink-proto/.../*.proto via glob.
    case "$1" in
        nativelink-proto/*) [[ "$1" == *.proto ]] && return 0 ;;
    esac
    return 1
}

is_soft_trigger() {
    case "$1" in
        nativelink-store/src/*_store.rs) return 0 ;;
    esac
    return 1
}

# --- Read input + collect signals ----------------------------------------

CHANGED_FILES=()
TLA_MODIFIED=()
HARD_TRIGGERS=()
SOFT_TRIGGERS=()

while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    CHANGED_FILES+=( "${line}" )
    if [[ "${line}" == specs/*.tla ]] || [[ "${line}" == */specs/*.tla ]]; then
        TLA_MODIFIED+=( "${line}" )
    fi
    if is_hard_trigger "${line}"; then
        HARD_TRIGGERS+=( "${line}" )
    elif is_soft_trigger "${line}"; then
        SOFT_TRIGGERS+=( "${line}" )
    fi
done

if (( ${#CHANGED_FILES[@]} == 0 )); then
    note "no changed files on stdin; nothing to check"
    exit 0
fi

note "scanned ${#CHANGED_FILES[@]} changed file(s); ${#TLA_MODIFIED[@]} .tla modified, \
${#HARD_TRIGGERS[@]} hard trigger(s), ${#SOFT_TRIGGERS[@]} soft trigger(s)"

# --- Soft-trigger promotion (store-trait public-API surface) -------------

# A soft trigger becomes HARD when the diff adds a `pub fn` or `pub trait`
# in the store file. Without a diff to inspect, we leave it soft and emit
# a WARN — the perf-optimizer / code-reviewer audit covers the manual case.
PROMOTED_FROM_SOFT=()
if (( ${#SOFT_TRIGGERS[@]} > 0 )); then
    if [[ -n "${DIFF_FILE}" && -f "${DIFF_FILE}" ]]; then
        # Extract added lines from the diff (lines beginning with "+" but not
        # "+++"); look for pub fn / pub trait additions inside store files.
        while IFS= read -r added; do
            for sft in "${SOFT_TRIGGERS[@]}"; do
                # Heuristic: if the diff text mentions the soft-trigger file
                # AND the added line declares pub fn / pub trait, promote it.
                if [[ "${added}" == *"pub fn"* ]] || [[ "${added}" == *"pub trait"* ]]; then
                    PROMOTED_FROM_SOFT+=( "${sft}" )
                    break
                fi
            done
        done < <(grep -E '^\+[^+]' "${DIFF_FILE}" 2>/dev/null || true)
    fi
fi

# Deduplicate promotions.
if (( ${#PROMOTED_FROM_SOFT[@]} > 0 )); then
    mapfile -t PROMOTED_FROM_SOFT < <(printf '%s\n' "${PROMOTED_FROM_SOFT[@]}" | sort -u)
fi

# Print a WARN line for un-promoted soft triggers so reviewers see them.
if (( ${#SOFT_TRIGGERS[@]} > ${#PROMOTED_FROM_SOFT[@]} )); then
    for sft in "${SOFT_TRIGGERS[@]}"; do
        is_promoted=0
        for p in "${PROMOTED_FROM_SOFT[@]+"${PROMOTED_FROM_SOFT[@]}"}"; do
            [[ "${sft}" == "${p}" ]] && is_promoted=1 && break
        done
        if (( is_promoted == 0 )); then
            warn "${sft}: store file changed (soft trigger). \
Reviewer must confirm no new cross-component contract introduced. \
If a new pub fn / pub trait is added, treat as a HARD trigger and provide a TLA+ spec."
        fi
    done
fi

# Combine HARD + PROMOTED into the effective trigger set. Promoted entries
# are tagged so the BLOCK message can cite the specific reason.
EFFECTIVE_TRIGGERS=( "${HARD_TRIGGERS[@]+"${HARD_TRIGGERS[@]}"}" )
for p in "${PROMOTED_FROM_SOFT[@]+"${PROMOTED_FROM_SOFT[@]}"}"; do
    EFFECTIVE_TRIGGERS+=( "${p} (new pub fn / pub trait)" )
done

if (( ${#EFFECTIVE_TRIGGERS[@]} == 0 )); then
    note "no protocol change detected (no hard triggers, no promoted soft triggers)"
    exit 0
fi

# --- Spec-modification check ---------------------------------------------

if (( ${#TLA_MODIFIED[@]} > 0 )); then
    note "protocol change detected (triggers: ${EFFECTIVE_TRIGGERS[*]}); \
.tla modified: ${TLA_MODIFIED[*]} — PASS"
    exit 0
fi

# --- Waiver check --------------------------------------------------------

if [[ -n "${COMMIT_MSG_FILE:-}" && -f "${COMMIT_MSG_FILE}" ]]; then
    waiver_line="$(grep -oE '\[no-tla-needed:[^]]+\]' "${COMMIT_MSG_FILE}" || true)"
    if [[ -n "${waiver_line}" ]]; then
        # Strip the brackets + label, then trim whitespace; require non-empty
        # rationale.
        reason="${waiver_line#\[no-tla-needed:}"
        reason="${reason%\]}"
        # Trim leading/trailing whitespace.
        reason="${reason#"${reason%%[![:space:]]*}"}"
        reason="${reason%"${reason##*[![:space:]]}"}"
        if [[ -n "${reason}" ]]; then
            note "protocol change detected; waiver present (no-tla-needed: ${reason}) — PASS"
            exit 0
        fi
        warn "[no-tla-needed:] tag found but reason is empty; waiver REJECTED"
    fi
fi

# --- BLOCK ---------------------------------------------------------------

err "protocol change detected, no TLA+ spec modified, no waiver in commit message"
err "triggers: ${EFFECTIVE_TRIGGERS[*]}"
err ""
err "REQUIRED: add or modify a TLA+ spec under specs/ that models the new"
err "          protocol surface. Each spec needs <Name>.tla + <Name>Bugged.cfg"
err "          (must violate invariant) + <Name>Fixed.cfg (must run clean)."
err "          See specs/README.md for conventions."
err ""
err "WAIVER: if the change is genuinely not a protocol change, add this line"
err "        to the commit message body:"
err "          [no-tla-needed: <one-sentence rationale>]"
err "        Examples of valid rationales: 'pure formatting change'; 'rename"
err "        of a private helper'; 'docs-only update to module comment'."
exit 1
