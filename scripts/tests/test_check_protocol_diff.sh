#!/usr/bin/env bash
# Tests for scripts/check_protocol_diff.sh (Layer 2 — protocol-change
# heuristic detection + spec-modification gate).
#
# The script consumes the CHANGED-FILES list (one path per line on stdin)
# and the optional COMMIT_MSG_FILE env var. It exits:
#   0 = no protocol change detected, OR a protocol change was detected and
#       at least one *.tla file was also modified, OR the commit message
#       has the [no-tla-needed: <reason>] waiver.
#   1 = protocol change detected, no .tla modified, no waiver — BLOCK.
#
# Heuristic triggers (kept in a single shell function for testability):
#   * nativelink-service/src/{worker_api_server,cas_server,bytestream_server}.rs
#   * nativelink-worker/src/{local_worker,running_actions_manager}.rs
#   * any .proto file under nativelink-proto/
#   * nativelink-util/src/store_trait.rs
#   * any *_store.rs whose diff adds a new `pub fn` (caller passes the diff
#     fragment via the FULL_DIFF env var; if unset, the *_store.rs match is
#     a SOFT trigger that requires confirming with --diff-file)
#
# These triggers were chosen from the spec authoring discussion:
# cross-component messaging entry points + the trait definition (because
# wrapper override gaps are a known bug class — see TraitDefaultNoop.tla).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/check_protocol_diff.sh"

PASS=0
FAIL=0
declare -a FAILURES=()

assert_eq() {
    if [[ "$2" == "$3" ]]; then
        PASS=$((PASS + 1))
        echo "PASS: $1"
    else
        FAIL=$((FAIL + 1))
        FAILURES+=("$1: expected='$2' actual='$3'")
        echo "FAIL: $1 — expected='$2' actual='$3'"
    fi
}

assert_contains() {
    if [[ "$3" == *"$2"* ]]; then
        PASS=$((PASS + 1))
        echo "PASS: $1"
    else
        FAIL=$((FAIL + 1))
        FAILURES+=("$1: substring='$2' not in output")
        echo "FAIL: $1 — substring='$2' missing"
        echo "----- output begin -----"
        echo "$3"
        echo "----- output end   -----"
    fi
}

run_script() {
    # Pipes the changed-files list (passed as $1, newline-separated) into
    # the script. Optional $2 = commit message file path, $3 = extra args.
    local files="$1"
    local commit_msg="${2:-}"
    local extra="${3:-}"
    if [[ -n "${commit_msg}" ]]; then
        printf '%s' "${files}" | COMMIT_MSG_FILE="${commit_msg}" "${SCRIPT}" ${extra} 2>&1
    else
        printf '%s' "${files}" | "${SCRIPT}" ${extra} 2>&1
    fi
}

# --- Tests -----------------------------------------------------------------

# T1: trivial diff (README change only) → not a protocol change → PASS.
test_docs_only_passes() {
    local out rc
    set +e
    out="$(run_script $'README.md\nCLAUDE.md\n')"
    rc=$?
    set -e
    assert_eq "T1.rc:docs-only-passes" "0" "${rc}"
    assert_contains "T1.msg:no-trigger-noted" "no protocol change" "${out}"
}

# T2: worker_api_server.rs touched, no .tla → BLOCK.
test_worker_api_server_without_tla_blocks() {
    local out rc
    set +e
    out="$(run_script $'nativelink-service/src/worker_api_server.rs\n')"
    rc=$?
    set -e
    assert_eq "T2.rc:worker-api-without-tla-blocks" "1" "${rc}"
    assert_contains "T2.msg:cites-trigger-file" "worker_api_server.rs" "${out}"
    assert_contains "T2.msg:cites-tla-required" "specs/" "${out}"
}

# T3: worker_api_server.rs + *.tla → PASS.
test_worker_api_server_with_tla_passes() {
    local out rc
    set +e
    out="$(run_script $'nativelink-service/src/worker_api_server.rs\nspecs/MyProtocol.tla\n')"
    rc=$?
    set -e
    assert_eq "T3.rc:worker-api-with-tla-passes" "0" "${rc}"
    assert_contains "T3.msg:detects-tla" "MyProtocol.tla" "${out}"
}

# T4: cas_server.rs change + waiver in commit message → PASS.
test_waiver_overrides_block() {
    local commit_msg
    commit_msg="$(mktemp)"
    cat > "${commit_msg}" <<'EOF'
cas_server: tighten error message wording

Pure cosmetic — error message text only. No protocol change.
[no-tla-needed: pure cosmetic error-message change]
EOF
    local out rc
    set +e
    out="$(run_script $'nativelink-service/src/cas_server.rs\n' "${commit_msg}")"
    rc=$?
    set -e
    rm -f "${commit_msg}"
    assert_eq "T4.rc:waiver-passes" "0" "${rc}"
    assert_contains "T4.msg:cites-waiver" "no-tla-needed" "${out}"
}

# T5: bytestream_server.rs touched (another trigger) → BLOCK.
test_bytestream_server_without_tla_blocks() {
    local out rc
    set +e
    out="$(run_script $'nativelink-service/src/bytestream_server.rs\n')"
    rc=$?
    set -e
    assert_eq "T5.rc:bytestream-without-tla-blocks" "1" "${rc}"
}

# T6: local_worker.rs touched → BLOCK.
test_local_worker_without_tla_blocks() {
    local out rc
    set +e
    out="$(run_script $'nativelink-worker/src/local_worker.rs\n')"
    rc=$?
    set -e
    assert_eq "T6.rc:local-worker-without-tla-blocks" "1" "${rc}"
}

# T7: a .proto file changes → BLOCK.
test_proto_change_without_tla_blocks() {
    local out rc
    set +e
    out="$(run_script $'nativelink-proto/com/github/trace_machina/nativelink/remote_execution/worker_api.proto\n')"
    rc=$?
    set -e
    assert_eq "T7.rc:proto-without-tla-blocks" "1" "${rc}"
    assert_contains "T7.msg:cites-proto" "proto" "${out}"
}

# T8: store_trait.rs touched → BLOCK (trait wrapper-default class).
test_store_trait_without_tla_blocks() {
    local out rc
    set +e
    out="$(run_script $'nativelink-util/src/store_trait.rs\n')"
    rc=$?
    set -e
    assert_eq "T8.rc:store-trait-without-tla-blocks" "1" "${rc}"
}

# T9: a NEW .tla file added counts as a TLA mod → PASS.
test_new_tla_added_counts() {
    local out rc
    set +e
    out="$(run_script $'nativelink-service/src/cas_server.rs\nspecs/NewProtocol.tla\nspecs/NewProtocolFixed.cfg\nspecs/NewProtocolBugged.cfg\n')"
    rc=$?
    set -e
    assert_eq "T9.rc:new-tla-counts" "0" "${rc}"
}

# T10: a fast_slow_store.rs change alone (without --diff-file) is a SOFT
#      trigger; should produce a WARN (rc 0) with a hint that store_trait
#      auditing is recommended. The hard trigger requires confirming the
#      change adds new pub fn (not a docs/comment-only edit).
test_store_change_soft_warn() {
    local out rc
    set +e
    out="$(run_script $'nativelink-store/src/fast_slow_store.rs\n')"
    rc=$?
    set -e
    assert_eq "T10.rc:store-soft-warn-passes" "0" "${rc}"
    assert_contains "T10.msg:warns-on-store" "WARN" "${out}"
}

# T11: a fast_slow_store.rs change with a --diff-file showing a new pub fn
#      → BLOCK.
test_store_change_with_pub_fn_blocks() {
    local diff_file
    diff_file="$(mktemp)"
    cat > "${diff_file}" <<'EOF'
diff --git a/nativelink-store/src/fast_slow_store.rs b/nativelink-store/src/fast_slow_store.rs
index abc..def 100644
--- a/nativelink-store/src/fast_slow_store.rs
+++ b/nativelink-store/src/fast_slow_store.rs
@@ -100,6 +100,10 @@ impl FastSlowStore {
         self.fast_store.get(key).await
     }
+    pub fn new_cross_component_handshake(&self, peer: PeerId) -> Result<(), Error> {
+        self.peer_registry.register(peer)?;
+        Ok(())
+    }
 }
EOF
    local out rc
    set +e
    out="$(run_script $'nativelink-store/src/fast_slow_store.rs\n' "" "--diff-file ${diff_file}")"
    rc=$?
    set -e
    rm -f "${diff_file}"
    assert_eq "T11.rc:store-pub-fn-blocks" "1" "${rc}"
    assert_contains "T11.msg:cites-new-pub-fn" "pub fn" "${out}"
}

# --- Runner ----------------------------------------------------------------

main() {
    if [[ ! -x "${SCRIPT}" ]]; then
        echo "ERROR: ${SCRIPT} missing or not executable"
        exit 1
    fi
    test_docs_only_passes
    test_worker_api_server_without_tla_blocks
    test_worker_api_server_with_tla_passes
    test_waiver_overrides_block
    test_bytestream_server_without_tla_blocks
    test_local_worker_without_tla_blocks
    test_proto_change_without_tla_blocks
    test_store_trait_without_tla_blocks
    test_new_tla_added_counts
    test_store_change_soft_warn
    test_store_change_with_pub_fn_blocks
    echo
    echo "Layer 2 tests: ${PASS} pass, ${FAIL} fail"
    if (( FAIL > 0 )); then
        printf '%s\n' "${FAILURES[@]}"
        exit 1
    fi
}

main "$@"
