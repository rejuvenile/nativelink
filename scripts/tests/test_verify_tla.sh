#!/usr/bin/env bash
# Tests for scripts/verify_tla.sh (Layer 1 — SANY+TLC sweep).
#
# Each test sets up a temporary specs/ tree, invokes verify_tla.sh against it,
# and asserts the script's exit code and a substring of its output.
#
# Conventions:
#   * Layer 1 exit codes:
#       0 = all specs PASS (every Bugged.cfg violated its invariant, every
#           Fixed.cfg ran clean under TLC, every .tla parsed under SANY).
#       1 = one or more specs FAILED (a Fixed.cfg surfaced a violation, a
#           Bugged.cfg ran clean, or SANY rejected a .tla).
#       2 = configuration error (no tla2tools.jar found, or specs/ missing
#           when --strict is passed). The script defaults to skip-clean when
#           the tooling is unavailable, mirroring how the CI gate degrades
#           gracefully on a runner without Java.
#
# Skip-clean behavior is critical because the gate must not BLOCK on a
# repository that hasn't yet adopted specs (specs/ unmerged), nor on a
# runner where tla2tools.jar isn't installed. The gate's job is to enforce
# the rule WHEN the spec exists — not to invent specs that don't exist.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VERIFY_TLA="${REPO_ROOT}/scripts/verify_tla.sh"
TLA_TOOLS="${TLA_TOOLS_JAR:-/tmp/tla2tools.jar}"

# Reference specs ship as test fixtures alongside the test script. They are
# copies of representative entries from the in-flight specs branch
# (worktree-agent-a5f65f73). Vendoring them here keeps the test
# self-contained and deterministic — the test does not depend on a sibling
# worktree being present, nor on `specs/` existing in the repo at run-time.
REF_SPECS_DIR="${REPO_ROOT}/scripts/tests/fixtures/specs"

PASS=0
FAIL=0
declare -a FAILURES=()

assert_eq() {
    # $1 = label, $2 = expected, $3 = actual
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
    # $1 = label, $2 = substring, $3 = haystack
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

# --- Test setup helpers ----------------------------------------------------

mk_tmp_specs() {
    # Create a temp dir with the layout `verify_tla.sh` expects: a `specs/`
    # subdir holding *.tla and *.cfg files. Echoes the parent path so the
    # caller can pass it as TLA_SPECS_ROOT.
    local tmp
    tmp="$(mktemp -d)"
    mkdir -p "${tmp}/specs"
    echo "${tmp}"
}

copy_ref_spec() {
    # $1 = tmp parent (one returned by mk_tmp_specs), $2 = spec basename
    # (no extension). Copies <name>.tla + every <name>*.cfg from REF_SPECS_DIR.
    local tmp="$1"
    local name="$2"
    cp "${REF_SPECS_DIR}/${name}.tla" "${tmp}/specs/"
    # PinLifecycle uses V1/V2 cfg names; everything else uses Bugged/Fixed.
    if [[ "${name}" == "PinLifecycle" ]]; then
        cp "${REF_SPECS_DIR}/${name}V1.cfg" "${tmp}/specs/"
        cp "${REF_SPECS_DIR}/${name}V2.cfg" "${tmp}/specs/"
    else
        cp "${REF_SPECS_DIR}/${name}Bugged.cfg" "${tmp}/specs/" 2>/dev/null \
            || cp "${REF_SPECS_DIR}/${name}Bug.cfg" "${tmp}/specs/" 2>/dev/null \
            || true
        cp "${REF_SPECS_DIR}/${name}Fixed.cfg" "${tmp}/specs/"
    fi
}

# --- Tests -----------------------------------------------------------------

# T1: tla2tools.jar present, no specs/ dir present at all → exit 0 with
#     "no specs to verify" message. The gate should NOT block a repository
#     that hasn't adopted specs yet.
test_no_specs_dir_exits_clean() {
    local tmp
    tmp="$(mktemp -d)"
    local out rc
    set +e
    out="$(TLA_TOOLS_JAR="${TLA_TOOLS}" TLA_SPECS_ROOT="${tmp}" \
           "${VERIFY_TLA}" 2>&1)"
    rc=$?
    set -e
    assert_eq "T1.rc:no-specs-dir-exits-clean" "0" "${rc}"
    assert_contains "T1.msg:no-specs-dir-mentions-skip" "no specs/" "${out}"
}

# T2: specs/ present but empty → also clean.
test_empty_specs_dir_exits_clean() {
    local tmp
    tmp="$(mk_tmp_specs)"
    local out rc
    set +e
    out="$(TLA_TOOLS_JAR="${TLA_TOOLS}" TLA_SPECS_ROOT="${tmp}" \
           "${VERIFY_TLA}" 2>&1)"
    rc=$?
    set -e
    assert_eq "T2.rc:empty-specs-dir-exits-clean" "0" "${rc}"
    assert_contains "T2.msg:empty-mentions-zero" "0 spec" "${out}"
}

# T3: one valid spec (PinLifecycle: V1 PASS, V2 FAIL-as-designed) → exit 0,
#     output reports both expected outcomes met. This is the COMPOSITE
#     pass case — both bugged and fixed configs behaved as their headers
#     declare.
test_one_valid_spec_passes() {
    if [[ ! -f "${TLA_TOOLS}" ]]; then
        echo "SKIP T3: tla2tools.jar not at ${TLA_TOOLS}"
        return
    fi
    local tmp
    tmp="$(mk_tmp_specs)"
    copy_ref_spec "${tmp}" "PinLifecycle"
    local out rc
    set +e
    out="$(TLA_TOOLS_JAR="${TLA_TOOLS}" TLA_SPECS_ROOT="${tmp}" \
           "${VERIFY_TLA}" 2>&1)"
    rc=$?
    set -e
    assert_eq "T3.rc:one-valid-spec-passes" "0" "${rc}"
    assert_contains "T3.sany:reports-sany-pass" "SANY" "${out}"
    assert_contains "T3.tlc:reports-tlc-bugged-violation" \
        "as expected" "${out}"
}

# T4: one BROKEN spec — Fixed config that should pass actually surfaces a
#     violation. Simulated by copying PinLifecycle.tla, supplying both a
#     genuine Bugged.cfg (so the "no bugged present" guard does NOT fire)
#     AND a Fixed.cfg that has the V2 (genuinely-violating) constants. The
#     verifier MUST detect the fixed-config-violated mismatch and BLOCK.
#     Expected: exit 1 with a clear failure line citing the spec.
#     This is the assertion that catches bug pattern "spec author labelled
#     a config Fixed but the TLC run actually violates an invariant."
test_broken_spec_fails() {
    if [[ ! -f "${TLA_TOOLS}" ]]; then
        echo "SKIP T4: tla2tools.jar not at ${TLA_TOOLS}"
        return
    fi
    local tmp
    tmp="$(mk_tmp_specs)"
    cp "${REF_SPECS_DIR}/PinLifecycle.tla" "${tmp}/specs/"
    # Real bugged so the bug-presence guard is satisfied.
    cp "${REF_SPECS_DIR}/PinLifecycleV2.cfg" "${tmp}/specs/PinLifecycleBugged.cfg"
    # The "fixed" file actually contains BUGGED constants — this is the
    # broken case the gate must catch. Reusing V2 here is legitimate
    # because the script classifies by FILENAME, not by content.
    cp "${REF_SPECS_DIR}/PinLifecycleV2.cfg" "${tmp}/specs/PinLifecycleFixed.cfg"
    local out rc
    set +e
    out="$(TLA_TOOLS_JAR="${TLA_TOOLS}" TLA_SPECS_ROOT="${tmp}" \
           "${VERIFY_TLA}" 2>&1)"
    rc=$?
    set -e
    assert_eq "T4.rc:broken-spec-fails" "1" "${rc}"
    assert_contains "T4.msg:cites-fixed-violation" "expected clean" "${out}"
    assert_contains "T4.msg:cites-spec-name" "PinLifecycle" "${out}"
}

# T5: tla2tools.jar missing → exit 0 (skip-clean) with a notice. We do
#     this with a fake path; the script must NOT block CI on a runner
#     without Java/TLA installed (the gate is best-effort on environments
#     that haven't provisioned the tool).
test_missing_tla_tools_skips_clean() {
    local tmp
    tmp="$(mk_tmp_specs)"
    copy_ref_spec "${tmp}" "PinLifecycle"
    local out rc
    set +e
    out="$(TLA_TOOLS_JAR="/nonexistent/tla2tools.jar" TLA_SPECS_ROOT="${tmp}" \
           "${VERIFY_TLA}" 2>&1)"
    rc=$?
    set -e
    assert_eq "T5.rc:no-tools-skip-clean" "0" "${rc}"
    assert_contains "T5.msg:cites-tools-missing" "tla2tools.jar" "${out}"
}

# T6: tla2tools.jar missing AND --strict — must FAIL (exit 2). This is the
#     mode CI uses when the runner is provisioned and we want missing-tool
#     to be a config error rather than a silent skip.
test_strict_mode_blocks_on_missing_tools() {
    local tmp
    tmp="$(mk_tmp_specs)"
    copy_ref_spec "${tmp}" "PinLifecycle"
    local out rc
    set +e
    out="$(TLA_TOOLS_JAR="/nonexistent/tla2tools.jar" TLA_SPECS_ROOT="${tmp}" \
           "${VERIFY_TLA}" --strict 2>&1)"
    rc=$?
    set -e
    assert_eq "T6.rc:strict-blocks-on-missing-tools" "2" "${rc}"
}

# --- Runner ----------------------------------------------------------------

main() {
    if [[ ! -x "${VERIFY_TLA}" ]]; then
        echo "ERROR: ${VERIFY_TLA} missing or not executable"
        exit 1
    fi
    test_no_specs_dir_exits_clean
    test_empty_specs_dir_exits_clean
    test_one_valid_spec_passes
    test_broken_spec_fails
    test_missing_tla_tools_skips_clean
    test_strict_mode_blocks_on_missing_tools
    echo
    echo "Layer 1 tests: ${PASS} pass, ${FAIL} fail"
    if (( FAIL > 0 )); then
        printf '%s\n' "${FAILURES[@]}"
        exit 1
    fi
}

main "$@"
