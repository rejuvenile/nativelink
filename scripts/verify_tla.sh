#!/usr/bin/env bash
# verify_tla.sh — Layer 1 of the protocol-verification gate.
#
# Walks `specs/` (or $TLA_SPECS_ROOT/specs/), and for each `<Name>.tla`:
#   1. Runs SANY (parser + name resolution). Must succeed.
#   2. For each accompanying `<Name>*.cfg`, runs TLC. The script CLASSIFIES
#      each .cfg by filename and asserts the TLC outcome matches:
#        * <Name>Bugged.cfg / <Name>Bug.cfg / <Name>V2.cfg → must report
#          a violation (invariant or temporal property).
#        * <Name>Fixed.cfg / <Name>V1.cfg → must report
#          "Model checking completed. No error has been found."
#      Other .cfg files are run but treated as informational only (no PASS/
#      FAIL assertion); they're typically variant configs the spec author
#      added for ad-hoc exploration. The classifier mirrors the README's
#      naming conventions in `.claude/worktrees/agent-a5f65f73/specs/`.
#
# Exit codes:
#   0 = all classified configs matched their expected outcome
#   1 = one or more configs did not match expected outcome (FAIL, gate blocks)
#   2 = configuration error in --strict mode (missing tla2tools.jar, missing
#       specs/ when --strict-specs is requested, etc.)
#
# Skip-clean policy: when not in --strict mode, the script returns 0 with a
# clear message in any of these cases:
#   * tla2tools.jar not at $TLA_TOOLS_JAR (or default `/tmp/tla2tools.jar`)
#   * specs/ directory does not exist
#   * specs/ exists but contains zero .tla files
# The rationale: this gate is REQUIRED only when a spec exists. A repo that
# hasn't yet adopted any specs (or a runner without TLC installed) is not in
# scope for this layer. CI sets --strict to surface tooling gaps as blockers.
#
# Env vars:
#   TLA_TOOLS_JAR   path to tla2tools.jar (default: /tmp/tla2tools.jar)
#   TLA_SPECS_ROOT  parent dir containing specs/ (default: repo root)
#   TLC_TIMEOUT     per-spec wall-clock timeout in seconds (default: 120;
#                   enforced via `timeout` per CLAUDE.md "Test Invocations")

set -euo pipefail

STRICT=0
STRICT_SPECS=0

while (( $# > 0 )); do
    case "$1" in
        --strict)        STRICT=1; STRICT_SPECS=1; shift ;;
        --strict-tools)  STRICT=1; shift ;;
        --strict-specs)  STRICT_SPECS=1; shift ;;
        -h|--help)
            sed -n '1,50p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "verify_tla.sh: unknown arg '$1'" >&2
            exit 2
            ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPECS_ROOT="${TLA_SPECS_ROOT:-${REPO_ROOT}}"
SPECS_DIR="${SPECS_ROOT}/specs"
TLA_TOOLS="${TLA_TOOLS_JAR:-/tmp/tla2tools.jar}"
TLC_TIMEOUT="${TLC_TIMEOUT:-120}"

note()  { printf '[verify_tla] %s\n' "$*"; }
warn()  { printf '[verify_tla][WARN] %s\n' "$*"; }
err()   { printf '[verify_tla][FAIL] %s\n' "$*"; }

# --- Skip-clean precondition gates ----------------------------------------

if [[ ! -f "${TLA_TOOLS}" ]]; then
    if (( STRICT )); then
        err "tla2tools.jar not found at ${TLA_TOOLS}; --strict mode requires it"
        exit 2
    fi
    note "tla2tools.jar not at ${TLA_TOOLS}; skipping (install with: \
wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar -O ${TLA_TOOLS})"
    exit 0
fi

if [[ ! -d "${SPECS_DIR}" ]]; then
    if (( STRICT_SPECS )); then
        err "no specs/ directory at ${SPECS_DIR}; --strict-specs requires it"
        exit 2
    fi
    note "no specs/ directory at ${SPECS_DIR}; skipping (gate enforces TLA \
verification only when a spec is present in the repo)"
    exit 0
fi

# Spec discovery — every <Name>.tla in specs/ becomes one work item.
# Use a portable shell glob; `mapfile` is bash-only, but this script is
# already #!/usr/bin/env bash.
shopt -s nullglob
TLA_FILES=( "${SPECS_DIR}"/*.tla )
shopt -u nullglob

if (( ${#TLA_FILES[@]} == 0 )); then
    note "0 spec files in ${SPECS_DIR}; nothing to verify"
    exit 0
fi

note "discovered ${#TLA_FILES[@]} spec(s) in ${SPECS_DIR}"

# --- Per-spec verification loop -------------------------------------------

# Classify a .cfg basename into one of {bugged, fixed, other}.
classify_cfg() {
    local base="$1"  # e.g. PinLifecycleV2.cfg, FailedSlowWritesRetryFixed.cfg
    case "${base}" in
        *Bugged.cfg|*Bug.cfg|*V2.cfg) echo "bugged" ;;
        *Fixed.cfg|*V1.cfg)           echo "fixed" ;;
        *)                             echo "other" ;;
    esac
}

# Run TLC for one spec+cfg pair, return a classification of the outcome.
# Echoes one of: clean, violated, error.
#   clean    = "Model checking completed. No error has been found."
#   violated = "Invariant ... is violated." OR "Temporal property ... was violated."
#   error    = anything else (parse error, TLC crash, timeout, etc.)
# Side effect: appends full TLC output to ${OUTLOG}.
run_tlc() {
    local spec_path="$1"
    local cfg_basename="$2"
    local spec_basename
    spec_basename="$(basename "${spec_path}" .tla)"
    local workdir
    workdir="$(mktemp -d)"
    # Copy spec + cfg into a clean workdir so trace files don't pollute
    # the source tree, and so concurrent runs don't collide.
    cp "${spec_path}" "${workdir}/"
    cp "${SPECS_DIR}/${cfg_basename}" "${workdir}/"
    local raw rc
    set +e
    raw="$(cd "${workdir}" && timeout "${TLC_TIMEOUT}" \
        java -XX:+UseParallelGC -cp "${TLA_TOOLS}" tlc2.TLC \
        -config "${cfg_basename}" "${spec_basename}" 2>&1)"
    rc=$?
    set -e
    rm -rf "${workdir}"
    {
        printf '\n=== TLC output: %s with %s (rc=%d) ===\n' \
            "${spec_basename}" "${cfg_basename}" "${rc}"
        printf '%s\n' "${raw}"
    } >> "${OUTLOG}"
    if [[ "${raw}" == *"is violated"* ]] || [[ "${raw}" == *"was violated"* ]]; then
        echo "violated"
    elif [[ "${raw}" == *"No error has been found"* ]]; then
        echo "clean"
    else
        echo "error"
    fi
}

run_sany() {
    local spec_path="$1"
    local raw rc
    set +e
    raw="$(timeout "${TLC_TIMEOUT}" java -cp "${TLA_TOOLS}" \
        tla2sany.SANY "${spec_path}" 2>&1)"
    rc=$?
    set -e
    {
        printf '\n=== SANY output: %s (rc=%d) ===\n' \
            "$(basename "${spec_path}")" "${rc}"
        printf '%s\n' "${raw}"
    } >> "${OUTLOG}"
    # SANY exits non-zero on parse / semantic errors; success is rc=0 AND
    # absence of "***Parse Error***" / "Semantic errors" lines. Be defensive
    # because SANY's exit-code convention has shifted across releases.
    if (( rc != 0 )); then return 1; fi
    if [[ "${raw}" == *"***Parse Error***"* ]]; then return 1; fi
    if [[ "${raw}" == *"Semantic errors"* ]]; then return 1; fi
    return 0
}

OUTLOG="$(mktemp -t verify_tla.XXXXXX.log)"
note "TLC/SANY full output: ${OUTLOG}"

OVERALL_FAIL=0
declare -i n_specs=0
declare -i n_pass=0
declare -i n_fail=0

for tla in "${TLA_FILES[@]}"; do
    n_specs+=1
    name="$(basename "${tla}" .tla)"
    note "spec ${name}: SANY..."
    if ! run_sany "${tla}"; then
        err "spec ${name}: SANY rejected"
        n_fail+=1
        OVERALL_FAIL=1
        continue
    fi
    note "spec ${name}: SANY passed"

    shopt -s nullglob
    cfgs=( "${SPECS_DIR}/${name}"*.cfg )
    shopt -u nullglob
    if (( ${#cfgs[@]} == 0 )); then
        warn "spec ${name}: no .cfg files (must have at least one Bugged + one Fixed); FAIL"
        n_fail+=1
        OVERALL_FAIL=1
        continue
    fi

    have_bugged=0
    have_fixed=0
    spec_fail=0
    for cfg_path in "${cfgs[@]}"; do
        cfg_base="$(basename "${cfg_path}")"
        cls="$(classify_cfg "${cfg_base}")"
        outcome="$(run_tlc "${tla}" "${cfg_base}")"
        case "${cls}:${outcome}" in
            bugged:violated)
                note "  ${cfg_base}: violation produced as expected"
                have_bugged=1
                ;;
            bugged:clean)
                err "  ${cfg_base}: expected violation but TLC ran clean"
                spec_fail=1; have_bugged=1
                ;;
            bugged:error)
                err "  ${cfg_base}: TLC errored (parse/timeout); see ${OUTLOG}"
                spec_fail=1; have_bugged=1
                ;;
            fixed:clean)
                note "  ${cfg_base}: clean as expected"
                have_fixed=1
                ;;
            fixed:violated)
                err "  ${cfg_base}: expected clean run but invariant was violated"
                spec_fail=1; have_fixed=1
                ;;
            fixed:error)
                err "  ${cfg_base}: TLC errored (parse/timeout); see ${OUTLOG}"
                spec_fail=1; have_fixed=1
                ;;
            other:*)
                note "  ${cfg_base}: informational (outcome=${outcome}; not classified)"
                ;;
        esac
    done

    if (( have_bugged == 0 )); then
        err "spec ${name}: no Bugged/Bug/V2 .cfg present (must demonstrate the bug class)"
        spec_fail=1
    fi
    if (( have_fixed == 0 )); then
        err "spec ${name}: no Fixed/V1 .cfg present (must demonstrate the clean run)"
        spec_fail=1
    fi
    if (( spec_fail )); then
        n_fail+=1
        OVERALL_FAIL=1
    else
        n_pass+=1
    fi
done

note "summary: ${n_pass}/${n_specs} specs PASS, ${n_fail} FAIL"
if (( OVERALL_FAIL )); then
    err "Layer 1 BLOCKED. Inspect ${OUTLOG} for TLC/SANY details."
    exit 1
fi
note "Layer 1 OK"
exit 0
