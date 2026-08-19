#!/usr/bin/env zsh
# synthetic-core-metric-validate.sh — pin a KNOWN load to a worker's P-cores, then
# its E-cores, and check what p_core_load_pct / e_core_load_pct actually report.
# Purpose: the p_headroom scheduler gate decides io_bound from p_core_load_pct; a
# live build showed it 9-30% while the operator watched P-cores pegged -> the metric
# is suspect. This validates it against ground truth before we trust/fix the gate.
#
#   Usage: synthetic-core-metric-validate.sh [target-worker=worker-10]
#
# SAFETY (a prior incident orphaned 64 spinners -> 6200% CPU/14h): every spinner is
# launched with a self-kill watchdog ON THE WORKER (dies after HOLD s regardless of
# this script or the ssh session), PLUS an explicit kill after each phase, PLUS an
# EXIT/INT/TERM trap. macOS has no `timeout`, hence the watchdog.
set -uo pipefail
CERT=/srv/casdata/nativelink/tls/clients/user.crt
KEY=/srv/casdata/nativelink/tls/clients/user.key
SERVER="https://cache.example.com:50061/metrics"
TARGET="${1:-worker-10}"
HOLD=30       # worker-side spinner self-kill watchdog (seconds) — hard backstop
SETTLE=12     # wait after launch before scraping (steady state + worker->server heartbeat)
# zsh: an array, not a string — zsh does NOT word-split an unquoted scalar.
ssh_cmd=(ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=6 -- ${TARGET}.local)

srv() { curl -s -m6 -k --http2-prior-knowledge --cert "$CERT" --key "$KEY" "$SERVER" 2>/dev/null; }

# uuid -> cpu_load map (one line "uuid cpu"), deduped
cpu_map() { srv | grep -oE 'workers_workers_[0-9a-f_]+_cpu_load_pct [0-9]+' | sort -u \
            | sed -E 's/workers_workers_([0-9a-f_]+)_cpu_load_pct ([0-9]+)/\1 \2/'; }
# read p/e/cpu for a specific uuid from a fresh scrape
read_uuid() { local M u="$1"
  M="$(srv)"
  local p e c
  p=$(printf '%s\n' "$M" | grep "workers_workers_${u}_p_core_load_pct "  | grep -v '^#' | awk '{print $2}' | head -1)
  e=$(printf '%s\n' "$M" | grep "workers_workers_${u}_e_core_load_pct "  | grep -v '^#' | awk '{print $2}' | head -1)
  c=$(printf '%s\n' "$M" | grep "workers_workers_${u}_cpu_load_pct "     | grep -v '^#' | awk '{print $2}' | head -1)
  echo "${p:-?} ${e:-?} ${c:-?}"
}

# launch N spinners on the worker; qos=fg (default->P) or bg (background->E);
# self-killing watchdog on the worker so nothing orphans.
launch() { local n="$1" qos="$2"
  "${ssh_cmd[@]}" "bash -s" -- "$n" "$qos" "$HOLD" <<'REMOTE'
n=$1; qos=$2; h=$3; rm -f /tmp/synbench_pids
for i in $(seq 1 "$n"); do
  if [ "$qos" = bg ]; then taskpolicy -c background nohup yes >/dev/null 2>&1 &
  else nohup yes >/dev/null 2>&1 & fi
  echo $! >> /tmp/synbench_pids
done
# hard self-kill watchdog — independent of the parent / ssh session
nohup bash -c "sleep $h; kill \$(cat /tmp/synbench_pids 2>/dev/null) 2>/dev/null; pkill -x yes 2>/dev/null; rm -f /tmp/synbench_pids" >/dev/null 2>&1 &
disown -a
echo "  launched $n $qos spinner(s) on $(hostname -s), watchdog ${h}s"
REMOTE
}
killspin() { "${ssh_cmd[@]}" 'kill $(cat /tmp/synbench_pids 2>/dev/null) 2>/dev/null; pkill -x yes 2>/dev/null; rm -f /tmp/synbench_pids' 2>/dev/null; }
trap 'echo "[cleanup] killing spinners on '"$TARGET"'"; killspin' EXIT INT TERM

echo "=== target worker: $TARGET  (P=4 E=6) ==="
echo "=== baseline: capturing idle cpu map ==="
typeset -A BASE; while read -r u c; do BASE[$u]=$c; done < <(cpu_map)
echo "  baseline workers: ${#BASE[@]}"

phase() { # $1=label $2=N $3=qos $4=expect
  echo "=== PHASE: $1  (launch $2 $3 spinners) ==="
  launch "$2" "$3"
  echo "  settling ${SETTLE}s (steady state + heartbeat report)..."; sleep "$SETTLE"
  # identify the loaded worker = biggest cpu increase from baseline
  local best_u="" best_d=-1 u c d
  while read -r u c; do d=$(( c - ${BASE[$u]:-0} )); (( d>best_d )) && { best_d=$d; best_u=$u; }; done < <(cpu_map)
  read p e cc <<<"$(read_uuid "$best_u")"
  echo "  loaded worker ${best_u:0:8} (cpu +${best_d} from baseline): metric reports  p_core_load=${p}%  e_core_load=${e}%  cpu_load=${cc}%"
  echo "  EXPECT: $4"
  eval "P_$3=$p; E_$3=$e; C_$3=$cc"
  killspin; sleep 4
}

phase "P-CORE saturation" 4 fg "p_core_load HIGH, e_core_load LOW"
phase "E-CORE saturation" 6 bg "e_core_load HIGH, p_core_load LOW"

echo "=== VERDICT ==="
echo "  P-load phase:  p=${P_fg}% e=${E_fg}%   |   E-load phase: p=${P_bg}% e=${E_bg}%"
awk -v pf="${P_fg:-0}" -v ef="${E_fg:-0}" -v pb="${P_bg:-0}" -v eb="${E_bg:-0}" 'BEGIN{
  pf+=0;ef+=0;pb+=0;eb+=0;
  # correct: P-phase -> p high & e low ; E-phase -> e high & p low
  if (pf>=50 && ef<pf*0.5 && eb>=50 && pb<eb*0.5) print "  -> metric CORRECT (P-phase read p-high; E-phase read e-high)";
  else if (ef>pf && pb>eb) print "  -> metric SWAPPED (P-load landed in e_core_load_pct and vice versa) !!";
  else if (pf<40 && ef<40 && pb<40 && eb<40) print "  -> metric UNDER-REPORTS (known saturation read as low load) !!";
  else print "  -> AMBIGUOUS — inspect the two rows above (partial/unexpected pattern)";
}'
echo "  (P-cluster theoretical max at 4 pegged cores; E-cluster at 6 pegged cores.)"
