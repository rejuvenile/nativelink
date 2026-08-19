#!/usr/bin/env zsh
# espill-refault-monitor.sh — validate the E-core spill fix (equilibrium 4→10) AND
# measure the refault rate at the new concurrency, during a LIVE heavy build.
# READ-ONLY. Start it, then kick a heavy build (a --config=dbg build is the refault-
# critical case: #64 refault-at-10000 NAK-stormed 8/10 workers on dbg builds).
#
#   Usage: espill-refault-monitor.sh [interval_s=3] [max_s=1800] [out.csv]
#
# E-SPILL WORKING  = total_running climbs ABOVE 40 (old 4/worker cap) toward 100,
#                    AND e_load rises (E-cores actually used), while p_load stays high.
# REFAULT THRESHOLD = watch max refault_ewma / rate_last across the fleet under load;
#                    set memory_gate_refault_confirm_rate ABOVE the observed peak + margin.
# DANGER           = nak_free_floor or nak_refault increments (backstop firing / near-OOM),
#                    or a worker vanishing from the scrape (OOM-SIGKILL/restart).
set -uo pipefail
CERT=/srv/casdata/nativelink/tls/clients/user.crt
KEY=/srv/casdata/nativelink/tls/clients/user.key
SRV="https://cache.example.com:50061/metrics"
WORKERS=(worker-01 worker-02 worker-03 worker-04 worker-05 \
         worker-06 worker-07 worker-08 worker-09 worker-10)
INT="${1:-3}"; MAX="${2:-1800}"; OUT="${3:-/tmp/espill-refault-$(date +%s).csv}"
srv(){ curl -s -m6 -k --http2-prior-knowledge --cert "$CERT" --key "$KEY" "$SRV" 2>/dev/null; }
wrk(){ curl -s -m3 --http2-prior-knowledge "http://$1.local:50061/metrics" 2>/dev/null; }
trap 'printf "\n[stopped] %s rows -> %s\n" "$n" "$OUT"; exit 0' INT TERM

echo "ts,elapsed,queued,total_running,workers_up,p_avg,e_avg,e_max,rf_ewma_max,rf_rate_max,nak_ff_d,nak_rf_d,verdict" > "$OUT"
printf 'monitoring -> %s (int %ss). Kick a heavy build (dbg = refault-critical). Ctrl-C to stop.\n' "$OUT" "$INT"
prev_ff=0; prev_rf=0; t0=0; start=$SECONDS; n=0
while (( SECONDS-start < MAX )); do
  M="$(srv)"
  q=$(printf '%s\n' "$M"  | grep -E '_sorted_action_infos_queued_count '     | grep -v '^#' | awk '{print $2}' | sort -rn | head -1); q=${q:-0}
  tr=$(printf '%s\n' "$M" | grep -E 'total_running_actions ' | grep -v '^#' | awk '{print $2}' | sort -rn | head -1); tr=${tr:-0}
  PL=(${(f)"$(printf '%s\n' "$M" | grep -oE 'workers_workers_[0-9a-f_]+_p_core_load_pct [0-9]+' | sort -u | awk '{print $NF}')"})
  EL=(${(f)"$(printf '%s\n' "$M" | grep -oE 'workers_workers_[0-9a-f_]+_e_core_load_pct [0-9]+' | sort -u | awk '{print $NF}')"})
  ps=0; for v in "${PL[@]:-0}"; do ((ps+=v)); done; pavg=$(( ${#PL[@]}>0 ? ps/${#PL[@]} : 0 ))
  es=0; emax=0; for v in "${EL[@]:-0}"; do ((es+=v)); (( v>emax )) && emax=$v; done; eavg=$(( ${#EL[@]}>0 ? es/${#EL[@]} : 0 ))
  # per-worker refault + fleet NAK totals
  up=0; rf_ewma_max=0; rf_rate_max=0; ff=0; rf=0
  for w in "${WORKERS[@]}"; do
    WM="$(wrk "$w")"; [ -z "$WM" ] && continue; ((up++))
    e=$(printf '%s\n' "$WM" | grep -E 'memory_gate_refault_ewma '        | grep -v '^#' | awk '{print $2+0}')
    r=$(printf '%s\n' "$WM" | grep -E 'memory_gate_refault_rate_last '   | grep -v '^#' | awk '{print $2+0}')
    (( ${e:-0} > rf_ewma_max )) && rf_ewma_max=${e:-0}
    (( ${r:-0} > rf_rate_max )) && rf_rate_max=${r:-0}
    ff=$(( ff + $(printf '%s\n' "$WM" | grep -E 'memory_gate_nak_free_floor_total ' | grep -v '^#' | awk '{s+=$2} END{print s+0}') ))
    rf=$(( rf + $(printf '%s\n' "$WM" | grep -E 'memory_gate_nak_refault_total '   | grep -v '^#' | awk '{s+=$2} END{print s+0}') ))
  done
  ff_d=$((ff-prev_ff)); prev_ff=$ff; rf_d=$((rf-prev_rf)); prev_rf=$rf
  (( t0==0 )) && (( q>0 || tr>10 )) && { t0=$SECONDS; echo "[t0] load started $(date +%T)"; }
  el=$(( t0>0 ? SECONDS-t0 : 0 ))
  if   (( q<=1 && tr<=10 )); then V="IDLE"
  elif (( ff_d>0 || rf_d>0 )); then V="!! MEMORY-GATE-NAK (ff+$ff_d rf+$rf_d) — near-OOM/backstop firing"
  elif (( tr>40 && emax>=30 )); then V="E-SPILL WORKING (run>40, e_load up)"
  elif (( tr<=40 && pavg>=80 )); then V="still ~4/worker? (run<=40, P pegged) — check e_spill live"
  else V="MIXED"; fi
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$(date +%T)" "$el" "$q" "$tr" "$up" "$pavg" "$eavg" "$emax" "$rf_ewma_max" "$rf_rate_max" "$ff_d" "$rf_d" "$V" >> "$OUT"
  printf '%s q=%-4s run=%-4s | Pavg=%-3s Eavg=%-3s Emax=%-3s | rf_ewma_max=%-6s rf_rate_max=%-6s | nak ff+%s rf+%s | %s\n' \
    "$(date +%T)" "$q" "$tr" "$pavg" "$eavg" "$emax" "$rf_ewma_max" "$rf_rate_max" "$ff_d" "$rf_d" "$V"
  ((n++)); sleep "$INT"
done
printf '[done] %s rows -> %s\n' "$n" "$OUT"
