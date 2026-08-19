#!/usr/bin/env zsh
# sched-peak-capture.sh — sample scheduler + worker + fetch-path signals during a
# LIVE heavy build to settle the 2026-07-15 perf question: is the fleet
# SCHEDULER-CAPPED (p_headroom overcommit override defeated by p_load spikes),
# UNDER-FED, COMPUTE-SATURATED, or UPSTREAM-FETCH-BOUND (server egress saturated)?
# READ-ONLY — scrapes metrics + the p_headroom journal + /proc/net/dev; changes nothing.
#
#   Usage: sched-peak-capture.sh [interval_s=3] [max_duration_s=1200] [out.csv]
#   Start it, then kick off a heavy build. Anchors t0 at first queued>0.
#
# KEY discriminator this version adds:
#   server egress (eth0 tx MB/s) + worker->server fetch MB/s + mean input-fetch ms.
#   - queue backed up & workers capped near p_core_count(4) & egress HIGH  -> fetch path is the wall
#       (overcommitting workers won't help; enable peer-fetch to offload the server link)
#   - queue backed up & workers capped near 4     & egress has HEADROOM    -> the p_headroom cap is
#       the limiter (override defeated by p_load spikes >50%; smooth/raise the io_bound threshold)
#   Overcommit ceiling = p_core_count * override_factor = 4 * 4 = 16 (per-worker).
set -uo pipefail

CERT=/srv/casdata/nativelink/tls/clients/user.crt
KEY=/srv/casdata/nativelink/tls/clients/user.key
SERVER="https://cache.example.com:50061/metrics"
WORKERS=(worker-01 worker-02 worker-03 worker-04 worker-05 \
         worker-06 worker-07 worker-08 worker-09 worker-10)
NIC="${NIC:-eth0}"            # buildcache fleet-facing NIC (server egress)
INTERVAL="${1:-3}"
MAX_DURATION="${2:-1200}"
OUT="${3:-/tmp/sched-cap-$(date +%s).csv}"
P_IDLE=50 ; E_IDLE=30

srv() { curl -s -m6 -k --http2-prior-knowledge --cert "$CERT" --key "$KEY" "$SERVER" 2>/dev/null; }
wrk() { curl -s -m3 --http2-prior-knowledge "http://$1.local:50061/metrics" 2>/dev/null; }
nic_tx() { sed -n "s/^ *${NIC}: *//p" /proc/net/dev | awk '{print $9+0}'; }   # tx_bytes

trap 'printf "\n[stopped] %s rows in %s\n" "$n" "$OUT"; exit 0' INT TERM

echo "ts,elapsed_s,queued,executing,total_running,workers,p_idle,p_min,p_max,p_avg,e_idle,pressured,excl_d,excl_run_max,egress_MBps,srv_fetch_MBps,fetch_ms_mean,pf_win_d,verdict" > "$OUT"
printf 'capturing -> %s  (interval %ss, max %ss, NIC %s; Ctrl-C to stop)\nwaiting for load (t0 at first queued>0)...\n' "$OUT" "$INTERVAL" "$MAX_DURATION" "$NIC"

prev_win=0 ; prev_tx="$(nic_tx)" ; prev_fetched=0 ; prev_fms_sum=0 ; prev_fms_cnt=0
t0=0 ; start=$SECONDS ; n=0 ; last_j="$(date -u +%s)"
while (( SECONDS - start < MAX_DURATION )); do
  loop_t=$SECONDS
  M="$(srv)"
  q=$(printf '%s\n' "$M"  | grep -E '_sorted_action_infos_queued_count '    | grep -v '^#' | awk '{print $2}' | sort -rn | head -1)
  ex=$(printf '%s\n' "$M" | grep -E '_sorted_action_infos_executing_count ' | grep -v '^#' | awk '{print $2}' | sort -rn | head -1)
  tr=$(printf '%s\n' "$M" | grep -E '_scheduler_metrics_total_running_actions ' | grep -v '^#' | awk '{print $2}' | sort -rn | head -1)
  q=${q:-0}; ex=${ex:-0}; tr=${tr:-0}

  PL=(${(f)"$(printf '%s\n' "$M" | grep -oE 'workers_workers_[0-9a-f_]+_p_core_load_pct [0-9]+' | sort -u | awk '{print $NF}')"})
  EL=(${(f)"$(printf '%s\n' "$M" | grep -oE 'workers_workers_[0-9a-f_]+_e_core_load_pct [0-9]+' | sort -u | awk '{print $NF}')"})
  pressured=$(printf '%s\n' "$M" | grep -oE 'workers_workers_[0-9a-f_]+_(swap|disk)_pressured 1' | sort -u | wc -l | tr -d ' ')
  wn=${#PL[@]}; p_idle=0; p_min=999; p_max=0; p_sum=0
  for v in "${PL[@]:-}"; do [ -z "$v" ] && continue; (( v<P_IDLE )) && ((p_idle++)); (( v<p_min )) && p_min=$v; (( v>p_max )) && p_max=$v; ((p_sum+=v)); done
  (( wn>0 )) && p_avg=$((p_sum/wn)) || { p_avg=0; p_min=0; }
  e_idle=0; for v in "${EL[@]:-}"; do [ -z "$v" ] && continue; (( v<E_IDLE )) && ((e_idle++)); done

  # peer-fetch + worker->server fetch volume + fetch-wait, summed across the fleet
  win=0; fetched=0; fms_sum=0; fms_cnt=0
  for w in "${WORKERS[@]}"; do
    WM="$(wrk "$w")"; [ -z "$WM" ] && continue
    win=$((     win     + $(printf '%s\n' "$WM" | grep -E '_worker_proxy_peer_fetch_win_count '     | grep -v '^#' | awk '{s+=$2} END{print s+0}') ))
    fetched=$(( fetched + $(printf '%s\n' "$WM" | grep -E '_input_server_missing_fetched_bytes '    | grep -v '^#' | awk '{s+=$2} END{print s+0}') ))
    fms_sum=$(( fms_sum + $(printf '%s\n' "$WM" | grep -E 'dir_cache_construct_fetch_ms_sum '       | grep -v '^#' | awk '{s+=$2} END{print int(s)}') ))
    fms_cnt=$(( fms_cnt + $(printf '%s\n' "$WM" | grep -E 'dir_cache_construct_fetch_ms_count '     | grep -v '^#' | awk '{s+=$2} END{print s+0}') ))
  done
  win_d=$((win-prev_win)); prev_win=$win
  fetched_d=$((fetched-prev_fetched)); prev_fetched=$fetched
  fms_sum_d=$((fms_sum-prev_fms_sum)); prev_fms_sum=$fms_sum
  fms_cnt_d=$((fms_cnt-prev_fms_cnt)); prev_fms_cnt=$fms_cnt

  # server egress (eth0 tx) MB/s over the actual elapsed loop time
  tx_now="$(nic_tx)"; dt=$(( SECONDS-loop_t + INTERVAL )); (( dt<1 )) && dt=$INTERVAL
  egress=$(awk -v a="$tx_now" -v b="$prev_tx" -v s="$dt" 'BEGIN{d=a-b; if(d<0)d=0; printf "%.0f", d/s/1000000}'); prev_tx="$tx_now"
  srv_fetch=$(awk -v d="$fetched_d" -v s="$dt" 'BEGIN{if(d<0)d=0; printf "%.0f", d/s/1000000}')
  fetch_ms=$(awk -v s="$fms_sum_d" -v c="$fms_cnt_d" 'BEGIN{printf "%.0f", (c>0? s/c : 0)}')

  # p_headroom exclusions + the max running_actions at exclusion (vs the 16 ceiling)
  now_j="$(date -u +%s)"; since=$(( now_j-last_j )); (( since<1 )) && since=1; last_j="$now_j"
  EJ=$(journalctl --namespace=nativelink --since "${since} sec ago" 2>/dev/null | grep 'p_headroom_gate_exclusion')
  excl_d=$(printf '%s\n' "$EJ" | grep -c .)
  excl_run_max=$(printf '%s\n' "$EJ" | grep -oE 'running_actions[=:][0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1); excl_run_max=${excl_run_max:-0}

  (( t0==0 )) && (( q>0 || ex>0 || tr>3 )) && { t0=$SECONDS; echo "[t0] load started $(date +%T)"; }
  elapsed=$(( t0>0 ? SECONDS-t0 : 0 ))

  if   (( q<=1 && ex<=1 && tr<=3 )); then V="IDLE"
  elif (( q<=2 )); then V="UNDER-FED (queue~0)"
  elif (( p_idle==0 && p_min>=90 )); then V="COMPUTE-SATURATED (P pegged)"
  elif (( excl_run_max>=1 && excl_run_max<8 && p_idle>=1 )); then V="SCHED-CAPPED@~pcore (override not engaging)"
  elif (( tr>=16 && p_avg<50 )); then V="FETCH-BOUND-shape (running+Pidle)"
  elif (( p_idle>=1 && e_idle>=1 && pressured==0 )); then V="UNDER-UTILIZED?"
  else V="MIXED"; fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date +%T)" "$elapsed" "$q" "$ex" "$tr" "$wn" "$p_idle" "$p_min" "$p_max" "$p_avg" "$e_idle" "$pressured" "$excl_d" "$excl_run_max" "$egress" "$srv_fetch" "$fetch_ms" "$win_d" "$V" >> "$OUT"
  printf '%s q=%-3s run=%-3s | Pidle %s/%s avg%%=%-3s | excl+%-3s runmax=%-2s | egress=%-4sMB/s srvfetch=%-4sMB/s fetch=%-5sms | pf+%s | %s\n' \
    "$(date +%T)" "$q" "$tr" "$p_idle" "$wn" "$p_avg" "$excl_d" "$excl_run_max" "$egress" "$srv_fetch" "$fetch_ms" "$win_d" "$V"
  ((n++))
  sleep "$INTERVAL"
done
printf '[done] %s rows -> %s\n' "$n" "$OUT"
