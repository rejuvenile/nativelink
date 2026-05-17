// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Strict pre-flight gate. Refuses to run when buildcache is serving live
//! production traffic; the operator must pass `--force` to override.
//!
//! Per design Section 4 + the user's `bench runner = buildcache` decision:
//! the bench shares hardware with production NL. To keep baseline noise
//! tractable, the gate checks two cheap signals:
//!
//! 1. **Uptime gate.** If `nativelink.service` (the systemd unit) was
//!    (re)started in the last [`MIN_NATIVELINK_UPTIME`], it's likely
//!    still warming caches and accepting reconnects — refuse.
//! 2. **Recent journal-traffic gate.** If
//!    `journalctl --namespace=nativelink --since='2 min ago'` shows more
//!    than [`MAX_RECENT_BYTESTREAM_LINES`] `ByteStream::` log lines, the
//!    server is actively serving requests — refuse.
//!
//! Either check can be skipped with `--force`. Both being false produces
//! a clean "all clear" report.
//!
//! Calibration notes (numeric constants below):
//!
//! - `MIN_NATIVELINK_UPTIME = 600s (10 min)`: derived from
//!   `feedback_committed_is_not_shipped.md` + post-deploy practice.
//!   After `just deploy`, the runtime needs ~5 min to repopulate the
//!   fast-tier MemoryStore from initial Bazel client reconnects.
//!   10 min is comfortable headroom.
//! - `MAX_RECENT_BYTESTREAM_LINES = 500`: at production peak load
//!   buildcache emits thousands of `ByteStream::Write/Read` info lines
//!   per minute (per `nativelink-service/src/bytestream_server.rs` info-
//!   level transfer logs). 500 lines in a 2-minute window = ~4 lines/sec
//!   — sustained quiet, below typical CI build burst.
//!
//! Falsification: the operator can verify the gate works by running the
//! bench on buildcache during active CI traffic; the gate must refuse with
//! a specific message naming which check tripped.

use std::process::Command;
use std::time::Duration;

/// Minimum uptime of `nativelink.service` required before the bench is
/// allowed to run. See module doc for rationale.
pub const MIN_NATIVELINK_UPTIME: Duration = Duration::from_secs(600);

/// Threshold of `ByteStream::` log lines in the last 2 minutes above
/// which the bench refuses to run. See module doc for rationale.
pub const MAX_RECENT_BYTESTREAM_LINES: u32 = 500;

/// Window over which [`MAX_RECENT_BYTESTREAM_LINES`] is measured.
pub const TRAFFIC_WINDOW: Duration = Duration::from_secs(120);

/// Verdict from the pre-flight gate.
#[derive(Debug)]
pub enum Verdict {
    /// Both checks passed — bench may run.
    AllClear,
    /// At least one check tripped; carries a diagnostic.
    Refuse(String),
    /// Gate could not run a check (e.g. journalctl absent because the
    /// bench is running off-prod-host). `Inconclusive` is treated as
    /// `AllClear` because forcing a gate to pass on a developer laptop
    /// would be user-hostile.
    Inconclusive(String),
}

impl Verdict {
    pub fn allowed(&self) -> bool {
        !matches!(self, Verdict::Refuse(_))
    }
}

/// Run the gate. Pure CPU + a few `systemctl` / `journalctl` shellouts;
/// fast enough to be a no-cost pre-bench step.
pub fn run_preflight() -> Verdict {
    let uptime_check = check_nativelink_uptime();
    if let Verdict::Refuse(msg) = &uptime_check {
        return Verdict::Refuse(msg.clone());
    }

    let traffic_check = check_recent_traffic();
    if let Verdict::Refuse(msg) = &traffic_check {
        return Verdict::Refuse(msg.clone());
    }

    // Both passed (or inconclusive). Inconclusive ⇒ AllClear with
    // diagnostic, so the bench can run on a dev laptop.
    match (&uptime_check, &traffic_check) {
        (Verdict::AllClear, Verdict::AllClear) => Verdict::AllClear,
        (Verdict::Inconclusive(a), Verdict::Inconclusive(b)) => {
            Verdict::Inconclusive(format!("{a}; {b}"))
        }
        (Verdict::Inconclusive(msg), Verdict::AllClear)
        | (Verdict::AllClear, Verdict::Inconclusive(msg)) => Verdict::Inconclusive(msg.clone()),
        // `Refuse` cases handled above by early return; the remaining
        // pattern is unreachable but Rust can't see that.
        _ => Verdict::AllClear,
    }
}

fn check_nativelink_uptime() -> Verdict {
    let output = Command::new("systemctl")
        .args([
            "show",
            "--property=ActiveEnterTimestampMonotonic",
            "nativelink.service",
        ])
        .output();
    let output = match output {
        Ok(o) => o,
        Err(e) => {
            return Verdict::Inconclusive(format!(
                "systemctl unavailable ({e:?}); assuming not-on-buildcache"
            ));
        }
    };
    if !output.status.success() {
        return Verdict::Inconclusive(format!(
            "systemctl show failed (exit {:?}); assuming nativelink.service absent",
            output.status.code()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let value = stdout
        .trim()
        .strip_prefix("ActiveEnterTimestampMonotonic=")
        .unwrap_or("");
    let active_enter_monotonic_us: u64 = value.parse().unwrap_or(0);
    if active_enter_monotonic_us == 0 {
        return Verdict::Inconclusive(
            "nativelink.service has never started on this host (assuming dev laptop)".to_string(),
        );
    }

    // CLOCK_MONOTONIC time in microseconds since boot. We compute
    // current monotonic time the same way systemd does — read
    // /proc/uptime first field, convert to microseconds.
    let uptime_us = read_proc_uptime_us();
    let svc_uptime_us = uptime_us.saturating_sub(active_enter_monotonic_us);
    let svc_uptime = Duration::from_micros(svc_uptime_us);
    if svc_uptime < MIN_NATIVELINK_UPTIME {
        return Verdict::Refuse(format!(
            "nativelink.service uptime {svc_uptime:?} < MIN_NATIVELINK_UPTIME \
             ({MIN_NATIVELINK_UPTIME:?}); pass --force to override (e.g. for first-baseline collection)"
        ));
    }
    Verdict::AllClear
}

fn read_proc_uptime_us() -> u64 {
    let s = std::fs::read_to_string("/proc/uptime").unwrap_or_default();
    let first = s.split_whitespace().next().unwrap_or("0");
    let secs: f64 = first.parse().unwrap_or(0.0);
    (secs * 1_000_000.0) as u64
}

fn check_recent_traffic() -> Verdict {
    let window_secs = TRAFFIC_WINDOW.as_secs();
    let since = format!("{window_secs} sec ago");
    let output = Command::new("journalctl")
        .args([
            "--namespace=nativelink",
            "--since",
            &since,
            "--no-pager",
            "--quiet",
            "--grep=ByteStream::",
        ])
        .output();
    let output = match output {
        Ok(o) => o,
        Err(e) => {
            return Verdict::Inconclusive(format!(
                "journalctl unavailable ({e:?}); skipping traffic gate"
            ));
        }
    };
    if !output.status.success() {
        return Verdict::Inconclusive(format!(
            "journalctl --namespace=nativelink failed (exit {:?}); skipping traffic gate",
            output.status.code()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line_count: u32 = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .count()
        .try_into()
        .unwrap_or(u32::MAX);
    classify_traffic(line_count, window_secs)
}

/// Pure comparator extracted from [`check_recent_traffic`] so the gate
/// logic can be unit-tested without shell-out. The cutoff is `>` (strict)
/// so a line count exactly equal to [`MAX_RECENT_BYTESTREAM_LINES`] still
/// returns `AllClear`.
///
/// Mutation: change `>` to `>=` — `classify_traffic_off_by_one_500_passes`
/// red-fails because a 500-line bucket now refuses.
fn classify_traffic(line_count: u32, window_secs: u64) -> Verdict {
    if line_count > MAX_RECENT_BYTESTREAM_LINES {
        return Verdict::Refuse(format!(
            "production traffic active: {line_count} ByteStream:: log lines in last {window_secs}s \
             > MAX_RECENT_BYTESTREAM_LINES ({MAX_RECENT_BYTESTREAM_LINES}); pass --force to override"
        ));
    }
    Verdict::AllClear
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_allowed_predicate_matches_variants() {
        assert!(Verdict::AllClear.allowed());
        assert!(Verdict::Inconclusive("dev laptop".to_string()).allowed());
        assert!(!Verdict::Refuse("active traffic".to_string()).allowed());
    }

    /// Pin the numeric constants this module exposes so a future commit
    /// can't drift them without a deliberate test edit. Mirrors the
    /// CLAUDE.md "numeric-constant verification" gate.
    #[test]
    fn preflight_thresholds_match_documented_values() {
        assert_eq!(
            MIN_NATIVELINK_UPTIME,
            Duration::from_secs(600),
            "doc-comment + module rationale name 10 min; declaration must match"
        );
        assert_eq!(
            MAX_RECENT_BYTESTREAM_LINES, 500,
            "doc-comment + module rationale name 500 lines; declaration must match"
        );
        assert_eq!(
            TRAFFIC_WINDOW,
            Duration::from_secs(120),
            "doc-comment + module rationale name 2-minute window; declaration must match"
        );
    }

    /// Behavior test: 499 lines passes the gate. Substance check —
    /// the constant could be anything, what matters is the comparator.
    #[test]
    fn classify_traffic_off_by_one_499_passes() {
        match classify_traffic(MAX_RECENT_BYTESTREAM_LINES - 1, 120) {
            Verdict::AllClear => {}
            other => panic!("499 lines should pass; got {other:?}"),
        }
    }

    /// Exactly-at-threshold passes (`>` strict). Mutation: change `>`
    /// to `>=` in `classify_traffic` — this test red-fails.
    #[test]
    fn classify_traffic_off_by_one_500_passes() {
        match classify_traffic(MAX_RECENT_BYTESTREAM_LINES, 120) {
            Verdict::AllClear => {}
            other => panic!("500 lines should pass; got {other:?}"),
        }
    }

    /// 501 lines refuses. Mutation: comment out the `if` body in
    /// `classify_traffic` — this test red-fails.
    #[test]
    fn classify_traffic_off_by_one_501_refuses() {
        match classify_traffic(MAX_RECENT_BYTESTREAM_LINES + 1, 120) {
            Verdict::Refuse(msg) => {
                assert!(
                    msg.contains("501"),
                    "diagnostic must include the line count: {msg}"
                );
                assert!(
                    msg.contains("MAX_RECENT_BYTESTREAM_LINES"),
                    "diagnostic must name the constant: {msg}"
                );
            }
            other => panic!("501 lines must refuse; got {other:?}"),
        }
    }
}
