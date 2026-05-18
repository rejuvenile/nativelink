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

// Benchmark-binary-specific lint relaxations; see benchmarks/src/lib.rs
// for full rationale.
#![allow(
    clippy::print_stdout,
    clippy::single_match_else,
    clippy::vec_init_then_push,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::decimal_literal_representation,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_clamp,
    clippy::match_same_arms,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::pub_underscore_fields,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::std_instead_of_core,
    clippy::too_many_lines,
    clippy::unchecked_time_subtraction,
    clippy::unreadable_literal,
    clippy::unused_self,
    clippy::use_debug,
    clippy::use_self
)]

//! `data_plane_bench` CLI — runs the #495 Phase 1 v3-anchoring smoke
//! suite and emits one JSON baseline file.
//!
//! Usage:
//!   cargo run --release --bin data_plane_bench -- \
//!     --output benchmarks/baselines/<timestamp>-<sha>.json
//!
//! Pre-flight gate:
//!   Refuses to run on buildcache if `nativelink.service` is up <10 min or
//!   if the journal shows >500 ByteStream:: log lines in the last 2 min.
//!   Override with `--force`.
//!
//! Tempdir default:
//!   `/dev/shm/nl-bench-XXXXXX` on Linux when `/dev/shm` is writable.
//!   This keeps bench writes off prod ZFS datasets (`tank`, `fast`) —
//!   the bench produces gigabytes of churn on the larger cells which
//!   would otherwise pollute the ARC during active prod traffic. The
//!   pre-flight gate REFUSES if the resolved tempdir lands inside a
//!   path containing "tank" or "fast" (substring match), to defend
//!   against accidental `--temp-dir /srv/bulk/...` invocations.
//!
//! **Deviation from design's "use criterion" decision:** the harness is
//! custom (not `criterion`). The custom envelope (`BaselineFile` with
//! `git_*` / `host` / `forced` / `temp_dir_used` / `schema_version`
//! metadata + checked-in baselines under `benchmarks/baselines/`) is a
//! meaningfully different deliverable from `criterion`'s ephemeral
//! `target/criterion/` output, and the latter cannot easily fold our
//! envelope. The deviation is intentional; rewriting on `criterion`
//! would require both API redesign and a separate CI-side `criterion-
//! compare-action` plumbing. If `criterion` adoption becomes desired
//! later, the swap is mechanical (one cell at a time).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use nativelink_benchmarks::output::{BaselineFile, BenchmarkResult, RunMetadata, SCHEMA_VERSION};
use nativelink_benchmarks::preflight::{Verdict, run_preflight};
use nativelink_benchmarks::scenarios::{
    MIN_ITERS, RunOpts, chunked_v2, existence_cache_micro, find_missing, legacy_read,
    legacy_write,
};

#[derive(Parser, Debug)]
#[command(name = "data_plane_bench", version, about = "#495 Phase 1 v3-anchoring smoke suite")]
struct Cli {
    /// Output baseline JSON path. If absent, writes to stdout.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Bypass the pre-flight gate. REQUIRED for first-baseline
    /// collection on a running buildcache; otherwise should be left off.
    #[arg(long)]
    force: bool,

    /// Iterations per cell. Default 20. Minimum `MIN_ITERS` (= 1) —
    /// values < `MIN_ITERS` are rejected at parse time to defend
    /// against `--iters 0 --fast` ending in an empty-vec panic.
    #[arg(long, default_value_t = 20, value_parser = parse_iters)]
    iters: u32,

    /// Substring filter — only run scenarios whose name contains this.
    #[arg(long)]
    filter: Option<String>,

    /// Reduce to a few iters per cell so the whole suite runs in
    /// seconds. Diff tooling refuses to compare fast-mode baselines.
    #[arg(long)]
    fast: bool,

    /// Comma-separated scenario families to run (default: all).
    /// Valid values: `w1`, `r1`, `f1`, `w3`, `r5`, `c1`. Empty string
    /// is rejected.
    #[arg(long, default_value = "w1,r1,f1,w3,r5,c1", value_parser = parse_scenarios)]
    scenarios: String,

    /// Print the pre-flight verdict and exit without running anything.
    #[arg(long)]
    preflight_only: bool,

    /// Override the parent directory the bench tempdir is created in.
    /// If absent, defaults to `/dev/shm` on Linux (when writable) and
    /// the OS temp dir otherwise. The bench REFUSES to use a tempdir
    /// path whose absolute form contains "tank" or "fast" (those are
    /// prod ZFS pool names; writing there pollutes ARC during prod
    /// traffic).
    #[arg(long)]
    temp_dir: Option<PathBuf>,
}

fn parse_iters(s: &str) -> Result<u32, String> {
    let n: u32 = s
        .parse()
        .map_err(|e| format!("--iters: {e} (must be a u32)"))?;
    if n < MIN_ITERS {
        return Err(format!(
            "--iters: {n} below MIN_ITERS ({MIN_ITERS}) — passing 0 \
             with --fast would panic the bench"
        ));
    }
    Ok(n)
}

fn parse_scenarios(s: &str) -> Result<String, String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("--scenarios: empty string not allowed".to_string());
    }
    Ok(trimmed.to_string())
}

fn main() -> ExitCode {
    // Defer to a tokio runtime so the bench can drive async stores.
    // Note: `tokio::runtime::Builder::new_multi_thread` is in the
    // workspace `disallowed-methods` list; the bench is one of the rare
    // top-level binaries that legitimately needs it (this IS the entry
    // point that constructs the runtime). Standard pattern for entry-
    // point binaries; see `src/bin/nativelink.rs` for the prod entry.
    // Entry-point binary: this IS the runtime constructor, the lint
    // exists to keep library code from doing it. Suppress for the
    // single legitimate call-site.
    #[allow(clippy::disallowed_methods)]
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[bench] failed to build tokio runtime: {e:?}");
            return ExitCode::FAILURE;
        }
    };
    #[allow(clippy::disallowed_methods)]
    rt.block_on(run_main())
}

async fn run_main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();

    let verdict = run_preflight();
    match &verdict {
        Verdict::AllClear => {
            eprintln!("[bench] pre-flight: all clear");
        }
        Verdict::Inconclusive(msg) => {
            eprintln!("[bench] pre-flight: inconclusive — {msg}");
        }
        Verdict::Refuse(msg) => {
            eprintln!("[bench] pre-flight: REFUSE — {msg}");
            if !cli.force {
                eprintln!("[bench] pass --force to override");
                return ExitCode::from(2);
            }
            eprintln!("[bench] --force: continuing despite refuse verdict");
        }
    }

    // Resolve and gate the bench tempdir.
    let temp_dir = match resolve_bench_temp_dir(cli.temp_dir.as_ref()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[bench] tempdir refused: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("[bench] tempdir: {}", temp_dir.display());

    if cli.preflight_only {
        return if verdict.allowed() || cli.force {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(2)
        };
    }

    let opts = RunOpts {
        iters: cli.iters,
        filter: cli.filter.clone(),
        fast: cli.fast,
    };

    let mut all_results: Vec<BenchmarkResult> = Vec::new();
    let want: Vec<String> = cli
        .scenarios
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .collect();

    let temp_dir_opt = Some(temp_dir.clone());

    if want.iter().any(|s| s == "w1") {
        eprintln!("[bench] W1 (store update_oneshot through FastSlow+Filesystem)");
        all_results.extend(legacy_write::run(&opts, temp_dir_opt.as_ref()).await);
    }
    if want.iter().any(|s| s == "r1") {
        eprintln!("[bench] R1 (store get_part_unchunked through FastSlow+Filesystem)");
        all_results.extend(legacy_read::run(&opts, temp_dir_opt.as_ref()).await);
    }
    if want.iter().any(|s| s == "f1") {
        eprintln!("[bench] F1 (FindMissingBlobs through ExistenceCache)");
        all_results.extend(find_missing::run(&opts, temp_dir_opt.as_ref()).await);
    }
    if want.iter().any(|s| s == "w3" || s == "r5") {
        eprintln!("[bench] W3 + R5 (chunked-v2 anchoring cells)");
        all_results.extend(chunked_v2::run(&opts, temp_dir_opt.as_ref()).await);
    }
    if want.iter().any(|s| s == "c1") {
        eprintln!("[bench] C1 (ExistenceCache micro-bench — hit/miss + batch 16/128)");
        all_results.extend(existence_cache_micro::run(&opts).await);
    }

    let metadata = collect_metadata(cli.force, &temp_dir);
    let baseline = BaselineFile {
        metadata,
        results: all_results,
    };

    let json = match serde_json::to_string_pretty(&baseline) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[bench] JSON serialization failed: {e:?}");
            return ExitCode::FAILURE;
        }
    };

    match cli.output {
        Some(path) => {
            if let Err(e) = std::fs::write(&path, &json) {
                eprintln!("[bench] failed to write baseline to {}: {e:?}", path.display());
                return ExitCode::FAILURE;
            }
            eprintln!("[bench] baseline written: {}", path.display());
        }
        None => {
            println!("{json}");
        }
    }

    ExitCode::SUCCESS
}

/// Choose a tempdir root for bench writes.
///
/// Refuses any path whose absolute form contains "tank" or "fast"
/// (substring match) — those are the prod ZFS pool names on buildcache,
/// and writes there pollute ARC during prod traffic.
fn resolve_bench_temp_dir(override_: Option<&PathBuf>) -> Result<PathBuf, String> {
    let chosen = match override_ {
        Some(p) => p.clone(),
        None => {
            // Prefer /dev/shm on Linux when writable.
            let dev_shm = PathBuf::from("/dev/shm");
            if cfg!(target_os = "linux") && dev_shm.is_dir() {
                dev_shm
            } else {
                std::env::temp_dir()
            }
        }
    };
    let abs = chosen
        .canonicalize()
        .unwrap_or_else(|_| chosen.clone());
    let abs_str = abs.to_string_lossy();
    if abs_str.contains("/srv/bulk/") || abs_str.contains("/fast/") || abs_str.ends_with("/srv/bulk") || abs_str.ends_with("/fast") {
        return Err(format!(
            "tempdir {} resolves under a prod ZFS pool (`tank`/`fast`); \
             refusing to write bench data there. Pass --temp-dir /dev/shm \
             or another non-prod path.",
            abs.display()
        ));
    }
    Ok(chosen)
}

fn collect_metadata(forced: bool, temp_dir: &std::path::Path) -> RunMetadata {
    let git_commit_sha = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());
    let git_dirty: Option<bool> = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(!o.stdout.is_empty())
            } else {
                None
            }
        });
    let host = hostname_string();
    let timestamp_utc = iso8601_now();
    let features = enabled_features();
    RunMetadata {
        schema_version: SCHEMA_VERSION,
        git_commit_sha,
        git_dirty,
        host,
        timestamp_utc,
        features,
        forced,
        temp_dir_used: temp_dir.display().to_string(),
    }
}

fn hostname_string() -> String {
    hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string())
}

fn iso8601_now() -> String {
    // chrono is in the workspace lockfile (transitive). 6-decimal
    // micros to keep the timestamp diff-stable across runs.
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn enabled_features() -> Vec<String> {
    #[allow(unused_mut)]
    let mut features = Vec::new();
    #[cfg(feature = "chunked_fast_slow")]
    features.push("chunked_fast_slow".to_string());
    features
}

fn init_tracing() {
    // try_init returns Err if a subscriber was already set (test
    // harness, repeated invocation); either way logging works after
    // the first installer wins, so the Err is benign.
    drop(
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_writer(std::io::stderr)
            .try_init(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_iters_rejects_below_min() {
        assert!(parse_iters("0").is_err());
        assert!(parse_iters("not-a-number").is_err());
        assert_eq!(parse_iters("1").unwrap(), 1);
        assert_eq!(parse_iters("20").unwrap(), 20);
    }

    #[test]
    fn parse_scenarios_rejects_empty() {
        assert!(parse_scenarios("").is_err());
        assert!(parse_scenarios("   ").is_err());
        assert_eq!(parse_scenarios("w1").unwrap(), "w1");
    }

    #[test]
    fn resolve_bench_temp_dir_refuses_zfs_pool_paths() {
        // We use a path that doesn't exist so canonicalize falls back
        // to the passed value; substring match still tags it.
        let bad_tank = PathBuf::from("/srv/bulk/nativelink/bench");
        assert!(resolve_bench_temp_dir(Some(&bad_tank)).is_err());
        let bad_fast = PathBuf::from("/fast/nativelink/bench");
        assert!(resolve_bench_temp_dir(Some(&bad_fast)).is_err());
        // /tmp is fine.
        let ok = PathBuf::from("/tmp");
        assert!(resolve_bench_temp_dir(Some(&ok)).is_ok());
    }

    #[test]
    fn iso8601_format_round_trips_through_chrono() {
        let s = iso8601_now();
        // Re-parse via chrono — sanity-check the formatter produced a
        // valid RFC3339 datetime.
        let parsed: chrono::DateTime<chrono::Utc> = chrono::DateTime::parse_from_rfc3339(&s)
            .expect("iso8601_now produces RFC3339")
            .with_timezone(&chrono::Utc);
        // Round-trip must be within a few seconds.
        let now = chrono::Utc::now();
        let dt = (now - parsed).num_seconds().abs();
        assert!(dt < 60, "round-trip ts within 60s: dt={dt}s");
    }
}
