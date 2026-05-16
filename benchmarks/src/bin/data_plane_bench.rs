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

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use nativelink_benchmarks::output::{BaselineFile, BenchmarkResult, RunMetadata, SCHEMA_VERSION};
use nativelink_benchmarks::preflight::{Verdict, run_preflight};
use nativelink_benchmarks::scenarios::{RunOpts, chunked_v2, find_missing, legacy_read, legacy_write};

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

    /// Iterations per cell. 0 = scenario default.
    #[arg(long, default_value_t = 20)]
    iters: u32,

    /// Substring filter — only run scenarios whose name contains this.
    #[arg(long)]
    filter: Option<String>,

    /// Reduce to a few iters per cell so the whole suite runs in
    /// seconds. Diff tooling refuses to compare fast-mode baselines.
    #[arg(long)]
    fast: bool,

    /// Comma-separated scenario families to run (default: all).
    /// Valid values: `w1`, `r1`, `f1`, `w3`, `r5`.
    #[arg(long, default_value = "w1,r1,f1,w3,r5")]
    scenarios: String,

    /// Print the pre-flight verdict and exit without running anything.
    #[arg(long)]
    preflight_only: bool,
}

fn main() -> ExitCode {
    // Defer to a tokio runtime so the bench can drive async stores.
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

    if cli.preflight_only {
        return if verdict.allowed() {
            ExitCode::SUCCESS
        } else if cli.force {
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

    if want.iter().any(|s| s == "w1") {
        eprintln!("[bench] W1 (legacy ByteStream::Write through FastSlow+Filesystem)");
        all_results.extend(legacy_write::run(&opts).await);
    }
    if want.iter().any(|s| s == "r1") {
        eprintln!("[bench] R1 (ByteStream::Read through FastSlow+Filesystem)");
        all_results.extend(legacy_read::run(&opts).await);
    }
    if want.iter().any(|s| s == "f1") {
        eprintln!("[bench] F1 (FindMissingBlobs through ExistenceCache)");
        all_results.extend(find_missing::run(&opts).await);
    }
    if want.iter().any(|s| s == "w3" || s == "r5") {
        eprintln!("[bench] W3 + R5 (chunked-v2 anchoring cells)");
        all_results.extend(chunked_v2::run(&opts).await);
    }

    let metadata = collect_metadata(cli.force);
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

fn collect_metadata(forced: bool) -> RunMetadata {
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
    let git_dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    let host = hostname();
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
    }
}

fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn iso8601_now() -> String {
    // Avoid adding a chrono dep; format from UNIX_EPOCH manually.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let nanos = now.subsec_nanos();
    let (year, month, day, hour, minute, second) = secs_to_ymd_hms(secs);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos:09}Z"
    )
}

fn secs_to_ymd_hms(mut secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let second = secs % 60;
    secs /= 60;
    let minute = secs % 60;
    secs /= 60;
    let hour = secs % 24;
    let mut days_since_epoch = secs / 24;
    let mut year: u64 = 1970;
    loop {
        let leap = is_leap(year);
        let days_in_year = if leap { 366 } else { 365 };
        if days_since_epoch < days_in_year {
            break;
        }
        days_since_epoch -= days_in_year;
        year += 1;
    }
    let leap = is_leap(year);
    let month_lens = if leap {
        [31u64, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 1u64;
    for &ml in &month_lens {
        if days_since_epoch < ml {
            break;
        }
        days_since_epoch -= ml;
        month += 1;
    }
    let day = days_since_epoch + 1;
    (year, month, day, hour, minute, second)
}

fn is_leap(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn enabled_features() -> Vec<String> {
    #[allow(unused_mut)]
    let mut features = Vec::new();
    #[cfg(feature = "chunked_fast_slow")]
    features.push("chunked_fast_slow".to_string());
    features
}

fn init_tracing() {
    let _result = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();
    // Ignore the result — subscriber may already be set by a test
    // harness or repeated invocation; either way we keep going.
    drop(_result);
}
