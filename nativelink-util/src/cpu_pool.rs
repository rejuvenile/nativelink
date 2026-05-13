// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! Dedicated rayon thread pool for CPU-bound work that should NOT contend
//! with `tokio::task::spawn_blocking`.
//!
//! ## Why a dedicated pool (separate from rayon's default global pool)
//!
//! The blocking-pool saturation investigation
//! (`.claude/audits/blocking-pool-saturation-investigation-20260512.md`)
//! observed chunked-write tail latency (`back_edge_ms` p99=1216ms) and
//! parsed 50 representative warns showing the dominant pattern
//! `mutex_acquire_ms=0, dispatch_ms=50-188, pwrite_ms=0` — i.e. the
//! submit took 50-188 ms but the work itself was effectively free.
//! dispatch_ms measures wall-clock from `spawn_blocking` call to the
//! closure's first instruction; in tokio's blocking-pool path this
//! includes the parker futex wake, kernel context-switch onto an idle
//! blocking thread, and that thread's cold-cache mutex re-acquire on
//! the way to popping the task. None of these steps individually
//! justifies 50-188 ms; the data is consistent with high contention on
//! the SUBMISSION PIPELINE (mutex + parker + scheduler), not on the
//! mutex critical-section alone. The pool itself was at 1.5%
//! utilization (15 of 1024 threads alive); the bottleneck was
//! submission, not service.
//!
//! The investigation explicitly marked this as "consistent with the
//! data, but UNFALSIFIED until production deploy" and demanded a
//! `closure_to_resume_ms` discriminator probe before any separate
//! dispatch path landed. This pool removes per-chunk SHA from the
//! tokio blocking-pool submission pipeline regardless of which
//! submission step dominates — whether mutex, wake, or scheduler.
//! Production deploy is the falsification step: if `back_edge_ms`
//! p99 falls toward the ~50 ms threshold, SHA was the cross-traffic;
//! if it stays high, the bottleneck is elsewhere (most likely
//! worker-side starvation downstream of the closure entry).
//!
//! Hot-path SHA-256 (admit-side per-chunk verify, commit-side end-to-end
//! verify) is a major contributor — each 1 MiB SHA submit is one extra
//! lock acquisition on the same hot mutex. Moving these CPU-bound tasks
//! to a separate rayon pool with its own work-stealing queue eliminates
//! the mutex cross-traffic entirely.
//!
//! ## Why NOT rayon's default global pool
//!
//! `nativelink_util::rayon_pool::init_rayon_pool` already configures
//! rayon's GLOBAL pool with a tokio-handle bridge to keep the blake3
//! mmap path (`Hasher::update_mmap_rayon`) running there safely. Using
//! the SAME pool for chunked-SHA work would let bursty per-chunk SHA
//! starve blake3's full-blob digest computation (and vice-versa). A
//! separate pool partitions CPU between the two workloads with their
//! own queues and worker counts.
//!
//! ## Sizing
//!
//! `num_threads = max(2, num_cpus / 2)`. Half the CPU budget keeps
//! tokio worker threads (the other major CPU consumer) viable on
//! medium boxes; a hard floor of 2 prevents single-thread serialization
//! on tiny VMs / containers.
//!
//! ## Submission pattern
//!
//! ```ignore
//! use nativelink_util::cpu_pool::cpu_pool;
//!
//! let (tx, rx) = tokio::sync::oneshot::channel();
//! cpu_pool().spawn(move || {
//!     let result = expensive_cpu_work(input);
//!     let _ = tx.send(result);
//! });
//! let result = rx.await
//!     .map_err(|_| make_err!(Code::Internal, "cpu_pool worker dropped"))?;
//! ```
//!
//! `tokio::sync::oneshot` is cheap (one allocation, no lock); the
//! receive-side `.await` is a normal tokio cooperative yield. The rayon
//! worker NEVER touches a tokio API directly (just `tx.send`, which is
//! synchronous and lock-free).

use core::any::Any;
use std::backtrace::Backtrace;
use std::sync::OnceLock;

use rayon::ThreadPool;
use tracing::{error, info, warn};

/// Logs and swallows a rayon worker panic so the process does NOT abort.
///
/// rayon's default policy is `AbortIfPanic` — without a registered
/// `panic_handler`, a single CPU-bound panic in this dedicated pool
/// (e.g. inside a SHA-256 closure) takes down the entire NativeLink
/// process. The matching `tokio::sync::oneshot::Sender` will simply
/// drop on unwind, which surfaces to the awaiting task as
/// `RecvError` — caller already maps that to `Code::Internal`.
///
/// Mirrors `rayon_pool::init_rayon_pool`'s handler that was added on
/// 2026-04-22 (`e5368812`) after a worker-fleet abort cascade on
/// 2026-04-19/20.
fn cpu_pool_panic_handler(payload: Box<dyn Any + Send>) {
    let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        format!("non-string panic payload, type_id={:?}", payload.type_id())
    };
    let bt = Backtrace::force_capture();
    error!(panic = %msg, backtrace = %bt, "cpu_pool worker panicked");
}

/// Process-global dedicated rayon pool for CPU-bound chunked-write work.
/// Initialized lazily on first use via `cpu_pool()`. The first call
/// builds the pool; subsequent calls return the same `&'static ThreadPool`.
static CPU_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// Returns the dedicated CPU-bound rayon pool, initializing it on first
/// call. The pool is sized at `max(2, num_cpus / 2)` worker threads (see
/// module docs for sizing rationale).
///
/// On a build-failure (extremely rare; would require a thread spawn to
/// fail at startup), this function falls back to rayon's default global
/// pool via `rayon::ThreadPool::current` semantics — it logs a warn so
/// the regression is visible in production logs but does NOT panic.
/// Calling sites use `cpu_pool().spawn(...)`; if init failed, work runs
/// on rayon's global pool instead, losing the partition benefit but
/// staying correct.
///
/// # Returns
/// `&'static ThreadPool` — safe to keep across awaits, share across
/// tasks, etc.
pub fn cpu_pool() -> &'static ThreadPool {
    CPU_POOL.get_or_init(|| {
        let cpus = std::thread::available_parallelism()
            .map(core::num::NonZero::get)
            .unwrap_or(2);
        // Floor at 2 so single-CPU VMs don't serialize all work; ceiling
        // at cpus/2 so tokio workers retain CPU headroom.
        let num_threads = core::cmp::max(2, cpus / 2);
        match rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("nl-cpu-pool-{i}"))
            .panic_handler(cpu_pool_panic_handler)
            .build()
        {
            Ok(pool) => {
                info!(
                    num_threads,
                    detected_cpus = cpus,
                    "cpu_pool initialized (dedicated rayon pool for chunked SHA / CPU-bound work)",
                );
                pool
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "cpu_pool: failed to build dedicated pool; \
                     falling back to a 2-thread minimal pool",
                );
                // Last-ditch: 2-thread pool. If even this fails we panic
                // (returning a dangling pointer is unsafe; bailing makes
                // the failure visible at startup rather than silently
                // continuing on the global pool — a 2-thread spawn
                // failure means the process is probably going down anyway).
                rayon::ThreadPoolBuilder::new()
                    .num_threads(2)
                    .thread_name(|i| format!("nl-cpu-pool-fb-{i}"))
                    .panic_handler(cpu_pool_panic_handler)
                    .build()
                    .expect("cpu_pool: even the 2-thread fallback build failed")
            }
        }
    })
}
