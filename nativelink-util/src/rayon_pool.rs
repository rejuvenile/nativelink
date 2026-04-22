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

//! Bridges the global rayon thread pool to the tokio runtime.
//!
//! The blake3 mmap path (`update_mmap_rayon`) and any direct `rayon::spawn`
//! callsite hand work to rayon worker threads that, by default, have no
//! tokio runtime context. If anything in the work — including `Drop` impls
//! that fire as values cross the closure boundary — touches a tokio API,
//! tokio panics with "there is no reactor running, must be called from the
//! context of a Tokio 1.x runtime", and rayon's default panic policy
//! aborts the whole process. This crashed the worker fleet on 2026-04-19/20.
//!
//! `init_rayon_pool` builds the global rayon pool with a `spawn_handler`
//! that captures the current tokio `Handle` and `enter()`s it on every
//! rayon worker thread for that worker's lifetime. After `init`, any
//! Handle::current() / tokio::spawn / sleep / channel send issued from a
//! rayon worker (or any Drop running there) finds a runtime context and
//! does not panic. It also installs a panic handler that logs with a
//! backtrace before rayon aborts, so future regressions are visible
//! instead of mysterious.
//!
//! Must be called once, after the tokio runtime is built and before any
//! `rayon::spawn` or blake3 mmap call. `build_global` returns an error if
//! called twice.

use std::backtrace::Backtrace;
use std::sync::OnceLock;

use tokio::runtime::Handle;
use tracing::{error, info, warn};

/// Global captured tokio handle, used by code that spawns from threads
/// without an active tokio context (e.g. rayon workers spawned before
/// our handler ran, or third-party threads). Set by `init_rayon_pool`.
static TOKIO_HANDLE: OnceLock<Handle> = OnceLock::new();

/// Returns the global tokio handle captured at startup, if any.
///
/// Use as a fallback when `Handle::try_current()` is `None` so we can
/// still spawn tokio tasks from foreign threads instead of panicking.
pub fn fallback_handle() -> Option<&'static Handle> {
    TOKIO_HANDLE.get()
}

/// Initialize the global rayon thread pool with a tokio-aware spawn
/// handler and a panic handler that logs before rayon aborts.
///
/// Panics if rayon's global pool has already been initialized — this
/// must be the first thing that touches rayon. Returns a meaningful
/// error otherwise.
///
/// # Arguments
///
/// * `handle` — the tokio runtime handle to enter on every rayon worker
///   thread. Typically `runtime.handle().clone()` from `main()`.
pub fn init_rayon_pool(handle: Handle) -> Result<(), rayon::ThreadPoolBuildError> {
    // Stash the handle for foreign-thread spawn fallbacks.
    if TOKIO_HANDLE.set(handle.clone()).is_err() {
        warn!("init_rayon_pool called more than once, ignoring duplicate");
    }

    let spawn_handle = handle;
    rayon::ThreadPoolBuilder::new()
        .thread_name(|i| format!("rayon-worker-{i}"))
        .spawn_handler(move |thread| {
            let handle = spawn_handle.clone();
            let mut builder = std::thread::Builder::new();
            if let Some(name) = thread.name() {
                builder = builder.name(name.to_string());
            }
            if let Some(stack) = thread.stack_size() {
                builder = builder.stack_size(stack);
            }
            builder.spawn(move || {
                let _guard = handle.enter();
                thread.run();
            })?;
            Ok(())
        })
        .panic_handler(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                format!("non-string panic payload, type_id={:?}", payload.type_id())
            };
            let bt = Backtrace::force_capture();
            error!(panic = %msg, backtrace = %bt, "rayon worker panicked");
        })
        .build_global()?;
    info!("rayon global pool initialized with tokio handle bridge");
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Drops a `JoinHandleDropGuard` from a foreign (non-tokio) thread to
    /// prove that `JoinHandle::abort()` does not require a tokio context.
    /// If a future change breaks this contract, the test will panic with
    /// "there is no reactor running" instead of silently passing.
    #[test]
    fn join_handle_drop_guard_drops_on_non_tokio_thread() {
        // Build a small runtime, spawn a task, hand the JoinHandleDropGuard
        // off to a plain std::thread (no tokio context), drop it there.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");
        let handle = rt.handle().clone();
        let guard = handle.block_on(async {
            crate::spawn!("test_drop", async {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            })
        });
        let result = std::thread::spawn(move || {
            // Drops here — must not panic even though no tokio runtime is
            // entered on this thread.
            drop(guard);
        })
        .join();
        assert!(result.is_ok(), "drop on non-tokio thread should not panic");
    }

    /// Mirrors what `init_rayon_pool`'s spawn_handler does at startup:
    /// enter a captured tokio handle on a foreign thread, then issue
    /// `tokio::spawn` from inside it. Without `enter()` this panics with
    /// "there is no reactor running"; with `enter()` it succeeds.
    /// We can't call `init_rayon_pool` from a test (build_global is
    /// process-wide), so this test exercises the underlying pattern.
    #[test]
    fn tokio_spawn_inside_rayon_with_handle_enter_succeeds() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");
        let handle = rt.handle().clone();
        let (tx, rx) = std::sync::mpsc::channel();
        rayon::spawn(move || {
            let _guard = handle.enter();
            #[expect(clippy::disallowed_methods, reason = "test of spawn pattern")]
            let join = tokio::spawn(async { 42_u32 });
            drop(tx.send(join));
        });
        let join = rx.recv().expect("rayon delivered JoinHandle");
        let value = rt.block_on(join).expect("spawned task completed");
        assert_eq!(value, 42);
    }
}
