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

//! Stall detection and thread dump utilities.
//!
//! When an async operation takes longer than a configured threshold,
//! [`StallGuard`] dumps all thread stacks to a file for post-mortem analysis.

use core::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};

/// Minimum interval between consecutive stack dumps (seconds).
/// Prevents flooding /tmp with dumps during a sustained stall.
const MIN_DUMP_INTERVAL_SECS: u64 = 30;

/// Unix epoch seconds of the last dump. Used for rate-limiting.
static LAST_DUMP_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Force-dump rate-limit: dumps marked "force" still get rate-limited
/// to avoid flooding /tmp during a sustained wedge, but use a separate
/// (much shorter) interval so a critical event can produce a dump even
/// if a generic StallGuard fired moments before. Without this separate
/// budget, two unrelated stalls within 30s would silently drop the
/// second (more interesting) dump because the first burned the slot.
const MIN_FORCE_DUMP_INTERVAL_SECS: u64 = 10;

/// Unix epoch seconds of the last force-dump. Tracked separately from
/// `LAST_DUMP_EPOCH` so a force dump and a normal dump have independent
/// rate-limits.
static LAST_FORCE_DUMP_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Default stall threshold for store operations.
pub const DEFAULT_STALL_THRESHOLD: Duration = Duration::from_secs(30);

/// Decide whether a force-dump request should proceed, given the
/// current and previous force-dump unix-epoch seconds. Pure function;
/// extracted from `force_dump_thread_stacks` to keep the rate-limit
/// rule unit-testable without touching the process-global `/tmp` dump
/// path. Returns `true` iff the gap is at or above
/// `MIN_FORCE_DUMP_INTERVAL_SECS`.
const fn force_dump_should_proceed(now_secs: u64, prev_secs: u64) -> bool {
    now_secs.saturating_sub(prev_secs) >= MIN_FORCE_DUMP_INTERVAL_SECS
}

/// Force a thread-stack dump for a critical event (e.g. streaming-blob
/// deadline exceeded), bypassing the normal `MIN_DUMP_INTERVAL_SECS`
/// rate-limit but still applying a much shorter
/// `MIN_FORCE_DUMP_INTERVAL_SECS` floor so a tight wedge loop can't
/// flood `/tmp`.
///
/// Use this for events that are themselves diagnostic (i.e. a
/// targeted-detector tripped) rather than generic stall guards. The
/// resulting `/tmp/nativelink-stall-*.txt` lets the operator see who
/// was wedged at the exact moment the upstream detector fired, even if
/// a generic StallGuard already burned the normal rate-limit slot.
///
/// Returns `true` if a dump was actually triggered, `false` if
/// suppressed by the force-rate-limit.
pub fn force_dump_thread_stacks(label: &str) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let prev = LAST_FORCE_DUMP_EPOCH.load(Ordering::Relaxed);
    if !force_dump_should_proceed(now, prev) {
        eprintln!(
            "FORCE THREAD DUMP requested ({label}) — suppressed by rate-limit \
             (last force dump {}s ago)",
            now.saturating_sub(prev)
        );
        return false;
    }
    if LAST_FORCE_DUMP_EPOCH
        .compare_exchange(prev, now, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        eprintln!(
            "FORCE THREAD DUMP requested ({label}) — lost CAS race with concurrent dump"
        );
        return false;
    }
    // Also bump LAST_DUMP_EPOCH so a normal StallGuard firing 1s after
    // this force dump doesn't immediately re-dump on top of us.
    LAST_DUMP_EPOCH.store(now, Ordering::Relaxed);
    eprintln!(
        "FORCE THREAD DUMP: {label} — dumping thread stacks (rate-limit bypassed)"
    );
    dump_thread_stacks(label);
    true
}

/// A guard that spawns a background task to detect stalls. When the
/// guarded operation completes (i.e., the guard is dropped), the
/// background task is cancelled. If the operation exceeds `threshold`,
/// a thread dump is written to `/tmp/nativelink-stall-<ts>.txt`.
///
/// This relies on tokio's timer infrastructure, so it cannot detect
/// stalls caused by the tokio runtime itself being blocked. The
/// runtime-watchdog OS thread in nativelink.rs covers that case.
#[must_use = "StallGuard is immediately cancelled if not held in a variable"]
#[derive(Debug)]
pub struct StallGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl StallGuard {
    /// Create a stall guard for an operation with the given label.
    /// If the guard is not dropped within `threshold`, a stack dump fires.
    pub fn new(threshold: Duration, label: &'static str) -> Self {
        Self::new_inner(threshold, label, None)
    }

    /// Create a stall guard with additional dynamic context (e.g. digest
    /// hash, size, operation details). The context string is included in
    /// the stall message and thread dump header when the threshold fires.
    pub fn with_context(threshold: Duration, label: &'static str, context: String) -> Self {
        Self::new_inner(threshold, label, Some(context))
    }

    fn new_inner(threshold: Duration, label: &'static str, context: Option<String>) -> Self {
        let handle = tokio::spawn(async move {
            tokio::time::sleep(threshold).await;
            let ctx_suffix = context
                .as_deref()
                .map_or_else(String::new, |c| format!(" [{c}]"));
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let prev = LAST_DUMP_EPOCH.load(Ordering::Relaxed);
            if now.saturating_sub(prev) >= MIN_DUMP_INTERVAL_SECS
                && LAST_DUMP_EPOCH
                    .compare_exchange(prev, now, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
            {
                eprintln!(
                    "STORE OPERATION STALL: {label}{ctx_suffix} has been running for >{threshold:.0?} — dumping thread stacks",
                );
                let dump_label = if ctx_suffix.is_empty() {
                    label.to_string()
                } else {
                    format!("{label}{ctx_suffix}")
                };
                // dump_thread_stacks does in-process work (signal
                // dispatch + symbol resolution + file I/O) bounded at
                // 5s. We still run it on the blocking pool because the
                // 1ms polling sleep would otherwise consume a tokio
                // worker for the duration of the dump, and the file
                // I/O is sync.
                let _ = tokio::task::spawn_blocking(move || {
                    dump_thread_stacks(&dump_label);
                });
            } else {
                eprintln!(
                    "STORE OPERATION STALL: {label}{ctx_suffix} has been running for >{threshold:.0?} (dump rate-limited)",
                );
            }
        });
        Self { handle }
    }
}

impl Drop for StallGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Dump all thread stacks to `/tmp/nativelink-stall-<timestamp>.txt`.
///
/// On Linux, reads `/proc/self/task/` to enumerate threads, collects
/// kernel-level info (comm, wchan, state, context switches, kernel
/// stack), and dispatches `SIGRTMIN+1` to each thread for cooperative
/// in-process userspace backtraces.
///
/// On macOS, enumerates threads via Mach APIs (`task_threads`,
/// `thread_info`) and dispatches `SIGUSR2` via `pthread_kill` for
/// cooperative in-process userspace backtraces. The previous
/// `sample(1)` invocation was removed in commit f6779f3a — it
/// whole-process suspended the target for 30s and triggered a
/// runtime-starvation feedback loop on workers.
///
/// On other platforms, this is a no-op (logs a message).
pub fn dump_thread_stacks(label: &str) {
    #[cfg(target_os = "linux")]
    dump_thread_stacks_linux(label);

    #[cfg(target_os = "macos")]
    dump_thread_stacks_macos(label);

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        eprintln!(
            "Thread dump not available on this platform (trigger: {label}, ts: {timestamp})"
        );
    }
}

/// Eagerly install the per-thread backtrace signal handler at process
/// start.
///
/// **Why eager.** The handler is the SIGUSR2 (macOS) / SIGRTMIN+1
/// (Linux) `sigaction` whose default disposition is process
/// termination. If we leave the install to the lazy
/// `capture_all_backtraces` path, an external `kill -USR2 $pid`
/// arriving before the first internal stall fires hits the kernel
/// default and kills the worker outright. Eagerly installing during
/// `main()` (before any worker thread spawns, so all threads inherit
/// the disposition) is the production-safety contract.
///
/// **Idempotent.** Backed by [`std::sync::Once`] inside each platform
/// module — calling twice is safe and the second call is a no-op. Safe
/// to call from a `tokio::spawn` block, from `main`, from a
/// per-thread-start hook.
///
/// **Order matters w.r.t. tokio's signal driver.** On macOS we want
/// our `sigaction` installed BEFORE
/// [`tokio::signal::unix::signal(SignalKind::user_defined2())`] is ever
/// constructed; tokio's `signal-hook-registry` chains the prior
/// `sigaction` (captured into its `prev` slot at registration time)
/// and calls it FIRST on signal arrival, so our slot-based capture
/// runs ahead of tokio's wakeup-pipe write. If we install after
/// tokio's signal driver, signal-hook will have already captured
/// `SIG_DFL` as `prev` and will never invoke our handler — internal
/// `pthread_kill(SIGUSR2)` rounds will then be no-ops.
///
/// On platforms without a per-thread backtrace path (anything other
/// than Linux / macOS) this is a no-op.
pub fn install_dump_signal_handler() {
    #[cfg(target_os = "linux")]
    signal_dumper::install_signal_handler();
    #[cfg(target_os = "macos")]
    signal_dumper_macos::install_signal_handler();
}

/// Spawn a long-running tokio task that listens for an external
/// `kill -USR2 $pid` (or platform-equivalent) and triggers a
/// per-thread backtrace dump from outside the signal handler.
///
/// **External trigger UX.** With this task running, an operator can
/// run `kill -USR2 $(pgrep -x nativelink)` and get a usable thread
/// dump at `/tmp/nativelink-stall-<ts>.txt` with all threads' state
/// and userspace backtraces — no in-process restart, no test harness
/// required.
///
/// **Coexistence with internal capture.** Internal stall paths
/// ([`StallGuard`], [`force_dump_thread_stacks`]) drive
/// `capture_all_backtraces` directly via `pthread_kill`. Each of those
/// per-thread `pthread_kill(SIGUSR2)` invocations also wakes this
/// listener (tokio's signal driver coalesces, so the listener only
/// sees one wake per round). The listener checks `dump_in_progress()`
/// before launching its own dump — if an internal round is already
/// running, the listener is a no-op. If the listener fires standalone,
/// it goes through [`force_dump_thread_stacks`], which is rate-limited
/// by `MIN_FORCE_DUMP_INTERVAL_SECS` so a `kill -USR2` storm cannot
/// flood `/tmp` or runaway the dump pipeline.
///
/// **Required ordering.** Call this AFTER
/// [`install_dump_signal_handler`] and AFTER the tokio runtime exists
/// (this function uses `tokio::signal::unix`, which needs a tokio
/// signal driver). The intended call site is inside the `runtime
/// .block_on(async { ... })` envelope, near the top of the async
/// section.
///
/// On platforms without a per-thread backtrace path this is a no-op.
#[cfg(unix)]
pub fn spawn_external_dump_listener() {
    use tokio::signal::unix::{SignalKind, signal};

    // SignalKind::user_defined2() corresponds to SIGUSR2 on every Unix
    // tokio supports. On Linux we keep the same external-trigger UX
    // (operators commonly script `kill -USR2`) even though our
    // internal Linux dumper uses SIGRTMIN+1 — the internal and
    // external triggers are intentionally on different signals on
    // Linux to avoid collision with libraries that already speak
    // SIGUSR2 in the same process.
    let mut stream = match signal(SignalKind::user_defined2()) {
        Ok(s) => s,
        Err(err) => {
            eprintln!(
                "failed to register SIGUSR2 listener for external thread-dump trigger: {err}",
            );
            return;
        }
    };

    tokio::spawn(async move {
        while stream.recv().await.is_some() {
            #[cfg(target_os = "macos")]
            let internal_active = signal_dumper_macos::dump_in_progress();
            #[cfg(target_os = "linux")]
            let internal_active = signal_dumper::dump_in_progress();
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let internal_active = false;

            if internal_active {
                // Internal capture round is mid-flight; the dump will
                // be produced by the StallGuard / force_dump caller.
                // Skipping prevents two parallel dumps from racing on
                // SLOT_COUNT/COLLECTOR bookkeeping.
                eprintln!(
                    "external SIGUSR2: internal dump already in progress, skipping",
                );
                continue;
            }
            // force_dump_thread_stacks ultimately calls
            // capture_all_backtraces, which polls for handler completion
            // via `std::thread::sleep(1ms)` for up to 5s. Running this
            // inline on the listener task would block one tokio worker
            // for the full dump duration — exactly the runtime-starvation
            // anti-pattern the deleted `sample`-subprocess wedge
            // represented. Wrap in spawn_blocking to mirror the
            // StallGuard pattern (see `new_inner` above).
            let _ = tokio::task::spawn_blocking(|| {
                force_dump_thread_stacks("external SIGUSR2");
            });
        }
    });
}

#[cfg(not(unix))]
pub fn spawn_external_dump_listener() {
    // No SIGUSR2 on non-Unix; the install is a no-op too.
}

/// Cooperative signal-based thread stack dumper for Linux.
///
/// Instead of spawning eu-stack (which takes 30s+ and can hang), we:
/// 1. Enumerate threads via /proc/self/task/
/// 2. Collect kernel-level info (comm, wchan, state, kernel stack)
/// 3. Send a realtime signal to each thread via tgkill()
/// 4. Each thread's signal handler captures its own backtrace (unresolved)
/// 5. Collector waits for all threads to respond (with timeout)
/// 6. Resolve symbols in bulk, format output
///
/// Total time: typically <100ms for hundreds of threads.
#[cfg(target_os = "linux")]
mod signal_dumper {
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
    use std::sync::Once;

    /// Maximum threads we can capture backtraces from in a single dump.
    /// Pre-allocated to avoid allocation in the signal handler.
    const MAX_THREADS: usize = 1024;

    /// Signal used for cooperative stack capture. SIGRTMIN is often used
    /// by glibc/pthreads internally, so we use SIGRTMIN + 1.
    fn dump_signal() -> i32 {
        libc::SIGRTMIN() + 1
    }

    /// A single slot for one thread's captured backtrace.
    ///
    /// The signal handler writes raw instruction pointer addresses here.
    /// We avoid using `backtrace::Backtrace` directly in the handler
    /// because its internal Vec allocation may not be async-signal-safe
    /// under all allocators. Instead we capture raw IPs into a fixed
    /// array, then build Backtrace frames after collection.
    struct BacktraceSlot {
        /// Raw instruction pointer addresses captured by the signal handler.
        ips: [usize; 128],
        /// Number of valid entries in `ips`.
        count: usize,
        /// TID that this slot belongs to (set before signaling).
        tid: u32,
        /// Set to true by the signal handler after capture completes.
        captured: AtomicBool,
    }

    impl BacktraceSlot {
        const fn empty() -> Self {
            Self {
                ips: [0; 128],
                count: 0,
                tid: 0,
                captured: AtomicBool::new(false),
            }
        }

        fn reset(&mut self, tid: u32) {
            self.count = 0;
            self.tid = tid;
            self.captured.store(false, Ordering::Release);
        }
    }

    /// Global state for the signal-based backtrace collector.
    ///
    /// Only one dump can be in progress at a time (enforced by
    /// `DUMP_IN_PROGRESS`). The collector thread sets up the slots,
    /// sends signals, and waits. Signal handlers write to their
    /// assigned slot.
    struct Collector {
        slots: [std::cell::UnsafeCell<BacktraceSlot>; MAX_THREADS],
        /// Number of active slots in this dump round.
        active_count: AtomicUsize,
        /// Number of threads that have finished capturing.
        done_count: AtomicUsize,
    }

    // SAFETY: The slots are only written to by their owning thread's
    // signal handler (one writer per slot), and read by the collector
    // after all signal handlers have completed or timed out. The
    // AtomicBool in each slot provides the synchronization barrier.
    unsafe impl Sync for Collector {}
    unsafe impl Send for Collector {}

    impl Collector {
        const fn new() -> Self {
            // Use a macro to repeat the UnsafeCell initialization
            // since UnsafeCell::new is not Copy.
            const EMPTY_CELL: std::cell::UnsafeCell<BacktraceSlot> =
                std::cell::UnsafeCell::new(BacktraceSlot::empty());
            Self {
                slots: [EMPTY_CELL; MAX_THREADS],
                active_count: AtomicUsize::new(0),
                done_count: AtomicUsize::new(0),
            }
        }
    }

    static COLLECTOR: Collector = Collector::new();
    static SIGNAL_INSTALLED: Once = Once::new();
    static DUMP_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

    /// Maps a TID to its slot index. Called from the signal handler
    /// and from the collector setup. Must be consistent.
    ///
    /// We store the TID->index mapping in each slot's `tid` field and
    /// the signal handler searches linearly. With MAX_THREADS=1024 and
    /// typical thread counts of 50-300, this is fast enough for a
    /// signal handler (~1us).
    static SLOT_COUNT: AtomicU32 = AtomicU32::new(0);

    fn find_slot_for_tid(tid: u32) -> Option<usize> {
        let count = SLOT_COUNT.load(Ordering::Acquire) as usize;
        for i in 0..count {
            // SAFETY: We only read the tid field, which was set before
            // signaling and won't be modified until the dump is done.
            let slot = unsafe { &*COLLECTOR.slots[i].get() };
            if slot.tid == tid {
                return Some(i);
            }
        }
        None
    }

    /// Signal handler invoked on the target thread. Captures raw
    /// instruction pointers using `backtrace::trace_unsynchronized`.
    ///
    /// SAFETY requirements for async-signal-safety:
    /// - No heap allocation (we write to pre-allocated fixed array)
    /// - No locks (we use atomic flag for completion)
    /// - `backtrace::trace_unsynchronized` walks the stack using
    ///   frame pointers or DWARF unwind info without allocating
    unsafe extern "C" fn signal_handler(
        _sig: libc::c_int,
        _info: *mut libc::siginfo_t,
        _ctx: *mut libc::c_void,
    ) {
        // SAFETY: SYS_gettid always succeeds and returns the caller's TID.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) } as u32;
        let Some(idx) = find_slot_for_tid(tid) else {
            return;
        };
        // SAFETY: Each slot is exclusively owned by the thread whose TID
        // matches slot.tid. The collector thread set up the slot before
        // sending the signal, and won't read it until captured=true.
        let slot = unsafe { &mut *COLLECTOR.slots[idx].get() };

        // Capture raw instruction pointers without resolving symbols.
        // trace_unsynchronized is the non-locking variant suitable for
        // signal handlers.
        let mut count = 0usize;
        let max = slot.ips.len();
        // SAFETY: We are in a signal handler context. trace_unsynchronized
        // is the correct function here — it skips internal locks that
        // trace() would take (which could deadlock in a signal handler).
        // We write only to pre-allocated stack-local and slot memory.
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                if count < max {
                    slot.ips[count] = frame.ip() as usize;
                    count += 1;
                    true
                } else {
                    false
                }
            });
        }
        slot.count = count;
        slot.captured.store(true, Ordering::Release);
        COLLECTOR.done_count.fetch_add(1, Ordering::Release);
    }

    /// Install the signal handler (once).
    ///
    /// **Should be called eagerly at process start** (see
    /// [`super::install_dump_signal_handler`]) so the SIGRTMIN+1
    /// disposition is set before any worker thread spawns. The Linux
    /// path uses a realtime signal whose default disposition is also
    /// process termination, so leaving the install to lazy first-stall
    /// is unsafe in production for the same reason as macOS SIGUSR2.
    pub(super) fn install_signal_handler() {
        SIGNAL_INSTALLED.call_once(|| {
            // SAFETY: sigaction is the standard POSIX install path. We
            // zero-initialize the struct and only set the documented
            // fields. SA_SIGINFO matches the 3-arg handler signature;
            // SA_RESTART asks the kernel to restart interrupted syscalls
            // so we don't trip up application code that wasn't expecting
            // EINTR from us. signal_handler is a static function pointer
            // with the C ABI signature sigaction expects.
            unsafe {
                let mut sa: libc::sigaction = core::mem::zeroed();
                sa.sa_sigaction = signal_handler as *const () as usize;
                sa.sa_flags = libc::SA_RESTART | libc::SA_SIGINFO;
                libc::sigemptyset(&mut sa.sa_mask);
                let ret = libc::sigaction(dump_signal(), &sa, core::ptr::null_mut());
                if ret != 0 {
                    eprintln!(
                        "failed to install backtrace signal handler: {}",
                        std::io::Error::last_os_error()
                    );
                }
            }
        });
    }

    /// True iff a per-thread backtrace dump is currently in flight.
    ///
    /// Mirrors the macOS `dump_in_progress` so the external-trigger
    /// listener has a single shape across platforms.
    pub(super) fn dump_in_progress() -> bool {
        DUMP_IN_PROGRESS.load(Ordering::Acquire)
    }

    /// Resolved backtrace for one thread.
    pub(super) struct ThreadBacktrace {
        pub tid: u32,
        pub symbols: Vec<ResolvedFrame>,
    }

    /// A single resolved stack frame.
    pub(super) struct ResolvedFrame {
        pub ip: usize,
        pub name: Option<String>,
        pub filename: Option<String>,
        pub lineno: Option<u32>,
    }

    /// Capture backtraces from all threads cooperatively.
    ///
    /// Returns a vec of per-thread resolved backtraces. Threads that
    /// did not respond within the timeout are omitted.
    pub(super) fn capture_all_backtraces(
        tids: &[u32],
    ) -> Vec<ThreadBacktrace> {
        install_signal_handler();

        // Only one dump at a time.
        if DUMP_IN_PROGRESS.swap(true, Ordering::SeqCst) {
            eprintln!("cooperative stack dump already in progress, skipping");
            return Vec::new();
        }

        // Ensure we clear the in-progress flag when done.
        struct DumpGuard;
        impl Drop for DumpGuard {
            fn drop(&mut self) {
                DUMP_IN_PROGRESS.store(false, Ordering::SeqCst);
            }
        }
        let _guard = DumpGuard;

        let thread_count = tids.len().min(MAX_THREADS);
        SLOT_COUNT.store(thread_count as u32, Ordering::Release);
        COLLECTOR.active_count.store(thread_count, Ordering::Release);
        COLLECTOR.done_count.store(0, Ordering::Release);

        // Initialize slots.
        for (i, &tid) in tids.iter().take(thread_count).enumerate() {
            // SAFETY: No signal handler is accessing these slots yet
            // because we haven't sent any signals.
            unsafe {
                (*COLLECTOR.slots[i].get()).reset(tid);
            }
        }

        // Send signal to each thread.
        let pid = std::process::id() as i32;
        let sig = dump_signal();
        let mut signaled = 0u32;
        for &tid in tids.iter().take(thread_count) {
            // SAFETY: tgkill is a Linux syscall that targets a specific
            // thread within a thread group. pid/tid come from
            // /proc/self/task enumeration and process::id(), both safe
            // to use as syscall arguments. dump_signal() returns a
            // valid realtime signal number. ESRCH (thread exited
            // between enumeration and signal) is the documented benign
            // failure and is silently dropped — that thread will
            // simply time out in the polling loop.
            let ret = unsafe {
                libc::syscall(libc::SYS_tgkill, pid, tid as i32, sig)
            };
            if ret == 0 {
                signaled += 1;
            }
            // Thread may have exited between enumeration and signal —
            // that's fine, we just won't get its backtrace.
        }

        // Wait for threads to respond, with timeout.
        const TIMEOUT: core::time::Duration = core::time::Duration::from_secs(5);
        const POLL_INTERVAL: core::time::Duration =
            core::time::Duration::from_millis(1);
        let deadline = std::time::Instant::now() + TIMEOUT;

        while COLLECTOR.done_count.load(Ordering::Acquire) < signaled as usize {
            if std::time::Instant::now() >= deadline {
                let done = COLLECTOR.done_count.load(Ordering::Acquire);
                eprintln!(
                    "backtrace capture timeout: {done}/{signaled} threads responded in {TIMEOUT:.0?}"
                );
                break;
            }
            std::thread::sleep(POLL_INTERVAL);
        }

        // Collect and resolve backtraces.
        let mut results = Vec::with_capacity(thread_count);
        for i in 0..thread_count {
            // SAFETY: Signal handlers have either completed (captured=true)
            // or timed out. We only read slots that are marked captured.
            let slot = unsafe { &*COLLECTOR.slots[i].get() };
            if !slot.captured.load(Ordering::Acquire) {
                // Thread didn't respond (D state, exited, etc.)
                results.push(ThreadBacktrace {
                    tid: slot.tid,
                    symbols: Vec::new(),
                });
                continue;
            }

            // Resolve symbols for each instruction pointer.
            let mut frames = Vec::with_capacity(slot.count);
            for j in 0..slot.count {
                let ip = slot.ips[j];
                let mut resolved = ResolvedFrame {
                    ip,
                    name: None,
                    filename: None,
                    lineno: None,
                };
                // backtrace::resolve takes a *mut c_void pointer.
                backtrace::resolve(ip as *mut core::ffi::c_void, |symbol| {
                    if resolved.name.is_none() {
                        resolved.name =
                            symbol.name().map(|n| n.to_string());
                    }
                    if resolved.filename.is_none() {
                        resolved.filename = symbol
                            .filename()
                            .map(|p| p.display().to_string());
                    }
                    if resolved.lineno.is_none() {
                        resolved.lineno = symbol.lineno();
                    }
                });
                frames.push(resolved);
            }
            results.push(ThreadBacktrace {
                tid: slot.tid,
                symbols: frames,
            });
        }

        // Clear slot tids so a stale signal arriving after this dump
        // completes (e.g., from a thread that woke up post-deadline)
        // doesn't write into a slot the next dump round may have re-keyed.
        // Mirrors the macOS dispatcher's mach_port=0 defensive pattern.
        for i in 0..thread_count {
            // SAFETY: The dump is over; no handler should be running
            // against these slots. Even if a late handler arrives,
            // tid=0 makes find_slot_for_tid return None.
            unsafe {
                (*COLLECTOR.slots[i].get()).tid = 0;
            }
        }

        results
    }
}

/// Cooperative signal-based thread stack dumper for macOS.
///
/// macOS analog of the Linux [`signal_dumper`]. Differences:
/// 1. Threads are enumerated via Mach `task_threads()` (not `/proc`).
/// 2. Each thread is identified by its **mach port**, not a `tid`.
/// 3. The signal is delivered with `pthread_kill(pthread_t, SIGUSR2)` —
///    we look up `pthread_t` from the mach port via the private-but-
///    stable `pthread_from_mach_thread_np` API. This is what async-
///    profiler does and what Apple's own tooling relies on.
/// 4. Inside the handler, we MUST use `mach_thread_self()` (an actual
///    syscall) to identify the running thread — NOT
///    `pthread_mach_thread_np()`, which is *not* async-signal-safe
///    (it walks pthread internal data and can deadlock against
///    pthread library locks). See:
///    <https://github.com/async-profiler/async-profiler/discussions/1557>
///
/// Bounded wall-clock budget:
/// - 5s outer timeout for the whole dump (matches Linux).
/// - The collector polls every 1ms; if a thread is wedged in a
///   signal-blocked state, we time out and report a `<no response>`
///   for that slot rather than hanging the watchdog.
#[cfg(target_os = "macos")]
mod signal_dumper_macos {
    use std::sync::Once;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

    /// Maximum number of threads a single dump can capture. Pre-allocated
    /// at compile time to avoid any allocation in the signal handler.
    /// Workers typically run with ~50–300 OS threads (tokio multi-thread
    /// runtime + blocking pool + assorted helper threads); 1024 is a
    /// generous ceiling.
    const MAX_THREADS: usize = 1024;

    /// Signal used for cooperative stack capture on macOS.
    ///
    /// We deliberately pick `SIGUSR2` (not `SIGUSR1`) because:
    /// - SIGUSR1 is widely conventionalized by libraries (e.g., Go runtime,
    ///   crash reporters) and reserving it for our use risks collisions.
    /// - SIGPROF is what setitimer-based samplers use; on macOS it is not
    ///   reliably delivered to the running thread (Russ Cox's note on
    ///   Go's macOS profiler). We avoid it to side-step that family of
    ///   issues, even though our `pthread_kill` direct delivery does not
    ///   suffer the setitimer path's bug.
    /// - SIGUSR2 is less commonly hijacked and its semantics are a clean
    ///   "user-defined" channel, ideal for in-process diagnostics.
    const DUMP_SIGNAL: libc::c_int = libc::SIGUSR2;

    /// One slot per thread. Pre-allocated; never resized; written to by
    /// exactly one signal handler invocation per dump round.
    struct BacktraceSlot {
        /// Mach port of the thread that owns this slot during a dump.
        /// The handler matches by calling `mach_thread_self()` and
        /// linearly searching `slots[].mach_port`. `0` means unused.
        mach_port: AtomicU32,
        /// Raw instruction pointers captured by the handler.
        ips: [usize; 128],
        /// Number of valid entries in `ips`.
        count: usize,
        /// Set to `true` by the handler once the capture is complete.
        captured: AtomicBool,
    }

    impl BacktraceSlot {
        const fn empty() -> Self {
            Self {
                mach_port: AtomicU32::new(0),
                ips: [0; 128],
                count: 0,
                captured: AtomicBool::new(false),
            }
        }

        fn reset(&mut self, mach_port: u32) {
            self.count = 0;
            self.captured.store(false, Ordering::Release);
            // mach_port is stored last so the handler observing this
            // slot also observes a clean `captured=false`.
            self.mach_port.store(mach_port, Ordering::Release);
        }
    }

    struct Collector {
        slots: [core::cell::UnsafeCell<BacktraceSlot>; MAX_THREADS],
    }

    // SAFETY: Each slot is exclusively owned by exactly one signal handler
    // per dump round (the one whose `mach_thread_self()` matches the slot's
    // `mach_port`). The collector reads slots only after
    // `captured.load(Acquire)` returns `true`, providing the synchronization
    // barrier between the handler's writes and the collector's reads.
    unsafe impl Sync for Collector {}
    unsafe impl Send for Collector {}

    impl Collector {
        const fn new() -> Self {
            const EMPTY_CELL: core::cell::UnsafeCell<BacktraceSlot> =
                core::cell::UnsafeCell::new(BacktraceSlot::empty());
            Self {
                slots: [EMPTY_CELL; MAX_THREADS],
            }
        }
    }

    static COLLECTOR: Collector = Collector::new();
    static SIGNAL_INSTALLED: Once = Once::new();
    static DUMP_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
    /// Number of slots populated for the current dump round. Read by the
    /// signal handler to bound its linear search.
    static SLOT_COUNT: AtomicU32 = AtomicU32::new(0);
    /// Number of handlers that have completed in the current round.
    static DONE_COUNT: AtomicUsize = AtomicUsize::new(0);

    // Mach FFI for the signal handler's port-deallocate path. Both
    // mach_task_self() (a port-name fetch — no refcount mutation) and
    // mach_port_deallocate() (a Mach trap that decrements a send-right
    // refcount) are async-signal-safe. They are declared here in the
    // module scope so the signal handler can call them without reaching
    // into the `enumerate_mach_threads_with_backtraces` function-local
    // extern block.
    type MachPortName = u32;
    type KernReturnT = i32;
    unsafe extern "C" {
        fn mach_task_self() -> MachPortName;
        fn mach_port_deallocate(task: MachPortName, name: MachPortName) -> KernReturnT;
    }

    /// Look up the slot index for a given mach port. Called only from the
    /// signal handler; must be async-signal-safe.
    ///
    /// The implementation is a simple linear scan over `[0, SLOT_COUNT)`.
    /// With MAX_THREADS=1024 and typical thread counts of 50–300 it costs
    /// well under a microsecond, which is acceptable inside a handler that
    /// itself runs unwinding.
    fn find_slot_for_mach_port(port: u32) -> Option<usize> {
        let count = SLOT_COUNT.load(Ordering::Acquire) as usize;
        for i in 0..count {
            // SAFETY: We only read the atomic mach_port field. The slot
            // was initialized by the collector before the signal was sent.
            let slot = unsafe { &*COLLECTOR.slots[i].get() };
            if slot.mach_port.load(Ordering::Acquire) == port {
                return Some(i);
            }
        }
        None
    }

    /// Async-signal-safe signal handler. Captures raw IPs into the
    /// thread's pre-allocated slot.
    ///
    /// SAFETY / async-signal-safety:
    /// - `mach_thread_self()` is a real syscall and is documented as
    ///   safe to call from any context (including signal handlers).
    /// - We do NOT call `pthread_mach_thread_np()`; that one walks
    ///   pthread internals and is *not* async-signal-safe. async-profiler
    ///   shipped this exact bug and fixed it by switching to
    ///   `mach_thread_self()`.
    /// - `backtrace::trace_unsynchronized` walks the stack via frame
    ///   pointers (or libunwind in the fallback path) without taking
    ///   any locks. Frame pointers are enabled across our build via
    ///   `.cargo/config.toml`'s `-C force-frame-pointers=yes` and
    ///   aarch64-apple-darwin's compiler default.
    /// - All writes target either stack-local variables or the slot's
    ///   pre-allocated `ips` array. No allocator calls.
    /// - `mach_port_deallocate` is a Mach trap (kernel-side syscall),
    ///   not a userspace pthread function — it does not take any
    ///   userspace locks and is safe from a signal handler. Every
    ///   `mach_thread_self()` call MUST be paired with deallocate to
    ///   avoid leaking send rights, which on a 24/7 worker accumulates
    ///   port-table pressure (each external SIGUSR2 with no matching
    ///   slot would otherwise leak one send right).
    ///
    /// TODO(#145 follow-up): the `dump_in_progress` flag protects the
    /// LISTENER from launching a second dump; it does NOT prevent
    /// kernel-driven SIGUSR2 delivery to a thread that's already
    /// targeted by an internal `pthread_kill` round. If a stray
    /// external `kill -USR2 $pid` lands on a tokio worker mid-internal
    /// round (the kernel routes it to the first non-blocking thread),
    /// the handler may run twice on the same slot — the second
    /// invocation overwrites `count`/`ips` (idempotent in practice
    /// because the same thread captures the same backtrace) and
    /// double-increments `DONE_COUNT` (causes the polling loop to
    /// exit slightly early, dropping a few late responders). A
    /// generation counter on the slot (handler bails when it
    /// observes `slot.gen != round.gen`) would close this. Low
    /// severity: the dump still completes, the duplicated frames
    /// match, and external SIGUSR2 mid-internal round is a corner
    /// case (operator + automatic dump colliding within ~5s).
    unsafe extern "C" fn signal_handler(
        _sig: libc::c_int,
        _info: *mut libc::siginfo_t,
        _ctx: *mut libc::c_void,
    ) {
        // SAFETY: mach_thread_self() is async-signal-safe by design — it
        // is a real syscall and the only correct way to get the running
        // Mach thread's port from inside a handler. The libc deprecation
        // suggests routing through the `mach2` crate, but `mach2` simply
        // wraps the same syscall; pulling in a new crate dependency is
        // not justified for a single FFI call. allow(deprecated)
        // documents the intentional choice.
        #[allow(deprecated)]
        let port = unsafe { libc::mach_thread_self() };

        // RAII guard: deallocate the send right on every exit path from
        // this handler (slot-not-found early return, capture completion,
        // or any future panic-safe exit). Mach trap, async-signal-safe.
        struct PortGuard(u32);
        impl Drop for PortGuard {
            fn drop(&mut self) {
                // SAFETY: self.0 is a send right we obtained from
                // mach_thread_self() in this handler invocation. The
                // matching deallocate is required to balance the
                // refcount; doing it in Drop ensures it runs on every
                // exit path. mach_task_self() is a port name (no
                // refcount mutation), so it's safe to fetch each time.
                unsafe {
                    let task = mach_task_self();
                    let _ = mach_port_deallocate(task, self.0);
                }
            }
        }
        let _port_guard = PortGuard(port);

        let Some(idx) = find_slot_for_mach_port(port) else {
            // Not our signal (or stale/cancelled dump). Don't touch any
            // slot; the PortGuard deallocates the send right on return.
            return;
        };
        // SAFETY: This slot is exclusively owned by the current thread for
        // the duration of this handler invocation. The collector won't
        // read until `captured.store(true)` below.
        let slot = unsafe { &mut *COLLECTOR.slots[idx].get() };

        let mut count = 0usize;
        let max = slot.ips.len();
        // SAFETY: trace_unsynchronized is the non-locking variant of
        // backtrace::trace, intended for signal-handler context. It
        // writes only to caller-provided memory.
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                if count < max {
                    slot.ips[count] = frame.ip() as usize;
                    count += 1;
                    true
                } else {
                    false
                }
            });
        }
        slot.count = count;
        slot.captured.store(true, Ordering::Release);
        DONE_COUNT.fetch_add(1, Ordering::Release);
        // _port_guard runs here, deallocating the send right.
    }

    /// Install the SIGUSR2 handler exactly once for the lifetime of the
    /// process. Subsequent `capture_all_backtraces` calls reuse it.
    ///
    /// **Must be called eagerly at process start** (see
    /// [`super::install_dump_signal_handler`]). If left to lazy install
    /// via `capture_all_backtraces`, an external `kill -USR2 $pid`
    /// arriving before any internal stall path runs will hit the kernel
    /// default disposition for SIGUSR2 — termination — and kill the
    /// process. Eager install is the production-safety contract.
    pub(super) fn install_signal_handler() {
        SIGNAL_INSTALLED.call_once(|| {
            // SAFETY: sigaction is the standard POSIX install path. We
            // zero-initialize the struct and only set the documented
            // fields. SA_SIGINFO matches the 3-arg handler signature;
            // SA_RESTART asks the kernel to restart interrupted syscalls
            // so we don't trip up application code that wasn't expecting
            // EINTR from us.
            unsafe {
                let mut sa: libc::sigaction = core::mem::zeroed();
                sa.sa_sigaction = signal_handler as *const () as usize;
                sa.sa_flags = libc::SA_RESTART | libc::SA_SIGINFO;
                libc::sigemptyset(&mut sa.sa_mask);
                let ret = libc::sigaction(DUMP_SIGNAL, &sa, core::ptr::null_mut());
                if ret != 0 {
                    eprintln!(
                        "failed to install macOS backtrace signal handler: {}",
                        std::io::Error::last_os_error(),
                    );
                }
            }
        });
    }

    /// True iff a per-thread backtrace dump is currently in flight.
    ///
    /// Used by the external-trigger listener to skip a `force_dump`
    /// while an internal `capture_all_backtraces` round is mid-flight —
    /// the round will already be capturing per-thread state, and a
    /// concurrent re-entry just churns the SLOT_COUNT bookkeeping.
    pub(super) fn dump_in_progress() -> bool {
        DUMP_IN_PROGRESS.load(Ordering::Acquire)
    }

    /// Resolved backtrace for one thread.
    pub(super) struct ThreadBacktrace {
        pub mach_port: u32,
        pub symbols: Vec<ResolvedFrame>,
        /// `false` if the thread did not respond before the timeout fired.
        pub responded: bool,
    }

    /// A single resolved stack frame.
    pub(super) struct ResolvedFrame {
        pub ip: usize,
        pub name: Option<String>,
        pub filename: Option<String>,
        pub lineno: Option<u32>,
    }

    /// Request a per-thread backtrace for every `(mach_port, pthread_t)`
    /// in `targets`, except for `self_mach_port` (the calling thread is
    /// dumped synchronously by the caller via `Backtrace::force_capture`).
    ///
    /// Returns one [`ThreadBacktrace`] per target. Threads that did not
    /// respond before the 5s outer deadline get an entry with
    /// `responded=false` and an empty `symbols` vec.
    pub(super) fn capture_all_backtraces(
        targets: &[(u32, libc::pthread_t)],
        self_mach_port: u32,
    ) -> Vec<ThreadBacktrace> {
        install_signal_handler();

        if DUMP_IN_PROGRESS.swap(true, Ordering::SeqCst) {
            eprintln!("macOS cooperative stack dump already in progress, skipping");
            return Vec::new();
        }
        struct DumpGuard;
        impl Drop for DumpGuard {
            fn drop(&mut self) {
                DUMP_IN_PROGRESS.store(false, Ordering::SeqCst);
            }
        }
        let _guard = DumpGuard;

        // Filter out the calling thread; we already have its backtrace.
        // Truncate to MAX_THREADS — extreme thread counts get the first N
        // covered. The output formatter notes the truncation.
        let active: Vec<(u32, libc::pthread_t)> = targets
            .iter()
            .copied()
            .filter(|(port, _)| *port != self_mach_port)
            .take(MAX_THREADS)
            .collect();
        let n = active.len();

        SLOT_COUNT.store(u32::try_from(n).unwrap_or(u32::MAX), Ordering::Release);
        DONE_COUNT.store(0, Ordering::Release);

        for (i, &(port, _)) in active.iter().enumerate() {
            // SAFETY: No handler is running for this slot yet because we
            // have not sent any signal. We hold exclusive write access.
            unsafe {
                (*COLLECTOR.slots[i].get()).reset(port);
            }
        }

        // Send SIGUSR2 to each target via pthread_kill. pthread_kill targets
        // the specific pthread (vs. process-wide kill(PID, sig)), which is
        // exactly what we want for per-thread sampling.
        let mut signaled = 0usize;
        for &(_, pthread) in &active {
            // pthread_kill returns 0 on success, errno on failure. The
            // most common failure is ESRCH (thread already exited between
            // enumeration and signal delivery) — that's expected and
            // benign, the corresponding slot will simply time out and
            // report `<no response>`.
            // SAFETY: pthread is a valid pthread_t obtained from
            // pthread_from_mach_thread_np in the caller. DUMP_SIGNAL is
            // a valid signal number.
            let ret = unsafe { libc::pthread_kill(pthread, DUMP_SIGNAL) };
            if ret == 0 {
                signaled += 1;
            }
        }

        // Wait for all signaled threads to respond, with a hard 5s outer
        // deadline. Per-slot per-iteration cost is ~1ms (the poll
        // interval); the deadline stops a single hung thread from
        // wedging the dump.
        const OUTER_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(5);
        const POLL_INTERVAL: core::time::Duration = core::time::Duration::from_millis(1);
        let deadline = std::time::Instant::now() + OUTER_TIMEOUT;

        while DONE_COUNT.load(Ordering::Acquire) < signaled {
            if std::time::Instant::now() >= deadline {
                let done = DONE_COUNT.load(Ordering::Acquire);
                eprintln!(
                    "macOS backtrace capture timeout: {done}/{signaled} threads responded in {OUTER_TIMEOUT:.0?}",
                );
                break;
            }
            std::thread::sleep(POLL_INTERVAL);
        }

        // Resolve symbols outside the handler. resolve()/resolve_unsynchronized
        // both allocate; doing this on the collector thread is safe.
        let mut results = Vec::with_capacity(n);
        for (i, &(port, _)) in active.iter().enumerate() {
            // SAFETY: All handlers have either completed (captured=true)
            // or timed out. We don't dereference `ips` for non-captured
            // slots.
            let slot = unsafe { &*COLLECTOR.slots[i].get() };
            let captured = slot.captured.load(Ordering::Acquire);
            if !captured {
                results.push(ThreadBacktrace {
                    mach_port: port,
                    symbols: Vec::new(),
                    responded: false,
                });
                continue;
            }

            let mut frames = Vec::with_capacity(slot.count);
            for j in 0..slot.count {
                let ip = slot.ips[j];
                let mut resolved = ResolvedFrame {
                    ip,
                    name: None,
                    filename: None,
                    lineno: None,
                };
                backtrace::resolve(ip as *mut core::ffi::c_void, |symbol| {
                    if resolved.name.is_none() {
                        resolved.name = symbol.name().map(|n| n.to_string());
                    }
                    if resolved.filename.is_none() {
                        resolved.filename =
                            symbol.filename().map(|p| p.display().to_string());
                    }
                    if resolved.lineno.is_none() {
                        resolved.lineno = symbol.lineno();
                    }
                });
                frames.push(resolved);
            }
            results.push(ThreadBacktrace {
                mach_port: port,
                symbols: frames,
                responded: true,
            });
        }

        // Clear slot mach ports so a stale signal arriving after this
        // dump completes (e.g., from a thread that woke up post-deadline)
        // doesn't write into a slot the next dump round may have re-keyed.
        for i in 0..n {
            // SAFETY: The dump is over; no handler should be running
            // against these slots. Even if a late handler arrives, the
            // mach_port=0 store makes find_slot_for_mach_port return None.
            unsafe {
                (*COLLECTOR.slots[i].get())
                    .mach_port
                    .store(0, Ordering::Release);
            }
        }

        results
    }
}

#[cfg(target_os = "linux")]
fn dump_thread_stacks_linux(label: &str) {
    use std::fmt::Write as _;

    let start = std::time::Instant::now();
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let path = format!("/tmp/nativelink-stall-{timestamp_ms}.txt");
    let mut output = String::new();

    let _ = writeln!(output, "=== STORE OPERATION STALL THREAD DUMP ===");
    let _ = writeln!(output, "Trigger: {label}");
    let _ = writeln!(output, "Timestamp: {timestamp_ms}");
    let _ = writeln!(output, "PID: {}", std::process::id());
    let _ = writeln!(output);

    let task_dir = "/proc/self/task";
    let entries = match std::fs::read_dir(task_dir) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("Failed to read {task_dir}: {err}");
            return;
        }
    };

    let mut tids: Vec<u32> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
        .collect();
    tids.sort();

    let _ = writeln!(output, "Thread count: {}", tids.len());
    let _ = writeln!(output);

    // Phase 1: Collect kernel-level info from /proc (fast, <10ms).
    // Build a map of tid -> (comm, kernel info) for later merging.
    let mut thread_names: std::collections::HashMap<u32, String> =
        std::collections::HashMap::new();

    for &tid in &tids {
        let base = format!("{task_dir}/{tid}");

        // Thread name
        let comm = std::fs::read_to_string(format!("{base}/comm"))
            .unwrap_or_default()
            .trim()
            .to_string();
        if !comm.is_empty() {
            thread_names.insert(tid, comm.clone());
        }
    }

    // Phase 2: Cooperative signal-based backtrace capture.
    let backtraces = signal_dumper::capture_all_backtraces(&tids);
    let capture_elapsed = start.elapsed();

    // Build a lookup from TID -> backtrace for output formatting.
    let bt_map: std::collections::HashMap<u32, &signal_dumper::ThreadBacktrace> =
        backtraces.iter().map(|bt| (bt.tid, bt)).collect();

    // Phase 3: Format combined output (kernel info + userspace backtrace).
    for &tid in &tids {
        let tid_str = tid.to_string();
        let base = format!("{task_dir}/{tid_str}");
        let comm = thread_names
            .get(&tid)
            .map(String::as_str)
            .unwrap_or("<unknown>");

        let _ = writeln!(output, "--- TID {tid} ({comm}) ---");

        // Wait channel
        if let Ok(wchan) = std::fs::read_to_string(format!("{base}/wchan")) {
            let wchan = wchan.trim();
            if !wchan.is_empty() && wchan != "0" {
                let _ = writeln!(output, "  wchan: {wchan}");
            }
        }
        // Status lines
        if let Ok(status) = std::fs::read_to_string(format!("{base}/status")) {
            for line in status.lines() {
                if line.starts_with("State:")
                    || line.starts_with("voluntary_ctxt_switches:")
                    || line.starts_with("nonvoluntary_ctxt_switches:")
                {
                    let _ = writeln!(output, "  {line}");
                }
            }
        }
        // Kernel stack
        if let Ok(stack) = std::fs::read_to_string(format!("{base}/stack")) {
            let trimmed = stack.trim();
            if !trimmed.is_empty() {
                let _ = writeln!(output, "  kernel stack:");
                for line in trimmed.lines() {
                    let _ = writeln!(output, "    {line}");
                }
            }
        }

        // Userspace backtrace from cooperative capture.
        if let Some(bt) = bt_map.get(&tid) {
            if bt.symbols.is_empty() {
                let _ = writeln!(output, "  userspace backtrace: <no response>");
            } else {
                let _ = writeln!(output, "  userspace backtrace:");
                for (i, frame) in bt.symbols.iter().enumerate() {
                    let name = frame.name.as_deref().unwrap_or("<unknown>");
                    if let (Some(file), Some(line)) =
                        (&frame.filename, frame.lineno)
                    {
                        let _ = writeln!(
                            output,
                            "    #{i:>3} {:#018x} {name}",
                            frame.ip
                        );
                        let _ = writeln!(
                            output,
                            "         at {file}:{line}"
                        );
                    } else {
                        let _ = writeln!(
                            output,
                            "    #{i:>3} {:#018x} {name}",
                            frame.ip
                        );
                    }
                }
            }
        }

        let _ = writeln!(output);
    }

    let total_elapsed = start.elapsed();
    let responded = backtraces.iter().filter(|bt| !bt.symbols.is_empty()).count();
    let _ = writeln!(
        output,
        "=== Dump complete: {responded}/{} threads responded, capture: {capture_elapsed:.1?}, total: {total_elapsed:.1?} ===",
        tids.len()
    );

    match write_dump_owner_only(&path, &output) {
        Ok(()) => eprintln!(
            "Thread dump written to {path} ({responded}/{} threads, {total_elapsed:.1?})",
            tids.len()
        ),
        Err(err) => eprintln!("Failed to write thread dump to {path}: {err}"),
    }

    cleanup_old_stall_dumps();
}

/// Dump thread info on macOS using Mach APIs, `pthread_kill(SIGUSR2)`, and
/// in-process libunwind via the `backtrace` crate.
///
/// Pipeline:
/// 1. Enumerate all threads in this task via `task_threads()`.
/// 2. For each thread, look up its `pthread_t` (for signaling),
///    `pthread_getname_np` (for the dump label), and
///    `thread_info(THREAD_BASIC_INFO)` (for CPU usage / run state).
/// 3. Capture the calling thread's backtrace synchronously via
///    `std::backtrace::Backtrace::force_capture()` (we already own it,
///    no need to signal ourselves).
/// 4. Send `SIGUSR2` to every other thread via `pthread_kill`. Each
///    signal handler captures its own raw IPs into a pre-allocated slot
///    (see [`signal_dumper_macos`]).
/// 5. Wait up to 5s for handlers to complete, then resolve symbols off
///    the handler thread (resolve() allocates and is not signal-safe).
/// 6. Format everything to `/tmp/nativelink-stall-<ts>.txt` and clean
///    up old dump files.
///
/// This is the macOS analog of [`dump_thread_stacks_linux`]. The
/// previous implementation invoked `sample(1)`; it was removed in
/// commit f6779f3a because `sample` whole-process suspended the target
/// for 30s and put workers into runtime-starvation feedback loops.
#[cfg(target_os = "macos")]
fn dump_thread_stacks_macos(label: &str) {
    use std::fmt::Write as _;

    let start = std::time::Instant::now();
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let path = format!("/tmp/nativelink-stall-{timestamp_ms}.txt");
    let pid = std::process::id();
    let mut output = String::new();

    let _ = writeln!(output, "=== STORE OPERATION STALL THREAD DUMP (macOS) ===");
    let _ = writeln!(output, "Trigger: {label}");
    let _ = writeln!(output, "Timestamp: {timestamp_ms}");
    let _ = writeln!(output, "PID: {pid}");
    let _ = writeln!(output);

    // Capture the calling thread's backtrace synchronously. We are the
    // dump driver, so we already have the right context — no need to
    // signal ourselves (and async-profiler-style self-signal would race
    // with the slot bookkeeping).
    let calling_bt = std::backtrace::Backtrace::force_capture();
    let _ = writeln!(output, "=== Calling thread backtrace ===");
    let _ = writeln!(output, "{calling_bt}");
    let _ = writeln!(output);

    // Per-thread enumeration + signal dispatch + formatting all happen
    // inside enumerate_mach_threads_with_backtraces. It owns the mach
    // port lifetimes, which is important: ports must remain live until
    // after pthread_kill, then deallocated.
    let (responded, total, capture_elapsed) =
        enumerate_mach_threads_with_backtraces(&mut output);

    let total_elapsed = start.elapsed();
    let _ = writeln!(
        output,
        "=== Dump complete: {responded}/{total} threads responded, capture: {capture_elapsed:.1?}, total: {total_elapsed:.1?} ===",
    );

    match write_dump_owner_only(&path, &output) {
        Ok(()) => eprintln!(
            "Thread dump written to {path} ({responded}/{total} threads, {total_elapsed:.1?})",
        ),
        Err(err) => eprintln!("Failed to write thread dump to {path}: {err}"),
    }

    cleanup_old_stall_dumps();
}

/// Enumerate all threads in the current task using Mach APIs, dispatch
/// per-thread backtrace capture via SIGUSR2, and write the formatted
/// output (per-thread name + run state + CPU + backtrace) to `output`.
///
/// Returns `(responded, total, capture_elapsed)` where:
/// - `responded` is the number of non-calling threads that produced a
///   backtrace before the 5s outer timeout.
/// - `total` is the total Mach-enumerated thread count (including the
///   calling thread).
/// - `capture_elapsed` is the wall-clock spent inside
///   [`signal_dumper_macos::capture_all_backtraces`].
///
/// Thread-port lifetime: Mach allocates a send right per thread on
/// `task_threads()`. We must hold the right until after `pthread_kill`,
/// then deallocate. We therefore deallocate at function exit, after the
/// signal round trip.
#[cfg(target_os = "macos")]
fn enumerate_mach_threads_with_backtraces(
    output: &mut String,
) -> (usize, usize, core::time::Duration) {
    use std::fmt::Write as _;

    // Mach types and constants
    type MachPort = u32;
    type KernReturn = i32;
    const KERN_SUCCESS: KernReturn = 0;
    const THREAD_BASIC_INFO: u32 = 3;
    const THREAD_BASIC_INFO_COUNT: u32 = 10; // sizeof(thread_basic_info) / sizeof(natural_t)

    // Mach thread run states
    const TH_STATE_RUNNING: i32 = 1;
    const TH_STATE_STOPPED: i32 = 2;
    const TH_STATE_WAITING: i32 = 3;
    const TH_STATE_UNINTERRUPTIBLE: i32 = 4;
    const TH_STATE_HALTED: i32 = 5;

    #[repr(C)]
    #[derive(Default)]
    struct ThreadBasicInfo {
        user_time_sec: i32,
        user_time_usec: i32,
        system_time_sec: i32,
        system_time_usec: i32,
        cpu_usage: i32, // scaled to TH_USAGE_SCALE (1000)
        policy: i32,
        run_state: i32,
        flags: i32,
        suspend_count: i32,
        sleep_time: i32,
    }

    unsafe extern "C" {
        fn mach_task_self() -> MachPort;
        fn task_threads(
            task: MachPort,
            thread_list: *mut *mut MachPort,
            thread_count: *mut u32,
        ) -> KernReturn;
        fn thread_info(
            thread: MachPort,
            flavor: u32,
            info: *mut i32,
            count: *mut u32,
        ) -> KernReturn;
        // Returns the pthread_t for the given Mach thread port, or 0 if
        // the port does not correspond to a known pthread. This is a
        // private-but-stable API exposed by Apple's pthread library and
        // documented in the open-sourced Libc / libpthread sources. It
        // is what async-profiler, sample(1), and lldb all use under the
        // hood; safe to call outside a signal handler.
        fn pthread_from_mach_thread_np(thread: MachPort) -> libc::pthread_t;
        fn mach_port_deallocate(task: MachPort, name: MachPort) -> KernReturn;
        fn vm_deallocate(task: MachPort, address: usize, size: usize) -> KernReturn;
    }

    let task = unsafe { mach_task_self() };
    let mut thread_list: *mut MachPort = core::ptr::null_mut();
    let mut thread_count: u32 = 0;

    let kr = unsafe { task_threads(task, &mut thread_list, &mut thread_count) };
    if kr != KERN_SUCCESS {
        let _ = writeln!(output, "Failed to enumerate threads: mach error {kr}");
        return (0, 0, core::time::Duration::ZERO);
    }

    let _ = writeln!(output, "Thread count: {thread_count}");
    let _ = writeln!(output);

    // SAFETY: Mach guarantees the returned thread_list is valid for
    // thread_count entries until we vm_deallocate it.
    let threads =
        unsafe { core::slice::from_raw_parts(thread_list, thread_count as usize) };

    // Pre-collect (port, pthread_t, name, info) for every enumerated
    // thread. We need pthread_t to call pthread_kill, so do this BEFORE
    // signaling. pthread_from_mach_thread_np is safe to call here (we
    // are not in a signal handler).
    struct EnumeratedThread {
        port: MachPort,
        pthread: libc::pthread_t,
        name: String,
        info: Option<ThreadBasicInfo>,
        info_kr: KernReturn,
    }

    // SAFETY: mach_thread_self() returns the calling thread's mach
    // port. We use this port number to identify which slot belongs to
    // us (so the dispatcher does not signal itself). See the
    // signal_dumper_macos handler for the deprecation rationale.
    //
    // Each mach_thread_self() invocation increments the port refcount;
    // the matching deallocate is required at function exit to avoid
    // leaking one send right per dump round (24/7 worker would
    // accumulate port-table pressure over weeks). The Drop guard runs
    // on every exit path including the early-return below from
    // task_threads failure isn't relevant (we already returned), but
    // any future early-return after this point is covered.
    #[allow(deprecated)]
    let self_port = unsafe { libc::mach_thread_self() };

    struct SelfPortGuard {
        task: MachPort,
        port: MachPort,
    }
    impl Drop for SelfPortGuard {
        fn drop(&mut self) {
            // SAFETY: self.port is a send right we obtained from
            // mach_thread_self() above. Matching deallocate is
            // required to balance the refcount.
            unsafe extern "C" {
                fn mach_port_deallocate(task: u32, name: u32) -> i32;
            }
            unsafe {
                let _ = mach_port_deallocate(self.task, self.port);
            }
        }
    }
    let _self_port_guard = SelfPortGuard { task, port: self_port };

    let mut enumerated = Vec::with_capacity(thread_count as usize);
    let mut signal_targets: Vec<(MachPort, libc::pthread_t)> =
        Vec::with_capacity(thread_count as usize);

    for &thread_port in threads {
        // SAFETY: pthread_from_mach_thread_np is safe outside signal
        // handlers; it walks the pthread library's bookkeeping.
        let pthread = unsafe { pthread_from_mach_thread_np(thread_port) };
        let mut name = String::new();
        if pthread != 0 {
            let mut name_buf = [0u8; 64];
            // SAFETY: pthread is valid (returned by libpthread API),
            // name_buf is a valid mutable pointer of length 64.
            let ret = unsafe {
                libc::pthread_getname_np(
                    pthread,
                    name_buf.as_mut_ptr().cast(),
                    name_buf.len(),
                )
            };
            if ret == 0 {
                if let Ok(c) = core::ffi::CStr::from_bytes_until_nul(&name_buf) {
                    name = c.to_string_lossy().into_owned();
                }
            }
        }

        let mut info = ThreadBasicInfo::default();
        let mut count = THREAD_BASIC_INFO_COUNT;
        // SAFETY: thread_port is a live mach port (we received it from
        // task_threads and have not deallocated). info points to a
        // properly sized stack-local struct.
        let info_kr = unsafe {
            thread_info(
                thread_port,
                THREAD_BASIC_INFO,
                core::ptr::from_mut(&mut info).cast(),
                &mut count,
            )
        };

        // Only add to signal_targets if we have a valid pthread AND the
        // thread is not the calling (collector) thread. pthread==0 means
        // the Mach thread has no associated pthread (e.g., kernel-only
        // helper) — we cannot pthread_kill it.
        if pthread != 0 && thread_port != self_port {
            signal_targets.push((thread_port, pthread));
        }

        enumerated.push(EnumeratedThread {
            port: thread_port,
            pthread,
            name,
            info: if info_kr == KERN_SUCCESS {
                Some(info)
            } else {
                None
            },
            info_kr,
        });
    }

    // Dispatch SIGUSR2 to all eligible threads, wait for handlers, and
    // resolve the captured IPs. capture_all_backtraces filters out
    // self_port a second time as a defense in depth, but we already
    // filtered above so the cost is negligible.
    let capture_start = std::time::Instant::now();
    let backtraces =
        signal_dumper_macos::capture_all_backtraces(&signal_targets, self_port);
    let capture_elapsed = capture_start.elapsed();

    let bt_map: std::collections::HashMap<MachPort, &signal_dumper_macos::ThreadBacktrace> =
        backtraces.iter().map(|bt| (bt.mach_port, bt)).collect();

    let responded = backtraces.iter().filter(|bt| bt.responded).count();

    for (idx, et) in enumerated.iter().enumerate() {
        let is_self = et.port == self_port;
        let label = if is_self { " [calling thread]" } else { "" };
        let _ = write!(
            output,
            "--- Thread {idx} (mach port {}){label}",
            et.port,
        );
        if !et.name.is_empty() {
            let _ = write!(output, "  name: {}", et.name);
        }
        let _ = writeln!(output);

        if let Some(info) = &et.info {
            let user_ms =
                i64::from(info.user_time_sec) * 1000 + i64::from(info.user_time_usec) / 1000;
            let sys_ms =
                i64::from(info.system_time_sec) * 1000 + i64::from(info.system_time_usec) / 1000;
            let state_str = match info.run_state {
                TH_STATE_RUNNING => "running",
                TH_STATE_STOPPED => "stopped",
                TH_STATE_WAITING => "waiting",
                TH_STATE_UNINTERRUPTIBLE => "uninterruptible",
                TH_STATE_HALTED => "halted",
                _ => "unknown",
            };
            let _ = writeln!(
                output,
                "  state: {state_str}  cpu_usage: {:.1}%  user: {user_ms}ms  sys: {sys_ms}ms  suspend_count: {}",
                f64::from(info.cpu_usage) / 10.0,
                info.suspend_count,
            );
        } else {
            let _ = writeln!(output, "  thread_info failed: mach error {}", et.info_kr);
        }

        // Userspace backtrace: calling thread's bt is already in the
        // file header; non-calling threads come from the cooperative
        // signal capture; threads without a pthread are unsignalable.
        if is_self {
            let _ = writeln!(output, "  userspace backtrace: see calling-thread section above");
        } else if et.pthread == 0 {
            let _ = writeln!(
                output,
                "  userspace backtrace: <skipped — no pthread for this Mach thread>"
            );
        } else if let Some(bt) = bt_map.get(&et.port) {
            if !bt.responded {
                let _ = writeln!(output, "  userspace backtrace: <no response within timeout>");
            } else if bt.symbols.is_empty() {
                let _ = writeln!(output, "  userspace backtrace: <empty>");
            } else {
                let _ = writeln!(output, "  userspace backtrace:");
                for (i, frame) in bt.symbols.iter().enumerate() {
                    let name = frame.name.as_deref().unwrap_or("<unknown>");
                    if let (Some(file), Some(line)) =
                        (frame.filename.as_ref(), frame.lineno)
                    {
                        let _ = writeln!(output, "    #{i:>3} {:#018x} {name}", frame.ip);
                        let _ = writeln!(output, "         at {file}:{line}");
                    } else {
                        let _ = writeln!(output, "    #{i:>3} {:#018x} {name}", frame.ip);
                    }
                }
            }
        } else {
            let _ = writeln!(output, "  userspace backtrace: <not dispatched>");
        }

        let _ = writeln!(output);
    }

    // Deallocate per-thread send rights AFTER capture_all_backtraces
    // returns. Premature deallocation would race with pthread_kill.
    for et in &enumerated {
        // SAFETY: Each port was returned by task_threads with a refcount
        // of 1; matching deallocate is required.
        unsafe {
            mach_port_deallocate(task, et.port);
        }
    }

    // Deallocate the thread list memory (allocated by Mach)
    if !thread_list.is_null() && thread_count > 0 {
        // SAFETY: thread_list and thread_count came from task_threads;
        // matching vm_deallocate is required.
        unsafe {
            vm_deallocate(
                task,
                thread_list as usize,
                thread_count as usize * core::mem::size_of::<MachPort>(),
            );
        }
    }

    (responded, thread_count as usize, capture_elapsed)
}

/// Write a stall dump to `path` with mode 0o600 (owner read/write only).
///
/// Default `std::fs::write` honors the process umask and typically
/// produces 0o644 (world-readable). Stall dumps include thread names,
/// in-process backtraces, and kernel state — defensive narrowing to
/// owner-only avoids leaking that to other local users on multi-tenant
/// hosts. Returns the I/O error if either open or write fails.
fn write_dump_owner_only(path: &str, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())
}

/// Maximum number of stall dump file pairs to retain. Older dumps are
/// deleted after each new dump is written.
const MAX_STALL_DUMPS: usize = 10;

/// Remove old stall dump files, keeping the newest [`MAX_STALL_DUMPS`] pairs.
/// Each dump produces two files (`-<ts>.txt` and `-<ts>-bt.txt`), so we
/// keep up to `MAX_STALL_DUMPS * 2` files total.
fn cleanup_old_stall_dumps() {
    let tmp = std::path::Path::new("/tmp");
    let entries = match std::fs::read_dir(tmp) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("stall dump cleanup: failed to read /tmp: {err}");
            return;
        }
    };

    let mut stall_files: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map_or(false, |n| n.starts_with("nativelink-stall-") && n.ends_with(".txt"))
        })
        .collect();

    // Each dump pair shares a timestamp, so sorting by filename (which
    // embeds the millisecond timestamp) gives chronological order.
    stall_files.sort();

    let max_files = MAX_STALL_DUMPS * 2;
    if stall_files.len() <= max_files {
        return;
    }

    let to_remove = stall_files.len() - max_files;
    for file in &stall_files[..to_remove] {
        if let Err(err) = std::fs::remove_file(file) {
            eprintln!("stall dump cleanup: failed to remove {}: {err}", file.display());
        }
    }
    eprintln!("stall dump cleanup: removed {to_remove} old dump files, kept {MAX_STALL_DUMPS} newest pairs");
}

#[cfg(test)]
mod tests {
    use super::{MIN_FORCE_DUMP_INTERVAL_SECS, force_dump_should_proceed};

    /// Spec: force-dump must proceed when `now - prev` is at or beyond
    /// `MIN_FORCE_DUMP_INTERVAL_SECS`. The exact boundary value MUST be
    /// allowed (>=, not >) so a dump exactly N seconds later still fires.
    #[test]
    fn force_dump_proceeds_at_or_above_threshold() {
        let prev = 1_000u64;
        // Below threshold — must NOT proceed.
        assert!(!force_dump_should_proceed(prev, prev));
        assert!(!force_dump_should_proceed(prev + 1, prev));
        assert!(!force_dump_should_proceed(
            prev + MIN_FORCE_DUMP_INTERVAL_SECS - 1,
            prev,
        ));
        // At threshold — MUST proceed.
        assert!(force_dump_should_proceed(
            prev + MIN_FORCE_DUMP_INTERVAL_SECS,
            prev,
        ));
        // Well past threshold — MUST proceed.
        assert!(force_dump_should_proceed(
            prev + MIN_FORCE_DUMP_INTERVAL_SECS + 1000,
            prev,
        ));
    }

    /// Spec: cold start (`prev == 0`) follows the same gap rule. In
    /// production `now` is unix-epoch seconds (~1.7e9), so the gap is
    /// always far above the threshold; the function does not special-
    /// case zero. This test pins the documented semantics so a future
    /// "cold-start exemption" cannot silently bypass the rate-limit.
    #[test]
    fn force_dump_cold_start_follows_gap_rule() {
        // Tiny `now` values < threshold are suppressed even when prev=0.
        assert!(!force_dump_should_proceed(1, 0));
        // At the exact threshold — proceeds (consistent with the
        // boundary semantics tested above).
        assert!(force_dump_should_proceed(MIN_FORCE_DUMP_INTERVAL_SECS, 0));
        // Real-world unix-epoch values — always proceed.
        assert!(force_dump_should_proceed(1_700_000_000, 0));
        assert!(force_dump_should_proceed(u64::MAX, 0));
    }

    /// Spec: clock-skew negative deltas (now < prev) MUST be treated as
    /// "still in cooldown" — no dump. `saturating_sub` makes the diff
    /// 0, which is below the threshold.
    #[test]
    fn force_dump_suppressed_on_clock_skew() {
        assert!(!force_dump_should_proceed(500, 1000));
        assert!(!force_dump_should_proceed(0, 1000));
    }

    // -------------------------------------------------------------
    // macOS signal_dumper_macos cooperative backtrace tests.
    //
    // These tests exercise the pthread_kill(SIGUSR2) + in-process
    // libunwind path on real OS threads. They are macOS-only because
    // they call Mach APIs and depend on Apple's pthread/Mach mapping.
    //
    // The dispatcher serializes via DUMP_IN_PROGRESS, so multiple
    // tests in this module that exercise the dumper will not interfere
    // with each other when run with `cargo test` even though tests
    // typically run in parallel — the second concurrent caller will
    // observe `dump_in_progress` and return Vec::new(). To avoid that
    // false-empty result we serialize the macOS dumper tests via a
    // dedicated mutex.
    // -------------------------------------------------------------

    /// Spec-fence: the macOS handler MUST call `libc::mach_thread_self`
    /// (a real syscall, async-signal-safe) and MUST NOT call
    /// `pthread_mach_thread_np` (which walks pthread internals and is
    /// not async-signal-safe — see async-profiler discussion #1557).
    ///
    /// We can't observe FFI calls at runtime cleanly, but we can pin
    /// the symbol identity at compile time: assigning the function
    /// pointer to a typed const is a static assertion that the symbol
    /// exists with the expected signature. Combined with the
    /// `mach_thread_self` mention in the handler comment block, any
    /// future refactor that swaps the call to `pthread_mach_thread_np`
    /// would have to also delete this assertion — making the
    /// regression visible in code review.
    ///
    /// This is a code-style fence, not a runtime contract test. The
    /// runtime contract is enforced by the OS: calling
    /// `pthread_mach_thread_np` from a signal handler can deadlock,
    /// and that would surface as flaky `single_thread_capture_*` tests
    /// on real hardware.
    #[cfg(target_os = "macos")]
    #[test]
    fn handler_uses_mach_thread_self_not_pthread_mach_thread_np() {
        // mach_thread_self is async-signal-safe.
        #[allow(deprecated)]
        let _: unsafe extern "C" fn() -> libc::mach_port_t = libc::mach_thread_self;
        // pthread_mach_thread_np exists but MUST NOT appear in the
        // handler. Asserting the symbol type here documents that we
        // are aware of it and chose not to use it.
        unsafe extern "C" {
            fn pthread_mach_thread_np(thread: libc::pthread_t) -> libc::mach_port_t;
        }
        let _: unsafe extern "C" fn(libc::pthread_t) -> libc::mach_port_t = pthread_mach_thread_np;
    }

    /// Helper: serialize macOS dumper tests so two parallel callers
    /// don't trip the in-progress guard.
    #[cfg(target_os = "macos")]
    static MACOS_DUMP_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Spec: when a single helper thread is signaled with SIGUSR2, the
    /// handler must populate that thread's slot — `responded=true`
    /// and at least one captured frame.
    ///
    /// This is the minimum-viable contract: signal delivery succeeds,
    /// the handler runs, and `backtrace::trace_unsynchronized` returns
    /// something non-empty.
    #[cfg(target_os = "macos")]
    #[test]
    fn single_thread_capture_returns_nonempty_backtrace() {
        use core::sync::atomic::AtomicBool;
        let _g = MACOS_DUMP_LOCK.lock().unwrap();

        // Channels: helper publishes its (mach_port, pthread_t) via
        // (port_tx, port_rx); main signals helper to exit via done flag.
        let (port_tx, port_rx) = std::sync::mpsc::channel::<(u32, libc::pthread_t)>();
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let done_helper = done.clone();

        let join = std::thread::spawn(move || {
            // SAFETY: mach_thread_self / pthread_self are safe in any
            // context outside a signal handler.
            #[allow(deprecated)]
            let port = unsafe { libc::mach_thread_self() };
            let pthread = unsafe { libc::pthread_self() };
            port_tx.send((port, pthread)).unwrap();
            // Spin-wait so the handler has a thread to deliver to.
            // Real `std::thread::park` would block in a syscall, which
            // SIGUSR2 will EINTR out of (with SA_RESTART set, the
            // syscall restarts) — that's fine, the handler still runs.
            while !done_helper.load(core::sync::atomic::Ordering::Acquire) {
                std::thread::park_timeout(core::time::Duration::from_millis(20));
            }
        });

        let (port, pthread) = port_rx
            .recv_timeout(core::time::Duration::from_secs(2))
            .expect("helper failed to publish its mach port");

        #[allow(deprecated)]
        let self_port = unsafe { libc::mach_thread_self() };
        let backtraces = super::signal_dumper_macos::capture_all_backtraces(
            &[(port, pthread)],
            self_port,
        );

        done.store(true, core::sync::atomic::Ordering::Release);
        join.thread().unpark();
        join.join().unwrap();

        assert_eq!(backtraces.len(), 1, "expected one backtrace per target");
        let bt = &backtraces[0];
        assert_eq!(bt.mach_port, port);
        assert!(
            bt.responded,
            "helper thread did not respond to SIGUSR2 within 5s outer deadline",
        );
        assert!(
            !bt.symbols.is_empty(),
            "captured backtrace should have at least one frame",
        );
    }

    /// Spec: a multi-threaded dump must capture every signaled thread
    /// independently. The handler's slot lookup (mach_port → index)
    /// must work concurrently across threads without cross-talk.
    #[cfg(target_os = "macos")]
    #[test]
    fn multi_thread_capture_returns_one_backtrace_per_target() {
        use core::sync::atomic::AtomicBool;
        let _g = MACOS_DUMP_LOCK.lock().unwrap();

        const N: usize = 5;
        let (port_tx, port_rx) = std::sync::mpsc::channel::<(u32, libc::pthread_t)>();
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let mut joins = Vec::with_capacity(N);

        for _ in 0..N {
            let port_tx = port_tx.clone();
            let done_helper = done.clone();
            joins.push(std::thread::spawn(move || {
                #[allow(deprecated)]
                let port = unsafe { libc::mach_thread_self() };
                let pthread = unsafe { libc::pthread_self() };
                port_tx.send((port, pthread)).unwrap();
                while !done_helper.load(core::sync::atomic::Ordering::Acquire) {
                    std::thread::park_timeout(core::time::Duration::from_millis(20));
                }
            }));
        }
        drop(port_tx); // close sender so we know we collected all N

        let mut targets = Vec::with_capacity(N);
        for _ in 0..N {
            targets.push(
                port_rx
                    .recv_timeout(core::time::Duration::from_secs(2))
                    .expect("helper failed to publish mach port"),
            );
        }

        #[allow(deprecated)]
        let self_port = unsafe { libc::mach_thread_self() };
        let backtraces =
            super::signal_dumper_macos::capture_all_backtraces(&targets, self_port);

        done.store(true, core::sync::atomic::Ordering::Release);
        for j in &joins {
            j.thread().unpark();
        }
        for j in joins {
            j.join().unwrap();
        }

        assert_eq!(backtraces.len(), N, "one backtrace per signaled thread");
        let responded = backtraces.iter().filter(|b| b.responded).count();
        let with_frames = backtraces.iter().filter(|b| !b.symbols.is_empty()).count();
        assert_eq!(responded, N, "all helper threads should respond");
        assert_eq!(with_frames, N, "all responses should have at least one frame");

        // Sanity: mach ports across results are distinct.
        let mut ports: Vec<u32> = backtraces.iter().map(|b| b.mach_port).collect();
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(ports.len(), N, "mach ports should be distinct across threads");
    }

    /// Spec: the 5s outer deadline MUST fire if a thread cannot
    /// service SIGUSR2 (e.g., it blocked the signal via
    /// `pthread_sigmask`). The dump returns `responded=false` for
    /// that slot and the wall-clock stays bounded — it does NOT hang
    /// the watchdog forever.
    ///
    /// We deliberately mask SIGUSR2 on a helper thread so the kernel
    /// queues the signal but never delivers it. The collector should
    /// trip its 5s deadline (we allow up to 6s of wall-clock to
    /// account for scheduling jitter on busy CI hardware).
    #[cfg(target_os = "macos")]
    #[test]
    fn uncooperative_thread_times_out_with_no_response() {
        use core::sync::atomic::AtomicBool;
        let _g = MACOS_DUMP_LOCK.lock().unwrap();

        let (port_tx, port_rx) = std::sync::mpsc::channel::<(u32, libc::pthread_t)>();
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let done_helper = done.clone();

        let join = std::thread::spawn(move || {
            // Block SIGUSR2 on this thread before publishing its port.
            // The kernel will mark the signal as pending but never
            // invoke the handler until we unblock — and we never do
            // before the dump deadline fires.
            unsafe {
                let mut set: libc::sigset_t = core::mem::zeroed();
                libc::sigemptyset(&mut set);
                libc::sigaddset(&mut set, libc::SIGUSR2);
                let ret = libc::pthread_sigmask(
                    libc::SIG_BLOCK,
                    &set,
                    core::ptr::null_mut(),
                );
                assert_eq!(ret, 0, "pthread_sigmask SIG_BLOCK failed");
            }
            #[allow(deprecated)]
            let port = unsafe { libc::mach_thread_self() };
            let pthread = unsafe { libc::pthread_self() };
            port_tx.send((port, pthread)).unwrap();
            while !done_helper.load(core::sync::atomic::Ordering::Acquire) {
                std::thread::park_timeout(core::time::Duration::from_millis(20));
            }
        });

        let (port, pthread) = port_rx
            .recv_timeout(core::time::Duration::from_secs(2))
            .expect("helper failed to publish mach port");

        #[allow(deprecated)]
        let self_port = unsafe { libc::mach_thread_self() };
        let start = std::time::Instant::now();
        let backtraces = super::signal_dumper_macos::capture_all_backtraces(
            &[(port, pthread)],
            self_port,
        );
        let elapsed = start.elapsed();

        done.store(true, core::sync::atomic::Ordering::Release);
        join.thread().unpark();
        join.join().unwrap();

        // Wall-clock must be bounded: the outer deadline is 5s; we
        // allow up to 6.5s of slack for slow CI / debug-build overhead.
        assert!(
            elapsed < core::time::Duration::from_millis(6500),
            "dump should respect 5s outer deadline; took {elapsed:?}",
        );
        // The dump still returns one entry per target, but the
        // uncooperative thread's slot must show responded=false and
        // an empty symbols vec.
        assert_eq!(backtraces.len(), 1);
        assert_eq!(backtraces[0].mach_port, port);
        assert!(
            !backtraces[0].responded,
            "thread that masked SIGUSR2 must not appear as responded",
        );
        assert!(
            backtraces[0].symbols.is_empty(),
            "no-response slot must have no frames",
        );
    }

    // -------------------------------------------------------------
    // Eager handler-install + external-trigger tests.
    //
    // Spec under test (from the worker-03 a367ed2d3e3f1610c field
    // test, which fired `kill -USR2 $pid` and saw the worker process
    // exit with the kernel default disposition for SIGUSR2):
    //
    //   1. `install_dump_signal_handler()` is idempotent. Calling it
    //      from any context (multiple threads, multiple times) leaves
    //      SIGUSR2 mapped to our slot-based capture, NOT SIG_DFL.
    //
    //   2. After the eager install, raising SIGUSR2 with no internal
    //      dump round in flight does NOT terminate the process — the
    //      handler runs (returns silently because no slot matches)
    //      and execution continues.
    //
    // These tests are macOS-only because the eager install on macOS
    // is the SIGUSR2 sigaction, and a SIG_DFL SIGUSR2 on macOS kills
    // the process. The Linux equivalent uses SIGRTMIN+1 with a
    // similar contract. We exercise the macOS install symmetrically
    // since that's where the field test failed.
    //
    // We deliberately don't kill(getpid(), SIGUSR2) and watch for
    // process termination — a passing test would mean the bug was
    // fixed, but a failing test (handler not installed) would crash
    // the whole `cargo test` runner. Instead we assert the SIGUSR2
    // disposition by reading it back via `sigaction(SIGUSR2, NULL,
    // &old)` and checking `old.sa_sigaction != SIG_DFL`. That fence
    // catches the regression without risking the test runner.
    // -------------------------------------------------------------

    /// Spec: `install_dump_signal_handler()` MUST replace the kernel
    /// default disposition for the dump signal with our handler.
    /// Calling it twice is a no-op and never reverts the disposition.
    #[cfg(target_os = "macos")]
    #[test]
    fn install_dump_signal_handler_replaces_sigusr2_default() {
        let _g = MACOS_DUMP_LOCK.lock().unwrap();

        // Read the current SIGUSR2 disposition. We may already be
        // installed (any prior macos test triggered lazy install via
        // capture_all_backtraces) — that's fine, the contract is
        // "after install it is NOT SIG_DFL", not "install changes it
        // from SIG_DFL".
        super::install_dump_signal_handler();

        let mut after: libc::sigaction = unsafe { core::mem::zeroed() };
        let ret = unsafe {
            libc::sigaction(libc::SIGUSR2, core::ptr::null(), &mut after)
        };
        assert_eq!(ret, 0, "sigaction(SIGUSR2, NULL, &out) must succeed");

        // SIG_DFL means the kernel default (terminate). The field-test
        // bug: if our install never ran, an external `kill -USR2 $pid`
        // hits SIG_DFL and kills the worker.
        assert_ne!(
            after.sa_sigaction, libc::SIG_DFL,
            "SIGUSR2 disposition is SIG_DFL after install_dump_signal_handler — \
             external `kill -USR2 $pid` would terminate the worker (field-test \
             a367ed2d3e3f1610c regression)",
        );
        // SIG_IGN means the signal is silently dropped — the process
        // would survive but external triggers would never produce a
        // dump (no handler runs).
        assert_ne!(
            after.sa_sigaction, libc::SIG_IGN,
            "SIGUSR2 disposition is SIG_IGN after install_dump_signal_handler — \
             external triggers would never produce a dump",
        );
        // SA_SIGINFO must be set; our handler is the 3-arg variant.
        assert!(
            (after.sa_flags & libc::SA_SIGINFO) != 0,
            "SIGUSR2 sigaction missing SA_SIGINFO flag; not our handler",
        );
    }

    /// Spec: install_dump_signal_handler is idempotent. Two calls (in
    /// any order, from any thread) leave the disposition unchanged
    /// from the first install. This is the production-safety
    /// invariant — code reachable from initialization, runtime
    /// startup, and lazy capture_all_backtraces all call into the
    /// same Once-guarded install path.
    #[cfg(target_os = "macos")]
    #[test]
    fn install_dump_signal_handler_is_idempotent() {
        let _g = MACOS_DUMP_LOCK.lock().unwrap();

        super::install_dump_signal_handler();
        let mut first: libc::sigaction = unsafe { core::mem::zeroed() };
        let ret = unsafe {
            libc::sigaction(libc::SIGUSR2, core::ptr::null(), &mut first)
        };
        assert_eq!(ret, 0);

        // Second call MUST be a no-op (Once guard).
        super::install_dump_signal_handler();
        let mut second: libc::sigaction = unsafe { core::mem::zeroed() };
        let ret = unsafe {
            libc::sigaction(libc::SIGUSR2, core::ptr::null(), &mut second)
        };
        assert_eq!(ret, 0);
        assert_eq!(
            first.sa_sigaction, second.sa_sigaction,
            "second install changed the SIGUSR2 disposition — Once guard broken",
        );
        assert_eq!(
            first.sa_flags, second.sa_flags,
            "second install changed the SIGUSR2 sa_flags — Once guard broken",
        );

        // Third call from a worker thread must also be safe.
        let join = std::thread::spawn(|| {
            super::install_dump_signal_handler();
        });
        join.join().unwrap();
        let mut third: libc::sigaction = unsafe { core::mem::zeroed() };
        let ret = unsafe {
            libc::sigaction(libc::SIGUSR2, core::ptr::null(), &mut third)
        };
        assert_eq!(ret, 0);
        assert_eq!(
            first.sa_sigaction, third.sa_sigaction,
            "cross-thread install changed disposition — Once not Sync-safe",
        );
    }

    /// Spec: after install, raising SIGUSR2 to the current process
    /// (with no internal dump round in flight) MUST be safely
    /// absorbed by the handler. The process must survive — no exit,
    /// no abort, no crash.
    ///
    /// We assert survival by checking that subsequent code runs
    /// (`continued.store(true)` after the raise) — if the handler
    /// were SIG_DFL the process would terminate before the assertion
    /// could fire. If the handler were SIG_IGN the process would
    /// survive but the assertion would still pass (acceptable
    /// fallback per the install_dump_signal_handler contract).
    ///
    /// We use `libc::raise` (not `libc::kill(getpid(), ...)`) because
    /// `raise` is documented as delivering to the calling thread,
    /// which means we get deterministic delivery (vs. `kill` which
    /// delivers to "any thread that doesn't have the signal masked"
    /// — likely some helper thread, which could race the assertion).
    /// A serial deterministic path is what we want for a survival
    /// test.
    #[cfg(target_os = "macos")]
    #[test]
    fn external_sigusr2_does_not_terminate_process() {
        use core::sync::atomic::{AtomicBool, Ordering};
        let _g = MACOS_DUMP_LOCK.lock().unwrap();

        super::install_dump_signal_handler();

        // Sanity: no internal dump in flight.
        assert!(
            !super::signal_dumper_macos::dump_in_progress(),
            "test precondition: no internal dump round should be active",
        );

        // Marker: if the handler kills the process, this stays false
        // and the test runner reports the failure as the cargo-test
        // process exiting with SIGUSR2 (signal 31) — which is the
        // exact field-test symptom.
        let continued = AtomicBool::new(false);

        // SAFETY: `raise` is a libc-defined async-signal-safe wrapper
        // that delivers `sig` to the calling thread. Returns 0 on
        // success, non-zero on failure.
        let ret = unsafe { libc::raise(libc::SIGUSR2) };
        assert_eq!(ret, 0, "libc::raise(SIGUSR2) must succeed");

        // If we reach here, the process survived the signal. Mark it
        // and assert — the assert is informational; the real signal
        // is reaching this line.
        continued.store(true, Ordering::SeqCst);
        assert!(
            continued.load(Ordering::SeqCst),
            "process survived SIGUSR2 — eager install fix is in effect",
        );
    }

    /// Spec (Linux mirror): the eager install on Linux must replace
    /// the SIGRTMIN+1 default disposition. Same contract shape as
    /// the macOS test, on the corresponding signal — SIGRTMIN+1's
    /// default disposition is also process termination, so the same
    /// "kill the worker" failure mode applies if the install path
    /// regresses on Linux.
    ///
    /// Running this test on Linux gives us a real mutation-testable
    /// fence: comment out the `install_dump_signal_handler` body and
    /// the assertion fires immediately.
    #[cfg(target_os = "linux")]
    #[test]
    fn install_dump_signal_handler_replaces_sigrtmin_default_linux() {
        super::install_dump_signal_handler();

        let sig = libc::SIGRTMIN() + 1;
        let mut after: libc::sigaction = unsafe { core::mem::zeroed() };
        let ret = unsafe {
            libc::sigaction(sig, core::ptr::null(), &mut after)
        };
        assert_eq!(ret, 0, "sigaction(SIGRTMIN+1, NULL, &out) must succeed");
        assert_ne!(
            after.sa_sigaction, libc::SIG_DFL,
            "SIGRTMIN+1 disposition is SIG_DFL after \
             install_dump_signal_handler — internal pthread_kill rounds \
             would terminate the worker on Linux (mirror of worker-03 \
             field-test regression)",
        );
        assert_ne!(
            after.sa_sigaction, libc::SIG_IGN,
            "SIGRTMIN+1 disposition is SIG_IGN — handler never called",
        );
        assert!(
            (after.sa_flags & libc::SA_SIGINFO) != 0,
            "SIGRTMIN+1 sigaction missing SA_SIGINFO flag; not our handler",
        );
    }
}
