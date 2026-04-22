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

//! Regression tests for the panic that crashed all 10 Mac Mini workers on
//! 2026-04-19/20: blake3's `update_mmap_rayon` runs on a rayon worker thread
//! that has no tokio runtime context. If anything inside the closure (or any
//! Drop running on the rayon thread) touches a tokio API, tokio panics with
//! "there is no reactor running, must be called from the context of a Tokio
//! 1.x runtime", and rayon's panic handler aborts the process.
//!
//! The fix is to capture `Handle::current()` *before* `rayon::spawn` and
//! `enter()` it inside the closure so the rayon worker thread has a tokio
//! runtime context for the lifetime of the work.

use std::io::Write;

use nativelink_macro::nativelink_test;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::fs;

/// Hash a file larger than `DEFAULT_READ_BUFF_SIZE` (64 KiB) with Blake3.
/// This forces the `update_mmap_rayon` rayon-pool path inside
/// `digest_for_file`, which is the production code path that panicked on
/// the workers. Before the fix, this could panic intermittently if any
/// captured value's Drop or any blake3-internal call reached a tokio API.
/// After the fix, the rayon worker enters the tokio runtime so any such
/// call finds a context and does not panic.
#[nativelink_test]
async fn blake3_digest_for_large_file_does_not_panic_in_rayon() {
    // 1 MiB > 64 KiB (DEFAULT_READ_BUFF_SIZE) so we hit the rayon mmap path,
    // not the small-file fallback.
    const SIZE: usize = 1024 * 1024;
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
    let payload = vec![0xABu8; SIZE];
    tmp.write_all(&payload).expect("write payload");
    tmp.flush().expect("flush");
    let path = tmp.path().to_path_buf();

    let file = fs::open_file(&path, 0).await.expect("open file");
    let hasher = DigestHasherFunc::Blake3.hasher();
    let (digest, _file) = hasher
        .digest_for_file(&path, file, Some(SIZE as u64))
        .await
        .expect("digest_for_file should succeed without panicking");

    assert_eq!(digest.size_bytes(), SIZE as u64);
}

/// Exercise the `rayon::spawn` + `Handle::current().enter()` *pattern* in
/// isolation. If a future change accidentally removes the `enter()` call
/// in `digest_hasher.rs`, this test would still pass on its own — but it
/// guards the underlying technique so reviewers can see the contract.
///
/// The closure deliberately invokes a tokio API (`tokio::runtime::Handle::current()`)
/// to prove that, with `enter()`, a rayon worker thread can reach the tokio
/// runtime. Without `enter()`, this would panic and abort.
#[nativelink_test]
async fn rayon_worker_with_handle_enter_can_call_tokio_api() {
    let runtime_handle = tokio::runtime::Handle::current();
    let (tx, rx) = tokio::sync::oneshot::channel::<bool>();
    rayon::spawn(move || {
        let _guard = runtime_handle.enter();
        // This call panics with "there is no reactor running" if no tokio
        // runtime is entered on the current thread. With the guard above,
        // it succeeds.
        let _h = tokio::runtime::Handle::current();
        let _ = tx.send(true);
    });
    let ok = rx.await.expect("rayon task delivered result");
    assert!(ok);
}
