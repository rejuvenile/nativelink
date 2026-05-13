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

use core::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use nativelink_macro::nativelink_test;
use nativelink_util::cpu_pool::cpu_pool;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

/// Verify that 100 SHA-256 jobs submitted to the dedicated cpu_pool all
/// complete with the correct hash. Exercises:
/// 1. Pool initializes on first call.
/// 2. Pool can serve concurrent submissions (FuturesUnordered drives 100).
/// 3. Each `tx.send` from a rayon worker reaches the awaiting receiver.
/// 4. Computed hashes match `sha2::Sha256::digest` over the same input,
///    so we know the pool's worker actually ran the closure (not a stub).
///
/// Bounded under `tokio::time::timeout(30s)` so a future deadlock in
/// `cpu_pool` initialization or rayon dispatch surfaces as a fast test
/// failure rather than a session-time hang.
#[nativelink_test]
async fn cpu_pool_runs_100_sha_jobs_with_correct_hashes() {
    let mut futs = FuturesUnordered::new();
    for i in 0u8..100 {
        // Per-job payload: 1 KiB filled with the index byte. Cheap enough
        // for a fast unit test; large enough that an empty / no-op
        // closure would not produce the expected hash.
        let bytes = vec![i; 1024];
        let expected: [u8; 32] = Sha256::digest(&bytes).into();
        let (tx, rx) = oneshot::channel::<[u8; 32]>();
        cpu_pool().spawn(move || {
            let computed: [u8; 32] = Sha256::digest(&bytes).into();
            // tx.send returns Err only if the receiver was dropped,
            // which the awaiting test below cannot do until rx.await
            // either resolves or is cancelled by the timeout.
            let _ = tx.send(computed);
        });
        futs.push(async move {
            let computed = rx.await.expect("cpu_pool worker dropped tx");
            (i, expected, computed)
        });
    }

    let mut completed = 0_usize;
    let timeout = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some((i, expected, computed)) = futs.next().await {
            assert_eq!(
                expected, computed,
                "job {i}: cpu_pool returned wrong SHA-256",
            );
            completed += 1;
        }
    });
    timeout
        .await
        .expect("cpu_pool: 100 SHA jobs did not all complete within 30s — \
                 deadlock-detector tripped");
    assert_eq!(
        completed, 100,
        "cpu_pool: expected 100 completions, got {completed}",
    );
}
