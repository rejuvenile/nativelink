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

//! (#dag-criticality) DAG-from-history critical-path prioritization.
//!
//! Reconstructs a target-level build DAG from persisted history — stable-key edges
//! inferred from the input↔output blob-digest linkage the server ALREADY resolves for
//! P2P locality — derives a longest-path criticality score per node in the background,
//! and publishes a quantized band per node so the scheduler can use it as a
//! WITHIN-priority-band tie-break in the already-priority-sorted pending set. Advisory,
//! correctness-neutral, flag-gated. See
//! `.claude/audits/scheduler-critical-path-dag-history-design-2026-07-16-v2.md`.
//!
//! # Node identity (v2 fix 8)
//!
//! The node key is [`ProfileKey`] EXACTLY — `(instance_name, target_id, action_mnemonic)`
//! — no `config_id`. This maximizes cross-build recurrence (the operator's stated
//! priority), aligns with the maturing resource-profile key, and respects the fence (the
//! profile deliberately omits config). Host-vs-target (opt/dbg) builds of the same target
//! merge into one node — an accepted approximation.
//!
//! # Correctness posture (v2 fix 1)
//!
//! The published snapshot contains a band ONLY for a CONFIDENT node: present in the graph
//! built from edges with `>= DAG_MIN_EDGE_OBS` cross-build observations AND in a
//! singleton SCC (not a spurious cycle from the target-level collapse). A non-confident /
//! absent node reads band `0`, so the tie-break degrades to TODAY'S FIFO
//! (`inverted_insert_ts`) — never "worse than FIFO". A stale score is never trusted.

use std::collections::HashMap;
use std::sync::Arc;

use lru::LruCache;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_util::common::DigestInfo;
use parking_lot::{Mutex, RwLock};
use wincode::{SchemaRead, SchemaWrite};

use crate::resource_profile::ProfileKey;

/// The DAG node identity — [`ProfileKey`] exactly `(instance, target, mnemonic)` (v2
/// fix 8). Reusing `ProfileKey` (rather than a wider sibling key) maximizes recurrence,
/// reuses its constructors' clamping/skip-on-empty-baggage discipline (security MED-1 —
/// all key strings clamped via `ProfileKey::from_parts`), and keeps ONE key family.
pub type DagNodeKey = ProfileKey;

/// (#dag-criticality) Maximum distinct stable-key edges in the bounded [`EdgeStore`] LRU.
///
/// A large Bazel repo has ~tens of thousands of distinct target-level edges; the LRU
/// bounds the map to the recency window of actively-observed edges. Over-cap evicts the
/// least-recently-observed edge (counted), so stale edges from deleted targets age out —
/// the desired recency semantics.
pub const EDGE_STORE_MAX: usize = 65536;

/// (#dag-criticality) Maximum distinct per-node duration histograms in the bounded
/// duration LRU. Sized to match [`crate::resource_profile::PROFILE_MAP_MAX_KEYS`] (the
/// node universe is the same `(instance,target,mnemonic)` family).
pub const NODE_DURATION_MAX: usize = 16384;

/// (#dag-criticality) Maximum entries in the transient output-blob-digest → producer
/// node map. Sized like the existing `output_file_producer_map` (a build produces far
/// more distinct output file blobs than distinct targets); over-cap LRU-evicts the
/// oldest output blob = the recency window.
pub const PRODUCER_MAP_MAX: usize = 65536;

/// (#dag-criticality, v2 fix 1) Minimum cross-build observations before an edge is
/// trusted enough to shape criticality. An edge observed in `>= DAG_MIN_EDGE_OBS`
/// distinct dispatched builds is "mature"; below it, the edge is ignored by the
/// criticality DP so an immature/one-off linkage never elevates a node above FIFO. The
/// DAG matures far faster than the K=20 resource-profile (an edge needs ~2 observations,
/// not 20 samples) — but only when the endpoint nodes recur at all.
pub const DAG_MIN_EDGE_OBS: u32 = 2;

/// (#dag-criticality, v2 fix 7) Fan-in exclusion threshold F. A non-empty blob that is
/// both an output AND the input of MORE than this many distinct consumers (a generated
/// common header, a toolchain manifest) creates degenerate near-universal fan-out that
/// would dominate every longest-path. Once a producer blob's observed consumer count
/// exceeds F it is treated as ubiquitous and produces NO further edges. The `size > 0`
/// filter alone is insufficient (§11.6).
pub const DAG_UBIQUITOUS_FANIN_MAX: u32 = 256;

/// (#dag-criticality) Milliseconds of longest-path criticality per quantization band.
/// The raw longest-path value (summed wall durations along the deepest downstream chain,
/// in ms) is divided by this and clamped into `[0, 255]` (8 bits, [`DAG_CRITICALITY_BITS`]).
/// A sink (unblocks nothing → criticality 0) lands in band 0, identical to an
/// absent/non-confident node — both fall back to FIFO. Monotone + deterministic; the
/// exact step is a tuning detail (correctness needs only monotonicity + sink→0).
pub const DAG_CRITICALITY_QUANT_MS: u64 = 1000;

/// (#dag-criticality, v2 fix 4) Bits of quantized criticality folded into the SECONDARY
/// region of `AwaitedActionSortKey`, between the 32-bit client priority (untouched) and
/// the (truncated) insert timestamp. `8` → 256 bands. The band is stored plain (higher =
/// sorts first within a priority band); it can NEVER cross a priority band (it sits below
/// bit 32). See `awaited_action.rs`.
pub const DAG_CRITICALITY_BITS: u32 = 8;

/// Maximum criticality band value (`2^DAG_CRITICALITY_BITS - 1`).
pub const DAG_MAX_BAND: u8 = 255;

// ── compact duration histogram ─────────────────────────────────────────────

/// Number of log2 buckets in the compact duration sketch (mirrors
/// `resource_profile::HIST_BUCKETS`). `64 * 4 = 256` bytes/node — never grows with the
/// number of samples folded.
const DUR_HIST_BUCKETS: usize = 64;

/// Map a value to its log2 histogram bucket (saturating at the top). Bucket `0` holds
/// `0`; bucket `i` (`1 <= i <= 62`) holds `[2^(i-1), 2^i)`; bucket `63` saturates.
/// (Duplicated from `resource_profile` deliberately so this module stays self-contained
/// — v2 fix 11 — and its persistence schema is decoupled.)
#[inline]
const fn dur_bucket_index(v: u64) -> usize {
    if v == 0 {
        0
    } else {
        let idx = (u64::BITS - v.leading_zeros()) as usize;
        if idx >= DUR_HIST_BUCKETS {
            DUR_HIST_BUCKETS - 1
        } else {
            idx
        }
    }
}

/// Representative value for a bucket: the geometric-ish midpoint `1.5 * 2^(i-1)`.
#[inline]
const fn dur_bucket_representative(i: usize) -> u64 {
    if i == 0 {
        0
    } else {
        let lower = 1u64 << (i - 1);
        lower + (lower >> 1)
    }
}

/// A fixed-size, log2-bucketed duration histogram + sample count. The vertex weight the
/// longest-path DP uses is its p50 representative — deterministic and cheap.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DurHist {
    buckets: [u32; DUR_HIST_BUCKETS],
    count: u64,
}

impl Default for DurHist {
    fn default() -> Self {
        Self {
            buckets: [0; DUR_HIST_BUCKETS],
            count: 0,
        }
    }
}

impl DurHist {
    #[inline]
    fn record(&mut self, ms: u64) {
        let idx = dur_bucket_index(ms);
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
        self.count = self.count.saturating_add(1);
    }

    /// The p50 representative (median duration bucket). `0` for an empty sketch.
    fn p50_ms(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        // 1-indexed rank of the median = ceil(count/2), clamped into [1, count].
        let rank = (self.count.div_ceil(2)).clamp(1, self.count);
        let mut cum: u64 = 0;
        for (i, &c) in self.buckets.iter().enumerate() {
            cum += u64::from(c);
            if cum >= rank {
                return dur_bucket_representative(i);
            }
        }
        dur_bucket_representative(DUR_HIST_BUCKETS - 1)
    }

    fn buckets_vec(&self) -> Vec<u32> {
        self.buckets.to_vec()
    }

    fn from_buckets_vec(v: &[u32], count: u64) -> Option<Self> {
        if v.len() != DUR_HIST_BUCKETS {
            return None;
        }
        let total: u64 = v.iter().map(|&c| u64::from(c)).sum();
        // saturating fold means the bucket total can be <= count, never >.
        if count == 0 || total > count {
            return None;
        }
        let mut buckets = [0u32; DUR_HIST_BUCKETS];
        buckets.copy_from_slice(v);
        Some(Self { buckets, count })
    }
}

// ── edge store ─────────────────────────────────────────────────────────────

/// A stable-key edge: producer node → consumer node (the consumer's input tree read the
/// producer's output blob). Content-addressed inference makes it exact when present.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DagEdge {
    pub producer: DagNodeKey,
    pub consumer: DagNodeKey,
}

/// Per-edge observation statistic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EdgeStat {
    /// Times this stable-key edge was observed (once per dispatched consumer build).
    count: u32,
    /// Most-recent build epoch that observed it (recency; reserved for future aging).
    last_seen_build_epoch: u32,
}

/// The bounded, self-contained edge store + per-node duration histograms (v2 fix 11).
/// Guarded by a strict-LEAF `parking_lot::Mutex` — never held across `.await`.
#[derive(Debug)]
pub struct EdgeStore {
    // CAPPED AT EDGE_STORE_MAX (65536): bounded LRU of stable-key edges; over-cap evicts
    // the LRU edge, counted via `evictions`. Worst case ≈ 8 clamped strings ×
    // PROFILE_KEY_MAX_STR_LEN(256) × 65536 ≈ ~144 MiB (typical ~32 MiB); no owned blob
    // bytes. (v2 fix 9 — honest worst-case bound.)
    edges: LruCache<DagEdge, EdgeStat>,
    // CAPPED AT NODE_DURATION_MAX (16384): bounded LRU of per-node duration sketches
    // (its OWN cap — the key differs from `ProfileMap`, v2 fix 5); 256 B/node fixed, no
    // per-sample growth. Over-cap evicts the LRU node.
    durations: LruCache<DagNodeKey, DurHist>,
    build_epoch: u32,
    evictions: u64,
}

impl EdgeStore {
    fn new() -> Self {
        Self {
            edges: LruCache::new(
                std::num::NonZeroUsize::new(EDGE_STORE_MAX).expect("EDGE_STORE_MAX nonzero"),
            ),
            durations: LruCache::new(
                std::num::NonZeroUsize::new(NODE_DURATION_MAX).expect("NODE_DURATION_MAX nonzero"),
            ),
            build_epoch: 0,
            evictions: 0,
        }
    }

    /// Observe one stable-key edge (producer → consumer). Bumps its count + recency, or
    /// inserts it fresh; over-cap eviction is counted. Called ONCE per dispatched
    /// consumer build so the count is a cross-build observation tally.
    fn observe_edge(&mut self, edge: DagEdge) {
        let epoch = self.build_epoch;
        if let Some(stat) = self.edges.get_mut(&edge) {
            stat.count = stat.count.saturating_add(1);
            stat.last_seen_build_epoch = epoch;
            return;
        }
        let stat = EdgeStat {
            count: 1,
            last_seen_build_epoch: epoch,
        };
        // `push` on a known-absent key returns the evicted (key,val) iff at capacity.
        if self.edges.push(edge, stat).is_some() {
            self.evictions = self.evictions.saturating_add(1);
        }
    }

    /// Fold one completed action's wall duration (ms) into its node's histogram.
    fn record_duration(&mut self, node: DagNodeKey, dur_ms: u64) {
        if let Some(h) = self.durations.get_mut(&node) {
            h.record(dur_ms);
            return;
        }
        let mut h = DurHist::default();
        h.record(dur_ms);
        drop(self.durations.push(node, h));
    }

    /// Node weights (p50 duration ms) for every node with a recorded histogram.
    fn node_weights(&self) -> HashMap<DagNodeKey, u32> {
        self.durations
            .iter()
            .map(|(k, h)| {
                // Clamp to u32 — durations far above ~49 days are not meaningful weights.
                let w = u32::try_from(h.p50_ms()).unwrap_or(u32::MAX);
                (k.clone(), w)
            })
            .collect()
    }

    /// Owned `(producer, consumer, count)` triples for the criticality DP.
    fn edges_for_compute(&self) -> Vec<(DagNodeKey, DagNodeKey, u32)> {
        self.edges
            .iter()
            .map(|(e, s)| (e.producer.clone(), e.consumer.clone(), s.count))
            .collect()
    }

    #[inline]
    fn edge_len(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    fn duration_len(&self) -> usize {
        self.durations.len()
    }
}

// ── transient producer map ──────────────────────────────────────────────────

/// One recently-produced output blob's producer node + a running count of the DISTINCT
/// consumers that have referenced it as input (the ubiquitous-blob fan-in tracker, v2
/// fix 7). Carries the producer's `instance_name` (inside the [`DagNodeKey`]) so the
/// consumer side can enforce the same-instance edge filter (v2 fix 6).
#[derive(Clone, Debug)]
struct ProducerEntry {
    node: DagNodeKey,
    consumer_count: u32,
}

// ── published criticality snapshot ──────────────────────────────────────────

/// The O(1)-readable published criticality snapshot. Holds a band ONLY for CONFIDENT
/// nodes (present + mature edges + singleton SCC); an absent node reads band `0` (FIFO
/// fallback). Immutable once published; swapped atomically via the `RwLock<Arc<..>>`.
#[derive(Clone, Debug, Default)]
pub struct CriticalitySnapshot {
    bands: HashMap<DagNodeKey, u8>,
}

impl CriticalitySnapshot {
    /// The confidence-gated criticality band for a node, or `0` when absent / not
    /// confident (v2 fix 1 — degrade to today's FIFO, never worse).
    #[inline]
    pub fn band(&self, node: &DagNodeKey) -> u8 {
        self.bands.get(node).copied().unwrap_or(0)
    }

    /// Number of confident nodes (for telemetry / the kill-keep ratio).
    #[inline]
    pub fn confident_nodes(&self) -> usize {
        self.bands.len()
    }
}

// ── the pure criticality computation (Tarjan SCC + reverse-topo longest path) ─

/// Quantize a raw longest-path criticality value (ms) into a band `[0, DAG_MAX_BAND]`.
/// Monotone non-decreasing; a sink (crit `0`) → band `0`.
#[inline]
fn quantize_criticality(crit_ms: u64) -> u8 {
    let band = crit_ms / DAG_CRITICALITY_QUANT_MS;
    u8::try_from(band.min(u64::from(DAG_MAX_BAND))).unwrap_or(DAG_MAX_BAND)
}

/// Compute the confidence-gated criticality bands from a raw edge multiset + node
/// weights (v1 §5, v2 fix 11).
///
/// 1. Keep only edges observed `>= min_obs` times (mature / cross-build).
/// 2. Build the producer→consumer graph over the surviving edges' endpoints.
/// 3. Tarjan SCC (iterative — safe for deep graphs) → components in reverse-topo order.
/// 4. Condense: super-node weight = **MAX** of member weights (v2 fix 11 — faithful for
///    the expected spurious-cycle case, NOT sum).
/// 5. Reverse-topo longest-path DP: `crit(scc) = weight(scc) + max(crit(successor))`.
/// 6. Publish a band ONLY for nodes in a SINGLETON SCC (multi-member SCCs = spurious
///    cycles from the target-level collapse → NOT confident → excluded → FIFO fallback).
pub fn compute_criticality(
    edges: &[(DagNodeKey, DagNodeKey, u32)],
    node_weights: &HashMap<DagNodeKey, u32>,
    min_obs: u32,
) -> CriticalitySnapshot {
    // Index every node that appears on a MATURE edge.
    let mut index: HashMap<DagNodeKey, usize> = HashMap::new();
    let mut nodes: Vec<DagNodeKey> = Vec::new();
    let mut adj: Vec<Vec<usize>> = Vec::new();
    let intern = |k: &DagNodeKey,
                      index: &mut HashMap<DagNodeKey, usize>,
                      nodes: &mut Vec<DagNodeKey>,
                      adj: &mut Vec<Vec<usize>>|
     -> usize {
        if let Some(&i) = index.get(k) {
            i
        } else {
            let i = nodes.len();
            index.insert(k.clone(), i);
            nodes.push(k.clone());
            adj.push(Vec::new());
            i
        }
    };
    for (p, c, count) in edges {
        if *count < min_obs {
            continue;
        }
        let pi = intern(p, &mut index, &mut nodes, &mut adj);
        let ci = intern(c, &mut index, &mut nodes, &mut adj);
        if pi != ci {
            adj[pi].push(ci);
        }
    }
    let n = nodes.len();
    if n == 0 {
        return CriticalitySnapshot::default();
    }

    // ── iterative Tarjan SCC ──
    // comp[v] = SCC id (assigned in reverse-topological order: a component is finalized
    // AFTER every component reachable from it, so successors get SMALLER ids and are
    // computed first in the crit DP below).
    let mut idx_of = vec![usize::MAX; n];
    let mut low = vec![0usize; n];
    let mut on_stack = vec![false; n];
    let mut comp = vec![usize::MAX; n];
    let mut scc_members: Vec<usize> = Vec::new(); // members-count per SCC id
    let mut tarjan_stack: Vec<usize> = Vec::new();
    let mut next_index = 0usize;
    let mut scc_count = 0usize;

    // Explicit DFS work stack: (node, next-adjacency-cursor).
    for start in 0..n {
        if idx_of[start] != usize::MAX {
            continue;
        }
        let mut work: Vec<(usize, usize)> = vec![(start, 0)];
        while let Some(&(v, ci)) = work.last() {
            if ci == 0 {
                idx_of[v] = next_index;
                low[v] = next_index;
                next_index += 1;
                tarjan_stack.push(v);
                on_stack[v] = true;
            }
            if ci < adj[v].len() {
                let w = adj[v][ci];
                // advance v's cursor before descending / relaxing
                work.last_mut().unwrap().1 += 1;
                if idx_of[w] == usize::MAX {
                    work.push((w, 0));
                } else if on_stack[w] {
                    low[v] = low[v].min(idx_of[w]);
                }
            } else {
                // done with v: if it's an SCC root, pop the component.
                if low[v] == idx_of[v] {
                    let mut members = 0usize;
                    loop {
                        let u = tarjan_stack.pop().expect("tarjan stack nonempty at root pop");
                        on_stack[u] = false;
                        comp[u] = scc_count;
                        members += 1;
                        if u == v {
                            break;
                        }
                    }
                    scc_members.push(members);
                    scc_count += 1;
                }
                work.pop();
                if let Some(&(parent, _)) = work.last() {
                    low[parent] = low[parent].min(low[v]);
                }
            }
        }
    }

    // ── condensation weights (MAX) + successor sets ──
    let mut scc_weight = vec![0u32; scc_count];
    for v in 0..n {
        let w = node_weights.get(&nodes[v]).copied().unwrap_or(0);
        let c = comp[v];
        scc_weight[c] = scc_weight[c].max(w);
    }
    let mut scc_succ: Vec<Vec<usize>> = vec![Vec::new(); scc_count];
    for v in 0..n {
        let cv = comp[v];
        for &w in &adj[v] {
            let cw = comp[w];
            if cv != cw {
                scc_succ[cv].push(cw);
            }
        }
    }
    for s in &mut scc_succ {
        s.sort_unstable();
        s.dedup();
    }

    // ── reverse-topo longest-path DP over the condensation ──
    // SCC ids are in reverse-topological order (successors have smaller ids), so
    // iterating ascending computes every successor before its predecessor.
    let mut crit = vec![0u64; scc_count];
    for c in 0..scc_count {
        let mut best_succ = 0u64;
        for &s in &scc_succ[c] {
            best_succ = best_succ.max(crit[s]);
        }
        crit[c] = u64::from(scc_weight[c]).saturating_add(best_succ);
    }

    // ── publish bands for SINGLETON-SCC nodes only (confidence gate) ──
    let mut bands: HashMap<DagNodeKey, u8> = HashMap::new();
    for v in 0..n {
        let c = comp[v];
        if scc_members[c] == 1 {
            bands.insert(nodes[v].clone(), quantize_criticality(crit[c]));
        }
    }
    CriticalitySnapshot { bands }
}

// ── DagState: the runtime object the scheduler holds ─────────────────────────

/// (#dag-criticality) The runtime DAG state a scheduler holds behind
/// `Option<Arc<DagState>>` (None when `dag_critical_path_enabled` is off). Owns the
/// transient producer map, the self-contained edge store + node durations, and the
/// published criticality snapshot.
///
/// # Lock discipline (v2 fix 11 / distsys MAJOR-3)
/// The edge-store `parking_lot::Mutex` is a strict LEAF: edge inference DRAINS the async
/// producer map into an owned `Vec`, RELEASES the async lock, and only THEN takes the
/// leaf lock — no lock is ever held across `.await`. The snapshot is a
/// `RwLock<Arc<..>>`; a read clones the `Arc` under a brief read lock (O(1)).
#[derive(Debug)]
pub struct DagState {
    // CAPPED AT PRODUCER_MAP_MAX (65536): transient output-blob-digest → producer node.
    // NEVER persisted (churns per build); only the resolved stable-key edges persist. No
    // owned blob bytes (a DagNodeKey = clamped strings + a small count).
    producer_map: tokio::sync::Mutex<LruCache<DigestInfo, ProducerEntry>>,
    store: Mutex<EdgeStore>,
    snapshot: RwLock<Arc<CriticalitySnapshot>>,
}

impl DagState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            producer_map: tokio::sync::Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(PRODUCER_MAP_MAX).expect("PRODUCER_MAP_MAX nonzero"),
            )),
            store: Mutex::new(EdgeStore::new()),
            snapshot: RwLock::new(Arc::new(CriticalitySnapshot::default())),
        }
    }

    /// Record that `node` produced the output blob `digest` (completion path). Resets the
    /// blob's consumer-count (a freshly-produced blob starts a new fan-in tally).
    pub async fn record_producer(&self, digest: DigestInfo, node: DagNodeKey) {
        let mut map = self.producer_map.lock().await;
        map.put(
            digest,
            ProducerEntry {
                node,
                consumer_count: 0,
            },
        );
    }

    /// Fold one completed action's wall duration (ms) into its node's histogram
    /// (completion path). Strict-leaf lock, no `.await` held.
    pub fn record_duration(&self, node: DagNodeKey, dur_ms: u64) {
        self.store.lock().record_duration(node, dur_ms);
    }

    /// Infer + record stable-key edges for a dispatched consumer from its resolved input
    /// blob digests (dispatch path, once per dispatched action). Drains the async
    /// producer map into an owned `Vec` (applying the same-instance filter v2 fix 6 + the
    /// ubiquitous-blob fan-in exclusion v2 fix 7), RELEASES the async lock, then takes the
    /// leaf edge-store lock. Returns the number of edges observed.
    pub async fn infer_edges(&self, consumer: &DagNodeKey, input_digests: &[DigestInfo]) -> usize {
        // Drain matching producers under the async producer-map lock into an owned Vec.
        let producers: Vec<DagNodeKey> = {
            let mut map = self.producer_map.lock().await;
            let mut out: Vec<DagNodeKey> = Vec::new();
            for d in input_digests {
                if let Some(entry) = map.get_mut(d) {
                    // Ubiquitous-blob fan-in: every distinct reference bumps the count;
                    // once it EXCEEDS F the blob produces no further edges (v2 fix 7).
                    entry.consumer_count = entry.consumer_count.saturating_add(1);
                    if entry.consumer_count > DAG_UBIQUITOUS_FANIN_MAX {
                        continue;
                    }
                    // Same-instance filter: the producer map is global-by-digest, so a
                    // cross-instance intersection would forge an edge (v2 fix 6). Emit
                    // only when producer.instance == consumer.instance.
                    if entry.node.instance_name == consumer.instance_name {
                        out.push(entry.node.clone());
                    }
                }
            }
            out
        }; // producer-map lock RELEASED here — before the leaf lock is taken.

        if producers.is_empty() {
            return 0;
        }
        let mut store = self.store.lock();
        let mut emitted = 0usize;
        for producer in producers {
            // A producer==consumer self-edge carries no ordering; skip it.
            if &producer == consumer {
                continue;
            }
            store.observe_edge(DagEdge {
                producer,
                consumer: consumer.clone(),
            });
            emitted += 1;
        }
        emitted
    }

    /// Recompute the criticality snapshot from the current edge store and publish it
    /// (background task). Advances the build epoch, snapshots edges + node weights under
    /// the leaf lock into owned structures, RELEASES the lock, runs the O(V+E) DP off the
    /// lock, then swaps the published `Arc`.
    pub fn recompute(&self) {
        let (edges, weights) = {
            let mut store = self.store.lock();
            store.build_epoch = store.build_epoch.saturating_add(1);
            (store.edges_for_compute(), store.node_weights())
        }; // leaf lock released before the (potentially larger) DP.
        let snapshot = compute_criticality(&edges, &weights, DAG_MIN_EDGE_OBS);
        *self.snapshot.write() = Arc::new(snapshot);
    }

    /// The confidence-gated criticality band for a node (dispatch/enqueue path). `0` when
    /// absent / not confident → FIFO fallback (v2 fix 1). O(1): a brief read-lock clone
    /// of the `Arc` then a `HashMap` lookup.
    #[inline]
    pub fn criticality_band(&self, node: &DagNodeKey) -> u8 {
        let snap = self.snapshot.read().clone();
        snap.band(node)
    }

    /// Clone the currently published snapshot `Arc` (for the enqueue path to read many
    /// bands without re-locking).
    #[inline]
    pub fn snapshot(&self) -> Arc<CriticalitySnapshot> {
        self.snapshot.read().clone()
    }

    /// (persistence) Plain-data snapshot of the edge store + node durations. Locks the
    /// leaf ONLY to clone entries out — no I/O under the lock.
    pub fn persist_snapshot(&self) -> DagPersistData {
        let store = self.store.lock();
        let edges = store
            .edges
            .iter()
            .map(|(e, s)| DagEdgeSnapshot {
                producer_instance: e.producer.instance_name.clone(),
                producer_target: e.producer.target_id.clone(),
                producer_mnemonic: e.producer.action_mnemonic.clone(),
                consumer_instance: e.consumer.instance_name.clone(),
                consumer_target: e.consumer.target_id.clone(),
                consumer_mnemonic: e.consumer.action_mnemonic.clone(),
                count: s.count,
                last_seen_build_epoch: s.last_seen_build_epoch,
            })
            .collect();
        let durations = store
            .durations
            .iter()
            .map(|(k, h)| DagNodeDurationSnapshot {
                instance_name: k.instance_name.clone(),
                target_id: k.target_id.clone(),
                action_mnemonic: k.action_mnemonic.clone(),
                dur_hist: h.buckets_vec(),
                sample_count: h.count,
            })
            .collect();
        DagPersistData { edges, durations }
    }

    /// (persistence) Load a plain-data snapshot into a fresh edge store BEFORE serving.
    /// Each entry is VALIDATED via the `DagNodeKey` constructor + histogram-length check;
    /// an invalid entry is SKIPPED (never panics). Returns `(edges_loaded, nodes_loaded)`.
    pub fn load_persist(&self, data: DagPersistData) -> (usize, usize) {
        let mut store = self.store.lock();
        let mut edges_loaded = 0usize;
        for e in data.edges {
            let (Some(producer), Some(consumer)) = (
                ProfileKey::from_parts(&e.producer_instance, &e.producer_target, &e.producer_mnemonic),
                ProfileKey::from_parts(&e.consumer_instance, &e.consumer_target, &e.consumer_mnemonic),
            ) else {
                continue;
            };
            store.edges.push(
                DagEdge { producer, consumer },
                EdgeStat {
                    count: e.count,
                    last_seen_build_epoch: e.last_seen_build_epoch,
                },
            );
            edges_loaded += 1;
        }
        let mut nodes_loaded = 0usize;
        for d in data.durations {
            let Some(key) =
                ProfileKey::from_parts(&d.instance_name, &d.target_id, &d.action_mnemonic)
            else {
                continue;
            };
            let Some(hist) = DurHist::from_buckets_vec(&d.dur_hist, d.sample_count) else {
                continue;
            };
            store.durations.push(key, hist);
            nodes_loaded += 1;
        }
        (edges_loaded, nodes_loaded)
    }

    /// Resident edge / duration / producer counts (telemetry).
    pub fn edge_count(&self) -> usize {
        self.store.lock().edge_len()
    }

    pub fn duration_count(&self) -> usize {
        self.store.lock().duration_len()
    }

    pub fn eviction_count(&self) -> u64 {
        self.store.lock().evictions
    }
}

impl Default for DagState {
    fn default() -> Self {
        Self::new()
    }
}

// ── persistence: self-contained sibling file (v2 fix 11) ─────────────────────

/// Preallocation cap (64 MiB) for DEserialization of the untrusted on-disk edge blob —
/// a garbage length field must fail as a graceful decode `Err`, not a multi-GB allocation
/// abort (mirrors `resource_profile_persist::SNAPSHOT_PREALLOC_LIMIT`).
const DAG_PREALLOC_LIMIT: usize = 64 << 20;

/// wincode config — bincode-style fixed-int LE with a BOUNDED prealloc limit.
type DagWincodeConfig = wincode::config::Configuration<
    true,
    DAG_PREALLOC_LIMIT,
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    wincode::int_encoding::FixInt,
    u32,
>;

/// Magic prefix ("NLDG" — NativeLink DAG). Independent of the resource-profile snapshot's
/// magic (v2 fix 11 — sibling file, own header, NOT one shared blob).
const DAG_SNAPSHOT_MAGIC: u32 = 0x4E4C_4447;

/// On-disk schema version. Bump on any incompatible layout change; a mismatch starts
/// FRESH (advisory data re-accumulates).
const DAG_SNAPSHOT_VERSION: u32 = 1;

/// One persisted stable-key edge (flattened node strings + stat).
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct DagEdgeSnapshot {
    pub producer_instance: String,
    pub producer_target: String,
    pub producer_mnemonic: String,
    pub consumer_instance: String,
    pub consumer_target: String,
    pub consumer_mnemonic: String,
    pub count: u32,
    pub last_seen_build_epoch: u32,
}

/// One persisted per-node duration histogram.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct DagNodeDurationSnapshot {
    pub instance_name: String,
    pub target_id: String,
    pub action_mnemonic: String,
    pub dur_hist: Vec<u32>,
    pub sample_count: u64,
}

/// The plain-data payload of a DAG snapshot (edges + node durations).
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct DagPersistData {
    pub edges: Vec<DagEdgeSnapshot>,
    pub durations: Vec<DagNodeDurationSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct DagPersistHeader {
    magic: u32,
    version: u32,
    snapshot_unix_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct DagPersistSnapshot {
    header: DagPersistHeader,
    data: DagPersistData,
}

/// Serialize a DAG snapshot (versioned header + edges + durations) to bytes.
///
/// # Errors
/// Returns `Err` if wincode serialization fails.
pub fn serialize_dag_snapshot(data: DagPersistData, snapshot_unix_secs: u64) -> Result<Vec<u8>, Error> {
    let snapshot = DagPersistSnapshot {
        header: DagPersistHeader {
            magic: DAG_SNAPSHOT_MAGIC,
            version: DAG_SNAPSHOT_VERSION,
            snapshot_unix_secs,
        },
        data,
    };
    wincode::config::serialize(&snapshot, DagWincodeConfig::new())
        .map_err(|e| make_err!(Code::Internal, "dag snapshot serialize failed: {e:?}"))
}

/// Deserialize a DAG snapshot blob → `(snapshot_unix_secs, data)`, validating magic +
/// version. A mismatch / corrupt blob returns `Err` (the caller warns + starts fresh —
/// NEVER panics).
///
/// # Errors
/// Returns `Err` on a wincode failure, a bad magic, or a version mismatch.
pub fn deserialize_dag_snapshot(bytes: &[u8]) -> Result<(u64, DagPersistData), Error> {
    let snapshot: DagPersistSnapshot =
        wincode::config::deserialize::<DagPersistSnapshot, DagWincodeConfig>(
            bytes,
            DagWincodeConfig::new(),
        )
        .map_err(|e| make_err!(Code::Internal, "dag snapshot deserialize failed: {e:?}"))?;
    if snapshot.header.magic != DAG_SNAPSHOT_MAGIC {
        return Err(make_err!(
            Code::InvalidArgument,
            "dag snapshot magic mismatch: got {:#x}, want {DAG_SNAPSHOT_MAGIC:#x}",
            snapshot.header.magic
        ));
    }
    if snapshot.header.version != DAG_SNAPSHOT_VERSION {
        return Err(make_err!(
            Code::InvalidArgument,
            "dag snapshot version mismatch: got {}, want {DAG_SNAPSHOT_VERSION} — starting fresh",
            snapshot.header.version
        ));
    }
    Ok((snapshot.header.snapshot_unix_secs, snapshot.data))
}

/// Atomically write a serialized DAG snapshot via `write tmp → rename`. NO fsync
/// (advisory data; ZFS `sync=disabled`). Bytes serialized OFF the leaf lock by the caller.
///
/// # Errors
/// Returns `Err` on a filesystem write / rename failure (best-effort).
pub async fn write_dag_snapshot_bytes(path: &std::path::Path, bytes: &[u8]) -> Result<(), Error> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    tokio::fs::write(&tmp, bytes)
        .await
        .err_tip(|| format!("writing dag snapshot tmp {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .err_tip(|| format!("renaming dag snapshot into place {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
impl EdgeStore {
    /// Test-only constructor with explicit small caps so eviction is reachable.
    fn with_caps(edge_cap: usize, dur_cap: usize) -> Self {
        Self {
            edges: LruCache::new(std::num::NonZeroUsize::new(edge_cap).unwrap()),
            durations: LruCache::new(std::num::NonZeroUsize::new(dur_cap).unwrap()),
            build_epoch: 0,
            evictions: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use nativelink_macro::nativelink_test;

    use super::*;

    fn node(instance: &str, target: &str, mnemonic: &str) -> DagNodeKey {
        ProfileKey::from_parts(instance, target, mnemonic).expect("non-empty parts")
    }

    fn dig(b: u8) -> DigestInfo {
        DigestInfo::new([b; 32], u64::from(b) + 1)
    }

    // ── DurHist ──────────────────────────────────────────────────────────

    #[test]
    fn dur_hist_p50_and_empty() {
        let mut h = DurHist::default();
        assert_eq!(h.p50_ms(), 0, "an empty duration sketch must weigh 0");
        for _ in 0..10 {
            h.record(1000); // 1000 → bucket 10 [512,1024) rep 768
        }
        assert_eq!(
            h.p50_ms(),
            768,
            "p50 of ten 1000ms samples must land in bucket 10 rep 768, got {}",
            h.p50_ms()
        );
    }

    // ── quantization (v2 fix: sink→0, monotone) ──────────────────────────

    #[test]
    fn quantize_sink_is_zero_and_monotone() {
        assert_eq!(quantize_criticality(0), 0, "a sink (crit 0) must quantize to band 0");
        assert_eq!(
            quantize_criticality(DAG_CRITICALITY_QUANT_MS),
            1,
            "one quant-step of criticality must be band 1"
        );
        assert!(
            quantize_criticality(5000) >= quantize_criticality(1000),
            "quantization must be monotone non-decreasing"
        );
        assert_eq!(
            quantize_criticality(u64::MAX),
            DAG_MAX_BAND,
            "criticality far above the top band must saturate at DAG_MAX_BAND (255)"
        );
    }

    // ── (d)/(e) confidence gate: below-min-obs edges are ignored ──────────

    #[test]
    fn criticality_ignores_edges_below_min_obs() {
        let (a, b) = (node("main", "//a", "M"), node("main", "//b", "M"));
        let mut weights = HashMap::new();
        weights.insert(a.clone(), 3000u32);
        weights.insert(b.clone(), 1000u32);

        // count 1 < DAG_MIN_EDGE_OBS (2) → edge ignored → NO confident node.
        let snap = compute_criticality(&[(a.clone(), b.clone(), 1)], &weights, DAG_MIN_EDGE_OBS);
        assert_eq!(
            snap.confident_nodes(),
            0,
            "an edge observed fewer than DAG_MIN_EDGE_OBS times must NOT create a confident \
             node (immature/one-off linkage never elevates above FIFO); got {} nodes",
            snap.confident_nodes()
        );
        assert_eq!(snap.band(&a), 0, "an ignored-edge node reads band 0 (FIFO fallback)");

        // count 2 == DAG_MIN_EDGE_OBS → edge matures → A + B become confident.
        let snap = compute_criticality(&[(a.clone(), b.clone(), 2)], &weights, DAG_MIN_EDGE_OBS);
        assert_eq!(
            snap.confident_nodes(),
            2,
            "an edge observed >= DAG_MIN_EDGE_OBS times must mature both endpoints into \
             confident nodes; got {}",
            snap.confident_nodes()
        );
    }

    // ── (f) longest-path over a chain; ordering is critical-path correct ──

    #[test]
    fn criticality_linear_chain_orders_by_downstream_work() {
        // A → B → C (all mature). A unblocks B+C; C is a sink. wall weights 3000/2000/1000ms.
        let (a, b, c) = (node("main", "//a", "M"), node("main", "//b", "M"), node("main", "//c", "M"));
        let mut w = HashMap::new();
        w.insert(a.clone(), 3000u32);
        w.insert(b.clone(), 2000u32);
        w.insert(c.clone(), 1000u32);
        let edges = [(a.clone(), b.clone(), 2), (b.clone(), c.clone(), 2)];
        let snap = compute_criticality(&edges, &w, DAG_MIN_EDGE_OBS);
        // crit(C)=1000 → band1; crit(B)=2000+1000=3000 → band3; crit(A)=3000+3000=6000 → band6.
        assert_eq!(snap.band(&c), 1, "sink C's band = its own 1000ms weight → band 1, got {}", snap.band(&c));
        assert_eq!(snap.band(&b), 3, "B's band = 2000+1000 downstream = 3000ms → band 3, got {}", snap.band(&b));
        assert_eq!(snap.band(&a), 6, "A's band = 3000+3000 downstream = 6000ms → band 6, got {}", snap.band(&a));
        assert!(
            snap.band(&a) > snap.band(&b) && snap.band(&b) > snap.band(&c),
            "the node unblocking the most serial downstream work must have the highest band \
             (A > B > C)"
        );
    }

    // ── (f) SCC: a spurious cycle collapses → members are NOT confident ───

    #[test]
    fn criticality_cyclic_accumulation_excludes_scc_members() {
        // A → B AND B → A (build 1 vs a later refactor) — a 2-member SCC. Neither is a
        // trustworthy longest-path node → both excluded → band 0 (FIFO fallback).
        let (a, b) = (node("main", "//a", "M"), node("main", "//b", "M"));
        let mut w = HashMap::new();
        w.insert(a.clone(), 5000u32);
        w.insert(b.clone(), 5000u32);
        let edges = [(a.clone(), b.clone(), 2), (b.clone(), a.clone(), 2)];
        let snap = compute_criticality(&edges, &w, DAG_MIN_EDGE_OBS);
        assert_eq!(
            snap.confident_nodes(),
            0,
            "a spurious cycle (A↔B) collapses to a multi-member SCC → NO confident node \
             (never a longest-path score on a cycle); got {}",
            snap.confident_nodes()
        );
        assert_eq!(snap.band(&a), 0, "an SCC member must read band 0 (FIFO fallback)");
    }

    // ── (f) SCC super-node weight = MAX (v2 fix 11), NOT sum ──────────────

    #[test]
    fn scc_supernode_weight_is_max_not_sum() {
        // X (singleton) → A, with A ↔ B a 2-member SCC (no downstream). X's criticality =
        // w(X) + crit(SCC{A,B}) = 0 + MAX(3000,5000) = 5000 → band 5. Under a (wrong) SUM
        // it would be 0 + 3000+5000 = 8000 → band 8. X is the only PUBLISHED node.
        let (x, a, b) = (node("main", "//x", "M"), node("main", "//a", "M"), node("main", "//b", "M"));
        let mut w = HashMap::new();
        w.insert(x.clone(), 0u32);
        w.insert(a.clone(), 3000u32);
        w.insert(b.clone(), 5000u32);
        let edges = [
            (x.clone(), a.clone(), 2),
            (a.clone(), b.clone(), 2),
            (b.clone(), a.clone(), 2),
        ];
        let snap = compute_criticality(&edges, &w, DAG_MIN_EDGE_OBS);
        assert_eq!(
            snap.band(&x),
            5,
            "the SCC super-node weight must be MAX(member weights)=5000, so X's band = \
             quantize(0+5000)=5 — a SUM would give band 8; got {}",
            snap.band(&x)
        );
        assert_eq!(snap.confident_nodes(), 1, "only the singleton X is a confident node");
    }

    // ── (e) edge inference soundness (async: producer map is a tokio Mutex) ─

    #[nativelink_test]
    async fn infer_edges_records_true_intersection_edge() {
        let s = DagState::new();
        let p = node("main", "//producer", "M");
        let c = node("main", "//consumer", "M");
        s.record_producer(dig(1), p.clone()).await;
        let emitted = s.infer_edges(&c, &[dig(1)]).await;
        assert_eq!(
            emitted, 1,
            "a consumer whose input tree reads the producer's output blob must emit exactly \
             one stable-key edge; got {emitted}"
        );
        assert_eq!(s.edge_count(), 1, "the edge store must hold the one inferred edge");
    }

    #[nativelink_test]
    async fn infer_edges_no_false_edge_without_intersection() {
        let s = DagState::new();
        let c = node("main", "//consumer", "M");
        s.record_producer(dig(1), node("main", "//producer", "M")).await;
        // Consumer reads a DIFFERENT blob → no intersection → no edge.
        let emitted = s.infer_edges(&c, &[dig(2)]).await;
        assert_eq!(emitted, 0, "no input↔output intersection must emit NO edge; got {emitted}");
        assert_eq!(s.edge_count(), 0);
    }

    #[nativelink_test]
    async fn infer_edges_cross_instance_excluded() {
        // Producer in instance "A", consumer in instance "B", SAME blob digest (the map is
        // global-by-digest). No edge may form across instances (v2 fix 6, security MED-2).
        let s = DagState::new();
        let p = node("instanceA", "//producer", "M");
        let c = node("instanceB", "//consumer", "M");
        s.record_producer(dig(1), p).await;
        let emitted = s.infer_edges(&c, &[dig(1)]).await;
        assert_eq!(
            emitted, 0,
            "a cross-instance producer/consumer intersection must NOT emit an edge (the \
             producer map is global-by-digest); got {emitted}"
        );
        assert_eq!(s.edge_count(), 0, "no cross-instance edge may be recorded");
    }

    #[nativelink_test]
    async fn infer_edges_ubiquitous_blob_excluded_over_fanin() {
        // One output blob referenced as input by many distinct consumers: once the fan-in
        // EXCEEDS DAG_UBIQUITOUS_FANIN_MAX the blob stops producing edges (v2 fix 7).
        let s = DagState::new();
        let p = node("main", "//common_header", "M");
        s.record_producer(dig(1), p).await;
        // The first F references emit; reference F+1 is over-threshold → skipped.
        let mut last_emitted = usize::MAX;
        for i in 0..=DAG_UBIQUITOUS_FANIN_MAX {
            let c = node("main", &format!("//consumer{i}"), "M");
            last_emitted = s.infer_edges(&c, &[dig(1)]).await;
        }
        assert_eq!(
            last_emitted, 0,
            "a producer blob referenced by MORE than DAG_UBIQUITOUS_FANIN_MAX distinct \
             consumers must stop emitting edges (degenerate fan-out exclusion); the \
             over-threshold reference emitted {last_emitted}"
        );
    }

    #[nativelink_test]
    async fn record_producer_and_duration_feed_recompute() {
        // Full path: record producer P → duration on P and C → two builds of edge P→C →
        // recompute publishes P (upstream) with a higher band than sink C.
        let s = DagState::new();
        let p = node("main", "//p", "M");
        let c = node("main", "//c", "M");
        s.record_duration(p.clone(), 4000);
        s.record_duration(c.clone(), 1000);
        s.record_producer(dig(1), p.clone()).await;
        // Two dispatched builds of the consumer → edge count reaches DAG_MIN_EDGE_OBS.
        s.infer_edges(&c, &[dig(1)]).await;
        s.infer_edges(&c, &[dig(1)]).await;
        s.recompute();
        assert!(
            s.criticality_band(&p) > s.criticality_band(&c),
            "after 2 observations the upstream producer P (unblocks C) must outrank the sink \
             C; got p={} c={}",
            s.criticality_band(&p),
            s.criticality_band(&c)
        );
        // A never-seen node reads band 0 (FIFO fallback / confidence gate).
        assert_eq!(
            s.criticality_band(&node("main", "//absent", "M")),
            0,
            "an absent node must read band 0 (confidence gate → FIFO fallback)"
        );
    }

    // ── (h) edge-store cap + eviction counter ────────────────────────────

    #[test]
    fn edge_store_bounds_and_counts_evictions() {
        let mut store = EdgeStore::with_caps(2, 2);
        let mk = |t: &str| DagEdge {
            producer: node("main", t, "M"),
            consumer: node("main", "//sink", "M"),
        };
        store.observe_edge(mk("//a"));
        store.observe_edge(mk("//b"));
        assert_eq!(store.edge_len(), 2);
        assert_eq!(store.evictions, 0, "no eviction while under cap");
        store.observe_edge(mk("//c")); // over cap 2 → evict LRU
        assert_eq!(store.edge_len(), 2, "the edge store must stay bounded at cap 2");
        assert_eq!(
            store.evictions, 1,
            "a third distinct edge over cap 2 must evict the LRU edge and count it; got {}",
            store.evictions
        );
    }

    #[test]
    fn observe_edge_bumps_count_not_size() {
        let mut store = EdgeStore::with_caps(4, 4);
        let e = DagEdge {
            producer: node("main", "//a", "M"),
            consumer: node("main", "//b", "M"),
        };
        store.observe_edge(e.clone());
        store.observe_edge(e.clone());
        assert_eq!(store.edge_len(), 1, "re-observing the same edge must not grow the store");
        let edges = store.edges_for_compute();
        assert_eq!(edges[0].2, 2, "re-observing the same edge must bump its count to 2");
    }

    // ── (h) persistence: round-trip + corrupt/version/oversized graceful ──

    #[test]
    fn persist_round_trip_preserves_edges_and_durations() {
        let s = DagState::new();
        s.record_duration(node("main", "//p", "M"), 4000);
        let mut store = s.store.lock();
        store.observe_edge(DagEdge {
            producer: node("main", "//p", "M"),
            consumer: node("main", "//c", "M"),
        });
        store.observe_edge(DagEdge {
            producer: node("main", "//p", "M"),
            consumer: node("main", "//c", "M"),
        });
        drop(store);

        let data = s.persist_snapshot();
        let bytes = serialize_dag_snapshot(data.clone(), 1_700_000_000).expect("serialize");
        let (secs, got) = deserialize_dag_snapshot(&bytes).expect("round-trip must deserialize");
        assert_eq!(secs, 1_700_000_000, "snapshot wall-time must round-trip");
        assert_eq!(got, data, "every edge + duration entry must round-trip byte-identically");

        // Load into a fresh state → the mature edge reconstructs a confident node.
        let fresh = DagState::new();
        let (edges_loaded, nodes_loaded) = fresh.load_persist(got);
        assert_eq!(edges_loaded, 1, "the one persisted edge must load back");
        assert_eq!(nodes_loaded, 1, "the one persisted node duration must load back");
        fresh.recompute();
        assert!(
            fresh.criticality_band(&node("main", "//p", "M")) > 0,
            "a loaded mature edge + duration must reconstruct a confident upstream band > 0"
        );
    }

    #[test]
    fn corrupt_blob_deserializes_to_err_not_panic() {
        let garbage = vec![0xABu8; 41];
        assert!(
            deserialize_dag_snapshot(&garbage).is_err(),
            "a corrupt blob must return Err (→ caller warns + starts fresh), never panic"
        );
    }

    #[test]
    fn version_mismatch_is_rejected() {
        let snap = DagPersistSnapshot {
            header: DagPersistHeader {
                magic: DAG_SNAPSHOT_MAGIC,
                version: DAG_SNAPSHOT_VERSION + 1,
                snapshot_unix_secs: 1,
            },
            data: DagPersistData {
                edges: vec![],
                durations: vec![],
            },
        };
        let bytes = wincode::config::serialize(&snap, DagWincodeConfig::new()).expect("serialize");
        let err = deserialize_dag_snapshot(&bytes).expect_err("a version mismatch must be rejected");
        assert_eq!(
            err.code,
            Code::InvalidArgument,
            "a version mismatch must surface as InvalidArgument so the caller starts fresh"
        );
    }

    #[test]
    fn oversized_edges_length_deserializes_to_err_not_panic() {
        // A VALID header + empty data, then overwrite the trailing Vec length prefix with an
        // absurd count — the SNAPSHOT_PREALLOC_LIMIT guard must fire a graceful Err, never a
        // capacity-overflow abort (the "never panic on load" rule).
        let valid = DagPersistSnapshot {
            header: DagPersistHeader {
                magic: DAG_SNAPSHOT_MAGIC,
                version: DAG_SNAPSHOT_VERSION,
                snapshot_unix_secs: 1_700_000_000,
            },
            data: DagPersistData {
                edges: vec![],
                durations: vec![],
            },
        };
        let mut bytes = wincode::config::serialize(&valid, DagWincodeConfig::new()).expect("serialize");
        let n = bytes.len();
        assert!(n >= 8, "an empty snapshot still carries a length prefix");
        // The FINAL 8 bytes are the durations Vec length (== 0 for empty). Make it absurd.
        let oversized: u64 = 1 << 40;
        bytes[n - 8..].copy_from_slice(&oversized.to_le_bytes());
        let err = deserialize_dag_snapshot(&bytes)
            .expect_err("an oversized length must fail gracefully, never abort/panic");
        assert_eq!(
            err.code,
            Code::Internal,
            "the oversized-length decode must surface as a wincode Internal decode Err (the \
             DAG_PREALLOC_LIMIT guard tripping), never a capacity-overflow process abort"
        );
    }

    #[test]
    fn load_persist_skips_invalid_key_entry() {
        // An edge whose producer target is empty is NOT a valid DagNodeKey → skipped.
        let s = DagState::new();
        let data = DagPersistData {
            edges: vec![DagEdgeSnapshot {
                producer_instance: "main".to_string(),
                producer_target: String::new(), // invalid (empty target)
                producer_mnemonic: "M".to_string(),
                consumer_instance: "main".to_string(),
                consumer_target: "//c".to_string(),
                consumer_mnemonic: "M".to_string(),
                count: 3,
                last_seen_build_epoch: 0,
            }],
            durations: vec![],
        };
        let (edges_loaded, _) = s.load_persist(data);
        assert_eq!(
            edges_loaded, 0,
            "an edge with an invalid (empty-target) node key must be SKIPPED on load, not \
             loaded and not a panic"
        );
    }
}
