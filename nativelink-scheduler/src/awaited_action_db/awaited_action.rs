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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_error::{Error, ResultExt, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionStage, ActionState, OperationId, WorkerId,
};
use nativelink_util::origin_event::{
    BAZEL_METADATA_KEY, OriginMetadata, request_metadata_from_baggage,
};
use opentelemetry::baggage::BaggageExt;
use opentelemetry::context::Context;
use opentelemetry_semantic_conventions::attribute::ENDUSER_ID;
use serde::{Deserialize, Serialize};
use static_assertions::{assert_eq_size, const_assert, const_assert_eq};

use crate::dag_criticality::CriticalitySnapshot;
use crate::resource_profile::ProfileKey;

/// The version of the awaited action.
/// This number will always increment by one each time
/// the action is updated.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
struct AwaitedActionVersion(i64);

impl MetricsComponent for AwaitedActionVersion {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Counter(u64::from_ne_bytes(
            self.0.to_ne_bytes(),
        )))
    }
}

/// An action that is being awaited on and last known state.
#[derive(Debug, Clone, MetricsComponent, Serialize, Deserialize)]
pub struct AwaitedAction {
    /// The current version of the action.
    #[metric(help = "The version of the AwaitedAction")]
    version: AwaitedActionVersion,

    /// The action that is being awaited on.
    #[metric(help = "The action info of the AwaitedAction")]
    action_info: Arc<ActionInfo>,

    /// The operation id of the action.
    // If you need the client operation id, it may be set in
    // ActionState::operation_id.
    #[metric(help = "The operation id of the AwaitedAction")]
    operation_id: OperationId,

    /// The currentsort key used to order the actions.
    #[metric(help = "The sort key of the AwaitedAction")]
    sort_key: AwaitedActionSortKey,

    /// The time the action was last updated.
    #[metric(help = "The last time the worker updated the AwaitedAction")]
    last_worker_updated_timestamp: SystemTime,

    /// The last time the client sent a keepalive message.
    #[metric(help = "The last time the client sent a keepalive message")]
    last_client_keepalive_timestamp: SystemTime,

    /// Worker that is currently running this action, None if unassigned.
    #[metric(help = "The worker id of the AwaitedAction")]
    worker_id: Option<WorkerId>,

    /// The current state of the action.
    #[metric(help = "The state of the AwaitedAction")]
    state: Arc<ActionState>,

    /// The origin metadata of the action.
    maybe_origin_metadata: Option<OriginMetadata>,

    /// Number of attempts the job has been tried.
    #[metric(help = "The number of attempts the AwaitedAction has been tried")]
    pub attempts: usize,
}

impl AwaitedAction {
    pub fn new(operation_id: OperationId, action_info: Arc<ActionInfo>, now: SystemTime) -> Self {
        // No DAG snapshot → criticality band 0 → the sort key is ORDER-EQUIVALENT to the
        // pre-feature `[priority | inverted_insert_ts]` within a ~194-day insert window
        // (flag-OFF path). It is NOT byte-identical: `new_with_criticality` unconditionally
        // narrows the inverted timestamp from 32 to 24 bits (the freed high 8 bits hold the
        // band, which is 0 here), so a band-0 key differs BYTE-wise from the old key while
        // sorting identically until the inverted seconds wrap past 2^24. The kill-switch
        // restores the pre-feature ORDER, not the pre-feature BYTES.
        Self::new_with_criticality(operation_id, action_info, now, None)
    }

    /// (#dag-criticality, v2 fix 4) Construct an awaited action folding a confidence-gated
    /// criticality band into the SECONDARY region of its (immutable) sort key AT ENQUEUE.
    ///
    /// This is the THIRD `DagNodeKey` derivation site — it MUST derive the key identically
    /// to `resource_profile_keys` (`api_worker_scheduler.rs`) and the completion-fold, or
    /// scores mis-attribute: `(instance_name, target_id, action_mnemonic)` where the target
    /// + mnemonic come from the SAME Bazel `RequestMetadata` baggage parsed just below for
    /// `maybe_origin_metadata`. An absent snapshot / non-derivable key / non-confident node
    /// yields band 0 → the tie-break degrades to today's FIFO (`inverted_insert_ts`).
    pub fn new_with_criticality(
        operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        now: SystemTime,
        dag_snapshot: Option<&CriticalitySnapshot>,
    ) -> Self {
        let ctx = Context::current();
        let baggage = ctx.baggage();

        let maybe_origin_metadata = if baggage.is_empty() {
            None
        } else {
            let bazel_metadata = baggage
                .get(BAZEL_METADATA_KEY)
                .and_then(|value| request_metadata_from_baggage(value.as_str()).ok());
            Some(OriginMetadata {
                identity: baggage
                    .get(ENDUSER_ID)
                    .map(|v| v.as_str().to_string())
                    .unwrap_or_default(),
                bazel_metadata,
            })
        };

        // (#dag-criticality, v2 fix 4d) Confidence-gated criticality band, read from the
        // published snapshot at enqueue. Same key derivation as `resource_profile_keys`.
        let criticality = dag_snapshot.map_or(0, |snap| {
            let (target_id, action_mnemonic) = maybe_origin_metadata
                .as_ref()
                .and_then(|m| m.bazel_metadata.as_ref())
                .map_or(("", ""), |bm| {
                    (bm.target_id.as_str(), bm.action_mnemonic.as_str())
                });
            ProfileKey::from_parts(action_info.instance_name(), target_id, action_mnemonic)
                .map_or(0, |key| snap.band(&key))
        });

        let sort_key = AwaitedActionSortKey::new_with_unique_key(
            action_info.priority,
            criticality,
            &action_info.insert_timestamp,
        );
        let action_state = Arc::new(ActionState {
            stage: ActionStage::Queued,
            // Note: We don't use the real client_operation_id here because
            // the only place AwaitedAction::new should ever be called is
            // when the action is first created and this struct will be stored
            // in the database, so we don't want to accidentally leak the
            // client_operation_id to all clients.
            client_operation_id: operation_id.clone(),
            action_digest: action_info.unique_qualifier.digest(),
            last_transition_timestamp: now,
        });

        Self {
            version: AwaitedActionVersion(0),
            action_info,
            operation_id,
            sort_key,
            attempts: 0,
            last_worker_updated_timestamp: now,
            last_client_keepalive_timestamp: now,
            maybe_origin_metadata,
            worker_id: None,
            state: action_state,
        }
    }

    pub(crate) const fn version(&self) -> i64 {
        self.version.0
    }

    pub(crate) const fn set_version(&mut self, version: i64) {
        self.version = AwaitedActionVersion(version);
    }

    pub(crate) const fn increment_version(&mut self) {
        self.version = AwaitedActionVersion(self.version.0 + 1);
    }

    pub const fn action_info(&self) -> &Arc<ActionInfo> {
        &self.action_info
    }

    pub const fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    pub(crate) const fn sort_key(&self) -> AwaitedActionSortKey {
        self.sort_key
    }

    /// Boost this action to maximum priority so it is scheduled next.
    /// Used for retrying infrastructure failures (e.g. OOM/SIGKILL).
    pub(crate) fn boost_priority(&mut self) {
        self.sort_key = AwaitedActionSortKey::new(i32::MAX, 0);
    }

    pub const fn state(&self) -> &Arc<ActionState> {
        &self.state
    }

    pub fn is_complete(&self) -> bool {
        match &self.state.stage {
            ActionStage::Unknown
            | ActionStage::CacheCheck
            | ActionStage::Queued
            | ActionStage::Executing => false,
            ActionStage::Completed(_) | ActionStage::CompletedFromCache(_) => true,
        }
    }

    pub(crate) const fn maybe_origin_metadata(&self) -> Option<&OriginMetadata> {
        self.maybe_origin_metadata.as_ref()
    }

    pub(crate) const fn worker_id(&self) -> Option<&WorkerId> {
        self.worker_id.as_ref()
    }

    pub(crate) const fn last_worker_updated_timestamp(&self) -> SystemTime {
        self.last_worker_updated_timestamp
    }

    pub(crate) const fn worker_keep_alive(&mut self, now: SystemTime) {
        self.last_worker_updated_timestamp = now;
    }

    pub(crate) const fn last_client_keepalive_timestamp(&self) -> SystemTime {
        self.last_client_keepalive_timestamp
    }

    pub(crate) const fn update_client_keep_alive(&mut self, now: SystemTime) {
        self.last_client_keepalive_timestamp = now;
    }

    pub(crate) fn set_client_operation_id(&mut self, client_operation_id: OperationId) {
        Arc::make_mut(&mut self.state).client_operation_id = client_operation_id;
    }

    /// Sets the worker id that is currently processing this action.
    pub(crate) fn set_worker_id(&mut self, new_maybe_worker_id: Option<WorkerId>, now: SystemTime) {
        if self.worker_id != new_maybe_worker_id {
            self.worker_id = new_maybe_worker_id;
            self.worker_keep_alive(now);
        }
    }

    /// Sets the current state of the action and updates the last worker updated timestamp.
    pub fn worker_set_state(&mut self, mut state: Arc<ActionState>, now: SystemTime) {
        core::mem::swap(&mut self.state, &mut state);
        self.worker_keep_alive(now);
    }
}

impl TryFrom<&[u8]> for AwaitedAction {
    type Error = Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_json::from_slice(value)
            .map_err(|e| make_input_err!("{}", e.to_string()))
            .err_tip(|| "In AwaitedAction::TryFrom::&[u8]")
    }
}

/// The key used to sort the awaited actions.
///
/// The rules for sorting are as follows:
/// 1. priority of the action
/// 2. insert order of the action (lower = higher priority)
/// 3. (mostly random hash based on the action info)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct AwaitedActionSortKey(u64);

impl MetricsComponent for AwaitedActionSortKey {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Counter(self.0))
    }
}

impl AwaitedActionSortKey {
    const fn new(priority: i32, insert_timestamp: u32) -> Self {
        // Criticality band 0 → the pre-feature ordering (used by `boost_priority` and the
        // priority-primary/FIFO const-asserts below).
        Self::new_with_criticality(priority, 0, insert_timestamp)
    }

    /// (#dag-criticality, v2 fix 4) Pack the sort key as
    /// `[priority:32 | criticality:8 | inverted_insert_ts:24]` so that, in descending
    /// order: (1) higher client priority ALWAYS sorts first (top 32 bits untouched — band
    /// preservation is math-guaranteed, distsys sign-off), (2) within a priority band a
    /// higher criticality band sorts first, (3) within equal priority AND criticality an
    /// EARLIER insert timestamp sorts first (FIFO — the confidence-gate degradation when
    /// criticality is 0/absent).
    ///
    /// The criticality band (8 bits) STEALS the HIGH 8 bits of the 32-bit insert-timestamp
    /// region, leaving the LOW 24 bits for the (inverted) timestamp. Within one build the
    /// seconds are close, so the low 24 bits strictly discriminate insert order; a
    /// write-burst collides hundreds of actions per second into one second, so the low bits
    /// still order (red-team). The truncation only wraps across a ~194-day window — an
    /// accepted approximation for a batch fleet (v2 fix 4b).
    const fn new_with_criticality(priority: i32, criticality: u8, insert_timestamp: u32) -> Self {
        // Shift the signed i32 range [i32::MIN, i32::MAX] to the unsigned u32 range
        // [0, u32::MAX] to preserve ordering. Occupies the TOP 32 bits (bits 32..64) — the
        // client-priority band, NEVER perturbed by criticality or the timestamp.
        let priority_u32 = i32::MIN.unsigned_abs().wrapping_add_signed(priority);

        // Invert the timestamp so a LARGER timestamp yields a SMALLER value (descending),
        // then keep only the LOW 24 bits (the high 8 are the criticality region).
        let inverted_ts_low24 = (insert_timestamp ^ u32::MAX) & 0x00FF_FFFF;

        // Secondary region (bits 0..32): [criticality:8 (bits 24..32) | inverted_ts:24].
        let low32 = ((criticality as u32) << 24) | inverted_ts_low24;

        Self(((priority_u32 as u64) << 32) | (low32 as u64))
    }

    fn new_with_unique_key(
        priority: i32,
        criticality: u8,
        insert_timestamp: &SystemTime,
    ) -> Self {
        let timestamp = u32::try_from(
            insert_timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap_or(u32::MAX);
        Self::new_with_criticality(priority, criticality, timestamp)
    }

    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }

    /// (#dag-criticality) The criticality band folded into this sort key — bits 24..32 of
    /// the low 32-bit region (`[priority:32 | criticality:8 | inv_ts:24]`). `0` means the
    /// FIFO-fallback band (absent / non-confident node, or an enqueue with no DAG snapshot).
    /// Read at enqueue to attribute the kill/keep ratio (band>0-applied vs band-0-fallback)
    /// WITHOUT re-deriving the node key — the band is already committed to the key.
    pub(crate) const fn criticality_band(self) -> u8 {
        ((self.0 >> 24) & 0xFF) as u8
    }
}

// Ensure the size of the sort key is the same as a `u64`.
assert_eq_size!(AwaitedActionSortKey, u64);

// (#dag-criticality, v2 fix 4a) Exact layout `[priority:32 | criticality:8 | inv_ts:24]`,
// REWRITTEN from the pre-feature `[priority:32 | inv_ts:32]`.
// - priority 0x1234_5678 + 0x8000_0000 = 0x9234_5678 (i32::MIN shifted to 0), top 32 bits.
// - criticality 0 (the plain `new`) → bits 24..32 are 0.
// - inverted ts: 0x9abc_def0 ^ 0xFFFF_FFFF = 0x6543_210f; LOW 24 bits = 0x0043_210f.
const_assert_eq!(
    AwaitedActionSortKey::new(0x1234_5678, 0x9abc_def0).0,
    AwaitedActionSortKey(0x9234_5678_0043_210f).0
);
// (#dag-criticality) Criticality is the SECONDARY key: within a priority band a higher
// criticality band sorts first (larger u64), above the insert timestamp.
const_assert!(
    AwaitedActionSortKey::new_with_criticality(0, 1, 0).0
        > AwaitedActionSortKey::new_with_criticality(0, 0, 0).0
);
// (#dag-criticality) BAND PRESERVATION (the correctness invariant): MAX criticality at a
// lower client priority NEVER outranks the MINIMUM criticality at a higher client priority
// — criticality can never cross a client-priority band (it sits below bit 32).
const_assert!(
    AwaitedActionSortKey::new_with_criticality(1, 0, u32::MAX).0
        > AwaitedActionSortKey::new_with_criticality(0, u8::MAX, 0).0
);
// Ensure the priority is used as the sort key first.
const_assert!(
    AwaitedActionSortKey::new(i32::MAX, 0).0 > AwaitedActionSortKey::new(i32::MAX - 1, 0).0
);
const_assert!(AwaitedActionSortKey::new(i32::MAX - 1, 0).0 > AwaitedActionSortKey::new(1, 0).0);
const_assert!(AwaitedActionSortKey::new(1, 0).0 > AwaitedActionSortKey::new(0, 0).0);
const_assert!(AwaitedActionSortKey::new(0, 0).0 > AwaitedActionSortKey::new(-1, 0).0);
const_assert!(AwaitedActionSortKey::new(-1, 0).0 > AwaitedActionSortKey::new(i32::MIN + 1, 0).0);
const_assert!(
    AwaitedActionSortKey::new(i32::MIN + 1, 0).0 > AwaitedActionSortKey::new(i32::MIN, 0).0
);

// Ensure the insert timestamp is used as the sort key second.
const_assert!(AwaitedActionSortKey::new(0, u32::MIN).0 > AwaitedActionSortKey::new(0, u32::MAX).0);

#[cfg(test)]
mod sort_key_tests {
    use super::*;

    /// (g) The rewritten exact-value layout `[priority:32 | criticality:8 | inv_ts:24]`.
    ///
    /// MUTATION: shift criticality to `<< 16` (wrong region) → the exact value changes →
    /// this red-fails. Numeric-constant rule: assert the value at the constructor, not a
    /// doc-comment.
    #[test]
    fn sort_key_exact_layout_value() {
        assert_eq!(
            AwaitedActionSortKey::new(0x1234_5678, 0x9abc_def0).as_u64(),
            0x9234_5678_0043_210f,
            "the sort key must pack [priority:32 | criticality:8=0 | inverted_ts_low24:24] \
             = 0x9234_5678_0043_210f"
        );
    }

    /// (a) BAND PRESERVATION: criticality NEVER reorders across client priorities. For
    /// EVERY adjacent priority pair, MAX criticality + earliest ts at the LOWER priority
    /// must still sort BELOW MIN criticality + latest ts at the HIGHER priority.
    ///
    /// MUTATION: place criticality at `<< 32` (into the priority region) → a high-criticality
    /// low-priority action outranks a high-priority one → this red-fails.
    #[test]
    fn criticality_never_crosses_a_priority_band() {
        for &(low, high) in &[
            (i32::MIN, i32::MIN + 1),
            (-1, 0),
            (0, 1),
            (5, 6),
            (i32::MAX - 1, i32::MAX),
        ] {
            // Lower priority, MAXED-out secondary region (band 255, earliest ts).
            let low_key = AwaitedActionSortKey::new_with_criticality(low, u8::MAX, u32::MIN).as_u64();
            // Higher priority, ZEROED secondary region (band 0, latest ts).
            let high_key = AwaitedActionSortKey::new_with_criticality(high, 0, u32::MAX).as_u64();
            assert!(
                high_key > low_key,
                "priority {high} (band 0, latest ts) must ALWAYS outrank priority {low} \
                 (band 255, earliest ts) — criticality can never cross a client-priority \
                 band; got high={high_key:#018x} low={low_key:#018x}"
            );
        }
    }

    /// Within a priority band, a higher criticality band sorts first (the feature).
    #[test]
    fn higher_criticality_sorts_first_within_band() {
        let more = AwaitedActionSortKey::new_with_criticality(0, 200, u32::MAX).as_u64();
        let less = AwaitedActionSortKey::new_with_criticality(0, 100, u32::MIN).as_u64();
        assert!(
            more > less,
            "within one priority band a higher criticality (200) must sort first even against \
             a lower criticality (100) with an earlier insert ts; got more={more:#018x} \
             less={less:#018x}"
        );
    }

    /// (d) FIFO FALLBACK (confidence gate): two band-0 (absent/non-confident) actions of
    /// equal priority order by insert timestamp — EARLIER first — exactly as today.
    ///
    /// MUTATION: stop inverting the timestamp (drop `^ u32::MAX`) → later ts would sort
    /// first → this red-fails (FIFO broken).
    #[test]
    fn band_zero_falls_back_to_exact_fifo() {
        // Both band 0 (the confidence-gate fallback). Timestamps within the low-24-bit range.
        let earlier = AwaitedActionSortKey::new_with_criticality(0, 0, 1000).as_u64();
        let later = AwaitedActionSortKey::new_with_criticality(0, 0, 2000).as_u64();
        assert!(
            earlier > later,
            "with band 0 at equal priority, the EARLIER insert ts (1000) must sort first \
             (larger key) — today's FIFO; got earlier={earlier:#018x} later={later:#018x}"
        );
    }

    /// Criticality dominates the insert timestamp within a band (secondary above tertiary),
    /// but equal-criticality peers still tie-break by FIFO.
    #[test]
    fn criticality_dominates_then_fifo() {
        let crit_late = AwaitedActionSortKey::new_with_criticality(0, 5, 9000).as_u64();
        let noncrit_early = AwaitedActionSortKey::new_with_criticality(0, 4, 1).as_u64();
        assert!(crit_late > noncrit_early, "criticality outranks a lower band regardless of ts");
        let same_crit_early = AwaitedActionSortKey::new_with_criticality(0, 5, 1).as_u64();
        let same_crit_late = AwaitedActionSortKey::new_with_criticality(0, 5, 9000).as_u64();
        assert!(
            same_crit_early > same_crit_late,
            "equal-criticality peers tie-break by FIFO (earlier ts first)"
        );
    }
}

/// (#dag-criticality T1) End-to-end enqueue-fold test: an action whose OTel baggage
/// `(instance, target, mnemonic)` key matches a CONFIDENT snapshot node must carry that
/// node's band>0 in its `AwaitedActionSortKey`; a keyless (no-baggage) action stays band 0
/// (FIFO). This is the THIRD key-derivation site (`new_with_criticality`) — the fold that
/// the pre-fix cadre flagged as having ONLY a silent-FIFO failure mode with no test. It
/// pins the baggage→ProfileKey→snapshot.band chain against the same-shaped key the DAG
/// stores (built here via the production `compute_criticality` path).
#[cfg(test)]
mod dag_enqueue_tests {
    use core::time::Duration;
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use nativelink_macro::nativelink_test;
    use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
    use nativelink_util::action_messages::{ActionInfo, ActionUniqueKey, ActionUniqueQualifier};
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::origin_event::{BAZEL_METADATA_KEY, request_metadata_to_baggage};
    use opentelemetry::{Context, KeyValue};

    use super::*;
    use crate::dag_criticality::{DAG_MIN_EDGE_OBS, DagNodeKey, compute_criticality};

    const INSTANCE: &str = "main";
    const TARGET: &str = "//pkg:lib";
    const MNEMONIC: &str = "CppCompile";

    fn action_info(instance: &str) -> Arc<ActionInfo> {
        Arc::new(ActionInfo {
            command_digest: DigestInfo::new([0u8; 32], 0),
            input_root_digest: DigestInfo::new([0u8; 32], 0),
            timeout: Duration::MAX,
            platform_properties: HashMap::new(),
            priority: 0,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: UNIX_EPOCH
                .checked_add(Duration::from_secs(1_700_000_000))
                .unwrap(),
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                instance_name: instance.to_string(),
                digest_function: DigestHasherFunc::Sha256,
                digest: DigestInfo::new([1u8; 32], 1),
            }),
            targetkey: None,
        })
    }

    fn key(instance: &str, target: &str, mnemonic: &str) -> DagNodeKey {
        ProfileKey::from_parts(instance, target, mnemonic).expect("non-empty parts")
    }

    fn baggage_ctx(target: &str, mnemonic: &str) -> Context {
        let md = RequestMetadata {
            target_id: target.to_string(),
            action_mnemonic: mnemonic.to_string(),
            ..Default::default()
        };
        Context::current_with_baggage(vec![KeyValue::new(
            BAZEL_METADATA_KEY,
            request_metadata_to_baggage(&md),
        )])
    }

    #[nativelink_test]
    async fn dag_criticality_baggage_to_enqueue_band_applied() {
        // Build a CONFIDENT snapshot the production way: the node K = (INSTANCE, TARGET,
        // MNEMONIC) sits upstream of a sink over a MATURE edge (count == DAG_MIN_EDGE_OBS),
        // so K is a singleton-SCC confident node with band = quantize(w(K) + w(sink)) > 0.
        let k = key(INSTANCE, TARGET, MNEMONIC);
        let sink = key(INSTANCE, "//pkg:bin", MNEMONIC);
        let mut weights: HashMap<DagNodeKey, u32> = HashMap::new();
        weights.insert(k.clone(), 4000);
        weights.insert(sink.clone(), 1000);
        let snap = compute_criticality(&[(k.clone(), sink.clone(), 2)], &weights, DAG_MIN_EDGE_OBS);
        let expected_band = snap.band(&k);
        assert!(
            expected_band > 0,
            "test setup: node K must be a confident band>0 node, got {expected_band}"
        );

        // (1) An action whose baggage carries K's target+mnemonic + whose instance == K's
        // instance must fold K's band into the sort key.
        let applied_band = {
            let _guard = baggage_ctx(TARGET, MNEMONIC).attach();
            AwaitedAction::new_with_criticality(
                OperationId::default(),
                action_info(INSTANCE),
                SystemTime::now(),
                Some(&snap),
            )
            .sort_key()
            .criticality_band()
        };
        assert_eq!(
            applied_band, expected_band,
            "the baggage-derived key (instance,target,mnemonic) must match the confident \
             snapshot node → its band {expected_band} folded into the sort key; got \
             {applied_band}. A mismatch here is the 3-site key-derivation drift the cadre \
             flagged (silent FIFO)."
        );

        // (2) A keyless action (no baggage at all → no derivable node key) stays band 0 →
        // today's FIFO fallback.
        let keyless_band = AwaitedAction::new_with_criticality(
            OperationId::default(),
            action_info(INSTANCE),
            SystemTime::now(),
            Some(&snap),
        )
        .sort_key()
        .criticality_band();
        assert_eq!(
            keyless_band, 0,
            "an action with no OTel baggage has no derivable node key → band 0 (FIFO); got \
             {keyless_band}"
        );

        // (3) A baggage key that does NOT match any confident node (different target) also
        // stays band 0 — the confidence gate, not a spurious fold.
        let nonmatch_band = {
            let _guard = baggage_ctx("//pkg:unknown", MNEMONIC).attach();
            AwaitedAction::new_with_criticality(
                OperationId::default(),
                action_info(INSTANCE),
                SystemTime::now(),
                Some(&snap),
            )
            .sort_key()
            .criticality_band()
        };
        assert_eq!(
            nonmatch_band, 0,
            "an action whose baggage key is absent from the snapshot must read band 0 \
             (confidence gate → FIFO), not a spurious band; got {nonmatch_band}"
        );
    }
}
