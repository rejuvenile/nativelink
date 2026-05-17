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

// Benchmark-crate-specific lint relaxations. The bench is a private,
// binary-only crate (publish=false). Pedantic doc-markdown and
// numeric-cast lints from the workspace-wide `pedantic`/`nursery`
// groups are noise here: prose-heavy rationale doc-comments routinely
// name types like `FastSlow`, `MemoryStore`, and `buildcache-native.json5`
// without backticks; PRNG byte-fill truncates u64 → u8 by design.
// Production lint coverage stays in place for the `nativelink-*` crates.
#![allow(
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

//! #495 Phase 1: data-plane benchmark harness for the v3-anchoring smoke
//! cells.
//!
//! Background (see `.claude/audits/495-data-plane-benchmark-design-2026-05-16.md`):
//! the 2026-05-15 v3 bundle (#494 / #499) flipped
//! `default_chunked_v2_enabled = true`, switched per-digest coordination to
//! a per-digest `Notify`, and moved deferred-publish ownership to the
//! reaper. None of these changes are anchored by an automated benchmark.
//! The existing `benches/transport_bench.rs` measures TCP/QUIC transport
//! through an in-memory store; `nativelink-util/benches/fs_io_bench.rs`
//! measures filesystem primitives. Neither covers the production wrapper
//! chain (`FastSlow` → `Filesystem` → `GrpcStore`-peer-mirror) and neither has
//! baseline-diff for CI.
//!
//! This crate is **purely additive observability infrastructure** — it
//! does not change any production code path. It provides:
//!
//! 1. A scenario runner that constructs the production composition store
//!    stack (or as much of it as a self-contained harness can) and drives
//!    deterministic workloads through it.
//! 2. A JSON output schema (`ScenarioOutput`) suitable for diffing
//!    against a baseline checked into `benchmarks/baselines/`.
//! 3. A pre-flight gate that refuses to run when buildcache is serving live
//!    production traffic, unless explicitly overridden via `--force`.
//!
//! Composite invariant being established: "data-plane semantics changes
//! ship anchored against per-cell baselines, so performance regressions
//! introduced by a future v-bundle flip cannot land silently." The
//! benchmarks themselves are the test: producing a baseline + diffing
//! against it on every push is the mechanism that re-establishes the
//! invariant. The first baseline (`benchmarks/baselines/`) is the anchor.

pub mod composition;
pub mod output;
pub mod preflight;
pub mod scenarios;
