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

//! Pins the `FilesystemSpec.chunked_idle_partial_reap_ttl_s` config
//! surface (#F3 sibling, aa3fa9e3f review T-3):
//! - ABSENT field → 600 (default ON; "existing configs parse
//!   unchanged" is a load-bearing claim of the commit).
//! - Explicit `0` → 0 (the operational kill-switch parses; the store
//!   spawns no reaper task at 0).
//! - `FilesystemSpec::default()` mirrors the serde default (the
//!   `SimpleSpec` LOAD-BLIND-derive incident class:
//!   `simple_spec_default_test.rs`).

use nativelink_config::stores::FilesystemSpec;

const MINIMAL_SPEC_NO_TTL_FIELD: &str = r#"{
    "content_path": "/tmp/content",
    "temp_path": "/tmp/temp"
}"#;

const MINIMAL_SPEC_TTL_ZERO: &str = r#"{
    "content_path": "/tmp/content",
    "temp_path": "/tmp/temp",
    "chunked_idle_partial_reap_ttl_s": 0
}"#;

#[test]
fn absent_field_defaults_to_600_on() {
    let spec: FilesystemSpec = serde_json5::from_str(MINIMAL_SPEC_NO_TTL_FIELD)
        .expect("a pre-existing FilesystemSpec config without the new field must still parse");
    assert_eq!(
        spec.chunked_idle_partial_reap_ttl_s, 600,
        "an ABSENT chunked_idle_partial_reap_ttl_s must default to 600 s (reaper \
         ON — house policy: features ship ON, the flag is a kill-switch; a 0 \
         default would dark the reaper on every existing config)",
    );
}

#[test]
fn explicit_zero_is_the_kill_switch() {
    let spec: FilesystemSpec = serde_json5::from_str(MINIMAL_SPEC_TTL_ZERO)
        .expect("an explicit 0 must parse");
    assert_eq!(
        spec.chunked_idle_partial_reap_ttl_s, 0,
        "explicit 0 must survive parsing — it is the operational kill-switch \
         (FilesystemStore::new spawns no reaper task at 0)",
    );
}

#[test]
fn rust_default_mirrors_serde_default() {
    assert_eq!(
        FilesystemSpec::default().chunked_idle_partial_reap_ttl_s,
        600,
        "FilesystemSpec::default() must mirror the serde default (600) — a \
         diverging Default is the SimpleSpec LOAD-BLIND-derive incident class: \
         tests built from Default would silently run with a different reaper \
         configuration than deserialized production configs",
    );
}
