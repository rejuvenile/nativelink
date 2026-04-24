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

use prost::Message;

use crate::common::DigestInfo;

/// Local mirror of `google.rpc.PreconditionFailure` (not generated in
/// `nativelink-proto`). Used so store-layer NotFound errors carry a REAPI
/// v2 §2.2.4 MISSING violation that lets Bazel re-upload the blob.
#[derive(Message)]
pub struct PreconditionFailure {
    #[prost(message, repeated, tag = "1")]
    pub violations: Vec<Violation>,
}

#[derive(Message)]
pub struct Violation {
    #[prost(string, tag = "1")]
    pub r#type: String,
    #[prost(string, tag = "2")]
    pub subject: String,
    #[prost(string, tag = "3")]
    pub description: String,
}

/// Build a `prost_types::Any` containing a single MISSING `PreconditionFailure`
/// violation for `digest`. Attach to `Error.details` on NotFound returns from
/// the CAS so Bazel can recover via re-upload (REAPI v2 §2.2.4).
#[must_use]
pub fn make_precondition_failure_any(digest: DigestInfo) -> prost_types::Any {
    let failure = PreconditionFailure {
        violations: vec![Violation {
            r#type: "MISSING".into(),
            subject: format!("blobs/{}/{}", digest.packed_hash(), digest.size_bytes()),
            description: String::new(),
        }],
    };
    prost_types::Any {
        type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
        value: failure.encode_to_vec(),
    }
}
