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

use core::convert::Into;
use core::str::Utf8Error;
use std::sync::{MutexGuard, PoisonError};

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use prost_types::TimestampError;
use serde::{Deserialize, Serialize};
use tokio::sync::AcquireError;
// Reexport of tonic's error codes which we use as "nativelink_error::Code".
pub use tonic::Code;

#[macro_export]
macro_rules! make_err {
    ($code:expr, $($arg:tt)+) => {{
        $crate::Error::new(
            $code,
            format!("{}", format_args!($($arg)+)),
        )
    }};
}

#[macro_export]
macro_rules! make_input_err {
    ($($arg:tt)+) => {{
        $crate::make_err!($crate::Code::InvalidArgument, $($arg)+)
    }};
}

#[macro_export]
macro_rules! error_if {
    ($cond:expr, $($arg:tt)+) => {{
        if $cond {
            Err($crate::make_err!($crate::Code::InvalidArgument, $($arg)+))?;
        }
    }};
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Error {
    #[serde(with = "CodeDef")]
    pub code: Code,
    pub messages: Vec<String>,
    #[serde(skip)]
    pub details: Vec<prost_types::Any>,
}

/// Local mirror of `google.rpc.PreconditionFailure` (defined again in
/// `nativelink-util::common` for callers; replicated here to keep
/// `nativelink-error` cycle-free). Used only by the custom `Debug` impl
/// to decode `Error::details` entries whose `type_url` is the REAPI v2
/// MISSING-violation type, so high-frequency NotFound logs stay compact
/// instead of dumping the encoded protobuf as a decimal byte array.
#[derive(prost::Message)]
struct DebugPreconditionFailure {
    #[prost(message, repeated, tag = "1")]
    violations: Vec<DebugViolation>,
}

#[derive(prost::Message)]
struct DebugViolation {
    #[prost(string, tag = "1")]
    r#type: String,
    #[prost(string, tag = "2")]
    subject: String,
    #[prost(string, tag = "3")]
    description: String,
}

const PRECONDITION_FAILURE_TYPE_URL: &str =
    "type.googleapis.com/google.rpc.PreconditionFailure";

/// `Debug` adapter for a single `prost_types::Any`: decodes well-known
/// types (currently `google.rpc.PreconditionFailure`) into a compact,
/// human-readable summary; for unknown types, emits `Any { type_url, len }`
/// instead of dumping `value` as a decimal byte array. Without this,
/// every NotFound log line carrying a REAPI MISSING detail printed
/// ~1KB of `[10, 88, 10, 7, ...]` and starved the tracing-appender.
struct AnyDebug<'a>(&'a prost_types::Any);

impl core::fmt::Debug for AnyDebug<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.0.type_url == PRECONDITION_FAILURE_TYPE_URL {
            // Best-effort decode. Fall through to byte-count summary on
            // failure rather than panicking — a malformed detail must
            // never break logging.
            if let Ok(pf) = <DebugPreconditionFailure as prost::Message>::decode(
                self.0.value.as_slice(),
            ) {
                let mut dbg = f.debug_struct("PreconditionFailure");
                dbg.field("violations", &ViolationsDebug(&pf.violations));
                return dbg.finish();
            }
        }
        f.debug_struct("Any")
            .field("type_url", &self.0.type_url)
            .field("len", &self.0.value.len())
            .finish()
    }
}

struct ViolationsDebug<'a>(&'a [DebugViolation]);

impl core::fmt::Debug for ViolationsDebug<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut list = f.debug_list();
        for v in self.0 {
            list.entry(&ViolationDebug(v));
        }
        list.finish()
    }
}

struct ViolationDebug<'a>(&'a DebugViolation);

impl core::fmt::Debug for ViolationDebug<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut dbg = f.debug_struct("Violation");
        dbg.field("type", &self.0.r#type);
        dbg.field("subject", &self.0.subject);
        if !self.0.description.is_empty() {
            dbg.field("description", &self.0.description);
        }
        dbg.finish()
    }
}

struct DetailsDebug<'a>(&'a [prost_types::Any]);

impl core::fmt::Debug for DetailsDebug<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut list = f.debug_list();
        for any in self.0 {
            list.entry(&AnyDebug(any));
        }
        list.finish()
    }
}

impl core::fmt::Debug for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Manually mirrored against `Display` below so `{:?}` and `{}`
        // produce the same field set; the only divergence from a derived
        // `Debug` is the `details` formatter (compact decode of REAPI
        // PreconditionFailure, byte-count fallback for unknown types).
        let mut builder = f.debug_struct("Error");
        builder.field("code", &self.code);
        if !self.messages.is_empty() {
            builder.field("messages", &self.messages);
        }
        if !self.details.is_empty() {
            builder.field("details", &DetailsDebug(&self.details));
        }
        builder.finish()
    }
}

impl MetricsComponent for Error {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        self.to_string().publish(kind, field_metadata)
    }
}

impl Error {
    #[must_use]
    pub const fn new_with_messages(code: Code, messages: Vec<String>) -> Self {
        Self {
            code,
            messages,
            details: Vec::new(),
        }
    }

    #[must_use]
    pub fn new(code: Code, msg: String) -> Self {
        if msg.is_empty() {
            Self::new_with_messages(code, vec![])
        } else {
            Self::new_with_messages(code, vec![msg])
        }
    }

    #[must_use]
    pub fn from_std_err(code: Code, mut err: &dyn core::error::Error) -> Self {
        let mut messages = vec![format!("{err}")];
        while let Some(src) = err.source() {
            messages.push(format!("{src}"));
            err = src;
        }
        messages.reverse();
        Self::new_with_messages(code, messages)
    }

    #[inline]
    #[must_use]
    pub fn append<S: Into<String>>(mut self, msg: S) -> Self {
        self.messages.push(msg.into());
        self
    }

    #[must_use]
    pub fn merge<E: Into<Self>>(mut self, other: E) -> Self {
        let mut other: Self = other.into();
        // This will help with knowing which messages are tied to different errors.
        self.messages.push("---".to_string());
        self.messages.append(&mut other.messages);
        self
    }

    #[must_use]
    pub fn merge_option<T: Into<Self>, U: Into<Self>>(
        this: Option<T>,
        other: Option<U>,
    ) -> Option<Self> {
        if let Some(this) = this {
            if let Some(other) = other {
                return Some(this.into().merge(other));
            }
            return Some(this.into());
        }
        other.map(Into::into)
    }

    #[must_use]
    pub fn to_std_err(self) -> std::io::Error {
        std::io::Error::new(self.code.into_error_kind(), self.messages.join(" : "))
    }

    #[must_use]
    pub fn message_string(&self) -> String {
        self.messages.join(" : ")
    }

    /// Construct a `NotFound` error for a missing blob digest with a
    /// pre-built detail (typically a `PreconditionFailure` MISSING violation
    /// from `nativelink_util::common::make_precondition_failure_any`)
    /// already attached. REAPI v2 §2.2.4 requires the structured detail so
    /// Bazel can re-upload. Use at every store-layer NotFound construction
    /// site that returns a missing-blob result.
    ///
    /// Lives on `Error` (not on a util helper) so the REAPI invariant is
    /// encapsulated in one named constructor and call sites stay one line.
    /// The detail is taken pre-built to avoid coupling `nativelink-error`
    /// to `nativelink-util` (cycle); callers pass
    /// `make_precondition_failure_any(digest)`.
    #[must_use]
    pub fn not_found_with_detail(msg: impl Into<String>, detail: prost_types::Any) -> Self {
        Self {
            code: Code::NotFound,
            messages: vec![msg.into()],
            details: vec![detail],
        }
    }

    /// Construct a `Code::ResourceExhausted` error tagged with a
    /// #212 `BackpressureSignal` proto detail. Phase 2 admission code
    /// (`FastSlowStore` + `WorkerProxyStore`) calls this on
    /// global-budget / per-blob-mpsc rejections so the receiver-side
    /// `looks_like_dead_channel` classifier in
    /// `nativelink-store/src/grpc_store.rs` can distinguish honest
    /// backpressure from the historic dead-h2-channel mapping.
    ///
    /// The detail bytes are the encoded
    /// `BackpressureSignal { reason, retry_after_ms }` proto. The
    /// `type_url` MUST match the `BACKPRESSURE_SIGNAL_TYPE_URL`
    /// constant in `nativelink-proto` — both ends compare against
    /// that exact string. Wire-stable per design §3.
    #[must_use]
    pub fn resource_exhausted_backpressure(
        msg: impl Into<String>,
        detail: prost_types::Any,
    ) -> Self {
        Self {
            code: Code::ResourceExhausted,
            messages: vec![msg.into()],
            details: vec![detail],
        }
    }

    /// Construct a `Code::Aborted` error carrying an arbitrary
    /// `prost_types::Any` detail. Used by the #212 WriteChunked handler
    /// to signal "another stream is racing you for the same digest;
    /// retry after a backoff" with a `BackpressureSignal` retry hint
    /// inside the detail. `Aborted` is preferred over `AlreadyExists`
    /// here because gRPC convention treats `AlreadyExists` as "the
    /// resource exists at the target" — a wire interpretation a
    /// worker-side BIS-style auto-unpinner could read as a license to
    /// drop its mirror pin (losing the only durable copy if the OTHER
    /// in-flight stream then errors on commit). `Aborted` carries the
    /// "transaction failed, retry" semantics that match the producer's
    /// real situation.
    #[must_use]
    pub fn aborted_with_detail(
        msg: impl Into<String>,
        detail: prost_types::Any,
    ) -> Self {
        Self {
            code: Code::Aborted,
            messages: vec![msg.into()],
            details: vec![detail],
        }
    }
}

impl core::error::Error for Error {}

impl From<Error> for nativelink_proto::google::rpc::Status {
    fn from(val: Error) -> Self {
        Self {
            code: val.code as i32,
            message: val.message_string(),
            details: val.details,
        }
    }
}

impl From<nativelink_proto::google::rpc::Status> for Error {
    fn from(val: nativelink_proto::google::rpc::Status) -> Self {
        Self {
            code: val.code.into(),
            messages: vec![val.message],
            details: val.details,
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // A manual impl to reduce the noise of frequently empty fields.
        // `details` is rendered through `DetailsDebug` so REAPI MISSING
        // payloads decode to a compact summary instead of dumping the
        // encoded bytes as a decimal array (would flood server logs).
        let mut builder = f.debug_struct("Error");

        builder.field("code", &self.code);

        if !self.messages.is_empty() {
            builder.field("messages", &self.messages);
        }

        if !self.details.is_empty() {
            builder.field("details", &DetailsDebug(&self.details));
        }

        builder.finish()
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<prost::UnknownEnumValue> for Error {
    fn from(err: prost::UnknownEnumValue) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<core::num::TryFromIntError> for Error {
    fn from(err: core::num::TryFromIntError) -> Self {
        Self::from_std_err(Code::InvalidArgument, &err)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl<T> From<PoisonError<MutexGuard<'_, T>>> for Error {
    fn from(err: PoisonError<MutexGuard<'_, T>>) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<serde_json5::Error> for Error {
    fn from(err: serde_json5::Error) -> Self {
        match err {
            serde_json5::Error::Message { msg, location } => {
                if let Some(has_location) = location {
                    make_err!(
                        Code::Internal,
                        "line {}, column {} - {}",
                        has_location.line,
                        has_location.column,
                        msg
                    )
                } else {
                    Self::new(Code::Internal, msg)
                }
            }
        }
    }
}

impl From<core::num::ParseIntError> for Error {
    fn from(err: core::num::ParseIntError) -> Self {
        Self::from_std_err(Code::InvalidArgument, &err)
    }
}

impl From<core::convert::Infallible> for Error {
    fn from(_err: core::convert::Infallible) -> Self {
        // Infallible is an error type that can never happen.
        unreachable!();
    }
}

impl From<TimestampError> for Error {
    fn from(err: TimestampError) -> Self {
        Self::from_std_err(Code::InvalidArgument, &err)
    }
}

impl From<AcquireError> for Error {
    fn from(err: AcquireError) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            code: err.kind().into_code(),
            messages: vec![err.to_string()],
            details: Vec::new(),
        }
    }
}

impl From<redis::RedisError> for Error {
    fn from(error: redis::RedisError) -> Self {
        use redis::ErrorKind::{
            AuthenticationFailed, InvalidClientConfig, Io as IoError, Parse as ParseError,
            UnexpectedReturnType,
        };

        // Conversions here are based on https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
        let code = match error.kind() {
            AuthenticationFailed => Code::PermissionDenied,
            ParseError | UnexpectedReturnType | InvalidClientConfig => Code::InvalidArgument,
            IoError => {
                if error.is_timeout() {
                    Code::DeadlineExceeded
                } else {
                    Code::Internal
                }
            }
            _ => Code::Unknown,
        };

        let kind = error.kind();
        make_err!(code, "{kind:?}: {error}")
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        // Round-trip the `grpc-status-details-bin` trailer encoded by the
        // sibling `From<Error> for tonic::Status` below — without this,
        // REAPI v2 §2.2.4 PreconditionFailure details are silently dropped
        // and Bazel cannot recover from missing-blob errors.
        //
        // The empty-bytes guard is *not* redundant: protobuf decodes an
        // empty buffer as an all-default `Status { code: 0, message: "",
        // details: vec![] }`, which would then convert to
        // `Error { code: Code::Ok, ... }` and silently lose the original
        // `status.code()` (e.g. `NotFound`).
        let details_bytes = status.details();
        if !details_bytes.is_empty() {
            match <nativelink_proto::google::rpc::Status as prost::Message>::decode(details_bytes) {
                Ok(rpc_status) => return Self::from(rpc_status),
                Err(err) => {
                    // A non-empty `grpc-status-details-bin` trailer that
                    // fails to decode is a real bug (peer encoded
                    // something other than `google.rpc.Status`). Log so
                    // the symptom isn't silently masked by the fallback.
                    tracing::warn!(
                        bytes_len = details_bytes.len(),
                        ?err,
                        code = ?status.code(),
                        "tonic::Status carried non-empty details that failed to decode as google.rpc.Status; falling back to code+message only",
                    );
                }
            }
        }
        Self::new(status.code(), status.to_string())
    }
}

impl From<Error> for tonic::Status {
    fn from(val: Error) -> Self {
        // Without preserving details, REAPI v2 §2.2.4 PreconditionFailure
        // entries are dropped on the wire. Bazel relies on the
        // `grpc-status-details-bin` trailer (an encoded `google.rpc.Status`)
        // to recover from missing-blob errors by re-uploading.
        if val.details.is_empty() {
            return Self::new(val.code, val.messages.join(" : "));
        }
        let code = val.code;
        let message = val.messages.join(" : ");
        let rpc_status = nativelink_proto::google::rpc::Status {
            code: code as i32,
            message: message.clone(),
            details: val.details,
        };
        let encoded = prost::Message::encode_to_vec(&rpc_status);
        Self::with_details(code, message, tonic::codegen::Bytes::from(encoded))
    }
}

impl From<walkdir::Error> for Error {
    fn from(err: walkdir::Error) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<uuid::Error> for Error {
    fn from(err: uuid::Error) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<rustls_pki_types::pem::Error> for Error {
    fn from(err: rustls_pki_types::pem::Error) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(err: tokio::time::error::Elapsed) -> Self {
        Self::from_std_err(Code::DeadlineExceeded, &err)
    }
}

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(err: mongodb::error::Error) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

impl From<zip::result::ZipError> for Error {
    fn from(err: zip::result::ZipError) -> Self {
        Self::from_std_err(Code::Internal, &err)
    }
}

pub trait ResultExt<T> {
    /// # Errors
    ///
    /// Will return `Err` if we can't convert the error.
    fn err_tip_with_code<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: ToString,
        F: (FnOnce(&Error) -> (Code, S)) + Sized;

    /// # Errors
    ///
    /// Will return `Err` if we can't convert the error.
    #[inline]
    fn err_tip<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: ToString,
        F: (FnOnce() -> S) + Sized,
    {
        self.err_tip_with_code(|e| (e.code, tip_fn()))
    }

    /// # Errors
    ///
    /// Will return `Err` if we can't merge the errors.
    fn merge<U>(self, _other: Result<U, Error>) -> Result<U, Error>
    where
        Self: Sized,
    {
        unreachable!();
    }
}

impl<T, E: Into<Error>> ResultExt<T> for Result<T, E> {
    #[inline]
    fn err_tip_with_code<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: ToString,
        F: (FnOnce(&Error) -> (Code, S)) + Sized,
    {
        self.map_err(|e| {
            let mut error: Error = e.into();
            let (code, message) = tip_fn(&error);
            error.code = code;
            error.messages.push(message.to_string());
            error
        })
    }

    fn merge<U>(self, other: Result<U, Error>) -> Result<U, Error>
    where
        Self: Sized,
    {
        if let Err(e) = self {
            let mut e: Error = e.into();
            if let Err(other_err) = other {
                let mut other_err: Error = other_err;
                // This will help with knowing which messages are tied to different errors.
                e.messages.push("---".to_string());
                e.messages.append(&mut other_err.messages);
            }
            return Err(e);
        }
        other
    }
}

impl<T> ResultExt<T> for Option<T> {
    #[inline]
    fn err_tip_with_code<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: ToString,
        F: (FnOnce(&Error) -> (Code, S)) + Sized,
    {
        self.ok_or_else(|| {
            let mut error = Error {
                code: Code::Internal,
                messages: vec![],
                details: Vec::new(),
            };
            let (code, message) = tip_fn(&error);
            error.code = code;
            error.messages.push(message.to_string());
            error
        })
    }
}

trait CodeExt {
    fn into_error_kind(self) -> std::io::ErrorKind;
}

impl CodeExt for Code {
    fn into_error_kind(self) -> std::io::ErrorKind {
        match self {
            Self::Aborted => std::io::ErrorKind::Interrupted,
            Self::AlreadyExists => std::io::ErrorKind::AlreadyExists,
            Self::DeadlineExceeded => std::io::ErrorKind::TimedOut,
            Self::InvalidArgument => std::io::ErrorKind::InvalidInput,
            Self::NotFound => std::io::ErrorKind::NotFound,
            Self::PermissionDenied => std::io::ErrorKind::PermissionDenied,
            Self::Unavailable => std::io::ErrorKind::ConnectionRefused,
            _ => std::io::ErrorKind::Other,
        }
    }
}

trait ErrorKindExt {
    fn into_code(self) -> Code;
}

impl ErrorKindExt for std::io::ErrorKind {
    fn into_code(self) -> Code {
        match self {
            Self::NotFound => Code::NotFound,
            Self::PermissionDenied => Code::PermissionDenied,
            Self::ConnectionRefused | Self::ConnectionReset | Self::ConnectionAborted => {
                Code::Unavailable
            }
            Self::AlreadyExists => Code::AlreadyExists,
            Self::InvalidInput | Self::InvalidData => Code::InvalidArgument,
            Self::TimedOut => Code::DeadlineExceeded,
            Self::Interrupted => Code::Aborted,
            Self::NotConnected
            | Self::AddrInUse
            | Self::AddrNotAvailable
            | Self::BrokenPipe
            | Self::WouldBlock
            | Self::WriteZero
            | Self::Other
            | Self::UnexpectedEof => Code::Internal,
            _ => Code::Unknown,
        }
    }
}

// Serde definition for tonic::Code. See: https://serde.rs/remote-derive.html
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(remote = "Code")]
pub enum CodeDef {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
    // NOTE: Additional codes must be added to stores.rs in ErrorCodes and also
    // in both match statements in retry.rs.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_to_rpc_status_preserves_details() {
        let detail = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
            value: vec![1, 2, 3], // Dummy bytes
        };
        let err = Error {
            code: Code::FailedPrecondition,
            messages: vec!["missing blob".into()],
            details: vec![detail.clone()],
        };
        let status: nativelink_proto::google::rpc::Status = err.into();
        assert_eq!(status.code, Code::FailedPrecondition as i32);
        assert_eq!(status.details.len(), 1);
        assert_eq!(status.details[0].type_url, detail.type_url);
        assert_eq!(status.details[0].value, detail.value);
    }

    #[test]
    fn rpc_status_to_error_preserves_details() {
        let detail = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
            value: vec![4, 5, 6],
        };
        let status = nativelink_proto::google::rpc::Status {
            code: Code::FailedPrecondition as i32,
            message: "test".into(),
            details: vec![detail.clone()],
        };
        let err: Error = status.into();
        assert_eq!(err.code, Code::FailedPrecondition);
        assert_eq!(err.details.len(), 1);
        assert_eq!(err.details[0].type_url, detail.type_url);
        assert_eq!(err.details[0].value, detail.value);
    }

    #[test]
    fn error_details_roundtrip_through_rpc_status() {
        let detail = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
            value: vec![10, 20, 30],
        };
        let original = Error {
            code: Code::FailedPrecondition,
            messages: vec!["missing".into()],
            details: vec![detail],
        };
        let status: nativelink_proto::google::rpc::Status = original.clone().into();
        let roundtripped: Error = status.into();
        assert_eq!(roundtripped.code, original.code);
        assert_eq!(roundtripped.details.len(), original.details.len());
        assert_eq!(roundtripped.details[0].type_url, original.details[0].type_url);
        assert_eq!(roundtripped.details[0].value, original.details[0].value);
    }

    #[test]
    fn make_err_macro_has_empty_details() {
        let err = make_err!(Code::Internal, "something failed");
        assert!(err.details.is_empty());
    }

    #[test]
    fn error_to_tonic_status_preserves_details() {
        use prost::Message;

        let detail = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
            value: vec![1, 2, 3],
        };
        let err = Error {
            code: Code::FailedPrecondition,
            messages: vec!["blob missing".into()],
            details: vec![detail.clone()],
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::FailedPrecondition);
        let details_bytes = status.details();
        assert!(
            !details_bytes.is_empty(),
            "tonic::Status::details() should be non-empty when Error has details",
        );
        // The details bytes are the encoded google.rpc.Status proto, which
        // gets sent via the grpc-status-details-bin trailer. Decode and
        // verify our PreconditionFailure detail round-tripped.
        let decoded = nativelink_proto::google::rpc::Status::decode(details_bytes)
            .expect("status details should decode as google.rpc.Status");
        assert_eq!(decoded.code, Code::FailedPrecondition as i32);
        assert_eq!(decoded.details.len(), 1);
        assert_eq!(decoded.details[0].type_url, detail.type_url);
        assert_eq!(decoded.details[0].value, detail.value);
    }

    #[test]
    fn error_to_tonic_status_no_details_when_empty() {
        let err = Error {
            code: Code::Internal,
            messages: vec!["boom".into()],
            details: Vec::new(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::Internal);
        assert!(
            status.details().is_empty(),
            "tonic::Status::details() should stay empty when Error has no details",
        );
    }

    #[test]
    fn tonic_status_to_error_decodes_details() {
        let any = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
            value: vec![7, 8, 9, 10],
        };

        // Build an Error → tonic::Status (uses the send-side encoder).
        let original_err = Error {
            code: Code::FailedPrecondition,
            messages: vec!["blob missing".into()],
            details: vec![any.clone()],
        };
        let status: tonic::Status = original_err.into();

        // Round-trip back via From<tonic::Status>.
        let recovered: Error = status.into();
        assert_eq!(recovered.code, Code::FailedPrecondition);
        assert_eq!(recovered.details.len(), 1, "details must round-trip");
        assert_eq!(recovered.details[0].type_url, any.type_url);
        assert_eq!(recovered.details[0].value, any.value);
    }

    #[test]
    fn tonic_status_to_error_no_trailer_falls_back_gracefully() {
        let status = tonic::Status::not_found("plain message");
        let err: Error = status.into();
        assert_eq!(err.code, Code::NotFound);
        assert!(err.details.is_empty());
        assert!(err.message_string().contains("plain message"));
    }

    /// Build a realistic `PreconditionFailure` Any payload — same wire shape
    /// `nativelink_util::common::make_precondition_failure_any` produces for
    /// a missing-blob NotFound. Uses raw prost encoding so this test does
    /// not depend on `nativelink-util` (would create a cycle).
    fn sample_precondition_failure_any(subject: &str) -> prost_types::Any {
        // PreconditionFailure { violations: [Violation { type, subject, description }] }
        // Hand-encode with prost::Message via a local mirror of the proto.
        #[derive(prost::Message)]
        struct Violation {
            #[prost(string, tag = "1")]
            r#type: String,
            #[prost(string, tag = "2")]
            subject: String,
            #[prost(string, tag = "3")]
            description: String,
        }
        #[derive(prost::Message)]
        struct PreconditionFailure {
            #[prost(message, repeated, tag = "1")]
            violations: Vec<Violation>,
        }
        let pf = PreconditionFailure {
            violations: vec![Violation {
                r#type: "MISSING".into(),
                subject: subject.into(),
                description: String::new(),
            }],
        };
        prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
            value: prost::Message::encode_to_vec(&pf),
        }
    }

    #[test]
    fn debug_format_decodes_precondition_failure_compactly() {
        // A real on-the-wire MISSING violation for a long blob subject —
        // the kind that floods server logs at ~1500 lines/sec via
        // not_found_with_detail. The derived `Debug` for `Vec<u8>` would
        // print every byte as a decimal (e.g. `[10, 88, 10, 7, 77, ...]`),
        // consuming ~1KB/line and falling tracing-appender 30-90s behind.
        let detail = sample_precondition_failure_any(
            "blobs/8513dc4e1a2b3c4d5e6f70819293a4b5c6d7e8f9001020304050607080910abc/183",
        );
        // Sanity: the encoded bytes really are long enough to flood logs
        // if printed as a decimal array.
        assert!(
            detail.value.len() > 60,
            "test fixture must be wide enough to expose the byte-array bug",
        );

        let err = Error::not_found_with_detail("Object not found in store", detail);
        let formatted = format!("{err:?}");

        // Semantic info must survive — readers need to know it's a
        // PreconditionFailure with the missing blob's subject.
        assert!(
            formatted.contains("PreconditionFailure"),
            "Debug must mention PreconditionFailure; got: {formatted}",
        );
        assert!(
            formatted.contains("MISSING"),
            "Debug must mention the violation type MISSING; got: {formatted}",
        );
        assert!(
            formatted.contains("blobs/8513dc4e"),
            "Debug must include the violation subject; got: {formatted}",
        );

        // The bug pattern: derived Debug renders Vec<u8> as
        // `[10, 88, 10, 7, ...]`. Forbid any decimal-array prefix of more
        // than 8 numbers — that's the signature of a raw bytes dump.
        let decimal_array = regex_like_decimal_run(&formatted);
        assert!(
            decimal_array <= 8,
            "Debug must not dump bytes as a decimal array (found run of {decimal_array}); got: {formatted}",
        );

        // Compact: a single-violation NotFound must not exceed 400 chars.
        // The pre-fix derived Debug ran ~600+ chars on this fixture.
        assert!(
            formatted.len() < 400,
            "Debug must stay compact (<400 chars); got {} chars: {formatted}",
            formatted.len(),
        );

        // Code and message must still print.
        assert!(formatted.contains("NotFound"));
        assert!(formatted.contains("Object not found in store"));
    }

    #[test]
    fn debug_format_unknown_type_url_falls_back_to_byte_count() {
        // Anys with type_urls we don't know how to decode must still
        // collapse to a compact byte-count summary instead of a raw byte
        // dump — same property as the PreconditionFailure path.
        let detail = prost_types::Any {
            type_url: "type.googleapis.com/some.unknown.Type".into(),
            value: vec![42u8; 200],
        };
        let err = Error {
            code: Code::Internal,
            messages: vec!["unknown detail".into()],
            details: vec![detail],
        };
        let formatted = format!("{err:?}");

        let decimal_array = regex_like_decimal_run(&formatted);
        assert!(
            decimal_array <= 8,
            "unknown-type Debug must not dump bytes as a decimal array (run of {decimal_array}); got: {formatted}",
        );
        assert!(
            formatted.contains("200 bytes") || formatted.contains("len: 200"),
            "unknown-type Debug must indicate the byte length; got: {formatted}",
        );
        assert!(
            formatted.contains("some.unknown.Type"),
            "unknown-type Debug must preserve the type_url; got: {formatted}",
        );
        assert!(
            formatted.len() < 200,
            "unknown-type Debug stays compact (<200 chars); got {} chars: {formatted}",
            formatted.len(),
        );
    }

    #[test]
    fn debug_format_no_details_unchanged() {
        // No details: the field should be omitted (matches existing
        // Display behavior so production logs stay clean).
        let err = Error {
            code: Code::Internal,
            messages: vec!["boom".into()],
            details: Vec::new(),
        };
        let formatted = format!("{err:?}");
        assert!(formatted.contains("Internal"));
        assert!(formatted.contains("boom"));
        assert!(
            !formatted.contains("details"),
            "details field should be omitted when empty; got: {formatted}",
        );
    }

    /// Find the longest run of consecutive `<int>, ` (or `<int>]`) tokens
    /// in `s` — the signature of a `Vec<u8>` Debug dump. Returns the count
    /// of decimal numbers in the longest such run. A "run" must look like
    /// `[N, N, N, ...]` to match — isolated numbers in legitimate text
    /// (e.g. byte counts, lengths) won't trigger.
    fn regex_like_decimal_run(s: &str) -> usize {
        let bytes = s.as_bytes();
        let mut max_run = 0usize;
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] != b'[' {
                i += 1;
                continue;
            }
            // Try to walk a `[N, N, N, ...]` sequence starting at i.
            let mut j = i + 1;
            let mut count = 0usize;
            loop {
                // Skip optional whitespace.
                while j < bytes.len() && bytes[j] == b' ' {
                    j += 1;
                }
                // Need at least one digit.
                let start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j == start {
                    break;
                }
                count += 1;
                // After a number, require either `, ` (continue) or `]` (end).
                if j < bytes.len() && bytes[j] == b',' {
                    j += 1;
                    continue;
                }
                break;
            }
            if count > max_run {
                max_run = count;
            }
            i = j.max(i + 1);
        }
        max_run
    }
}
