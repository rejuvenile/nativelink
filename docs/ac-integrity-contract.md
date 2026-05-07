# Action Cache Integrity Contract

**Audience:** maintainers, integrity-tooling authors, anyone considering
extending `VerifyStore`-style hash checks to the AC chain.

**Status:** describes the as-of-2026-05-07 contract. See `#226` for the
investigation that produced this document.

## TL;DR

**The AC payload is NOT content-addressed by its filename hash.**
Filename hash != `SHA-256(file bytes)`. Wrapping the AC chain in a
`VerifyStore { verify_hash = true }` would always fail, by design. The
AC contract is structural, not cryptographic; integrity flows transitively
through the CAS digests embedded inside the `ActionResult` proto.

## CAS vs. AC: how each is keyed

### CAS — content-addressed

For the Content-Addressable Store, the storage key IS the cryptographic
hash of the stored bytes:

```
key = SHA-256(file bytes)            # by construction
```

`VerifyStore { verify_size, verify_hash }` re-hashes incoming and outgoing
streams and rejects any value where `H(bytes) != key`
(`nativelink-store/src/verify_store.rs:154-162` for write,
`:222-241` for read). Production wires the CAS chain through `cas_STORE`
which is exactly that: `verify` over `cas_INNER`
(`/home/user/fl/bld/infra/nativelink/prod-server.json5:109-120`).

### AC — *referentially* keyed by the action_digest

The AC stores `ActionResult` proto bytes under the **`action_digest`** —
which is the CAS digest of the *Action* proto, NOT a hash of the
ActionResult bytes themselves:

```
ac_key   = action_digest             # = SHA-256(Action proto bytes), already in CAS
ac_value = serialize(ActionResult)   # arbitrary; Bazel may rewrite
H(ac_value) != ac_key                # in general
```

This is the REAPI design (`build.bazel.remote.execution.v2`,
`ActionCache.UpdateActionResult` keys the ActionResult by
`action_digest`).

Source:

- `nativelink-service/src/ac_server.rs:158-188` — write path: digest =
  `request.action_digest`, value = `action_result.encode(...)`.
- `nativelink-worker/src/running_actions_manager.rs:4208-4254` —
  worker-side `upload_ac_results`: same shape.
- `nativelink-store/src/ac_utils.rs:42-92` — read path
  `get_and_decode_digest`: pulls bytes by key, decodes as proto. The
  ONLY integrity check is the protobuf decode success; no hash compare.

The same key value (the action_digest) appears in TWO different stores
holding TWO different byte sequences:

- **CAS** under that digest holds the `Action` proto (the
  request-side description of the action: command, input root, etc.).
  Content-addressed: `H(bytes) = digest`.
- **AC** under that digest holds the `ActionResult` proto (the
  response-side execution outcome: stdout/stderr digests, output file
  digests, exit code, etc.). NOT content-addressed: `H(bytes) != digest`
  in general.

Cross-reference: `ac_proxy_store.rs` module docs explicitly call out
this digest-collision footgun
(`nativelink-store/src/ac_proxy_store.rs:30-50`) — short-circuiting CAS
uploads on AC-pin presence would silently swallow the Action-proto bytes
because the CAS bytes are the truth-of-record but the AC pin shares the
digest.

## What integrity IS enforceable on AC

A byte-level hash check on AC is impossible (see above). But the
`ActionResult` proto carries CAS digests that ARE checkable. An end-to-end
"trust the cached action" flow looks like:

1. Read `ActionResult` from AC at key `action_digest`.
2. Decode as proto — fails with `Code::NotFound` ("Stored value appears
   to be corrupt") on a corrupt-but-not-decodable payload
   (`ac_utils.rs:84-91`).
3. For each `OutputFile { digest, … }` in the result, fetch bytes from
   CAS at that digest. CAS' `VerifyStore` enforces
   `H(bytes) == digest` on retrieval, so a corrupt output blob is
   detected at fetch time.
4. Same for `stdout_digest`, `stderr_digest`, `output_directories[].tree_digest`,
   nested file/directory digests inside the `Tree` proto, etc.
5. The Action proto itself sits in CAS at `action_digest`; if a caller
   wants end-to-end "this AC entry corresponds to this Action", they
   re-fetch the Action proto, walk its inputs (input-root tree digest →
   files, command digest → bytes), and check those CAS digests too.

Every leaf in this graph IS content-addressed in CAS and has byte-level
integrity. The AC entry is a structural index pointing into that graph.
Integrity is preserved transitively, not by re-hashing the AC payload.

## What if the AC payload is silently corrupted?

There is no crypto check that catches "AC bytes flipped a bit but still
parse as a valid ActionResult." The downstream consequences are bounded
by the CAS-digest re-checks above: a corrupted `output_files[].digest`
will resolve to a CAS `NotFound` (digest doesn't exist) or a CAS
`VerifyStore` mismatch (if some other blob happens to live at that
digest). A corrupted `exit_code` or non-digest field would silently
mis-cache action results — but this risk is symmetric with any non-hashed
metadata in any system, and is mitigated in production by:

- AC payloads are tiny (~100-1000 bytes; see
  `worker.json5:7-8` and `ac_utils.rs:32-34`), so the corruption
  surface is small.
- Production AC backend is Redis (`prod-server.json5:5-13, 35-51`),
  which has its own internal CRC checks and is RAM-backed; on-disk AC
  rewrites are not a hot path.
- The surface for *malicious* corruption is gated by the same trust
  boundary as any other store write (per-endpoint TLS + auth).

If a future deployment adds a FilesystemStore-backed AC, the integrity
contract is the same: filename hash IS NOT a re-hash of the bytes.
Filesystem-level integrity (e.g. ZFS checksums) covers bit-rot. There is
no "scan the AC directory and check `H(file) == filename digest`" tool
to write — that check would always fail.

## Why no `VerifyStore` on the AC chain

Production wires AC as:

```
AC_STORE
  = CompletenessCheckingStore { backend: AC_BACKEND_CACHED, cas: cas_STORE }
AC_BACKEND_CACHED
  = FastSlowStore { fast: MemoryStore(4 GB), slow: REDIS_AC_STORE }
```

(`/home/user/fl/bld/infra/nativelink/prod-server.json5:34-66`).

There is no `VerifyStore` anywhere on this chain, deliberately. Adding
one with `verify_hash = true` would reject every write because
`H(ActionResult bytes) != action_digest`. Adding one with
`verify_size = true` only would also reject every write, because the
caller's `digest.size_bytes` comes from the action_digest and is the size
of the *Action* proto, not the *ActionResult* proto.

Both flags are CAS-only invariants.

## Why no integrity scanner is recommended for AC

Any tool that walks AC files and checks `H(file_bytes) == filename hash`
would 100% fail on every entry. The filename is referential, not a
content hash.

What COULD be useful and IS NOT yet implemented:

- A scan that decodes each AC entry as a proto, lists referenced CAS
  digests, and verifies they all exist in CAS — i.e. an AC-graph
  consistency check. This is structurally what
  `CompletenessCheckingStore` already does on every `has` call
  (`nativelink-store/src/completeness_checking_store.rs`), so an
  offline scanner is largely redundant.

If you are extending the CAS integrity scanner (e.g. a sweep that walks
`/srv/casdata/nativelink/stores/content_path-cas/d/` and re-hashes each
file against its filename), DO NOT extend it to walk
`content_path-ac/d/`. That is intentionally not the same kind of store.

## Summary

| Property                               | CAS                              | AC                                   |
|----------------------------------------|----------------------------------|--------------------------------------|
| Filename / key                         | `SHA-256(bytes)`                 | `action_digest` (CAS digest of Action proto) |
| Filename hash == hash of stored bytes? | Yes (by construction)            | No (in general)                      |
| `VerifyStore` wraps in production?     | Yes (`verify_hash`+`verify_size`)| No (intentionally)                   |
| `content_is_immutable: true` allowed?  | Yes                              | No (same key may receive new bytes)  |
| Byte-level integrity check possible?   | Yes (re-hash)                    | No (proto-decode is the only check)  |
| Indirect integrity?                    | n/a                              | Via CAS digests inside `ActionResult` |

## Cross-references

- `nativelink-store/src/verify_store.rs` — the CAS integrity primitive.
- `nativelink-store/src/filesystem_store.rs:79-83` — `digest_content_path`
  layout (used for both CAS and any AC FilesystemStore; the **interpretation
  of the hash differs** between the two).
- `nativelink-service/src/ac_server.rs` — REAPI AC server.
- `nativelink-worker/src/running_actions_manager.rs:4208-` —
  `upload_ac_results` worker-side write path.
- `nativelink-store/src/ac_utils.rs` — AC read + decode helper.
- `nativelink-store/src/ac_proxy_store.rs` — peer-fetch wrapper (calls
  out the digest-collision concern in module docs).
- `/home/user/fl/bld/infra/nativelink/prod-server.json5` lines 27-66 (AC) and
  107-120 (CAS) — production wiring contrast.
